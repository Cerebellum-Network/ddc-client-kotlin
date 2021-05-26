package network.cere.ddc.client.producer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.smallrye.mutiny.Uni
import io.vertx.core.Vertx
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.producer.exception.InsufficientNetworkCapacityException
import network.cere.ddc.client.producer.exception.InvalidAppTopologyException
import network.cere.ddc.client.producer.exception.ServiceUnavailableException
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.CRC32

class DdcProducer(
    private val config: ProducerConfig,
    vertx: Vertx = Vertx.vertx(),
) : Producer {

    private companion object {
        private const val API_PREFIX = "/api/rest"
    }

    private val log = LoggerFactory.getLogger(javaClass)

    private val client: WebClient = WebClient.create(vertx)

    private val appTopology: AtomicReference<AppTopology> = AtomicReference()

    private val signer = Ed25519Sign(Hex.decode(config.appPrivKey.removePrefix("0x")))

    init {
        DatabindCodec.mapper().registerModule(KotlinModule())

        updateAppTopology()
    }

    override fun send(piece: Piece) {
        sign(piece)

        val ringToken = CRC32().apply { update(piece.userPubKey.toByteArray()) }.value

        Uni.createFrom().item {
            val targetNode =
                appTopology.get().partitions.reversed().first { it.ringToken <= ringToken }.master.nodeHttpAddress
            client.postAbs("$targetNode$API_PREFIX/pieces").sendJson(piece).toCompletionStage().toCompletableFuture()
                .join()
        }
            .onItem().invoke { res ->
                if (res.statusCode() == CREATED.code()) {
                    return@invoke
                }

                if (res.statusCode() == CONFLICT.code()) {
                    log.warn("Duplicate message with id ${piece.id}. Skipping.")
                }

                if (res.statusCode() == MISDIRECTED_REQUEST.code()) {
                    log.warn("Invalid local network topology")
                    updateAppTopology()
                    throw InvalidAppTopologyException()
                }

                if (res.statusCode() == INSUFFICIENT_STORAGE.code()) {
                    log.warn("Insufficient network capacity")
                    updateAppTopology()
                    throw InsufficientNetworkCapacityException()
                }

                if (res.statusCode() == SERVICE_UNAVAILABLE.code()) {
                    log.warn("Service unavailable (body=${res.bodyAsString()})")
                    throw ServiceUnavailableException()
                }

                log.warn("Unknown exception (statusCode=${res.statusCode()})")
                throw RuntimeException(res.bodyAsString())
            }
            .onFailure(InvalidAppTopologyException::class.java).retry().atMost(1)
            .onFailure(InsufficientNetworkCapacityException::class.java).retry().withBackOff(config.retryBackoff)
            .indefinitely()
            .onFailure().retry().withBackOff(config.retryBackoff).atMost(config.retries.toLong())
            .await().indefinitely()
    }

    private fun updateAppTopology() {
        log.info("Updating app topology")
        val topology = getAppTopology(config.appPubKey)

        log.debug("Topology received:\n${topology}")
        appTopology.set(topology)
    }

    private fun getAppTopology(appPubKey: String): AppTopology {
        var retryAttempt = 0
        return Uni.createFrom().item {
            val res = client.getAbs("${config.bootstrapNodes[retryAttempt++]}${API_PREFIX}/apps/${appPubKey}/topology")
                .`as`(BodyCodec.json(AppTopology::class.java))
                .send()
                .toCompletionStage().toCompletableFuture().join()
            if (res.statusCode() != OK.code()) {
                log.error("Can't load app topology (statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                throw RuntimeException("Can't load app topology")
            }

            res.body()
        }.onFailure().retry().atMost(config.bootstrapNodes.size.toLong()).await().indefinitely()
    }

    private fun sign(piece: Piece) {
        val msg = piece.id + piece.timestamp + piece.appPubKey + piece.userPubKey + piece.data
        val signature = signer.sign(msg.toByteArray())
        piece.signature = Hex.encode(signature)
    }
}
