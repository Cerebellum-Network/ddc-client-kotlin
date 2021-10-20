package network.cere.ddc.client.producer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.smallrye.mutiny.Uni
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.ext.web.client.WebClient
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.common.MetadataManager
import network.cere.ddc.client.producer.exception.InsufficientNetworkCapacityException
import network.cere.ddc.client.producer.exception.InvalidAppTopologyException
import network.cere.ddc.client.producer.exception.ServiceUnavailableException
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import java.util.concurrent.atomic.AtomicReference

class DdcProducer(
    private val config: ProducerConfig,
    vertx: Vertx = Vertx.vertx(),
) : Producer {

    private val log = LoggerFactory.getLogger(javaClass)

    private val client: WebClient = WebClient.create(vertx)

    private val metadataManager = MetadataManager(config.bootstrapNodes, client, config.retries, config.connectionNodesCacheSize)

    private val appTopology: AtomicReference<AppTopology> = AtomicReference()

    private val signer = Ed25519Sign(Hex.decode(config.appPrivKey.removePrefix("0x")).sliceArray(0 until 32))

    init {
        DatabindCodec.mapper().registerModule(KotlinModule())

        updateAppTopology()
    }

    override fun send(piece: Piece): Uni<SendPieceResponse> {
        sign(piece)

        return Uni.createFrom().deferred {
            val targetNode = metadataManager.getProducerTargetNode(piece.userPubKey!!, appTopology.get())
            client.postAbs("$targetNode/api/rest/pieces").sendJson(piece)
        }
            .onItem().transform { res ->
                return@transform when (res.statusCode()) {
                    CREATED.code() -> res.bodyAsJson(SendPieceResponse::class.java)
                    CONFLICT.code() -> {
                        log.warn("Duplicate message with id ${piece.id}. Skipping.")
                        SendPieceResponse("")
                    }
                    MISDIRECTED_REQUEST.code() -> {
                        log.warn("Invalid local app topology")
                        updateAppTopology()
                        throw InvalidAppTopologyException()
                    }
                    INSUFFICIENT_STORAGE.code() -> {
                        log.warn("Insufficient network capacity")
                        updateAppTopology()
                        throw InsufficientNetworkCapacityException()
                    }
                    SERVICE_UNAVAILABLE.code() -> {
                        log.warn("Service unavailable (body=${res.bodyAsString()})")
                        throw ServiceUnavailableException()
                    }
                    else -> {
                        log.warn("Unknown exception (statusCode=${res.statusCode()})")
                        throw RuntimeException(res.bodyAsString())
                    }
                }
            }
            .onFailure { it is InvalidAppTopologyException || it is InsufficientNetworkCapacityException }.retry()
            .atMost(1)
            .onFailure().retry().withBackOff(config.retryBackoff).atMost(config.retries.toLong())
    }

    private fun updateAppTopology() {
        log.info("Updating app topology")
        val topology = metadataManager.getAppTopology(config.appPubKey)

        log.debug("Topology received:\n${topology}")
        appTopology.set(topology)
    }

    private fun sign(piece: Piece) {
        val msg = piece.id + piece.timestamp + piece.appPubKey + piece.userPubKey + piece.data
        val signature = signer.sign(msg.toByteArray())
        piece.signature = Hex.encode(signature)
    }
}
