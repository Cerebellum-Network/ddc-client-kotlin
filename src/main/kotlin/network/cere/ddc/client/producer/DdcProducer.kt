package network.cere.ddc.client.producer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.smallrye.mutiny.Uni
import io.vertx.core.http.HttpClientOptions.DEFAULT_MAX_POOL_SIZE
import io.vertx.core.http.HttpVersion
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.ext.web.client.WebClient
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.common.Ed25519Signer
import network.cere.ddc.client.common.MetadataManager
import network.cere.ddc.client.producer.exception.InsufficientNetworkCapacityException
import network.cere.ddc.client.producer.exception.InvalidAppTopologyException
import network.cere.ddc.client.producer.exception.ServiceUnavailableException
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class DdcProducer(
    private val config: ProducerConfig,
    vertx: Vertx = Vertx.vertx(),
) : Producer {

    private val log = LoggerFactory.getLogger(javaClass)

    private val client: WebClient = WebClient.create(
        vertx,
        WebClientOptions().setHttp2MaxPoolSize(DEFAULT_MAX_POOL_SIZE).setProtocolVersion(HttpVersion.HTTP_2)
    )

    private val metadataManager =
        MetadataManager(config.bootstrapNodes, client, config.retries, config.connectionNodesCacheSize)

    private val appTopology: AtomicReference<AppTopology> = AtomicReference()

    private val signer = Ed25519Signer(config.appPrivKey)

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
            .onFailure().invoke { -> updateAppTopology() }
            .onFailure().retry().withBackOff(config.connectionRetryBackOff).expireIn(config.retryExpiration.toMillis())
            .onItem().transform { res ->
                when (res.statusCode()) {
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
        piece.signature = signer.sign(msg)
    }
}
