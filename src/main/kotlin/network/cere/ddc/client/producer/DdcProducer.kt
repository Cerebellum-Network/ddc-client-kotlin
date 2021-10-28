package network.cere.ddc.client.producer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.smallrye.mutiny.Uni
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.ext.web.client.WebClient
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.common.Ed25519Signer
import network.cere.ddc.client.common.MetadataManager
import network.cere.ddc.client.common.exception.InitializeException
import network.cere.ddc.client.producer.exception.InsufficientNetworkCapacityException
import network.cere.ddc.client.producer.exception.InvalidAppTopologyException
import network.cere.ddc.client.producer.exception.ServiceUnavailableException
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Predicate

class DdcProducer(
    private val config: ProducerConfig,
    vertx: Vertx = Vertx.vertx(),
) : Producer {

    private val log = LoggerFactory.getLogger(javaClass)

    private val client: WebClient = WebClient.create(vertx)

    private val metadataManager =
        MetadataManager(config.bootstrapNodes, client, config.retries, config.connectionNodesCacheSize)

    private val appTopology: AtomicReference<CompletableFuture<AppTopology>> = AtomicReference()

    private val signer = Ed25519Signer(config.appPrivKey)

    init {
        DatabindCodec.mapper().registerModule(KotlinModule())

        initializeAppTopology()
    }

    override fun send(piece: Piece): Uni<SendPieceResponse> {
        sign(piece)
        val failurePredicate =
            Predicate<Throwable> { it is InvalidAppTopologyException || it is InsufficientNetworkCapacityException }

        return Uni.createFrom().completionStage { appTopology.get() }
            .onItem().transformToUni { item ->
                val targetNode = metadataManager.getProducerTargetNode(piece.userPubKey!!, item)
                client.postAbs("$targetNode/api/rest/pieces").sendJson(piece)
            }
            .onFailure().call { -> updateAppTopology() }
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
                        throw InvalidAppTopologyException()
                    }
                    INSUFFICIENT_STORAGE.code() -> {
                        log.warn("Insufficient network capacity")
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
            .onFailure(failurePredicate).call { -> updateAppTopology() }
            .onFailure(failurePredicate).retry().atMost(1)
            .onFailure().retry().withBackOff(config.retryBackoff).atMost(config.retries.toLong())

    }

    private fun updateAppTopology(): Uni<Void> {
        log.info("Updating app topology")
        return metadataManager.getAppTopology(config.appPubKey)
            .onItem().invoke { item ->
                log.debug("Topology received:\n{}", item)
                appTopology.set(Uni.createFrom().item(item).subscribeAsCompletionStage())
            }
            .replaceWithVoid()
    }

    private fun initializeAppTopology() {
        val appTopologyInitializer = Uni.createFrom().deferred {
            log.debug("Start initializing appTopology")
            metadataManager.getAppTopology(config.appPubKey)
        }
            .onFailure().transform { ex -> InitializeException(ex) }
            .onFailure().invoke{ ex ->
                log.warn("Error initializing appTopology", ex)
                initializeAppTopology()
            }
            .onCancellation().invoke{ initializeAppTopology() }
            .onItem().invoke{ item -> appTopology.set(Uni.createFrom().item(item).subscribeAsCompletionStage()) }
            .subscribeAsCompletionStage()

        appTopology.set(appTopologyInitializer)
    }

    private fun sign(piece: Piece) {
        val msg = piece.id + piece.timestamp + piece.appPubKey + piece.userPubKey + piece.data
        piece.signature = signer.sign(msg)
    }
}
