package network.cere.ddc.client.consumer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor
import io.smallrye.mutiny.subscription.Cancellable
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpVersion
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.core.parsetools.JsonParser
import io.vertx.mutiny.core.streams.WriteStream
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.api.Partition
import network.cere.ddc.client.api.PartitionTopology
import network.cere.ddc.client.common.MetadataManager
import network.cere.ddc.client.common.exception.InitializeException
import network.cere.ddc.client.consumer.checkpointer.Checkpointer
import network.cere.ddc.client.consumer.checkpointer.InMemoryCheckpointer
import network.cere.ddc.client.consumer.exception.PartitionTopologyMissException
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.concurrent.schedule

class DdcConsumer(
    private val config: ConsumerConfig,
    vertx: Vertx = Vertx.vertx(),
    private val checkpointer: Checkpointer = InMemoryCheckpointer()
) : Consumer {
    private val partitionPollInterval = Duration.ofMillis(config.partitionPollIntervalMs.toLong())

    private val log = LoggerFactory.getLogger(javaClass)

    // <streamId + partitionId> to subscription
    private val partitionSubscriptions = ConcurrentHashMap<String, Cancellable>()

    private val streams = ConcurrentHashMap<String, Stream>()

    private val uncommittedCheckpoints = HashMap<String, String>()

    private val executor = Executors.newFixedThreadPool(config.partitionPollExecutorSize)

    private val client: WebClient

    private val metadataManager: MetadataManager

    private var appTopology: CompletableFuture<AppTopology>

    init {
        DatabindCodec.mapper().registerModule(KotlinModule())

        val clientOptions = WebClientOptions()
            .setMaxPoolSize(config.nodeConnectionHttp1PoolSize)
            .setHttp2MaxPoolSize(config.nodeConnectionHttp2PoolSize)
            .setHttp2ConnectionWindowSize(config.nodeConnectionWindowSize)
            .setProtocolVersion(HttpVersion.HTTP_2)
            .setUseAlpn(true)
        //.setHttp2ClearTextUpgrade(false) TODO uncomment when require only HTTP2
        client = WebClient.create(vertx, clientOptions)

        metadataManager =
            MetadataManager(config.bootstrapNodes, client, config.retries, config.connectionNodesCacheSize)

        appTopology = initializeAppTopology()

        if (config.enableAutoCommit) {
            Timer("commitCheckpoints").schedule(0, config.autoCommitIntervalMs.toLong()) { commitCheckpoints() }
        }
    }

    override fun consume(streamId: String, fields: List<String>, offsetReset: OffsetReset): Multi<ConsumerRecord> {
        return metadataManager.getAppTopology(config.appPubKey)
            /** todo normal solution? Don't all methods of [Checkpointer] must have suspend keyword?
             *   hotfix: to allow blocking calls [consumePartition] -> [Checkpointer.getCheckpoint]
             */
            .emitOn(Infrastructure.getDefaultExecutor())
            .onItem().transform { item ->
                val stream =
                    streams.getOrPut(streamId) { Stream(streamId, UnicastProcessor.create(), fields, offsetReset) }
                item.partitions!!.forEach { partitionTopology -> consumePartition(stream, partitionTopology) }

                stream
            }.onItem().transformToMulti { it.processor }
    }

    override fun getAppPieces(from: String, to: String, fields: List<String>): Multi<Piece> {
        var pathQuery = ""
        if (from.isNotEmpty() && to.isNotEmpty()) {
            pathQuery += "&from=$from&to=$to"
        }

        if (fields.isNotEmpty()) {
            pathQuery += "&fields=" + fields.joinToString(",")
        }
        return metadataManager.getAppTopology(config.appPubKey)
            .onItem().transformToMulti { topology ->
                Multi.createFrom().iterable(topology.partitions!!.filter { partitionMatchesTimeRange(it, from, to) })
            }.cache()
            .flatMap { partition ->
                val stream = UnicastProcessor.create<Piece>()
                val parser = JsonParser.newParser().objectValueMode().handler { event ->
                    stream.onNext(event.mapTo(Piece::class.java))
                }

                val url =
                    "${partition.master!!.nodeHttpAddress}/api/rest/pieces?appPubKey=${config.appPubKey}&partitionId=${partition.partitionId}" + pathQuery

                log.debug("Fetching app pieces (url={})", url)
                client.getAbs(url)
                    .`as`(BodyCodec.jsonStream(parser))
                    .send()
                    .subscribe().with({ res ->
                        if (res.statusCode() == OK.code()) {
                            log.debug("App pieces successfully fetched (url={})", url)
                            stream.onComplete()
                        } else {
                            log.debug(
                                "Failed to fetch app pieces (url={}, statusCode={}, body={})",
                                url, res.statusCode(), res.bodyAsString()
                            )
                            stream.onFailure()
                        }
                    }, { e ->
                        log.error("Failed to fetch app pieces (url=$url)", e)
                    })
                stream
            }
    }

    override fun getUserPieces(userPubKey: String, from: String, to: String, fields: List<String>): Multi<Piece> {
        var pathQuery = ""
        if (from.isNotEmpty() && to.isNotEmpty()) {
            pathQuery += "&from=$from&to=$to"
        }

        if (fields.isNotEmpty()) {
            pathQuery += "&fields=" + fields.joinToString(",")
        }

        return Uni.createFrom().completionStage { appTopology }
            .onItem().transformToMulti { appTopology ->
                Multi.createBy().concatenating().streams(
                    metadataManager.getConsumerTargetPartitions(userPubKey, appTopology)
                        .filter { partitionMatchesTimeRange(it, from, to) }
                        .sortedBy { it.createdAt }
                        .map { partition ->
                            val stream = UnicastProcessor.create<Piece>()
                            val parser = JsonParser.newParser().objectValueMode().handler { event ->
                                stream.onNext(event.mapTo(Piece::class.java))
                            }

                            val url =
                                "${partition.master!!.nodeHttpAddress}/api/rest/pieces?userPubKey=$userPubKey&appPubKey=${config.appPubKey}&partitionId=${partition.partitionId}" + pathQuery

                            log.debug("Fetching user pieces (url={})", url)
                            client.getAbs(url)
                                .`as`(BodyCodec.jsonStream(parser))
                                .send()
                                .subscribe().with({ res ->
                                    if (res.statusCode() == OK.code()) {
                                        log.debug("User pieces successfully fetched (url={})", url)
                                        stream.onComplete()
                                    } else {
                                        log.debug(
                                            "Failed to fetch user pieces (url={}, statusCode={}, body={})",
                                            url, res.statusCode(), res.bodyAsString()
                                        )
                                        stream.onFailure()
                                    }
                                }, { e ->
                                    log.error("Failed to fetch user pieces (url=$url)", e)
                                })
                            stream
                        }
                )
            }
    }

    override fun getPiece(userPubKey: String, cid: String): Uni<Piece> {
        return Uni.createFrom().completionStage { appTopology }
            .onItem().transformToUni { appTopology ->
                Uni.join().first(
                    metadataManager.getConsumerTargetPartitions(userPubKey, appTopology)
                        .distinctBy { it.master!!.nodeId }
                        .map { targetPartition ->
                            client.getAbs("${targetPartition.master!!.nodeHttpAddress}/api/rest/pieces/$cid").send()
                                .onItem().transform { res ->
                                    when (res.statusCode()) {
                                        OK.code() -> res.bodyAsJson(Piece::class.java)
                                        NOT_FOUND.code() -> {
                                            log.warn("Not found (body=${res.bodyAsString()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                        INTERNAL_SERVER_ERROR.code() -> {
                                            log.warn("Internal server error (body=${res.bodyAsString()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                        SERVICE_UNAVAILABLE.code() -> {
                                            log.warn("Service unavailable (body=${res.bodyAsString()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                        else -> {
                                            log.warn("Unknown exception (statusCode=${res.statusCode()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                    }
                                }
                        }).withItem()
            }
    }

    override fun getPieceData(userPubKey: String, cid: String): Multi<Buffer> {
        return Uni.createFrom().completionStage { appTopology }
            .onItem().transformToUni { appTopology ->
                Uni.join().first(
                    metadataManager.getConsumerTargetPartitions(userPubKey, appTopology)
                        .distinctBy { it.master!!.nodeId }
                        .map { targetPartition ->
                            val url = "${targetPartition.master!!.nodeHttpAddress}/api/rest/pieces/$cid/data"
                            val chunkStream = ChunkStream()
                            client.getAbs(url)
                                .`as`(BodyCodec.pipe(WriteStream.newInstance(chunkStream)))
                                .send()
                                .onItem().transform { res ->
                                    when (res.statusCode()) {
                                        OK.code() -> {
                                            chunkStream
                                        }
                                        NOT_FOUND.code() -> {
                                            log.warn("Not found (url=$url, body=${res.bodyAsString()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                        INTERNAL_SERVER_ERROR.code() -> {
                                            log.warn("Internal server error (url=$url, body=${res.bodyAsString()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                        SERVICE_UNAVAILABLE.code() -> {
                                            log.warn("Service unavailable (url=$url, body=${res.bodyAsString()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                        else -> {
                                            log.warn("Unknown exception (url=$url, statusCode=${res.statusCode()})")
                                            throw RuntimeException(res.bodyAsString())
                                        }
                                    }
                                }
                        }
                ).withItem()
            }.toMulti().flatMap { it.toMulti() }
    }

    override fun commitCheckpoint(streamId: String, consumerRecord: ConsumerRecord) {
        val checkpointKey = "${config.appPubKey}:${consumerRecord.partitionId}:${streamId}"
        checkpointer.setCheckpoint(checkpointKey, consumerRecord.checkpointValue)
    }

    private fun commitCheckpoints() {
        uncommittedCheckpoints.forEach { (checkpointKey, checkpointValue) ->
            checkpointer.setCheckpoint(checkpointKey, checkpointValue)
        }
    }

    override fun close() {
        partitionSubscriptions.forEach { (_, subscription) ->
            subscription.cancel()
        }
    }

    private fun consumePartition(
        stream: Stream,
        partitionTopology: PartitionTopology,
    ) {
        log.debug(
            "Going to start consuming the partition (streamId={}, partitionId={})",
            stream.id, partitionTopology.partitionId
        )
        val checkpointKey = "${config.appPubKey}:${partitionTopology.partitionId}:${stream.id}"
        var checkpointValue = checkpointer.getCheckpoint(checkpointKey)
        var node = partitionTopology.master!!.nodeHttpAddress

        val pollPartition = Uni.createFrom().item {
            var url = "$node/api/rest/pieces?appPubKey=${config.appPubKey}&partitionId=${partitionTopology.partitionId}"

            if (checkpointValue == null) {
                checkpointValue = when (stream.offsetReset) {
                    OffsetReset.EARLIEST -> "1"
                    OffsetReset.LATEST -> {
                        val partitionUrl =
                            "$node/api/rest/partitions/${partitionTopology.partitionId}?appPubKey=${config.appPubKey}"
                        val partition = client.getAbs(partitionUrl)
                            .`as`(BodyCodec.json(Partition::class.java))
                            .sendAndAwait()
                            .body()
                        (partition.offset!! + 1).toString()
                    }
                }
            }
            url += "&offset=$checkpointValue"

            if (stream.fields.isNotEmpty()) {
                url += "&fields=" + stream.fields.joinToString(",")
            }

            val parser = JsonParser.newParser().objectValueMode().handler { event ->
                val piece = event.mapTo(Piece::class.java)
                checkpointValue = (piece.offset!! + 1).toString()
                val consumerRecord = ConsumerRecord(piece, partitionTopology.partitionId!!, checkpointValue!!)
                stream.processor.onNext(consumerRecord)
            }

            log.debug("Polling partition (url={})", url)
            client.getAbs(url)
                .`as`(BodyCodec.jsonStream(parser))
                .send()
                .onItem().invoke { _ ->
                    log.debug("Partition successfully polled (url={}, checkpointValue={})", url, checkpointValue)
                    if (checkpointValue != null) {
                        uncommittedCheckpoints[checkpointKey] = checkpointValue!!
                    }
                }
                .onFailure().invoke { e ->
                    log.error("Error on streaming data from DDC from URL $url", e)
                }.await().indefinitely()
        }
            .onFailure().call { ->
                Uni.createFrom().completionStage { appTopology }
                    .onItem().invoke { appTopology ->
                        node =
                            appTopology.partitions?.find { it.partitionId == partitionTopology.partitionId }?.master?.nodeHttpAddress
                                ?: throw PartitionTopologyMissException("Partition id=${partitionTopology.partitionId} missed from AppTopology")
                    }
            }
            .onFailure { it !is PartitionTopologyMissException }.retry()
            .withBackOff(config.minRetryBackOff, config.maxRetryBackOff).indefinitely()

        val pollPartitionUntilSealedWithInterval = pollPartition.onItem()
            .delayIt().by(partitionPollInterval)
            .runSubscriptionOn(executor)
            .repeat().until { it.statusCode() == NO_CONTENT.code() }

        val partitionSubscription = pollPartitionUntilSealedWithInterval.subscribe()
            .with(
                { res -> log.debug("Partition polled (statusCode={})", res.statusCode()) },
                { e -> log.error("Partition poll failure", e) },
                {
                    log.debug("Sealed partition is completely consumed (partitionId={})", partitionTopology.partitionId)
                }
            )

        partitionSubscriptions[stream.id + partitionTopology.partitionId] = partitionSubscription
    }

    private fun updateAppTopology(newAppTopology: AppTopology) {
        Uni.createFrom().completionStage { appTopology }
            /** todo normal solution? Don't all methods of [Checkpointer] must have suspend keyword?
             *   hotfix: to allow blocking calls [consumePartition] -> [Checkpointer.getCheckpoint]
             */
            .emitOn(Infrastructure.getDefaultExecutor())
            .onItem().invoke { appTopology ->
                val partitionIds = appTopology.partitions!!.map { it.partitionId }

                val newPartitions = newAppTopology.partitions!!.filterNot { partitionIds.contains(it.partitionId) }

                if (newPartitions.isNotEmpty()) {
                    log.debug("{} new partitions found", newPartitions.size)
                    streams.values.forEach { stream ->
                        newPartitions.forEach { partitionTopology ->
                            consumePartition(stream, partitionTopology)
                        }
                    }
                }
            }.subscribe().with { appTopology = CompletableFuture.completedFuture(newAppTopology) }
    }

    private fun initializeAppTopology(): CompletableFuture<AppTopology> {
        val scheduleDelay = Duration.ofMillis(config.updateAppTopologyIntervalMs.toLong())
        val scheduledGetAppTopology = Uni.createFrom().deferred {
            metadataManager.getAppTopology(config.appPubKey)
        }
            .repeat().withDelay(scheduleDelay).indefinitely()
            .onFailure().invoke { ex -> log.info("Failed to update AppTopology, retry", ex) }
            .onFailure().retry().withBackOff(scheduleDelay).indefinitely()

        return Uni.createFrom().deferred {
            log.debug("Start initializing appTopology")
            metadataManager.getAppTopology(config.appPubKey)
        }
            .onFailure().transform { ex -> InitializeException(ex) }
            .onFailure().invoke { ex ->
                log.warn("Error initializing appTopology", ex)
                appTopology = initializeAppTopology()
            }
            .onCancellation().invoke { appTopology = initializeAppTopology() }
            .onItem().invoke { loadedAppTopology ->
                appTopology = CompletableFuture.completedFuture(loadedAppTopology)
                scheduledGetAppTopology.subscribe().with { newAppTopology -> updateAppTopology(newAppTopology) }
            }.subscribeAsCompletionStage()
    }

    private fun partitionMatchesTimeRange(partitionTopology: PartitionTopology, from: String, to: String): Boolean {
        if (from.isEmpty() || to.isEmpty()) {
            return true
        }

        val createdBeforeEndOfTimeRange = partitionTopology.createdAt!! < to
        val activeOrSealedAfterStartOfTimeRange = partitionTopology.active || partitionTopology.updatedAt!! > from
        return createdBeforeEndOfTimeRange && activeOrSealedAfterStartOfTimeRange
    }

    private class Stream(
        val id: String,
        val processor: UnicastProcessor<ConsumerRecord>,
        val fields: List<String>,
        val offsetReset: OffsetReset
    )
}
