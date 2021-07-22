package network.cere.ddc.client.consumer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.smallrye.mutiny.Multi
import io.vertx.core.json.jackson.DatabindCodec
import org.slf4j.LoggerFactory
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.consumer.checkpointer.Checkpointer
import network.cere.ddc.client.consumer.checkpointer.InMemoryCheckpointer

import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.subscription.Cancellable
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.core.parsetools.JsonParser
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.Partition
import network.cere.ddc.client.api.PartitionTopology
import network.cere.ddc.client.common.MetadataManager
import java.lang.RuntimeException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.collections.HashMap
import kotlin.concurrent.schedule

class DdcConsumer(
    private val config: ConsumerConfig,
    vertx: Vertx = Vertx.vertx(),
    private val checkpointer: Checkpointer = InMemoryCheckpointer()
) : Consumer {
    private val partitionPollInterval = Duration.ofMillis(config.partitionPollIntervalMs.toLong())

    private val log = LoggerFactory.getLogger(javaClass)

    private val client: WebClient = WebClient.create(vertx)

    private val metadataManager: MetadataManager = MetadataManager(config.bootstrapNodes, client)

    // <streamId + partitionId> to subscription
    private val partitionSubscriptions = HashMap<String, Cancellable>()

    private val streams = HashMap<String, Stream>()

    private val uncommittedCheckpoints = HashMap<String, String>()

    private val executor = Executors.newFixedThreadPool(config.partitionPollExecutorSize)

    private var appTopology: AppTopology

    init {
        DatabindCodec.mapper().registerModule(KotlinModule())

        appTopology = metadataManager.getAppTopology(config.appPubKey)

        Timer("updateAppTopology").schedule(0, config.updateAppTopologyIntervalMs.toLong()) { updateAppTopology() }

        if (config.enableAutoCommit) {
            Timer("commitCheckpoints").schedule(0, config.autoCommitIntervalMs.toLong()) { commitCheckpoints() }
        }
    }

    override fun consume(streamId: String, fields: List<String>, offsetReset: OffsetReset): Multi<ConsumerRecord> {
        return streams.getOrPut(streamId, { initializeStream(streamId, fields, offsetReset) }).processor
    }

    override fun getAppPieces(from: String, to: String, fields: List<String>): Multi<Piece> {
        var pathQuery = ""
        if (from.isNotEmpty() && to.isNotEmpty()) {
            pathQuery += "&from=$from&to=$to"
        }

        if (fields.isNotEmpty()) {
            pathQuery += "&fields=" + fields.joinToString(",")
        }

        return Multi.createBy().merging().streams(
            metadataManager.getAppTopology(config.appPubKey).partitions!!
                .filter { partitionMatchesTimeRange(it, from, to) }
                .map { partition ->
                    val stream = UnicastProcessor.create<Piece>()
                    val parser = JsonParser.newParser().objectValueMode().handler { event ->
                        stream.onNext(event.mapTo(Piece::class.java))
                    }

                    val url =
                        "${partition.master!!.nodeHttpAddress}/api/rest/pieces?appPubKey=${config.appPubKey}&partitionId=${partition.partitionId}" + pathQuery

                    log.debug("Fetching app pieces (url=$url)")
                    client.getAbs(url)
                        .`as`(BodyCodec.jsonStream(parser))
                        .send()
                        .subscribe().with({ res ->
                            if (res.statusCode() == OK.code()) {
                                log.debug("App pieces successfully fetched (url=$url)")
                                stream.onComplete()
                            } else {
                                log.debug("Failed to fetch app pieces (url=$url, statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                                stream.onFailure()
                            }
                        }, { e ->
                            log.error("Failed to fetch app pieces (url=$url)", e)
                        })
                    stream
                }
        )
    }

    override fun getUserPieces(userPubKey: String, from: String, to: String, fields: List<String>): Multi<Piece> {
        var pathQuery = ""
        if (from.isNotEmpty() && to.isNotEmpty()) {
            pathQuery += "&from=$from&to=$to"
        }

        if (fields.isNotEmpty()) {
            pathQuery += "&fields=" + fields.joinToString(",")
        }

        return Multi.createBy().concatenating().streams(
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

                    log.debug("Fetching user pieces (url=$url)")
                    client.getAbs(url)
                        .`as`(BodyCodec.jsonStream(parser))
                        .send()
                        .subscribe().with({ res ->
                            if (res.statusCode() == OK.code()) {
                                log.debug("User pieces successfully fetched (url=$url)")
                                stream.onComplete()
                            } else {
                                log.debug("Failed to fetch user pieces (url=$url, statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                                stream.onFailure()
                            }
                        }, { e ->
                            log.error("Failed to fetch user pieces (url=$url)", e)
                        })
                    stream
                }
        )
    }

    override fun getByCid(userPubKey: String, cid: String): Uni<Piece> {
        return Uni.join().first(
            metadataManager.getConsumerTargetPartitions(userPubKey, appTopology)
                .map { targetPartition ->
                    client.getAbs("${targetPartition.master!!.nodeHttpAddress}/api/rest/ipfs/pieces/$cid").send()
                        .onItem().transform { res ->
                            return@transform when (res.statusCode()) {
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

    private fun initializeStream(streamId: String, fields: List<String>, offsetReset: OffsetReset): Stream {
        val stream = Stream(streamId, UnicastProcessor.create(), fields, offsetReset)
        streams[streamId] = stream

        metadataManager.getAppTopology(config.appPubKey).partitions!!.forEach { partitionTopology ->
            consumePartition(stream, partitionTopology)
        }
        return stream
    }

    private fun consumePartition(
        stream: Stream,
        partitionTopology: PartitionTopology,
    ) {
        log.debug("Going to start consuming the partition (streamId=${stream.id}, partitionId=${partitionTopology.partitionId})")
        val checkpointKey = "${config.appPubKey}:${partitionTopology.partitionId}:${stream.id}"
        var checkpointValue = checkpointer.getCheckpoint(checkpointKey)

        val pollPartition = Uni.createFrom().item {
            val node = partitionTopology.master!!.nodeHttpAddress
            var url =
                "$node/api/rest/pieces?appPubKey=${config.appPubKey}&partitionId=${partitionTopology.partitionId}"

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
                        (partition.latestOffset!! + 1).toString()
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

            log.debug("Polling partition (url=$url)")
            client.getAbs(url)
                .`as`(BodyCodec.jsonStream(parser))
                .send()
                .onItem().invoke { _ ->
                    log.debug("Partition successfully polled (url=$url, checkpointValue=$checkpointValue)")
                    if (checkpointValue != null) {
                        uncommittedCheckpoints[checkpointKey] = checkpointValue!!
                    }
                }
                .onFailure().invoke { e ->
                    log.error("Error on streaming data from DDC from URL $url", e)
                }.await().indefinitely()
        }

        val pollPartitionUntilSealedWithInterval = pollPartition.onItem()
            .delayIt().by(partitionPollInterval)
            .runSubscriptionOn(executor)
            .repeat().until { it.statusCode() == NO_CONTENT.code() }

        val partitionSubscription = pollPartitionUntilSealedWithInterval.subscribe()
            .with(
                { res -> log.debug("Partition polled (statusCode=${res.statusCode()})") },
                { e -> log.error("Partition poll failure", e) },
                {
                    log.debug("Sealed partition is completely consumed (partitionId=${partitionTopology.partitionId})")
                }
            )

        partitionSubscriptions[stream.id + partitionTopology.partitionId] = partitionSubscription
    }

    private fun updateAppTopology() {
        val partitionIds = appTopology.partitions!!.map { it.partitionId }

        val updatedAppTopology = metadataManager.getAppTopology(config.appPubKey)
        val newPartitions = updatedAppTopology.partitions!!.filterNot { partitionIds.contains(it.partitionId) }

        if (newPartitions.isNotEmpty()) {
            log.debug("${newPartitions.size} new partitions found")
            streams.values.forEach { stream ->
                newPartitions.forEach { partitionTopology ->
                    consumePartition(stream, partitionTopology)
                }
            }
        }
        appTopology = updatedAppTopology
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
