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
import network.cere.ddc.client.api.PartitionTopology
import network.cere.ddc.client.common.MetadataManager
import network.cere.ddc.client.producer.exception.ServiceUnavailableException
import java.lang.RuntimeException
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import java.util.zip.CRC32
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

    override fun consume(streamId: String, dataQuery: DataQuery): Multi<ConsumerRecord> {
        return streams.getOrPut(streamId, { initializeStream(streamId, dataQuery) }).processor
    }

    override fun getByCid(userPubKey: String, cid: String): Uni<Piece> {
        val ringToken = CRC32().apply { update(userPubKey.toByteArray()) }.value
        val targetNode =
            appTopology.partitions!!.reversed().first { it.ringToken!! <= ringToken }.master!!.nodeHttpAddress
        return client.getAbs("$targetNode/api/rest/ipfs/pieces/$cid").send()
            .onItem().transform { res ->
                return@transform when (res.statusCode()) {
                    OK.code() -> res.bodyAsJson(Piece::class.java)
                    INTERNAL_SERVER_ERROR.code() -> {
                        log.warn("Internal server error (body=${res.bodyAsString()})")
                        throw RuntimeException(res.bodyAsString())
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

    private fun initializeStream(streamId: String, dataQuery: DataQuery): Stream {
        val stream = Stream(streamId, UnicastProcessor.create(), dataQuery)
        streams[streamId] = stream

        metadataManager.getAppTopology(config.appPubKey).partitions!!.forEach { partitionTopology ->
            consumePartition(stream, partitionTopology)
        }
        return stream
    }

    private fun consumePartition(
        stream: Stream,
        partitionTopology: PartitionTopology,
        lastToken: String = "",
    ) {
        log.debug("Going to start consuming the partition (streamId=${stream.id}, partitionId=${partitionTopology.partitionId})")
        val checkpointKey = "${config.appPubKey}:${partitionTopology.partitionId}:${stream.id}"
        var checkpointValue = if (lastToken.isNotEmpty()) {
            lastToken
        } else {
            checkpointer.getCheckpoint(checkpointKey)
        }

        val pollPartition = Uni.createFrom().item {
            val node = partitionTopology.master!!.nodeHttpAddress
            var url =
                "$node/api/rest/pieces?appPubKey=${config.appPubKey}&partitionId=${partitionTopology.partitionId}"

            //TODO handle end of stream when 'to' specified
            if (checkpointValue != null) {
                url += "&from=$checkpointValue&to=" + Instant.now().toString()
            } else {
                if (stream.dataQuery.from.isNotEmpty() && stream.dataQuery.to.isNotEmpty()) {
                    url += "&from=${stream.dataQuery.from}&to=${stream.dataQuery.to}"
                }
            }

            if (stream.dataQuery.fields.isNotEmpty()) {
                url += "&fields=" + stream.dataQuery.fields.joinToString(",")
            }

            val parser = JsonParser.newParser().objectValueMode().handler { event ->
                val piece = event.mapTo(Piece::class.java)
                //TODO introduce lastToken to DDC node?
                checkpointValue = piece.timestamp!!.plusMillis(1).toString()
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

        val pollPartitionIndefinitelyWithInterval = pollPartition.onItem()
            .delayIt().by(partitionPollInterval)
            .runSubscriptionOn(executor)
            .repeat().indefinitely()

        val partitionSubscription = pollPartitionIndefinitelyWithInterval.subscribe()
            .with(
                { res -> log.debug("Partition polled (statusCode=${res.statusCode()})") },
                { e -> log.error("Partition poll failure", e) }
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
                    consumePartition(stream, partitionTopology, partitionTopology.createdAt!!)
                }
            }
        }
        appTopology = updatedAppTopology
    }

    private class Stream(
        val id: String,
        val processor: UnicastProcessor<ConsumerRecord>,
        val dataQuery: DataQuery,
    )
}
