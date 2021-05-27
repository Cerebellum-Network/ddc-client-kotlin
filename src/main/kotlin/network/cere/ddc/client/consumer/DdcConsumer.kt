package network.cere.ddc.client.consumer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.OK
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
import java.lang.RuntimeException
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import kotlin.collections.HashMap
import kotlin.concurrent.schedule

class DdcConsumer(
    private val config: ConsumerConfig,
    vertx: Vertx = Vertx.vertx(),
    private val checkpointer: Checkpointer = InMemoryCheckpointer()
) : Consumer {

    private companion object {
        private const val API_PREFIX = "/api/rest"
    }

    private val partitionPollInterval = Duration.ofMillis(config.partitionPollIntervalMs.toLong())

    private val log = LoggerFactory.getLogger(javaClass)

    private val client: WebClient = WebClient.create(vertx)

    // <streamId + partitionId> to subscription
    private val partitionSubscriptions = HashMap<String, Cancellable>()

    private val streams = HashMap<String, Stream>()

    private val uncommittedCheckpoints = HashMap<String, String>()

    private val executor = Executors.newFixedThreadPool(config.partitionPollExecutorSize)

    private var appTopology: AppTopology

    init {
        DatabindCodec.mapper().registerModule(KotlinModule())

        appTopology = getAppTopology(config.appPubKey)

        Timer("updateAppTopology").schedule(0, config.updateAppTopologyIntervalMs.toLong()) { updateAppTopology() }

        if (config.enableAutoCommit) {
            Timer("commitCheckpoints").schedule(0, config.autoCommitIntervalMs.toLong()) { commitCheckpoints() }
        }
    }

    override fun consume(streamId: String, dataQuery: DataQuery): Multi<ConsumerRecord> {
        return streams.getOrPut(streamId, { initializeStream(streamId, dataQuery) }).processor
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

        getAppTopology(config.appPubKey).partitions.forEach { partitionTopology ->
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
            val node = partitionTopology.master.nodeHttpAddress
            var url =
                "$node$API_PREFIX/pieces?appPubKey=${config.appPubKey}&partitionId=${partitionTopology.partitionId}"

            if (checkpointValue != null) {
                url += "&from=$checkpointValue&to=" + Instant.now().toString()
            }

            if (stream.dataQuery.fields.isNotEmpty()) {
                url += "&fields=" + stream.dataQuery.fields.joinToString(",")
            }

            val parser = JsonParser.newParser().objectValueMode().handler { event ->
                val piece = event.mapTo(Piece::class.java)
                //TODO introduce lastToken to DDC node?
                checkpointValue = piece.timestamp.plusMillis(1).toString()
                val consumerRecord = ConsumerRecord(piece, partitionTopology.partitionId, checkpointValue!!)
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
            .with { res -> log.debug("Partition polled (statusCode=${res.statusCode()})") }

        partitionSubscriptions[stream.id + partitionTopology.partitionId] = partitionSubscription
    }

    private fun updateAppTopology() {
        val partitionIds = appTopology.partitions.map { it.partitionId }

        val updatedAppTopology = getAppTopology(config.appPubKey)
        val newPartitions = updatedAppTopology.partitions.filterNot { partitionIds.contains(it.partitionId) }

        if (newPartitions.isNotEmpty()) {
            log.debug("${newPartitions.size} new partitions found")
            streams.values.forEach { stream ->
                newPartitions.forEach { partitionTopology ->
                    consumePartition(stream, partitionTopology, partitionTopology.createdAt)
                }
            }
        }
        appTopology = updatedAppTopology
    }

    private fun getAppTopology(appPubKey: String): AppTopology {
        var retryAttempt = 0
        return Uni.createFrom().deferred {
            client.getAbs("${config.bootstrapNodes[retryAttempt++]}${API_PREFIX}/apps/${appPubKey}/topology")
                .`as`(BodyCodec.json(AppTopology::class.java))
                .send()
        }.onItem().transform { res ->
            if (res.statusCode() != OK.code()) {
                log.error("Can't load app topology (statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                throw RuntimeException("Can't load app topology")
            }

            res.body()
        }.onFailure().retry().atMost(config.bootstrapNodes.size.toLong())
            .runSubscriptionOn { Thread(it).start() }
            .await().indefinitely()
    }

    private class Stream(
        val id: String,
        val processor: UnicastProcessor<ConsumerRecord>,
        val dataQuery: DataQuery,
    )
}
