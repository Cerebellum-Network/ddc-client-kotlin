package network.cere.ddc.client.common

import io.netty.handler.codec.http.HttpResponseStatus
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.tuples.Tuple4
import io.smallrye.mutiny.unchecked.Unchecked
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.api.PartitionTopology
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.ConnectException
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentSkipListSet
import java.util.zip.CRC32

class MetadataManager(
    bootstrapNodes: List<String>,
    private val client: WebClient,
    private val retries: Int,
    connectionNodesCacheSize: Int,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    private val nodes = ConcurrentDynamicNodeList(connectionNodesCacheSize, bootstrapNodes.map { it.removeSuffix("/") })

    fun getAppTopology(appPubKey: String): AppTopology {
        val appTopology = nodes.getValues()
            .onItem().transformToUni {
                client.getAbs("$it/api/rest/apps/${appPubKey}/topology")
                    .`as`(BodyCodec.json(AppTopology::class.java))
                    .send()
                    .onFailure(ConnectException::class.java).recoverWithNull()
            }.merge(1).map(Unchecked.function { res ->
                if (res.statusCode() != HttpResponseStatus.OK.code()) {
                    log.warn("Can't load app topology (statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                    throw IOException("Can't load app topology")
                }

                res.body()
            }).onFailure().retry().atMost(retries.toLong())
            .onFailure(NoSuchElementException::class.java).transform {
                log.error("Can't load app topology. All known nodes are unavailable. message=${it.message}")
                IOException("Can't load app topology. All known nodes are unavailable.")
            }
            .collect().first().await().indefinitely()


        appTopology?.partitions?.forEach { partition ->
            partition.replicas?.forEach { nodes.add(it.nodeHttpAddress) }
            nodes.add(partition.master?.nodeHttpAddress)
        }

        return appTopology
    }

    fun getProducerTargetNode(userPubKey: String, appTopology: AppTopology): String? {
        val ringToken = CRC32().apply { update(userPubKey.toByteArray()) }.value
        return appTopology.partitions!!.filter { it.active }
            .first { it.sectorStart!! <= ringToken && ringToken <= it.sectorEnd!! }.master!!.nodeHttpAddress
    }

    fun getConsumerTargetPartitions(userPubKey: String, appTopology: AppTopology): List<PartitionTopology> {
        val ringToken = CRC32().apply { update(userPubKey.toByteArray()) }.value
        return appTopology.partitions!!.filter { it.sectorStart!! <= ringToken && ringToken <= it.sectorEnd!! }
    }

    private class ConcurrentDynamicNodeList(
        private val size: Int,
        startValues: Collection<String> = emptyList()
    ) {
        private val addressDeque = ConcurrentLinkedDeque(startValues)
        private val addressSet = ConcurrentSkipListSet(startValues)

        fun getValues(): Multi<String> {
            return Multi.createFrom().generator<Tuple4<MutableIterator<String>, Int, String, Int>?, String?>(
                { Tuple4.of(addressDeque.iterator(), addressDeque.size, null as String?, 0) }
            ) { state, emitter ->
                state.item3?.also { moveToLast(it) }
                val item = state.item1
                    .takeIf { state.item4 < state.item2 && state.item1.hasNext() }
                    ?.next()

                item?.also { emitter.emit(it) } ?: emitter.fail(NoSuchElementException("No more nodes to connect"))

                Tuple4.of(state.item1, state.item2, item, state.item4 + 1)
            }
        }

        fun add(nodeHttpAddress: String?) = nodeHttpAddress
            ?.let { addressSet.size < size && addressSet.add(nodeHttpAddress) && addressDeque.add(nodeHttpAddress) }

        private fun moveToLast(nodeHttpAddress: String) {
            if (addressDeque.size <= 1 || addressDeque.peek() != nodeHttpAddress) {
                return
            }

            if (addressDeque.removeFirstOccurrence(nodeHttpAddress)) {
                addressDeque.add(nodeHttpAddress)
            }
        }
    }
}