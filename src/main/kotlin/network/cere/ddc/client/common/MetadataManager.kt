package network.cere.ddc.client.common

import io.netty.handler.codec.http.HttpResponseStatus
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.api.PartitionTopology
import network.cere.ddc.client.common.exception.AppTopologyException
import org.slf4j.LoggerFactory
import java.lang.Long.max
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentSkipListSet
import java.util.function.Consumer
import java.util.zip.CRC32

class MetadataManager(
    bootstrapNodes: List<String>,
    private val client: WebClient,
    private val retries: Int,
    private val connectionNodesCacheSize: Int,
) {

    private val addressesDeque = ConcurrentLinkedDeque<String>()
    private val addressesSet = ConcurrentSkipListSet<String>()

    init {
        val nodes = bootstrapNodes.map { it.removeSuffix("/") }
        addressesDeque.addAll(nodes)
        addressesSet.addAll(nodes)
    }


    private val log = LoggerFactory.getLogger(javaClass)

    fun getAppTopology(appPubKey: String): AppTopology {
        val appTopology = Uni.createFrom().deferred {
            val address = addressesDeque.peek()
            client.getAbs("$address/api/rest/apps/${appPubKey}/topology")
                .`as`(BodyCodec.json(AppTopology::class.java))
                .send()
                .onFailure().transform { AppTopologyException("Can't connect to node", address, it) }
                .map { address to it }
        }.onItem().transform { pair ->
            if (pair.second.statusCode() != HttpResponseStatus.OK.code()) {
                throw AppTopologyException("Bad response from node (statusCode=${pair.second.statusCode()}, body=${pair.second.bodyAsString()})", pair.first)
            }

            pair.second.body()
        }
            .onFailure(AppTopologyException::class.java).invoke { ex ->
                val exception = ex as AppTopologyException
                log.warn("Can't load app topology from node address=${exception.address}")
                exception.address?.also { moveAddressToLast(it) }
            }
            .onFailure().retry().atMost(max(retries.toLong(), 1) * addressesSet.size - 1)
            .onFailure().transform { ex -> AppTopologyException("App topology is not available from nodes", ex) }
            .onFailure().invoke(Consumer { log.error("Couldn't load App from any node", it) })
            .runSubscriptionOn { Thread(it).start() }.await().indefinitely()

        updateNodeAddresses(appTopology)

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

    private fun updateNodeAddresses(appTopology: AppTopology) {
        if (addressesSet.size < connectionNodesCacheSize) {
            appTopology.partitions?.forEach { partition ->
                addAddress(partition.master?.nodeHttpAddress)
                partition.replicas?.forEach { addAddress(it.nodeHttpAddress) }
            }
        }
    }

    private fun addAddress(address: String?) =
        address != null && addressesSet.size < connectionNodesCacheSize
                && addressesSet.add(address) && addressesDeque.add(address)

    private fun moveAddressToLast(address: String) {
        if (addressesDeque.size > 1 && addressesDeque.removeFirstOccurrence(address)) {
            addressesDeque.add(address)
        }
    }
}