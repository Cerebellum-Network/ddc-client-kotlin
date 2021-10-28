package network.cere.ddc.client.common

import io.netty.handler.codec.http.HttpResponseStatus
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.api.PartitionTopology
import network.cere.ddc.client.common.exception.AppTopologyLoadException
import network.cere.ddc.client.common.exception.InitializeException
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

    private val log = LoggerFactory.getLogger(javaClass)

    private val addressesDeque = ConcurrentLinkedDeque<String>()
    private val addressesSet = ConcurrentSkipListSet<String>()

    init {
        addressesSet.addAll(bootstrapNodes.map { it.removeSuffix("/") })
        addressesDeque.addAll(addressesSet)
    }

    fun getAppTopology(appPubKey: String): Uni<AppTopology> {
        val appTopologyUni = Uni.createFrom().deferred {
            sendRequestAppTopology(appPubKey, addressesDeque.peek())
        }.onItem().invoke { item -> updateNodeAddresses(item) }

        val appTopologyFailResistedUni = appTopologyUni
            .onFailure(AppTopologyLoadException::class.java).invoke { ex ->
                val exception = ex as AppTopologyLoadException
                log.warn("Can't load app topology from node (address=${exception.address}, issue='${exception.message}')")
                exception.address?.also { moveAddressToLast(it) }
            }
            .onFailure().retry().atMost(max(retries.toLong(), 1) * addressesSet.size)
            .onFailure().transform { ex ->
                AppTopologyLoadException("App topology is not available from nodes", ex)
                    .also { log.error("Couldn't load App from any node", it) }
            }

        return appTopologyFailResistedUni
    }

    fun getAppTopologyInitializer(
        appPubKey: String,
        onItem: Consumer<AppTopology>,
        onFailure: Consumer<Throwable>,
        onCancellation: Runnable
    ): Uni<AppTopology> {
        return Uni.createFrom().deferred {
            log.debug("Start initializing appTopology")
            getAppTopology(appPubKey)
        }
            .onFailure().transform { ex -> InitializeException(ex) }
            .onFailure().invoke(onFailure)
            .onCancellation().invoke(onCancellation)
            .onItem().invoke(onItem)
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

    private fun sendRequestAppTopology(appPubKey: String, address: String) =
        client.getAbs("$address/api/rest/apps/${appPubKey}/topology")
            .`as`(BodyCodec.json(AppTopology::class.java)).send()
            .onFailure().transform { AppTopologyLoadException("Can't connect to node", address, it) }
            .onItem().transform { resp ->
                if (resp.statusCode() != HttpResponseStatus.OK.code()) {
                    throw AppTopologyLoadException(
                        "Bad response from node (statusCode=${resp.statusCode()}, body=${resp.bodyAsString()})",
                        address
                    )
                }

                resp.body()
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