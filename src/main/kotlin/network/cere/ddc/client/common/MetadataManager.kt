package network.cere.ddc.client.common

import io.netty.handler.codec.http.HttpResponseStatus
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import java.util.zip.CRC32

class MetadataManager(
    private var bootstrapNodes: List<String>,
    private val client: WebClient
) {

    init {
        bootstrapNodes = bootstrapNodes.map { it.removeSuffix("/") }
    }

    private val log = LoggerFactory.getLogger(javaClass)

    fun getAppTopology(appPubKey: String): AppTopology {
        var retryAttempt = 0
        var getAppTopology = Uni.createFrom().deferred {
            client.getAbs("${bootstrapNodes[retryAttempt++]}/api/rest/apps/${appPubKey}/topology")
                .`as`(BodyCodec.json(AppTopology::class.java))
                .send()
        }.onItem().transform { res ->
            if (res.statusCode() != HttpResponseStatus.OK.code()) {
                log.error("Can't load app topology (statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                throw RuntimeException("Can't load app topology")
            }

            res.body()
        }

        if (bootstrapNodes.size.toLong() > 1) {
            getAppTopology = getAppTopology.onFailure().retry().atMost(bootstrapNodes.size.toLong() - 1)
        }

        return getAppTopology.runSubscriptionOn { Thread(it).start() }.await().indefinitely()
    }

    fun getTargetNode(userPubKey: String, appTopology: AppTopology): String? {
        val ringToken = CRC32().apply { update(userPubKey.toByteArray()) }.value
        return appTopology.partitions!!.reversed().first { it.ringToken!! <= ringToken }.master!!.nodeHttpAddress
    }
}