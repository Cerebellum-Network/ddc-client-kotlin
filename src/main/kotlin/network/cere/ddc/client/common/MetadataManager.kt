package network.cere.ddc.client.common

import io.netty.handler.codec.http.HttpResponseStatus
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import org.slf4j.LoggerFactory
import java.lang.RuntimeException

class MetadataManager(
    private val bootstrapNodes: List<String>,
    private val client: WebClient
) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun getAppTopology(appPubKey: String): AppTopology {
        var retryAttempt = 0
        return Uni.createFrom().deferred {
            client.getAbs("${bootstrapNodes[retryAttempt++]}/api/rest/apps/${appPubKey}/topology")
                .`as`(BodyCodec.json(AppTopology::class.java))
                .send()
        }.onItem().transform { res ->
            if (res.statusCode() != HttpResponseStatus.OK.code()) {
                log.error("Can't load app topology (statusCode=${res.statusCode()}, body=${res.bodyAsString()})")
                throw RuntimeException("Can't load app topology")
            }

            res.body()
        }.onFailure().retry().atMost(bootstrapNodes.size.toLong())
            .runSubscriptionOn { Thread(it).start() }
            .await().indefinitely()
    }
}