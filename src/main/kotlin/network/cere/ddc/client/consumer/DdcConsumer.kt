package network.cere.ddc.client.consumer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.smallrye.mutiny.Multi
import io.vertx.ext.web.client.WebClient
import io.vertx.core.Vertx
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.codec.BodyCodec
import org.slf4j.LoggerFactory
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor
import network.cere.ddc.client.api.ApplicationTopology

class DdcConsumer(
    private val ddcNodeUrl: String,
    private val appPubKey: String,
) : Consumer {

    private companion object {
        private const val API_PREFIX = "/api/rest"
    }

    private val log = LoggerFactory.getLogger(javaClass)

    private val vertx: Vertx = Vertx.vertx().apply {
        DatabindCodec.mapper().registerModule(KotlinModule())
    }

    private val client: WebClient = WebClient.create(vertx)

    override fun consume(streamId: String, dataQuery: DataQuery): Multi<Piece> {
        val stream = UnicastProcessor.create<Piece>()

        client.getAbs("$ddcNodeUrl$API_PREFIX/apps/${appPubKey}/topology")
            .`as`(BodyCodec.json(ApplicationTopology::class.java))
            .send()
            .onSuccess {
                it.body().partitions.stream().parallel().forEach { partition ->
                    val node = partition.master.nodeHttpAddress
                    val parser = JsonParser.newParser().objectValueMode().handler { event ->
                        stream.onNext(event.mapTo(Piece::class.java))
                    }

                    var url = "$node$API_PREFIX/pieces?appPubKey=${appPubKey}&partitionId=${partition.partitionId}"
                    if (dataQuery.fields.isNotEmpty()) {
                        url += "&fields=" + dataQuery.fields.joinToString(",")
                    }

                    client.getAbs(url)
                        .`as`(BodyCodec.jsonStream(parser))
                        .send()
                        .onFailure {
                            log.error("Error on streaming data from DDC from URL $url", it)
                        }
                }
            }
            .onFailure {
                log.error("Unable to get application topology for app $appPubKey", it)
                stream.onError(it)
            }
        return stream
    }
}
