package network.cere.ddc.client.producer

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.mutiny.core.Vertx
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 2)
@Measurement(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
open class ProducerBenchmark {

    companion object {
        private const val APPROXIMATE_PIECE_METADATA_OVERHEAD = 500 // metadata + indexes
        private const val USERS = 100 // number users

        // Piece size
        private const val sizeMin = 4 shl 10 // 8 KB
        private const val sizeMax = 400 shl 10 // 800 KB

        // DDC Node post delay ms
        private const val delayMin = 300
        private const val delayMax = 400
    }

    private var server: WireMockServer? = null
    private var config: ProducerConfig? = null
    private var producer: DdcProducer? = null
    private var vertx: Vertx? = null

    @Setup
    fun setup() {
        val appPubKey = "847e95612c054758922084b6b3c998fe0a8ff8108baf2dae289c01d1e52e39c8"

        vertx = Vertx.vertx()

        server =
            WireMockServer(options().dynamicPort().asynchronousResponseEnabled(true).disableRequestJournal()).apply {
                start()

                stubFor(post(urlEqualTo("/api/rest/pieces"))
                    .willReturn(aResponse()
                        .withRandomDelay { Random.nextInt(delayMin, delayMax).toLong() }
                        .withStatus(HttpResponseStatus.CREATED.code())))

                stubFor(
                    get(urlEqualTo("/api/rest/apps/${appPubKey}/topology"))
                        .willReturn(
                            aResponse().withStatus(HttpResponseStatus.OK.code())
                                .withBody(
                                    """{
                                    "appPubKey": "$appPubKey",
                                    "partitions": [ {
                                        "partitionId":"677422aa-cfcc-42f8-9ef0-6e9dea83322c",
                                        "sectorStart":-9223372036854775808,
                                        "sectorEnd":9223372036854775807,
                                        "master":{
                                            "nodeId":"12D3KooWBTgBvQHbqRhMyERYr39h6YPK7iZHnVv12oJJhZLjuwBx",
                                            "nodeHttpAddress":"${baseUrl()}"
                                        },
                                        "replicas":[],
                                        "active":true,
                                        "createdAt":"2021-11-02T09:36:23Z",
                                        "updatedAt":"2021-11-02T09:36:23Z"
                                        }
                                    ]
                                      }"""
                                )
                        )
                )
            }

        config = ProducerConfig(
            appPubKey,
            "1149446a66b44518abdc91edc0923b20090629661246c1bbd1bfff315603abb2",
            listOf(server!!.baseUrl()),
            nodeConnectionPoolSize = USERS
        )

        producer = DdcProducer(config!!, vertx!!)
    }

    @TearDown
    fun tearDown() {
        vertx?.closeAndAwait()
        server?.stop()
    }

    @Threads(USERS)
    @Benchmark
    fun producePieces(parameters: ProducerBenchmarkParameters, blackhole: Blackhole) {
        val pieceSize = Random.nextInt(sizeMin, sizeMax)
        val dataSize = pieceSize - APPROXIMATE_PIECE_METADATA_OVERHEAD
        val data = "0".repeat(dataSize)

        producer!!.send(
            Piece(
                id = UUID.randomUUID().toString(),
                appPubKey = config!!.appPubKey,
                userPubKey = parameters.userPubKey,
                timestamp = Instant.now(),
                data = data
            )
        ).await().indefinitely().also {
            blackhole.consume(it)
        }
    }

    @State(Scope.Thread)
    open class ProducerBenchmarkParameters {
        val userPubKey = UUID.randomUUID().toString()
    }

}