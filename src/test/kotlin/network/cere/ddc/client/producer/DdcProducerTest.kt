package network.cere.ddc.client.producer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals

internal class DdcProducerTest {

    private companion object {
        private const val API_PREFIX = "/api/rest"
        private const val DDC_NODE_URL = "http://localhost:8080"
        private const val TIER_ID = "2"
    }

    private var vertx: Vertx = Vertx.vertx().apply {
        DatabindCodec.mapper().registerModule(KotlinModule())
    }
    var client: WebClient = WebClient.create(vertx)

    @Test
    fun `DDC producer - produce (positive scenario)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val appPrivKey = Hex.encode(appKeyPair.privateKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val testSubject = DdcProducer(
            ProducerConfig(
                appPubKey = appPubKey,
                appPrivKey = appPrivKey,
                bootstrapNodes = listOf(DDC_NODE_URL)
            )
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")

        val piece1 = Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\"}")
        val piece2 = Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\"}")

        //when
        testSubject.send(piece1)
        testSubject.send(piece2)

        val expectedPieces = setOf(
            """{"id":"1","appPubKey":"$appPubKey","userPubKey":"user_1","timestamp":"2021-01-01T00:00:00Z","data":"{\"event_type\":\"first event\"}"}""",
            """{"id":"2","appPubKey":"$appPubKey","userPubKey":"user_2","timestamp":"2021-01-01T00:01:00Z","data":"{\"event_type\":\"second event\"}"}""",
        )

        //then
        val pieces = getPieces(appPubKey)

        //then
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())
    }

    @Test
    fun `DDC producer - produce while app scales (positive scenario)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val appPrivKey = Hex.encode(appKeyPair.privateKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val testSubject = DdcProducer(
            ProducerConfig(
                appPubKey = appPubKey,
                appPrivKey = appPrivKey,
                bootstrapNodes = listOf(DDC_NODE_URL)
            )
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece3Timestamp = Instant.parse("2021-01-01T00:02:00.000Z")
        val piece4Timestamp = Instant.parse("2021-01-01T00:03:00.000Z")
        val piece5Timestamp = Instant.parse("2021-01-01T00:04:00.000Z")
        val piece6Timestamp = Instant.parse("2021-01-01T00:05:00.000Z")

        val piece1 = Piece("1", appPubKey, "user_1", piece1Timestamp, "1".repeat(200))
        val piece2 = Piece("2", appPubKey, "user_2", piece2Timestamp, "2".repeat(200))
        val piece3 = Piece("3", appPubKey, "user_3", piece3Timestamp, "3".repeat(200))
        val piece4 = Piece("4", appPubKey, "user_4", piece4Timestamp, "4".repeat(200))
        val piece5 = Piece("5", appPubKey, "user_5", piece5Timestamp, "5".repeat(200))
        val piece6 = Piece("6", appPubKey, "user_6", piece6Timestamp, "6".repeat(200))

        //when
        testSubject.send(piece1)
        testSubject.send(piece2)
        testSubject.send(piece3)
        testSubject.send(piece4)
        testSubject.send(piece5)
        testSubject.send(piece6)

        val expectedPieces = mutableSetOf(
            """{"id":"1","appPubKey":"$appPubKey","userPubKey":"user_1","timestamp":"2021-01-01T00:00:00Z","data":"${"1".repeat(200)}"}""",
            """{"id":"2","appPubKey":"$appPubKey","userPubKey":"user_2","timestamp":"2021-01-01T00:01:00Z","data":"${"2".repeat(200)}"}""",
            """{"id":"3","appPubKey":"$appPubKey","userPubKey":"user_3","timestamp":"2021-01-01T00:02:00Z","data":"${"3".repeat(200)}"}""",
            """{"id":"4","appPubKey":"$appPubKey","userPubKey":"user_4","timestamp":"2021-01-01T00:03:00Z","data":"${"4".repeat(200)}"}""",
            """{"id":"5","appPubKey":"$appPubKey","userPubKey":"user_5","timestamp":"2021-01-01T00:04:00Z","data":"${"5".repeat(200)}"}""",
            """{"id":"6","appPubKey":"$appPubKey","userPubKey":"user_6","timestamp":"2021-01-01T00:05:00Z","data":"${"6".repeat(200)}"}""",
        )

        var pieces = getPieces(appPubKey)

        //then
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when next piece triggers partition scaling
        val piece7Timestamp = Instant.parse("2021-01-01T00:06:00.000Z")
        val piece7 = Piece("7", appPubKey, "user_7", piece7Timestamp, "7".repeat(200))
        testSubject.send(piece7)

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add("""{"id":"7","appPubKey":"$appPubKey","userPubKey":"user_7","timestamp":"2021-01-01T00:06:00Z","data":"${"7".repeat(200)}"}""")
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece8Timestamp = Instant.parse("2021-01-01T00:07:00.000Z")
        val piece8 = Piece("8", appPubKey, "user_8", piece8Timestamp, "8".repeat(200))
        testSubject.send(piece8)

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add("""{"id":"8","appPubKey":"$appPubKey","userPubKey":"user_8","timestamp":"2021-01-01T00:07:00Z","data":"${"8".repeat(200)}"}""")
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece9Timestamp = Instant.parse("2021-01-01T00:08:00.000Z")
        val piece9 = Piece("9", appPubKey, "user_9", piece9Timestamp, "9".repeat(200))
        testSubject.send(piece9)

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add("""{"id":"9","appPubKey":"$appPubKey","userPubKey":"user_9","timestamp":"2021-01-01T00:08:00Z","data":"${"9".repeat(200)}"}""")
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())
    }

    private fun createApp(appPubKey: String?, signer: Ed25519Sign) {
        val createAppReq = mapOf(
            "appPubKey" to appPubKey,
            "tierId" to TIER_ID,
            "signature" to Hex.encode(signer.sign("$appPubKey$TIER_ID".toByteArray()))
        ).let(::JsonObject)

        client.postAbs("$DDC_NODE_URL$API_PREFIX/apps")
            .sendJsonObject(createAppReq)
            .toCompletionStage()
            .toCompletableFuture()
            .get()
            .also { assertEquals(OK.code(), it.statusCode()) }
    }

    private fun getPieces(appPubKey: String): List<String> {
        val topology = client.getAbs("${DDC_NODE_URL}${API_PREFIX}/apps/$appPubKey/topology")
            .`as`(BodyCodec.json(AppTopology::class.java))
            .send()
            .toCompletionStage()
            .toCompletableFuture()
            .get()
            .body()

        val pieces = CopyOnWriteArrayList<String>()
        topology.partitions.forEach { partition ->
            client.getAbs("${partition.master.nodeHttpAddress}${API_PREFIX}/pieces?appPubKey=$appPubKey&partitionId=${partition.partitionId}")
                .`as`(BodyCodec.jsonStream(JsonParser.newParser().objectValueMode().handler { event ->
                    pieces.add(event.objectValue().encode())
                }))
                .send()
                .toCompletionStage()
                .toCompletableFuture()
                .get()
        }

        return pieces
    }
}
