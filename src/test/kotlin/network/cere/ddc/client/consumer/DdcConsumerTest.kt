package network.cere.ddc.client.consumer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.netty.handler.codec.http.HttpResponseStatus.CREATED
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.ApplicationTopology
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CopyOnWriteArraySet
import java.util.zip.CRC32
import kotlin.test.assertEquals

internal class DdcConsumerTest {

    private companion object {
        private const val API_PREFIX = "/api/rest"
        private const val DDC_NODE_URL = "http://localhost:8080"
        private const val TIER_ID = "2"
    }

    private val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
    private val appPubKey = Hex.encode(appKeyPair.publicKey)
    private val signer = Ed25519Sign(appKeyPair.privateKey)


    private var vertx: Vertx = Vertx.vertx().apply {
        DatabindCodec.mapper().registerModule(KotlinModule())
    }
    var client: WebClient = WebClient.create(vertx)

    var testSubject = DdcConsumer(DDC_NODE_URL, appPubKey)

    @Test
    fun `DDC consumer - consume existing data (positive scenario)`() {
        //given
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

        savePiece("user_1", "1", "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}", "2021-01-01T00:00:00.000Z")
        savePiece("user_2", "2", "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}", "2021-01-01T00:01:00.000Z")

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", Instant.parse("2021-01-01T00:00:00.000Z"), "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", Instant.parse("2021-01-01T00:01:00.000Z"), "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with{ piece -> pieces.add(piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)
    }

    @Test
    fun `DDC consumer - streaming ongoing data (positive scenario)`() {
        //given
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

        savePiece("user_1", "1", "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}", "2021-01-01T00:00:00.000Z")
        savePiece("user_2", "2", "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}", "2021-01-01T00:01:00.000Z")

        val expectedPieces = mutableSetOf(
            Piece("1", appPubKey, "user_1", Instant.parse("2021-01-01T00:00:00.000Z"), "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", Instant.parse("2021-01-01T00:01:00.000Z"), "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with{ piece -> pieces.add(piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)

        //when
        savePiece("user_1", "3", "{\"event_type\":\"third event\",\"location\":\"Canada\",\"success\":\"true\"}", "2021-01-01T00:02:00.000Z")
        Thread.sleep(1000L)

        //then
        expectedPieces.add(Piece("3", appPubKey, "user_1", Instant.parse("2021-01-01T00:02:00.000Z"), "{\"event_type\":\"third event\",\"location\":\"Canada\"}"))
        assertEquals(expectedPieces, pieces)

        //when
        savePiece("user_3", "4", "{\"event_type\":\"forth event\",\"location\":\"Japan\"}", "2021-01-01T00:03:00.000Z")
        Thread.sleep(1000L)

        //then
        expectedPieces.add(Piece("4", appPubKey, "user_3", Instant.parse("2021-01-01T00:03:00.000Z"), "{\"event_type\":\"forth event\",\"location\":\"Japan\"}"))
        assertEquals(expectedPieces, pieces)
    }

    private fun savePiece(userPubKey: String, id: String, data: String, timestamp: String) {

        val toSign = "$id$timestamp$appPubKey$userPubKey$data"
        val signature = Hex.encode(signer.sign(toSign.toByteArray()))
        val piece = JsonObject(
            mapOf(
                "id" to id,
                "appPubKey" to appPubKey,
                "userPubKey" to userPubKey,
                "timestamp" to timestamp,
                "data" to data,
                "signature" to signature
            )
        )
        val crc = CRC32()
        crc.update(userPubKey.toByteArray())
        val ringToken = crc.value
        val topology = client.getAbs("$DDC_NODE_URL$API_PREFIX/apps/$appPubKey/topology")
            .`as`(BodyCodec.json(ApplicationTopology::class.java))
            .send()
            .toCompletionStage()
            .toCompletableFuture()
            .get()
            .body()
        topology.partitions
            .asSequence()
            .sortedByDescending { it.ringToken }
            .first { it.ringToken <= ringToken }
            .master
            .nodeHttpAddress
            .let { "$it$API_PREFIX/pieces" }
            .let(client::postAbs)
            .sendJsonObject(piece)
            .toCompletionStage()
            .toCompletableFuture()
            .get()
            .also { assertEquals(CREATED.code(), it.statusCode()) }
    }
}
