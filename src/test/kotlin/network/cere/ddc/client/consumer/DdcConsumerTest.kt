package network.cere.ddc.client.consumer

import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.consumer.checkpointer.InMemoryCheckpointer
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet
import java.util.zip.CRC32
import kotlin.test.assertEquals

internal class DdcConsumerTest {

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
    fun `DDC consumer - consume existing data (positive scenario)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val testSubject = DdcConsumer(
            ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500
            )
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "1",
            "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}",
            piece1Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_2",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)
    }

    @Test
    fun `DDC consumer - consumer auto commit enabled (consumer failure)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val checkpointer = InMemoryCheckpointer()

        val testSubject = DdcConsumer(
            config = ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500,
                autoCommitIntervalMs = 100
            ),
            checkpointer = checkpointer
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "1",
            "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}",
            piece1Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_2",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)

        //when consumer is re-created after restart/failure
        testSubject.close()

        savePiece(appPubKey, signer, "user_3", "3", "{\"event_type\":\"third event\"}", piece2Timestamp)
        Thread.sleep(1000)

        val testSubjectAfterFailure = DdcConsumer(
            config = ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500,
                autoCommitIntervalMs = 100
            ),
            checkpointer = checkpointer
        )

        val dataAfterFailure =
            testSubjectAfterFailure.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val piecesAfterFailure = CopyOnWriteArraySet<Piece>()
        dataAfterFailure.subscribe().with { cr -> piecesAfterFailure.add(cr.piece) }
        Thread.sleep(1000L)

        //then he continues to consume from checkpoint
        val expectedPiecesAfterFailure = setOf(
            Piece("3", appPubKey, "user_3", piece2Timestamp, "{\"event_type\":\"third event\"}")
        )
        assertEquals(expectedPiecesAfterFailure, piecesAfterFailure)
    }

    @Test
    fun `DDC consumer - consumer auto commit disabled (consumer failure)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val checkpointer = InMemoryCheckpointer()

        val testSubject = DdcConsumer(
            config = ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500,
                enableAutoCommit = false
            ),
            checkpointer = checkpointer
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "1",
            "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}",
            piece1Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_2",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)

        //when consumer is re-created after restart/failure
        testSubject.close()

        savePiece(appPubKey, signer, "user_3", "3", "{\"event_type\":\"third event\"}", piece2Timestamp)
        Thread.sleep(1000)

        val testSubjectAfterFailure = DdcConsumer(
            config = ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500,
                enableAutoCommit = false
            ),
            checkpointer = checkpointer
        )

        val dataAfterFailure =
            testSubjectAfterFailure.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val piecesAfterFailure = CopyOnWriteArraySet<Piece>()
        dataAfterFailure.subscribe().with { cr -> piecesAfterFailure.add(cr.piece) }
        Thread.sleep(1000L)

        //then he consume from the beginning (checkpoint wasn't committed)
        val expectedPiecesAfterFailure = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}"),
            Piece("3", appPubKey, "user_3", piece2Timestamp, "{\"event_type\":\"third event\"}")
        )
        assertEquals(expectedPiecesAfterFailure, piecesAfterFailure)
    }

    @Test
    fun `DDC consumer - consumer auto commit disabled (use resolveCheckpoint before consumer failure)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val checkpointer = InMemoryCheckpointer()

        val testSubject = DdcConsumer(
            config = ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500,
                enableAutoCommit = false
            ),
            checkpointer = checkpointer
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "1",
            "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}",
            piece1Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_2",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr ->
            pieces.add(cr.piece)
            testSubject.commitCheckpoint("test-stream", cr)
        }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)

        //when consumer is re-created after restart/failure
        testSubject.close()

        savePiece(appPubKey, signer, "user_3", "3", "{\"event_type\":\"third event\"}", piece2Timestamp)
        Thread.sleep(1000)

        val testSubjectAfterFailure = DdcConsumer(
            config = ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500,
                autoCommitIntervalMs = 100,
                enableAutoCommit = false
            ),
            checkpointer = checkpointer
        )

        val dataAfterFailure =
            testSubjectAfterFailure.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val piecesAfterFailure = CopyOnWriteArraySet<Piece>()
        dataAfterFailure.subscribe().with { cr -> piecesAfterFailure.add(cr.piece) }
        Thread.sleep(1000L)

        //then he continues to consume from checkpoint (checkpoint was manually committed)
        val expectedPiecesAfterFailure = setOf(
            Piece("3", appPubKey, "user_3", piece2Timestamp, "{\"event_type\":\"third event\"}")
        )
        assertEquals(expectedPiecesAfterFailure, piecesAfterFailure)
    }

    @Test
    fun `DDC consumer - works as expected when one of bootstrap nodes is unavailable`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val testSubject = DdcConsumer(
            ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf("https://ddc.unavailable-node.network", DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500
            )
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "1",
            "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}",
            piece1Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_2",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces, pieces)
    }

    @Test
    fun `DDC consumer - streaming ongoing data (positive scenario)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val testSubject = DdcConsumer(
            ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500
            )
        )

        val piece1Timestamp = Instant.now()
        val piece2Timestamp = Instant.now()
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "1",
            "{\"event_type\":\"first event\",\"location\":\"USA\",\"success\":\"false\"}",
            piece1Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_2",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )

        val expectedPieces = mutableSetOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}"),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}")
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", listOf("event_type", "location")))

        val pieces = CopyOnWriteArrayList<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece3Timestamp = Instant.now()
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "3",
            "{\"event_type\":\"third event\",\"location\":\"Canada\",\"success\":\"true\"}",
            piece3Timestamp
        )
        Thread.sleep(1000L)

        //then
        expectedPieces.add(
            Piece(
                "3",
                appPubKey,
                "user_1",
                piece3Timestamp,
                "{\"event_type\":\"third event\",\"location\":\"Canada\"}"
            )
        )
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece4Timestamp = Instant.now()
        savePiece(
            appPubKey,
            signer,
            "user_3",
            "4",
            "{\"event_type\":\"forth event\",\"location\":\"Japan\"}",
            piece4Timestamp
        )
        Thread.sleep(1000L)

        //then
        expectedPieces.add(
            Piece(
                "4",
                appPubKey,
                "user_3",
                piece4Timestamp,
                "{\"event_type\":\"forth event\",\"location\":\"Japan\"}"
            )
        )
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())
    }

    @Test
    fun `DDC consumer - streaming ongoing data while app scales (positive scenario)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val testSubject = DdcConsumer(
            ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500
            )
        )

        val piece1Timestamp = Instant.now()
        val piece2Timestamp = Instant.now()
        val piece3Timestamp = Instant.now()
        val piece4Timestamp = Instant.now()
        val piece5Timestamp = Instant.now()
        savePiece(appPubKey, signer, "user_1", "1", "1".repeat(200), piece1Timestamp)
        savePiece(appPubKey, signer, "user_2", "2", "2".repeat(200), piece2Timestamp)
        savePiece(appPubKey, signer, "user_3", "3", "3".repeat(200), piece3Timestamp)
        savePiece(appPubKey, signer, "user_4", "4", "4".repeat(200), piece4Timestamp)
        savePiece(appPubKey, signer, "user_5", "5", "5".repeat(200), piece5Timestamp)

        val expectedPieces = mutableSetOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "1".repeat(200)),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "2".repeat(200)),
            Piece("3", appPubKey, "user_3", piece3Timestamp, "3".repeat(200)),
            Piece("4", appPubKey, "user_4", piece4Timestamp, "4".repeat(200)),
            Piece("5", appPubKey, "user_5", piece5Timestamp, "5".repeat(200))
        )

        //when
        val data = testSubject.consume("test-stream", DataQuery("", "", emptyList()))

        val pieces = CopyOnWriteArrayList<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //then
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece6Timestamp = Instant.now()
        savePiece(appPubKey, signer, "user_6", "6", "6".repeat(200), piece6Timestamp)
        Thread.sleep(1000L)

        //then
        expectedPieces.add(Piece("6", appPubKey, "user_6", piece6Timestamp, "6".repeat(200)))
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when next piece triggers partition scaling
        savePiece(appPubKey, signer, "user_7", "7", "7".repeat(200), Instant.now())
        Thread.sleep(1000L)
        val piece7Timestamp = Instant.now()
        savePiece(appPubKey, signer, "user_7", "7", "7".repeat(200), piece7Timestamp)
        Thread.sleep(1000L)

        //then
        expectedPieces.add(Piece("7", appPubKey, "user_7", piece7Timestamp, "7".repeat(200)))
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece8Timestamp = Instant.now()
        savePiece(appPubKey, signer, "user_8", "8", "8".repeat(200), piece8Timestamp)
        Thread.sleep(1000L)

        //then
        expectedPieces.add(Piece("8", appPubKey, "user_8", piece8Timestamp, "8".repeat(200)))
        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece9Timestamp = Instant.now()
        savePiece(appPubKey, signer, "user_9", "9", "9".repeat(200), piece9Timestamp)
        Thread.sleep(1000L)

        //then
        expectedPieces.add(Piece("9", appPubKey, "user_9", piece9Timestamp, "9".repeat(200)))
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

    private fun savePiece(
        appPubKey: String,
        signer: Ed25519Sign,
        userPubKey: String,
        id: String,
        data: String,
        timestamp: Instant
    ) {
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
            .`as`(BodyCodec.json(AppTopology::class.java))
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
    }
}
