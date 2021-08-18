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
import network.cere.ddc.client.producer.SendPieceResponse
import org.junit.jupiter.api.Test
import java.lang.Thread.sleep
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet
import java.util.zip.CRC32
import kotlin.test.assertEquals
import java.util.*


internal class DdcConsumerTest {

    private companion object {
        private const val API_PREFIX = "/api/rest"
        private const val DDC_NODE_URL = "http://localhost:8080"
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
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val data = testSubject.consume("test-stream", listOf("event_type", "location"))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        sleep(1000)
        testSubject.close()

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
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val data = testSubject.consume("test-stream", listOf("event_type", "location"))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        sleep(1000)

        //then
        assertEquals(expectedPieces, pieces)

        //when consumer is re-created after restart/failure
        testSubject.close()

        val piece3Timestamp = Instant.parse("2021-01-01T00:02:00.000Z")
        savePiece(appPubKey, signer, "user_3", "3", "{\"event_type\":\"third event\"}", piece3Timestamp)
        sleep(1000)

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
            testSubjectAfterFailure.consume("test-stream", listOf("event_type", "location"))

        val piecesAfterFailure = CopyOnWriteArraySet<Piece>()
        dataAfterFailure.subscribe().with { cr -> piecesAfterFailure.add(cr.piece) }
        sleep(1000)
        testSubjectAfterFailure.close()

        //then he continues to consume from checkpoint
        val expectedPiecesAfterFailure = setOf(
            Piece("3", appPubKey, "user_3", piece3Timestamp, "{\"event_type\":\"third event\"}", 3)
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
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val data = testSubject.consume("test-stream", listOf("event_type", "location"))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        sleep(1000)

        //then
        assertEquals(expectedPieces, pieces)

        //when consumer is re-created after restart/failure
        testSubject.close()

        savePiece(appPubKey, signer, "user_3", "3", "{\"event_type\":\"third event\"}", piece2Timestamp)
        sleep(1000)

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
            testSubjectAfterFailure.consume("test-stream", listOf("event_type", "location"))

        val piecesAfterFailure = CopyOnWriteArraySet<Piece>()
        dataAfterFailure.subscribe().with { cr -> piecesAfterFailure.add(cr.piece) }
        sleep(1000)
        testSubjectAfterFailure.close()

        //then he consume from the beginning (checkpoint wasn't committed)
        val expectedPiecesAfterFailure = setOf(
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece(
                "2",
                appPubKey,
                "user_2",
                piece2Timestamp,
                "{\"event_type\":\"second event\",\"location\":\"EU\"}",
                2
            ),
            Piece("3", appPubKey, "user_3", piece2Timestamp, "{\"event_type\":\"third event\"}", 3)
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
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val data = testSubject.consume("test-stream", listOf("event_type", "location"))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr ->
            pieces.add(cr.piece)
            testSubject.commitCheckpoint("test-stream", cr)
        }
        sleep(1000)

        //then
        assertEquals(expectedPieces, pieces)

        //when consumer is re-created after restart/failure
        testSubject.close()

        val piece3Timestamp = Instant.parse("2021-01-01T00:02:00.000Z")
        savePiece(appPubKey, signer, "user_3", "3", "{\"event_type\":\"third event\"}", piece3Timestamp)
        sleep(1000)

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
            testSubjectAfterFailure.consume("test-stream", listOf("event_type", "location"))

        val piecesAfterFailure = CopyOnWriteArraySet<Piece>()
        dataAfterFailure.subscribe().with { cr -> piecesAfterFailure.add(cr.piece) }
        sleep(1000)
        testSubjectAfterFailure.close()

        //then he continues to consume from checkpoint (checkpoint was manually committed)
        val expectedPiecesAfterFailure = setOf(
            Piece("3", appPubKey, "user_3", piece3Timestamp, "{\"event_type\":\"third event\"}", 3)
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
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val data = testSubject.consume("test-stream", listOf("event_type", "location"))

        val pieces = CopyOnWriteArraySet<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        sleep(1000)
        testSubject.close()

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
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val data = testSubject.consume("test-stream", listOf("event_type", "location"))

        val pieces = CopyOnWriteArrayList<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        sleep(1000)

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
        sleep(1000L)

        //then
        expectedPieces.add(
            Piece(
                "3",
                appPubKey,
                "user_1",
                piece3Timestamp,
                "{\"event_type\":\"third event\",\"location\":\"Canada\"}",
                3
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
        sleep(1000)
        testSubject.close()

        //then
        expectedPieces.add(
            Piece(
                "4",
                appPubKey,
                "user_3",
                piece4Timestamp,
                "{\"event_type\":\"forth event\",\"location\":\"Japan\"}",
                4
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
        val piece1Data = "1".repeat(3000000)
        val piece2Data = "2".repeat(3000000)
        val piece3Data = "3".repeat(3000000)
        savePiece(appPubKey, signer, "user_1", "1", piece1Data, piece1Timestamp)
        savePiece(appPubKey, signer, "user_2", "2", piece2Data, piece2Timestamp)
        savePiece(appPubKey, signer, "user_3", "3", piece3Data, piece3Timestamp)

        val expectedPieces = mutableSetOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, piece1Data, 1),
            Piece("2", appPubKey, "user_2", piece2Timestamp, piece2Data, 2),
            Piece("3", appPubKey, "user_3", piece3Timestamp, piece3Data, 3),
        )

        //when
        val data = testSubject.consume("test-stream")

        val pieces = CopyOnWriteArrayList<Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        sleep(1000)

        //then
        assertEquals(expectedPieces, pieces.toSet())

        //when next piece triggers partition scaling
        val piece4Timestamp = Instant.now()
        val piece4Data = "4".repeat(3000000)
        savePiece(appPubKey, signer, "user_4", "4", piece4Data, piece4Timestamp)
        sleep(1000)

        //then
        expectedPieces.add(Piece("4", appPubKey, "user_4", piece4Timestamp, piece4Data, 1))
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece5Timestamp = Instant.now()
        val piece5Data = "5".repeat(3000000)
        savePiece(appPubKey, signer, "user_5", "5", piece5Data, piece5Timestamp)
        sleep(1000)

        //then
        expectedPieces.add(Piece("5", appPubKey, "user_5", piece5Timestamp, piece5Data, 2))
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece6Timestamp = Instant.now()
        val piece6Data = "6".repeat(3000000)
        savePiece(appPubKey, signer, "user_6", "6", piece6Data, piece6Timestamp)
        sleep(1000)

        //then
        expectedPieces.add(Piece("6", appPubKey, "user_6", piece6Timestamp, piece6Data, 3))
        assertEquals(expectedPieces, pieces.toSet())

        //when next piece triggers partition scaling
        val piece7Timestamp = Instant.now()
        val piece7Data = "7".repeat(3000000)
        savePiece(appPubKey, signer, "user_7", "7", piece7Data, piece7Timestamp)
        sleep(1000)
        testSubject.close()

        //then
        expectedPieces.add(Piece("7", appPubKey, "user_7", piece7Timestamp, piece7Data, 1))
        assertEquals(expectedPieces, pieces.toSet())
    }

    @Test
    fun `DDC consumer - get app pieces (different users)`() {
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

        val expectedPieces = listOf(
            Piece(
                "1",
                appPubKey,
                "user_1",
                piece1Timestamp,
                "{\"event_type\":\"first event\",\"location\":\"USA\"}",
                1
            ),
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val pieces = testSubject.getAppPieces(fields = listOf("event_type", "location"))

        //then
        assertEquals(expectedPieces, pieces.collect().asList().await().indefinitely())
    }

    @Test
    fun `DDC consumer - get app pieces (different users and different partitions)`() {
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
        val piece3Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece4Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece5Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece1Data = "1".repeat(3000000)
        val piece2Data = "2".repeat(3000000)
        val piece3Data = "3".repeat(3000000)
        val piece4Data = "4".repeat(3000000)
        val piece5Data = "5".repeat(3000000)
        savePiece(appPubKey, signer, "user_1", "1", piece1Data, piece1Timestamp)
        savePiece(appPubKey, signer, "user_2", "2", piece2Data, piece2Timestamp)
        savePiece(appPubKey, signer, "user_1", "3", piece3Data, piece3Timestamp)
        savePiece(appPubKey, signer, "user_2", "4", piece4Data, piece4Timestamp)
        savePiece(appPubKey, signer, "user_1", "5", piece5Data, piece5Timestamp)

        val expectedPieces = setOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, piece1Data, 1),
            Piece("2", appPubKey, "user_2", piece2Timestamp, piece2Data, 2),
            Piece("3", appPubKey, "user_1", piece3Timestamp, piece3Data, 3),
            Piece("4", appPubKey, "user_2", piece4Timestamp, piece4Data, 1),
            Piece("5", appPubKey, "user_1", piece5Timestamp, piece5Data, 2)
        )

        //when
        val pieces = testSubject.getAppPieces()

        //then
        assertEquals(expectedPieces, pieces.collect().asList().await().indefinitely().toSet())
    }

    @Test
    fun `DDC consumer - get app pieces (time filtered)`() {
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
        val piece3Timestamp = Instant.parse("2021-01-01T00:02:00.000Z")
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
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "3",
            "{\"event_type\":\"third event\",\"location\":\"CH\"}",
            piece3Timestamp
        )

        val expectedPieces = setOf(
            Piece(
                "2",
                appPubKey,
                "user_2",
                piece2Timestamp,
                "{\"event_type\":\"second event\",\"location\":\"EU\"}",
                2
            ),
            Piece("3", appPubKey, "user_1", piece3Timestamp, "{\"event_type\":\"third event\",\"location\":\"CH\"}", 3)
        )

        //when
        val pieces = testSubject.getAppPieces(
            from = "2021-01-01T00:01:00.000Z",
            to = Instant.now().toString(),
            fields = listOf("event_type", "location")
        )

        //then
        assertEquals(expectedPieces, pieces.collect().asList().await().indefinitely().toSet())
    }

    @Test
    fun `DDC consumer - get user pieces (different users)`() {
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

        val expectedPiecesUser1 = listOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, "{\"event_type\":\"first event\",\"location\":\"USA\"}", 1)
        )

        val expectedPiecesUser2 = listOf(
            Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\",\"location\":\"EU\"}", 2)
        )

        //when
        val piecesUser1 = testSubject.getUserPieces(userPubKey = "user_1", fields = listOf("event_type", "location"))
        val piecesUser2 = testSubject.getUserPieces(userPubKey = "user_2", fields = listOf("event_type", "location"))

        //then
        assertEquals(expectedPiecesUser1, piecesUser1.collect().asList().await().indefinitely())
        assertEquals(expectedPiecesUser2, piecesUser2.collect().asList().await().indefinitely())
    }

    @Test
    fun `DDC consumer - get user pieces (different users and different partitions)`() {
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
        val piece3Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece4Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece5Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece1Data = "1".repeat(3000000)
        val piece2Data = "2".repeat(3000000)
        val piece3Data = "3".repeat(3000000)
        val piece4Data = "4".repeat(3000000)
        val piece5Data = "5".repeat(3000000)
        savePiece(appPubKey, signer, "user_1", "1", piece1Data, piece1Timestamp)
        savePiece(appPubKey, signer, "user_2", "2", piece2Data, piece2Timestamp)
        savePiece(appPubKey, signer, "user_1", "3", piece3Data, piece3Timestamp)
        savePiece(appPubKey, signer, "user_2", "4", piece4Data, piece4Timestamp)
        savePiece(appPubKey, signer, "user_1", "5", piece5Data, piece5Timestamp)

        val expectedPiecesUser1 = listOf(
            Piece("1", appPubKey, "user_1", piece1Timestamp, piece1Data, 1),
            Piece("3", appPubKey, "user_1", piece3Timestamp, piece3Data, 3),
            Piece("5", appPubKey, "user_1", piece5Timestamp, piece5Data, 2)
        )

        val expectedPiecesUser2 = listOf(
            Piece("2", appPubKey, "user_2", piece2Timestamp, piece2Data, 2),
            Piece("4", appPubKey, "user_2", piece4Timestamp, piece4Data, 1)
        )

        //when
        val piecesUser1 = testSubject.getUserPieces("user_1")
        val piecesUser2 = testSubject.getUserPieces("user_2")

        //then
        assertEquals(expectedPiecesUser1, piecesUser1.collect().asList().await().indefinitely())
        assertEquals(expectedPiecesUser2, piecesUser2.collect().asList().await().indefinitely())
    }

    @Test
    fun `DDC consumer - get user pieces (time filtered)`() {
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
        val piece3Timestamp = Instant.parse("2021-01-01T00:02:00.000Z")
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
            "user_1",
            "2",
            "{\"event_type\":\"second event\",\"location\":\"EU\",\"count\":2}",
            piece2Timestamp
        )
        savePiece(
            appPubKey,
            signer,
            "user_1",
            "3",
            "{\"event_type\":\"third event\",\"location\":\"CH\"}",
            piece3Timestamp
        )

        val expectedPieces = listOf(
            Piece(
                "2",
                appPubKey,
                "user_1",
                piece2Timestamp,
                "{\"event_type\":\"second event\",\"location\":\"EU\"}",
                2
            ),
            Piece("3", appPubKey, "user_1", piece3Timestamp, "{\"event_type\":\"third event\",\"location\":\"CH\"}", 3)
        )

        //when
        val pieces = testSubject.getUserPieces(
            userPubKey = "user_1",
            from = "2021-01-01T00:01:00.000Z",
            to = Instant.now().toString(),
            fields = listOf("event_type", "location")
        )

        //then
        assertEquals(expectedPieces, pieces.collect().asList().await().indefinitely())
    }

    @Test
    fun `DDC consumer - get piece (different users and different partitions)`() {
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
        val piece3Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece4Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece5Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        savePiece(appPubKey, signer, "user_1", "1", "1".repeat(300), piece1Timestamp)
        val piece2Res = savePiece(appPubKey, signer, "user_2", "2", "2".repeat(300), piece2Timestamp)
        savePiece(appPubKey, signer, "user_1", "3", "3".repeat(300), piece3Timestamp)
        savePiece(appPubKey, signer, "user_2", "4", "4".repeat(300), piece4Timestamp)
        val piece5Res = savePiece(appPubKey, signer, "user_1", "5", "5".repeat(300), piece5Timestamp)

        val expectedPiece2 = Piece("2", appPubKey, "user_2", piece2Timestamp, "2".repeat(300), 0)
        val expectedPiece5 = Piece("5", appPubKey, "user_1", piece5Timestamp, "5".repeat(300), 0)

        //when
        val piece2 = testSubject.getPiece(userPubKey = "user_2", cid = piece2Res.cid!!)
        val piece5 = testSubject.getPiece(userPubKey = "user_1", cid = piece5Res.cid!!)

        //then
        assertEquals(expectedPiece2, piece2.await().indefinitely())
        assertEquals(expectedPiece5, piece5.await().indefinitely())
    }

    @Test
    fun `DDC consumer - get piece data`() {
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

        val user = "user_1"
        val pieceTimestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val data = String(ByteArray(6000000).apply { Random().nextBytes(this) })
        val sendPieceResponse = savePiece(appPubKey, signer, user, "1", data, pieceTimestamp)

        //when
        val pieceDataStream = testSubject.getPieceData(userPubKey = user, cid = sendPieceResponse.cid!!)

        //then
        var pieceDataAsBytes = byteArrayOf()
        pieceDataStream.subscribe().with { buffer -> pieceDataAsBytes += buffer.bytes }
        sleep(3000)

        val pieceData = String(pieceDataAsBytes)
        assertEquals(data.length, pieceData.length)
        assertEquals(data, pieceData)
    }

    private fun createApp(appPubKey: String?, signer: Ed25519Sign) {
        val createAppReq = mapOf(
            "appPubKey" to appPubKey,
            "signature" to Hex.encode(signer.sign("$appPubKey".toByteArray()))
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
    ): SendPieceResponse {
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
        return topology.partitions!!
            .filter { it.active }
            .first { it.sectorStart!! <= ringToken && ringToken <= it.sectorEnd!! }
            .master!!
            .nodeHttpAddress
            .let { "$it$API_PREFIX/pieces" }
            .let(client::postAbs)
            .sendJsonObject(piece)
            .toCompletionStage()
            .toCompletableFuture()
            .get()
            .bodyAsJson(SendPieceResponse::class.java)
    }
}
