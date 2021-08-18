package network.cere.ddc.client

import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClient
import network.cere.ddc.client.consumer.ConsumerConfig
import network.cere.ddc.client.consumer.DdcConsumer
import network.cere.ddc.client.producer.DdcProducer
import network.cere.ddc.client.producer.Piece
import network.cere.ddc.client.producer.ProducerConfig
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.test.assertEquals

internal class CommonTest {

    private companion object {
        private const val API_PREFIX = "/api/rest"
        private const val DDC_NODE_URL = "http://localhost:8080"
    }

    private var vertx: Vertx = Vertx.vertx().apply {
        DatabindCodec.mapper().registerModule(KotlinModule())
    }
    var client: WebClient = WebClient.create(vertx)

    @Test
    fun `DDC producer and consumer - fill completely 3 nodes and test allocation of app partitions on other nodes (storage scaling)`() {
        //given
        val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
        val appPubKey = Hex.encode(appKeyPair.publicKey)
        val appPrivKey = Hex.encode(appKeyPair.privateKey)
        val signer = Ed25519Sign(appKeyPair.privateKey)

        createApp(appPubKey, signer)

        val ddcProducer = DdcProducer(
            ProducerConfig(
                appPubKey = appPubKey,
                appPrivKey = appPrivKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                retryBackoff = Duration.ofMillis(500)
            )
        )

        val ddcConsumer = DdcConsumer(
            ConsumerConfig(
                appPubKey = appPubKey,
                bootstrapNodes = listOf(DDC_NODE_URL),
                partitionPollIntervalMs = 500,
                updateAppTopologyIntervalMs = 500
            )
        )

        val data = ddcConsumer.consume("test-stream")

        val pieces = CopyOnWriteArraySet<network.cere.ddc.client.consumer.Piece>()
        data.subscribe().with { cr -> pieces.add(cr.piece) }
        Thread.sleep(1000L)

        //when
        val users = 4
        val piecesPerUser = 7
        val expectedPieces = CopyOnWriteArrayList<network.cere.ddc.client.consumer.Piece>()
        val pieceResponses = ConcurrentHashMap<String, network.cere.ddc.client.consumer.Piece>()

        val userThreads = mutableListOf<Thread>()
        repeat(users) { userId ->
                repeat(piecesPerUser) { pieceId ->
                    val piece = Piece("$userId-$pieceId", appPubKey, "$userId", Instant.now(), "1".repeat(6000000))
                    val expectedPiece = network.cere.ddc.client.consumer.Piece(
                        piece.id,
                        piece.appPubKey,
                        piece.userPubKey,
                        piece.timestamp,
                        piece.data,
                        0 // ignore offsets
                    )
                    expectedPieces.add(expectedPiece)
                    val sendPieceRes = ddcProducer.send(piece).await().indefinitely()
                    pieceResponses[sendPieceRes.cid!!] = expectedPiece
                }
        }
        userThreads.forEach { it.join() }

        //then verify app pieces
        pieces.forEach { it.offset = 0 } // ignore offsets
        assertEquals(expectedPieces.toSet(), pieces)

        //then verify user pieces
        repeat(users) { userId ->
            val expectedUserPieces =
                expectedPieces.slice(userId * piecesPerUser until userId * piecesPerUser + piecesPerUser)
            val userPieces = ddcConsumer.getUserPieces("$userId").collect().asList().await().indefinitely()
            userPieces.forEach { it.offset = 0 } // ignore offsets
            assertEquals(expectedUserPieces.toSet(), userPieces.toSet())
        }
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

}
