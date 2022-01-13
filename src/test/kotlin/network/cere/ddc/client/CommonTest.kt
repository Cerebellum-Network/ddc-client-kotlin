package network.cere.ddc.client

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.client.WebClient
import network.cere.ddc.client.common.signer.Scheme
import network.cere.ddc.client.common.signer.Scheme.Ed25519
import network.cere.ddc.client.consumer.ConsumerConfig
import network.cere.ddc.client.consumer.DdcConsumer
import network.cere.ddc.client.producer.DdcProducer
import network.cere.ddc.client.producer.Piece
import network.cere.ddc.client.producer.ProducerConfig
import org.bouncycastle.crypto.generators.Ed25519KeyPairGenerator
import org.bouncycastle.crypto.params.Ed25519KeyGenerationParameters
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters
import org.bouncycastle.crypto.signers.Ed25519Signer
import org.bouncycastle.util.encoders.Hex
import org.junit.jupiter.api.Test
import java.security.SecureRandom
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.test.Ignore
import kotlin.test.assertEquals

@Ignore
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
        val appKeyPair = Ed25519KeyPairGenerator().apply { init(Ed25519KeyGenerationParameters(SecureRandom())) }
            .generateKeyPair()
        val appPubKey = Hex.toHexString((appKeyPair.public as Ed25519PublicKeyParameters).encoded)
        val appPrivKey = Hex.toHexString((appKeyPair.private as Ed25519PrivateKeyParameters).encoded)
        val signer = Ed25519Signer().apply { init(true, appKeyPair.private) }

        createApp(appPubKey, signer)

        val ddcProducer = DdcProducer(
            ProducerConfig(
                appPubKey = appPubKey,
                appPrivKey = appPrivKey,
                signatureScheme = Ed25519,
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
        val expectedPieces = ArrayList<network.cere.ddc.client.consumer.Piece>(users * piecesPerUser)

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
                ddcProducer.send(piece).await().indefinitely()
            }
        }
        userThreads.forEach { it.join() }

        //then verify app pieces
        pieces.forEach {
            it.offset = 0
            it.checksum = null
        } // ignore offsets and checksum
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

    private fun createApp(appPubKey: String?, signer: Ed25519Signer) {
        val toSign = "$appPubKey".toByteArray()
        signer.update(toSign, 0, toSign.size)
        val createAppReq = mapOf(
            "appPubKey" to appPubKey,
            "signature" to Hex.toHexString(signer.generateSignature())
        ).let(::JsonObject)

        client.postAbs("$DDC_NODE_URL$API_PREFIX/apps")
            .sendJsonObject(createAppReq)
            .toCompletionStage()
            .toCompletableFuture()
            .get()
            .also { assertEquals(OK.code(), it.statusCode()) }
    }

}
