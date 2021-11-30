package network.cere.ddc.client.producer

import com.debuggor.schnorrkel.sign.KeyPair
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.api.Metadata
import network.cere.ddc.client.common.signer.Ed25519Signer
import network.cere.ddc.client.common.signer.Scheme.Ed25519
import network.cere.ddc.client.common.signer.Scheme.Sr25519
import network.cere.ddc.client.common.signer.Signer
import network.cere.ddc.client.common.signer.Sr25519Signer
import org.bouncycastle.crypto.generators.Ed25519KeyPairGenerator
import org.bouncycastle.crypto.params.Ed25519KeyGenerationParameters
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters
import org.bouncycastle.util.encoders.Hex
import org.junit.jupiter.api.Test
import java.security.SecureRandom
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals

internal class DdcProducerTest {

    private companion object {
        private const val API_PREFIX = "/api/rest"
        private const val DDC_NODE_URL = "http://localhost:8080"
    }

    private var vertx: Vertx = Vertx.vertx().apply {
        DatabindCodec.mapper().registerModule(KotlinModule())
    }
    var client: WebClient = WebClient.create(vertx)

    @Test
    fun `DDC producer - produce (positive scenario)`() {
        //given
        val appKeyPair = KeyPair.generateKeyPair()
        val appPubKey = Hex.toHexString((appKeyPair.publicKey.toPublicKey()))
        val appPrivKey = Hex.toHexString((appKeyPair.privateKey.seed))
        val signer = Sr25519Signer(appPrivKey)

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

        val piece1 = Piece(
            "1",
            appPubKey,
            "user_1",
            piece1Timestamp,
            "{\"event_type\":\"first event\"}",
            "",
            Metadata("bytes", "jpg", true, mapOf("encryptionAttribute" to "value"), mapOf("customAttribute" to "value"))
        )
        val piece2 = Piece("2", appPubKey, "user_2", piece2Timestamp, "{\"event_type\":\"second event\"}")

        //when
        testSubject.send(piece1).await().indefinitely()
        testSubject.send(piece2).await().indefinitely()

        //then
        val pieces = getPieces(appPubKey)

        //then
        val expectedPieces = setOf(
            """{"id":"1","appPubKey":"$appPubKey","userPubKey":"user_1","timestamp":"2021-01-01T00:00:00Z","data":"{\"event_type\":\"first event\"}","metadata":{"contentType":"bytes","mimeType":"jpg","isEncrypted":true,"encryptionAttributes":{"encryptionAttribute":"value"},"customAttributes":{"customAttribute":"value"}},"offset":1,"checksum":"${
                findChecksumById(
                    pieces,
                    "1"
                )
            }"}""",
            """{"id":"2","appPubKey":"$appPubKey","userPubKey":"user_2","timestamp":"2021-01-01T00:01:00Z","data":"{\"event_type\":\"second event\"}","offset":2,"checksum":"${
                findChecksumById(
                    pieces,
                    "2"
                )
            }"}""".trimMargin(),
        )

        assertEquals(expectedPieces.size, pieces.size)
        assertEquals(expectedPieces, pieces.toSet())
    }

    @Test
    fun `DDC producer - produce while app scales (positive scenario)`() {
        //given
        val appKeyPair = Ed25519KeyPairGenerator().apply { init(Ed25519KeyGenerationParameters(SecureRandom())) }
            .generateKeyPair()
        val appPubKey = Hex.toHexString((appKeyPair.public as Ed25519PublicKeyParameters).encoded)
        val appPrivKey = Hex.toHexString((appKeyPair.private as Ed25519PrivateKeyParameters).encoded)
        val signer = Ed25519Signer(appPrivKey)

        createApp(appPubKey, signer)

        val testSubject = DdcProducer(
            ProducerConfig(
                appPubKey = appPubKey,
                appPrivKey = appPrivKey,
                signatureScheme = Ed25519,
                bootstrapNodes = listOf(DDC_NODE_URL)
            )
        )

        val piece1Timestamp = Instant.parse("2021-01-01T00:00:00.000Z")
        val piece2Timestamp = Instant.parse("2021-01-01T00:01:00.000Z")
        val piece3Timestamp = Instant.parse("2021-01-01T00:02:00.000Z")

        val piece1Data = "1".repeat(3000000)
        val piece2Data = "2".repeat(3000000)
        val piece3Data = "3".repeat(3000000)
        val piece1 = Piece("1", appPubKey, "user_1", piece1Timestamp, piece1Data)
        val piece2 = Piece("2", appPubKey, "user_2", piece2Timestamp, piece2Data)
        val piece3 = Piece("3", appPubKey, "user_3", piece3Timestamp, piece3Data)

        //when
        testSubject.send(piece1).await().indefinitely()
        testSubject.send(piece2).await().indefinitely()
        testSubject.send(piece3).await().indefinitely()

        var pieces = getPieces(appPubKey)

        //then
        val expectedPieces = mutableSetOf(
            """{"id":"1","appPubKey":"$appPubKey","userPubKey":"user_1","timestamp":"2021-01-01T00:00:00Z","data":"$piece1Data","offset":1,"checksum":"${
                findChecksumById(
                    pieces,
                    "1"
                )
            }"}""",
            """{"id":"2","appPubKey":"$appPubKey","userPubKey":"user_2","timestamp":"2021-01-01T00:01:00Z","data":"$piece2Data","offset":2,"checksum":"${
                findChecksumById(
                    pieces,
                    "2"
                )
            }"}""",
            """{"id":"3","appPubKey":"$appPubKey","userPubKey":"user_3","timestamp":"2021-01-01T00:02:00Z","data":"$piece3Data","offset":3,"checksum":"${
                findChecksumById(
                    pieces,
                    "3"
                )
            }"}""",
        )

        assertEquals(expectedPieces, pieces.toSet())

        //when next piece triggers partition scaling
        val piece4Timestamp = Instant.parse("2021-01-01T00:03:00.000Z")
        val piece4Data = "4".repeat(3000000)
        val piece4 = Piece("4", appPubKey, "user_4", piece4Timestamp, piece4Data)
        testSubject.send(piece4).await().indefinitely()

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add(
            """{"id":"4","appPubKey":"$appPubKey","userPubKey":"user_4","timestamp":"2021-01-01T00:03:00Z","data":"$piece4Data","offset":1,"checksum":"${
                findChecksumById(
                    pieces,
                    "4"
                )
            }"}"""
        )
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece5Timestamp = Instant.parse("2021-01-01T00:04:00.000Z")
        val piece5Data = "5".repeat(3000000)
        val piece5 = Piece("5", appPubKey, "user_5", piece5Timestamp, piece5Data)
        testSubject.send(piece5).await().indefinitely()

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add(
            """{"id":"5","appPubKey":"$appPubKey","userPubKey":"user_5","timestamp":"2021-01-01T00:04:00Z","data":"$piece5Data","offset":2,"checksum":"${
                findChecksumById(
                    pieces,
                    "5"
                )
            }"}"""
        )
        assertEquals(expectedPieces, pieces.toSet())

        //when
        val piece6Timestamp = Instant.parse("2021-01-01T00:05:00.000Z")
        val piece6Data = "6".repeat(3000000)
        val piece6 = Piece("6", appPubKey, "user_6", piece6Timestamp, piece6Data)
        testSubject.send(piece6).await().indefinitely()

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add(
            """{"id":"6","appPubKey":"$appPubKey","userPubKey":"user_6","timestamp":"2021-01-01T00:05:00Z","data":"$piece6Data","offset":3,"checksum":"${
                findChecksumById(
                    pieces,
                    "6"
                )
            }"}"""
        )
        assertEquals(expectedPieces, pieces.toSet())

        //when next piece triggers partition scaling
        val piece7Timestamp = Instant.parse("2021-01-01T00:06:00.000Z")
        val piece7Data = "7".repeat(3000000)
        val piece7 = Piece("7", appPubKey, "user_7", piece7Timestamp, piece7Data)
        testSubject.send(piece7).await().indefinitely()

        pieces = getPieces(appPubKey)

        //then
        expectedPieces.add(
            """{"id":"7","appPubKey":"$appPubKey","userPubKey":"user_7","timestamp":"2021-01-01T00:06:00Z","data":"$piece7Data","offset":1,"checksum":"${
                findChecksumById(
                    pieces,
                    "7"
                )
            }"}"""
        )
        assertEquals(expectedPieces, pieces.toSet())
    }

    private fun createApp(appPubKey: String?, signer: Signer) {
        val toSign = "$appPubKey"
        val createAppReq = mapOf(
            "appPubKey" to appPubKey,
            "signature" to signer.sign(toSign)
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
        topology.partitions!!.forEach { partition ->
            client.getAbs("${partition.master!!.nodeHttpAddress}${API_PREFIX}/pieces?appPubKey=$appPubKey&partitionId=${partition.partitionId}")
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

    private fun findChecksumById(values: Iterable<String>, id: String) =
        values.find { it.contains("\"id\":\"$id\"") }
            ?.let { """"checksum":"([\W|\w]+)"""".toRegex().find(it)?.groupValues?.get(1) }

}
