package network.cere.ddc.client.common

import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Ed25519Verify
import com.google.crypto.tink.subtle.Hex
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.ext.web.client.WebClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.security.GeneralSecurityException
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

internal class RequestSignerTest {

    private val appKeyPair = Ed25519Sign.KeyPair.newKeyPair()
    private val appPubKey = Hex.encode(appKeyPair.publicKey)
    private val appPrivateKey = Hex.encode(appKeyPair.privateKey)
    private val requestSigner = RequestSigner(appPrivateKey, appPubKey)
    private val url = "https://node-0.ddc.dev.cere.network/api/rest/pieces?appPubKey=${appPubKey}&partitionId=testPartitionId"
    private val request = WebClient.create(Vertx.vertx()).getAbs(url)
    private val signedRequest = requestSigner.signRequest(request, url)
    private val authorizationHeader: String = signedRequest.headers().get("Authorization")
    private val hostHeader = signedRequest.headers().get("Host")
    private val expirationHeader = signedRequest.headers().get("Expires")

    @Test
    fun `RequestSigner - signature verification should succeed`() {
        val signature = common()
        val headers = hostHeader + expirationHeader
        val data = "GET/api/rest/piecesappPubKey=${appPubKey}&partitionId=testPartitionId$headers"
        val verifier = Ed25519Verify(Hex.decode(appPubKey.removePrefix("0x")).sliceArray(0 until 32))
        assertDoesNotThrow { verifier.verify(signature, data.toByteArray()) }
    }

    @Test
    fun `RequestSigner - signature verification should fail`() {
        val signature = common()
        val headers = hostHeader + expirationHeader
        val data = "/api/rest/piecesappPubKey=${appPubKey}&partitionId=testPartitionId$headers" // missing GET
        val verifier = Ed25519Verify(Hex.decode(appPubKey.removePrefix("0x")).sliceArray(0 until 32))
        assertThrows<GeneralSecurityException> { verifier.verify(signature, data.toByteArray()) }
    }

    private fun common(): ByteArray {
        assert(authorizationHeader.contains("Signature"))
        assert(authorizationHeader.contains("SignedHeaders"))
        assert(authorizationHeader.contains("Credential"))
        assertNotNull(hostHeader)
        assertNotNull(expirationHeader)
        assertNotNull(authorizationHeader)

        val authorizationHeaderParts = authorizationHeader.split(",")
        assertEquals(authorizationHeaderParts.size, 3)

        val signatureParts = authorizationHeaderParts[2].split("=")
        assertEquals(signatureParts.size, 2)

        return Hex.decode(signatureParts[1].removePrefix("0x"))
    }
}
