package network.cere.ddc.client.common

import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import io.vertx.mutiny.core.MultiMap
import io.vertx.mutiny.core.buffer.Buffer
import io.vertx.mutiny.ext.web.client.HttpRequest
import java.net.URL
import java.time.Instant

class RequestSigner(
    privateKey: String,
    private val publicKey: String
) {

    private val signer = Ed25519Sign(Hex.decode(privateKey.removePrefix("0x")).sliceArray(0 until 32))

    fun signRequest(request: HttpRequest<Buffer>, url: String, httpMethod: String = "GET", requestExpiration: Long = 300L): HttpRequest<Buffer> {
        val urlObject = URL(url)
        val headers = listOf(
            HttpHeader("Host", urlObject.host),
            HttpHeader("Expires", Instant.now().plusSeconds(requestExpiration).toString())
        )
        val queryString = if (urlObject.query == null) "" else urlObject.query
        val data = httpMethod + urlObject.path + queryString +  headers.joinToString("") { header -> header.value }
        val signature = Hex.encode(signer.sign(data.toByteArray()))
        val headerList = headers.joinToString(";") { header -> header.name }
        val authorizationHeader = "Credential=${publicKey},SignedHeaders=$headerList,Signature=$signature"

        var httpHeaders = MultiMap.caseInsensitiveMultiMap()
        for (header in headers) {
            httpHeaders = httpHeaders.add(header.name, header.value)
        }

        return request.putHeaders(httpHeaders.add("Authorization", authorizationHeader))
    }

    private class HttpHeader(val name: String, val value: String)
}
