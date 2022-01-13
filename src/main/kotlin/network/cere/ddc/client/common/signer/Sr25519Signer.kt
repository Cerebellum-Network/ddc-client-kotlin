package network.cere.ddc.client.common.signer

import com.debuggor.schnorrkel.sign.KeyPair
import com.debuggor.schnorrkel.sign.SigningContext

class Sr25519Signer(privateKeyHex: String): Signer {

    private val keyPair = KeyPair.fromPrivateKey(privateKeyHex.hexToBytes())

    override fun sign(data: String): String {
        val signingContext = SigningContext.createSigningContext("substrate".toByteArray())
        return keyPair.sign(signingContext.bytes(data.toByteArray())).to_bytes().toHex()
    }

}
