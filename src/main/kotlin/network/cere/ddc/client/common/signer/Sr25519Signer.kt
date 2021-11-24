package network.cere.ddc.client.common.signer

import com.debuggor.schnorrkel.sign.ExpansionMode
import com.debuggor.schnorrkel.sign.KeyPair
import com.debuggor.schnorrkel.sign.SigningContext

class Sr25519Signer(privateKey: String): Signer {

    private val signingContext = SigningContext.createSigningContext("substrate".toByteArray())

    private val keyPair = KeyPair.fromSecretSeed(privateKey.hexToBytes(), ExpansionMode.Ed25519)

    override fun sign(data: String): String {
        return keyPair.sign(signingContext.bytes(data.toByteArray())).to_bytes().toHex()
    }

}