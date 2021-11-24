package network.cere.ddc.client.common.signer

import network.cere.ddc.client.common.signer.Scheme.Ed25519
import network.cere.ddc.client.common.signer.Scheme.Sr25519

interface Signer {

    fun sign(data: String): String

    companion object {
        fun create(scheme: Scheme, privateKey: String): Signer = when (scheme) {
            Sr25519 -> Sr25519Signer(privateKey)
            Ed25519 -> Ed25519Signer(privateKey)
        }
    }

}