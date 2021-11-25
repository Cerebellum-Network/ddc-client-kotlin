package network.cere.ddc.client.common.signer

import org.bouncycastle.math.ec.rfc8032.Ed25519

class Ed25519Signer(privateKey: String) : Signer {

    private val private: ByteArray = privateKey.hexToBytes().sliceArray(0 until Ed25519.SECRET_KEY_SIZE)
    private val public: ByteArray = ByteArray(Ed25519.PUBLIC_KEY_SIZE).also {
        Ed25519.generatePublicKey(private, 0, it, 0)
    }

    override fun sign(data: String): String {
        val bytes = data.toByteArray()
        val signatureBytes = ByteArray(Ed25519.SIGNATURE_SIZE)

        Ed25519.sign(private, 0, public, 0, bytes, 0, bytes.size, signatureBytes, 0)

        return signatureBytes.toHex()
    }

}
