package network.cere.ddc.client.common

import org.bouncycastle.math.ec.rfc8032.Ed25519
import org.bouncycastle.util.encoders.Hex

class Ed25519Signer(privateKey: String) {

    private val private: ByteArray
    private val public: ByteArray

    init {
        private = Hex.decode(privateKey.removePrefix("0x")).sliceArray(0 until Ed25519.SECRET_KEY_SIZE)

        public = ByteArray(Ed25519.PUBLIC_KEY_SIZE).also {
            Ed25519.generatePublicKey(private, 0, it, 0)
        }
    }


    fun sign(data: String): String {
        val bytes = data.toByteArray()
        val signatureBytes = ByteArray(Ed25519.SIGNATURE_SIZE)

        Ed25519.sign(private, 0, public, 0, bytes, 0, bytes.size, signatureBytes, 0)

        return Hex.toHexString(signatureBytes)
    }

}