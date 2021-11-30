package network.cere.ddc.client.common.signer

import org.bouncycastle.util.encoders.Hex

private const val HEX_PREFIX = "0x"

fun String.hexToBytes(): ByteArray = Hex.decode(this.removePrefix(HEX_PREFIX))

fun ByteArray.toHex(withPrefix: Boolean = true): String {
    val hex = String(Hex.encode(this))
    return if (withPrefix) """$HEX_PREFIX$hex""" else hex
}
