package network.cere.ddc.client.producer

import java.time.Instant

data class Piece(
    val id: String,
    val appPubKey: String,
    val userPubKey: String,
    val timestamp: Instant,
    val data: String,

    var signature: String = ""
)