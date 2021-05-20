package network.cere.ddc.client.consumer

import java.time.Instant

data class Piece(
    val id: String,
    val appPubKey: String,
    val userPubKey: String,
    val timestamp: Instant,
    val data: String
)