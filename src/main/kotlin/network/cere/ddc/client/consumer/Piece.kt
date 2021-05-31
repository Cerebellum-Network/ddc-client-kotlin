package network.cere.ddc.client.consumer

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class Piece(
    @field:JsonProperty("id")
    val id: String,

    @field:JsonProperty("appPubKey")
    val appPubKey: String,

    @field:JsonProperty("userPubKey")
    val userPubKey: String,

    @field:JsonProperty("timestamp")
    val timestamp: Instant,

    @field:JsonProperty("data")
    val data: String
)