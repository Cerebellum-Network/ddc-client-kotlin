package network.cere.ddc.client.producer

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class Piece(
    @field:JsonProperty("id")
    val id: String? = null,

    @field:JsonProperty("appPubKey")
    val appPubKey: String? = null,

    @field:JsonProperty("userPubKey")
    val userPubKey: String? = null,

    @field:JsonProperty("timestamp")
    val timestamp: Instant? = null,

    @field:JsonProperty("data")
    val data: String? = null,

    @field:JsonProperty("signature")
    var signature: String = ""
)