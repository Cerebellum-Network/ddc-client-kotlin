package network.cere.ddc.client.producer

import com.fasterxml.jackson.annotation.JsonProperty
import network.cere.ddc.client.api.Metadata
import java.time.Instant

data class Piece(
    @field:JsonProperty("id")
    var id: String? = null,

    @field:JsonProperty("appPubKey")
    var appPubKey: String? = null,

    @field:JsonProperty("userPubKey")
    var userPubKey: String? = null,

    @field:JsonProperty("timestamp")
    var timestamp: Instant? = null,

    @field:JsonProperty("data")
    var data: String? = null,

    @field:JsonProperty("signature")
    var signature: String = "",

    @field:JsonProperty("metadata")
    var metadata: Metadata? = null
)
