package network.cere.ddc.client.producer

import com.fasterxml.jackson.annotation.JsonProperty

data class SendPieceResponse(
    @field:JsonProperty("cid")
    var cid: String? = null
)