package network.cere.ddc.client.api

import com.fasterxml.jackson.annotation.JsonProperty

/*
Piece metadata used by client
 */
data class Metadata(
    @field:JsonProperty("contentType")
    var contentType: String? = null,

    @field:JsonProperty("mimeType")
    var mimeType: String? = null,

    @field:JsonProperty("isEncrypted")
    var isEncrypted: Boolean? = null,

    @field:JsonProperty("encryptionAttributes")
    var encryptionAttributes: Map<String, String>? = null,

    @field:JsonProperty("customAttributes")
    var customAttributes: Map<String, String>? = null,
)
