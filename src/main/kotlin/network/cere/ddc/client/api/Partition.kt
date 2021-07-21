package network.cere.ddc.client.api

import com.fasterxml.jackson.annotation.JsonProperty

data class Partition(
    @field:JsonProperty("id")
    var id: String? = null,

    @field:JsonProperty("appPubKey")
    var appPubKey: String? = null,

    @field:JsonProperty("nodeId")
    var nodeId: String? = null,

    @field:JsonProperty("status")
    var status: String? = null,

    @field:JsonProperty("isMaster")
    var isMaster: Boolean? = null,

    @field:JsonProperty("sectorStart")
    var sectorStart: Long? = null,

    @field:JsonProperty("sectorEnd")
    var sectorEnd: Long? = null,

    @field:JsonProperty("backup")
    var backup: Boolean? = null,

    @field:JsonProperty("deleted")
    var deleted: Boolean? = null,

    @field:JsonProperty("active")
    var active: Boolean? = null,

    @field:JsonProperty("replicaIndex")
    var replicaIndex: Long? = null,

    @field:JsonProperty("createdAt")
    var createdAt: String? = null,

    @field:JsonProperty("updatedAt")
    var updatedAt: String? = null,

    @field:JsonProperty("latestOffset")
    var latestOffset: Long? = null
)
