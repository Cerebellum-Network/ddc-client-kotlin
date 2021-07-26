package network.cere.ddc.client.api

import com.fasterxml.jackson.annotation.JsonProperty

data class AppTopology(
    @field:JsonProperty("appPubKey")
    var appPubKey: String? = null,

    @field:JsonProperty("partitions")
    var partitions: List<PartitionTopology>? = null
)

data class PartitionTopology(
    @field:JsonProperty("partitionId")
    var partitionId: String? = null,

    @field:JsonProperty("sectorStart")
    var sectorStart: Long? = null,

    @field:JsonProperty("sectorEnd")
    var sectorEnd: Long? = null,

    @field:JsonProperty("master")
    var master: NodeMetadata? = null,

    @field:JsonProperty("replicas")
    var replicas: Set<NodeMetadata>? = null,

    @field:JsonProperty("active")
    var active: Boolean = true,

    @field:JsonProperty("createdAt")
    var createdAt: String? = null,

    @field:JsonProperty("updatedAt")
    var updatedAt: String? = null
)

data class NodeMetadata(
    @field:JsonProperty("nodeId")
    var nodeId: String? = null,

    @field:JsonProperty("nodeHttpAddress")
    var nodeHttpAddress: String? = null
)