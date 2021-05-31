package network.cere.ddc.client.api

import com.fasterxml.jackson.annotation.JsonProperty

data class AppTopology(
    @field:JsonProperty("appPubKey")
    val appPubKey: String? = null,

    @field:JsonProperty("partitions")
    val partitions: List<PartitionTopology>? = null
)

data class PartitionTopology(
    @field:JsonProperty("partitionId")
    val partitionId: String? = null,

    @field:JsonProperty("ringToken")
    val ringToken: Long? = null,

    @field:JsonProperty("master")
    val master: NodeMetadata? = null,

    @field:JsonProperty("replicas")
    val replicas: Set<NodeMetadata>? = null,

    @field:JsonProperty("createdAt")
    val createdAt: String? = null,

    @field:JsonProperty("updatedAt")
    val updatedAt: String? = null
)

data class NodeMetadata(
    @field:JsonProperty("nodeId")
    val nodeId: String? = null,

    @field:JsonProperty("nodeHttpAddress")
    val nodeHttpAddress: String? = null
)