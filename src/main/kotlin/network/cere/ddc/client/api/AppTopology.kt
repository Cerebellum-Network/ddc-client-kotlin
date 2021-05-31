package network.cere.ddc.client.api

import com.fasterxml.jackson.annotation.JsonProperty

data class AppTopology(
    @field:JsonProperty("appPubKey")
    val appPubKey: String,

    @field:JsonProperty("partitions")
    val partitions: List<PartitionTopology>
)

data class PartitionTopology(
    @field:JsonProperty("partitionId")
    val partitionId: String,

    @field:JsonProperty("ringToken")
    val ringToken: Long,

    @field:JsonProperty("master")
    val master: NodeMetadata,

    @field:JsonProperty("replicas")
    val replicas: Set<NodeMetadata>,

    @field:JsonProperty("createdAt")
    val createdAt: String,

    @field:JsonProperty("updatedAt")
    val updatedAt: String
)

data class NodeMetadata(
    @field:JsonProperty("nodeId")
    val nodeId: String,

    @field:JsonProperty("nodeHttpAddress")
    val nodeHttpAddress: String
)