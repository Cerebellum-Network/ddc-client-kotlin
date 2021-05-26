package network.cere.ddc.client.api

data class AppTopology(
    val appPubKey: String,
    val partitions: List<PartitionTopology>
)

data class PartitionTopology(
    val partitionId: String,
    val ringToken: Long,
    val master: NodeMetadata,
    val replicas: Set<NodeMetadata>,
    val createdAt: String,
    val updatedAt: String
)

data class NodeMetadata(
    val nodeId: String,
    val nodeHttpAddress: String
)