package network.cere.ddc.client.api

data class ApplicationTopology(
     val appPubKey: String,
     val partitions: List<PartitionTopology>
 )

data class PartitionTopology(
    val partitionId: String,
    val ringToken: Long,
    val master: NodeMetadata,
    val replicas: List<NodeMetadata>
)

data class NodeMetadata(
    val nodeId: String,
    val nodeHttpAddress: String
)