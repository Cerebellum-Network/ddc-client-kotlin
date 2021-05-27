package network.cere.ddc.client.consumer

data class ConsumerRecord(
    val piece: Piece,
    val partitionId: String,
    val checkpointValue: String
)
