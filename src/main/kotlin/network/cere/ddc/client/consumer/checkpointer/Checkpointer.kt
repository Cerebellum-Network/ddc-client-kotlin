package network.cere.ddc.client.consumer.checkpointer

//checkpointKey - <appPubKey>:<partitionId>:<streamId>
interface Checkpointer {
   fun setCheckpoint(checkpointKey: String, checkpointValue: String)
   fun getCheckpoint(checkpointKey: String): String?
}
