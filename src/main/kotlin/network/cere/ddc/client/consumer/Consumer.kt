package network.cere.ddc.client.consumer

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni

interface Consumer : AutoCloseable {
    fun consume(streamId: String, dataQuery: DataQuery): Multi<ConsumerRecord>

    fun getByCid(userPubKey: String, cid: String): Uni<Piece>

    /*
    If enableAutoCommit is set to false - resolve checkpoint should be triggered by client when piece was successfully
    processed. Otherwise, checkpoint won't be saved to storage and after failure or restart consumer will start consuming
    from the beginning instead of checkpoint.

    If enableAutoCommit is set to true - consumer commits checkpoints each autoCommitIntervalMs.
     */
    fun commitCheckpoint(streamId: String, consumerRecord: ConsumerRecord)
}
