package network.cere.ddc.client.consumer

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import network.cere.ddc.client.consumer.OffsetReset.EARLIEST

interface Consumer : AutoCloseable {
    /*
    Consume stream of application pieces. Uses offsets as checkpoints.
     */
    fun consume(
        streamId: String,
        fields: List<String> = listOf(),
        offsetReset: OffsetReset = EARLIEST
    ): Multi<ConsumerRecord>

    fun getAppPieces(
        from: String = "",
        to: String = "",
        fields: List<String> = listOf()
    ): Multi<Piece>

    fun getUserPieces(
        userPubKey: String,
        from: String = "",
        to: String = "",
        fields: List<String> = listOf()
    ): Multi<Piece>

    fun getByCid(userPubKey: String, cid: String): Uni<Piece>

    /*
    If enableAutoCommit is set to false - resolve checkpoint should be triggered by client when piece was successfully
    processed. Otherwise, checkpoint won't be saved to storage and after failure or restart consumer will start consuming
    from the beginning instead of checkpoint.

    If enableAutoCommit is set to true - consumer commits checkpoints each autoCommitIntervalMs.
     */
    fun commitCheckpoint(streamId: String, consumerRecord: ConsumerRecord)
}
