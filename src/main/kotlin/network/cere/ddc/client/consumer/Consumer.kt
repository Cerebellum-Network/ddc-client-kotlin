package network.cere.ddc.client.consumer

import io.smallrye.mutiny.Multi

interface Consumer {
    fun consume(streamId: String, dataQuery: DataQuery): Multi<Piece>
}