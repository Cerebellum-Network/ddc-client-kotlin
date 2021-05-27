package network.cere.ddc.client.producer

import io.smallrye.mutiny.Uni

interface Producer {
    fun send(piece: Piece): Uni<SendPieceResponse>
}