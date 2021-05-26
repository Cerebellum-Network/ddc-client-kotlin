package network.cere.ddc.client.producer

interface Producer {
    fun send(piece: Piece)
}