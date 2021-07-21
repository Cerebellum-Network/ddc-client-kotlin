package network.cere.ddc.client.consumer

enum class OffsetReset {
    // earliest offset starts to consume data from the beginning if there is no checkpoints stored (new stream)
    EARLIEST,
    // latest offsets starts to consume data in real-time (old data isn't consumed)
    LATEST
}
