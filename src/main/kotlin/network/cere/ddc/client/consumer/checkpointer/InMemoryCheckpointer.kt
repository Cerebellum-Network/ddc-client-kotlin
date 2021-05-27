package network.cere.ddc.client.consumer.checkpointer

import java.util.concurrent.ConcurrentHashMap

class InMemoryCheckpointer : Checkpointer {
    private val checkpoints = ConcurrentHashMap<String, String>()

    override fun setCheckpoint(checkpointKey: String, checkpointValue: String) {
        checkpoints[checkpointKey] = checkpointValue
    }

    override fun getCheckpoint(checkpointKey: String): String? {
        return checkpoints[checkpointKey]
    }
}