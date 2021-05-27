package network.cere.ddc.client.producer

import java.time.Duration

data class ProducerConfig(
    val appPubKey: String,
    val appPrivKey: String,
    val bootstrapNodes: List<String>,
    val retries: Int = 3,
    val retryBackoff: Duration = Duration.ofMillis(5000)
)