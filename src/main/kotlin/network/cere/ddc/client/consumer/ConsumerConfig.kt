package network.cere.ddc.client.consumer

import io.vertx.core.http.HttpClientOptions
import java.time.Duration

data class ConsumerConfig(
    val appPubKey: String,
    val bootstrapNodes: List<String>,
    val partitionPollIntervalMs: Int = 5_000,
    // number of threads in partition poll executor (number of partitions to be consumed in parallel)
    val partitionPollExecutorSize: Int = 4,
    val updateAppTopologyIntervalMs: Int = 30_000,
    val enableAutoCommit: Boolean = true,
    val autoCommitIntervalMs: Int = 5_000,
    val retries: Int = 3,
    val connectionNodesCacheSize: Int = 20,
    val minRetryBackOff: Duration = Duration.ofMillis(100),
    val maxRetryBackOff: Duration = Duration.ofSeconds(10),
    val nodeConnectionPoolSize: Int = HttpClientOptions.DEFAULT_MAX_POOL_SIZE
)