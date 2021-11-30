package network.cere.ddc.client.producer

import io.vertx.core.http.HttpClientOptions
import network.cere.ddc.client.common.signer.Scheme
import java.time.Duration

data class ProducerConfig(
    val appPubKey: String,
    val appPrivKey: String,
    val signatureScheme: Scheme = Scheme.Sr25519,
    val bootstrapNodes: List<String>,
    val retries: Int = 3,
    val retryBackoff: Duration = Duration.ofMillis(5000),
    val connectionNodesCacheSize: Int = 20,
    val connectionRetryBackOff: Duration = Duration.ofMillis(100),
    val retryExpiration: Duration = Duration.ofSeconds(2),
    val nodeConnectionHttp1PoolSize: Int = HttpClientOptions.DEFAULT_MAX_POOL_SIZE,
    val nodeConnectionHttp2PoolSize: Int = HttpClientOptions.DEFAULT_MAX_POOL_SIZE
)
