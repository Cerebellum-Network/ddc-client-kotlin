package network.cere.ddc.client.common

import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import network.cere.ddc.client.common.AbstractEd25519SignerBenchmark.Companion.PRIVATE_KEY
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
open class TinkEd25519SignerBenchmark : AbstractEd25519SignerBenchmark() {

    private val tinkSigner = Ed25519Sign(Hex.decode(PRIVATE_KEY).sliceArray(0 until 32))

    override fun sign(data: ByteArray): String {
        return Hex.encode(tinkSigner.sign(data))
    }

}