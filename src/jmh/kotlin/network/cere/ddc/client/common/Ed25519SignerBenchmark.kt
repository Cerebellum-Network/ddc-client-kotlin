package network.cere.ddc.client.common

import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.crypto.tink.subtle.Hex
import org.bouncycastle.math.ec.rfc8032.Ed25519
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
open class Ed25519SignerBenchmark {

    private val privateKey = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"

    private val bouncycastlePrivate =
        org.bouncycastle.util.encoders.Hex.decode(privateKey.removePrefix("0x")).sliceArray(0 until Ed25519.SECRET_KEY_SIZE)
    private val bouncycastlePublic = ByteArray(Ed25519.PUBLIC_KEY_SIZE).also {
        Ed25519.generatePublicKey(bouncycastlePrivate, 0, it, 0)
    }

    private val tinkSigner = Ed25519Sign(Hex.decode(privateKey).sliceArray(0 until 32))

    @Benchmark
    fun tinkEd25519Sign(params: BenchmarkParameters, blackhole: Blackhole) {
        blackhole.consume(Hex.encode(tinkSigner.sign(params.data)))
    }

    @Benchmark
    fun bouncycastleEd25519Sign(params: BenchmarkParameters, blackhole: Blackhole) {
        val signatureBytes = ByteArray(Ed25519.SIGNATURE_SIZE)

        Ed25519.sign(bouncycastlePrivate, 0, bouncycastlePublic, 0, params.data, 0, params.data.size, signatureBytes, 0)
        blackhole.consume(org.bouncycastle.util.encoders.Hex.toHexString(signatureBytes))
    }

    @State(Scope.Benchmark)
    open class BenchmarkParameters {
        val data = "asd".repeat(200000000).toByteArray()
    }
}

