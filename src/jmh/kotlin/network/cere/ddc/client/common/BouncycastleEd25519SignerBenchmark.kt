package network.cere.ddc.client.common

import network.cere.ddc.client.common.AbstractEd25519SignerBenchmark.Companion.PRIVATE_KEY
import org.bouncycastle.math.ec.rfc8032.Ed25519
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
open class BouncycastleEd25519SignerBenchmark : AbstractEd25519SignerBenchmark() {

    private val bouncycastlePrivate =
        org.bouncycastle.util.encoders.Hex.decode(PRIVATE_KEY)
            .sliceArray(0 until Ed25519.SECRET_KEY_SIZE)
    private val bouncycastlePublic = ByteArray(Ed25519.PUBLIC_KEY_SIZE).also {
        Ed25519.generatePublicKey(bouncycastlePrivate, 0, it, 0)
    }

    override fun sign(data: ByteArray): String {
        val signatureBytes = ByteArray(Ed25519.SIGNATURE_SIZE)

        Ed25519.sign(bouncycastlePrivate, 0, bouncycastlePublic, 0, data, 0, data.size, signatureBytes, 0)
        return org.bouncycastle.util.encoders.Hex.toHexString(signatureBytes)
    }

}