package network.cere.ddc.client.common

import net.i2p.crypto.eddsa.EdDSAEngine
import net.i2p.crypto.eddsa.EdDSAPrivateKey
import net.i2p.crypto.eddsa.Utils
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec
import network.cere.ddc.client.common.AbstractEd25519SignerBenchmark.Companion.PRIVATE_KEY
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
open class EdDSAEd25519SignerBenchmark : AbstractEd25519SignerBenchmark() {

    private val messageDigest =
        MessageDigest.getInstance(EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519).hashAlgorithm)
    private val edDSAPrivateKey = EdDSAPrivateKey(
        EdDSAPrivateKeySpec(
            Utils.hexToBytes(PRIVATE_KEY).sliceArray(0 until 32),
            EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519)
        )
    )

    override fun sign(data: ByteArray): String {
        val sgr = EdDSAEngine(messageDigest)

        sgr.initSign(edDSAPrivateKey)
        sgr.update(data)

        return Utils.bytesToHex(sgr.sign())
    }
}