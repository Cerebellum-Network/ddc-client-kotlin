package network.cere.ddc.client.common

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

abstract class AbstractEd25519SignerBenchmark {

    companion object {
        const val PRIVATE_KEY = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
    }

    abstract fun sign(data: ByteArray): String

    @Benchmark
    fun sign1KBData(params: Perameters, blackhole: Blackhole) {
        blackhole.consume(sign(params.data1KB))
    }

    @Benchmark
    fun sign10KBData(params: Perameters, blackhole: Blackhole) {
        blackhole.consume(sign(params.data10KB))
    }

    @Benchmark
    fun sign100KBData(params: Perameters, blackhole: Blackhole) {
        blackhole.consume(sign(params.data100KB))
    }

    @Benchmark
    fun sign1MBData(params: Perameters, blackhole: Blackhole) {
        blackhole.consume(sign(params.data1MB))
    }

    @Benchmark
    fun sign10MBData(params: Perameters, blackhole: Blackhole) {
        blackhole.consume(sign(params.data10MB))
    }

    @Benchmark
    fun sign100MBData(params: Perameters, blackhole: Blackhole) {
        blackhole.consume(sign(params.data100MB))
    }

    @State(Scope.Benchmark)
    open class Perameters {
        val data1KB = "1".repeat(512).toByteArray()
        val data10KB = "2".repeat(512 * 10).toByteArray()
        val data100KB = "3".repeat(512 * 100).toByteArray()
        val data1MB = "4".repeat(512 * 1024).toByteArray()
        val data10MB = "5".repeat(512 * 1024 * 10).toByteArray()
        val data100MB = "6".repeat(512 * 1024 * 100).toByteArray()
    }

}