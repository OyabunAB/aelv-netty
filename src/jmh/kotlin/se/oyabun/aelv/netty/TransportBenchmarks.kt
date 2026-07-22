package se.oyabun.aelv.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import kotlinx.coroutines.runBlocking
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import se.oyabun.aelv.await
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.map
import se.oyabun.aelv.rightOrThrow
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Measures raw TCP round-trip throughput using aelv-netty.
 *
 * Run with: `./gradlew :jmh`
 * Results: build/reports/jmh/results.json
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
open class TransportBenchmarks {

    private lateinit var transport:  NettyTransport
    private lateinit var connection: NettyConnection
    private lateinit var server:     ServerSocket
    private val running = AtomicBoolean(true)

    private val payload = byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8)

    @Setup(Level.Trial)
    fun setup() {
        server = ServerSocket(0)
        Thread(::echoLoop, "bench-echo-server").also { it.isDaemon = true }.start()
        transport  = NettyTransport()
        connection = runBlocking { transport.connect("localhost", server.localPort).await().rightOrThrow() }
    }

    @TearDown(Level.Trial)
    fun teardown() {
        running.set(false)
        runCatching { connection.channel.close().sync() }
        runCatching { runBlocking { transport.close().await() } }
        runCatching { server.close() }
    }

    /** Writes [payload] and waits for the echoed response. */
    @Benchmark
    fun writeAndReadEcho(): Int = runBlocking {
        connection.write(Unpooled.wrappedBuffer(payload)).await().rightOrThrow()
        connection.inbound()
            .map { buf: ByteBuf ->
                val n = buf.readableBytes()
                buf.release()
                n
            }
            .firstMaybe()
            .await()
            .rightOrThrow()
            ?: 0
    }

    private fun echoLoop() {
        while (running.get()) {
            runCatching {
                val client = server.accept()
                Thread({
                    val buf = ByteArray(256)
                    try {
                        while (true) {
                            val n = client.getInputStream().read(buf)
                            if (n == -1) break
                            client.getOutputStream().write(buf, 0, n)
                            client.getOutputStream().flush()
                        }
                    } finally {
                        runCatching { client.close() }
                    }
                }, "bench-echo-client").also { it.isDaemon = true }.start()
            }
        }
    }
}
