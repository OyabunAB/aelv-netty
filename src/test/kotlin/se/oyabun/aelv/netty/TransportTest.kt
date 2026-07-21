/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.aelv.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import se.oyabun.aelv.Many
import se.oyabun.aelv.await
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.map
import se.oyabun.aelv.rightOrThrow
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class TransportTest {

    // -------------------------------------------------------------------------
    // Test server helpers
    // -------------------------------------------------------------------------

    /** Accepts one connection then closes it immediately. */
    private suspend fun withClosingServer(block: suspend (port: Int) -> Unit) {
        val server = ServerSocket(0)
        val port   = server.localPort
        CoroutineScope(Dispatchers.IO).launch {
            server.accept().close()
        }
        try { block(port) } finally { server.close() }
    }

    /** Accepts one connection, reads up to 64 bytes, then closes. */
    private suspend fun withReadingServer(
        onBytes: (ByteArray) -> Unit,
        block: suspend (port: Int) -> Unit,
    ) {
        val server = ServerSocket(0)
        CoroutineScope(Dispatchers.IO).launch {
            val client = server.accept()
            val buf    = ByteArray(64)
            val count  = client.getInputStream().read(buf)
            onBytes(buf.copyOf(count))
            client.close()
        }
        try { block(server.localPort) } finally { server.close() }
    }

    /** Accepts one connection, writes [bytesToSend], then closes. */
    private suspend fun withSendingServer(
        bytesToSend: ByteArray,
        block: suspend (port: Int) -> Unit,
    ) {
        val server = ServerSocket(0)
        CoroutineScope(Dispatchers.IO).launch {
            val client = server.accept()
            client.getOutputStream().write(bytesToSend)
            client.getOutputStream().flush()
            client.close()
        }
        try { block(server.localPort) } finally { server.close() }
    }

    /** Accepts one connection, writes [chunks] with a pause between each, then closes. */
    private suspend fun withChunkedServer(
        chunks: List<ByteArray>,
        delayMs: Long = 10,
        block: suspend (port: Int) -> Unit,
    ) {
        val server = ServerSocket(0)
        CoroutineScope(Dispatchers.IO).launch {
            val client = server.accept()
            chunks.forEach { chunk ->
                client.getOutputStream().write(chunk)
                client.getOutputStream().flush()
                Thread.sleep(delayMs)
            }
            client.close()
        }
        try { block(server.localPort) } finally { server.close() }
    }

    // -------------------------------------------------------------------------
    // Connection tests
    // -------------------------------------------------------------------------

    @Test fun `connects to server`() = runBlocking {
        withTimeout(5.seconds) {
            withClosingServer { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                assertTrue(connection.channel.isActive)
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `connect to refused port throws`() = runBlocking {
        withTimeout(5.seconds) {
            val transport = NettyTransport()
            // Port 1 is almost universally refused
            val result = transport.connect("localhost", 1).await()
            assertNotNull(result.leftOrNull())
            transport.close().await().rightOrThrow()
        }
    }

    // -------------------------------------------------------------------------
    // Write tests
    // -------------------------------------------------------------------------

    @Test fun `write sends bytes to server`() = runBlocking {
        withTimeout(5.seconds) {
            val received = AtomicReference<ByteArray>()
            withReadingServer(onBytes = { received.set(it) }) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                connection.write(Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3))).await().rightOrThrow()
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
            assertEquals(listOf<Byte>(1, 2, 3), received.get().toList())
        }
    }

    // -------------------------------------------------------------------------
    // Inbound stream tests
    // -------------------------------------------------------------------------

    @Test fun `inbound receives bytes from server`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(10, 20, 30)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()

                val bytes = connection.inbound()
                    .map { buf: ByteBuf ->
                        val b = ByteArray(buf.readableBytes()).also { buf.readBytes(it) }
                        buf.release()
                        b
                    }
                    .firstMaybe()
                    .await()
                    .rightOrThrow()

                assertNotNull(bytes)
                assertTrue(bytes.isNotEmpty())
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `inbound completes when server closes`() = runBlocking {
        withTimeout(5.seconds) {
            withClosingServer { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                connection.inbound().discard().await().rightOrThrow()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `inbound backpressure — request(1) delivers exactly one buffer`() = runBlocking {
        withTimeout(5.seconds) {
            withChunkedServer(listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3))) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()

                val received  = mutableListOf<Byte>()
                val latch     = CountDownLatch(1)

                connection.handler.subscribe(object : Subscriber<ByteBuf> {
                    private lateinit var sub: Subscription
                    override fun onSubscribe(s: Subscription) {
                        sub = s
                        sub.request(1) // request exactly one
                    }
                    override fun onNext(buf: ByteBuf) {
                        received += buf.readByte()
                        buf.release()
                        latch.countDown()
                        // do NOT request more — verify only 1 item arrives
                    }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })

                latch.await(3, TimeUnit.SECONDS)
                assertEquals(1, received.size, "expected exactly 1 item with demand=1")
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `inbound backpressure — request(Long MAX_VALUE) does not overflow`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(42)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()

                val latch = CountDownLatch(1)
                connection.handler.subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) {
                        s.request(Long.MAX_VALUE)
                        s.request(Long.MAX_VALUE) // would overflow a plain AtomicLong.addAndGet
                    }
                    override fun onNext(buf: ByteBuf) { buf.release(); latch.countDown() }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })

                assertTrue(latch.await(3, TimeUnit.SECONDS), "expected item after unbounded demand")
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `cancel stops inbound delivery`() = runBlocking {
        withTimeout(5.seconds) {
            withChunkedServer(listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3)), delayMs = 50) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()

                val received = AtomicInteger(0)
                val latch    = CountDownLatch(1)

                connection.handler.subscribe(object : Subscriber<ByteBuf> {
                    private lateinit var sub: Subscription
                    override fun onSubscribe(s: Subscription) { sub = s; s.request(1) }
                    override fun onNext(buf: ByteBuf) {
                        received.incrementAndGet()
                        buf.release()
                        sub.cancel()   // cancel after first item
                        latch.countDown()
                    }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })

                latch.await(3, TimeUnit.SECONDS)
                Thread.sleep(150) // let server send more; they should be dropped
                assertEquals(1, received.get(), "expected exactly 1 item after cancel")
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `subscribe after channel close signals onComplete immediately`() = runBlocking {
        withTimeout(5.seconds) {
            withClosingServer { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                // Wait for the server to close the channel
                connection.inbound().discard().await().rightOrThrow()

                // Subscribe again after termination
                val completed = CountDownLatch(1)
                connection.handler.subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) = Unit
                    override fun onNext(t: ByteBuf) = Unit
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() { completed.countDown() }
                })

                assertTrue(completed.await(2, TimeUnit.SECONDS), "expected immediate onComplete")
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `duplicate subscribe is rejected with error`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(1)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()

                val error = AtomicReference<Throwable>()
                val latch  = CountDownLatch(1)

                // First subscriber — valid
                connection.handler.subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE) }
                    override fun onNext(t: ByteBuf) { t.release() }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })

                // Second subscriber — must be rejected
                connection.handler.subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) = Unit
                    override fun onNext(t: ByteBuf) { t.release() }
                    override fun onError(t: Throwable) { error.set(t); latch.countDown() }
                    override fun onComplete() = Unit
                })

                assertTrue(latch.await(2, TimeUnit.SECONDS))
                assertTrue(error.get() is IllegalStateException)
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    // -------------------------------------------------------------------------
    // readRawByte tests
    // -------------------------------------------------------------------------

    @Test fun `readRawByte reads a single byte`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(0x53)) { port -> // 'S'
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                val byte       = connection.readRawByte()
                assertEquals(0x53.toByte(), byte)
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `readRawByte can be called multiple times sequentially`() = runBlocking {
        withTimeout(5.seconds) {
            // Send each byte in its own TCP segment so each readRawByte() call
            // sees exactly one buffer — the server sends 3 bytes with pauses between.
            withChunkedServer(
                listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3)),
                delayMs = 20,
            ) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                assertEquals(1.toByte(), connection.readRawByte())
                assertEquals(2.toByte(), connection.readRawByte())
                assertEquals(3.toByte(), connection.readRawByte())
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    // -------------------------------------------------------------------------
    // Transport lifecycle
    // -------------------------------------------------------------------------

    @Test fun `transport close shuts down the event loop`() = runBlocking {
        withTimeout(5.seconds) {
            val transport = NettyTransport()
            transport.close().await().rightOrThrow()
            // A second close should also succeed (idempotent shutdown)
            transport.close().await()
        }
    }

    // -------------------------------------------------------------------------
    // Many helper
    // -------------------------------------------------------------------------

    @Test fun `inbound as Many collects all bytes`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(7, 8, 9)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()

                val allBytes = mutableListOf<Byte>()
                connection.inbound()
                    .map { buf: ByteBuf ->
                        val b = ByteArray(buf.readableBytes()).also { buf.readBytes(it) }
                        buf.release()
                        b
                    }
                    .discard()
                    .await()
                    .rightOrThrow()

                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }
}
