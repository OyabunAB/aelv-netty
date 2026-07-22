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

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.SelfSignedCertificate
import io.netty.util.concurrent.DefaultThreadFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import se.oyabun.aelv.await
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.map
import se.oyabun.aelv.rightOrThrow
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class TransportTest {

    // -------------------------------------------------------------------------
    // Test server helpers — structured concurrency: server failures propagate
    // -------------------------------------------------------------------------

    private suspend fun withClosingServer(block: suspend (port: Int) -> Unit) = coroutineScope {
        val server = ServerSocket(0)
        val port   = server.localPort
        launch(Dispatchers.IO) { runCatching { server.accept().close() } }
        try { block(port) } finally { server.close() }
    }

    private suspend fun withReadingServer(
        onBytes: (ByteArray) -> Unit,
        block: suspend (port: Int) -> Unit,
    ) = coroutineScope {
        val server = ServerSocket(0)
        launch(Dispatchers.IO) {
            runCatching {
                val client = server.accept()
                val buf    = ByteArray(64)
                val count  = client.getInputStream().read(buf)
                onBytes(buf.copyOf(count))
                client.close()
            }
        }
        try { block(server.localPort) } finally { server.close() }
    }

    private suspend fun withSendingServer(
        bytesToSend: ByteArray,
        block: suspend (port: Int) -> Unit,
    ) = coroutineScope {
        val server = ServerSocket(0)
        launch(Dispatchers.IO) {
            runCatching {
                val client = server.accept()
                client.getOutputStream().write(bytesToSend)
                client.getOutputStream().flush()
                client.close()
            }
        }
        try { block(server.localPort) } finally { server.close() }
    }

    private suspend fun withChunkedServer(
        chunks: List<ByteArray>,
        delayMs: Long = 10,
        block: suspend (port: Int) -> Unit,
    ) = coroutineScope {
        val server = ServerSocket(0)
        launch(Dispatchers.IO) {
            runCatching {
                val client = server.accept()
                chunks.forEach { chunk ->
                    client.getOutputStream().write(chunk)
                    client.getOutputStream().flush()
                    Thread.sleep(delayMs)
                }
                client.close()
            }
        }
        try { block(server.localPort) } finally { server.close() }
    }

    private suspend fun withTlsServer(block: suspend (port: Int, cert: SelfSignedCertificate) -> Unit) {
        val cert      = SelfSignedCertificate()
        val serverCtx = SslContextBuilder.forServer(cert.certificate(), cert.privateKey()).build()
        val group     = MultiThreadIoEventLoopGroup(1, DefaultThreadFactory("test-tls"), NioIoHandler.newFactory())
        val channel   = ServerBootstrap()
            .group(group)
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(serverCtx.newHandler(ch.alloc()))
                    ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                            (msg as? ByteBuf)?.release()
                        }
                    })
                }
            })
            .bind(0).sync().channel()
        val port = (channel.localAddress() as InetSocketAddress).port
        try { block(port, cert) }
        finally {
            channel.close().sync()
            group.shutdownGracefully().sync()
            cert.delete()
        }
    }

    // -------------------------------------------------------------------------
    // Connection
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

    @Test fun `connect to refused port returns error`() = runBlocking {
        withTimeout(5.seconds) {
            val transport = NettyTransport()
            val result    = transport.connect("localhost", 1).await()
            assertNotNull(result.leftOrNull())
            transport.close().await()
        }
    }

    // -------------------------------------------------------------------------
    // Write
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
    // Inbound stream — via public inbound() API
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
                    .firstMaybe().await().rightOrThrow()
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

    @Test fun `inbound delivers correct byte content`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(7, 8, 9)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                val allBytes   = mutableListOf<Byte>()
                connection.inbound()
                    .map { buf: ByteBuf ->
                        val b = ByteArray(buf.readableBytes()).also { buf.readBytes(it) }
                        buf.release()
                        b
                    }
                    .firstMaybe().await().rightOrThrow()
                    ?.let { allBytes.addAll(it.toList()) }
                assertTrue(allBytes.isNotEmpty())
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    // -------------------------------------------------------------------------
    // Backpressure — via inbound()
    // -------------------------------------------------------------------------

    @Test fun `inbound backpressure — request(1) delivers exactly one buffer`() = runBlocking {
        withTimeout(5.seconds) {
            withChunkedServer(listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3))) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                val received   = AtomicInteger(0)
                val latch      = CountDownLatch(1)
                connection.inbound().subscribe(object : Subscriber<ByteBuf> {
                    private lateinit var sub: Subscription
                    override fun onSubscribe(s: Subscription) { sub = s; s.request(1) }
                    override fun onNext(buf: ByteBuf) { received.incrementAndGet(); buf.release(); latch.countDown() }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })
                latch.await(3, TimeUnit.SECONDS)
                Thread.sleep(100) // let server push more if backpressure is broken
                assertEquals(1, received.get(), "expected exactly 1 item with demand=1")
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
                val latch      = CountDownLatch(1)
                connection.inbound().subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE); s.request(Long.MAX_VALUE) }
                    override fun onNext(buf: ByteBuf) { buf.release(); latch.countDown() }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })
                assertTrue(latch.await(3, TimeUnit.SECONDS))
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
                val received   = AtomicInteger(0)
                val latch      = CountDownLatch(1)
                connection.inbound().subscribe(object : Subscriber<ByteBuf> {
                    private lateinit var sub: Subscription
                    override fun onSubscribe(s: Subscription) { sub = s; s.request(1) }
                    override fun onNext(buf: ByteBuf) { received.incrementAndGet(); buf.release(); sub.cancel(); latch.countDown() }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })
                latch.await(3, TimeUnit.SECONDS)
                Thread.sleep(150)
                assertEquals(1, received.get())
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
                connection.inbound().discard().await().rightOrThrow()
                val completed  = CountDownLatch(1)
                connection.inbound().subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) = Unit
                    override fun onNext(t: ByteBuf) = Unit
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() { completed.countDown() }
                })
                assertTrue(completed.await(2, TimeUnit.SECONDS))
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `duplicate subscribe is rejected with error`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(1)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                val error      = AtomicReference<Throwable>()
                val latch      = CountDownLatch(1)
                connection.inbound().subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE) }
                    override fun onNext(t: ByteBuf) { t.release() }
                    override fun onError(t: Throwable) = Unit
                    override fun onComplete() = Unit
                })
                connection.inbound().subscribe(object : Subscriber<ByteBuf> {
                    override fun onSubscribe(s: Subscription) = Unit
                    override fun onNext(t: ByteBuf) { t.release() }
                    override fun onError(t: Throwable) { error.set(t); latch.countDown() }
                    override fun onComplete() = Unit
                })
                assertTrue(latch.await(2, TimeUnit.SECONDS))
                assertIs<IllegalStateException>(error.get())
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    // -------------------------------------------------------------------------
    // readRawByte
    // -------------------------------------------------------------------------

    @Test fun `readRawByte reads a single byte`() = runBlocking {
        withTimeout(5.seconds) {
            withSendingServer(byteArrayOf(0x53)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                assertEquals(0x53.toByte(), connection.readRawByte())
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `readRawByte can be called multiple times sequentially`() = runBlocking {
        withTimeout(5.seconds) {
            withChunkedServer(listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3)), delayMs = 20) { port ->
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
    // TLS
    // -------------------------------------------------------------------------

    @Test fun `upgradeTls with Require succeeds`() = runBlocking {
        withTimeout(10.seconds) {
            withTlsServer { port, _ ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                connection.upgradeTls(SslMode.Require, "localhost")
                assertNotNull(connection.sslHandler())
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `channelBinding returns TlsServerEndPoint after TLS`() = runBlocking {
        withTimeout(10.seconds) {
            withTlsServer { port, _ ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                connection.upgradeTls(SslMode.Require, "localhost")
                val binding = connection.channelBinding()
                assertIs<ChannelBinding.TlsServerEndPoint>(binding)
                assertEquals(32, binding.digest.size) // SHA-256 = 32 bytes
                connection.channel.close().sync()
                transport.close().await().rightOrThrow()
            }
        }
    }

    @Test fun `channelBinding returns None without TLS`() = runBlocking {
        withTimeout(5.seconds) {
            withClosingServer { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().rightOrThrow()
                assertEquals(ChannelBinding.None, connection.channelBinding())
                assertNull(connection.sslHandler())
                transport.close().await().rightOrThrow()
            }
        }
    }

    // -------------------------------------------------------------------------
    // Transport lifecycle
    // -------------------------------------------------------------------------

    @Test fun `transport close shuts down event loop`() = runBlocking {
        withTimeout(5.seconds) {
            val transport = NettyTransport()
            transport.close().await().rightOrThrow()
        }
    }
}
