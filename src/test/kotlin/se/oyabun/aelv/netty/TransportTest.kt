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
import kotlinx.coroutines.delay
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.Observable
import se.oyabun.aelv.One
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.Verify
import se.oyabun.aelv.andThen
import se.oyabun.aelv.await
import se.oyabun.aelv.concatWith
import se.oyabun.aelv.map
import se.oyabun.aelv.mergeWith
import se.oyabun.aelv.resource
import se.oyabun.aelv.take
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class TransportTest {

    private data class TestServer(val socket: ServerSocket, val thread: Thread) {
        val port get() = socket.localPort
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any, X : Observable<T, *>> withServer(
        behavior: (Socket) -> Unit = { it.getInputStream().read() },
        use: (Int) -> X,
    ): X {
        val server = ServerSocket(0)
        val thread = Thread { runCatching { behavior(server.accept()) } }.also { it.start() }
        return use(server.localPort).doFinally { server.close(); thread.join(2000) } as X
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any, X : Observable<T, *>> withTransport(use: (NettyTransport) -> X): X {
        val transport = NettyTransport()
        return use(transport).doFinally { transport.close().await() } as X
    }


    private fun sending(vararg bytes: Byte): (Socket) -> Unit = { s ->
        s.getOutputStream().write(byteArrayOf(*bytes)); s.getOutputStream().flush(); s.close()
    }

    private fun chunked(chunks: List<ByteArray>, delayMs: Long = 10): (Socket) -> Unit = { s ->
        chunks.forEach { c -> s.getOutputStream().write(c); s.getOutputStream().flush(); Thread.sleep(delayMs) }
        s.close()
    }

    private data class TlsServer(
        val port:    Int,
        val cert:    SelfSignedCertificate,
        val channel: io.netty.channel.Channel,
        val group:   MultiThreadIoEventLoopGroup,
    )

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any, X : Observable<T, *>> withTlsServer(use: (Int, SelfSignedCertificate) -> X): X {
        val server = tlsServerAcquire()
        return use(server.port, server.cert).doFinally {
            server.channel.close().sync(); server.group.shutdownGracefully().sync(); server.cert.delete()
        } as X
    }

    private fun tlsServerAcquire(): TlsServer {
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
                        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) { (msg as? ByteBuf)?.release() }
                    })
                }
            })
            .bind(0).sync().channel()
        return TlsServer((channel.localAddress() as InetSocketAddress).port, cert, channel, group)
    }



    @Test fun `connects to server`() = Verify.that(
        withServer { port ->
            withTransport { t ->
                None.resource(
                    acquire = { t.connect("localhost", port) },
                    release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                    use     = { None.complete<Unit>() },
                )
            }
        }
    ).completes(within = 5.seconds)

    @Test fun `connect to refused port returns error`() = Verify.that(
        withTransport { t -> t.connect("localhost", 1) }
    ).failsWith<Exception>(within = 5.seconds)

    @Test fun `write sends bytes to server`() = Sinks.unicast<ByteArray>().let { received ->
        Verify.that(
            withServer(behavior = { socket: Socket ->
                val buf = ByteArray(64); received.emit(buf.copyOf(socket.getInputStream().read(buf))); received.complete(); socket.close()
            }) { port ->
                withTransport { t ->
                    One.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn -> conn.write(Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3))).andThen { received.asOne() } },
                    )
                }
            }
        ).assertNext { assertEquals(listOf<Byte>(1, 2, 3), it.toList()) }.completes(within = 5.seconds)
    }

    @Test fun `inbound delivers bytes from server`() = Verify.that(
        withServer(sending(7, 8, 9)) { port ->
            withTransport { t ->
                Many.resource(
                    acquire = { t.connect("localhost", port) },
                    release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                    use     = { conn ->
                        conn.inbound()
                            .map { buf: ByteBuf -> ByteArray(buf.readableBytes()).also { buf.readBytes(it) }.also { buf.release() } }
                            .take(1L)
                    },
                )
            }
        }
    ).assertNext { assertTrue(it.contentEquals(byteArrayOf(7, 8, 9))) }.completes(within = 5.seconds)

    @Test fun `inbound completes when server closes`() = Verify.that(
        withServer(behavior = { socket: Socket -> socket.close() }) { port ->
            withTransport { t ->
                Many.resource(
                    acquire = { t.connect("localhost", port) },
                    release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                    use     = { conn -> conn.inbound() },
                )
            }
        }
    ).completes(within = 5.seconds)

    @Test fun `inbound backpressure limits delivery to demanded items`() {
        val items = Sinks.unicast<Byte>()
        Verify.that(
            withServer(chunked(listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3)), delayMs = 50)) { port ->
                withTransport { t ->
                    None.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn ->
                            // request(1) — after first item autoRead must be disabled
                            conn.inbound().subscribe(object : Subscriber<ByteBuf> {
                                override fun onSubscribe(s: Subscription) { s.request(1) }
                                override fun onNext(buf: ByteBuf) { items.emit(buf.readByte().also { buf.release() }) }
                                override fun onError(t: Throwable) = Unit
                                override fun onComplete() = Unit
                            })
                            // wait 200ms — server would push 2nd and 3rd chunks if backpressure is broken
                            None.defer<Byte> { delay(200.milliseconds); items.complete() }
                        },
                    ).andThen { items.asMany() }
                }
            }
        ).emitsCount(1).completes(within = 5.seconds)
    }

    @Test fun `inbound backpressure — request(Long MAX_VALUE) does not overflow`() {
        val received = Sinks.unicast<Byte>()
        Verify.that(
            withServer(sending(42)) { port ->
                withTransport { t ->
                    One.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn ->
                            // two MAX_VALUE requests — demand guard must prevent overflow
                            conn.inbound().subscribe(object : Subscriber<ByteBuf> {
                                override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE); s.request(Long.MAX_VALUE) }
                                override fun onNext(buf: ByteBuf) { received.emit(buf.readByte().also { buf.release() }); received.complete() }
                                override fun onError(t: Throwable) = Unit
                                override fun onComplete() = Unit
                            })
                            None.defer<Byte> { }.andThen { received.asOne() }
                        },
                    )
                }
            }
        ).assertNext { assertEquals(42.toByte(), it) }.completes(within = 5.seconds)
    }

    @Test fun `subscribe after channel close signals onComplete immediately`() = Verify.that(
        withServer(behavior = { socket: Socket -> socket.close() }) { port ->
            withTransport { t ->
                Many.resource(
                    acquire = { t.connect("localhost", port) },
                    release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                    use     = { conn -> conn.inbound().concatWith(conn.inbound()) },
                )
            }
        }
    ).completes(within = 5.seconds)

    @Test fun `duplicate subscribe is rejected with error`() = Verify.that(
        withServer { port ->
            withTransport { t ->
                Many.resource(
                    acquire = { t.connect("localhost", port) },
                    release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                    use     = { conn -> conn.inbound().mergeWith(conn.inbound()) },
                )
            }
        }
    ).failsWith<IllegalStateException>(within = 5.seconds)

    @Test fun `readRawByte reads a single byte`() = Sinks.unicast<Byte>().let { result ->
        Verify.that(
            withServer(sending(0x53)) { port ->
                withTransport { t ->
                    One.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn ->
                            None.defer<Byte> { result.emit(conn.readRawByte()); result.complete() }
                                .andThen { result.asOne() }
                        },
                    )
                }
            }
        ).assertNext { assertEquals(0x53.toByte(), it) }.completes(within = 5.seconds)
    }

    @Test fun `readRawByte can be called multiple times sequentially`() = Sinks.unicast<List<Byte>>().let { result ->
        Verify.that(
            withServer(chunked(listOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3)), delayMs = 20)) { port ->
                withTransport { t ->
                    One.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn ->
                            None.defer<List<Byte>> {
                                result.emit(listOf(conn.readRawByte(), conn.readRawByte(), conn.readRawByte()))
                                result.complete()
                            }.andThen { result.asOne() }
                        },
                    )
                }
            }
        ).assertNext { assertEquals(listOf(1.toByte(), 2.toByte(), 3.toByte()), it) }.completes(within = 5.seconds)
    }

    @Test fun `upgradeTls with Require succeeds`() = Sinks.unicast<Boolean>().let { result ->
        Verify.that(
            withTlsServer { port, _ ->
                withTransport { t ->
                    One.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn ->
                            None.defer<Boolean> {
                                conn.upgradeTls(SslMode.Require, "localhost")
                                result.emit(conn.sslHandler() != null); result.complete()
                            }.andThen { result.asOne() }
                        },
                    )
                }
            }
        ).assertNext { assertTrue(it) }.completes(within = 10.seconds)
    }

    @Test fun `channelBinding returns TlsServerEndPoint after TLS`() = Sinks.unicast<ChannelBinding>().let { result ->
        Verify.that(
            withTlsServer { port, _ ->
                withTransport { t ->
                    One.resource(
                        acquire = { t.connect("localhost", port) },
                        release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                        use     = { conn ->
                            None.defer<ChannelBinding> {
                                conn.upgradeTls(SslMode.Require, "localhost")
                                result.emit(conn.channelBinding()); result.complete()
                            }.andThen { result.asOne() }
                        },
                    )
                }
            }
        ).assertNext { binding ->
            assertIs<ChannelBinding.TlsServerEndPoint>(binding)
            assertEquals(32, binding.digest.size)
        }.completes(within = 10.seconds)
    }

    @Test fun `channelBinding returns None without TLS`() = Verify.that(
        withServer(behavior = { socket: Socket -> socket.close() }) { port ->
            withTransport { t ->
                Many.resource(
                    acquire = { t.connect("localhost", port) },
                    release = { conn, _ -> None.defer<Unit> { conn.channel.close().sync() } },
                    use     = { conn -> Many.items(conn.channelBinding() to conn.sslHandler()) },
                )
            }
        }
    ).assertNext { (binding, ssl) ->
        assertEquals(ChannelBinding.None, binding)
        assertNull(ssl)
    }.completes(within = 5.seconds)

    @Test fun `transport close shuts down event loop`() = Verify.that(
        withTransport { Many.empty<Unit>() }
    ).completes(within = 5.seconds)
}
