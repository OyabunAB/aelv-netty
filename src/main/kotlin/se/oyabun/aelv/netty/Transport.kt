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

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import kotlinx.coroutines.suspendCancellableCoroutine
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * A lightweight Netty TCP transport.
 *
 * [eventLoopThreads] defaults to 1 — for database driver use cases a single event loop
 * thread is sufficient since each connection is pipelined and the coroutine runtime
 * handles concurrency above the transport layer.
 *
 * Call [close] on application shutdown to release the event loop threads.
 */
class NettyTransport(eventLoopThreads: Int = 1) {

    private val log   = Logging.of<NettyTransport>()
    private val group = MultiThreadIoEventLoopGroup(eventLoopThreads, DefaultThreadFactory("netty-io"), NioIoHandler.newFactory())

    /**
     * Establishes a TCP connection to [host]:[port], returning a [NettyConnection].
     *
     * The [InboundHandler] is installed during [ChannelInitializer.initChannel] — before
     * [channelRegistered] and [channelActive] fire — so no lifecycle events are missed.
     */
    fun connect(host: String, port: Int, tcpOptions: TcpOptions = TcpOptions()): One<NettyConnection> = One.defer(context = NettyDispatchers.io) {
        log.channel.connecting(host, port)
        suspendCancellableCoroutine { continuation ->
            val handler = InboundHandler()
            val bootstrap = Bootstrap()
                .group(group)
                .channel(NioSocketChannel::class.java)
                .option(ChannelOption.TCP_NODELAY, tcpOptions.noDelay)
                .option(ChannelOption.SO_KEEPALIVE, tcpOptions.keepAlive)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.config().setAutoRead(false)
                        ch.pipeline().addLast(handler)
                    }
                })

            bootstrap.connect(host, port).addListener { future ->
                if (future.isSuccess) {
                    val ch = (future as ChannelFuture).channel()
                    log.channel.connected(ch.id(), ch.remoteAddress())
                    continuation.resume(NettyConnection(ch, handler))
                } else continuation.resumeWithException(future.cause())
            }
        }
    }

    fun close(): One<Unit> = One.defer(context = NettyDispatchers.io) {
        suspendCancellableCoroutine { continuation ->
            group.shutdownGracefully().addListener { future ->
                if (future.isSuccess) continuation.resume(Unit)
                else continuation.resumeWithException(future.cause())
            }
        }
    }

    /**
     * Connects via a Unix domain socket at [path].
     *
     * Requires `netty-transport-native-epoll` on Linux or
     * `netty-transport-native-kqueue` on macOS to be on the runtime classpath.
     * Throws [UnsupportedOperationException] if neither is available.
     */
    fun connectUnix(path: String, tcpOptions: TcpOptions = TcpOptions()): One<NettyConnection> = One.defer(context = NettyDispatchers.io) {
        log.channel.connecting(path, 0)
        suspendCancellableCoroutine { continuation ->
            val (channelClass, address) = resolveUnixChannel(path)
            val handler   = InboundHandler()
            val bootstrap = Bootstrap()
                .group(group)
                .channel(channelClass)
                .handler(object : ChannelInitializer<Channel>() {
                    override fun initChannel(ch: Channel) {
                        ch.config().setAutoRead(false)
                        ch.pipeline().addLast(handler)
                    }
                })
            bootstrap.connect(address).addListener { future ->
                if (future.isSuccess) {
                    val ch = (future as ChannelFuture).channel()
                    log.channel.connected(ch.id(), ch.remoteAddress())
                    continuation.resume(NettyConnection(ch, handler))
                } else continuation.resumeWithException(future.cause())
            }
        }
    }
}

/**
 * A connected Netty TCP channel with its [InboundHandler] pre-installed in the pipeline.
 */
class NettyConnection(
    val channel: Channel,
    internal val handler: InboundHandler,
)

/**
 * Bridges a Netty channel's inbound byte stream into an aelv [Many].
 *
 * Installed in the pipeline during [ChannelInitializer.initChannel] so it is present
 * for all lifecycle events.
 *
 * Backpressure propagates via `autoRead`: when downstream has demand, `autoRead=true`
 * lets Netty push data; when demand is exhausted, `autoRead=false` stops reads at the
 * TCP level. This mirrors reactor-netty's approach and avoids the timing issues of
 * single-shot `channel.read()` calls.
 */
class InboundHandler : ChannelInboundHandlerAdapter(), Publisher<ByteBuf> {

    private val log        = Logging.of<InboundHandler>()
    private val demand     = AtomicLong(0L)
    private val cancelled  = AtomicBoolean(false)
    private val terminated = AtomicBoolean(false)

    @Volatile private var subscriber: Subscriber<in ByteBuf>? = null
    @Volatile private var ctx: ChannelHandlerContext?         = null

    override fun subscribe(sub: Subscriber<in ByteBuf>) {
        require(subscriber == null) { "InboundHandler only supports a single subscriber" }
        subscriber = sub
        sub.onSubscribe(object : Subscription {
            override fun request(n: Long) {
                if (n <= 0L) {
                    sub.onError(IllegalArgumentException("request must be positive, got $n (RS spec §3.9)"))
                    return
                }
                log.inbound.demand(n)
                val prev = demand.getAndAdd(n)
                if (prev == 0L && !cancelled.get()) {
                    log.inbound.demandSignalled(ctx?.channel()?.id() ?: return)
                    ctx?.let { c ->
                        c.channel().eventLoop().execute {
                            c.channel().config().setAutoRead(true)
                        }
                    }
                }
            }

            override fun cancel() {
                log.inbound.cancelled()
                cancelled.set(true)
                subscriber = null
                ctx?.let { c -> c.channel().eventLoop().execute { c.channel().config().setAutoRead(false) } }
            }
        })
    }

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        this.ctx = ctx
    }

    /**
     * Delivers an inbound [ByteBuf] to the subscriber.
     *
     * The [buf] has been `retain()`ed before `onNext` is called. The subscriber MUST call
     * `buf.release()` after processing to avoid a Netty memory leak.
     */
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (cancelled.get()) { (msg as ByteBuf).release(); return }
        val sub = subscriber ?: run {
            log.inbound.droppedNoSubscriber(ctx.channel().id(), (msg as ByteBuf).readableBytes())
            (msg as ByteBuf).release()
            return
        }
        val buf = msg as ByteBuf
        buf.retain()
        log.inbound.received(ctx.channel().id(), buf.readableBytes())
        val remaining = demand.decrementAndGet()
        if (remaining == 0L) {
            log.inbound.demandExhausted(ctx.channel().id())
            ctx.channel().config().setAutoRead(false)
        }
        sub.onNext(buf)
    }

    override fun channelRegistered(ctx: ChannelHandlerContext) {
        log.channel.registered(ctx.channel().id())
        super.channelRegistered(ctx)
    }

    override fun channelActive(ctx: ChannelHandlerContext) {
        log.channel.active(ctx.channel().id())
        super.channelActive(ctx)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        log.channel.inactive(ctx.channel().id())
        if (terminated.compareAndSet(false, true)) {
            subscriber?.onComplete()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        val ex = if (cause is Exception) cause else RuntimeException(cause)
        log.channel.error(ctx.channel().id(), ex)
        if (terminated.compareAndSet(false, true)) {
            subscriber?.onError(ex)
        }
    }
}

/**
 * Reads exactly one byte from the channel, bypassing the normal inbound stream.
 *
 * Used for protocol-level handshakes that exchange a single raw byte before the
 * framing layer is active — e.g. PGwire's TLS negotiation response ('S' or 'N').
 *
 * Pipeline modification and read trigger run on the event loop to ensure ordering.
 */
suspend fun NettyConnection.readRawByte(): Byte = suspendCancellableCoroutine { continuation ->
    val eventLoop = channel.eventLoop()
    eventLoop.execute {
        channel.pipeline().addFirst("raw-byte-reader", object : io.netty.channel.ChannelInboundHandlerAdapter() {
            override fun channelRead(ctx: io.netty.channel.ChannelHandlerContext, msg: Any) {
                if (msg is io.netty.buffer.ByteBuf && msg.isReadable) {
                    val byte = msg.readByte()
                    msg.release()
                    ctx.pipeline().remove(this)
                    continuation.resume(byte)
                }
            }
            override fun exceptionCaught(ctx: io.netty.channel.ChannelHandlerContext, cause: Throwable) {
                ctx.pipeline().remove(this)
                continuation.resumeWithException(cause)
            }
        })
         channel.read()
    }
    continuation.invokeOnCancellation { /* handler will be removed on next read or connection close */ }
}

/**
 * Resolves the platform-appropriate Unix domain socket channel class and address.
 *
 * Checks for epoll (Linux) first, then kqueue (macOS). Throws [UnsupportedOperationException]
 * if neither native transport is on the classpath.
 */
private fun resolveUnixChannel(path: String): Pair<Class<out Channel>, java.net.SocketAddress> {
    runCatching {
        val epollAvailable = Class.forName("io.netty.channel.epoll.Epoll")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (epollAvailable) {
            val channelClass = Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                .asSubclass(Channel::class.java)
            val address = Class.forName("io.netty.channel.unix.DomainSocketAddress")
                .getConstructor(String::class.java).newInstance(path) as java.net.SocketAddress
            return channelClass to address
        }
    }
    runCatching {
        val kqueueAvailable = Class.forName("io.netty.channel.kqueue.KQueue")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (kqueueAvailable) {
            val channelClass = Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                .asSubclass(Channel::class.java)
            val address = Class.forName("io.netty.channel.unix.DomainSocketAddress")
                .getConstructor(String::class.java).newInstance(path) as java.net.SocketAddress
            return channelClass to address
        }
    }
    throw UnsupportedOperationException(
        "Unix domain sockets require netty-transport-native-epoll (Linux) or netty-transport-native-kqueue (macOS) on the classpath"
    )
}

fun NettyConnection.inbound(): Many<ByteBuf> = Many.from(handler)

/**
 * Writes [buf] to the channel and flushes, suspending until Netty confirms completion.
 *
 * Runs on [NettyDispatchers.io] to avoid blocking the caller's dispatcher.
 * The caller retains ownership of [buf] — release it after this call returns.
 */
fun NettyConnection.write(buf: ByteBuf): None<ByteBuf> = None.defer(context = NettyDispatchers.io) {
    val log = Logging.of<NettyConnection>()
    log.outbound.write(channel.id(), buf.readableBytes())
    suspendCancellableCoroutine { continuation ->
        channel.writeAndFlush(buf).addListener { future ->
            if (future.isSuccess) { log.outbound.writeComplete(channel.id()); continuation.resume(Unit) }
            else { log.outbound.writeFailed(channel.id(), future.cause()); continuation.resumeWithException(future.cause()) }
        }
    }
}
