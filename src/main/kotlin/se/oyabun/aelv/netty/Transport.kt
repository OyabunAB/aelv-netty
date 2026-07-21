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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * A lightweight Netty TCP transport.
 *
 * Owns a NIO [MultiThreadIoEventLoopGroup] for TCP connections and, if Unix domain
 * socket support is requested at runtime, a lazily-created native group backed by
 * Epoll (Linux) or KQueue (macOS). Both groups are shut down by [close].
 *
 * [eventLoopThreads] defaults to 1 — for database driver use cases a single event loop
 * thread is sufficient since each connection is pipelined and the coroutine runtime
 * handles concurrency above the transport layer.
 */
class NettyTransport(eventLoopThreads: Int = 1) {

    private val log   = Logging.of<NettyTransport>()
    private val group = MultiThreadIoEventLoopGroup(
        eventLoopThreads, DefaultThreadFactory("netty-io"), NioIoHandler.newFactory()
    )

    // Lazily created on first connectUnix() call; null if no native transport is available.
    private val nativeGroup: MultiThreadIoEventLoopGroup? by lazy { resolveNativeGroup(1) }

    /**
     * Establishes a TCP connection to [host]:[port], returning a [NettyConnection].
     *
     * The [InboundHandler] is installed during [ChannelInitializer.initChannel] — before
     * [Channel.channelRegistered] and [Channel.channelActive] fire — so no lifecycle
     * events are missed.
     *
     * If the coroutine is cancelled while the TCP handshake is in flight, the half-opened
     * channel is closed immediately.
     */
    fun connect(host: String, port: Int, tcpOptions: TcpOptions = TcpOptions()): One<NettyConnection> =
        One.defer(context = NettyDispatchers.io) {
            log.channel.connecting(host, port)
            suspendCancellableCoroutine { continuation ->
                val handler    = InboundHandler()
                val bootstrap  = Bootstrap()
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
                val connectFuture = bootstrap.connect(host, port)
                continuation.invokeOnCancellation {
                    connectFuture.cancel(true)
                    runCatching { (connectFuture as? ChannelFuture)?.channel()?.close() }
                }
                connectFuture.addListener { future ->
                    if (!continuation.isActive) {
                        (future as? ChannelFuture)?.channel()?.close()
                        return@addListener
                    }
                    if (future.isSuccess) {
                        val ch = (future as ChannelFuture).channel()
                        log.channel.connected(ch.id(), ch.remoteAddress())
                        continuation.resume(NettyConnection(ch, handler))
                    } else {
                        continuation.resumeWithException(future.cause())
                    }
                }
            }
        }

    /**
     * Connects via a Unix domain socket at [path].
     *
     * Requires `netty-transport-native-epoll` on Linux or
     * `netty-transport-native-kqueue` on macOS to be on the runtime classpath.
     * Throws [UnsupportedOperationException] if neither is available.
     *
     * [tcpOptions] fields that are TCP-specific (`noDelay`, `keepAlive`) do not apply
     * to Unix domain sockets and are silently ignored.
     */
    fun connectUnix(path: String, tcpOptions: TcpOptions = TcpOptions()): One<NettyConnection> =
        One.defer(context = NettyDispatchers.io) {
            log.channel.connecting(path, 0)
            val group = nativeGroup
                ?: throw UnsupportedOperationException(
                    "Unix domain sockets require netty-transport-native-epoll (Linux) or " +
                    "netty-transport-native-kqueue (macOS) on the classpath"
                )
            suspendCancellableCoroutine { continuation ->
                val (channelClass, address) = resolveUnixChannel(path)
                val handler    = InboundHandler()
                val bootstrap  = Bootstrap()
                    .group(group)
                    .channel(channelClass)
                    .handler(object : ChannelInitializer<Channel>() {
                        override fun initChannel(ch: Channel) {
                            ch.config().setAutoRead(false)
                            ch.pipeline().addLast(handler)
                        }
                    })
                val connectFuture = bootstrap.connect(address)
                continuation.invokeOnCancellation {
                    connectFuture.cancel(true)
                    runCatching { (connectFuture as? ChannelFuture)?.channel()?.close() }
                }
                connectFuture.addListener { future ->
                    if (!continuation.isActive) {
                        (future as? ChannelFuture)?.channel()?.close()
                        return@addListener
                    }
                    if (future.isSuccess) {
                        val ch = (future as ChannelFuture).channel()
                        log.channel.connected(ch.id(), ch.remoteAddress())
                        continuation.resume(NettyConnection(ch, handler))
                    } else {
                        continuation.resumeWithException(future.cause())
                    }
                }
            }
        }

    /**
     * Shuts down the event loop group(s) and waits for them to terminate.
     *
     * Call this on application shutdown after all connections have been closed.
     */
    fun close(): One<Unit> = One.defer(context = NettyDispatchers.io) {
        suspendCancellableCoroutine { continuation ->
            val ng = nativeGroup
            group.shutdownGracefully().addListener { future ->
                if (ng != null) {
                    ng.shutdownGracefully().addListener { _ ->
                        if (future.isSuccess) continuation.resume(Unit)
                        else continuation.resumeWithException(future.cause())
                    }
                } else {
                    if (future.isSuccess) continuation.resume(Unit)
                    else continuation.resumeWithException(future.cause())
                }
            }
        }
    }
}

/**
 * A connected Netty TCP or Unix socket channel with its [InboundHandler] pre-installed.
 */
class NettyConnection(
    val channel: Channel,
    internal val handler: InboundHandler,
) {
    internal val log = Logging.of<NettyConnection>()
}

/**
 * Bridges a Netty channel's inbound byte stream into an aelv [Many].
 *
 * Installed in the pipeline during [ChannelInitializer.initChannel] so it is present
 * for all lifecycle events. Only one subscriber is supported at a time.
 *
 * **Backpressure** propagates via `autoRead`: when downstream has demand, `autoRead=true`
 * lets Netty push data; when demand is exhausted, `autoRead=false` stops reads at the
 * TCP level.
 *
 * **Buffer ownership**: the [ByteBuf] passed to `onNext` has refcnt=1. The subscriber
 * **must** call `buf.release()` after processing to avoid a Netty memory leak.
 *
 * **Post-termination subscribe**: if [subscribe] is called after the channel has already
 * closed or errored, `onComplete` (or `onError`) is signalled immediately per RS §1.9.
 */
class InboundHandler : ChannelInboundHandlerAdapter(), Publisher<ByteBuf> {

    private val log           = Logging.of<InboundHandler>()
    private val demand        = AtomicLong(0L)
    private val cancelled     = AtomicBoolean(false)
    private val terminated    = AtomicBoolean(false)
    private val subscriberRef = AtomicReference<Subscriber<in ByteBuf>?>(null)

    @Volatile private var ctx:           ChannelHandlerContext? = null
    @Volatile private var terminalError: Throwable?             = null

    override fun subscribe(sub: Subscriber<in ByteBuf>) {
        // RS §1.9: if already terminated, signal immediately
        if (terminated.get()) {
            sub.onSubscribe(NoopSubscription)
            val err = terminalError
            if (err != null) sub.onError(err) else sub.onComplete()
            return
        }
        if (!subscriberRef.compareAndSet(null, sub)) {
            sub.onSubscribe(NoopSubscription)
            sub.onError(IllegalStateException("InboundHandler only supports a single subscriber"))
            return
        }
        sub.onSubscribe(object : Subscription {
            override fun request(n: Long) {
                if (n <= 0L) {
                    sub.onError(IllegalArgumentException("request must be positive, got $n (RS §3.9)"))
                    return
                }
                log.inbound.demand(n)
                // Cap at Long.MAX_VALUE to avoid overflow (RS §3.17)
                val prev = demand.getAndUpdate { curr ->
                    if (n >= Long.MAX_VALUE - curr) Long.MAX_VALUE else curr + n
                }
                if (prev == 0L && !cancelled.get()) {
                    val c = ctx ?: return
                    log.inbound.demandSignalled(c.channel().id())
                    c.channel().eventLoop().execute {
                        c.channel().config().setAutoRead(true)
                    }
                }
            }

            override fun cancel() {
                log.inbound.cancelled()
                cancelled.set(true)
                subscriberRef.set(null)
                ctx?.let { c ->
                    c.channel().eventLoop().execute { c.channel().config().setAutoRead(false) }
                }
            }
        })
    }

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        this.ctx = ctx
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val buf = msg as ByteBuf
        if (cancelled.get()) { buf.release(); return }
        val sub = subscriberRef.get() ?: run {
            log.inbound.droppedNoSubscriber(ctx.channel().id(), buf.readableBytes())
            buf.release()
            return
        }
        log.inbound.received(ctx.channel().id(), buf.readableBytes())
        val remaining = demand.decrementAndGet()
        if (remaining == 0L) {
            log.inbound.demandExhausted(ctx.channel().id())
            ctx.channel().config().setAutoRead(false)
        }
        // Transfer ownership to subscriber (refcnt stays at 1; subscriber must release)
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
            subscriberRef.getAndSet(null)?.onComplete()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.channel.error(ctx.channel().id(), cause)
        if (terminated.compareAndSet(false, true)) {
            terminalError = cause
            subscriberRef.getAndSet(null)?.onError(cause)
        }
    }
}

// Reused for post-termination subscriptions and rejected duplicate subscriptions.
private object NoopSubscription : Subscription {
    override fun request(n: Long) = Unit
    override fun cancel()        = Unit
}

private val rawByteReaderCounter = AtomicInteger(0)

/**
 * Reads exactly one raw byte from the channel, bypassing the normal inbound stream.
 *
 * Used for protocol-level handshakes that exchange a single raw byte before the
 * framing layer is active — e.g. PGwire's TLS negotiation response (`S` or `N`).
 *
 * Each call installs a uniquely-named one-shot handler at the head of the pipeline.
 * The handler is removed immediately after the byte is read, on exception, or on
 * coroutine cancellation — no state leaks to subsequent reads.
 */
suspend fun NettyConnection.readRawByte(): Byte {
    val handlerName = "raw-byte-reader-${rawByteReaderCounter.incrementAndGet()}"
    val eventLoop   = channel.eventLoop()
    return suspendCancellableCoroutine { continuation ->
        eventLoop.execute {
            channel.pipeline().addFirst(handlerName, object : ChannelInboundHandlerAdapter() {
                override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                    if (msg is ByteBuf && msg.isReadable) {
                        val byte = msg.readByte()
                        msg.release()
                        ctx.pipeline().remove(handlerName)
                        continuation.resume(byte)
                    } else {
                        (msg as? ByteBuf)?.release()
                    }
                }
                override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                    ctx.pipeline().remove(handlerName)
                    continuation.resumeWithException(cause)
                }
            })
            channel.read()
        }
        continuation.invokeOnCancellation {
            eventLoop.execute { runCatching { channel.pipeline().remove(handlerName) } }
        }
    }
}

/**
 * Resolves the platform-appropriate native [io.netty.channel.IoHandlerFactory] and creates
 * a new [MultiThreadIoEventLoopGroup] for Unix domain socket connections.
 *
 * Returns null if neither Epoll (Linux) nor KQueue (macOS) is available on the classpath.
 */
private fun resolveNativeGroup(threads: Int): MultiThreadIoEventLoopGroup? {
    runCatching {
        val available = Class.forName("io.netty.channel.epoll.Epoll")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (available) {
            val factory = Class.forName("io.netty.channel.epoll.EpollIoHandler")
                .getMethod("newFactory").invoke(null) as io.netty.channel.IoHandlerFactory
            return MultiThreadIoEventLoopGroup(threads, DefaultThreadFactory("netty-unix"), factory)
        }
    }
    runCatching {
        val available = Class.forName("io.netty.channel.kqueue.KQueue")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (available) {
            val factory = Class.forName("io.netty.channel.kqueue.KQueueIoHandler")
                .getMethod("newFactory").invoke(null) as io.netty.channel.IoHandlerFactory
            return MultiThreadIoEventLoopGroup(threads, DefaultThreadFactory("netty-unix"), factory)
        }
    }
    return null
}

/**
 * Resolves the platform-appropriate Unix domain socket channel class and connect address.
 *
 * Checks for Epoll (Linux) first, then KQueue (macOS).
 * Throws [UnsupportedOperationException] if neither native transport is available.
 */
private fun resolveUnixChannel(path: String): Pair<Class<out Channel>, java.net.SocketAddress> {
    runCatching {
        val available = Class.forName("io.netty.channel.epoll.Epoll")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (available) {
            val channelClass = Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                .asSubclass(Channel::class.java)
            val address = Class.forName("io.netty.channel.unix.DomainSocketAddress")
                .getConstructor(String::class.java).newInstance(path) as java.net.SocketAddress
            return channelClass to address
        }
    }
    runCatching {
        val available = Class.forName("io.netty.channel.kqueue.KQueue")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (available) {
            val channelClass = Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                .asSubclass(Channel::class.java)
            val address = Class.forName("io.netty.channel.unix.DomainSocketAddress")
                .getConstructor(String::class.java).newInstance(path) as java.net.SocketAddress
            return channelClass to address
        }
    }
    throw UnsupportedOperationException(
        "Unix domain sockets require netty-transport-native-epoll (Linux) or " +
        "netty-transport-native-kqueue (macOS) on the classpath"
    )
}

/** Returns the inbound byte stream of this connection as an aelv [Many]. */
fun NettyConnection.inbound(): Many<ByteBuf> = Many.from(handler)

/**
 * Writes [buf] to the channel and flushes, suspending until Netty confirms the write.
 *
 * Runs on [NettyDispatchers.io] to avoid blocking the caller's dispatcher.
 * The caller retains ownership of [buf] — release it after this call returns if
 * the underlying [io.netty.buffer.ByteBuf] is reference-counted.
 */
fun NettyConnection.write(buf: ByteBuf): None<ByteBuf> = None.defer(context = NettyDispatchers.io) {
    log.outbound.write(channel.id(), buf.readableBytes())
    suspendCancellableCoroutine { continuation ->
        channel.writeAndFlush(buf).addListener { future ->
            if (future.isSuccess) {
                log.outbound.writeComplete(channel.id())
                continuation.resume(Unit)
            } else {
                log.outbound.writeFailed(channel.id(), future.cause())
                continuation.resumeWithException(future.cause())
            }
        }
    }
}
