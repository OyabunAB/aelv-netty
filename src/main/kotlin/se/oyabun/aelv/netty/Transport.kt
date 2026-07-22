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
import io.netty.channel.IoHandlerFactory
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import kotlinx.coroutines.suspendCancellableCoroutine
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Lightweight Netty TCP transport.
 *
 * Owns a NIO [MultiThreadIoEventLoopGroup] for TCP connections. If [connectUnix] is
 * called, a native group (Epoll on Linux, KQueue on macOS) is created on first use and
 * reused for all subsequent Unix connections. Both groups are shut down by [close].
 *
 * [eventLoopThreads] defaults to 1. For database driver use cases one event loop
 * thread is sufficient — pipelining and coroutine concurrency live above this layer.
 */
class NettyTransport(eventLoopThreads: Int = 1) {

    private val log = Logging.of<NettyTransport>()

    private val group = MultiThreadIoEventLoopGroup(
        eventLoopThreads, DefaultThreadFactory("netty-io"), NioIoHandler.newFactory()
    )

    // Created on first connectUnix call; null until then so close() does not spin
    // up threads just to destroy them immediately.
    private val nativeGroupRef = AtomicReference<MultiThreadIoEventLoopGroup?>(null)

    /**
     * Establishes a TCP connection to [host]:[port].
     *
     * If cancelled while the handshake is in flight, the half-opened channel is closed.
     */
    fun connect(host: String, port: Int, tcpOptions: TcpOptions = TcpOptions()): One<NettyConnection> =
        One.defer(context = NettyDispatchers.io) {
            log.connecting("$host:$port")
            connect(group, NioSocketChannel::class.java, java.net.InetSocketAddress(host, port), tcpOptions)
        }

    /**
     * Connects via a Unix domain socket at [path].
     *
     * Requires `netty-transport-native-epoll` on Linux or `netty-transport-native-kqueue`
     * on macOS on the runtime classpath. Throws [UnsupportedOperationException] if neither
     * is available.
     *
     * [tcpOptions] fields that are TCP-specific do not apply to Unix domain sockets
     * and are silently ignored.
     */
    fun connectUnix(path: String, @Suppress("UNUSED_PARAMETER") tcpOptions: TcpOptions = TcpOptions()): One<NettyConnection> =
        One.defer(context = NettyDispatchers.io) {
            log.connecting(path)
            val native = nativeGroupRef.get()
                ?: run {
                    val resolved = resolveNative()
                        ?: throw UnsupportedOperationException(
                            "Unix domain sockets require netty-transport-native-epoll (Linux) or " +
                            "netty-transport-native-kqueue (macOS) on the classpath"
                        )
                    val group = MultiThreadIoEventLoopGroup(
                        1, DefaultThreadFactory("netty-unix"), resolved.ioHandlerFactory
                    )
                    nativeGroupRef.compareAndSet(null, group)
                    nativeGroupRef.get()!!
                }
            val resolved = resolveNative()!! // already validated above
            connect(native, resolved.channelClass, resolved.addressFor(path), null)
        }

    /**
     * Shuts down the event loop group(s) and waits for termination.
     *
     * Call this on application shutdown after all connections are closed.
     */
    fun close(): One<Unit> = One.defer(context = NettyDispatchers.io) {
        suspendCancellableCoroutine { continuation ->
            val ng = nativeGroupRef.get()
            group.shutdownGracefully().addListener { mainFuture ->
                if (ng != null) {
                    ng.shutdownGracefully().addListener { nativeFuture ->
                        val cause = mainFuture.cause() ?: nativeFuture.cause()
                        if (cause != null) continuation.resumeWithException(cause)
                        else continuation.resume(Unit)
                    }
                } else {
                    if (mainFuture.isSuccess) continuation.resume(Unit)
                    else continuation.resumeWithException(mainFuture.cause())
                }
            }
        }
    }

    private suspend fun connect(
        group:       MultiThreadIoEventLoopGroup,
        channelType: Class<out Channel>,
        address:     SocketAddress,
        tcpOptions:  TcpOptions?,
    ): NettyConnection = suspendCancellableCoroutine { continuation ->
        val handler   = InboundHandler()
        val bootstrap = Bootstrap().group(group).channel(channelType)
            .apply {
                if (tcpOptions != null) {
                    option(ChannelOption.TCP_NODELAY, tcpOptions.noDelay)
                    option(ChannelOption.SO_KEEPALIVE, tcpOptions.keepAlive)
                }
            }
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
                log.connected(ch.id(), ch.remoteAddress())
                continuation.resume(NettyConnection(ch, handler))
            } else {
                continuation.resumeWithException(future.cause())
            }
        }
    }
}

/** A connected TCP or Unix socket channel with its [InboundHandler] installed. */
class NettyConnection(
    val channel:  Channel,
    internal val handler: InboundHandler,
) {
    internal val log = Logging.of<NettyConnection>()
}

/**
 * Bridges a Netty channel's inbound byte stream into an aelv [Many].
 *
 * Backpressure: when downstream has demand, `autoRead=true` lets Netty push frames;
 * when demand is exhausted, `autoRead=false` halts reads at the TCP level.
 *
 * **Buffer ownership**: the [ByteBuf] passed to `onNext` has refcnt=1. The subscriber
 * must call `buf.release()` after processing.
 *
 * **Post-termination subscribe**: if [subscribe] is called after the channel has already
 * closed or errored, `onComplete`/`onError` is signalled immediately (RS §1.9).
 *
 * Only one subscriber is supported at a time. A second concurrent [subscribe] call is
 * rejected with `onError(IllegalStateException)`.
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
        // Register before checking terminated so we never miss a terminal signal:
        // if terminated is true when we check, we signal here; if it becomes true
        // after our CAS, channelInactive/exceptionCaught will find our subscriber
        // and signal it. The subscriberRef.CAS in the terminated path prevents
        // double-signal when both paths race.
        if (!subscriberRef.compareAndSet(null, sub)) {
            sub.onSubscribe(NoopSubscription)
            sub.onError(IllegalStateException("InboundHandler only supports a single subscriber"))
            return
        }
        if (terminated.get()) {
            // Claim the subscriber atomically; if channelInactive raced here it
            // already claimed it via getAndSet and signalled — our CAS will fail.
            if (subscriberRef.compareAndSet(sub, null)) {
                sub.onSubscribe(NoopSubscription)
                val err = terminalError
                if (err != null) sub.onError(err) else sub.onComplete()
            }
            return
        }
        sub.onSubscribe(object : Subscription {
            override fun request(n: Long) {
                if (n <= 0L) {
                    sub.onError(IllegalArgumentException("request must be positive, got $n (RS §3.9)"))
                    return
                }
                log.demand(n)
                val prev = demand.getAndUpdate { curr ->
                    if (n >= Long.MAX_VALUE - curr) Long.MAX_VALUE else curr + n
                }
                if (prev == 0L && !cancelled.get()) {
                    val c = ctx ?: return
                    log.demandSignalled(c.channel().id())
                    c.channel().eventLoop().execute { c.channel().config().setAutoRead(true) }
                }
            }
            override fun cancel() {
                log.cancelled()
                cancelled.set(true)
                subscriberRef.set(null)
                ctx?.let { c ->
                    c.channel().eventLoop().execute { c.channel().config().setAutoRead(false) }
                }
            }
        })
    }

    override fun handlerAdded(ctx: ChannelHandlerContext) { this.ctx = ctx }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val buf = msg as ByteBuf
        if (cancelled.get()) { buf.release(); return }
        val sub = subscriberRef.get() ?: run {
            log.dropped(ctx.channel().id(), buf.readableBytes())
            buf.release()
            return
        }
        log.received(ctx.channel().id(), buf.readableBytes())
        val remaining = demand.updateAndGet { curr ->
            if (curr == Long.MAX_VALUE) Long.MAX_VALUE else curr - 1
        }
        if (remaining == 0L) {
            log.demandExhausted(ctx.channel().id())
            ctx.channel().config().setAutoRead(false)
        }
        sub.onNext(buf)
    }

    override fun channelRegistered(ctx: ChannelHandlerContext) {
        log.registered(ctx.channel().id()); super.channelRegistered(ctx)
    }
    override fun channelActive(ctx: ChannelHandlerContext) {
        log.active(ctx.channel().id()); super.channelActive(ctx)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        log.inactive(ctx.channel().id())
        if (terminated.compareAndSet(false, true)) {
            subscriberRef.getAndSet(null)?.onComplete()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.error(ctx.channel().id(), cause)
        if (terminated.compareAndSet(false, true)) {
            terminalError = cause
            subscriberRef.getAndSet(null)?.onError(cause)
        }
    }
}

private object NoopSubscription : Subscription {
    override fun request(n: Long) = Unit
    override fun cancel()        = Unit
}

private val rawByteReaderCounter = AtomicInteger(0)

/**
 * Reads exactly one raw byte from the channel, bypassing the inbound stream.
 *
 * Used for protocol handshakes that exchange a single byte before framing is
 * active — e.g. PGwire's TLS negotiation response (`S` or `N`).
 *
 * If the inbound [ByteBuf] contains more than one byte (TCP coalescing), the
 * remaining bytes are forwarded to the next handler before the one-shot handler
 * is removed. Each call uses a unique handler name so sequential calls are safe.
 * Handler removal on cancellation runs on the event loop to preserve ordering.
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
                        // Forward any remaining bytes to the next handler before removal
                        if (msg.isReadable) ctx.fireChannelRead(msg.retainedSlice())
                        msg.release()
                        ctx.pipeline().remove(handlerName)
                        continuation.resume(byte)
                    } else {
                        ctx.fireChannelRead(msg)
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

// -------------------------------------------------------------------------
// Native transport detection
// -------------------------------------------------------------------------

private data class NativeTransport(
    val ioHandlerFactory: IoHandlerFactory,
    val channelClass:     Class<out Channel>,
    val addressFor:       (String) -> SocketAddress,
)

/**
 * Detects the platform-appropriate native transport (Epoll on Linux, KQueue on macOS)
 * via reflection to avoid a hard compile-time dependency.
 *
 * Returns null if neither native transport is available at runtime.
 */
private fun resolveNative(): NativeTransport? {
    // Try Epoll (Linux)
    runCatching {
        val available = Class.forName("io.netty.channel.epoll.Epoll")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (available) {
            val factory  = Class.forName("io.netty.channel.epoll.EpollIoHandler")
                .getMethod("newFactory").invoke(null) as IoHandlerFactory
            val channel  = Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                .asSubclass(Channel::class.java)
            val addrCtor = Class.forName("io.netty.channel.unix.DomainSocketAddress")
                .getConstructor(String::class.java)
            return NativeTransport(factory, channel) { addrCtor.newInstance(it) as SocketAddress }
        }
    }
    // Try KQueue (macOS)
    runCatching {
        val available = Class.forName("io.netty.channel.kqueue.KQueue")
            .getMethod("isAvailable").invoke(null) as Boolean
        if (available) {
            val factory  = Class.forName("io.netty.channel.kqueue.KQueueIoHandler")
                .getMethod("newFactory").invoke(null) as IoHandlerFactory
            val channel  = Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                .asSubclass(Channel::class.java)
            val addrCtor = Class.forName("io.netty.channel.unix.DomainSocketAddress")
                .getConstructor(String::class.java)
            return NativeTransport(factory, channel) { addrCtor.newInstance(it) as SocketAddress }
        }
    }
    return null
}

// -------------------------------------------------------------------------
// Public extension functions
// -------------------------------------------------------------------------

/** Returns the inbound byte stream as an aelv [Many]. */
fun NettyConnection.inbound(): Many<ByteBuf> = Many.from(handler)

/**
 * Writes [buf] to the channel and flushes, suspending until Netty confirms completion.
 *
 * Runs on [NettyDispatchers.io]. The caller retains ownership of [buf] — release after
 * this call if the buffer is reference-counted.
 */
fun NettyConnection.write(buf: ByteBuf): None<ByteBuf> = None.defer(context = NettyDispatchers.io) {
    log.write(channel.id(), buf.readableBytes())
    suspendCancellableCoroutine { continuation ->
        channel.writeAndFlush(buf).addListener { future ->
            if (future.isSuccess) { log.writeComplete(channel.id()); continuation.resume(Unit) }
            else { log.writeFailed(channel.id(), future.cause()); continuation.resumeWithException(future.cause()) }
        }
    }
}
