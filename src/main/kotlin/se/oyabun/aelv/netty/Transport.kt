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
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import kotlinx.coroutines.suspendCancellableCoroutine
import se.oyabun.aelv.One
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

    private val group = MultiThreadIoEventLoopGroup(eventLoopThreads, NioIoHandler.newFactory())

    /**
     * Establishes a TCP connection to [host]:[port].
     *
     * The returned [Channel] has auto-read disabled. Call [Channel.inbound] to start
     * receiving data with backpressure, and [Channel.write] to send.
     */
    fun connect(host: String, port: Int): One<Channel> = One.defer {
        suspendCancellableCoroutine { continuation ->
            val bootstrap = Bootstrap()
                .group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.config().setAutoRead(false)
                    }
                })

            bootstrap.connect(host, port).addListener { future ->
                if (future.isSuccess) continuation.resume((future as io.netty.channel.ChannelFuture).channel())
                else continuation.resumeWithException(future.cause())
            }
        }
    }

    fun close(): One<Unit> = One.defer {
        suspendCancellableCoroutine { continuation ->
            group.shutdownGracefully().addListener { future ->
                if (future.isSuccess) continuation.resume(Unit)
                else continuation.resumeWithException(future.cause())
            }
        }
    }
}
