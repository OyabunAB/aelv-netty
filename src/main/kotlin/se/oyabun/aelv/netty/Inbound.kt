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
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import kotlinx.coroutines.channels.Channel as KChannel
import kotlinx.coroutines.flow.flow
import se.oyabun.aelv.Many

/**
 * Bridges a Netty [Channel]'s inbound byte stream into an aelv [Many].
 *
 * Backpressure propagates end-to-end: each item is consumed before the next
 * [Channel.read] is issued, controlling the TCP receive window. When the
 * subscriber stops requesting, the channel stops reading and the TCP stack
 * applies flow control naturally.
 *
 * Each emitted [ByteBuf] has its reference count retained. The subscriber
 * must call [ByteBuf.release] after consuming each buffer.
 */
fun Channel.inbound(): Many<ByteBuf> {
    val pipe = KChannel<InboundEvent>(capacity = 1)

    pipeline().addLast(InboundBridge(pipe))
    config().setAutoRead(false)

    val nettyChannel = this
    return Many.from(flow {
        nettyChannel.read()
        try {
            for (event in pipe) {
                when (event) {
                    is InboundEvent.Data -> {
                        emit(event.buf)
                        nettyChannel.read()
                    }
                    is InboundEvent.Complete -> return@flow
                    is InboundEvent.Error    -> throw event.cause
                }
            }
        } finally {
            pipe.cancel()
        }
    })
}

internal sealed interface InboundEvent {
    data class  Data(val buf: ByteBuf)     : InboundEvent
    data object Complete                   : InboundEvent
    data class  Error(val cause: Exception) : InboundEvent
}

/**
 * Netty [ChannelInboundHandlerAdapter] that forwards channel events into [pipe].
 *
 * Retains each [ByteBuf] before forwarding — the consumer is responsible for release.
 * If the pipe is full (capacity = 1) and [trySend] fails, the buffer is released
 * immediately to avoid leaks; this should not occur under normal backpressure operation
 * since we only call [Channel.read] after consuming the previous buffer.
 */
internal class InboundBridge(
    private val pipe: KChannel<InboundEvent>,
) : ChannelInboundHandlerAdapter() {

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val buf = msg as ByteBuf
        buf.retain()
        val result = pipe.trySend(InboundEvent.Data(buf))
        if (result.isFailure) buf.release()
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        pipe.trySend(InboundEvent.Complete)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        val exception = if (cause is Exception) cause else RuntimeException(cause)
        pipe.trySend(InboundEvent.Error(exception))
    }
}
