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

import io.netty.channel.ChannelId
import org.slf4j.LoggerFactory
import org.slf4j.Logger as Slf4jLogger

internal inline fun Slf4jLogger.trace(msg: () -> String) { if (isTraceEnabled) trace(msg()) }
internal inline fun Slf4jLogger.debug(msg: () -> String) { if (isDebugEnabled) debug(msg()) }
internal inline fun Slf4jLogger.warn(msg: () -> String)  { if (isWarnEnabled)  warn(msg())  }
internal inline fun Slf4jLogger.warn(cause: Throwable, msg: () -> String) { if (isWarnEnabled) warn(msg(), cause) }

internal class Log(private val slf4j: Slf4jLogger) {
    fun connecting(target: String)                    = slf4j.debug { "connecting to $target" }
    fun connected(id: ChannelId, remote: Any)         = slf4j.debug { "connected [$id] -> $remote" }
    fun registered(id: ChannelId)                     = slf4j.trace { "registered [$id]" }
    fun active(id: ChannelId)                         = slf4j.trace { "active [$id]" }
    fun inactive(id: ChannelId)                       = slf4j.debug { "inactive [$id]" }
    fun error(id: ChannelId, cause: Throwable)        = slf4j.warn(cause) { "error [$id]" }
    fun demand(n: Long)                               = slf4j.trace { "request($n)" }
    fun cancelled()                                   = slf4j.debug { "inbound cancelled" }
    fun received(id: ChannelId, bytes: Int)           = slf4j.trace { "read [$id] $bytes bytes" }
    fun demandSignalled(id: ChannelId)                = slf4j.trace { "demand signalled [$id] - reading" }
    fun demandExhausted(id: ChannelId)                = slf4j.trace { "demand exhausted [$id] - paused" }
    fun dropped(id: ChannelId, bytes: Int)            = slf4j.warn { "dropped $bytes bytes [$id] - no subscriber" }
    fun write(id: ChannelId, bytes: Int)              = slf4j.trace { "write [$id] $bytes bytes" }
    fun writeComplete(id: ChannelId)                  = slf4j.trace { "write complete [$id]" }
    fun writeFailed(id: ChannelId, cause: Throwable)  = slf4j.warn(cause) { "write failed [$id]" }
}

internal object Logging {
    inline fun <reified T : Any> of(): Log = Log(LoggerFactory.getLogger(T::class.java))
}
