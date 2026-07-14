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

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

/**
 * Coroutine dispatchers used internally by aelv-netty.
 *
 * Using named thread pools makes coroutine activity visible and identifiable
 * in thread dumps, profilers, and distributed traces.
 */
object NettyDispatchers {

    /**
     * Dispatcher for Netty I/O operations — connect, write, close.
     *
     * Sized to the number of available processors. All Netty future callbacks
     * resume on this dispatcher so they never block the caller's thread.
     */
    val io: CoroutineDispatcher = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors(),
    ) { runnable ->
        Thread(runnable, "aelv-netty-io-${threadCounter.incrementAndGet()}")
            .also { it.isDaemon = true }
    }.asCoroutineDispatcher()

    /**
     * Dispatcher for inbound message processing — framing, decoding, routing.
     *
     * Single-threaded to preserve message ordering within a connection.
     */
    val inbound: CoroutineDispatcher = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "aelv-netty-inbound")
            .also { it.isDaemon = true }
    }.asCoroutineDispatcher()

    private val threadCounter = java.util.concurrent.atomic.AtomicInteger(0)
}
