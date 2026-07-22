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
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Coroutine dispatchers used internally by aelv-netty.
 *
 * Thread pools are created on first access, not at class-load time.
 * Both pools use daemon threads so they do not prevent JVM exit.
 */
object NettyDispatchers {

    /**
     * Dispatcher for Netty I/O bridge operations — connect, write, close.
     *
     * Sized to at least 4 threads so concurrent connection pool operations
     * (multiple simultaneous connects or writes) don't serialise behind a
     * two-thread bottleneck. Actual I/O runs on Netty's own event loop threads.
     */
    val io: CoroutineDispatcher by lazy {
        val threads = Runtime.getRuntime().availableProcessors().coerceAtLeast(4)
        Executors.newFixedThreadPool(threads) { runnable ->
            Thread(runnable, "aelv-netty-io-${counter.incrementAndGet()}")
                .also { it.isDaemon = true }
        }.asCoroutineDispatcher()
    }

    private val counter = AtomicInteger(0)
}
