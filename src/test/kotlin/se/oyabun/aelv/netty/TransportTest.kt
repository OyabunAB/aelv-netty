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
import io.netty.buffer.Unpooled
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import se.oyabun.aelv.await
import se.oyabun.aelv.discard
import se.oyabun.aelv.first
import se.oyabun.aelv.getOrThrow
import se.oyabun.aelv.map
import java.net.ServerSocket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class TransportTest {

    private suspend fun withEchoServer(block: suspend (port: Int) -> Unit) {
        val server  = ServerSocket(0)
        val port    = server.localPort
        CoroutineScope(Dispatchers.IO).launch {
            val client = server.accept()
            client.close()
        }
        try {
            block(port)
        } finally {
            server.close()
        }
    }

    private suspend fun withWriteServer(
        onBytes: (ByteArray) -> Unit,
        block: suspend (port: Int) -> Unit,
    ) {
        val server = ServerSocket(0)
        CoroutineScope(Dispatchers.IO).launch {
            val client     = server.accept()
            val readBuffer = ByteArray(16)
            val count      = client.getInputStream().read(readBuffer)
            onBytes(readBuffer.copyOf(count))
            client.close()
        }
        try {
            block(server.localPort)
        } finally {
            server.close()
        }
    }

    private suspend fun withSendServer(
        bytesToSend: ByteArray,
        block: suspend (port: Int) -> Unit,
    ) {
        val server = ServerSocket(0)
        CoroutineScope(Dispatchers.IO).launch {
            val client = server.accept()
            client.getOutputStream().write(bytesToSend)
            client.getOutputStream().flush()
            client.close()
        }
        try {
            block(server.localPort)
        } finally {
            server.close()
        }
    }

    @Test
    fun `connects to server`() = runBlocking {
        withTimeout(5.seconds) {
            withEchoServer { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().getOrThrow()
                assertTrue(connection.channel.isActive)
                connection.channel.close().sync()
                transport.close().await()
            }
        }
    }

    @Test
    fun `write sends bytes to server`() = runBlocking {
        withTimeout(5.seconds) {
            val received = mutableListOf<Byte>()
            withWriteServer(onBytes = { received += it.toList() }) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().getOrThrow()
                connection.write(Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3))).await()
                connection.channel.close().sync()
                transport.close().await()
            }
            assertEquals(listOf<Byte>(1, 2, 3), received)
        }
    }

    @Test
    fun `inbound receives bytes from server`() = runBlocking {
        withTimeout(5.seconds) {
            withSendServer(byteArrayOf(1, 2, 3)) { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().getOrThrow()

                val receivedBytes = connection.inbound()
                    .map { buffer: ByteBuf ->
                        val bytes = ByteArray(buffer.readableBytes())
                        buffer.readBytes(bytes)
                        buffer.release()
                        bytes
                    }
                    .first().getOrThrow()

                assertTrue(receivedBytes.isNotEmpty())
                connection.channel.close().sync()
                transport.close().await()
            }
        }
    }

    @Test
    fun `inbound completes when server closes`() = runBlocking {
        withTimeout(5.seconds) {
            withEchoServer { port ->
                val transport  = NettyTransport()
                val connection = transport.connect("localhost", port).await().getOrThrow()
                connection.inbound().discard().await()
                transport.close().await()
            }
        }
    }
}
