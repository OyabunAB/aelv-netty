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

/**
 * TCP socket options applied when [NettyTransport] establishes a connection.
 *
 * [noDelay] disables Nagle's algorithm — each write is sent immediately rather than
 * being coalesced with subsequent writes. Recommended for request-response protocols
 * where latency matters more than throughput.
 *
 * [keepAlive] enables TCP keepalive probes. The OS sends periodic probes on idle
 * connections and closes them if the peer stops responding. Useful for detecting
 * silently dropped connections through NAT or firewalls without application-level pings.
 */
data class TcpOptions(
    val noDelay:   Boolean = true,
    val keepAlive: Boolean = false,
)
