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

import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslHandler
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import kotlinx.coroutines.suspendCancellableCoroutine
import java.io.File
import java.io.FileInputStream
import java.security.KeyStore
import java.security.MessageDigest
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManagerFactory
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Controls whether and how TLS is established for a TCP connection.
 *
 * [Disable] — no TLS.
 *
 * [Prefer] — TLS if the server supports it, plain otherwise. The caller handles the
 * negotiation byte exchange and calls [upgradeTls] only if the server accepts.
 *
 * [Require] — TLS required; throws if the server declines. Does not verify the
 * certificate. Protects against passive eavesdropping, not active MITM.
 *
 * [Verify] — TLS + certificate verification against [trustStorePath]. A null
 * [trustStorePath] uses the JVM default trust store. [trustStorePassword] is required
 * for password-protected JKS and PKCS12 stores; null for unprotected or PEM.
 *
 * [VerifyFull] — TLS + certificate + hostname verification (RFC 2818 algorithm).
 * Strongest protection. [trustStorePath] and [trustStorePassword] same as [Verify].
 */
sealed interface SslMode {
    data object Disable    : SslMode
    data object Prefer     : SslMode
    data object Require    : SslMode
    data class  Verify    (val trustStorePath: String? = null, val trustStorePassword: String? = null) : SslMode
    data class  VerifyFull(val trustStorePath: String? = null, val trustStorePassword: String? = null) : SslMode
}

/**
 * Upgrades this plain TCP connection to TLS.
 *
 * Inserts an [SslHandler] at the head of the pipeline and suspends until the TLS
 * handshake completes. The caller is responsible for any protocol-level negotiation
 * (e.g. PGwire SSLRequest/S) before calling this.
 *
 * [mode] must be [SslMode.Require], [SslMode.Verify], or [SslMode.VerifyFull].
 * [SslMode.Disable] and [SslMode.Prefer] are handled by the caller before deciding
 * whether to call this function.
 */
suspend fun NettyConnection.upgradeTls(mode: SslMode, serverHostname: String) {
    val sslContext = buildSslContext(mode)
    val port       = (channel.remoteAddress() as? java.net.InetSocketAddress)?.port ?: -1
    val sslEngine  = sslContext.newEngine(channel.alloc(), serverHostname, port)
    val sslHandler = SslHandler(sslEngine)

    suspendCancellableCoroutine<Unit> { continuation ->
        channel.pipeline().addFirst("ssl", sslHandler)
        sslHandler.handshakeFuture().addListener { future ->
            if (future.isSuccess) continuation.resume(Unit)
            else continuation.resumeWithException(
                future.cause() ?: RuntimeException("TLS handshake failed")
            )
        }
        continuation.invokeOnCancellation {
            channel.eventLoop().execute { sslHandler.close() }
        }
    }
}

private fun buildSslContext(mode: SslMode): SslContext = when (mode) {
    is SslMode.Disable,
    is SslMode.Prefer     -> error("buildSslContext must not be called with ${mode::class.simpleName}")
    is SslMode.Require    -> SslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .build()
    is SslMode.Verify     -> SslContextBuilder.forClient()
        .trustManager(loadTrustManagerFactory(mode.trustStorePath, mode.trustStorePassword))
        .build()
    is SslMode.VerifyFull -> SslContextBuilder.forClient()
        .trustManager(loadTrustManagerFactory(mode.trustStorePath, mode.trustStorePassword))
        .endpointIdentificationAlgorithm("HTTPS")
        .build()
}

private fun loadTrustManagerFactory(path: String?, password: String?): TrustManagerFactory {
    val passwordChars = password?.toCharArray()
    if (path == null) {
        return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .also { it.init(null as KeyStore?) }
    }
    val file    = File(path)
    val keyStore: KeyStore = when {
        path.endsWith(".jks") -> {
            KeyStore.getInstance("JKS").also { ks ->
                FileInputStream(file).use { ks.load(it, passwordChars) }
            }
        }
        path.endsWith(".p12") || path.endsWith(".pfx") -> {
            KeyStore.getInstance("PKCS12").also { ks ->
                FileInputStream(file).use { ks.load(it, passwordChars) }
            }
        }
        else -> {
            // PEM — load as X.509 certificate entry
            val cert = FileInputStream(file).use { stream ->
                java.security.cert.CertificateFactory.getInstance("X.509")
                    .generateCertificate(stream)
            }
            return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .also { tmf ->
                    val ks = KeyStore.getInstance(KeyStore.getDefaultType())
                    ks.load(null)
                    ks.setCertificateEntry("ca", cert)
                    tmf.init(ks)
                }
        }
    }
    return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        .also { it.init(keyStore) }
}

/**
 * The channel binding established during a TLS handshake, used by SCRAM-SHA-256-PLUS.
 *
 * [None] — no TLS active.
 * [TlsServerEndPoint] — TLS active; [digest] is `SHA-256(server_leaf_certificate_DER)`
 * per RFC 5929 §4.
 */
sealed interface ChannelBinding {
    data object None                                      : ChannelBinding
    data class  TlsServerEndPoint(val digest: ByteArray)  : ChannelBinding
}

/** Returns the [SslHandler] installed in this connection's pipeline, or null. */
fun NettyConnection.sslHandler(): SslHandler? =
    channel.pipeline().get(SslHandler::class.java)

/**
 * Computes the [ChannelBinding] for this connection.
 *
 * Returns [ChannelBinding.TlsServerEndPoint] with `SHA-256(server_leaf_cert_DER)` when
 * TLS is active, or [ChannelBinding.None] if TLS was not negotiated.
 */
fun NettyConnection.channelBinding(): ChannelBinding {
    val cert = sslHandler()
        ?.engine()?.session?.peerCertificates
        ?.takeIf { it.isNotEmpty() }?.get(0) as? X509Certificate
        ?: return ChannelBinding.None
    return ChannelBinding.TlsServerEndPoint(
        MessageDigest.getInstance("SHA-256").digest(cert.encoded)
    )
}
