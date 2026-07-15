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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import kotlinx.coroutines.suspendCancellableCoroutine
import java.io.File
import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.TrustManagerFactory
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Controls whether and how TLS is established for a TCP connection.
 *
 * [Disable] sends no [SSLRequest] and connects plain.
 *
 * [Prefer] attempts TLS if the server supports it, falls back to plain if the server
 * declines. Use when the server may or may not have TLS enabled.
 *
 * [Require] requires TLS — throws if the server declines. Does not verify the server
 * certificate. Protects against passive eavesdropping but not active MITM.
 *
 * [Verify] requires TLS and verifies the server certificate against [trustStorePath].
 * A null [trustStorePath] uses the JVM's default trust store (works for publicly-trusted CAs).
 *
 * [VerifyFull] requires TLS, verifies the server certificate, and additionally checks
 * that the server hostname matches the certificate's CN or SAN. Strongest protection.
 * A null [trustStorePath] uses the JVM's default trust store.
 */
sealed interface SslMode {
    data object Disable    : SslMode
    data object Prefer     : SslMode
    data object Require    : SslMode
    data class  Verify  (val trustStorePath: String? = null) : SslMode
    data class  VerifyFull(val trustStorePath: String? = null) : SslMode
}

/**
 * Upgrades this plain TCP connection to TLS using Netty's [io.netty.handler.ssl.SslHandler].
 *
 * Inserts the [io.netty.handler.ssl.SslHandler] at the head of the pipeline and suspends
 * until the TLS handshake completes. The caller is responsible for any protocol-level
 * TLS negotiation (e.g. PGwire SSLRequest/S) before calling this.
 *
 * [mode] must not be [SslMode.Disable] or [SslMode.Prefer] — those are handled by the
 * caller before deciding whether to call this function.
 *
 * [serverHostname] is used for SNI and, when [mode] is [SslMode.VerifyFull],
 * for hostname verification against the server certificate.
 */
suspend fun NettyConnection.upgradeTls(mode: SslMode, serverHostname: String) {
    val sslContext = buildSslContext(mode, serverHostname)
    val sslEngine  = sslContext.newEngine(channel.alloc(), serverHostname, channel.remoteAddress().let {
        (it as? java.net.InetSocketAddress)?.port ?: 5432
    })

    val sslHandler = io.netty.handler.ssl.SslHandler(sslEngine)

    suspendCancellableCoroutine<Unit> { continuation ->
        channel.pipeline().addFirst("ssl", sslHandler)
        sslHandler.handshakeFuture().addListener { future ->
            if (future.isSuccess) continuation.resume(Unit)
            else continuation.resumeWithException(
                future.cause() ?: RuntimeException("TLS handshake failed")
            )
        }
        continuation.invokeOnCancellation { sslHandler.close() }
    }
}

private fun buildSslContext(mode: SslMode, hostname: String): SslContext = when (mode) {
    is SslMode.Disable    -> error("buildSslContext called with SslMode.Disable")
    is SslMode.Prefer,
    is SslMode.Require    -> SslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .build()
    is SslMode.Verify   -> SslContextBuilder.forClient()
        .trustManager(loadTrustManagerFactory(mode.trustStorePath))
        .build()
    is SslMode.VerifyFull -> SslContextBuilder.forClient()
        .trustManager(loadTrustManagerFactory(mode.trustStorePath))
        .endpointIdentificationAlgorithm("HTTPS")
        .build()
}

private fun loadTrustManagerFactory(trustStorePath: String?): TrustManagerFactory {
    if (trustStorePath == null) {
        return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .also { it.init(null as KeyStore?) }
    }
    val file = File(trustStorePath)
    val keyStore = when {
        trustStorePath.endsWith(".jks") || trustStorePath.endsWith(".p12") -> {
            val ks = KeyStore.getInstance(if (trustStorePath.endsWith(".p12")) "PKCS12" else "JKS")
            FileInputStream(file).use { ks.load(it, null) }
            ks
        }
        else -> {
            // Treat as PEM — load cert and add to a KeyStore
            return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .also { tmf ->
                    val ks   = KeyStore.getInstance(KeyStore.getDefaultType())
                    ks.load(null)
                    val cert = java.security.cert.CertificateFactory.getInstance("X.509")
                        .generateCertificate(FileInputStream(file))
                    ks.setCertificateEntry("ca", cert)
                    tmf.init(ks)
                }
        }
    }
    return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        .also { it.init(keyStore) }
}
