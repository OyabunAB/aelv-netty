# aelv-netty

Netty TCP transport adapter for [aelv](https://github.com/OyabunAB/aelv). Provides a
backpressure-aware inbound stream and write capability over a raw TCP channel. No Reactor,
no Flux — just aelv primitives over Netty channels.

## Install

Available on Maven Central and GitHub Packages.

```kotlin
// build.gradle.kts
dependencies {
    implementation("se.oyabun:aelv:1.0.0")
    implementation("se.oyabun:aelv-netty:1.0.0")
}
```

For release candidates (GitHub Packages only):

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/OyabunAB/aelv-netty")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}
```

## Components

### NettyTransport

Entry point. Owns the Netty `EventLoopGroup`. One instance per application is typical;
call `close()` on shutdown.

```kotlin
val transport = NettyTransport()                // 1 NIO event loop thread (sufficient for driver use)
val transport = NettyTransport(threads = 4)     // explicit thread count
```

### NettyConnection

A connected TCP channel. Exposes a backpressure-aware `Many<ByteBuf>` inbound stream and
a write function.

### InboundHandler

Installed in the Netty pipeline by `NettyTransport.connect()`. Bridges Netty's push-based
`channelRead` into an aelv reactive stream with demand-driven backpressure. When downstream
has demand, `autoRead=true` lets Netty push frames; when demand is exhausted, `autoRead=false`
halts reads at the TCP level.

**Buffer ownership**: the `ByteBuf` delivered to `onNext` has refcnt=1. The subscriber
must call `buf.release()` after processing.

### NettyDispatchers

Two coroutine dispatchers for Netty operations:

| Name | Backed by | Used for |
|---|---|---|
| `NettyDispatchers.io` | Fixed thread pool (`availableProcessors` threads) | connect, write, close |
| `NettyDispatchers.inbound` | Single thread | inbound message processing |

## Usage

```kotlin
val transport = NettyTransport()

// Connect
val connection = transport.connect("db.example.com", 5432).await().rightOrThrow()

// Consume inbound frames (backpressure-aware)
connection.inbound()
    .map { buf ->
        val bytes = ByteArray(buf.readableBytes()).also { buf.readBytes(it) }
        buf.release()
        bytes
    }
    .subscribe(mySubscriber)

// Write outbound data
connection.write(myByteBuf).await().rightOrThrow()

// Shutdown
connection.channel.close().sync()
transport.close().await().rightOrThrow()
```

## TLS

`SslMode` controls TLS negotiation. Pass it to `upgradeTls` after any protocol-level
TLS handshake byte exchange (e.g. PGwire `SSLRequest`/`S`):

```kotlin
connection.upgradeTls(SslMode.VerifyFull(), serverHostname = host)
```

| Mode | Behaviour |
|---|---|
| `Disable` | No TLS |
| `Prefer` | TLS if server supports it, plain otherwise |
| `Require` | TLS required, no certificate verification |
| `Verify(trustStorePath?)` | TLS + certificate verification |
| `VerifyFull(trustStorePath?)` | TLS + certificate + hostname verification |

`trustStorePath` accepts PEM, JKS, or PKCS12 files. `null` uses the JVM default trust store.

## Unix domain sockets

Requires `netty-transport-native-epoll` (Linux) or `netty-transport-native-kqueue` (macOS)
on the classpath:

```kotlin
// build.gradle.kts
runtimeOnly("io.netty:netty-transport-native-epoll:4.x:linux-x86_64")
```

```kotlin
val connection = transport.connectUnix("/var/run/postgresql/.s.PGSQL.5432").await().rightOrThrow()
```

## Low-level byte reads

`readRawByte()` reads a single raw byte outside the normal inbound stream. Used for
protocol handshakes before framing is active:

```kotlin
val response = connection.readRawByte() // e.g. 'S'=TLS ok, 'N'=TLS declined
```

## Benchmarks

Run with:

```
./gradlew :jmh
```

Results land in `build/reports/jmh/results.json`.

## What is not included

| Feature | Note |
|---|---|
| Automatic reconnect | Handle at the driver layer |
| Connection pooling | Not provided at this layer |
| HTTP/WebSocket | TCP only |
