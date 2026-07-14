# aelv-netty

Netty TCP transport adapter for [aelv](https://github.com/OyabunAB/aelv). Provides a backpressure-aware inbound stream and write capability over a raw TCP channel.

## Requirements

- Kotlin 2.x
- JVM 21+
- GitHub Packages credentials (`GITHUB_ACTOR` / `GITHUB_TOKEN`)

## Install

```kotlin
// build.gradle.kts
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/OyabunAB/aelv-netty")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("se.oyabun:aelv:1.0.0-rc.4")
    implementation("se.oyabun:aelv-netty:<version>")
}
```

## Components

### NettyTransport

Entry point. Creates TCP connections and owns the Netty `NioEventLoopGroup`. One transport instance per application is typical.

```kotlin
val transport = NettyTransport()
```

### NettyConnection

A connected TCP channel. Exposes a `Publisher<ByteBuf>` inbound stream with backpressure and a write function.

### NettyDispatchers

Two coroutine dispatchers backed by Netty threads:

| Dispatcher | Backed by |
|---|---|
| `NettyDispatchers.io` | Netty event loop threads |
| `NettyDispatchers.connection` | aelv connection processing threads |

### InboundHandler

Installed in the Netty pipeline. Receives `ByteBuf` frames from Netty and delivers them downstream as a `Publisher<ByteBuf>` with demand-driven backpressure. Demand is propagated back to the channel's read interest.

## Usage

```kotlin
val transport = NettyTransport()

val connection: NettyConnection = transport.connect("db.example.com", 5432).get()

// Consume inbound frames
connection.inbound()
    .map { buf -> buf.readBytes(buf.readableBytes()) }
    .subscribe(mySubscriber)

// Write outbound data
connection.write(myByteBuf)

// Close
connection.close()
```

## What is not included

| Feature | Alternative |
|---|---|
| TLS / SSL | Not implemented |
| Automatic reconnect | Not implemented |
| Connection pooling | Not provided at this layer |
