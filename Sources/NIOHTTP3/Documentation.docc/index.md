# ``NIOHTTP3``

Implementation of HTTP3 on top of swift-nio.

## Overview

## Topics

### Servers

- ``HTTP3ServerConfiguration``
- ``HTTP3ServerConnection``
- ``HTTP3ServerConnectionMultiplexer``
- ``HTTP3ServerTypesHandler``

### Clients
- ``HTTP3ClientConfiguration``
- ``HTTP3ClientConnection``
- ``HTTP3ClientConnectionMultiplexer``
- ``HTTP3ClientTypesHandler``

### Common 
- ``HTTP3ConnectionHandler``

### Lightweight streams

Serve the streams of a connection without a `Channel` per stream, on top of NIOQUIC's `QUICStreamConsumer`.

- ``HTTP3ServerConsumer``
- ``HTTP3ClientConsumer``
- ``HTTP3Consumer``
- ``HTTP3Connection``
- ``HTTP3StreamState``
- ``HTTP3RequestPart``
- ``HTTP3ResponsePart``
- ``HTTP3StreamsConfiguration``
