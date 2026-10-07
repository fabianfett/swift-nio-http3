//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import HTTPTypes
import Logging
public import NIOCore
import NIOHTTP3
import NIOPosix
import NIOQUIC
import NIOQUICHelpers

/// Load for the echo servers: keeps requests in flight on each connection, and measures how fast they complete.
///
/// `http3` sends HTTP/3 requests (GET without a body, POST with one) with an `HTTP3ClientConsumer`. `quic` sends as
/// many bytes as that on a bare QUIC stream, for ``QUICEchoConsumer``: the difference is what HTTP/3 costs.
@available(anyAppleOS 27, *)
public enum EchoLoad {
    public enum Mode: String, Sendable {
        case http3 = "h3"
        case quic
    }

    /// Runs the load, returning how long it took for all requests to complete.
    public static func run(
        mode: Mode,
        address: SocketAddress,
        serverName: String,
        trustRoots: String,
        connections: Int,
        concurrency: Int,
        requests: Int,
        bodySize: Int,
        group: any EventLoopGroup
    ) async throws -> TimeAmount {
        let perConnection = requests / connections
        let configuration: QUICConfiguration = {
            var configuration = QUICConfiguration.forClient(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: trustRoots),
                applicationProtocols: [mode == .quic ? "echo" : "h3"]
            )
            configuration.initialMaxData = 16_777_216
            configuration.initialMaxStreamDataBidiLocal = 2_097_152
            configuration.initialMaxStreamDataBidiRemote = 2_097_152
            configuration.initialMaxStreamDataUni = 2_097_152
            configuration.initialMaxStreamsBidi = 100
            configuration.initialMaxStreamsUni = 100
            return configuration
        }()
        let logger = Logger(label: "h3-load")

        var body = ByteBuffer()
        body.writeRepeatingByte(UInt8(ascii: "x"), count: bodySize)
        let request =
            bodySize == 0
            ? HTTPRequest(method: .get, scheme: "https", authority: serverName, path: "/")
            : HTTPRequest(method: .post, scheme: "https", authority: serverName, path: "/echo")
        let requestBody: ByteBuffer? = bodySize == 0 ? nil : body
        // The bare QUIC request carries about as many bytes as the HTTP/3 one: its HEADERS frame, and the body.
        let payload = ByteBuffer(repeating: UInt8(ascii: "q"), count: 32 + bodySize)

        var loads: [(any EventLoop, @Sendable () -> Void, EventLoopFuture<Void>)] = []

        for _ in 0..<connections {
            let eventLoop = group.next()
            let done = eventLoop.makePromise(of: Void.self)
            let load = NIOLoopBoundBox.makeBoxSendingValue(
                Load(total: perConnection, concurrency: concurrency, done: done),
                eventLoop: eventLoop
            )

            let udp = try await DatagramBootstrap(group: eventLoop).bind(host: "127.0.0.1", port: 0).get()
            let starter: @Sendable () -> Void

            if mode == .quic {
                try await eventLoop.submit {
                    let handler = QUICHandler<QUICLoadClient>(
                        channel: udp,
                        quicConfiguration: configuration,
                        asyncVerifier: try AsyncVerifier(trustRootsPath: trustRoots, eventLoop: eventLoop),
                        authenticator: nil,
                        logger: logger
                    ) { _ in QUICLoadClient(payload: payload, load: load.value) }
                    try udp.pipeline.syncOperations.addHandler(handler)
                }.get()
                let connection = try await udp.pipeline.handler(type: QUICHandler<QUICLoadClient>.self).flatMap {
                    $0.createOutboundConnection(serverName: serverName, remoteAddress: address)
                }.get()
                starter = {
                    _ = connection.assumeIsolated().withStreams { streams in
                        QUICLoadClient.start(load.value.concurrency, streams: &streams, load: load.value)
                    }
                }
            } else {
                try await eventLoop.submit {
                    let handler = QUICHandler<HTTP3LoadClient>(
                        channel: udp,
                        quicConfiguration: configuration,
                        asyncVerifier: try AsyncVerifier(trustRootsPath: trustRoots, eventLoop: eventLoop),
                        authenticator: nil,
                        logger: logger
                    ) { connection in
                        HTTP3LoadClient(
                            http3Connection: .client(connection),
                            request: request,
                            body: requestBody,
                            load: load.value
                        )
                    }
                    try udp.pipeline.syncOperations.addHandler(handler)
                }.get()
                let connection = try await udp.pipeline.handler(type: QUICHandler<HTTP3LoadClient>.self).flatMap {
                    $0.createOutboundConnection(serverName: serverName, remoteAddress: address)
                }.get()
                starter = {
                    let http3 = HTTP3Connection(configuredOn: connection)!
                    HTTP3LoadClient.start(
                        load.value.concurrency,
                        on: http3,
                        request: request,
                        body: requestBody,
                        load: load.value
                    )
                }
            }
            loads.append((eventLoop, starter, done.futureResult))
        }

        let start = NIODeadline.now()
        for (eventLoop, starter, _) in loads {
            eventLoop.execute(starter)
        }
        for (_, _, done) in loads {
            try await done.get()
        }
        let elapsed = NIODeadline.now() - start
        return elapsed
    }
}

/// The requests one connection makes. Only used on the connection's event loop.
final class Load {
    let total: Int
    let concurrency: Int
    let done: EventLoopPromise<Void>
    var started = 0
    var completed = 0

    init(total: Int, concurrency: Int, done: EventLoopPromise<Void>) {
        self.total = total
        self.concurrency = concurrency
        self.done = done
    }

    /// Records a completed request, returning whether another should start.
    func complete() -> Bool {
        self.completed += 1
        if self.completed == self.total {
            self.done.succeed()
        }
        return self.started < self.total
    }
}

// MARK: - HTTP/3

@available(anyAppleOS 27, *)
struct HTTP3LoadClient: HTTP3ClientConsumer {
    struct RequestState {
        var isComplete = false
    }

    let http3Connection: HTTP3Connection<Self>
    let request: HTTPRequest
    let body: ByteBuffer?
    let load: Load

    /// Starts requests until `count` are in flight or the load is done.
    static func start(
        _ count: Int,
        on connection: HTTP3Connection<Self>,
        request: HTTPRequest,
        body: ByteBuffer?,
        load: Load
    ) {
        for _ in 0..<count where load.started < load.total {
            load.started += 1
            do {
                _ = try connection.openRequest(state: RequestState()) { state, stream in
                    try state.writeRequest(request, body: body, to: &stream)
                }
            } catch {
                fatalError("Couldn't open a request: \(error)")
            }
        }
    }

    func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        do {
            while let part = try request.readResponsePart(from: &stream, body: { bytes, _ in bytes.byteCount }) {
                if case .end = part {
                    request.state.isComplete = true
                }
            }
        } catch {
            fatalError("Request failed: \(String(reflecting: error))")
        }
    }

    func requestStreamClosed(_ request: inout HTTP3StreamState<RequestState>, error: (any Error)?) {
        guard request.state.isComplete else {
            fatalError("Request stream closed early: \(String(describing: error))")
        }
        if self.load.complete() {
            Self.start(1, on: self.http3Connection, request: self.request, body: self.body, load: self.load)
        }
    }
}

// MARK: - QUIC

@available(anyAppleOS 27, *)
struct QUICLoadClient: QUICStreamConsumer {
    struct StreamState {}

    let payload: ByteBuffer
    let load: Load

    func makeStreamState(_ stream: inout QUICStream<Self>) -> StreamState {
        StreamState()
    }

    static func start(_ count: Int, streams: inout QUICStreams<Self>, load: Load) {
        for _ in 0..<count where load.started < load.total {
            load.started += 1
            do {
                _ = try streams.open(.clientInitiatedBidirectional, state: StreamState())
            } catch {
                fatalError("Couldn't open a stream: \(error)")
            }
        }
    }

    func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while var visit = streams.next() {
            let events = visit.events
            let payload = self.payload
            visit.withStream { stream, _ in
                if events.contains(.opened) {
                    // A stream can't be flushed before it has an ID, so the request goes out now.
                    stream.write(payload)
                    do {
                        try stream.flush(fin: true)
                    } catch {
                        fatalError("Couldn't send a request: \(error)")
                    }
                }
                _ = stream.read { bytes in bytes.byteCount }
            }
            if events.contains(.closed), self.load.complete() {
                var all = visit.streams
                Self.start(1, streams: &all, load: self.load)
            }
        }
    }
}

@available(*, unavailable)
extension Load: Sendable {}
