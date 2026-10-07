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
        path: String? = nil,
        group: any EventLoopGroup
    ) async throws -> TimeAmount {
        try await Self.run(
            mode: mode,
            address: address,
            serverName: serverName,
            trustRoots: trustRoots,
            connections: connections,
            concurrency: concurrency,
            requestsPerConnection: requests / connections,
            warmUp: nil,
            duration: nil,
            bodySize: bodySize,
            path: path,
            group: group,
            timedPhase: { _ in }
        ).elapsed
    }

    /// Runs the load: for `warmUp` first if given, then either `requestsPerConnection` requests on each connection,
    /// or, if `duration` is given, as many as complete in it.
    ///
    /// - Parameter timedPhase: Called with `true` right before the measured phase starts, and with `false` right
    ///   after it ended.
    /// - Returns: How long the measured phase took, and how many requests completed and failed in it.
    public static func run(
        mode: Mode,
        address: SocketAddress,
        serverName: String,
        trustRoots: String,
        connections: Int,
        concurrency: Int,
        requestsPerConnection perConnection: Int,
        warmUp: TimeAmount?,
        duration: TimeAmount?,
        bodySize: Int,
        path: String?,
        group: any EventLoopGroup,
        timedPhase: @Sendable (Bool) -> Void
    ) async throws -> (elapsed: TimeAmount, completed: Int, failed: Int) {
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
            ? HTTPRequest(method: .get, scheme: "https", authority: serverName, path: path ?? "/")
            : HTTPRequest(method: .post, scheme: "https", authority: serverName, path: "/echo")
        let requestBody: ByteBuffer? = bodySize == 0 ? nil : body
        // The bare QUIC request carries about as many bytes as the HTTP/3 one: its HEADERS frame, and the body.
        let payload = ByteBuffer(repeating: UInt8(ascii: "q"), count: 32 + bodySize)

        var loads: [(any EventLoop, @Sendable () -> Void, NIOLoopBoundBox<Load>)] = []

        for _ in 0..<connections {
            let eventLoop = group.next()
            let load = NIOLoopBoundBox.makeBoxSendingValue(
                Load(concurrency: concurrency),
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
                    guard let http3 = HTTP3Connection(configuredOn: connection) else {
                        // The connection closed already.
                        load.value.started += 1
                        load.value.connectionFailed()
                        return
                    }
                    HTTP3LoadClient.start(
                        load.value.concurrency,
                        on: http3,
                        request: request,
                        body: requestBody,
                        load: load.value
                    )
                }
            }
            loads.append((eventLoop, starter, load))
        }

        /// Runs one phase on every connection, returning how many requests completed and failed.
        func phase(total: Int, duration: TimeAmount?) async throws -> (completed: Int, failed: Int) {
            let deadline = duration.map { NIODeadline.now() + $0 }
            var futures: [EventLoopFuture<(completed: Int, failed: Int)>] = []
            for (eventLoop, starter, load) in loads {
                let done = eventLoop.makePromise(of: (completed: Int, failed: Int).self)
                eventLoop.execute {
                    load.value.begin(total: total, deadline: deadline, done: done)
                    starter()
                }
                futures.append(done.futureResult)
            }
            var completed = 0
            var failed = 0
            for future in futures {
                let result = try await future.get()
                completed += result.completed
                failed += result.failed
            }
            return (completed, failed)
        }

        if let duration {
            // Like h2load: requests run continuously from the start, and the measurement is the difference of the
            // counters between the end of the warm-up and the end of the duration. Nothing waits for requests in
            // flight to drain, so a request that never completes costs only its slot, not the connection.
            for (eventLoop, starter, load) in loads {
                let done = eventLoop.makePromise(of: (completed: Int, failed: Int).self)
                done.futureResult.whenComplete { _ in }
                eventLoop.execute {
                    load.value.begin(total: .max, deadline: nil, done: done)
                    starter()
                }
            }

            func counters() async throws -> (completed: Int, failed: Int) {
                var completed = 0
                var failed = 0
                for (eventLoop, _, load) in loads {
                    let counts = try await eventLoop.submit { (load.value.completed, load.value.failed) }.get()
                    completed += counts.0
                    failed += counts.1
                }
                return (completed, failed)
            }

            try await Task.sleep(nanoseconds: UInt64((warmUp ?? .zero).nanoseconds))
            timedPhase(true)
            let start = NIODeadline.now()
            let before = try await counters()
            try await Task.sleep(nanoseconds: UInt64(duration.nanoseconds))
            timedPhase(false)
            let elapsed = NIODeadline.now() - start
            let after = try await counters()
            for (eventLoop, _, load) in loads {
                eventLoop.execute { load.value.stop() }
            }
            return (elapsed, after.completed - before.completed, after.failed - before.failed)
        }

        if let warmUp {
            _ = try await phase(total: .max, duration: warmUp)
        }

        timedPhase(true)
        let start = NIODeadline.now()
        let (completed, failed) = try await phase(total: perConnection, duration: nil)
        let elapsed = NIODeadline.now() - start
        timedPhase(false)
        return (elapsed, completed, failed)
    }
}

/// The requests one connection makes in a phase. Only used on the connection's event loop.
///
/// Like h2load, a request that fails is counted, not fatal. A connection that can't open requests any more stops.
final class Load {
    let concurrency: Int
    private var total = 0
    private var deadline: NIODeadline?
    private var done: EventLoopPromise<(completed: Int, failed: Int)>?
    private var stopped = false
    var started = 0
    var completed = 0
    var failed = 0

    init(concurrency: Int) {
        self.concurrency = concurrency
    }

    func begin(total: Int, deadline: NIODeadline?, done: EventLoopPromise<(completed: Int, failed: Int)>) {
        self.total = total
        self.deadline = deadline
        self.done = done
        self.stopped = false
        self.started = 0
        self.completed = 0
        self.failed = 0
    }

    /// Whether another request should start.
    var canStart: Bool {
        !self.stopped && self.started < self.total
    }

    /// Records a completed request, returning whether another should start.
    func complete() -> Bool {
        self.completed += 1
        return self.finishedOne()
    }

    /// Records a failed request, returning whether another should start.
    func fail() -> Bool {
        self.failed += 1
        return self.finishedOne()
    }

    /// Stops starting requests.
    func stop() {
        self.stopped = true
    }

    /// Records that the connection can't open requests any more.
    func connectionFailed() {
        self.stopped = true
        _ = self.fail()
    }

    private func finishedOne() -> Bool {
        let finished = self.completed + self.failed
        if !self.stopped, let deadline = self.deadline, NIODeadline.now() >= deadline {
            self.stopped = true
        }
        if finished == self.total || (self.stopped && finished == self.started) {
            self.done?.succeed((self.completed, self.failed))
            self.done = nil
        }
        return self.canStart
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
        for _ in 0..<count where load.canStart {
            load.started += 1
            do {
                _ = try connection.openRequest(state: RequestState()) { state, stream in
                    try state.writeRequest(request, body: body, to: &stream)
                }
            } catch {
                load.connectionFailed()
                return
            }
        }
    }

    func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        do {
            while let part = try request.readResponsePart(from: &stream, body: { bytes, _ in bytes.byteCount }) {
                if case .end = part {
                    request.state.isComplete = true
                    // Like h2load: a request is done when its response is, not when its stream has closed too.
                    if self.load.complete() {
                        Self.start(1, on: self.http3Connection, request: self.request, body: self.body, load: self.load)
                    }
                }
            }
        } catch {
            // Counted once its stream closes.
        }
    }

    func requestStreamClosed(_ request: inout HTTP3StreamState<RequestState>, error: (any Error)?) {
        guard !request.state.isComplete else { return }
        if self.load.fail() {
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
        for _ in 0..<count where load.canStart {
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
