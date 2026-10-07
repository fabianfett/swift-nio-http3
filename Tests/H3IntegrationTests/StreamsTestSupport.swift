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

import H3EchoServer
import HTTP3
import HTTPTypes
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTPTypes
import NIOPosix
import NIOQUIC
import NIOQUICHelpers

@_spi(HTTP3AsyncInterface) @testable import NIOHTTP3

/// A response as a client saw it.
struct CollectedResponse: Sendable {
    var head: HTTPResponse
    var body: ByteBuffer
    var trailers: HTTPFields?
}

/// A request stream closed before its response was complete.
struct IncompleteResponse: Error {
    var cause: (any Error)?
}

// MARK: - Lightweight client

/// An HTTP/3 client on `HTTP3ClientConsumer` which writes a request and collects the response.
///
/// It writes the request body only as fast as the stream takes it, and stops reading the response body while
/// its request's read gate is closed: together they exercise back pressure in both directions.
@available(anyAppleOS 27, *)
struct StreamsTestClient: HTTP3ClientConsumer {
    struct RequestState {
        /// What's left of the request body to write.
        var toSend: ByteBuffer
        var trailers: HTTPFields?
        /// The response so far.
        var response: HTTPResponse?
        var interimResponses: [HTTPResponse]
        var body: ByteBuffer
        let promise: EventLoopPromise<CollectedResponse>
        /// While `true`, response body bytes aren't read.
        let readGate: NIOLockedValueBox<Bool>
        /// The number of request body bytes written so far.
        let written: NIOLockedValueBox<Int>
        var isComplete: Bool
        /// Set when the test wrote the stream itself, rather than through the HTTP/3 layer.
        var writesRaw: Bool
    }

    let http3Connection: HTTP3Connection<Self>

    func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        Self.drive(&request, stream: &stream)
    }

    func requestStreamClosed(_ request: inout HTTP3StreamState<RequestState>, error: (any Error)?) {
        if !request.state.isComplete {
            request.state.isComplete = true
            request.state.promise.fail(IncompleteResponse(cause: error))
        }
    }

    /// Writes as much of the request as the stream takes, then reads as much of the response as is there.
    static func drive(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        guard !request.state.isComplete else { return }

        if !request.state.writesRaw {
            Self.write(&request, stream: &stream)
        }
        Self.read(&request, stream: &stream)
    }

    private static func write(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        do {
            var wrote = false
            while request.state.toSend.readableBytes > 0 {
                let room = request.writableBytes(&stream)
                guard room > 0 else { break }
                let length = min(room, request.state.toSend.readableBytes, 16 * 1024)
                let chunk = request.state.toSend.readSlice(length: length)!
                try request.writeBody(chunk, to: &stream)
                request.state.written.withLockedValue { $0 += length }
                wrote = true
            }

            if request.state.toSend.readableBytes == 0, !request.isOutboundComplete {
                try request.writeEnd(trailers: request.state.trailers, to: &stream)
            } else if wrote {
                try request.flush(&stream)
            }
        } catch {
            request.state.isComplete = true
            request.state.promise.fail(error)
            request.cancel(stream: &stream)
        }
    }

    private static func read(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        guard !request.state.isComplete else { return }
        if !request.state.readGate.withLockedValue({ $0 }) {
            request.resumeReading()
        }
        do {
            while !request.state.isComplete,
                let part = try request.readResponsePart(
                    from: &stream,
                    body: { bytes, state in
                        if state.readGate.withLockedValue({ $0 }) {
                            return 0
                        }
                        bytes.withUnsafeBytes { _ = state.body.writeBytes($0) }
                        return bytes.byteCount
                    }
                )
            {
                switch part {
                case .head(let head):
                    if head.status.kind == .informational {
                        request.state.interimResponses.append(head)
                    } else {
                        request.state.response = head
                    }
                case .body:
                    ()
                case .end(let trailers):
                    request.state.isComplete = true
                    request.state.promise.succeed(
                        CollectedResponse(head: request.state.response!, body: request.state.body, trailers: trailers)
                    )
                }
            }
        } catch {
            request.state.isComplete = true
            request.state.promise.fail(error)
        }
    }
}

/// A `StreamsTestClient` connected to a server.
@available(anyAppleOS 27, *)
struct StreamsClient: Sendable {
    let udpChannel: any Channel
    /// Only used on its event loop: `HTTP3Connection` isn't `Sendable`.
    let connection: NIOLoopBound<HTTP3Connection<StreamsTestClient>>
    let closeFuture: EventLoopFuture<Void>

    static func connect(
        to address: SocketAddress,
        credentials: Credentials,
        group: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) async throws -> StreamsClient {
        let (quicConfiguration, serverName) = credentials.clientConfiguration

        let udpChannel = try await DatagramBootstrap(group: group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let asyncVerifier: AsyncVerifier? =
                    switch credentials {
                    case .certificates(_, _, _, let trustRootsPath):
                        try AsyncVerifier(trustRootsPath: trustRootsPath, eventLoop: channel.eventLoop)
                    case .rawKeys:
                        nil
                    }
                let handler = QUICHandler<StreamsTestClient>(
                    channel: channel,
                    quicConfiguration: quicConfiguration,
                    asyncVerifier: asyncVerifier,
                    authenticator: nil,
                    logger: Logger(label: "StreamsClient")
                ) { connection in
                    StreamsTestClient(http3Connection: .client(connection))
                }
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()

        let connection = try await udpChannel.pipeline.handler(type: QUICHandler<StreamsTestClient>.self)
            .flatMap { handler in
                handler.createOutboundConnection(serverName: serverName, remoteAddress: address)
            }
            .flatMapThrowing { quicConnection in
                guard let connection = HTTP3Connection(configuredOn: quicConnection) else {
                    throw ChannelError.inappropriateOperationForState
                }
                return NIOLoopBound(connection, eventLoop: quicConnection.eventLoop)
            }
            .get()

        return StreamsClient(
            udpChannel: udpChannel,
            connection: connection,
            closeFuture: try await connection.eventLoop.submit { connection.value.closeFuture }.get()
        )
    }

    /// Starts a request, returning its handle and the future of its response.
    func start(
        _ head: HTTPRequest,
        body: ByteBuffer = ByteBuffer(),
        trailers: HTTPFields? = nil,
        readGate: NIOLockedValueBox<Bool> = NIOLockedValueBox(false),
        written: NIOLockedValueBox<Int> = NIOLockedValueBox(0),
        writeRaw: (@Sendable (_ stream: inout QUICStream<StreamsTestClient>) throws -> Void)? = nil
    ) async throws -> (QUICStreamHandle, EventLoopFuture<CollectedResponse>) {
        let connection = self.connection
        return try await connection.eventLoop.submit {
            let promise = connection.eventLoop.makePromise(of: CollectedResponse.self)
            let state = StreamsTestClient.RequestState(
                toSend: body,
                trailers: trailers,
                response: nil,
                interimResponses: [],
                body: ByteBuffer(),
                promise: promise,
                readGate: readGate,
                written: written,
                isComplete: false,
                writesRaw: writeRaw != nil
            )
            do {
                let (handle, _) = try connection.value.openRequest(state: state) { request, stream in
                    if let writeRaw {
                        try writeRaw(&stream)
                    } else {
                        try request.writeRequestHead(head, to: &stream)
                        StreamsTestClient.drive(&request, stream: &stream)
                    }
                }
                return (handle, promise.futureResult)
            } catch {
                promise.fail(error)
                throw error
            }
        }.get()
    }

    func request(
        _ head: HTTPRequest,
        body: ByteBuffer = ByteBuffer(),
        trailers: HTTPFields? = nil
    ) async throws -> CollectedResponse {
        try await self.start(head, body: body, trailers: trailers).1.get(timeout: .seconds(30))
    }

    /// Carries on with a request outside of a visit, e.g. after its read gate opened.
    func resume(_ handle: QUICStreamHandle) async throws {
        let connection = self.connection
        try await connection.eventLoop.submit {
            _ = try connection.value.withRequest(handle) { request, stream in
                StreamsTestClient.drive(&request, stream: &stream)
            }
        }.get()
    }

    func close() async throws {
        try await self.udpChannel.close()
    }
}

// MARK: - Channel client

/// Collects the response on a request stream channel.
final class ResponseCollector: ChannelInboundHandler {
    typealias InboundIn = HTTPResponsePart

    private let promise: EventLoopPromise<CollectedResponse>
    private var head: HTTPResponse?
    private var body = ByteBuffer()

    init(promise: EventLoopPromise<CollectedResponse>) {
        self.promise = promise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch self.unwrapInboundIn(data) {
        case .head(let head):
            if head.status.kind != .informational {
                self.head = head
            }
        case .body(let body):
            self.body.writeImmutableBuffer(body)
        case .end(let trailers):
            if let head = self.head {
                self.promise.succeed(CollectedResponse(head: head, body: self.body, trailers: trailers))
            } else {
                self.promise.fail(IncompleteResponse(cause: nil))
            }
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.promise.fail(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.promise.fail(IncompleteResponse(cause: nil))
        context.fireChannelInactive()
    }
}

/// A client on the `Channel` based API of `NIOHTTP3`, connected to a server.
@available(anyAppleOS 27, *)
struct ChannelClient: Sendable {
    let udpChannel: any Channel
    let connectionChannel: any Channel

    static func connect(
        to address: SocketAddress,
        credentials: Credentials,
        group: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) async throws -> ChannelClient {
        let (quicConfiguration, serverName) = credentials.clientConfiguration
        let udpChannel = try await DatagramBootstrap(group: group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .bind(host: "127.0.0.1", port: 0)
            .get()
        let multiplexer = try await udpChannel.eventLoop.submit {
            try udpChannel.pipeline.syncOperations.configureHTTP3Client(
                channel: udpChannel,
                quicConfiguration: quicConfiguration,
                logger: Logger(label: "ChannelClient")
            )
        }.get()
        let connectionChannel = try await multiplexer.createConnectionChannel(
            serverName: serverName,
            remoteAddress: address,
            connectionInitializer: nil
        ).get()
        return ChannelClient(udpChannel: udpChannel, connectionChannel: connectionChannel)
    }

    func request(
        _ head: HTTPRequest,
        body: ByteBuffer = ByteBuffer(),
        trailers: HTTPFields? = nil
    ) async throws -> CollectedResponse {
        let promise = self.connectionChannel.eventLoop.makePromise(of: CollectedResponse.self)
        let streamChannel = try await self.connectionChannel.pipeline.handler(type: QUICHTTP3ConnectionHandler.self)
            .flatMap { handler in
                handler.createRequestStream { parameters in
                    parameters.channel.eventLoop.makeCompletedFuture {
                        try parameters.channel.pipeline.syncOperations.addHandler(ResponseCollector(promise: promise))
                        return parameters.channel
                    }
                }
            }
            .get()

        streamChannel.write(HTTPRequestPart.head(head), promise: nil)
        var body = body
        while let chunk = body.readSlice(length: min(body.readableBytes, 64 * 1024)), chunk.readableBytes > 0 {
            streamChannel.write(HTTPRequestPart.body(chunk), promise: nil)
        }
        streamChannel.writeAndFlush(HTTPRequestPart.end(trailers), promise: nil)
        return try await promise.futureResult.get(timeout: .seconds(30))
    }

    func close() async throws {
        try? await self.connectionChannel.close()
        try await self.udpChannel.close()
    }
}

// MARK: - Configuration

@available(anyAppleOS 27, *)
extension Credentials {
    var serverConfiguration: QUICConfiguration {
        switch self {
        case .rawKeys(let serverName, let publicKeyPath, let privateKeyPath, _):
            .makeH3ServerConfig(serverName: serverName, publicKeyPath: publicKeyPath, privateKeyPath: privateKeyPath)
        case .certificates(_, let certPath, let keyPath, _):
            .makeH3ServerConfig(certPath: certPath, keyPath: keyPath)
        }
    }

    var clientConfiguration: (QUICConfiguration, serverName: String) {
        switch self {
        case .rawKeys(let serverName, let publicKeyPath, _, _):
            (.makeH3ClientConfig(publicKeyPath: publicKeyPath), serverName)
        case .certificates(let serverName, _, _, let trustRootsPath):
            (.makeH3ClientConfig(trustedRootsPath: trustRootsPath), serverName)
        }
    }
}

extension HTTPRequest {
    static func post(_ path: String = "/echo") -> HTTPRequest {
        HTTPRequest(method: .post, scheme: "https", authority: "localhost", path: path)
    }

    static func get(_ path: String = "/") -> HTTPRequest {
        HTTPRequest(method: .get, scheme: "https", authority: "localhost", path: path)
    }
}

extension ByteBuffer {
    /// Bytes which are different at each offset, so a misordered echo doesn't match.
    static func pattern(count: Int, seed: UInt8 = 0) -> ByteBuffer {
        var buffer = ByteBuffer()
        buffer.reserveCapacity(count)
        for index in 0..<count {
            buffer.writeInteger(UInt8(truncatingIfNeeded: index &* 31 &+ Int(seed)))
        }
        return buffer
    }
}

extension EventLoopFuture where Value: Sendable {
    /// Waits for the future, failing if it doesn't complete within `timeout`: futures aren't cancelled with the
    /// task waiting for them, so a test which hangs would never reach its time limit.
    func get(timeout: TimeAmount) async throws -> Value {
        let promise = self.eventLoop.makePromise(of: Value.self)
        let timeoutTask = self.eventLoop.scheduleTask(in: timeout) {
            promise.fail(TimedOut())
        }
        self.cascade(to: promise)
        promise.futureResult.whenComplete { _ in timeoutTask.cancel() }
        return try await promise.futureResult.get()
    }
}

struct TimedOut: Error {}
