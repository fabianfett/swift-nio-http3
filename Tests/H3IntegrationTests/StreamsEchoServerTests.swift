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
import NIOHTTP3
import NIOPosix
import NIOQUIC
import NIOQUICHelpers
import Testing

@Suite(.timeLimit(.minutes(2)))
struct StreamsEchoServerTests {
    private static let authenticationConfigurations: [AuthenticationConfiguration] = [.keys, .certs]

    // MARK: Lightweight server, channel client

    @Test(arguments: Self.authenticationConfigurations)
    @available(anyAppleOS 27, *)
    func lightweightServerEchoesToChannelClient(authenticationConfiguration: AuthenticationConfiguration) async throws {
        try await withEchoServer(.lightweight, authenticationConfiguration) { address, credentials in
            let client = try await ChannelClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            // No body: the response is a head only.
            let get = try await client.request(.get())
            #expect(get.head.status == .ok)
            #expect(get.body.readableBytes == 0)
            #expect(get.trailers == nil)

            // A body is echoed.
            let post = try await client.request(.post(), body: ByteBuffer(string: "Hello, HTTP/3!"))
            #expect(post.head.status == .ok)
            #expect(post.body == ByteBuffer(string: "Hello, HTTP/3!"))

            // So are trailers.
            let trailers: HTTPFields = [HTTPField.Name("x-checksum")!: "1234"]
            let withTrailers = try await client.request(.post(), body: ByteBuffer(string: "abc"), trailers: trailers)
            #expect(withTrailers.body == ByteBuffer(string: "abc"))
            #expect(withTrailers.trailers == trailers)
        }
    }

    @Test
    @available(anyAppleOS 27, *)
    func lightweightServerEchoesLargeBodyToChannelClient() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await ChannelClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            // Several times the stream's flow control window (1 MB), so it can only get through if the server
            // keeps reading as the client's credit is replenished.
            let body = ByteBuffer.pattern(count: 6 * 1024 * 1024)
            let response = try await client.request(.post(), body: body)
            #expect(response.head.status == .ok)
            #expect(response.body.readableBytes == body.readableBytes)
            #expect(response.body == body)
        }
    }

    @Test
    @available(anyAppleOS 27, *)
    func lightweightServerHandlesConcurrentChannelClientRequests() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await ChannelClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            // More requests than the server allows streams (100): the later ones wait for credit.
            try await withThrowingTaskGroup(of: Void.self) { group in
                for index in 0..<250 {
                    group.addTask {
                        let body = ByteBuffer.pattern(count: 100 + index * 37, seed: UInt8(truncatingIfNeeded: index))
                        let response = try await client.request(.post("/\(index)"), body: body)
                        #expect(response.head.status == .ok)
                        #expect(response.body == body)
                    }
                }
                try await group.waitForAll()
            }
        }
    }

    // MARK: Lightweight client, channel server

    @Test(arguments: Self.authenticationConfigurations)
    @available(anyAppleOS 27, *)
    func lightweightClientTalksToChannelServer(authenticationConfiguration: AuthenticationConfiguration) async throws {
        try await withEchoServer(.channel, authenticationConfiguration) { address, credentials in
            let client = try await StreamsClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            let get = try await client.request(.get())
            #expect(get.head.status == .ok)
            #expect(get.body.readableBytes == 0)

            let post = try await client.request(.post(), body: ByteBuffer(string: "Hello from the lightweight client"))
            #expect(post.body == ByteBuffer(string: "Hello from the lightweight client"))

            let trailers: HTTPFields = [HTTPField.Name("x-checksum")!: "5678"]
            let withTrailers = try await client.request(.post(), body: ByteBuffer(string: "xyz"), trailers: trailers)
            #expect(withTrailers.body == ByteBuffer(string: "xyz"))
            #expect(withTrailers.trailers == trailers)

            let body = ByteBuffer.pattern(count: 4 * 1024 * 1024)
            let large = try await client.request(.post(), body: body)
            #expect(large.body == body)

            try await withThrowingTaskGroup(of: Void.self) { group in
                for index in 0..<250 {
                    group.addTask {
                        let body = ByteBuffer.pattern(count: 50 + index * 13, seed: UInt8(truncatingIfNeeded: index))
                        let response = try await client.request(.post("/\(index)"), body: body)
                        #expect(response.body == body)
                    }
                }
                try await group.waitForAll()
            }
        }
    }

    // MARK: Lightweight client, lightweight server

    @Test
    @available(anyAppleOS 27, *)
    func lightweightClientTalksToLightweightServer() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await StreamsClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            let get = try await client.request(.get())
            #expect(get.head.status == .ok)

            let body = ByteBuffer.pattern(count: 4 * 1024 * 1024 + 17)
            let large = try await client.request(.post(), body: body)
            #expect(large.body == body)

            try await withThrowingTaskGroup(of: Void.self) { group in
                for index in 0..<250 {
                    group.addTask {
                        let body = ByteBuffer.pattern(count: index * 101, seed: UInt8(truncatingIfNeeded: index))
                        let response = try await client.request(.post("/\(index)"), body: body)
                        #expect(response.body == body)
                    }
                }
                try await group.waitForAll()
            }
        }
    }

    // MARK: Back pressure

    /// A server which stops reading a request body holds the client to the stream's flow control window: the
    /// bytes it doesn't take stay in the QUIC stream, which doesn't give the client more credit. Once it reads
    /// again, the rest of the body arrives.
    @Test(arguments: [false, true])
    @available(anyAppleOS 27, *)
    func readBackPressureHoldsTheClientToTheFlowControlWindow(lightweightClient: Bool) async throws {
        let credentials = try TestCertificates.makeCredentials(for: .keys)
        let paused = NIOLockedValueBox(true)
        let pausedRequests = NIOLockedValueBox<[QUICStreamHandle]>([])
        let serverConnection = NIOLockedValueBox<NIOLoopBound<HTTP3Connection<PausingServer>>?>(nil)
        let receivedBytes = NIOLockedValueBox(0)

        let serverChannel = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                try channel.pipeline.syncOperations.addHandler(DatagramByteCounter(count: receivedBytes))
                let handler = QUICHandler<PausingServer>(
                    channel: channel,
                    quicConfiguration: credentials.serverConfiguration,
                    asyncVerifier: nil,
                    authenticator: nil,
                    logger: Logger(label: "Server")
                ) { connection in
                    let http3 = HTTP3Connection<PausingServer>.server(connection)
                    serverConnection.withLockedValue { $0 = NIOLoopBound(http3, eventLoop: connection.eventLoop) }
                    return PausingServer(http3Connection: http3, paused: paused, pausedRequests: pausedRequests)
                }
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
        defer { Task { try? await serverChannel.close() } }

        // Eight times the stream's flow control window of 1 MB.
        let bodyLength = 8 * 1_000_000
        let body = ByteBuffer.pattern(count: bodyLength)
        let response: EventLoopFuture<CollectedResponse>
        let closeClient: @Sendable () async throws -> Void
        if lightweightClient {
            let client = try await StreamsClient.connect(to: serverChannel.localAddress!, credentials: credentials)
            response = try await client.start(.post(), body: body).1
            closeClient = { try await client.close() }
        } else {
            let client = try await ChannelClient.connect(to: serverChannel.localAddress!, credentials: credentials)
            let promise = MultiThreadedEventLoopGroup.singleton.any().makePromise(of: CollectedResponse.self)
            promise.completeWithTask { try await client.request(.post(), body: body) }
            response = promise.futureResult
            closeClient = { try await client.close() }
        }

        // While the server doesn't read, the client can't send much more than the stream's window: wait until
        // it has sent all it's allowed to.
        var plateau = -1
        for _ in 0..<50 {
            try await Task.sleep(for: .milliseconds(100))
            let now = receivedBytes.withLockedValue { $0 }
            if now == plateau { break }
            plateau = now
        }
        #expect(plateau > 0)
        #expect(plateau < 2 * 1_000_000, "The server received \(plateau) bytes without reading them")

        // Resume out of band: the rest of the body follows.
        let server = try #require(serverConnection.withLockedValue { $0 })
        let handle = try #require(pausedRequests.withLockedValue { $0.first })
        paused.withLockedValue { $0 = false }
        _ = try await server.eventLoop.submit {
            try server.value.withRequest(handle) { request, stream in
                PausingServer.drive(&request, stream: &stream)
            }
        }.get()

        let collected = try await response.get(timeout: .seconds(30))
        #expect(collected.body == ByteBuffer(string: "\(bodyLength)"))
        #expect(receivedBytes.withLockedValue { $0 } > bodyLength)
        try await closeClient()
    }

    /// The client stops reading the response: the echo server should stop reading the request, as it can't
    /// send it back, which should then stop the client sending.
    ///
    /// The API supports this, but the QUIC stack doesn't yet provide what it needs: a stream's room is a
    /// snapshot which doesn't shrink as it's written to, and the stack never signals `writable`
    /// (`ManyToManyApplicationStreamProtocol.unblockSending(flow:)`, which raises it, has no callers). So the
    /// server keeps writing into the stack, and the client isn't held back.
    @Test
    @available(anyAppleOS 27, *)
    func writeBackPressurePropagatesThroughTheEchoServer() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await StreamsClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            let body = ByteBuffer.pattern(count: 16 * 1024 * 1024)
            let readGate = NIOLockedValueBox(true)
            let written = NIOLockedValueBox(0)
            let (handle, response) = try await client.start(.post(), body: body, readGate: readGate, written: written)

            // Wait until the client can't write any more.
            var stalledAt = -1
            for _ in 0..<100 {
                try await Task.sleep(for: .milliseconds(50))
                let now = written.withLockedValue { $0 }
                if now == stalledAt { break }
                stalledAt = now
            }

            withKnownIssue("SwiftNetwork doesn't report a QUIC stream's room, nor signal writable") {
                #expect(stalledAt < body.readableBytes / 2, "The client wrote \(stalledAt) bytes without being read")
            }

            // Whether or not it was held back, the whole body makes it through once the client reads.
            readGate.withLockedValue { $0 = false }
            try await client.resume(handle)

            let collected = try await response.get(timeout: .seconds(30))
            #expect(written.withLockedValue { $0 } == body.readableBytes)
            #expect(collected.body == body)
        }
    }

    // MARK: Errors

    /// RFC 9114 § 4.1: a request stream which ends before a complete request is aborted with
    /// H3_REQUEST_INCOMPLETE.
    @Test
    @available(anyAppleOS 27, *)
    func requestStreamEndingBeforeHeadIsReset() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await StreamsClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            let (_, response) = try await client.start(.get()) { stream in
                // FIN without a HEADERS frame.
                try stream.flush(fin: true)
            }

            let error = await #expect(throws: (any Error).self) { try await response.get(timeout: .seconds(30)) }
            #expect(Self.h3ErrorCode(of: error) == .requestIncomplete)

            // The connection is fine.
            let get = try await client.request(.get())
            #expect(get.head.status == .ok)
        }
    }

    /// RFC 9114 § 4.1.2: a malformed request is a stream error of type H3_MESSAGE_ERROR.
    @Test
    @available(anyAppleOS 27, *)
    func malformedRequestIsAStreamError() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await StreamsClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            // An https request without a path.
            let malformed = HTTPRequest(method: .get, scheme: "https", authority: "localhost", path: nil)
            let error = await #expect(throws: (any Error).self) { try await client.request(malformed) }
            #expect(Self.h3ErrorCode(of: error) == .messageError)

            let get = try await client.request(.get())
            #expect(get.head.status == .ok)
        }
    }

    /// RFC 9114 § 7.2.8: frame types which were used by HTTP/2 are a connection error of type
    /// H3_FRAME_UNEXPECTED.
    @Test
    @available(anyAppleOS 27, *)
    func reservedFrameTypeClosesTheConnection() async throws {
        try await withEchoServer(.lightweight, .keys) { address, credentials in
            let client = try await StreamsClient.connect(to: address, credentials: credentials)
            defer { Task { try? await client.close() } }

            let (_, response) = try await client.start(.get()) { stream in
                // A PING frame (0x06) of length 0.
                stream.write(ByteBuffer(bytes: [0x06, 0x00]))
                try stream.flush(fin: true)
            }

            await #expect(throws: (any Error).self) { try await response.get(timeout: .seconds(30)) }
            try await client.closeFuture.get(timeout: .seconds(30))
        }
    }

    // MARK: Graceful shutdown

    /// The server sends GOAWAY while a request is in flight: that request completes, the client can't open new
    /// ones, and the connection closes once it's idle.
    @Test
    @available(anyAppleOS 27, *)
    func gracefulShutdownFinishesRequestsInFlight() async throws {
        let credentials = try TestCertificates.makeCredentials(for: .keys)
        let serverConnection = NIOLockedValueBox<NIOLoopBound<HTTP3Connection<LightweightEchoServer>>?>(nil)

        let serverChannel = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "127.0.0.1", port: 0)
            .flatMapThrowing { channel in
                let handler = QUICHandler<LightweightEchoServer>(
                    channel: channel,
                    quicConfiguration: credentials.serverConfiguration,
                    asyncVerifier: nil,
                    authenticator: nil,
                    logger: Logger(label: "Server")
                ) { connection in
                    let http3 = HTTP3Connection<LightweightEchoServer>.server(connection)
                    serverConnection.withLockedValue { $0 = NIOLoopBound(http3, eventLoop: connection.eventLoop) }
                    return LightweightEchoServer(http3Connection: http3)
                }
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()
        defer { Task { try? await serverChannel.close() } }

        let client = try await StreamsClient.connect(to: serverChannel.localAddress!, credentials: credentials)
        defer { Task { try? await client.close() } }

        // Complete one request so each side has the other's SETTINGS.
        _ = try await client.request(.get())
        let server = try #require(serverConnection.withLockedValue { $0 })
        let serverSawSettings = try await server.eventLoop.submit {
            server.value.peerSettings != nil
        }.get()
        #expect(serverSawSettings)

        // A request whose body is still being written when the server shuts down.
        let readGate = NIOLockedValueBox(false)
        let body = ByteBuffer.pattern(count: 3 * 1024 * 1024)
        let (_, inFlight) = try await client.start(.post(), body: body, readGate: readGate)

        server.eventLoop.execute {
            server.value.initiateGracefulShutdown()
        }

        let response = try await inFlight.get(timeout: .seconds(30))
        #expect(response.body == body)

        // The connection closes once the in-flight request is done and the final GOAWAY was sent.
        try await client.closeFuture.get(timeout: .seconds(30))

        let afterGoaway = await #expect(throws: (any Error).self) { try await client.request(.get()) }
        #expect(afterGoaway != nil)
    }

    // MARK: Helpers

    @available(anyAppleOS 27, *)
    private func withEchoServer(
        _ api: EchoServerAPI,
        _ authenticationConfiguration: AuthenticationConfiguration,
        _ body: (_ address: SocketAddress, _ credentials: Credentials) async throws -> Void
    ) async throws {
        let credentials = try TestCertificates.makeCredentials(for: authenticationConfiguration)
        let server = try await EchoServer.bind(
            api: api,
            host: "127.0.0.1",
            port: 0,
            quicConfiguration: credentials.serverConfiguration,
            logger: Logger(label: "EchoServer")
        )

        do {
            try await body(server.localAddress!, credentials)
        } catch {
            try? await server.close()
            throw error
        }
        try await server.close()
    }

    @available(anyAppleOS 27, *)
    private static func h3ErrorCode(of error: (any Error)?) -> HTTP3ErrorCode? {
        switch error {
        case let error as HTTP3Error:
            return error.h3ErrorCode
        case let error as IncompleteResponse:
            return Self.h3ErrorCode(of: error.cause)
        default:
            return nil
        }
    }
}

/// A server which doesn't read request bodies while it's paused, and responds with the number of body bytes it
/// read.
@available(anyAppleOS 27, *)
struct PausingServer: HTTP3ServerConsumer {
    struct RequestState {
        var consumed: Int
        var isPaused: NIOLockedValueBox<Bool>
    }

    let http3Connection: HTTP3Connection<Self>
    let paused: NIOLockedValueBox<Bool>
    let pausedRequests: NIOLockedValueBox<[QUICStreamHandle]>

    func makeRequestState() -> RequestState {
        RequestState(consumed: 0, isPaused: self.paused)
    }

    func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        if self.paused.withLockedValue({ $0 }) {
            let handle = stream.handle
            self.pausedRequests.withLockedValue { requests in
                if !requests.contains(handle) {
                    requests.append(handle)
                }
            }
        }
        Self.drive(&request, stream: &stream)
    }

    static func drive(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        if !request.state.isPaused.withLockedValue({ $0 }) {
            request.resumeReading()
        }
        do {
            while let part = try request.readRequestPart(
                from: &stream,
                body: { bytes, state in
                    if state.isPaused.withLockedValue({ $0 }) {
                        return 0
                    }
                    state.consumed += bytes.byteCount
                    return bytes.byteCount
                }
            ) {
                switch part {
                case .head, .body:
                    ()
                case .end:
                    try request.writeResponse(
                        HTTPResponse(status: .ok),
                        body: ByteBuffer(string: "\(request.state.consumed)"),
                        to: &stream
                    )
                }
            }
        } catch {
            request.cancel(stream: &stream)
        }
    }
}

/// Counts the bytes of the datagrams a UDP channel receives.
final class DatagramByteCounter: ChannelInboundHandler, Sendable {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias InboundOut = AddressedEnvelope<ByteBuffer>

    let count: NIOLockedValueBox<Int>

    init(count: NIOLockedValueBox<Int>) {
        self.count = count
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let length = self.unwrapInboundIn(data).data.readableBytes
        self.count.withLockedValue { $0 += length }
        context.fireChannelRead(data)
    }
}
