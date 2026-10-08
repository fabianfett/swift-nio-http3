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

public import HTTP3
public import Logging
public import NIOCore
public import NIOQUIC
public import NIOQUICHelpers

/// Holds the QUIC connection for as long as it's open.
///
/// The QUIC connection's channel owns the consumer, which owns this: the reference is dropped when the
/// channel closes to break the cycle.
@usableFromInline
@available(anyAppleOS 27, *)
final class HTTP3QUICConnectionBox<Consumer: HTTP3Consumer> {
    @usableFromInline
    var connection: QUICStreamConnection<Consumer>?

    init(_ connection: QUICStreamConnection<Consumer>) {
        self.connection = connection
    }
}

/// An HTTP/3 connection whose streams are served by a consumer rather than by a `Channel` each.
///
/// Create it in the `makeConsumer` closure of the `QUICHandler` and store it in the consumer, which vends it
/// as ``HTTP3Consumer/http3Connection``:
///
/// ```swift
/// QUICHandler<EchoServer>(
///     channel: channel,
///     quicConfiguration: configuration,
///     asyncVerifier: nil,
///     authenticator: authenticator,
///     logger: logger
/// ) { connection in
///     EchoServer(http3Connection: .server(connection))
/// }
/// ```
///
/// Use it on the connection's event loop to reach the connection's requests outside of a visit: to open requests,
/// or to continue one which is waiting for something other than the peer. It isn't `Sendable`: to use it from
/// elsewhere, hop to its ``eventLoop`` first, for example with an `NIOLoopBound`.
@available(anyAppleOS 27, *)
public struct HTTP3Connection<Consumer: HTTP3Consumer> {
    @usableFromInline
    let context: HTTP3ConnectionContext

    @usableFromInline
    let box: HTTP3QUICConnectionBox<Consumer>

    /// The event loop the connection is on.
    public let eventLoop: any EventLoop

    /// The closure of the connection channel.
    public let closeFuture: EventLoopFuture<Void>

    private init(
        _ connection: QUICStreamConnection<Consumer>,
        isServer: Bool,
        settings: HTTP3Settings,
        configuration: HTTP3StreamsConfiguration,
        logger: Logger
    ) {
        connection.eventLoop.preconditionInEventLoop()
        self.box = HTTP3QUICConnectionBox(connection)
        self.context = HTTP3ConnectionContext(
            isServer: isServer,
            settings: settings,
            configuration: configuration,
            channel: connection.channel,
            logger: logger
        )
        self.eventLoop = connection.eventLoop
        self.closeFuture = connection.channel.closeFuture

        let handler = HTTP3ConnectionEventsHandler(context: self.context, box: self.box)
        do {
            try connection.channel.pipeline.syncOperations.addHandler(handler)
        } catch {
            // The channel was only just created, so its pipeline can't be closed yet.
            preconditionFailure("Can't add the HTTP/3 connection handler: \(error)")
        }
    }

    /// Serves the server side of an HTTP/3 connection.
    ///
    /// - Parameters:
    ///   - connection: The QUIC connection, as passed to `makeConsumer`.
    ///   - settings: The SETTINGS to send. The QPACK dynamic table isn't supported.
    ///   - configuration: The HTTP/3 layer's configuration.
    ///   - logger: For the connection's events.
    /// - Returns: The connection, which the consumer vends as ``HTTP3Consumer/http3Connection``.
    public static func server(
        _ connection: QUICStreamConnection<Consumer>,
        settings: HTTP3Settings = HTTP3Settings(),
        configuration: HTTP3StreamsConfiguration = HTTP3StreamsConfiguration(),
        logger: Logger = Logger(label: "HTTP3", factory: { _ in SwiftLogNoOpLogHandler() })
    ) -> Self where Consumer: HTTP3ServerConsumer {
        Self(connection, isServer: true, settings: settings, configuration: configuration, logger: logger)
    }

    /// Serves the client side of an HTTP/3 connection.
    ///
    /// - Parameters:
    ///   - connection: The QUIC connection, as passed to `makeConsumer`.
    ///   - settings: The SETTINGS to send. The QPACK dynamic table isn't supported.
    ///   - configuration: The HTTP/3 layer's configuration.
    ///   - logger: For the connection's events.
    /// - Returns: The connection, which the consumer vends as ``HTTP3Consumer/http3Connection``.
    public static func client(
        _ connection: QUICStreamConnection<Consumer>,
        settings: HTTP3Settings = HTTP3Settings(),
        configuration: HTTP3StreamsConfiguration = HTTP3StreamsConfiguration(),
        logger: Logger = Logger(label: "HTTP3", factory: { _ in SwiftLogNoOpLogHandler() })
    ) -> Self where Consumer: HTTP3ClientConsumer {
        Self(connection, isServer: false, settings: settings, configuration: configuration, logger: logger)
    }

    /// The HTTP/3 connection which was created for `connection` in `makeConsumer`, or `nil` if there is none.
    ///
    /// This is how a client gets hold of the HTTP/3 connection for the QUIC connection that
    /// `QUICHandler.createOutboundConnection(serverName:remoteAddress:)` returned.
    ///
    /// - Precondition: Called on the connection's event loop.
    public init?(configuredOn connection: QUICStreamConnection<Consumer>) {
        connection.eventLoop.preconditionInEventLoop()
        guard
            let handler = try? connection.channel.pipeline.syncOperations.handler(
                type: HTTP3ConnectionEventsHandler<Consumer>.self
            )
        else {
            return nil
        }
        self.init(context: handler.context, box: handler.box, channel: connection.channel)
    }
}

@available(anyAppleOS 27, *)
@available(*, unavailable)
extension HTTP3Connection: Sendable {}

@available(anyAppleOS 27, *)
extension HTTP3Connection {
    /// The SETTINGS the peer sent, or `nil` if it hasn't yet.
    ///
    /// - Precondition: Called on the connection's event loop.
    public var peerSettings: HTTP3Settings? {
        self.eventLoop.assertInEventLoop()
        return self.context.peerSettings
    }

    /// Closes the connection straight away.
    ///
    /// - Precondition: Called on the connection's event loop.
    public func close() -> EventLoopFuture<Void> {
        self.eventLoop.assertInEventLoop()
        guard let channel = self.box.connection?.channel else {
            return self.eventLoop.makeSucceededVoidFuture()
        }
        return channel.close()
    }

    /// Opens a request stream (clients only) and runs `body` on it, to write the request.
    ///
    /// The request may be written straight away. It's sent once the stack assigned the stream an ID,
    /// which may be later if the server's stream limit was reached. The response is read in
    /// ``HTTP3Consumer/processRequest(_:stream:)``.
    ///
    /// - Parameters:
    ///   - state: The request's state.
    ///   - body: Handed the request and its stream.
    /// - Returns: The handle of the new stream and what `body` returned.
    /// - Throws: If the request can't be opened, for example because the server sent GOAWAY or the
    ///   connection closed, or what `body` threw.
    /// - Precondition: Called on the connection's event loop.
    @inlinable
    public func openRequest<Result>(
        state: Consumer.RequestState,
        _ body: (_ request: inout HTTP3StreamState<Consumer.RequestState>, _ stream: inout QUICStream<Consumer>)
            throws -> Result
    ) throws -> (handle: QUICStreamHandle, result: Result) where Consumer: HTTP3ClientConsumer {
        self.eventLoop.assertInEventLoop()
        let context = self.context
        try context.outboundRequestStreamRequested()

        guard let quic = self.box.connection else {
            throw ChannelError.ioOnClosedChannel
        }

        let opened = try quic.assumeIsolated().withStreams { streams throws -> (QUICStreamHandle, Result) in
            let handle = try streams.open(
                .clientInitiatedBidirectional,
                state: HTTP3StreamState(core: HTTP3StreamCore(kind: .request, context: context), state: state)
            )
            let result = try streams.withStream(handle: handle) { stream, request in
                defer { request._sendPendingWrites(&stream) }
                return try body(&request, &stream)
            }
            // The stream was only just opened, so it's there.
            return (handle, result!)
        }

        guard let opened else {
            throw ChannelError.ioOnClosedChannel
        }
        return opened
    }

    /// Runs `body` on a request stream outside of a visit.
    ///
    /// Use this to continue a request which was waiting for something other than the peer, for example to
    /// read more of a request after the user took fewer bytes than it was offered.
    ///
    /// - Returns: What `body` returned, or `nil` if there is no such request (any more).
    /// - Precondition: Called on the connection's event loop.
    @inlinable
    public func withRequest<Result>(
        _ handle: QUICStreamHandle,
        _ body: (_ request: inout HTTP3StreamState<Consumer.RequestState>, _ stream: inout QUICStream<Consumer>)
            throws -> Result
    ) throws -> Result? {
        self.eventLoop.assertInEventLoop()
        guard let quic = self.box.connection else { return nil }
        let result = try quic.assumeIsolated().withStreams { streams throws -> Result? in
            try streams.withStream(handle: handle) { stream, request throws -> Result? in
                guard request.core.kind == .request else { return nil }
                defer { request._sendPendingWrites(&stream) }
                return try body(&request, &stream)
            } ?? nil
        }
        return result ?? nil
    }

    /// Starts shutting down gracefully (RFC 9114 § 5.2).
    ///
    /// A server sends a first GOAWAY which tells the client to stop opening requests, then a second one
    /// after ``HTTP3StreamsConfiguration/gracefulShutdownDelay`` which says which requests it will still
    /// process; it closes the connection once they're done. A client sends a single GOAWAY.
    ///
    /// - Precondition: Called on the connection's event loop.
    public func initiateGracefulShutdown() {
        self.eventLoop.assertInEventLoop()
        self.sendGoaway(final: false)
    }

    private func sendGoaway(final: Bool) {
        let context = self.context
        guard let quic = self.box.connection,
            let action = context.sendGoaway(final: final)
        else {
            return
        }

        _ = quic.assumeIsolated().withStreams { streams in
            if let control = action.controlStream {
                _ = streams.withStream(handle: control) { stream, _ in
                    HTTP3StreamState<Consumer.RequestState>._commitScratch(context, to: &stream)
                    try? stream.flush()
                }
            }

            for id in action.cancel {
                context.pendingCancellations.append((id, HTTP3ConnectionContext.rejectedByGoawayError))
            }
            Consumer._applyCancellations(&streams, context: context)
        }

        context.finishGoaway(closeNow: action.closeNow)

        if !final && context.isServer {
            let connection = self
            self.eventLoop.assumeIsolated().scheduleTask(
                in: context.configuration.gracefulShutdownDelay
            ) {
                connection.sendGoaway(final: true)
            }
        }
    }
}

/// Opens the control stream once the connection is up, and follows the connection's lifecycle.
@available(anyAppleOS 27, *)
final class HTTP3ConnectionEventsHandler<Consumer: HTTP3Consumer>: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    fileprivate let context: HTTP3ConnectionContext
    fileprivate let box: HTTP3QUICConnectionBox<Consumer>

    init(context: HTTP3ConnectionContext, box: HTTP3QUICConnectionBox<Consumer>) {
        self.context = context
        self.box = box
    }

    func channelActive(context: ChannelHandlerContext) {
        self.openControlStream()
        context.fireChannelActive()
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.context.connectionClosed()
        self.box.connection = nil
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        // Also covers a channel which closes without ever becoming active.
        self.context.connectionClosed()
        self.box.connection = nil
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        if let error = error as? QUICConnectionError {
            self.context.peerClosedConnection(error)
        }
        context.fireErrorCaught(error)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is ChannelShouldQuiesceEvent {
            // Graceful shutdown is HTTP/3's business: RFC 9114 § 5.2.
            self.initiateGracefulShutdown()
        }
        context.fireUserInboundEventTriggered(event)
    }

    private func initiateGracefulShutdown() {
        guard let connection = self.box.connection else { return }
        // Rebuild the handle: it's only a pair of references.
        HTTP3Connection<Consumer>(context: self.context, box: self.box, channel: connection.channel)
            .initiateGracefulShutdown()
    }

    private func openControlStream() {
        let context = self.context
        let type: QUICStreamType = context.isServer ? .serverInitiatedUnidirectional : .clientInitiatedUnidirectional

        _ = self.box.connection?.assumeIsolated().withStreams { streams in
            do {
                let handle = try streams.open(
                    type,
                    state: HTTP3StreamState<Consumer.RequestState>(
                        core: HTTP3StreamCore(kind: .outboundControl, context: context),
                        state: nil
                    )
                )
                context.outboundControlStreamCreated(handle: handle)
                _ = streams.withStream(handle: handle) { stream, _ in
                    context.encodeControlStreamPreamble()
                    HTTP3StreamState<Consumer.RequestState>._commitScratch(context, to: &stream)
                    if stream.id != nil {
                        try? stream.flush()
                    }
                }
            } catch {
                // RFC 9114 § 6.2.1: each side MUST initiate a single control stream.
                context.outboundControlStreamFailed()
            }
        }
    }
}

@available(anyAppleOS 27, *)
extension HTTP3Connection {
    fileprivate init(context: HTTP3ConnectionContext, box: HTTP3QUICConnectionBox<Consumer>, channel: any Channel) {
        self.context = context
        self.box = box
        self.eventLoop = channel.eventLoop
        self.closeFuture = channel.closeFuture
    }
}

@available(*, unavailable)
extension HTTP3ConnectionEventsHandler: Sendable {}
