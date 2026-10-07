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
public import HTTPTypes
public import NIOQUIC
public import NIOQUICHelpers

// Why `HTTP3Consumer` refines `QUICStreamConsumer`, rather than a generic
// `HTTP3Handler<Consumer: HTTP3Consumer>: QUICStreamConsumer` wrapping the user's type:
//
// The QUIC layer is reached through `any ChannelHandler`, so `QUICHandler<Consumer>` runs unspecialized and calls
// `processStreams(_:)` through the `QUICStreamConsumer` witness table. Nothing is specialized across that call: the
// witness runs as it was compiled. A conformance has exactly one witness table, compiled for the conforming type as
// written:
//
// - Refinement: the user's concrete type conforms, so the default implementations below are compiled with
//   `Self == UserType`. Everything they call (the HTTP/3 glue, `QUICStreamIterator`, `QUICStream`) is called with
//   concrete types, and is specialized and inlined.
// - Wrapper: `HTTP3Handler<C>` conforms, so its single witness table serves every `C` and its `processStreams(_:)`
//   is generic, getting `C` only as runtime metadata. Nothing below it can be specialized (there's no concrete type
//   to specialize with), and its generic callees aren't inlined either: the optimizer doesn't inline generic callees
//   into generic callers unless they're `@inline(__always)` or `@_transparent`, even if they're `@inlinable`.
//
// Measured with `EchoOverhead` (macOS, 1 server thread), the wrapper cost +0.35 µs per header-only request (+5%),
// which doubled the HTTP/3 layer's overhead on top of bare QUIC. About two thirds of it was the missed inlining, the
// rest runtime metadata lookups and copies of the stream state through value witnesses.

/// Serves the streams of an HTTP/3 connection without a `Channel` per stream.
///
/// Conform to ``HTTP3ServerConsumer`` or ``HTTP3ClientConsumer`` rather than to this protocol directly. The
/// conforming type is the `QUICStreamConsumer` of the connection: the HTTP/3 layer provides its
/// `makeStreamState(_:)` and `processStreams(_:)`, handles the control and QPACK streams itself, and hands
/// each request stream with events to ``processRequest(_:stream:)``.
///
/// Because the HTTP/3 layer's entry points are default implementations of the protocol, they are compiled
/// for the conforming type, not for an abstract one: nothing on the hot path goes through a witness table.
@available(anyAppleOS 27, *)
public protocol HTTP3Consumer: QUICStreamConsumer where StreamState == HTTP3StreamState<RequestState> {
    /// The user's state for each request stream.
    associatedtype RequestState

    /// The connection this consumer serves, as created in the `makeConsumer` closure of the `QUICHandler`.
    var http3Connection: HTTP3Connection<Self> { get }

    /// Processes a request stream which has events.
    ///
    /// Read and write the request with `request`, passing it `stream`. Anything which can't be done now, for
    /// example because the peer's flow control ran out, can be continued on a later visit (the `writable`
    /// event says when there is room again) or out of band with ``HTTP3Connection/withRequest(_:_:)``.
    ///
    /// - Parameters:
    ///   - request: The request's state.
    ///   - stream: The QUIC stream of the request.
    mutating func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>)

    /// Called once when a request stream has closed, after its final ``processRequest(_:stream:)``.
    ///
    /// - Parameters:
    ///   - request: The request's state. It is dropped once this returns.
    ///   - error: Why the stream closed, or `nil` if it closed cleanly.
    mutating func requestStreamClosed(_ request: inout HTTP3StreamState<RequestState>, error: (any Error)?)
}

/// The consumer of a server's HTTP/3 connection.
///
/// Every request stream the client opens gets its state from ``makeRequestState()`` and is then processed by
/// ``HTTP3Consumer/processRequest(_:stream:)``.
@available(anyAppleOS 27, *)
public protocol HTTP3ServerConsumer: HTTP3Consumer {
    /// Makes the state for a request stream the client opened.
    mutating func makeRequestState() -> RequestState
}

/// The consumer of a client's HTTP/3 connection.
///
/// Requests are opened with ``HTTP3Connection/openRequest(state:_:)``, which takes their state.
@available(anyAppleOS 27, *)
public protocol HTTP3ClientConsumer: HTTP3Consumer {}

@available(anyAppleOS 27, *)
extension HTTP3Consumer {
    @inlinable
    public mutating func requestStreamClosed(_ request: inout HTTP3StreamState<RequestState>, error: (any Error)?) {}
}

@available(anyAppleOS 27, *)
extension HTTP3ServerConsumer {
    @inlinable
    public mutating func makeStreamState(_ stream: inout QUICStream<Self>) -> HTTP3StreamState<RequestState> {
        let context = self.http3Connection.context
        // Streams the peer opened always have an ID.
        let id = stream.id!

        if id.type.isUnidirectional {
            return HTTP3StreamState(core: HTTP3StreamCore(kind: .inboundUnidirectional, context: context), state: nil)
        }

        if let code = context.inboundRequestStreamReceived(id: id) {
            // RFC 9114 § 4.1.1: a request the server won't process is rejected, both ways.
            stream.sendStopSending(code: code)
            stream.sendResetStream(code: code)
            return HTTP3StreamState(core: HTTP3StreamCore(kind: .rejected, context: context), state: nil)
        }

        var core = HTTP3StreamCore(kind: .request, context: context)
        core.isTracked = true
        return HTTP3StreamState(core: core, state: self.makeRequestState())
    }
}

@available(anyAppleOS 27, *)
extension HTTP3ClientConsumer {
    @inlinable
    public mutating func makeStreamState(_ stream: inout QUICStream<Self>) -> HTTP3StreamState<RequestState> {
        let context = self.http3Connection.context
        // Streams the peer opened always have an ID.
        let id = stream.id!

        if id.type.isUnidirectional {
            return HTTP3StreamState(core: HTTP3StreamCore(kind: .inboundUnidirectional, context: context), state: nil)
        }

        // RFC 9114 § 6.1: a client MUST treat a server-initiated bidirectional stream as a connection error of
        // type H3_STREAM_CREATION_ERROR.
        _ = context.inboundRequestStreamReceived(id: id)
        return HTTP3StreamState(core: HTTP3StreamCore(kind: .rejected, context: context), state: nil)
    }
}

@available(anyAppleOS 27, *)
extension HTTP3Consumer {
    @inlinable
    public mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let context = self.http3Connection.context

        while var visit = streams.next() {
            let events = visit.events

            if visit.state.core.kind == .request {
                self._processRequest(&visit, events: events, context: context)
            } else {
                Self._processInternalStream(&visit, events: events, context: context)
            }
        }
    }

    @inlinable
    mutating func _processRequest(
        _ visit: inout QUICStreamVisit<Self>,
        events: QUICStreamEvents,
        context: HTTP3ConnectionContext
    ) {
        let closed = events.contains(.closed)
        let closeError = closed ? visit.closeError : nil
        let resetCode = visit.resetCode
        let stopSendingCode = visit.stopSendingCode

        visit.withStream { stream, request in
            request.core.events = events
            request.core.writer.writtenInVisit = 0

            if let resetCode {
                request.core.reader.peerReset(code: resetCode)
            }
            if let stopSendingCode {
                request.core.writer.peerStoppedSending(code: stopSendingCode)
            }
            if events.contains(.opened), let id = stream.id {
                // Streams the peer opened were tracked when their state was made: only streams this endpoint
                // opened start being tracked now, as they have only just been assigned their ID.
                if !request.core.isTracked {
                    context.outboundRequestStreamOpened(id: id)
                    request.core.isTracked = true
                }
            }

            self.processRequest(&request, stream: &stream)
            // Whatever was written before the stream had an ID, and in this visit, goes out together.
            request._sendPendingWrites(&stream)

            guard closed else { return }

            let error: (any Error)? = closeError ?? context.connectionError
            self.requestStreamClosed(&request, error: error)
            if request.core.isTracked, let id = stream.id {
                context.requestStreamClosed(id: id)
            }
        }
    }

    @inlinable
    static func _processInternalStream(
        _ visit: inout QUICStreamVisit<Self>,
        events: QUICStreamEvents,
        context: HTTP3ConnectionContext
    ) {
        visit.withStream { stream, state in
            state.core.events = events

            switch state.core.kind {
            case .outboundControl:
                if events.contains(.opened), let id = stream.id {
                    context.outboundControlStreamOpened(id: id)
                    // SETTINGS was written when the stream was opened, but can't be sent before it had an ID.
                    do {
                        try stream.flush()
                    } catch {
                        context.outboundControlStreamFailed()
                    }
                }
                if events.contains(.stopSending) {
                    // RFC 9114 § 6.2.1: the control stream must not be closed.
                    context.outboundControlStreamFailed()
                }

            case .inboundUnidirectional, .inboundControl, .inboundQPACKEncoder, .inboundQPACKDecoder:
                guard let id = stream.id else { return }
                if events.contains(.reset) {
                    context.unidirectionalStreamReset(state.core, streamID: id)
                }

                var minimum = context.unidirectionalReadMinimum(state.core)
                reading: while true {
                    let kindBefore = state.core.kind
                    let needBefore = state.core.streamTypeNeed
                    let outcome = stream.read(minContiguousBytes: minimum) { bytes in
                        context.consumeUnidirectional(&state.core, bytes, streamID: id)
                    }
                    let typeProgressed = kindBefore != state.core.kind || needBefore != state.core.streamTypeNeed

                    switch context.unidirectionalReadCompleted(
                        &state.core,
                        outcome,
                        consumedType: typeProgressed,
                        streamID: id
                    ) {
                    case .readAgain:
                        minimum = context.unidirectionalReadMinimum(state.core)
                    case .done:
                        break reading
                    case .stopSending(let code):
                        stream.sendStopSending(code: code)
                        _ = stream.read { bytes in bytes.byteCount }
                        break reading
                    }
                }

            case .rejected, .discarded:
                // Only read to let go of the bytes, and with them the flow control credit they take.
                _ = stream.read { bytes in bytes.byteCount }

            case .request:
                preconditionFailure("Request streams are processed by the consumer")
            }
        }

        if !context.pendingCancellations.isEmpty {
            var all = visit.streams
            Self._applyCancellations(&all, context: context)
        }
    }

    /// Cancels the requests the connection decided to cancel.
    @inlinable
    static func _applyCancellations(_ streams: inout QUICStreams<Self>, context: HTTP3ConnectionContext) {
        let cancellations = context.pendingCancellations
        context.pendingCancellations.removeAll()

        for (id, error) in cancellations {
            guard let handle = streams.handle(forID: id) else { continue }
            _ = streams.withStream(handle: handle) { stream, request in
                guard request.core.kind == .request else { return }
                HTTP3StreamState<RequestState>._cancel(
                    &request.core,
                    error: error,
                    code: error.h3ErrorCode ?? .requestCancelled,
                    stream: &stream
                )
            }
        }
    }
}
