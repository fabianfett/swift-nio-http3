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

public import NIOCore
public import NIOQUIC
public import NIOQUICHelpers

/// Configuration for an HTTP/3 connection whose streams are served by an ``HTTP3ServerConsumer`` or an
/// ``HTTP3ClientConsumer``.
@available(anyAppleOS 27, *)
public struct HTTP3StreamsConfiguration: Sendable {
    /// Whether to Huffman encode field values where that is shorter.
    public var preferHuffmanEncoding: Bool

    /// The longest HEADERS frame payload accepted from the peer. A longer one is a connection error of type
    /// H3\_EXCESSIVE\_LOAD.
    ///
    /// A HEADERS frame is only parsed once it is complete, so this bounds how many bytes of one stream are
    /// held at once.
    public var maximumHeadersFrameLength: Int

    /// How long a server waits between its first and second GOAWAY during graceful shutdown, so that requests
    /// which were in flight when it started can still arrive (RFC 9114 § 5.2).
    public var gracefulShutdownDelay: TimeAmount

    /// The largest body part which is copied into the DATA frame alongside its header. Larger ones are sent
    /// without copying, as a separate buffer.
    public var maximumCopiedBodyLength: Int

    /// How many bytes ``HTTP3StreamState/writableBytes(_:)`` offers, once per visit, a stream which says it
    /// has no room.
    ///
    /// A QUIC stream's room is advisory, and a stream which stopped because it had none can't count on being
    /// told when it has some again: the stack only signals `writable` to a stream which queued more than it
    /// had room for, and the room it reports is a snapshot which may not change until then. So a stream is
    /// allowed to queue this much when it has no room, which keeps it moving.
    public var minimumWritableBytes: Int

    public init() {
        self.preferHuffmanEncoding = true
        self.maximumHeadersFrameLength = 256 * 1024
        self.gracefulShutdownDelay = .milliseconds(100)
        self.maximumCopiedBodyLength = 4096
        self.minimumWritableBytes = 16 * 1024
    }
}

/// The HTTP/3 layer's own state for a stream.
@usableFromInline
@available(anyAppleOS 27, *)
struct HTTP3StreamCore {
    @usableFromInline
    enum Kind: UInt8 {
        /// A request stream the user sees.
        case request
        /// A request stream the peer opened, which this endpoint refused. Its bytes are discarded.
        case rejected
        /// This endpoint's control stream.
        case outboundControl
        /// A unidirectional stream the peer opened, whose type hasn't been read yet.
        case inboundUnidirectional
        case inboundControl
        case inboundQPACKEncoder
        case inboundQPACKDecoder
        /// A stream whose bytes are discarded.
        case discarded
    }

    @usableFromInline
    var kind: Kind

    @usableFromInline
    let context: HTTP3ConnectionContext

    @usableFromInline
    var reader: HTTP3MessageReader

    @usableFromInline
    var writer: HTTP3MessageWriter

    /// The events of the current visit.
    @usableFromInline
    var events: QUICStreamEvents

    /// Whether the stream was counted as an open request stream by the connection.
    @usableFromInline
    var isTracked: Bool

    /// The shortest run of contiguous bytes a unidirectional stream's type needs.
    @usableFromInline
    var streamTypeNeed: UInt8

    /// A STOP\_SENDING to send once the current read is over.
    @usableFromInline
    var pendingStopSending: QUICApplicationErrorCode?

    @usableFromInline
    init(kind: Kind, context: HTTP3ConnectionContext) {
        self.kind = kind
        self.context = context
        self.reader = HTTP3MessageReader(
            isServer: context.isServer,
            maximumHeadersLength: context.configuration.maximumHeadersFrameLength
        )
        self.writer = HTTP3MessageWriter(isServer: context.isServer)
        self.events = []
        self.isTracked = false
        self.streamTypeNeed = 1
        self.pendingStopSending = nil
    }
}

/// The state of one HTTP/3 stream: the HTTP/3 layer's own, and the user's state for requests.
///
/// This is the `StreamState` of every ``HTTP3ServerConsumer`` and ``HTTP3ClientConsumer``. For a request
/// stream, it is how the request is read and written: see ``readRequestPart(from:body:)`` and
/// ``writeResponseHead(_:to:)`` for servers, and ``writeRequestHead(_:to:)`` and
/// ``readResponsePart(from:body:)`` for clients.
///
/// Reading and writing take the `QUICStream` the state belongs to, which the consumer is handed alongside it.
/// Write to that stream only through this type: it frames what's written.
@available(anyAppleOS 27, *)
public struct HTTP3StreamState<RequestState> {
    @usableFromInline
    var core: HTTP3StreamCore

    @usableFromInline
    var _state: RequestState?

    @inlinable
    init(core: HTTP3StreamCore, state: RequestState?) {
        self.core = core
        self._state = state
    }

    /// The user's state for this request.
    @inlinable
    public var state: RequestState {
        _read {
            yield self._state!
        }
        _modify {
            yield &self._state!
        }
    }

    /// The events which happened to the stream since it was last visited.
    @inlinable
    public var events: QUICStreamEvents {
        self.core.events
    }

    /// Whether the whole inbound message has been read.
    @inlinable
    public var isInboundComplete: Bool {
        self.core.reader.isComplete
    }

    /// Whether the end of the outbound message has been written.
    @inlinable
    public var isOutboundComplete: Bool {
        self.core.writer.hasEnded
    }
}

@available(anyAppleOS 27, *)
@available(*, unavailable)
extension HTTP3StreamState: Sendable {}

@available(anyAppleOS 27, *)
@available(*, unavailable)
extension HTTP3StreamCore: Sendable {}
