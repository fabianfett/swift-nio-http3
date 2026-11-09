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

/// Validates the order of the messages on one request stream (RFC 9114 § 4.1), in both directions.
///
/// "Inbound" is the peer's message: the request for a server, the response for a client. "Outbound" is ours. Each
/// message is a head (for a response, possibly preceded by interim responses), any number of body parts, and optional
/// trailers.
///
/// The validator works on message events rather than frames, so it doesn't care how the frames are parsed or encoded:
///
/// - Inbound, ``receivedFrame(_:)`` checks the type of the next frame before its payload is parsed, and
///   ``receivedHead(informational:)`` records the head once it has been decoded. ``receivedEnd()`` records the FIN.
/// - Outbound, the `willWrite…` methods check a part may be written, and throw if it may not.
///
/// A rejected event leaves the state as it was. The validator doesn't remember errors: the caller decides what an
/// error means for the stream, and stops using the validator once the stream failed.
@available(anyAppleOS 27, *)
@_spi(PackageInternal)
public struct HTTP3RequestStreamValidator: Sendable {
    private enum InboundState: UInt8, Sendable {
        /// Waiting for the (final) head. Interim responses keep a client here.
        case awaitingHead
        case awaitingBodyOrTrailers
        /// Trailers or the FIN ended the message. No more frames may follow.
        case ended
    }

    private enum OutboundState: UInt8, Sendable {
        /// Nothing, or only interim responses, have been written.
        case awaitingHead
        case writingBody
        case ended
    }

    /// Whether this is the server's end of the stream, which receives the request and writes the response.
    public let isServer: Bool

    private var inbound: InboundState
    private var outbound: OutboundState

    public init(isServer: Bool) {
        self.isServer = isServer
        self.inbound = .awaitingHead
        self.outbound = .awaitingHead
    }

    // MARK: Inbound

    /// What to do with the next inbound frame.
    public enum InboundFrameAction: Sendable {
        /// A HEADERS frame carrying the head (possibly an interim response): decode it, then call
        /// ``HTTP3RequestStreamValidator/receivedHead(informational:)``.
        case decodeHead
        /// A HEADERS frame carrying trailers. The inbound message is complete.
        case decodeTrailers
        /// A DATA frame.
        case body
        /// A frame of an unknown type, which is ignored (RFC 9114 § 9).
        case ignore
        /// The frame is a connection error.
        case connectionError(HTTP3Error)
        /// The frame is a stream error.
        case streamError(HTTP3Error)
    }

    /// Validates the type of the next inbound frame, before its payload is parsed.
    public mutating func receivedFrame(_ type: HTTP3FrameType) -> InboundFrameAction {
        switch type {
        case .headers:
            switch self.inbound {
            case .awaitingHead:
                if !self.isServer && self.outbound == .awaitingHead {
                    // There is no request yet, so there can't be a response.
                    return .streamError(
                        Self.responseBeforeRequestError(errorCode: .messageError, location: .here())
                    )
                }
                // The state only moves on once the head is decoded: an interim response doesn't move it.
                return .decodeHead
            case .awaitingBodyOrTrailers:
                self.inbound = .ended
                return .decodeTrailers
            case .ended:
                return .connectionError(self.unexpectedFrameError(type: type, location: .here()))
            }

        case .data:
            switch self.inbound {
            case .awaitingBodyOrTrailers:
                return .body
            case .awaitingHead, .ended:
                // RFC 9114 § 4.1: receipt of an invalid sequence of frames MUST be treated as a connection error of
                // type H3_FRAME_UNEXPECTED.
                return .connectionError(self.unexpectedFrameError(type: type, location: .here()))
            }

        case .unknown:
            // RFC 9114 § 9: frames of unknown types MUST be ignored.
            return .ignore

        case .pushPromise:
            if self.isServer {
                // RFC 9114 § 7.2.5: a client MUST NOT send a PUSH_PROMISE frame. A server MUST treat the receipt of a
                // PUSH_PROMISE frame as a connection error of type H3_FRAME_UNEXPECTED.
                return .connectionError(self.unexpectedFrameError(type: type, location: .here()))
            } else {
                // RFC 9114 § 7.2.5: a client MUST treat receipt of a PUSH_PROMISE frame that contains a larger push ID
                // than the client has advertised as a connection error of H3_ID_ERROR. We never send MAX_PUSH_ID, so
                // every push ID is larger than the one we advertised.
                return .connectionError(Self.unexpectedPushPromiseError(location: .here()))
            }

        case .settings, .goaway, .maxPushID, .cancelPush:
            // RFC 9114 § 7.2: these frames are only allowed on the control stream.
            return .connectionError(self.unexpectedFrameError(type: type, location: .here()))
        }
    }

    /// Records the decoded head announced by ``InboundFrameAction/decodeHead``.
    ///
    /// - Parameter informational: Whether the head is an interim (1xx) response, which keeps waiting for the final
    ///   head. Only a client receives responses, so a server must always pass `false`.
    public mutating func receivedHead(informational: Bool) {
        precondition(!(informational && self.isServer), "A request can't be informational")
        precondition(self.inbound == .awaitingHead, "A head was received which wasn't announced by decodeHead")
        // RFC 9114 § 4.1: a response MAY consist of multiple messages when and only when one or more interim
        // responses (1xx) precede a final response to the same request.
        if !informational {
            self.inbound = .awaitingBodyOrTrailers
        }
    }

    /// What to do when the peer finished its side of the stream.
    public enum InboundEndAction: Sendable {
        /// The inbound message was complete.
        case complete
        /// A server's request ended before its head: reset the stream with H3_REQUEST_INCOMPLETE (RFC 9114 § 4.1).
        case incompleteRequest(HTTP3Error)
        /// A client's response ended before its final head: tell the user, it must not be used (RFC 9114 § 4.1.1).
        case incompleteResponse(HTTP3Error)
    }

    /// Records that the peer finished its side of the stream (FIN).
    public mutating func receivedEnd() -> InboundEndAction {
        switch self.inbound {
        case .awaitingHead:
            if self.isServer {
                // RFC 9114 § 4.1: if a client-initiated stream terminates without enough of the HTTP message to
                // provide a complete response, the server SHOULD abort its response stream with the error code
                // H3_REQUEST_INCOMPLETE.
                return .incompleteRequest(
                    HTTP3Error(
                        code: .peerTerminatedInboundStream,
                        message: "Inbound request stream closed before a complete request was received",
                        cause: nil,
                        errorCode: .requestIncomplete,
                        location: .here()
                    )
                )
            } else {
                // RFC 9114 § 4.1.1: if a stream is cancelled after receiving a partial response, the response SHOULD
                // NOT be used. This is not a stream or connection error: it's up to the user what to do.
                return .incompleteResponse(
                    HTTP3Error(
                        code: .peerTerminatedInboundStream,
                        message: "Inbound response stream closed before a complete response was received",
                        cause: nil,
                        errorCode: nil,
                        location: .here()
                    )
                )
            }

        case .awaitingBodyOrTrailers, .ended:
            // TODO: If a Content-Length header was specified, we need to check whether we have received all the
            // specified bytes. If that is not the case, then the message is malformed per RFC 9114 § 4.1.2, and it
            // must be treated as a stream error.
            self.inbound = .ended
            return .complete
        }
    }

    // MARK: Outbound

    /// Whether no final head has been written yet, so the next HEADERS frame is a head rather than trailers.
    public var outboundAwaitsHead: Bool {
        self.outbound == .awaitingHead
    }

    /// Whether the end of the outbound message has been written.
    public var outboundHasEnded: Bool {
        self.outbound == .ended
    }

    /// Checks a head may be written now.
    ///
    /// - Parameter informational: Whether it's an interim (1xx) response, which only a server may send, and which
    ///   doesn't start the body.
    /// - Throws: If a server hasn't received the request head yet, or if a final head was already written.
    public mutating func willWriteHead(informational: Bool) throws(HTTP3Error) {
        if self.isServer && self.inbound == .awaitingHead {
            // There is no request yet, so there can't be a response.
            throw Self.responseBeforeRequestError(errorCode: nil, location: .here())
        }
        guard self.outbound == .awaitingHead else {
            throw Self.orderError("A head was already written", location: .here())
        }
        if informational {
            guard self.isServer else {
                throw Self.orderError("Only responses can be informational", location: .here())
            }
        } else {
            self.outbound = .writingBody
        }
    }

    /// Checks a body part may be written now.
    public mutating func willWriteBody() throws(HTTP3Error) {
        guard self.outbound == .writingBody else {
            throw Self.orderError(
                self.outbound == .ended ? "The message already ended" : "The head must be written before the body",
                location: .here()
            )
        }
    }

    /// Checks trailers may be written now. Trailers end the outbound message.
    public mutating func willWriteTrailers() throws(HTTP3Error) {
        try self.end()
    }

    /// Checks the outbound message may end now, without trailers.
    public mutating func willEnd() throws(HTTP3Error) {
        try self.end()
    }

    private mutating func end() throws(HTTP3Error) {
        guard self.outbound == .writingBody else {
            throw Self.orderError(
                self.outbound == .ended ? "The message already ended" : "The head must be written before the end",
                location: .here()
            )
        }
        self.outbound = .ended
    }

    // MARK: Errors

    @inline(never)
    private func unexpectedFrameError(type: HTTP3FrameType, location: HTTP3Error.SourceLocation) -> HTTP3Error {
        let expected =
            switch self.inbound {
            case .awaitingHead: "headers"
            case .awaitingBodyOrTrailers: "headers or data"
            case .ended:
                self.isServer ? "no further frames after request trailers" : "no further frames after response trailers"
            }
        return HTTP3Error(
            code: .unexpectedFrame,
            message: "Expected \(expected), got \(type)",
            cause: nil,
            errorCode: .frameUnexpected,
            location: location
        )
    }

    @inline(never)
    private static func unexpectedPushPromiseError(location: HTTP3Error.SourceLocation) -> HTTP3Error {
        HTTP3Error(
            code: .unexpectedFrame,
            message: "Unexpected push promise",
            cause: nil,
            errorCode: .idError,
            location: location
        )
    }

    @inline(never)
    private static func responseBeforeRequestError(
        errorCode: HTTP3ErrorCode?,
        location: HTTP3Error.SourceLocation
    ) -> HTTP3Error {
        HTTP3Error(
            code: .malformedMessage,
            message: "A HTTP response was sent before a request",
            cause: nil,
            errorCode: errorCode,
            location: location
        )
    }

    /// An outbound part written out of order. This is a misuse of our own API, so it never goes on the wire.
    @inline(never)
    private static func orderError(_ message: String, location: HTTP3Error.SourceLocation) -> HTTP3Error {
        HTTP3Error(code: .unexpectedFrame, message: message, cause: nil, errorCode: nil, location: location)
    }
}
