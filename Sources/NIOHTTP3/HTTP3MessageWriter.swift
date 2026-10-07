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

@_spi(PackageInternal) public import HTTP3
public import NIOQUICHelpers

/// Validates the order of the parts written to one request stream (RFC 9114 § 4.1): a head, any number of
/// body parts, optional trailers, then the end.
@usableFromInline
@available(anyAppleOS 27, *)
struct HTTP3MessageWriter {
    private enum State: UInt8 {
        /// Nothing, or only interim responses, have been written.
        case awaitingHead
        case writingBody
        case ended
    }

    private let isServer: Bool
    private var state: State

    /// Set when the peer sent STOP\_SENDING: nothing may be written any more.
    private var stopSendingCode: QUICApplicationErrorCode?

    /// Whether something was written which hasn't been sent yet. Writes are sent together when the visit (or
    /// out of band access) ends, or once the stack assigned the stream its ID if it hadn't yet.
    @usableFromInline
    var flushPending: Bool

    /// Whether the pending flush finishes the send side.
    @usableFromInline
    var finPending: Bool

    /// The number of bytes written in the current visit.
    @usableFromInline
    var writtenInVisit: Int

    init(isServer: Bool) {
        self.isServer = isServer
        self.state = .awaitingHead
        self.stopSendingCode = nil
        self.flushPending = false
        self.finPending = false
        self.writtenInVisit = 0
    }

    /// Whether the end of the message has been written.
    @usableFromInline
    var hasEnded: Bool {
        self.state == .ended
    }

    @usableFromInline
    mutating func peerStoppedSending(code: QUICApplicationErrorCode) {
        self.stopSendingCode = code
    }

    /// Checks a head may be written now.
    ///
    /// - Parameter informational: Whether it's an interim (1xx) response, which only a server may send and
    ///   which doesn't start the body.
    /// - Throws: If a final head was already written, or the peer asked the stream to stop.
    @usableFromInline
    mutating func willWriteHead(informational: Bool) throws(HTTP3Error) {
        try self.checkOpen()
        guard self.state == .awaitingHead else {
            throw Self.orderError("A head was already written", location: .here())
        }
        if informational {
            guard self.isServer else {
                throw Self.orderError("Only responses can be informational", location: .here())
            }
        } else {
            self.state = .writingBody
        }
    }

    @usableFromInline
    mutating func willWriteBody() throws(HTTP3Error) {
        try self.checkOpen()
        guard self.state == .writingBody else {
            throw Self.orderError(
                self.state == .ended ? "The message already ended" : "The head must be written before the body",
                location: .here()
            )
        }
    }

    /// Checks the message may end now, optionally with trailers.
    @usableFromInline
    mutating func willEnd() throws(HTTP3Error) {
        try self.checkOpen()
        guard self.state == .writingBody else {
            throw Self.orderError(
                self.state == .ended ? "The message already ended" : "The head must be written before the end",
                location: .here()
            )
        }
        self.state = .ended
    }

    private func checkOpen() throws(HTTP3Error) {
        if let code = self.stopSendingCode {
            throw HTTP3MessageReader.remoteStreamError(code: code, location: .here())
        }
    }

    @inline(never)
    private static func orderError(_ message: String, location: HTTP3Error.SourceLocation) -> HTTP3Error {
        HTTP3Error(code: .unexpectedFrame, message: message, cause: nil, errorCode: nil, location: location)
    }
}
