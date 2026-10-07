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

@_spi(PackageInternal) import HTTP3
public import NIOQUICHelpers

/// Checks the parts written to one request stream may be written, and keeps the bookkeeping of sending them.
///
/// The order of the parts (RFC 9114 § 4.1: a head, any number of body parts, optional trailers, then the end) is
/// checked by the stream's ``HTTP3RequestStreamValidator``, which the caller passes in: it sees both directions of
/// the stream.
@usableFromInline
@available(anyAppleOS 27, *)
struct HTTP3MessageWriter {
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

    init() {
        self.stopSendingCode = nil
        self.flushPending = false
        self.finPending = false
        self.writtenInVisit = 0
    }

    @usableFromInline
    mutating func peerStoppedSending(code: QUICApplicationErrorCode) {
        self.stopSendingCode = code
    }

    /// Checks a head may be written now.
    ///
    /// - Parameters:
    ///   - informational: Whether it's an interim (1xx) response, which only a server may send and which
    ///     doesn't start the body.
    ///   - validator: The stream's validator.
    /// - Throws: If the peer asked the stream to stop, or the validator refuses the head.
    func willWriteHead(informational: Bool, validator: inout HTTP3RequestStreamValidator) throws(HTTP3Error) {
        try self.checkOpen()
        try validator.willWriteHead(informational: informational)
    }

    /// Checks a body part may be written now.
    func willWriteBody(validator: inout HTTP3RequestStreamValidator) throws(HTTP3Error) {
        try self.checkOpen()
        try validator.willWriteBody()
    }

    /// Checks trailers may be written now. They end the message.
    func willWriteTrailers(validator: inout HTTP3RequestStreamValidator) throws(HTTP3Error) {
        try self.checkOpen()
        try validator.willWriteTrailers()
    }

    /// Checks the message may end now, without trailers.
    func willEnd(validator: inout HTTP3RequestStreamValidator) throws(HTTP3Error) {
        try self.checkOpen()
        try validator.willEnd()
    }

    private func checkOpen() throws(HTTP3Error) {
        if let code = self.stopSendingCode {
            throw HTTP3MessageReader.remoteStreamError(code: code, location: .here())
        }
    }
}
