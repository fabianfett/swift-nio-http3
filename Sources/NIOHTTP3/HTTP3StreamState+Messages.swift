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
public import NIOCore
public import NIOQUIC
public import NIOQUICHelpers

/// A part of a request, as read by a server.
@available(anyAppleOS 27, *)
public enum HTTP3RequestPart: Sendable {
    /// The request head.
    case head(HTTPRequest)
    /// Body bytes were handed to the body closure.
    case body
    /// The request ended, with trailers if there were any.
    case end(HTTPFields?)
}

/// A part of a response, as read by a client.
@available(anyAppleOS 27, *)
public enum HTTP3ResponsePart: Sendable {
    /// A response head. Interim (1xx) heads may precede the final one.
    case head(HTTPResponse)
    /// Body bytes were handed to the body closure.
    case body
    /// The response ended, with trailers if there were any.
    case end(HTTPFields?)
}

// MARK: - Reading

@available(anyAppleOS 27, *)
extension HTTP3StreamState {
    /// Reads the next part of the request.
    ///
    /// Body bytes are handed to `body` straight out of the QUIC stream, without being copied. `body` returns
    /// how many of them it took. Taking fewer than it was offered is how a server applies back pressure:
    /// reading pauses, and the bytes it left are offered again once ``resumeReading()`` is called. Until then
    /// this doesn't read from the QUIC stream at all, so its flow control stops the peer once it has used up the
    /// stream's window.
    ///
    /// Call this until it returns `nil`, which means nothing more can be read right now: either no bytes are
    /// available (the stream is visited again when they are), reading is paused, or the request has been read
    /// completely.
    ///
    /// - Parameters:
    ///   - stream: The QUIC stream this state belongs to.
    ///   - body: Handed body bytes and the request's state, returning how many of the bytes it took. It may be
    ///     called more than once. It must not use `stream`.
    /// - Returns: The next part, or `nil` if there isn't one right now.
    /// - Throws: An `HTTP3Error` if the request is invalid (in which case the stream has been aborted as
    ///   RFC 9114 requires), if the peer reset the stream, or if the connection failed.
    @inlinable
    public mutating func readRequestPart<Consumer: QUICStreamConsumer & ~Copyable>(
        from stream: inout QUICStream<Consumer>,
        body: (_ bytes: borrowing RawSpan, _ state: inout RequestState) -> Int
    ) throws(HTTP3Error) -> HTTP3RequestPart? {
        switch try self._readPart(from: &stream, body: body) {
        case .some(.requestHead(let head)):
            return .head(head)
        case .some(.body):
            return .body
        case .some(.end(let trailers)):
            return .end(trailers)
        case .some(.responseHead):
            preconditionFailure("A client read a request")
        case .none:
            return nil
        }
    }

    /// Reads the next part of the response.
    ///
    /// This works like ``readRequestPart(from:body:)``: body bytes are handed to `body` without copying, and
    /// taking fewer than offered pauses reading until ``resumeReading()``, leaving the peer to QUIC's flow
    /// control.
    ///
    /// - Parameters:
    ///   - stream: The QUIC stream this state belongs to.
    ///   - body: Handed body bytes and the request's state, returning how many of the bytes it took. It may be
    ///     called more than once. It must not use `stream`.
    /// - Returns: The next part, or `nil` if there isn't one right now.
    /// - Throws: An `HTTP3Error` if the response is invalid, incomplete, or the stream or connection failed.
    @inlinable
    public mutating func readResponsePart<Consumer: QUICStreamConsumer & ~Copyable>(
        from stream: inout QUICStream<Consumer>,
        body: (_ bytes: borrowing RawSpan, _ state: inout RequestState) -> Int
    ) throws(HTTP3Error) -> HTTP3ResponsePart? {
        switch try self._readPart(from: &stream, body: body) {
        case .some(.responseHead(let head)):
            return .head(head)
        case .some(.body):
            return .body
        case .some(.end(let trailers)):
            return .end(trailers)
        case .some(.requestHead):
            preconditionFailure("A server read a response")
        case .none:
            return nil
        }
    }

    /// Whether reading is paused because the body closure took fewer bytes than it was offered.
    @inlinable
    public var isReadingPaused: Bool {
        self.core.reader.isPaused
    }

    /// Resumes reading after the body closure took fewer bytes than it was offered.
    ///
    /// The next read offers the bytes it left again. Call it when there's room for more, for example on the
    /// stream's next visit, or out of band with ``HTTP3Connection/withRequest(_:_:)``.
    @inlinable
    public mutating func resumeReading() {
        self.core.reader.resume()
    }

    @inlinable
    mutating func _readPart<Consumer: QUICStreamConsumer & ~Copyable>(
        from stream: inout QUICStream<Consumer>,
        body: (_ bytes: borrowing RawSpan, _ state: inout RequestState) -> Int
    ) throws(HTTP3Error) -> HTTP3MessageReader.Part? {
        let context = self.core.context
        var afterRead = false

        while true {
            switch self.core.reader.nextAction(afterRead: afterRead) {
            case .read(let minimum):
                let outcome = stream.read(minContiguousBytes: minimum) { bytes in
                    self.core.reader.consume(bytes, context: context) { payload in
                        body(payload, &self._state!)
                    }
                }
                self.core.reader.readCompleted(outcome)
                afterRead = true

            case .part(let part):
                return part

            case .finished:
                return nil

            case .streamError(let error, let stopSending, let reset):
                if let stopSending {
                    stream.sendStopSending(code: stopSending)
                }
                if let reset {
                    stream.sendResetStream(code: reset)
                }
                throw error

            case .connectionError(let error):
                context.closeConnection(error)
                throw error

            case .error(let error):
                throw error
            }
        }
    }
}

// MARK: - Writing

@available(anyAppleOS 27, *)
extension HTTP3StreamState {
    /// Writes the response head.
    ///
    /// Interim (1xx) responses may be written before the final one.
    ///
    /// Everything written to a stream is sent together when the visit ends (or the out of band access, see
    /// ``HTTP3Connection/withRequest(_:_:)``): so write as much as there is before then, as each send
    /// costs packets of its own.
    ///
    /// Like `QUICStream`, writing doesn't wait for room in the stream: use `stream.writableBytes` and the
    /// `writable` event to avoid queueing more than the peer's flow control lets through.
    ///
    /// - Throws: If a final head was already written, or the peer asked the stream to stop.
    @inlinable
    public mutating func writeResponseHead<Consumer: QUICStreamConsumer & ~Copyable>(
        _ response: HTTPResponse,
        to stream: inout QUICStream<Consumer>
    ) throws(HTTP3Error) {
        try self.core.writer.willWriteHead(informational: response.status.kind == .informational)
        self.core.context.encodeHead(response)
        self._commit(to: &stream)
    }

    /// Writes the request head. It's sent when the visit ends, like everything written.
    ///
    /// - Throws: If the head was already written, or the peer asked the stream to stop.
    @inlinable
    public mutating func writeRequestHead<Consumer: QUICStreamConsumer & ~Copyable>(
        _ request: HTTPRequest,
        to stream: inout QUICStream<Consumer>
    ) throws(HTTP3Error) {
        try self.core.writer.willWriteHead(informational: false)
        self.core.context.encodeHead(request)
        self._commit(to: &stream)
    }

    /// Writes part of the body. It's sent when the visit ends, like everything written.
    ///
    /// Small parts are copied into the frame; larger ones are queued as they are, without copying.
    ///
    /// - Throws: If the head wasn't written yet, the message already ended, or the peer asked the stream to
    ///   stop.
    @inlinable
    public mutating func writeBody<Consumer: QUICStreamConsumer & ~Copyable>(
        _ body: ByteBuffer,
        to stream: inout QUICStream<Consumer>
    ) throws(HTTP3Error) {
        try self.core.writer.willWriteBody()
        let length = body.readableBytes
        guard length > 0 else { return }

        let context = self.core.context
        if length <= context.configuration.maximumCopiedBodyLength {
            context.encodeDataFrame(copying: body)
            Self._commitScratch(context, to: &stream)
            self.core.writer.writtenInVisit &+= HTTP3ConnectionContext.dataFrameHeaderLength(payloadLength: length)
        } else {
            context.encodeDataFrameHeader(payloadLength: length)
            Self._commitScratch(context, to: &stream)
            stream.write(body)
        }
        self.core.writer.flushPending = true
        self.core.writer.writtenInVisit &+= length
    }

    /// Ends the message, with trailers if there are any. It's sent, with a FIN, when the visit ends.
    ///
    /// - Throws: If the head wasn't written yet, the message already ended, the peer asked the stream to
    ///   stop, or the stream can't send any more.
    @inlinable
    public mutating func writeEnd<Consumer: QUICStreamConsumer & ~Copyable>(
        trailers: HTTPFields? = nil,
        to stream: inout QUICStream<Consumer>
    ) throws {
        try self.core.writer.willEnd()
        if let trailers {
            self.core.context.encodeTrailers(trailers)
            self._commit(to: &stream)
        }
        try self._flush(&stream, fin: true)
    }

    /// Writes a whole response, as a single frame, which is sent with a FIN when the visit ends.
    ///
    /// - Throws: If a final head was already written, the peer asked the stream to stop, or the stream can't
    ///   send any more.
    @inlinable
    public mutating func writeResponse<Consumer: QUICStreamConsumer & ~Copyable>(
        _ response: HTTPResponse,
        body: ByteBuffer? = nil,
        trailers: HTTPFields? = nil,
        to stream: inout QUICStream<Consumer>
    ) throws {
        try self.core.writer.willWriteHead(informational: false)
        try self.core.writer.willEnd()
        self.core.context.encodeResponse(response, body: body, trailers: trailers)
        self._commit(to: &stream)
        try self._flush(&stream, fin: true)
    }

    /// Writes a whole request, as a single frame, which is sent with a FIN when the visit ends.
    ///
    /// - Throws: If the head was already written, the peer asked the stream to stop, or the stream can't send
    ///   any more.
    @inlinable
    public mutating func writeRequest<Consumer: QUICStreamConsumer & ~Copyable>(
        _ request: HTTPRequest,
        body: ByteBuffer? = nil,
        trailers: HTTPFields? = nil,
        to stream: inout QUICStream<Consumer>
    ) throws {
        try self.core.writer.willWriteHead(informational: false)
        try self.core.writer.willEnd()
        self.core.context.encodeRequest(request, body: body, trailers: trailers)
        self._commit(to: &stream)
        try self._flush(&stream, fin: true)
    }

    /// How many more bytes to write in this visit, to keep the stream busy without queueing more than it will
    /// send.
    ///
    /// Use this rather than `stream.writableBytes` to apply back pressure: write (and read whatever is being
    /// written) no more than this, and carry on when the stream is next visited, for example with the
    /// `writable` event. It's the room the stream reports, less what this visit wrote already. When the stream
    /// reports no room, a visit may still write ``HTTP3StreamsConfiguration/minimumWritableBytes``: the room a
    /// QUIC stream reports is advisory, and a stream which waited for it to change might never be visited again.
    @inlinable
    public func writableBytes<Consumer: QUICStreamConsumer & ~Copyable>(
        _ stream: inout QUICStream<Consumer>
    ) -> Int {
        guard stream.id != nil else {
            // A stream without an ID yet is sent to, and visited, once it has one.
            return 0
        }
        let room = stream.writableBytes
        let allowed = room > 0 ? room : self.core.context.configuration.minimumWritableBytes
        return max(allowed &- self.core.writer.writtenInVisit, 0)
    }

    /// Marks what was written to be sent. Everything written is sent when the visit ends anyway: this exists
    /// for symmetry with `QUICStream`.
    @inlinable
    public mutating func flush<Consumer: QUICStreamConsumer & ~Copyable>(
        _ stream: inout QUICStream<Consumer>
    ) throws {
        try self._flush(&stream, fin: false)
    }

    /// Aborts the stream in both directions, with H3\_REQUEST\_CANCELLED unless given another code.
    ///
    /// Reads made afterwards fail with `error`, once.
    @inlinable
    public mutating func cancel<Consumer: QUICStreamConsumer & ~Copyable>(
        code: HTTP3ErrorCode = .requestCancelled,
        stream: inout QUICStream<Consumer>
    ) {
        Self._cancel(&self.core, error: nil, code: code, stream: &stream)
    }

    @inlinable
    static func _cancel<Consumer: QUICStreamConsumer & ~Copyable>(
        _ core: inout HTTP3StreamCore,
        error: HTTP3Error?,
        code: HTTP3ErrorCode,
        stream: inout QUICStream<Consumer>
    ) {
        let applicationCode = core.reader.cancelled(code: code, error: error)
        if stream.isReceiveOpen {
            stream.sendStopSending(code: applicationCode)
        }
        if stream.isSendOpen {
            stream.sendResetStream(code: applicationCode)
        }
    }

    @inlinable
    mutating func _flush<Consumer: QUICStreamConsumer & ~Copyable>(
        _ stream: inout QUICStream<Consumer>,
        fin: Bool
    ) throws {
        // Sent when the visit ends: each send makes packets of its own, so the writes of a visit go together.
        self.core.writer.flushPending = true
        self.core.writer.finPending = self.core.writer.finPending || fin
    }

    /// Sends what was written, once the stream has an ID.
    @inlinable
    mutating func _sendPendingWrites<Consumer: QUICStreamConsumer & ~Copyable>(
        _ stream: inout QUICStream<Consumer>
    ) {
        self.core.writer.writtenInVisit = 0
        guard self.core.writer.flushPending, stream.id != nil else { return }
        self.core.writer.flushPending = false
        do {
            try stream.flush(fin: self.core.writer.finPending)
        } catch {
            self.core.reader.flushFailed(error)
        }
    }

    /// Queues what's been encoded into the connection's scratch buffer as one QUIC frame.
    @inlinable
    mutating func _commit<Consumer: QUICStreamConsumer & ~Copyable>(to stream: inout QUICStream<Consumer>) {
        let context = self.core.context
        self.core.writer.flushPending = true
        self.core.writer.writtenInVisit &+= context.scratch.readableBytes
        Self._commitScratch(context, to: &stream)
    }

    /// Queues what's been encoded into the connection's scratch buffer as one QUIC frame.
    @inlinable
    static func _commitScratch<Consumer: QUICStreamConsumer & ~Copyable>(
        _ context: HTTP3ConnectionContext,
        to stream: inout QUICStream<Consumer>
    ) {
        let count = context.scratch.readableBytes
        stream.withWritableSpan(count: count) { span in
            context.copyScratch(into: &span)
        }
    }
}
