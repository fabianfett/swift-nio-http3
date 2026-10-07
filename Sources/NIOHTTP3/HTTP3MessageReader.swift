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
public import HTTPTypes
public import NIOQUIC
public import NIOQUICHelpers

/// Holds an error out of line, so it doesn't bloat every stream's state.
@usableFromInline
@available(anyAppleOS 27, *)
final class HTTP3ErrorBox: Sendable {
    @usableFromInline
    let error: HTTP3Error

    init(_ error: HTTP3Error) {
        self.error = error
    }
}

/// Parses the frames of one request stream into the parts of the message it carries.
///
/// The order of the frames is checked by the stream's ``HTTP3RequestStreamValidator``, which the caller passes in:
/// it sees both directions of the stream.
///
/// The reader works on the bytes the QUIC stream hands over and never buffers them itself: frame headers
/// and HEADERS payloads are parsed in place (asking the stream to coalesce when they straddle the stack's
/// frames), and DATA payloads are handed on as they are. When the user takes fewer body bytes than offered,
/// reading pauses without reading from the QUIC stream again, which is what keeps QUIC's flow control in charge
/// of how much the peer may send.
@usableFromInline
@available(anyAppleOS 27, *)
struct HTTP3MessageReader {
    /// What the caller should do next.
    @usableFromInline
    enum Action {
        /// Read from the stream, feeding the bytes to ``consume(_:context:_:)``.
        case read(minimumContiguousBytes: Int)
        /// Hand the part to the user.
        case part(Part)
        /// Nothing more can be read right now (or ever, once the message is complete).
        case finished
        /// Abort the stream with the given codes, then surface the error.
        case streamError(HTTP3Error, stopSending: QUICApplicationErrorCode?, reset: QUICApplicationErrorCode?)
        /// Close the connection with the error, then surface it.
        case connectionError(HTTP3Error)
        /// Surface the error: nothing needs to be sent.
        case error(HTTP3Error)
    }

    /// A part of the inbound message.
    @usableFromInline
    enum Part {
        case requestHead(HTTPRequest)
        case responseHead(HTTPResponse)
        case body
        case end(HTTPFields?)
    }

    private enum FrameState {
        /// The next bytes are the type and length of a frame.
        case header
        /// In a DATA frame's payload.
        case data(remaining: Int)
        /// In the payload of a frame which is ignored.
        case skip(remaining: Int)
        /// A HEADERS frame's payload, which is only parsed once it's all contiguous. `trailers` says whether the
        /// validator announced it as the head or as trailers.
        case headers(length: Int, trailers: Bool)
    }

    private enum MessageState: UInt8 {
        /// The message is still being read.
        case reading
        /// Trailers were delivered, which end the message. Only FIN may follow.
        case awaitingFIN
        /// The message is over and the end has been reported.
        case done
    }

    private enum FailureKind: UInt8 {
        case stream
        case connection
        case report
    }

    /// Whether a server accepts Extended CONNECT requests, because it sent `SETTINGS_ENABLE_CONNECT_PROTOCOL`.
    private let extendedConnectSupported: Bool
    private let maximumHeadersLength: Int

    private var frame: FrameState
    private var message: MessageState

    /// The shortest run of contiguous bytes the next read needs.
    private var need: Int

    /// A decoded field section which hasn't been turned into a part yet.
    private var pendingFields: [HTTPField]?
    /// Whether ``pendingFields`` are trailers rather than a head.
    private var pendingFieldsAreTrailers: Bool

    private var failure: HTTP3ErrorBox?
    private var failureKind: FailureKind
    private var failureReported: Bool

    /// Whether the stream said it has no more bytes.
    private var finReceived: Bool

    /// Body bytes handed over since the last `.body` part.
    private var bodyDelivered: Int

    /// Set during a read once there is a reason to stop feeding bytes in, after which ``consume(_:context:_:)``
    /// takes nothing.
    private var stopped: Bool

    /// The body closure took fewer bytes than it was offered, which is the user's way of applying back pressure.
    ///
    /// Reading stays paused until ``resume()``. It has to: a read from the QUIC stream takes everything the stack
    /// holds for it, whether or not it's consumed, which gives the peer more flow control credit. Only not reading
    /// holds the peer back.
    private var paused: Bool

    init(extendedConnectSupported: Bool, maximumHeadersLength: Int) {
        self.extendedConnectSupported = extendedConnectSupported
        self.maximumHeadersLength = maximumHeadersLength
        self.frame = .header
        self.message = .reading
        self.need = 1
        self.pendingFields = nil
        self.pendingFieldsAreTrailers = false
        self.failure = nil
        self.failureKind = .report
        self.failureReported = false
        self.finReceived = false
        self.bodyDelivered = 0
        self.stopped = false
        self.paused = false
    }

    /// Whether the whole message has been read and reported.
    @usableFromInline
    var isComplete: Bool {
        self.message == .done
    }

    // MARK: Driving a read

    /// What to do next.
    ///
    /// - Parameters:
    ///   - afterRead: Whether the caller already read from the stream in this call.
    ///   - validator: The stream's validator.
    /// - Returns: What to do.
    mutating func nextAction(afterRead: Bool, validator: inout HTTP3RequestStreamValidator) -> Action {
        if let failure = self.failure {
            if self.failureReported {
                return .finished
            }
            self.failureReported = true
            switch self.failureKind {
            case .connection:
                return .connectionError(failure.error)
            case .stream:
                let code = failure.error.h3ErrorCode.map { QUICApplicationErrorCode($0) }
                return .streamError(failure.error, stopSending: code, reset: code)
            case .report:
                return .error(failure.error)
            }
        }

        if let fields = self.pendingFields.take() {
            return self.makePart(fields: fields, validator: &validator)
        }

        if self.bodyDelivered > 0 {
            self.bodyDelivered = 0
            return .part(.body)
        }

        if self.finReceived {
            return self.inputClosed(validator: &validator)
        }

        if self.message == .done || self.paused {
            return .finished
        }

        // Read if nothing has been read yet in this call, or if the last read stopped early (to coalesce bytes,
        // which may now be available).
        if !afterRead || self.stopped {
            self.stopped = false
            return .read(minimumContiguousBytes: self.need)
        }

        return .finished
    }

    /// Whether reading is paused because the body closure took fewer bytes than it was offered.
    @usableFromInline
    var isPaused: Bool {
        self.paused
    }

    /// Resumes reading after the body closure took fewer bytes than it was offered.
    @usableFromInline
    mutating func resume() {
        self.paused = false
    }

    /// Records what a read returned.
    @usableFromInline
    mutating func readCompleted(_ outcome: QUICStreamReadOutcome) {
        switch outcome {
        case .endOfStream:
            self.finReceived = true
        case .read, .nothingAvailable:
            ()
        }
    }

    /// The peer reset the stream.
    @usableFromInline
    mutating func peerReset(code: QUICApplicationErrorCode) {
        guard self.failure == nil, self.message != .done else { return }
        self.fail(.report, Self.remoteStreamError(code: code, location: .here()))
    }

    /// The stream was cancelled locally; subsequent reads report `error` once.
    ///
    /// - Returns: The code to abort the stream with.
    @usableFromInline
    mutating func cancelled(code: HTTP3ErrorCode, error: HTTP3Error?) -> QUICApplicationErrorCode {
        if self.failure == nil {
            self.message = .done
            self.fail(
                .report,
                error
                    ?? HTTP3Error(
                        code: .rejected,
                        message: "The stream was cancelled",
                        cause: nil,
                        errorCode: code,
                        location: .here()
                    )
            )
        }
        return QUICApplicationErrorCode(code)
    }

    /// A flush which had to wait for the stream's ID failed once it had one.
    @usableFromInline
    mutating func flushFailed(_ error: any Error) {
        guard self.failure == nil else { return }
        self.message = .done
        self.fail(
            .report,
            HTTP3Error(
                code: .streamCreationError,
                message: "The request couldn't be sent",
                cause: error,
                errorCode: nil,
                location: .here()
            )
        )
    }

    // MARK: Consuming bytes

    /// Parses `bytes`, returning how many of them were consumed.
    ///
    /// DATA payloads are passed to `deliver`, which returns how many bytes it took.
    mutating func consume(
        _ bytes: borrowing RawSpan,
        validator: inout HTTP3RequestStreamValidator,
        context: HTTP3ConnectionContext,
        _ deliver: (_ payload: borrowing RawSpan) -> Int
    ) -> Int {
        if self.stopped {
            return 0
        }

        let count = bytes.byteCount
        let coalesced = self.need > 1

        if coalesced && count < self.need {
            // The stream only hands over fewer bytes than asked for when the peer has finished it, so what's left
            // can never form the rest of the frame. RFC 9114 § 7.1: a truncated frame on a cleanly terminated
            // stream is a connection error of type H3_FRAME_ERROR.
            self.fail(.connection, Self.truncatedFrameError(location: .here()))
            return 0
        }
        self.need = 1

        var offset = 0
        parsing: while offset < count {
            switch self.frame {
            case .header:
                let type: UInt64
                let typeLength: Int
                switch QUICVarint.read(bytes, at: offset) {
                case .value(let value, let length):
                    type = value
                    typeLength = length
                case .incomplete(let needed):
                    self.need = needed
                    self.stopped = true
                    break parsing
                }

                let length: UInt64
                let lengthLength: Int
                switch QUICVarint.read(bytes, at: offset &+ typeLength) {
                case .value(let value, let valueLength):
                    length = value
                    lengthLength = valueLength
                case .incomplete(let needed):
                    self.need = typeLength &+ needed
                    self.stopped = true
                    break parsing
                }

                offset &+= typeLength &+ lengthLength
                if !self.beginFrame(type: type, length: length, validator: &validator) {
                    self.stopped = true
                    break parsing
                }

            case .data(let remaining):
                let available = min(remaining, count &- offset)
                let taken = deliver(bytes.extracting(offset..<(offset &+ available)))
                assert((0...available).contains(taken), "The body closure took \(taken) of \(available) bytes")
                let consumed = min(max(taken, 0), available)

                offset &+= consumed
                self.bodyDelivered &+= consumed
                let left = remaining &- consumed
                self.frame = left == 0 ? .header : .data(remaining: left)

                if consumed < available {
                    self.paused = true
                    self.stopped = true
                    break parsing
                }

            case .skip(let remaining):
                let skipped = min(remaining, count &- offset)
                offset &+= skipped
                let left = remaining &- skipped
                self.frame = left == 0 ? .header : .skip(remaining: left)

            case .headers(let length, let trailers):
                guard count &- offset >= length else {
                    self.need = length
                    self.stopped = true
                    break parsing
                }

                self.decodeFieldSection(
                    bytes.extracting(offset..<(offset &+ length)),
                    trailers: trailers,
                    context: context
                )
                offset &+= length
                self.frame = .header
                // The part has to be handed over before anything after it.
                self.stopped = true
                break parsing
            }
        }

        if coalesced {
            // Stop so that the rest of the stream isn't coalesced too: the next read goes back to taking the
            // stack's bytes as they are.
            self.stopped = true
        }

        return offset
    }

    /// Starts a frame whose header has just been parsed.
    ///
    /// - Returns: Whether parsing may continue.
    private mutating func beginFrame(
        type rawType: UInt64,
        length: UInt64,
        validator: inout HTTP3RequestStreamValidator
    ) -> Bool {
        let type: HTTP3FrameType
        do {
            type = try HTTP3FrameType(rawValue: rawType)
        } catch {
            self.fail(.connection, error)
            return false
        }

        let trailers: Bool
        switch validator.receivedFrame(type) {
        case .body:
            // A QUIC stream can't carry more than 2^62 bytes, so a longer frame could never be completed.
            let length = Int(clamping: length)
            self.frame = length == 0 ? .header : .data(remaining: length)
            return true

        case .ignore:
            // RFC 9114 § 9: frames of unknown types MUST be ignored. Their payload is skipped without being
            // held, so there is no need to bound it.
            let length = Int(clamping: length)
            self.frame = length == 0 ? .header : .skip(remaining: length)
            return true

        case .connectionError(let error):
            self.fail(.connection, error)
            return false

        case .streamError(let error):
            self.fail(.stream, error)
            return false

        case .decodeHead:
            trailers = false

        case .decodeTrailers:
            trailers = true
        }

        guard length <= UInt64(self.maximumHeadersLength) else {
            // RFC 9114 § 10.5: an endpoint MAY treat activity that is suspicious as a connection error of
            // type H3_EXCESSIVE_LOAD.
            self.fail(
                .connection,
                HTTP3Error(
                    code: .invalidFramePayload,
                    message: "Payload too large",
                    cause: nil,
                    errorCode: .excessiveLoad,
                    location: .here()
                )
            )
            return false
        }
        self.frame = .headers(length: Int(length), trailers: trailers)
        return true
    }

    private mutating func decodeFieldSection(
        _ payload: borrowing RawSpan,
        trailers: Bool,
        context: HTTP3ConnectionContext
    ) {
        switch context.decodeFieldSection(payload) {
        case .success(let fields):
            self.pendingFields = fields
            self.pendingFieldsAreTrailers = trailers
        case .failure(.connectionError(let error)):
            self.fail(.connection, error)
        case .failure(.streamError(let error)):
            self.fail(.stream, error)
        }
    }

    // MARK: Parts

    private mutating func makePart(fields: [HTTPField], validator: inout HTTP3RequestStreamValidator) -> Action {
        do throws(HTTP3Error) {
            if self.pendingFieldsAreTrailers {
                let trailers = try HTTP3MessageDecoding.trailers(fields: fields)
                self.message = .awaitingFIN
                return .part(.end(trailers))
            } else if validator.isServer {
                let request = try HTTP3MessageDecoding.request(
                    fields: fields,
                    extendedConnectSupported: self.extendedConnectSupported
                )
                validator.receivedHead(informational: false)
                return .part(.requestHead(request))
            } else {
                let response = try HTTP3MessageDecoding.response(fields: fields)
                // Interim responses precede the final one (RFC 9114 § 4.1).
                validator.receivedHead(informational: response.status.kind == .informational)
                return .part(.responseHead(response))
            }
        } catch {
            self.fail(.stream, error)
            self.failureReported = true
            let code = QUICApplicationErrorCode(error.h3ErrorCode ?? .messageError)
            return .streamError(error, stopSending: code, reset: code)
        }
    }

    private mutating func inputClosed(validator: inout HTTP3RequestStreamValidator) -> Action {
        switch self.frame {
        case .header:
            ()
        case .data, .skip, .headers:
            self.message = .done
            self.fail(.connection, Self.truncatedFrameError(location: .here()))
            self.failureReported = true
            return .connectionError(self.failure!.error)
        }

        guard self.message != .done else {
            return .finished
        }
        let trailersDelivered = self.message == .awaitingFIN
        self.message = .done

        switch validator.receivedEnd() {
        case .complete:
            return trailersDelivered ? .finished : .part(.end(nil))

        case .incompleteRequest(let error):
            self.fail(.stream, error)
            self.failureReported = true
            return .streamError(error, stopSending: nil, reset: QUICApplicationErrorCode(.requestIncomplete))

        case .incompleteResponse(let error):
            self.fail(.report, error)
            self.failureReported = true
            return .error(error)
        }
    }

    // MARK: Errors

    private mutating func fail(_ kind: FailureKind, _ error: HTTP3Error) {
        if self.failure == nil {
            self.failure = HTTP3ErrorBox(error)
            self.failureKind = kind
        }
        self.stopped = true
    }

    @inline(never)
    private static func truncatedFrameError(location: HTTP3Error.SourceLocation) -> HTTP3Error {
        HTTP3Error(
            code: .leftoverBytes,
            message: "There were leftover bytes when the input was closed",
            cause: nil,
            errorCode: .frameError,
            location: location
        )
    }

    @inline(never)
    static func remoteStreamError(
        code: QUICApplicationErrorCode,
        location: HTTP3Error.SourceLocation
    ) -> HTTP3Error {
        // Preserve the peer's error code verbatim, even if this library doesn't know it: RFC 9114 § 8 treats an
        // unknown code as equivalent to H3_NO_ERROR, which is still reported as a remote stream error.
        HTTP3Error(
            code: .remoteStreamError,
            message: "The remote peer closed the stream",
            cause: nil,
            errorCode: HTTP3ErrorCode(rawValue: code.rawValue),
            location: location
        )
    }
}
