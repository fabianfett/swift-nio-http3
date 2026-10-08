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
import Logging
public import NIOCore
public import NIOQUIC
public import NIOQUICHelpers
@_spi(PackageInternal) import QPACK

/// The connection-wide HTTP/3 state: the control streams, SETTINGS, GOAWAY and QPACK.
///
/// This is shared by every stream on the connection and deliberately not generic: only the thin layer which
/// touches `QUICStream` is, so that it can be specialized for the user's consumer.
@usableFromInline
@available(anyAppleOS 27, *)
final class HTTP3ConnectionContext {
    @usableFromInline
    let isServer: Bool

    @usableFromInline
    let configuration: HTTP3StreamsConfiguration

    @usableFromInline
    let eventLoop: any EventLoop

    private let localSettings: HTTP3Settings
    private var stateMachine: HTTP3ConnectionStateMachine
    private let logger: Logger

    /// The connection channel, to close the connection with.
    ///
    /// The channel owns the consumer, which owns this: the reference is dropped in ``connectionClosed()`` to
    /// break the cycle.
    private var channel: (any Channel)?

    /// Where outbound frames are encoded before they are copied into a stream. Reused, so encoding doesn't
    /// allocate.
    @usableFromInline
    var scratch: ByteBuffer

    /// Where field sections are copied to be decoded.
    private var decodeScratch: ByteBuffer

    /// The SETTINGS the peer sent, once it has.
    private(set) var peerSettings: HTTP3Settings?

    /// The handle of the control stream this endpoint opened.
    @usableFromInline
    private(set) var outboundControlStream: QUICStreamHandle?

    /// The frame decoder for the peer's control stream.
    private var inboundControl: HTTP3StreamStateMachine?

    /// Bytes of the peer's QPACK streams which haven't formed a whole instruction yet.
    private var qpackEncoderInstructions: ByteBuffer
    private var qpackDecoderInstructions: ByteBuffer

    /// Request streams which must be cancelled, but which couldn't be reached from where that was decided.
    @usableFromInline
    var pendingCancellations: [(QUICStreamID, HTTP3Error)]

    /// Set once the connection is closing, so that streams closing as a consequence aren't treated as errors.
    @usableFromInline
    private(set) var isClosing: Bool

    /// The error the connection closed with, reported to streams which close because of it.
    @usableFromInline
    private(set) var connectionError: HTTP3Error?

    /// Request streams which are currently open.
    private var openRequestStreams: Int

    /// Whether this endpoint sent GOAWAY, after which it closes once idle.
    private var isQuiescing: Bool

    init(
        isServer: Bool,
        settings: HTTP3Settings,
        configuration: HTTP3StreamsConfiguration,
        channel: any Channel,
        logger: Logger
    ) {
        precondition(
            settings.qpackMaximumTableCapacity == 0 && settings.qpackBlockedStreams == 0,
            "The QPACK dynamic table is not supported"
        )
        self.isServer = isServer
        self.configuration = configuration
        self.eventLoop = channel.eventLoop
        self.localSettings = settings
        self.stateMachine = HTTP3ConnectionStateMachine(settings: settings, type: isServer ? .server : .client)
        self.logger = logger
        self.channel = channel
        self.scratch = ByteBuffer()
        self.scratch.reserveCapacity(1024)
        self.decodeScratch = ByteBuffer()
        self.decodeScratch.reserveCapacity(1024)
        self.peerSettings = nil
        self.outboundControlStream = nil
        self.inboundControl = nil
        self.qpackEncoderInstructions = ByteBuffer()
        self.qpackDecoderInstructions = ByteBuffer()
        self.pendingCancellations = []
        self.isClosing = false
        self.connectionError = nil
        self.openRequestStreams = 0
        self.isQuiescing = false

        switch self.stateMachine.initialize() {
        case .createControlStream:
            ()  // Opened once the connection is active, see `openControlStream`.
        case .createControlAndDecoderStreams:
            preconditionFailure("A QPACK decoder stream is only needed for a dynamic table")
        case .none:
            ()
        }
    }

    // MARK: Closing

    /// Closes the connection because of `error`.
    ///
    /// The close happens on the next loop tick: this is usually called while the stream table is being drained,
    /// which a close must not re-enter.
    @usableFromInline
    func closeConnection(_ error: HTTP3Error) {
        switch self.stateMachine.emitConnectionErrorFromStream(error: error, allowNotStarted: true) {
        case .emitConnectionError(let error):
            self.close(error)
        case .none:
            ()
        }
    }

    /// Closes the connection with H3\_NO\_ERROR.
    private func closeGracefully() {
        switch self.stateMachine.shutdownConnectionImmediately() {
        case .shutdown:
            self.close(
                HTTP3Error(code: .none, message: "", cause: nil, errorCode: .noError, location: .here())
            )
        }
    }

    private func close(_ error: HTTP3Error) {
        guard !self.isClosing else { return }
        self.isClosing = true
        self.logger.debug(
            "Closing connection",
            metadata: [
                "h3.error": "\(error.h3ErrorCode ?? .noError)", "h3.reason": "\(error.message)",
            ]
        )
        if error.h3ErrorCode != HTTP3ErrorCode.noError {
            self.connectionError = error
        }

        let event = QUICCloseConnectionEvent(
            code: QUICApplicationErrorCode(error.h3ErrorCode ?? .noError),
            reasonPhrase: error.message
        )
        guard let channel = self.channel else { return }
        self.eventLoop.assumeIsolated().execute {
            channel.triggerUserOutboundEvent(event, promise: nil)
        }
    }

    /// The peer closed the connection.
    func peerClosedConnection(_ error: QUICConnectionError) {
        let h3Error = HTTP3Error(
            code: .remoteConnectionError,
            message: error.reason,
            cause: error,
            errorCode: error.isApplication ? HTTP3ErrorCode(rawValue: error.code) : nil,
            location: .here()
        )
        self.isClosing = true
        if self.connectionError == nil {
            self.connectionError = h3Error
        }
        _ = self.stateMachine.caughtRemoteError(h3Error)
    }

    /// The connection is gone.
    func connectionClosed() {
        self.isClosing = true
        self.channel = nil
        _ = self.stateMachine.shutdownConnectionImmediately()
    }

    // MARK: Request streams

    /// Decides whether to accept a request stream the peer opened.
    ///
    /// - Returns: `nil` if it's accepted, otherwise the code to reject it with.
    @usableFromInline
    func inboundRequestStreamReceived(id: QUICStreamID) -> QUICApplicationErrorCode? {
        switch self.stateMachine.inboundRequestStreamReceived(streamID: id) {
        case .addHandlers:
            self.openRequestStreams += 1
            return nil
        case .emitStreamError(let error):
            self.logger.trace("Rejecting inbound request stream", metadata: ["quic.stream.id": "\(id)"])
            return QUICApplicationErrorCode(error.h3ErrorCode ?? .requestRejected)
        case .emitConnectionError(let error):
            self.close(error)
            return QUICApplicationErrorCode(error.h3ErrorCode ?? .streamCreationError)
        }
    }

    /// Checks a request stream may be opened (a client may not, once the server sent GOAWAY).
    @usableFromInline
    func outboundRequestStreamRequested() throws(HTTP3Error) {
        switch self.stateMachine.outboundRequestStreamRequested() {
        case .create:
            ()
        case .failedToCreateStream(let error):
            throw error
        }
    }

    /// The stack assigned an ID to a request stream this endpoint opened.
    @usableFromInline
    func outboundRequestStreamOpened(id: QUICStreamID) {
        self.openRequestStreams += 1
        self.stateMachine.outboundRequestStreamReady(streamID: id)
    }

    /// A request stream which was accepted or opened has closed.
    @usableFromInline
    func requestStreamClosed(id: QUICStreamID) {
        self.openRequestStreams -= 1
        switch self.stateMachine.streamClosed(streamID: id, streamType: .request) {
        case .closeConnection:
            self.logger.trace("Closing the connection: GOAWAY was exchanged and no request streams are open")
            self.closeGracefully()
        case .emitConnectionError(let error):
            self.close(error)
        case .none:
            ()
        }
    }

    // MARK: Encoding

    /// Copies what's been encoded into ``scratch`` into `span`, returning the number of bytes copied.
    @usableFromInline
    func copyScratch(into span: inout MutableRawSpan) -> Int {
        let count = self.scratch.readableBytes
        self.scratch.withUnsafeReadableBytes { source in
            span.withUnsafeMutableBytes { destination in
                destination.copyMemory(from: source)
            }
        }
        return count
    }

    @usableFromInline
    func encodeHead(_ request: HTTPRequest) {
        self.scratch.clear()
        HTTP3MessageEncoding.writeHeadersFrame(
            request: request,
            into: &self.scratch,
            preferHuffmanEncoding: self.configuration.preferHuffmanEncoding
        )
    }

    @usableFromInline
    func encodeHead(_ response: HTTPResponse) {
        self.scratch.clear()
        HTTP3MessageEncoding.writeHeadersFrame(
            response: response,
            into: &self.scratch,
            preferHuffmanEncoding: self.configuration.preferHuffmanEncoding
        )
    }

    /// Encodes `head`, `body` and `trailers`, to be sent together.
    @usableFromInline
    func encodeResponse(_ head: HTTPResponse, body: ByteBuffer?, trailers: HTTPFields?) {
        self.encodeHead(head)
        if let body, body.readableBytes > 0 {
            self.appendDataFrame(copying: body)
        }
        if let trailers {
            self.appendTrailers(trailers)
        }
    }

    /// Encodes `head`, `body` and `trailers`, to be sent together.
    @usableFromInline
    func encodeRequest(_ head: HTTPRequest, body: ByteBuffer?, trailers: HTTPFields?) {
        self.encodeHead(head)
        if let body, body.readableBytes > 0 {
            self.appendDataFrame(copying: body)
        }
        if let trailers {
            self.appendTrailers(trailers)
        }
    }

    @usableFromInline
    func encodeTrailers(_ trailers: HTTPFields) {
        self.scratch.clear()
        self.appendTrailers(trailers)
    }

    /// Encodes a DATA frame carrying a copy of `body`.
    @usableFromInline
    func encodeDataFrame(copying body: ByteBuffer) {
        self.scratch.clear()
        self.appendDataFrame(copying: body)
    }

    /// Encodes the header of a DATA frame whose payload is written separately.
    @usableFromInline
    func encodeDataFrameHeader(payloadLength: Int) {
        self.scratch.clear()
        HTTP3MessageEncoding.writeDataFrameHeader(payloadLength: payloadLength, into: &self.scratch)
    }

    private func appendDataFrame(copying body: ByteBuffer) {
        HTTP3MessageEncoding.writeDataFrameHeader(payloadLength: body.readableBytes, into: &self.scratch)
        self.scratch.writeImmutableBuffer(body)
    }

    private func appendTrailers(_ trailers: HTTPFields) {
        HTTP3MessageEncoding.writeHeadersFrame(
            trailers: trailers,
            into: &self.scratch,
            preferHuffmanEncoding: self.configuration.preferHuffmanEncoding
        )
    }

    /// The length of the header of a DATA frame with a payload of `payloadLength` bytes.
    @usableFromInline
    static func dataFrameHeaderLength(payloadLength: Int) -> Int {
        HTTP3MessageEncoding.dataFrameHeaderLength(payloadLength: payloadLength)
    }

    /// Decodes the field section of a HEADERS frame.
    func decodeFieldSection(_ payload: borrowing RawSpan) -> Result<[HTTPField], HTTP3MessageDecoding.DecodeError> {
        self.decodeScratch.clear()
        payload.withUnsafeBytes { bytes in
            _ = self.decodeScratch.writeBytes(bytes)
        }
        do {
            return .success(try HTTP3MessageDecoding.decodeHeadersFramePayload(&self.decodeScratch))
        } catch {
            return .failure(error)
        }
    }

    // MARK: Control stream

    /// Encodes the start of this endpoint's control stream: its type and the SETTINGS frame.
    @usableFromInline
    func encodeControlStreamPreamble() {
        self.scratch.clear()
        self.scratch.writeEncodedInteger(HTTP3StreamType.Unidirectional.control.rawValue, strategy: .quic)
        HTTP3MessageEncoding.writeFrame(.settings(self.localSettings), into: &self.scratch)
    }

    @usableFromInline
    func outboundControlStreamCreated(handle: QUICStreamHandle) {
        self.outboundControlStream = handle
    }

    @usableFromInline
    func outboundControlStreamOpened(id: QUICStreamID) {
        self.stateMachine.outboundControlStreamReady(streamID: id)
        self.logger.trace("Opened outbound control stream", metadata: ["quic.stream.id": "\(id)"])
    }

    /// The peer stopped this endpoint's control stream, or it failed.
    @usableFromInline
    func outboundControlStreamFailed() {
        guard !self.isClosing else { return }
        self.closeConnection(Self.criticalStreamClosedError(type: .control, location: .here()))
    }

    // MARK: Inbound unidirectional streams

    /// What to do after reading from an inbound unidirectional stream.
    @usableFromInline
    enum UnidirectionalReadAction {
        /// Read again: the stream type was only just parsed, the rest of the bytes may already be available.
        case readAgain(minimumContiguousBytes: Int)
        /// Nothing more to do.
        case done
        /// Tell the peer to stop sending on this stream, and ignore it.
        case stopSending(QUICApplicationErrorCode)
    }

    /// The shortest run of contiguous bytes the next read of the stream needs.
    @usableFromInline
    func unidirectionalReadMinimum(_ core: HTTP3StreamCore) -> Int {
        core.kind == .inboundUnidirectional ? Int(core.streamTypeNeed) : 1
    }

    /// Consumes bytes of an inbound unidirectional stream, returning how many were consumed.
    @usableFromInline
    func consumeUnidirectional(
        _ core: inout HTTP3StreamCore,
        _ bytes: borrowing RawSpan,
        streamID: QUICStreamID
    ) -> Int {
        switch core.kind {
        case .inboundUnidirectional:
            // Only the stream type is consumed: the next read hands over the rest as it is, rather than as
            // coalesced as the type needed.
            switch QUICVarint.read(bytes, at: 0) {
            case .incomplete(let needed):
                if Int(core.streamTypeNeed) > 1 && bytes.byteCount < Int(core.streamTypeNeed) {
                    // The peer finished the stream before sending its whole type.
                    core.kind = .discarded
                    return bytes.byteCount
                }
                core.streamTypeNeed = UInt8(needed)
                return 0
            case .value(let rawType, let length):
                core.streamTypeNeed = 1
                self.inboundUnidirectionalStreamType(&core, rawType: rawType, streamID: streamID)
                return length
            }

        case .inboundControl:
            bytes.withUnsafeBytes { self.inboundControl?.buffer(ByteBuffer(bytes: $0)) }
            return bytes.byteCount

        case .inboundQPACKEncoder:
            bytes.withUnsafeBytes { _ = self.qpackEncoderInstructions.writeBytes($0) }
            return bytes.byteCount

        case .inboundQPACKDecoder:
            bytes.withUnsafeBytes { _ = self.qpackDecoderInstructions.writeBytes($0) }
            return bytes.byteCount

        case .discarded, .rejected, .request, .outboundControl:
            return bytes.byteCount
        }
    }

    private func inboundUnidirectionalStreamType(
        _ core: inout HTTP3StreamCore,
        rawType: UInt64,
        streamID: QUICStreamID
    ) {
        let type = HTTP3StreamType.Unidirectional(rawValue: rawType)
        self.logger.trace(
            "Received a new inbound stream",
            metadata: ["quic.stream.id": "\(streamID)", "h3.stream.type": "\(type)"]
        )

        switch type {
        case .control:
            switch self.stateMachine.inboundControlStreamReceived(streamID: streamID) {
            case .addHandlers:
                core.kind = .inboundControl
                self.inboundControl = HTTP3StreamStateMachine(
                    streamType: .control,
                    incoming: true,
                    preferHuffmanEncoding: false
                )
            case .emitConnectionError(let error):
                core.kind = .discarded
                self.close(error)
            case .emitStreamError(let error):
                core.kind = .discarded
                core.pendingStopSending = QUICApplicationErrorCode(error.h3ErrorCode ?? .streamCreationError)
            }

        case .qpackEncoder, .qpackDecoder:
            let action =
                type == .qpackEncoder
                ? self.stateMachine.inboundQPACKEncoderStreamReceived(streamID: streamID)
                : self.stateMachine.inboundQPACKDecoderStreamReceived(streamID: streamID)
            switch action {
            case .addHandlers:
                core.kind = type == .qpackEncoder ? .inboundQPACKEncoder : .inboundQPACKDecoder
            case .emitConnectionError(let error):
                core.kind = .discarded
                self.close(error)
            case .emitStreamError(let error):
                core.kind = .discarded
                core.pendingStopSending = QUICApplicationErrorCode(error.h3ErrorCode ?? .streamCreationError)
            }

        case .push:
            core.kind = .discarded
            switch self.stateMachine.inboundPushStreamReceived(streamID: streamID) {
            case .emitConnectionError(let error):
                self.close(error)
            case .emitStreamError(let error):
                core.pendingStopSending = QUICApplicationErrorCode(error.h3ErrorCode ?? .streamCreationError)
            }

        case .unknown:
            // RFC 9114 § 6.2: recipients of unknown stream types MUST either abort reading of the stream or
            // discard incoming data. Reading is aborted with H3_STREAM_CREATION_ERROR.
            core.kind = .discarded
            switch self.stateMachine.inboundUnknownStreamReceived(streamID: streamID, streamType: type) {
            case .emitStreamError(let error):
                core.pendingStopSending = QUICApplicationErrorCode(error.h3ErrorCode ?? .streamCreationError)
            }
        }
    }

    /// Processes what a read of an inbound unidirectional stream consumed.
    @usableFromInline
    func unidirectionalReadCompleted(
        _ core: inout HTTP3StreamCore,
        _ outcome: QUICStreamReadOutcome,
        consumedType: Bool,
        streamID: QUICStreamID
    ) -> UnidirectionalReadAction {
        let sawFIN: Bool
        switch outcome {
        case .endOfStream:
            sawFIN = true
        case .read, .nothingAvailable:
            sawFIN = false
        }

        if let code = core.pendingStopSending.take() {
            return .stopSending(code)
        }

        switch core.kind {
        case .inboundControl:
            self.processControlFrames(inputClosed: sawFIN, streamID: streamID)
        case .inboundQPACKEncoder:
            self.processQPACKEncoderInstructions()
            if sawFIN {
                self.criticalStreamClosed(type: .qpackEncoder, streamID: streamID)
            }
        case .inboundQPACKDecoder:
            self.processQPACKDecoderInstructions()
            if sawFIN {
                self.criticalStreamClosed(type: .qpackDecoder, streamID: streamID)
            }
        case .inboundUnidirectional, .discarded, .rejected, .request, .outboundControl:
            ()
        }

        if consumedType && !sawFIN {
            return .readAgain(minimumContiguousBytes: 1)
        }
        return .done
    }

    /// The peer reset one of its unidirectional streams.
    @usableFromInline
    func unidirectionalStreamReset(_ core: HTTP3StreamCore, streamID: QUICStreamID) {
        switch core.kind {
        case .inboundControl:
            self.criticalStreamClosed(type: .control, streamID: streamID)
        case .inboundQPACKEncoder:
            self.criticalStreamClosed(type: .qpackEncoder, streamID: streamID)
        case .inboundQPACKDecoder:
            self.criticalStreamClosed(type: .qpackDecoder, streamID: streamID)
        case .inboundUnidirectional, .discarded, .rejected, .request, .outboundControl:
            ()
        }
    }

    private func criticalStreamClosed(type: HTTP3StreamType.Unidirectional, streamID: QUICStreamID) {
        guard !self.isClosing else { return }
        switch self.stateMachine.streamClosed(streamID: streamID, streamType: .unidirectional(type)) {
        case .emitConnectionError(let error):
            self.close(error)
        case .closeConnection:
            self.closeGracefully()
        case .none:
            ()
        }
    }

    private func processControlFrames(inputClosed: Bool, streamID: QUICStreamID) {
        if inputClosed {
            self.inboundControl?.inputClosed()
        }

        decoding: while true {
            guard let action = self.inboundControl?.decodeNext() else { return }
            switch action {
            case .returnFrame(let frame):
                self.receivedControlFrame(frame)
            case .emitConnectionError(let error), .emitStreamError(let error):
                // Anything wrong with the control stream affects the whole connection.
                self.closeConnection(error)
                break decoding
            case .inputClosed:
                self.criticalStreamClosed(type: .control, streamID: streamID)
                break decoding
            case .decodeHeader:
                // The validator rejects HEADERS on a control stream before they get here.
                preconditionFailure("HEADERS on the control stream")
            case .callAgain:
                continue decoding
            case .needMoreBytes, .alreadyClosed, .previousError:
                break decoding
            }
        }
    }

    private func receivedControlFrame(_ frame: HTTP3Frame) {
        self.logger.trace("Received control frame", metadata: ["h3.frame": "\(frame)"])
        if case .settings(let settings) = frame {
            self.peerSettings = settings.settings
        }

        switch self.stateMachine.receivedControlFrame(frame) {
        case .none:
            ()
        case .onSettings:
            ()
        case .emitConnectionError(let error):
            self.close(error)
        case .cancelStreams(let ids):
            // The peer won't process these requests. RFC 9114 § 5.2: they may be retried on a new connection.
            for id in ids {
                self.pendingCancellations.append(
                    (
                        id,
                        HTTP3Error(
                            code: .rejected,
                            message: "Stream cancelled due to GOAWAY",
                            cause: nil,
                            errorCode: .requestCancelled,
                            location: .here()
                        )
                    )
                )
            }
        case .closeConnection:
            self.logger.trace("GOAWAY received with no requests in flight, closing the connection")
            self.closeGracefully()
        }
    }

    private func processQPACKEncoderInstructions() {
        let decoder = QPACKEncoderInstructionDecoder()
        do {
            while let instruction = try decoder.decode(buffer: &self.qpackEncoderInstructions) {
                // Setting the capacity to zero is the one instruction which doesn't imply a dynamic table.
                guard case .setDynamicTableCapacity(0) = instruction else {
                    self.closeConnection(
                        Self.qpackStreamError(
                            code: .qpackEncoderStreamError,
                            errorCode: .qpackEncoderStreamError,
                            message: "The peer's QPACK encoder tried to use the dynamic table, which is not supported",
                            location: .here()
                        )
                    )
                    return
                }
            }
        } catch {
            self.closeConnection(
                Self.qpackStreamError(
                    code: .qpackEncoderStreamError,
                    errorCode: .qpackEncoderStreamError,
                    message: "Invalid QPACK encoder instruction",
                    location: .here()
                )
            )
        }
        self.qpackEncoderInstructions.discardReadBytes()
    }

    private func processQPACKDecoderInstructions() {
        let decoder = QPACKDecoderInstructionDecoder()
        do {
            if try decoder.decode(buffer: &self.qpackDecoderInstructions) != nil {
                self.closeConnection(
                    Self.qpackStreamError(
                        code: .qpackDecoderStreamError,
                        errorCode: .qpackDecoderStreamError,
                        message: "Received a QPACK decoder instruction, but the dynamic table is not in use",
                        location: .here()
                    )
                )
                return
            }
        } catch {
            self.closeConnection(
                Self.qpackStreamError(
                    code: .qpackDecoderStreamError,
                    errorCode: .qpackDecoderStreamError,
                    message: "Invalid QPACK decoder instruction",
                    location: .here()
                )
            )
        }
        self.qpackDecoderInstructions.discardReadBytes()
    }

    // MARK: Graceful shutdown

    /// What a GOAWAY requires of the streams.
    @usableFromInline
    struct GoawayAction {
        /// The GOAWAY frame to write to the control stream, already encoded into ``scratch``.
        @usableFromInline
        var controlStream: QUICStreamHandle?
        /// The requests which will no longer be processed.
        @usableFromInline
        var cancel: [QUICStreamID]
        /// Whether the connection can close now: nothing is in flight.
        @usableFromInline
        var closeNow: Bool
    }

    /// Starts shutting down: sends GOAWAY (RFC 9114 § 5.2).
    ///
    /// - Parameter final: For a server, whether this is the second GOAWAY, which carries the lowest request ID
    ///   it won't process. The first one carries the highest possible ID, so requests in flight still arrive.
    /// - Returns: What to do, or `nil` if a GOAWAY isn't to be sent.
    @usableFromInline
    func sendGoaway(final: Bool) -> GoawayAction? {
        let id: HTTP3GoawayID
        if final {
            guard self.isServer, let next = self.stateMachine.nextExpectedClientInitiatedBidirectionalStreamID() else {
                return nil
            }
            id = HTTP3GoawayID(rawValue: next.rawValue)
        } else {
            guard self.stateMachine.canInitiateGracefulShutdown() else { return nil }
            // A client which never sent MAX_PUSH_ID accepts no pushes, so it may send zero (RFC 9114 § 5.2).
            id = self.isServer ? .maxServerValue : HTTP3GoawayID(rawValue: 0)
        }

        switch self.stateMachine.sendGoaway(goawayID: id) {
        case .some(.sendGoaway(let id, let idsToCancel, _)):
            self.isQuiescing = true
            self.logger.trace("Sending GOAWAY", metadata: ["h3.goaway.id": "\(id)"])
            self.scratch.clear()
            HTTP3MessageEncoding.writeFrame(.goaway(id), into: &self.scratch)
            return GoawayAction(
                controlStream: self.outboundControlStream,
                cancel: idsToCancel,
                closeNow: final && self.openRequestStreams == 0
            )
        case .some(.closeImmediately):
            self.closeGracefully()
            return nil
        case .some(.throwError(let error)):
            self.logger.debug("Failed to send GOAWAY", metadata: ["error": "\(error)"])
            return nil
        case .none:
            return nil
        }
    }

    /// Closes the connection once shutdown has finished.
    @usableFromInline
    func finishGoaway(closeNow: Bool) {
        if closeNow {
            self.closeGracefully()
        }
    }

    /// The error a request stream cancelled by this endpoint's GOAWAY is failed with.
    @usableFromInline
    static var rejectedByGoawayError: HTTP3Error {
        HTTP3Error(
            code: .rejected,
            message: "Stream cancelled due to GOAWAY",
            cause: nil,
            errorCode: .requestRejected,
            location: .here()
        )
    }

    // MARK: Errors

    @inline(never)
    private static func criticalStreamClosedError(
        type: HTTP3StreamType.Unidirectional,
        location: HTTP3Error.SourceLocation
    ) -> HTTP3Error {
        HTTP3Error(
            code: .criticalStreamClosed,
            message: "The \(type) stream was closed",
            cause: nil,
            errorCode: .closedCriticalStream,
            location: location
        )
    }

    @inline(never)
    private static func qpackStreamError(
        code: HTTP3Error.Code,
        errorCode: HTTP3ErrorCode,
        message: String,
        location: HTTP3Error.SourceLocation
    ) -> HTTP3Error {
        HTTP3Error(code: code, message: message, cause: nil, errorCode: errorCode, location: location)
    }
}

@available(*, unavailable)
extension HTTP3ConnectionContext: Sendable {}
