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
import HTTPTypes
import Logging
import NIOCore
import NIOQUICHelpers

/// This is an internal protocol that shall only be implemented by HTTP3ConnectionCoordinator
/// It exists to enable testing of `HTTP3StreamHandler` in isolation.
protocol HTTP3StreamDelegate {
    /// Tell the connection state when this stream becomes inactive.
    ///
    /// - Parameters:
    ///     - sawEOF: `true` if we read an EOF before closure. That means no incoming frames were dropped.
    ///     - streamID: The closed stream's ID
    ///     - streamType: The closed stream's type
    func onStreamClosed(_ sawEOF: Bool, streamID: QUICStreamID, streamType: HTTP3StreamType.Framed)

    /// Ask the connection coordinator to send connection-level error to the remote peer.
    func onConnectionError(_ error: HTTP3Error)
}

/// This handler should be added to every incoming and outgoing HTTP/3 stream which carries HTTP frames.
/// It handles encoding and decoding of these frames.
/// It will only pass through valid frames, and handles things such as QPACK header decoding.
final class HTTP3StreamHandler<
    Delegate: HTTP3StreamDelegate,
    ConnectionDelegate: HTTP3.ConnectionDelegate
>: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = HTTP3Frame

    typealias OutboundIn = HTTP3Frame
    typealias OutboundOut = ByteBuffer

    private let streamID: QUICStreamID
    private let streamType: HTTP3StreamType.Framed

    private let delegate: Delegate
    private let qpackCoder: NIOQPACKCoder<ConnectionDelegate, Delegate>

    /// The channel context. This handler can only be in one channel at a time.
    private var context: ChannelHandlerContext?

    /// Bytes for frames that have been written but not yet flushed.
    private var pendingBytes: ByteBuffer?

    /// The promise which will be fulfilled when `pendingBytes` has been written.
    private var pendingPromise: EventLoopPromise<Void>?

    /// The state machine which handles processing incoming bytes into frames, including validating them and decoding QPACK.
    private var stateMachine: HTTP3StreamStateMachine

    /// Whether ``deliverFrames(context:completingRead:)`` is currently reading frames out of the state machine.
    ///
    /// Asking the QPACK coder to decode a header can complete synchronously, which calls straight back
    /// into ``onQPACKDecodeResult(fields:)`` and from there into ``deliverFrames(context:completingRead:)``
    /// again, while the outer loop is still running. This flag lets the re-entrant call bow out: the loop
    /// it would duplicate is still going, and will pick up the newly unblocked frames on its next
    /// iteration.
    private var isDeliveringFrames = false

    /// Turns the incoming bytes into frames, and holds any partial frame at the end of a read for us.
    private let frameProcessor = NIOSingleStepByteToMessageProcessor(HTTP3FrameDecoder())

    /// A frame the decoder produced while the state machine was blocked on a QPACK decode.
    ///
    /// At most one: as soon as we have to hold a frame we stop the processor, so the bytes behind it stay
    /// undecoded until the decode result arrives.
    private var heldFrame: HTTP3DecodedFrame?

    /// Whether the peer has finished sending and the decoder still needs flushing, which is what reports
    /// a final truncated frame.
    private var needsDecoderFlush = false

    /// Whether frames have been fired downstream which no read-complete has followed yet.
    ///
    /// Frames go out as their bytes arrive, so a read burst can end without producing any, and a QPACK
    /// decode can produce some long after the burst that carried them. This tracks which of the two owes
    /// downstream a read-complete.
    private var didFireChannelRead = false

    private let logger: Logger

    init(
        stateMachine: consuming HTTP3StreamStateMachine,
        streamID: QUICStreamID,
        streamType: HTTP3StreamType.Framed,
        qpackCoder: NIOQPACKCoder<ConnectionDelegate, Delegate>,
        delegate: Delegate,
        logger: Logger
    ) {
        self.streamID = streamID
        self.streamType = streamType
        self.stateMachine = stateMachine
        self.qpackCoder = qpackCoder
        self.delegate = delegate
        self.logger = logger
    }

    func handlerAdded(context: ChannelHandlerContext) {
        guard self.context == nil else {
            fatalError("HTTP3StreamHandler must only be added to one Channel")
        }
        self.context = context
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("HTTP3StreamHandler.channelInactive")

        // Don't leak the pending promise.
        self.pendingBytes = nil
        self.pendingPromise.take()?.fail(ChannelError.ioOnClosedChannel)

        // We want to flush out anything that's buffered which can be flushed.
        // There's unlikely to be anything...only if we got a channelInactive between a read and a readComplete.
        // We need to buffer any such actions into an array and save it for after we close the state machine
        var actionBuffer: [HTTP3StreamStateMachine.DecodeNextAction] = []

        loop: while true {
            let action = self.stateMachine.nextAction()
            switch action {
            case .needMoreBytes, .needDecodeResult, .alreadyClosed, .previousError:
                break loop
            case .returnFrame, .emitConnectionError, .emitStreamError, .decodeHeader, .inputClosed:
                actionBuffer.append(action)
            case .callAgain:
                continue loop
            }
        }

        // Tell our state machine we closed, and call our callback to tell the connection coordinator too.
        // The coordinator will clean up QPACK state etc.
        let closeAction = self.stateMachine.closed()
        switch closeAction {
        case .streamClosed(let seenEOF):
            // unbuffer our read actions
            loop: for action in actionBuffer {
                switch action {
                case .inputClosed(let inputClosedAction):
                    switch inputClosedAction {
                    case .emitEvent:
                        context.fireUserInboundEventTriggered(ChannelEvent.inputClosed)

                    case .emitErrorAndEvent(let error):
                        context.fireErrorCaught(error)
                        context.fireUserInboundEventTriggered(ChannelEvent.inputClosed)

                    case .resetStream(let error):
                        // The channel is already inactive, so we cannot send a RESET_STREAM. Just emit the error and
                        // event downstream.
                        context.fireErrorCaught(error)
                        context.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
                    }
                case .returnFrame(let frame):
                    context.fireChannelRead(wrapInboundOut(frame))
                    self.didFireChannelRead = true
                case .decodeHeader:
                    // No point waiting for qpack decodes, the channel won't be around by the time we get a result
                    // Then we have to break the whole loop: can't allow further actions to overtake
                    break loop
                case .emitStreamError:
                    // ignore that now
                    break
                case .emitConnectionError(let error):
                    self.delegate.onConnectionError(error)
                case .alreadyClosed, .needMoreBytes, .needDecodeResult, .previousError, .callAgain:
                    fatalError("Action shouldn't have been buffered")
                }
            }
            // Anything we fired here, plus anything fired by an earlier read that no read-complete has
            // followed yet, has to be completed before channel inactive goes downstream.
            self.completeReadIfNeeded(context: context)
            self.delegate.onStreamClosed(seenEOF, streamID: self.streamID, streamType: self.streamType)
        }
        // Cleanup reference to avoid leaks.
        self.context = nil
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        // Don't leak the pending promise.
        self.pendingBytes = nil
        self.pendingPromise.take()?.fail(ChannelError.ioOnClosedChannel)

        // Cleanup reference to avoid leaks.
        self.context = nil
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let bytes = self.unwrapInboundIn(data)
        self.logger.trace("HTTP3StreamHandler.channelRead", metadata: [LoggingKeys.bytes: "\(bytes.readableBytes)"])
        // Decode and forward whatever these bytes complete right away, rather than holding them until
        // the read burst ends. The processor keeps anything left over.
        self.deliverFrames(context: context, bytes: bytes, completingRead: false)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.logger.trace("HTTP3StreamHandler.channelReadComplete")
        // The frames went out as the bytes arrived, so there is nothing to decode here: just close off
        // the burst. If nothing came out of it, because every frame in it is still waiting on a QPACK
        // decode, we stay quiet and let the decode fire the read-complete when it lands.
        self.completeReadIfNeeded(context: context)
    }

    /// Fire a read-complete, if frames have gone downstream that aren't covered by one yet.
    private func completeReadIfNeeded(context: ChannelHandlerContext) {
        guard self.didFireChannelRead else { return }
        self.didFireChannelRead = false
        context.fireChannelReadComplete()
    }

    /// Thrown out of the frame processor's receiver to stop it decoding any further.
    ///
    /// This leans on how `NIOSingleStepByteToMessageProcessor` behaves when its receiver throws, which
    /// its documentation doesn't spell out. As of swift-nio 2.101:
    ///
    /// - Its decode loop puts its buffer back, minus the bytes the frame we were just handed consumed,
    ///   *before* it calls the receiver. Throwing therefore leaves everything we haven't decoded yet
    ///   intact, rather than discarding it.
    /// - All that gets skipped is its post-decode step, which reclaims already-read bytes and enforces
    ///   `maximumBufferSize`. We set no maximum, so the only cost is that reclaiming waits for the next
    ///   call.
    /// - Handing it an empty buffer afterwards resumes decoding whatever it still holds.
    ///
    /// If any of that changes, this handler would quietly drop the bytes queued behind a blocked header,
    /// so `pausesDecodingWhileHeaderDecodeIsOutstanding` exercises exactly this path.
    ///
    /// None of this would be needed if `append(_:)` and `decodeNext(decodeMode:seenEOF:)` were public.
    /// They already exist on the processor, and pulling one frame at a time would let us simply stop
    /// asking, instead of unwinding out of a loop that wants to run to completion.
    private struct StopDecoding: Error {}

    /// Whether the state machine can be handed another frame.
    private enum Readiness {
        /// It is ready for the next frame.
        case ready
        /// It is waiting on a QPACK decode result. Frames must not overtake it, so we have to stop.
        case blockedOnDecode
        /// Nothing further will be read from this stream.
        case closed
    }

    /// Decode frames out of `bytes` and fire them downstream.
    ///
    /// - Parameters:
    ///   - context: The context to fire the frames on.
    ///   - bytes: Newly arrived bytes, or `nil` when we are picking up where a QPACK decode left off.
    ///   - completingRead: Whether to finish the read burst with a read-complete once no more frames are
    ///     available. This is what frames that arrive outside a read want, i.e. those that a late QPACK
    ///     decode unblocks: there is no `channelReadComplete` coming for them.
    private func deliverFrames(context: ChannelHandlerContext, bytes: ByteBuffer?, completingRead: Bool) {
        // A synchronous QPACK decode result re-enters us from inside the loop below. Bow out: the call we
        // are nested inside is still running and picks up the frames the decode just unblocked. Carrying
        // on here would deliver frames out of order and fire a second read-complete for one read burst.
        guard !self.isDeliveringFrames else {
            // Only the QPACK callback re-enters, and it never brings bytes with it.
            assert(bytes == nil)
            return
        }
        self.isDeliveringFrames = true
        defer { self.isDeliveringFrames = false }

        do {
            // Whatever the state machine is already holding, e.g. the header a decode just completed.
            var readiness = try self.drainStateMachine(context: context)

            // A frame the decoder handed us while the state machine had no room for it.
            if readiness == .ready, let held = self.heldFrame.take() {
                try self.handle(self.stateMachine.frameDecoded(held), context: context)
                readiness = try self.drainStateMachine(context: context)
            }

            // Anything the new bytes complete. Passing an empty buffer resumes decoding whatever the
            // processor still holds from an earlier read.
            if readiness != .closed {
                try self.frameProcessor.process(buffer: bytes ?? ByteBuffer()) { frame in
                    try self.accept(frame, context: context)
                }
                // Whatever the last frame left behind: most often a header whose QPACK decode completed
                // while we were inside `accept`, with no frame behind it to pick it up.
                readiness = try self.drainStateMachine(context: context)
            }

            // The peer has finished sending, and we are no longer blocked, so the decoder can be flushed.
            if self.needsDecoderFlush, readiness != .blockedOnDecode {
                self.needsDecoderFlush = false
                try self.frameProcessor.finishProcessing(seenEOF: true) { frame in
                    try self.accept(frame, context: context)
                }
                try self.handle(self.stateMachine.endOfInput(), context: context)
                _ = try self.drainStateMachine(context: context)
            }
        } catch is StopDecoding {
            // We are waiting on a QPACK decode. The bytes we haven't decoded stay in the processor, and
            // we pick them up again in the call this handler makes when the result arrives.
        } catch let error as HTTP3Error {
            // The peer sent something we can't parse at all. That kills the connection, and nothing more
            // can be read from this stream: we no longer know where a frame would start.
            self.handleIgnoringPause(self.stateMachine.decoderFailed(error), context: context)
        } catch {
            let h3Error = HTTP3Error(
                code: .invalidFramePayload,
                message: "Could not decode incoming frames",
                cause: error,
                errorCode: .generalProtocolError,
                location: .here()
            )
            self.handleIgnoringPause(self.stateMachine.decoderFailed(h3Error), context: context)
        }

        if completingRead {
            self.completeReadIfNeeded(context: context)
        }
    }

    /// Hand a freshly decoded frame to the state machine, or hold onto it if it can't be taken yet.
    private func accept(_ frame: HTTP3DecodedFrame, context: ChannelHandlerContext) throws {
        switch try self.drainStateMachine(context: context) {
        case .ready:
            try self.handle(self.stateMachine.frameDecoded(frame), context: context)
        case .blockedOnDecode:
            // The state machine has nowhere to put this, and it must not overtake the header being
            // decoded. Hold it and stop the processor: the bytes behind it stay where they are.
            assert(self.heldFrame == nil, "Held onto more than one frame")
            self.heldFrame = frame
            throw StopDecoding()
        case .closed:
            // Nothing further will be read from this stream, so this frame goes nowhere.
            throw StopDecoding()
        }
    }

    /// Read out everything the state machine is holding right now.
    ///
    /// - Returns: Whether it can be handed the next frame afterwards.
    private func drainStateMachine(context: ChannelHandlerContext) throws -> Readiness {
        while true {
            let action = self.stateMachine.nextAction()
            switch action {
            case .needMoreBytes:
                return .ready
            case .needDecodeResult:
                return .blockedOnDecode
            case .alreadyClosed, .previousError:
                return .closed
            case .callAgain:
                continue
            case .returnFrame, .inputClosed, .decodeHeader, .emitStreamError, .emitConnectionError:
                try self.handle(action, context: context)
            }
        }
    }

    /// Act on an action that cannot ask us to stop decoding, because we already have.
    private func handleIgnoringPause(
        _ action: HTTP3StreamStateMachine.DecodeNextAction,
        context: ChannelHandlerContext
    ) {
        do {
            try self.handle(action, context: context)
        } catch {
            assertionFailure("Unexpected error from handling \(action): \(error)")
        }
    }

    /// Act on one action from the state machine.
    private func handle(
        _ action: HTTP3StreamStateMachine.DecodeNextAction,
        context: ChannelHandlerContext
    ) throws {
        switch action {
        case .inputClosed(let inputClosedAction):
            switch inputClosedAction {
            case .emitEvent:
                context.fireUserInboundEventTriggered(ChannelEvent.inputClosed)

            case .emitErrorAndEvent(let error):
                context.fireErrorCaught(error)
                context.fireUserInboundEventTriggered(ChannelEvent.inputClosed)

            case .resetStream(let error):
                context.triggerUserOutboundEvent(
                    QUICResetStreamEvent(code: QUICApplicationErrorCode(error.h3ErrorCode ?? .noError)),
                    promise: nil
                )
                context.fireErrorCaught(error)
                context.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
            }
        case .needMoreBytes, .needDecodeResult, .alreadyClosed, .previousError, .callAgain:
            break
        case .returnFrame(let frame):
            self.logger.trace(
                "HTTP3StreamHandler forwarding frame",
                metadata: [LoggingKeys.h3FrameType: "\(frame.type)"]
            )
            context.fireChannelRead(wrapInboundOut(frame))
            self.didFireChannelRead = true
        case .decodeHeader(let partialHeader):
            self.logger.trace("HTTP3StreamHandler waiting for QPACK decode")
            // This call may re-enter us: if the coder can decode the header right away (which is the
            // common case, and always the case when the peer doesn't use the dynamic table) it calls
            // `decodeResult(_:)` before returning, which feeds the result back into the state machine.
            // We deliberately don't handle the result here; the loops above pick it up.
            self.qpackCoder.decodeHeaders(partialHeader, forStream: self.streamID, decodeReceiver: self)
        case .emitStreamError(let error):
            context.triggerUserOutboundEvent(
                QUICStopSendingEvent(code: QUICApplicationErrorCode(error.h3ErrorCode ?? .noError)),
                promise: nil
            )
            context.fireErrorCaught(error)
        case .emitConnectionError(let error):
            self.delegate.onConnectionError(error)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let frame = self.unwrapOutboundIn(data)
        self.logger.trace("HTTP3StreamHandler.write", metadata: [LoggingKeys.h3FrameType: "\(frame.type)"])

        if self.pendingBytes == nil {
            self.pendingBytes = context.channel.allocator.buffer(capacity: 256)
        }

        let action = self.stateMachine.writeFrame(frame: frame, into: &self.pendingBytes!)

        switch action {
        case .previousError:
            // Just drop the byte
            promise?.fail(
                HTTP3Error(
                    code: .previousError,
                    message: "A previous error is preventing further writes",
                    cause: nil,
                    errorCode: nil,
                    location: .here()
                )
            )
        case .wroteBytes:
            self.pendingPromise.setOrCascade(to: promise)
        case .wouldBeStreamError(let error):
            context.fireErrorCaught(error)
            promise?.fail(error)
        case .alreadyClosed:
            context.fireErrorCaught(ChannelError.ioOnClosedChannel)
            promise?.fail(ChannelError.ioOnClosedChannel)
        case .wouldBeConnectionError(let error):
            context.fireErrorCaught(error)
            promise?.fail(error)
        case .encodeHeaders(let fields):
            let encoded = self.qpackCoder.encodeHeaders(fields, forStream: self.streamID)
            let action = self.stateMachine.gotHeaderEncodeResult(encoded, from: fields, into: &self.pendingBytes!)

            switch action {
            case .previousError(let previousError):
                promise?.fail(
                    HTTP3Error(
                        code: .previousError,
                        message: "A previous error is preventing further writes",
                        cause: previousError,
                        errorCode: nil,
                        location: .here()
                    )
                )
            case .wroteBytes:
                self.pendingPromise.setOrCascade(to: promise)
            case .alreadyClosed:
                promise?.fail(ChannelError.ioOnClosedChannel)
            }
        }
    }

    func flush(context: ChannelHandlerContext) {
        self.emitPendingBytes(context: context)
        context.flush()
    }

    func close(
        context: ChannelHandlerContext,
        mode: CloseMode,
        promise: EventLoopPromise<Void>?
    ) {
        switch mode {
        case .output, .all:
            self.emitPendingBytes(context: context)
        case .input:
            ()
        }
        context.close(mode: mode, promise: promise)
    }

    /// Write any pending bytes.
    private func emitPendingBytes(context: ChannelHandlerContext) {
        if let bytes = self.pendingBytes.take() {
            let promise = self.pendingPromise.take()
            context.write(HTTP3StreamHandler.wrapOutboundOut(bytes), promise: promise)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        switch error {
        case let error as QUICStreamResetError:
            self.logger.trace("Caught RESET_STREAM")
            let action = self.stateMachine.streamErrorCaught(errorCode: error.code)
            switch action {
            case .emitStreamError(let newError):
                context.fireErrorCaught(newError)
            case .none:
                break
            }
        case let error as QUICStopSendingError:
            self.logger.trace("Caught STOP_SENDING")
            let action = self.stateMachine.streamErrorCaught(errorCode: error.code)
            switch action {
            case .emitStreamError(let newError):
                context.fireErrorCaught(newError)
            case .none:
                break
            }
        case let error as QUICConnectionError:
            self.logger.trace("Caught CONNECTION_CLOSE")
            context.fireErrorCaught(
                HTTP3Error(
                    code: .remoteConnectionError,
                    message: error.reason,
                    cause: error,
                    errorCode: error.isApplication ? HTTP3ErrorCode(rawValue: error.code) : nil,
                    location: .here()
                )
            )
        default:
            context.fireErrorCaught(error)
        }
    }

    /// Call this when `header` has been decoded.
    func onQPACKDecodeResult(fields: [HTTPField]) {
        self.logger.trace("HTTP3StreamHandler.onQPACKDecodeResult")
        guard let context = self.context else {
            // The stream must have been created and registered to get QPACK events and thus already have
            // the context available. Since pending decodes are dropped when the stream closes it must
            // still be open and active.
            fatalError("Tried to deliver QPACK results before handler was added")
        }
        self.stateMachine.gotHeaderDecodeResult(fields)
        // Read out as much as this unblocked, and close off the burst: no `channelReadComplete` is coming
        // for frames delivered this late. If the decode completed synchronously we are being called from
        // within that very loop, in which case this is a no-op and the loop we are nested inside reads.
        self.deliverFrames(context: context, bytes: nil, completingRead: true)
    }

    /// Call this if an error is encountered whilst trying to decode `header`.
    func onQPACKDecodeError(_ error: HTTP3Error) {
        guard let context = self.context else {
            // The stream must have been created an registered to get QPACK events and thus already have
            // the context available. Since pending decodes are dropped when the stream closes it must
            // still be open and active.
            fatalError("Tried to deliver QPACK error before handler was set")
        }
        self.stateMachine.gotHeaderDecodeError(error)
        // As in `onQPACKDecodeResult`, this is a no-op when the coder failed the decode synchronously.
        self.deliverFrames(context: context, bytes: nil, completingRead: true)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if (event as? ChannelEvent) == ChannelEvent.inputClosed {
            // We don't pass this through immediately, we buffer it behind any buffered reads to prevent overtaking.
            self.logger.trace("HTTP3StreamHandler intercepted inputClosed")
            self.stateMachine.inputClosed()
            // The event only goes downstream once everything queued ahead of it has been read out, which
            // includes flushing the decoder to find a truncated final frame.
            self.needsDecoderFlush = true
            self.deliverFrames(context: context, bytes: nil, completingRead: false)
        } else {
            // Pass it through
            context.fireUserInboundEventTriggered(event)
        }
    }

    /// A GOAWAY frame was sent with an ID lower than or equal to that of this stream.
    /// I.e., we will NOT process this stream, and we should just close it.
    func cancelStreamDueToSendingGoaway() {
        guard let context = self.context else {
            assertionFailure("Tried to send cancel stream before handler was added")
            return
        }

        @inline(never)
        func streamCancelledDueToSendingGoawayError(location: HTTP3Error.SourceLocation) -> HTTP3Error {
            HTTP3Error(
                code: .rejected,
                message: "Stream cancelled due to GOAWAY",
                cause: nil,
                errorCode: .requestRejected,
                location: location
            )
        }
        self.logger.trace("Sending goaway, closing stream")
        let error = streamCancelledDueToSendingGoawayError(location: .here())
        self.triggerUserOutboundEvent(
            context: context,
            event: QUICResetStreamEvent(code: QUICApplicationErrorCode(error.h3ErrorCode!)),
            promise: nil
        )
        context.fireErrorCaught(error)
    }

    /// A GOAWAY frame was received with an ID lower than or equal to that of this stream.
    /// I.e., the remote will NOT process this stream, and we should just close it.
    func cancelStreamDueToReceivedGoaway() {
        guard let context = self.context else {
            assertionFailure("Tried to propagate stream cancelation before handler was added")
            return
        }
        @inline(never)
        func streamCancelledDueToReceivedGoawayError(location: HTTP3Error.SourceLocation) -> HTTP3Error {
            HTTP3Error(
                code: .rejected,
                message: "Stream cancelled due to GOAWAY",
                cause: nil,
                errorCode: nil,  // This error isn't being sent to remote, so code is not relevant.
                location: location
            )
        }
        self.logger.trace("Received goaway, closing stream")
        let error = streamCancelledDueToReceivedGoawayError(location: .here())
        context.fireErrorCaught(error)
        // Defer close to ensure error propagates first
        let loopBoundContext = NIOLoopBound.init(context, eventLoop: context.eventLoop)
        context.eventLoop.execute {
            loopBoundContext.value.close(mode: .all, promise: nil)
        }
    }

    /// The remote closed the connection (CONNECTION_CLOSE). All active streams must be cancelled.
    func cancelStreamDueToConnectionClose() {
        guard let context = self.context else {
            assertionFailure("Tried to cancel stream before handler was added")
            return
        }
        @inline(never)
        func streamCancelledDueToConnectionCloseError(location: HTTP3Error.SourceLocation) -> HTTP3Error {
            HTTP3Error(
                code: .remoteConnectionError,
                message: "Stream cancelled due to connection close",
                cause: nil,
                errorCode: nil,
                location: location
            )
        }
        self.logger.trace("Connection closed, closing stream")
        let error = streamCancelledDueToConnectionCloseError(location: .here())
        context.fireErrorCaught(error)
        // Defer close to ensure error propagates first
        let loopBoundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        context.eventLoop.execute {
            loopBoundContext.value.close(mode: .all, promise: nil)
        }
    }
}

@available(*, unavailable)
extension HTTP3StreamHandler: Sendable {}

extension HTTP3StreamHandler: QPACKDecodeReceiver {
    /// The QPACK coder calls this with the outcome of a decode we asked for.
    ///
    /// This is called either synchronously from inside our own call to `decodeHeaders`, when the coder
    /// had everything it needed, or later on, from whichever QPACK encoder-stream instruction unblocked
    /// the decode. Both paths must work; see `isDrainingFrames`.
    func decodeResult(_ result: Result<[HTTPTypes.HTTPField], HTTP3Error>) {
        switch result {
        case .success(let fields):
            self.onQPACKDecodeResult(fields: fields)
        case .failure(let error):
            self.onQPACKDecodeError(error)
        }
    }
}
