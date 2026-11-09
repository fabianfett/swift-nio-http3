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

/// Validates incoming and outgoing frames to be a valid sequence for a given stream type.
@available(anyAppleOS 27, *)
enum HTTP3FrameValidator: ~Copyable {
    case incomingControlStream(IncomingControlStreamValidator)
    case outgoingControlStream(OutgoingControlStreamValidator)
    case incomingPushStream(IncomingPushStreamValidator)
    case outgoingPushStream(OutgoingPushStreamValidator)
    /// A request stream on a server.
    case incomingRequestStream(RequestStreamValidator)
    /// A request stream on a client.
    case outgoingRequestStream(RequestStreamValidator)

    /// Checks the order of frames is valid for a control stream.
    /// I.e. first the settings, and then anything but settings after that.
    /// This works for outbound and inbound streams.
    struct ControlStreamSequenceValidator: ~Copyable {
        private enum State: ~Copyable {
            case awaitingSettings
            case gotSettings
            case previousError
        }

        private let state: State

        init() {
            self.init(state: .awaitingSettings)
        }

        private init(state: consuming State) {
            self.state = state
        }

        mutating func processUnknownFrame() -> UnknownFrameAction {
            // RFC 9114 § 9: where a known frame type is required to be in a specific location, such as the SETTINGS frame as the first frame of
            // the control stream (see Section 6.2.1), an unknown frame type does not satisfy that requirement and SHOULD be treated as an error.
            switch consume self.state {
            case .awaitingSettings:
                self = .init(state: .previousError)
                return .emitConnectionError(
                    HTTP3Error(
                        code: .firstControlFrameNotSettings,
                        message: "Expected settings, got unknown",
                        cause: nil,
                        errorCode: .missingSettings,
                        location: .here()
                    )
                )
            case .gotSettings:
                self = .init(state: .gotSettings)
                return .dropFrame
            case .previousError:
                self = .init(state: .previousError)
                return .previousError
            }
        }

        mutating func processNextFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            // RFC 9114 § 7.2.4: A SETTINGS frame MUST be sent as the first frame of each control stream (see Section 6.2.1) by each peer, and it MUST NOT be sent subsequently.
            // RFC 9114 § 7: Only CANCEL_PUSH, GOAWAY and MAX_PUSH_ID are allowed after the first settings frame.
            switch consume self.state {
            case .awaitingSettings:
                switch frame {
                case .settings:
                    self = .init(state: .gotSettings)
                    return .forwardFrame(frame)
                case .cancelPush, .goaway, .maxPushID, .data, .headers, .pushPromise:
                    self = .init(state: .previousError)
                    // If the first frame of the control stream is any other frame type, this MUST be treated as a connection error of type H3_MISSING_SETTINGS
                    return .emitConnectionError(
                        HTTP3Error(
                            code: .firstControlFrameNotSettings,
                            message: "Expected settings, got \(frame.type)",
                            cause: nil,
                            errorCode: .missingSettings,
                            location: .here()
                        )
                    )
                }
            case .gotSettings:
                switch frame {
                case .data, .headers, .pushPromise:
                    self = .init(state: .previousError)
                    return .emitConnectionError(
                        HTTP3Error(
                            code: .unexpectedFrame,
                            message: "Expected cancelPush or goaway or maxPushID, got \(frame.type)",
                            cause: nil,
                            errorCode: .frameUnexpected,
                            location: .here()
                        )
                    )
                case .settings:
                    // if we already got settings, we must not get settings again
                    @inline(never)
                    func duplicateSettingsError(location: HTTP3Error.SourceLocation) -> HTTP3Error {
                        HTTP3Error(
                            code: .unexpectedFrame,
                            message: "Received a second settings frame",
                            cause: nil,
                            errorCode: .frameUnexpected,
                            location: location
                        )
                    }
                    self = .init(state: .previousError)
                    return .emitConnectionError(duplicateSettingsError(location: .here()))
                case .cancelPush, .goaway, .maxPushID:
                    self = .init(state: .gotSettings)
                    return .forwardFrame(frame)
                }
            case .previousError:
                self = .init(state: .previousError)
                return .previousError
            }
        }
    }

    /// Checks the order of frames is valid for a push stream.
    /// TODO: https://github.com/apple/swift-nio-http3/issues/1
    /// Not implemented yet,  only validates the frame types. This works for outbound and inbound streams.
    struct PushStreamSequenceValidator: ~Copyable {
        private enum State: ~Copyable {
            case none
            case previousError
        }

        private let state: State

        private init(state: consuming State) {
            self.state = state
        }

        init() {
            self.init(state: .none)
        }

        mutating func processNextFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            // For now, we are only implementing the frame types which are allowed
            // RFC 9114 § 7: Only headers and data frames are allowed in push streams
            switch consume self.state {
            case .none:
                switch frame {
                case .headers, .data:
                    self = .init(state: .none)
                    return .forwardFrame(frame)
                case .cancelPush, .goaway, .maxPushID, .settings, .pushPromise:
                    self = .init(state: .previousError)
                    return .emitConnectionError(
                        HTTP3Error(
                            code: .unexpectedFrame,
                            message: "Expected headers or data, got \(frame.type)",
                            cause: nil,
                            errorCode: .frameUnexpected,
                            location: .here()
                        )
                    )
                }
            case .previousError:
                self = .init(state: .previousError)
                return .previousError
            }
        }
    }

    /// The action to take when the inbound side of a stream closes.
    enum InboundClosedAction {
        /// We received the full request or response before the inbound closed. As such, there is nothing to do.
        case doNothing

        /// The inbound closed before we received the full response. The downstream should be notified so they can
        /// decide what to do with the partial response.
        case notifyDownstream(HTTP3Error)

        /// The inbound closed before we received the full request. Per RFC 9114 § 4.1, the server should abort the
        /// response stream by sending a RESET_STREAM frame.
        case resetStream(HTTP3Error)
    }

    /// Checks the order of frames read and written is correct for a request stream, by mapping them to the message
    /// events of a ``HTTP3RequestStreamValidator``.
    ///
    /// For a server, the request frames are inbound and the response frames are outbound. This is reversed for a
    /// client.
    ///
    /// Unlike the other validators, this one doesn't remember errors: ``HTTP3StreamStateMachine`` stops using the
    /// validator once it reported one.
    struct RequestStreamValidator {
        /// A frame type the frame decoder doesn't know. The decoder doesn't keep the type of an unknown frame, but any
        /// will do: they are all treated the same (RFC 9114 § 9). This one is reserved (RFC 9114 § 7.2.8).
        private static let unknownFrameType = HTTP3FrameType.unknown(type: 0x21)

        private var validator: HTTP3RequestStreamValidator

        init(isServer: Bool) {
            self.validator = HTTP3RequestStreamValidator(isServer: isServer)
        }

        /// Validates an inbound frame.
        mutating func processInboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            switch self.validator.receivedFrame(frame.type) {
            case .decodeHead:
                // The channel API validates HEADERS frames after QPACK decoding them, so the head's fields are already
                // here. Only a client receives (interim) responses.
                let informational: Bool
                if !self.validator.isServer, case .headers(let headers) = frame {
                    informational = headers.representsInterimResponse
                } else {
                    informational = false
                }
                self.validator.receivedHead(informational: informational)
                return .forwardFrame(frame)
            case .decodeTrailers, .body:
                return .forwardFrame(frame)
            case .ignore:
                preconditionFailure("An HTTP3Frame is never of an unknown type")
            case .connectionError(let error):
                return .emitConnectionError(error)
            case .streamError(let error):
                return .emitStreamError(error)
            }
        }

        /// Validates an outbound frame.
        mutating func processOutboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            do throws(HTTP3Error) {
                switch frame {
                case .headers(let headers):
                    if self.validator.outboundAwaitsHead {
                        // Only a server writes (interim) responses.
                        try self.validator.willWriteHead(
                            informational: self.validator.isServer && headers.representsInterimResponse
                        )
                    } else {
                        try self.validator.willWriteTrailers()
                    }
                case .data:
                    try self.validator.willWriteBody()
                case .cancelPush, .goaway, .maxPushID, .settings, .pushPromise:
                    // RFC 9114 § 7.2: these frames are only allowed on the control stream. PUSH_PROMISE frames are
                    // allowed on request streams, but this implementation doesn't support server push.
                    throw Self.unwritableFrameError(type: frame.type, location: .here())
                }
                return .forwardFrame(frame)
            } catch {
                return .emitStreamError(error)
            }
        }

        /// Validates an inbound frame of an unknown type.
        mutating func processInboundUnknownFrame() -> UnknownFrameAction {
            switch self.validator.receivedFrame(Self.unknownFrameType) {
            case .ignore:
                return .dropFrame
            case .decodeHead, .decodeTrailers, .body, .connectionError, .streamError:
                preconditionFailure("Frames of unknown types are always ignored")
            }
        }

        /// Records that the inbound side of the stream has closed.
        mutating func processInboundClosed() -> InboundClosedAction {
            switch self.validator.receivedEnd() {
            case .complete:
                return .doNothing
            case .incompleteRequest(let error):
                return .resetStream(error)
            case .incompleteResponse(let error):
                return .notifyDownstream(error)
            }
        }

        @inline(never)
        private static func unwritableFrameError(
            type: HTTP3FrameType,
            location: HTTP3Error.SourceLocation
        ) -> HTTP3Error {
            HTTP3Error(
                code: .unexpectedFrame,
                message: "A \(type) frame can't be written on a request stream",
                cause: nil,
                errorCode: nil,
                location: location
            )
        }
    }

    /// Checks that incoming frames are in the right order for a control stream, and that there are no outgoing frames.
    struct IncomingControlStreamValidator: ~Copyable {
        private var sequenceValidator = ControlStreamSequenceValidator()

        mutating func processInboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            // The only thing to check is the ordering, we don't need to do anything more than the sequence validator
            self.sequenceValidator.processNextFrame(frame)
        }

        mutating func processOutboundFrame(_: HTTP3Frame) -> ProcessFrameAction {
            .emitConnectionError(
                HTTP3Error(
                    code: .invalidStream,
                    message: "Tried to write on an incoming unidirectional control stream",
                    cause: nil,
                    errorCode: nil,
                    location: .here()
                )
            )
        }

        mutating func processInboundUnknownFrame() -> UnknownFrameAction {
            self.sequenceValidator.processUnknownFrame()
        }
    }

    /// Checks that outgoing frames are in the right order for a control stream, and there are no incoming frames.
    struct OutgoingControlStreamValidator: ~Copyable {
        private var sequenceValidator = ControlStreamSequenceValidator()

        mutating func processInboundFrame(_: HTTP3Frame) -> ProcessFrameAction {
            .emitConnectionError(
                HTTP3Error(
                    code: .invalidStream,
                    message: "Tried to read on an outgoing unidirectional control stream",
                    cause: nil,
                    errorCode: nil,
                    location: .here()
                )
            )
        }

        mutating func processOutboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            // The only thing to check is the ordering, we don't need to do anything more than the sequence validator
            self.sequenceValidator.processNextFrame(frame)
        }

        mutating func processInboundUnknownFrame() -> UnknownFrameAction {
            .emitConnectionError(
                HTTP3Error(
                    code: .invalidStream,
                    message: "Tried to read on an outgoing unidirectional control stream",
                    cause: nil,
                    errorCode: nil,
                    location: .here()
                )
            )
        }
    }

    /// Checks that incoming frames are in the right order for a push stream, and there are no outgoing frames.
    struct IncomingPushStreamValidator: ~Copyable {
        private var sequenceValidator = PushStreamSequenceValidator()

        mutating func processInboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            // The only thing to check is the ordering, we don't need to do anything more than the sequence validator
            self.sequenceValidator.processNextFrame(frame)
        }

        mutating func processOutboundFrame(_: HTTP3Frame) -> ProcessFrameAction {
            .emitConnectionError(
                HTTP3Error(
                    code: .invalidStream,
                    message: "Tried to write on an incoming unidirectional push stream",
                    cause: nil,
                    errorCode: nil,
                    location: .here()
                )
            )
        }
    }

    /// Checks that outgoing frames are in the right order for a push stream, and there are no incoming frames.
    struct OutgoingPushStreamValidator: ~Copyable {
        private var sequenceValidator = PushStreamSequenceValidator()

        mutating func processInboundFrame(_: HTTP3Frame) -> ProcessFrameAction {
            .emitConnectionError(
                HTTP3Error(
                    code: .invalidStream,
                    message: "Tried to read on an outgoing unidirectional push stream",
                    cause: nil,
                    errorCode: nil,
                    location: .here()
                )
            )
        }

        mutating func processOutboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
            // The only thing to check is the ordering, we don't need to do anything more than the sequence validator
            self.sequenceValidator.processNextFrame(frame)
        }
    }

    init(streamType: HTTP3StreamType.Framed, incoming: Bool) {
        switch (streamType, incoming) {
        case (.control, true): self = .incomingControlStream(.init())
        case (.request, true): self = .incomingRequestStream(.init(isServer: true))
        case (.push, true): self = .incomingPushStream(.init())
        case (.control, false): self = .outgoingControlStream(.init())
        case (.request, false): self = .outgoingRequestStream(.init(isServer: false))
        case (.push, false): self = .outgoingPushStream(.init())
        }
    }

    enum ProcessFrameAction {
        case forwardFrame(HTTP3Frame)
        case emitConnectionError(HTTP3Error)
        case emitStreamError(HTTP3Error)
        case previousError
    }

    mutating func processInboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
        switch consume self {
        case .incomingControlStream(var v):
            let result = v.processInboundFrame(frame)
            self = .incomingControlStream(v)
            return result
        case .outgoingControlStream(var v):
            let result = v.processInboundFrame(frame)
            self = .outgoingControlStream(v)
            return result
        case .incomingRequestStream(var v):
            let result = v.processInboundFrame(frame)
            self = .incomingRequestStream(v)
            return result
        case .outgoingRequestStream(var v):
            let result = v.processInboundFrame(frame)
            self = .outgoingRequestStream(v)
            return result
        case .incomingPushStream(var v):
            let result = v.processInboundFrame(frame)
            self = .incomingPushStream(v)
            return result
        case .outgoingPushStream(var v):
            let result = v.processInboundFrame(frame)
            self = .outgoingPushStream(v)
            return result
        }
    }

    mutating func processOutboundFrame(_ frame: HTTP3Frame) -> ProcessFrameAction {
        switch consume self {
        case .incomingControlStream(var v):
            let result = v.processOutboundFrame(frame)
            self = .incomingControlStream(v)
            return result
        case .outgoingControlStream(var v):
            let result = v.processOutboundFrame(frame)
            self = .outgoingControlStream(v)
            return result
        case .incomingRequestStream(var v):
            let result = v.processOutboundFrame(frame)
            self = .incomingRequestStream(v)
            return result
        case .outgoingRequestStream(var v):
            let result = v.processOutboundFrame(frame)
            self = .outgoingRequestStream(v)
            return result
        case .incomingPushStream(var v):
            let result = v.processOutboundFrame(frame)
            self = .incomingPushStream(v)
            return result
        case .outgoingPushStream(var v):
            let result = v.processOutboundFrame(frame)
            self = .outgoingPushStream(v)
            return result
        }
    }

    enum UnknownFrameAction {
        case dropFrame
        case emitConnectionError(HTTP3Error)
        case previousError
    }

    mutating func processInboundUnknownFrame() -> UnknownFrameAction {
        // Only the control stream cares about unknown frames, the other streams can just drop it.
        switch consume self {
        case .incomingControlStream(var v):
            let result = v.processInboundUnknownFrame()
            self = .incomingControlStream(v)
            return result
        case .outgoingControlStream(var v):
            let result = v.processInboundUnknownFrame()
            self = .outgoingControlStream(v)
            return result
        case .incomingRequestStream(var v):
            let result = v.processInboundUnknownFrame()
            self = .incomingRequestStream(v)
            return result
        case .outgoingRequestStream(var v):
            let result = v.processInboundUnknownFrame()
            self = .outgoingRequestStream(v)
            return result
        case .incomingPushStream(let v):
            self = .incomingPushStream(v)
            return .dropFrame
        case .outgoingPushStream(let v):
            self = .outgoingPushStream(v)
            return .dropFrame
        }
    }

    /// Records that the inbound side of the stream has closed.
    mutating func processInboundClosed() -> InboundClosedAction {
        switch consume self {
        case .incomingRequestStream(var validator):
            let result = validator.processInboundClosed()
            self = .incomingRequestStream(validator)
            return result

        case .outgoingRequestStream(var validator):
            let result = validator.processInboundClosed()
            self = .outgoingRequestStream(validator)
            return result

        case .incomingControlStream(let validator):
            self = .incomingControlStream(validator)
            return .doNothing

        case .outgoingControlStream(let validator):
            self = .outgoingControlStream(validator)
            return .doNothing

        case .incomingPushStream(let validator):
            self = .incomingPushStream(validator)
            return .doNothing

        case .outgoingPushStream(let validator):
            self = .outgoingPushStream(validator)
            return .doNothing
        }
    }
}

@available(anyAppleOS 27, *)
extension HTTP3Frame {
    @_spi(PackageInternal)
    public var type: HTTP3FrameType {
        switch self {
        case .headers: return .headers
        case .data: return .data
        case .cancelPush: return .cancelPush
        case .settings: return .settings
        case .goaway: return .goaway
        case .maxPushID: return .maxPushID
        case .pushPromise: return .pushPromise
        }
    }
}
