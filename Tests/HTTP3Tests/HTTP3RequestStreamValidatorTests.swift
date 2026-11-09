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

import Testing

@_spi(PackageInternal) @testable import HTTP3

/// How far the inbound message has got.
enum InboundProgress: CaseIterable, Sendable {
    case awaitingHead
    case awaitingBodyOrTrailers
    case ended
}

struct HTTP3RequestStreamValidatorTests {
    /// The frame types which are only allowed on the control stream.
    @available(anyAppleOS 27, *)
    static var controlFrameTypes: [HTTP3FrameType] {
        [.settings, .goaway, .maxPushID, .cancelPush]
    }

    /// A validator whose inbound message got as far as `progress`. A client has written its request head, so that it
    /// may receive a response.
    @available(anyAppleOS 27, *)
    static func makeValidator(isServer: Bool, inbound progress: InboundProgress) -> HTTP3RequestStreamValidator {
        var validator = HTTP3RequestStreamValidator(isServer: isServer)
        #expect(validator.isServer == isServer)
        if !isServer {
            #expect(throws: Never.self) { try validator.willWriteHead(informational: false) }
        }
        switch progress {
        case .awaitingHead:
            break
        case .awaitingBodyOrTrailers:
            validator.expectReceivedHead()
        case .ended:
            validator.expectReceivedHead()
            #expect(validator.receivedFrame(.headers).kind == .decodeTrailers)
        }
        return validator
    }

    /// A server validator which received the request head, so that it may write a response.
    @available(anyAppleOS 27, *)
    static func makeValidator(isServer: Bool) -> HTTP3RequestStreamValidator {
        var validator = HTTP3RequestStreamValidator(isServer: isServer)
        if isServer {
            validator.expectReceivedHead()
        }
        return validator
    }

    // MARK: Inbound

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func inboundMessage(isServer: Bool) throws {
        var validator = Self.makeValidator(isServer: isServer, inbound: .awaitingHead)

        try #require(validator.receivedFrame(.headers).kind == .decodeHead)
        validator.receivedHead(informational: false)
        for _ in 0..<3 {
            #expect(validator.receivedFrame(.data).kind == .body)
        }
        #expect(validator.receivedFrame(.headers).kind == .decodeTrailers)

        // The trailers ended the message.
        let action = validator.receivedFrame(.data)
        expectH3ErrorEqual(
            error: action.connectionError,
            expectedCode: .unexpectedFrame,
            expectedH3ErrorCode: .frameUnexpected,
            expectedMessage: "Expected no further frames after \(isServer ? "request" : "response") trailers, got data"
        )
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func inboundMessageWithoutBody(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer, inbound: .awaitingHead)
        validator.expectReceivedHead()
        #expect(validator.receivedFrame(.headers).kind == .decodeTrailers)
        #expect(validator.receivedEnd().isComplete)
    }

    /// The head is only recorded by `receivedHead(informational:)`, not by `receivedFrame(_:)`.
    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func headStateOnlyMovesOnceReceived(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer, inbound: .awaitingHead)
        #expect(validator.receivedFrame(.headers).kind == .decodeHead)
        #expect(validator.receivedFrame(.headers).kind == .decodeHead)
        #expect(validator.receivedFrame(.data).kind == .connectionError)
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func dataBeforeHead(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer, inbound: .awaitingHead)
        let action = validator.receivedFrame(.data)
        expectH3ErrorEqual(
            error: action.connectionError,
            expectedCode: .unexpectedFrame,
            expectedH3ErrorCode: .frameUnexpected,
            expectedMessage: "Expected headers, got data"
        )

        // The rejected frame left the state as it was.
        #expect(validator.receivedFrame(.headers).kind == .decodeHead)
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func headersAfterTrailers(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer, inbound: .ended)
        let action = validator.receivedFrame(.headers)
        expectH3ErrorEqual(
            error: action.connectionError,
            expectedCode: .unexpectedFrame,
            expectedH3ErrorCode: .frameUnexpected,
            expectedMessage:
                "Expected no further frames after \(isServer ? "request" : "response") trailers, got headers"
        )
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false], InboundProgress.allCases)
    func controlFrames(isServer: Bool, progress: InboundProgress) {
        let expected =
            switch progress {
            case .awaitingHead: "headers"
            case .awaitingBodyOrTrailers: "headers or data"
            case .ended: "no further frames after \(isServer ? "request" : "response") trailers"
            }
        for type in Self.controlFrameTypes {
            var validator = Self.makeValidator(isServer: isServer, inbound: progress)
            let action = validator.receivedFrame(type)
            expectH3ErrorEqual(
                error: action.connectionError,
                expectedCode: .unexpectedFrame,
                expectedH3ErrorCode: .frameUnexpected,
                expectedMessage: "Expected \(expected), got \(type)"
            )
        }
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false], InboundProgress.allCases)
    func pushPromise(isServer: Bool, progress: InboundProgress) {
        var validator = Self.makeValidator(isServer: isServer, inbound: progress)
        let action = validator.receivedFrame(.pushPromise)
        if isServer {
            // A client must never send a PUSH_PROMISE frame.
            expectH3ErrorEqual(
                error: action.connectionError,
                expectedCode: .unexpectedFrame,
                expectedH3ErrorCode: .frameUnexpected
            )
        } else {
            // We never send MAX_PUSH_ID, so the push ID is always one we didn't allow.
            expectH3ErrorEqual(
                error: action.connectionError,
                expectedCode: .unexpectedFrame,
                expectedH3ErrorCode: .idError,
                expectedMessage: "Unexpected push promise"
            )
        }
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false], InboundProgress.allCases)
    func unknownFramesAreIgnored(isServer: Bool, progress: InboundProgress) {
        var validator = Self.makeValidator(isServer: isServer, inbound: progress)
        let dataBefore = validator.receivedFrame(.data).kind
        #expect(validator.receivedFrame(.unknown(type: 0x21)).kind == .ignore)
        #expect(validator.receivedFrame(.unknown(type: 0xdb)).kind == .ignore)
        // They don't change the state either.
        #expect(validator.receivedFrame(.data).kind == dataBefore)
    }

    /// Unknown frames don't satisfy the need for a request head, so a client still can't get a response first.
    @available(anyAppleOS 27, *)
    @Test func unknownFramesBeforeTheRequest() {
        var validator = HTTP3RequestStreamValidator(isServer: false)
        #expect(validator.receivedFrame(.unknown(type: 0x21)).kind == .ignore)
        #expect(validator.receivedFrame(.headers).kind == .streamError)
    }

    @available(anyAppleOS 27, *)
    @Test func inboundInterimResponses() throws {
        var validator = Self.makeValidator(isServer: false, inbound: .awaitingHead)
        for _ in 0..<3 {
            // Stop here otherwise: only a HEADERS frame announced by `.decodeHead` may be received as a head.
            try #require(validator.receivedFrame(.headers).kind == .decodeHead)
            validator.receivedHead(informational: true)
        }

        // Interim responses have no body: the final head is still to come.
        expectH3ErrorEqual(
            error: validator.receivedFrame(.data).connectionError,
            expectedCode: .unexpectedFrame,
            expectedH3ErrorCode: .frameUnexpected,
            expectedMessage: "Expected headers, got data"
        )

        try #require(validator.receivedFrame(.headers).kind == .decodeHead)
        validator.receivedHead(informational: false)
        #expect(validator.receivedFrame(.data).kind == .body)
        #expect(validator.receivedFrame(.headers).kind == .decodeTrailers)
    }

    @available(anyAppleOS 27, *)
    @Test func inboundResponseBeforeRequest() {
        var validator = HTTP3RequestStreamValidator(isServer: false)
        let action = validator.receivedFrame(.headers)
        expectH3ErrorEqual(
            error: action.streamError,
            expectedCode: .malformedMessage,
            expectedH3ErrorCode: .messageError,
            expectedMessage: "A HTTP response was sent before a request"
        )

        // Once the request head is written, the response may come.
        #expect(throws: Never.self) { try validator.willWriteHead(informational: false) }
        #expect(validator.receivedFrame(.headers).kind == .decodeHead)
    }

    /// A server receives the request whether or not it has written anything.
    @available(anyAppleOS 27, *)
    @Test func serverReceivesRequestBeforeWriting() {
        var validator = HTTP3RequestStreamValidator(isServer: true)
        #expect(validator.receivedFrame(.headers).kind == .decodeHead)
    }

    // MARK: Inbound end

    @available(anyAppleOS 27, *)
    @Test func requestEndsBeforeHead() {
        var validator = Self.makeValidator(isServer: true, inbound: .awaitingHead)
        guard case .incompleteRequest(let error) = validator.receivedEnd() else {
            Issue.record("Expected an incomplete request")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .peerTerminatedInboundStream,
            expectedH3ErrorCode: .requestIncomplete,
            expectedMessage: "Inbound request stream closed before a complete request was received"
        )
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: 0...2)
    func responseEndsBeforeFinalHead(interimResponses: Int) throws {
        var validator = Self.makeValidator(isServer: false, inbound: .awaitingHead)
        for _ in 0..<interimResponses {
            // Stop here otherwise: only a HEADERS frame announced by `.decodeHead` may be received as a head.
            try #require(validator.receivedFrame(.headers).kind == .decodeHead)
            validator.receivedHead(informational: true)
        }
        guard case .incompleteResponse(let error) = validator.receivedEnd() else {
            Issue.record("Expected an incomplete response")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .peerTerminatedInboundStream,
            expectedH3ErrorCode: nil,
            expectedMessage: "Inbound response stream closed before a complete response was received"
        )
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func messageEndsAfterHead(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer, inbound: .awaitingBodyOrTrailers)
        #expect(validator.receivedEnd().isComplete)
        // The message is over.
        #expect(validator.receivedFrame(.data).kind == .connectionError)
        #expect(validator.receivedEnd().isComplete)
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func messageEndsAfterTrailers(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer, inbound: .ended)
        #expect(validator.receivedEnd().isComplete)
    }

    /// The end of the inbound message doesn't stop the outbound one.
    @available(anyAppleOS 27, *)
    @Test func serverWritesAfterTheRequestEnded() {
        var validator = Self.makeValidator(isServer: true, inbound: .awaitingBodyOrTrailers)
        #expect(validator.receivedEnd().isComplete)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
            try validator.willWriteBody()
            try validator.willEnd()
        }
        #expect(validator.outboundHasEnded)
    }

    // MARK: Outbound

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func outboundMessageWithTrailers(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer)
        #expect(validator.outboundAwaitsHead)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
        }
        #expect(!validator.outboundAwaitsHead)
        #expect(throws: Never.self) {
            try validator.willWriteBody()
            try validator.willWriteBody()
        }
        #expect(!validator.outboundHasEnded)
        #expect(throws: Never.self) {
            try validator.willWriteTrailers()
        }
        #expect(validator.outboundHasEnded)
        #expect(!validator.outboundAwaitsHead)

        // The trailers ended the message.
        expectMisuse("A head was already written") { () throws(HTTP3Error) in
            try validator.willWriteHead(informational: false)
        }
        expectMisuse("The message already ended") { () throws(HTTP3Error) in try validator.willWriteBody() }
        expectMisuse("The message already ended") { () throws(HTTP3Error) in try validator.willWriteTrailers() }
        expectMisuse("The message already ended") { () throws(HTTP3Error) in try validator.willEnd() }
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func outboundMessageWithoutTrailers(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
            try validator.willEnd()
        }
        #expect(validator.outboundHasEnded)

        expectMisuse("The message already ended") { () throws(HTTP3Error) in try validator.willWriteBody() }
        expectMisuse("The message already ended") { () throws(HTTP3Error) in try validator.willWriteTrailers() }
        expectMisuse("The message already ended") { () throws(HTTP3Error) in try validator.willEnd() }
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func outboundPartsBeforeHead(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer)
        expectMisuse("The head must be written before the body") { () throws(HTTP3Error) in
            try validator.willWriteBody()
        }
        expectMisuse("The head must be written before the end") { () throws(HTTP3Error) in
            try validator.willWriteTrailers()
        }
        expectMisuse("The head must be written before the end") { () throws(HTTP3Error) in try validator.willEnd() }
        #expect(!validator.outboundHasEnded)

        // The rejected parts left the state as it was.
        #expect(validator.outboundAwaitsHead)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
            try validator.willWriteBody()
        }
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func secondFinalHead(isServer: Bool) {
        var validator = Self.makeValidator(isServer: isServer)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
            try validator.willWriteBody()
        }
        expectMisuse("A head was already written") { () throws(HTTP3Error) in
            try validator.willWriteHead(informational: false)
        }
        // Trailers may still follow.
        #expect(throws: Never.self) {
            try validator.willWriteTrailers()
        }
    }

    @available(anyAppleOS 27, *)
    @Test func outboundInterimResponses() {
        var validator = Self.makeValidator(isServer: true)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: true)
            try validator.willWriteHead(informational: true)
        }
        #expect(validator.outboundAwaitsHead)

        // Interim responses have no body: the final head is still to come.
        expectMisuse("The head must be written before the body") { () throws(HTTP3Error) in
            try validator.willWriteBody()
        }

        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
            try validator.willWriteBody()
        }

        // No interim response may follow the final one.
        expectMisuse("A head was already written") { () throws(HTTP3Error) in
            try validator.willWriteHead(informational: true)
        }
    }

    @available(anyAppleOS 27, *)
    @Test func requestsAreNeverInformational() {
        var validator = Self.makeValidator(isServer: false)
        expectMisuse("Only responses can be informational") { () throws(HTTP3Error) in
            try validator.willWriteHead(informational: true)
        }
        // The rejected head left the state as it was.
        #expect(validator.outboundAwaitsHead)
        #expect(validator.receivedFrame(.headers).kind == .streamError)
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func outboundResponseBeforeRequest(informational: Bool) {
        var validator = HTTP3RequestStreamValidator(isServer: true)
        // This is a misuse of our own API, so the error isn't sent to the peer.
        expectMisuse("A HTTP response was sent before a request", code: .malformedMessage) { () throws(HTTP3Error) in
            try validator.willWriteHead(informational: informational)
        }
        #expect(validator.outboundAwaitsHead)

        // Once the request head arrived, the response may be written.
        validator.expectReceivedHead()
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: informational)
        }
    }

    /// A client writes its request whether or not it has received anything.
    @available(anyAppleOS 27, *)
    @Test func clientWritesRequestBeforeReceiving() {
        var validator = HTTP3RequestStreamValidator(isServer: false)
        #expect(throws: Never.self) {
            try validator.willWriteHead(informational: false)
        }
    }
}

// MARK: Helpers

/// Expects `body` to throw the error for a misuse of the outbound API, which is never sent to the peer.
@available(anyAppleOS 27, *)
private func expectMisuse(
    _ message: String,
    code: HTTP3Error.Code = .unexpectedFrame,
    sourceLocation: SourceLocation = #_sourceLocation,
    _ body: () throws(HTTP3Error) -> Void
) {
    do {
        try body()
        Issue.record("Expected an error: \(message)", sourceLocation: sourceLocation)
    } catch {
        expectH3ErrorEqual(
            error: error,
            expectedCode: code,
            expectedH3ErrorCode: nil,
            expectedMessage: message,
            sourceLocation: sourceLocation
        )
    }
}

@available(anyAppleOS 27, *)
extension HTTP3RequestStreamValidator {
    /// Receives a final head.
    fileprivate mutating func expectReceivedHead(sourceLocation: SourceLocation = #_sourceLocation) {
        let action = self.receivedFrame(.headers)
        // Only a HEADERS frame announced by `.decodeHead` may be received as a head.
        guard case .decodeHead = action else {
            Issue.record("Unexpected action \(action)", sourceLocation: sourceLocation)
            return
        }
        self.receivedHead(informational: false)
    }
}

@available(anyAppleOS 27, *)
extension HTTP3RequestStreamValidator.InboundFrameAction {
    enum Kind: Hashable {
        case decodeHead
        case decodeTrailers
        case body
        case ignore
        case connectionError
        case streamError
    }

    var kind: Kind {
        switch self {
        case .decodeHead: return .decodeHead
        case .decodeTrailers: return .decodeTrailers
        case .body: return .body
        case .ignore: return .ignore
        case .connectionError: return .connectionError
        case .streamError: return .streamError
        }
    }

    var connectionError: HTTP3Error? {
        switch self {
        case .connectionError(let error): return error
        default: return nil
        }
    }

    var streamError: HTTP3Error? {
        switch self {
        case .streamError(let error): return error
        default: return nil
        }
    }
}

@available(anyAppleOS 27, *)
extension HTTP3RequestStreamValidator.InboundEndAction {
    var isComplete: Bool {
        switch self {
        case .complete: return true
        case .incompleteRequest, .incompleteResponse: return false
        }
    }
}
