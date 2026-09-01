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

import NIOQUICHelpers
@_spi(PackageInternal) import QPACK
import Testing

@_spi(PackageInternal) @testable import HTTP3

/// The state machine hands the decode context back to us with every decode action. These tests use the
/// ID of the stream the decode was started for, which is what makes the actions easy to assert on.
private typealias TestQPACKStateMachine = QPACKStateMachine<QUICStreamID>

struct QPACKStateMachineTests {
    @Test
    func testBeginUsingDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
    }

    @Test
    func testSettingsWithoutDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let action = stateMachine.receivedRemoteSettings(maxQueueSize: 0, effectiveDynamicTableSize: 0)
        switch action {
        case .makeEncoderInstructionStream:
            Issue.record("Expected no outbound encoder stream")
            return
        case .none:
            break  // Good
        }
    }

    /// The remote's dynamic table capacity is what the encoder must adopt, even when we would allow a larger one.
    @Test
    func testSettingsWithSmallerRemoteDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 200, decoderMaxBlockedStreams: 100)
        let action1 = stateMachine.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: 100)
        #expect(action1 == .makeEncoderInstructionStream)

        let action2 = stateMachine.outboundEncoderStreamReady()
        #expect(action2 == .sendEncoderInstruction(.setDynamicTableCapacity(100)))
    }

    // MARK: Encoding headers

    @Test
    func testEncodeHeadersInInitialState() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let result = stateMachine.encodeHeaders([.init(name: .cookie, value: "test")], forStream: 1)
        #expect(
            result.fieldSection.lines
                == [
                    .literalWithNameReference(
                        requireLiteralRepresentation: false,
                        table: .staticTable,
                        index: 5,
                        value: "test"
                    )
                ]
        )
    }

    @Test
    func testEncodeHeadersInWaitingForStreamState() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let action = stateMachine.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: 100)
        #expect(action == .makeEncoderInstructionStream)
        // We have received remote settings, and been asked to create outbound encoder stream
        // However, the outbound stream isn't ready yet, so the dynamic table should not be used
        stateMachine.assertEncodesWithoutUsingDynamicTable()
    }

    @Test
    func testEncodeHeadersInWithoutDynamicState() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let action = stateMachine.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: 0)
        #expect(action == nil)  // No outbound stream because 0 size
        // We have received remote settings, but they specify 0 table size. Therefore we should not use dynamic table
        stateMachine.assertEncodesWithoutUsingDynamicTable()
    }

    @Test
    func testEncodeHeadersInWithDynamicState() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let action1 = stateMachine.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: 300)
        #expect(action1 == .makeEncoderInstructionStream)
        let action2 = stateMachine.outboundEncoderStreamReady()
        // The stream is ready so we should immediately start using the table at max capacity
        #expect(action2 == .sendEncoderInstruction(.setDynamicTableCapacity(300)))

        // Doing an encode should now use the table
        let encodeResult = stateMachine.encodeHeaders([.init(name: .cookie, value: "test")], forStream: 1)
        let expectedPrefix = FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 300)
        let expectedFieldSection = FieldSection(
            prefix: expectedPrefix,
            lines: [.indexedWithPostBase(index: 0)]
        )
        #expect(encodeResult.fieldSection == expectedFieldSection)
        #expect(
            encodeResult.instructions
                == [.insertWithNameReference(.staticTable, relativeIndex: 5, value: "test")]
        )
    }

    // MARK: Decoding headers

    @Test
    func testDecodeHeadersWithoutDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: .init(encodedRequiredInsertCount: 0, deltaBase: 0, signBit: false),
                lines: [.literal(requireLiteralRepresentation: false, name: "cookie", value: "test")]
            )
        )
        let action = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        guard case .informDecodeResult(let result, _) = action else {
            Issue.record("Unexpected action \(String(describing: action))")
            return
        }
        #expect(
            result
                == TestQPACKStateMachine.DecodeHeaderAction.InformDecodeResult(
                    fields: [.init(name: .cookie, value: "test")],
                    headers: testHeader,
                    streamID: streamID,
                    instructionToWrite: nil
                )
        )
    }

    @Test
    func testDecodeHeadersWithDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // Give the machine a table entry
        let actions1 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test")
        )
        #expect(actions1?.decoderInstructions == .insertCountIncrement(increment: 1))

        // Ask the machine to decode a header section containing reference to the new entry
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 100),
                lines: [.indexedWithPostBase(index: 0)]
            )
        )
        let actions2 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        guard case .informDecodeResult(let decodeResult, _) = actions2 else {
            Issue.record("Unexpected action \(String(describing: actions2))")
            return
        }
        #expect(
            decodeResult
                == TestQPACKStateMachine.DecodeHeaderAction.InformDecodeResult(
                    fields: [.init(name: .cookie, value: "test")],
                    headers: testHeader,
                    streamID: streamID,
                    instructionToWrite: .sectionAcknowledgement(streamID: streamID)
                )
        )
    }

    @Test
    func testDecodeHeadersConnectionError() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // Ask the machine to decode some nonsense header. This is a connection level failure because the references don't exist
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 0, base: 0).encode(maxCapacity: 100),
                lines: [.indexedWithPostBase(index: 0)]
            )
        )
        let action2 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)

        guard case .emitConnectionError(let error, _) = action2 else {
            Issue.record("Unexpected actions \(String(describing: action2))")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackDecoderError,
            expectedH3ErrorCode: .qpackDecompressionFailed
        )
    }

    @Test
    func testDecodeHeadersStreamError() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // This header will decode fine, but is a malformed message (upper case field names). This is a stream error
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 0, base: 0).encode(maxCapacity: 0),
                lines: [.literal(requireLiteralRepresentation: false, name: "ILLEGAL", value: "value")]
            )
        )
        let action2 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        guard case .informDecodeError(let error, _) = action2 else {
            Issue.record("Unexpected actions \(String(describing: action2))")
            return
        }
        #expect(error.headers == testHeader)
        #expect(error.streamID == streamID)
        expectH3ErrorEqual(error: error.error, expectedCode: .qpackDecoderError, expectedH3ErrorCode: .messageError)
    }

    @Test
    func testDecodeHeadersWithDynamicTableDelayed() throws {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // Ask the machine to decode a header section containing dynamic table references
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 100),
                lines: [.indexedWithPostBase(index: 0)]
            )
        )
        let actions3 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        // The machine can't decode it, because it hasn't received that entry yet
        #expect(actions3 == nil)

        // Give it the entry
        let action4 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test")
        )
        #expect(action4?.decoderInstructions == .insertCountIncrement(increment: 1))

        let action5 = stateMachine.checkPendingDecodes()
        #expect(
            action5
                == .informDecodeResult(
                    .init(
                        fields: [.init(name: .cookie, value: "test")],
                        headers: testHeader,
                        streamID: streamID,
                        instructionToWrite: .sectionAcknowledgement(streamID: streamID)
                    ),
                    streamID
                )
        )
        #expect(stateMachine.checkPendingDecodes() == nil)
    }

    @Test
    func testDecodeHeadersStreamErrorDelayed() throws {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // Ask the machine to decode a header section containing dynamic table references, plus an invalid literal
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 100),
                lines: [
                    .indexedWithPostBase(index: 0),
                    // Invalid due to uppercase
                    .literal(requireLiteralRepresentation: false, name: "Test", value: "test"),
                ]
            )
        )
        let actions3 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        // The machine can't decode it, because it hasn't received that entry yet
        #expect(actions3 == nil)

        // Give it the entry
        let actions4 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test")
        )
        #expect(actions4?.decoderInstructions == .insertCountIncrement(increment: 1))

        // Decoding now becomes possible and gives us the error
        let action5 = stateMachine.checkPendingDecodes()
        guard case .informDecodeError(let decodeError, _) = action5 else {
            Issue.record("Unexpected action \(String(describing: action5))")
            return
        }
        #expect(decodeError.headers == testHeader)
        #expect(decodeError.streamID == streamID)
        expectH3ErrorEqual(
            error: decodeError.error,
            expectedCode: .qpackDecoderError,
            expectedH3ErrorCode: .messageError
        )
    }

    @Test
    func testInvalidFieldPrefix() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)
        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: .init(encodedRequiredInsertCount: 200, deltaBase: 100, signBit: true),
                lines: [
                    .literal(requireLiteralRepresentation: false, name: "test", value: "test")
                ]
            )
        )
        let actions3 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        // The machine can't decode it, because the prefix is nonsense
        guard case .emitConnectionError(let error, _) = actions3 else {
            Issue.record("Unexpected action \(String(describing: actions3))")
            return
        }
        expectH3ErrorEqual(
            error:
                error,
            expectedCode: .qpackDecoderError,
            expectedH3ErrorCode: .qpackDecompressionFailed,
            expectedMessage: "Invalid field section prefix"
        )
    }

    @Test
    func testMaxBlockedStreams() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 3)

        let streamID1 = QUICStreamID(1)
        let streamID2 = QUICStreamID(2)
        let streamID3 = QUICStreamID(3)
        let streamID4 = QUICStreamID(4)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // a header which requires an insert count of 1
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 1024),
                lines: [.literal(requireLiteralRepresentation: false, name: "test", value: "test")]
            )
        )

        // Max blocked streams is 3, so the first 3 are fine, they just get queued
        let action1 = stateMachine.decodeHeaders(testHeader, forStream: streamID1, receiver: streamID1)
        #expect(action1 == nil)
        let action2 = stateMachine.decodeHeaders(testHeader, forStream: streamID2, receiver: streamID2)
        #expect(action2 == nil)
        let action3 = stateMachine.decodeHeaders(testHeader, forStream: streamID3, receiver: streamID3)
        #expect(action3 == nil)

        // Trying to queue on a 4th stream is a connection error
        // RFC 9204 2.1.2: If a decoder encounters more blocked streams than it promised to support, it MUST treat this as a connection error of type QPACK_DECOMPRESSION_FAILED.
        let action4 = stateMachine.decodeHeaders(testHeader, forStream: streamID4, receiver: streamID4)
        guard case .emitConnectionError(let error, _) = action4 else {
            Issue.record("Unexpected action \(String(describing: action4))")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackDecoderError,
            expectedH3ErrorCode: .qpackDecompressionFailed,
            expectedMessage: "Too many streams blocked on QPACK"
        )
    }

    @Test
    func testDecodeInstructionsBufferedWhenStreamNotReady() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)

        // Give the machine a table entry
        let action1 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test")
        )
        // Would normally expect the action to be to send an insert count increment, but we don't because the outbound stream isn't ready
        #expect(action1?.decoderInstructions == nil)

        // Ask the machine to decode a header section containing reference to the new entry
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 100),
                lines: [.indexedWithPostBase(index: 0)]
            )
        )
        let streamID = QUICStreamID(0)
        let actions2 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        // The action is only to inform the decode result, there is no section acknowledgment because the outbound stream isn't ready
        #expect(
            actions2
                == .informDecodeResult(
                    .init(
                        fields: [.init(name: .cookie, value: "test")],
                        headers: testHeader,
                        streamID: streamID,
                        instructionToWrite: nil
                    ),
                    streamID
                )
        )

        // Make the outbound stream be ready. This should tell us to send the 2 instructions buffered from before
        let actions3 = stateMachine.outboundDecoderStreamReady()
        #expect(
            actions3
                == .sendDecoderInstructions([
                    .insertCountIncrement(increment: 1), .sectionAcknowledgement(streamID: streamID),
                ])
        )

        // Further instructions should not be buffered
        let actions4 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test2")
        )
        #expect(actions4?.decoderInstructions == .insertCountIncrement(increment: 1))
    }

    // MARK: Decoder instructions

    @Test
    func testGotIncomingDecoderInstruction() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        let streamID = QUICStreamID(4)
        _ = stateMachine.encodeHeaders([.init(name: .cookie, value: "test")], forStream: streamID)
        let actions = stateMachine.receivedIncomingDecoderInstruction(
            .sectionAcknowledgement(streamID: streamID)
        )
        #expect(actions == nil)
    }

    /// The remote decoder acknowledging the entry our encoder inserted is valid, and needs no action.
    @Test
    func testGotIncomingInsertCountIncrement() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        // Encoding inserts an entry into our encoder's dynamic table, which the remote decoder can then ack.
        _ = stateMachine.encodeHeaders([.init(name: .cookie, value: "test")], forStream: 0)
        let action = stateMachine.receivedIncomingDecoderInstruction(.insertCountIncrement(increment: 1))
        #expect(action == nil)
    }

    @Test
    func testGotDecoderInstructionWhenImplicitlyNoDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        // This instruction is invalid because there is no dynamic table initially and no stream with id 1
        let action = stateMachine.receivedIncomingDecoderInstruction(.sectionAcknowledgement(streamID: 1))
        guard case .emitConnectionError(let error) = action else {
            Issue.record("Unexpected action")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackDecoderStreamError,
            expectedH3ErrorCode: .qpackDecoderStreamError
        )
    }

    @Test
    func testGotDecoderInstructionWhenExplicitlyNoDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        _ = stateMachine.receivedRemoteSettings(maxQueueSize: 0, effectiveDynamicTableSize: 0)
        // This instruction is invalid because remote explicitly told us no dynamic table capacity
        let action = stateMachine.receivedIncomingDecoderInstruction(.sectionAcknowledgement(streamID: 1))
        guard case .emitConnectionError(let error) = action else {
            Issue.record("Unexpected action")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackDecoderStreamError,
            expectedH3ErrorCode: .qpackDecoderStreamError
        )
    }

    @Test
    func testGotDecoderInstructionWhenAwaitingStream() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        _ = stateMachine.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: 100)
        // We can't receive instructions from the remote decoder until we ourselves have sent an instruction to indicate support of the dynamic table
        let action = stateMachine.receivedIncomingDecoderInstruction(.sectionAcknowledgement(streamID: 1))
        guard case .emitConnectionError(let error) = action else {
            Issue.record("Unexpected action")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackDecoderStreamError,
            expectedH3ErrorCode: .qpackDecoderStreamError
        )
    }

    @Test
    func testGotInvalidIncomingDecoderInstruction() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        stateMachine.setupRemoteDynamicTable(maxSize: 1)
        // This instruction is invalid because we can't ack an insert which hasn't happened
        let action = stateMachine.receivedIncomingDecoderInstruction(.insertCountIncrement(increment: 1))
        guard case .emitConnectionError(let error) = action else {
            Issue.record("Unexpected action")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackDecoderStreamError,
            expectedH3ErrorCode: .qpackDecoderStreamError
        )
    }

    // MARK: Encoder instructions

    /// Decoder instructions produced before the outbound decoder stream exists must be buffered until it does.
    @Test
    func testIncomingEncoderInstructionWithQueue() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 100, decoderMaxBlockedStreams: 100)

        let action1 = stateMachine.receivedIncomingEncoderInstruction(.setDynamicTableCapacity(100))
        #expect(action1?.decoderInstructions == nil)

        let action2 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "hello", value: "world")
        )
        // No action yet because the stream isn't ready
        #expect(action2?.decoderInstructions == nil)

        let action3 = stateMachine.outboundDecoderStreamReady()
        // Now the instructions come out
        #expect(action3 == .sendDecoderInstructions([.insertCountIncrement(increment: 1)]))
    }

    @Test
    func testIncomingEncoderInstructionNoQueue() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 100, decoderMaxBlockedStreams: 100)

        let action1 = stateMachine.outboundDecoderStreamReady()
        #expect(action1 == nil)

        let action2 = stateMachine.receivedIncomingEncoderInstruction(.setDynamicTableCapacity(100))
        #expect(action2?.decoderInstructions == nil)

        let action3 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "hello", value: "world")
        )
        // The instructions come out immediately because the stream is already ready
        #expect(action3?.decoderInstructions == .insertCountIncrement(increment: 1))
    }

    @Test
    func testGotInvalidIncomingEncoderInstruction() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)

        // Invalid because 1025 is higher than allowed max capacity
        let action = stateMachine.receivedIncomingEncoderInstruction(.setDynamicTableCapacity(1025))
        guard case .emitConnectionError(let error) = action else {
            Issue.record("Unexpected action")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackEncoderStreamError,
            expectedH3ErrorCode: .qpackEncoderStreamError
        )
    }

    @Test
    func testInsertTooLargeEntry() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1, decoderMaxBlockedStreams: 100)

        let action1 = stateMachine.receivedIncomingEncoderInstruction(.setDynamicTableCapacity(1))
        #expect(action1?.decoderInstructions == nil)

        // Capacity is only 1, so inserting this is a connection level error
        let action2 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "toooo", value: "long")
        )
        /// It is an error if the encoder attempts to add an entry that is larger than the dynamic table capacity; the decoder MUST treat this as a connection error of type QPACK_ENCODER_STREAM_ERROR.
        guard case .emitConnectionError(let error) = action2 else {
            Issue.record("Unexpected action")
            return
        }
        expectH3ErrorEqual(
            error: error,
            expectedCode: .qpackEncoderStreamError,
            expectedH3ErrorCode: .qpackEncoderStreamError
        )
    }

    // MARK: Shutdown

    @Test
    func testIncomingEncoderInstructionAfterShutdown() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 100, decoderMaxBlockedStreams: 100)

        let action1 = stateMachine.outboundDecoderStreamReady()
        #expect(action1 == nil)

        let action2 = stateMachine.receivedIncomingEncoderInstruction(.setDynamicTableCapacity(100))
        #expect(action2?.decoderInstructions == nil)

        stateMachine.shutdown()

        // The instruction is dropped: there is nobody left to send the insert count increment to.
        let action3 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "hello", value: "world")
        )
        #expect(action3 == nil)
    }

    @Test
    func testIncomingDecoderInstructionAfterShutdown() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        stateMachine.setupRemoteDynamicTable(maxSize: 1024)

        stateMachine.shutdown()

        // This instruction would be a connection error on a live connection, because nothing has been
        // inserted. After shutdown it is dropped instead.
        let action = stateMachine.receivedIncomingDecoderInstruction(.insertCountIncrement(increment: 1))
        #expect(action == nil)
    }

    @Test
    func testEncoderStreamReadyAfterShutdown() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 200, decoderMaxBlockedStreams: 100)
        let action1 = stateMachine.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: 100)
        #expect(action1 == .makeEncoderInstructionStream)

        stateMachine.shutdown()

        // The stream finished being created after we shut down. We don't send our table capacity on it.
        let action2 = stateMachine.outboundEncoderStreamReady()
        #expect(action2 == nil)
    }

    @Test
    func testDecodeHeadersAfterShutdown() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        stateMachine.shutdown()

        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: .init(encodedRequiredInsertCount: 0, deltaBase: 0, signBit: false),
                lines: [.literal(requireLiteralRepresentation: false, name: "cookie", value: "test")]
            )
        )
        // The stream that asked for this is gone along with the connection, so there is nobody to tell.
        let action = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        #expect(action == nil)
    }

    /// Shutting down must drop blocked decodes: their streams are gone, and the queue holds the decode
    /// contexts (the stream handlers) alive.
    @Test
    func testBlockedDecodesAreDroppedOnShutdown() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(0)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // This can't be decoded yet: it references an entry we haven't been given.
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 100),
                lines: [.indexedWithPostBase(index: 0)]
            )
        )
        #expect(stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID) == nil)

        stateMachine.shutdown()

        // Giving the state machine the entry would have completed the decode, had it not been dropped.
        let action = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test")
        )
        #expect(action == nil)
        #expect(stateMachine.checkPendingDecodes() == nil)
    }

    // MARK: Request stream closing

    @Test
    func testClosedRequestStreamAfterEOF() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(1)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // Cancel the stream
        let actions = stateMachine.requestStreamClosed(streamID: streamID, seenEOF: true)
        #expect(actions == nil)
    }

    @Test
    func testClosedRequestStreamBeforeEOFWithoutDynamicTable() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(1)

        // Cancel the stream
        let actions = stateMachine.requestStreamClosed(streamID: streamID, seenEOF: false)
        #expect(actions == nil)  // No instruction to send, because no dynamic table
    }

    @Test
    func testClosedRequestStreamWhilstDecodingQPACK() {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)
        let streamID = QUICStreamID(1)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // Ask the machine to decode a header section containing reference which doesn't exist.
        let testHeader = HTTP3PartialFrame.Headers(
            fieldSection: FieldSection(
                prefix: FieldSectionPrefix(requiredInsertCount: 1, base: 0).encode(maxCapacity: 100),
                lines: [.indexedWithPostBase(index: 0)]
            )
        )
        let actions1 = stateMachine.decodeHeaders(testHeader, forStream: streamID, receiver: streamID)
        // The state machine will have queued the decoding, so we don't have an action yet.
        #expect(actions1 == nil)

        // We tell the state machine that the stream has gone away, so it'll remove the pending decode from the queue.
        // Also, it will tell us to inform the remote that we've cancelled the stream, so the remote will know that we won't be acking the relevant field sections.
        let actions2 = stateMachine.requestStreamClosed(streamID: streamID, seenEOF: false)
        #expect(actions2 == .sendDecoderInstruction(.streamCancellation(streamID: streamID)))

        // Give the machine the table entry. Normally, this would allow the machine to complete the queued decode. But the decode is no longer in the queue.
        // So we have no action (only the insert count increment, which we always have on every insert)
        let actions3 = stateMachine.receivedIncomingEncoderInstruction(
            .insertWithLiteralName(name: "cookie", value: "test")
        )
        #expect(actions3?.decoderInstructions == .insertCountIncrement(increment: 1))
    }

    /// This tests the scenario where a request is fully completed, the stream is closed, and then an ack comes in on the QPACK decoder stream for that request.
    /// This test is for a potential bug where we accidentally treat it as an error to receive an ack for a 'nonexistent' stream.
    @Test
    func testClosedRequestStreamThenReceiveSectionAck() throws {
        var stateMachine = TestQPACKStateMachine(decoderMaxTableSize: 1024, decoderMaxBlockedStreams: 100)

        stateMachine.setupRemoteDynamicTable(maxSize: 1024)
        stateMachine.setupLocalDynamicTable(maxSize: 1024)
        stateMachine.setupOutboundDecoderStream()

        // An outbound request stream is made
        let streamID = QUICStreamID(0)

        // Some fields are encoded to be sent on that stream
        _ = stateMachine.encodeHeaders([.init(name: .cookie, value: "test")], forStream: streamID)

        // The stream is closed cleanly
        _ = stateMachine.requestStreamClosed(streamID: streamID, seenEOF: true)

        // An ack comes in for the field section
        let action = stateMachine.receivedIncomingDecoderInstruction(.sectionAcknowledgement(streamID: streamID))
        switch action {
        case .emitConnectionError(let error):
            throw error  // unexpected
        case .none:
            break  // expected
        }
    }
}

extension QPACKStateMachine.DecodeHeaderAction: Equatable where DecodeContext: Equatable {
    static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.informDecodeResult(let l, let lContext), .informDecodeResult(let r, let rContext)):
            return l == r && lContext == rContext
        case (.informDecodeError, .informDecodeError):
            return false  // no good way to equate these
        default:
            return false
        }
    }
}

extension QPACKStateMachine {
    /// Simulate receiving settings from the remote which allows the local encoder to use the dynamic table.
    fileprivate mutating func setupRemoteDynamicTable(maxSize: Int) {
        // remote sends us settings saying we may use the dynamic table
        let actions1 = self.receivedRemoteSettings(maxQueueSize: 100, effectiveDynamicTableSize: maxSize)
        switch actions1 {
        // We open an encoder stream
        case .makeEncoderInstructionStream:
            let actions2 = self.outboundEncoderStreamReady()
            // we send an instruction on the stream telling the remote that we want to use the table
            let expectedInstruction = QPACKEncoderInstruction.setDynamicTableCapacity(maxSize)
            #expect(actions2 == .sendEncoderInstruction(expectedInstruction))
        case .none:
            Issue.record("Unexpected action")
        }
    }

    /// Simulate the remote sending the local an instruction telling the local that it wants to use the dynamic table.
    fileprivate mutating func setupLocalDynamicTable(maxSize: Int) {
        let action = self.receivedIncomingEncoderInstruction(.setDynamicTableCapacity(maxSize))
        switch action {
        case .emitConnectionError:
            Issue.record("Unexpected error")
        case .sendDecoderInstruction(let instruction):
            Issue.record("Unexpected instruction \(instruction)")
        case .none:
            break
        }
    }

    fileprivate mutating func setupOutboundDecoderStream() {
        let actions = self.outboundDecoderStreamReady()
        #expect(actions == nil)
    }

    fileprivate mutating func assertEncodesWithoutUsingDynamicTable() {
        let result = self.encodeHeaders([.init(name: .cookie, value: "test")], forStream: 1)
        #expect(
            result.fieldSection.lines
                == [
                    .literalWithNameReference(
                        requireLiteralRepresentation: false,
                        table: .staticTable,
                        index: 5,
                        value: "test"
                    )
                ]
        )
    }
}

extension QPACKStateMachine.IncomingEncoderInstructionAction {
    fileprivate var decoderInstructions: QPACKDecoderInstruction? {
        switch self {
        case .sendDecoderInstruction(let decoderInstruction): return decoderInstruction
        case .emitConnectionError: return nil
        }
    }
}
