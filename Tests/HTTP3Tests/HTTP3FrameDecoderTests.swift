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

import DequeModule
import NIOCore
@_spi(PackageInternal) import QPACK
import Testing

@testable @_spi(PackageInternal) import HTTP3

/// Drives ``HTTP3FrameDecoder`` through a ``NIOSingleStepByteToMessageProcessor``, the way
/// ``HTTP3StreamHandler`` does, while keeping the one-frame-at-a-time shape these tests are written
/// against.
///
/// The queue of decoded frames is a convenience for the tests only. The handler never holds more than a
/// single frame, because it stops the processor as soon as it can't take another.
private struct FrameDecoderDriver {
    enum DecodeAction {
        case returnFrame(HTTP3PartialFrame)
        case returnUnknownFrame
        /// The input ended part way through a frame.
        case truncated
        case needMoreBytes
        case emitConnectionError(HTTP3Error)
        case previousError
    }

    private let processor = NIOSingleStepByteToMessageProcessor(HTTP3FrameDecoder())
    private var decoded = Deque<HTTP3DecodedFrame>()
    /// An error to report on the next `decodeNext()`.
    private var errorToReport: HTTP3Error?
    /// Whether an error has already been reported. Nothing is decoded after that.
    private var seenError = false

    /// Feed bytes in, decoding as much as possible.
    mutating func buffer(_ buffer: ByteBuffer) {
        let processor = self.processor
        self.run { try processor.process(buffer: buffer, $0) }
    }

    /// Tell the decoder no more bytes are coming.
    mutating func inputClosed(seenEOF: Bool = true) {
        let processor = self.processor
        self.run { try processor.finishProcessing(seenEOF: seenEOF, $0) }
    }

    private mutating func run(_ body: ((HTTP3DecodedFrame) throws -> Void) throws -> Void) {
        // A real pipeline stops feeding the decoder once it has failed the connection.
        guard !self.seenError else { return }
        var decoded = self.decoded
        do {
            try body { decoded.append($0) }
        } catch let error as HTTP3Error {
            self.seenError = true
            self.errorToReport = error
        } catch {
            Issue.record("Unexpected error \(error)")
        }
        self.decoded = decoded
    }

    /// Take the next thing the decoder produced.
    mutating func decodeNext() -> DecodeAction {
        if let frame = self.decoded.popFirst() {
            switch frame {
            case .known(let frame): return .returnFrame(frame)
            case .unknown: return .returnUnknownFrame
            case .truncated: return .truncated
            }
        }
        if let error = self.errorToReport {
            self.errorToReport = nil
            return .emitConnectionError(error)
        }
        return self.seenError ? .previousError : .needMoreBytes
    }
}

struct HTTP3FrameDecoderTests {
    private let testDataFrameContent: [UInt8] = [1, 2, 3, 4]
    // Type is 0, length is 4, data is 1,2,3,4
    private let testDataFrameBytes: [UInt8] = [0, 4, 1, 2, 3, 4]

    private var testHeader: HTTP3PartialFrame.Headers {
        let fieldSectionPrefix = FieldSectionPrefix(requiredInsertCount: 0, base: 0).encode(maxCapacity: 0)
        let line = FieldLine.literal(requireLiteralRepresentation: false, name: "test", value: "hello")
        return .init(fieldSection: .init(prefix: fieldSectionPrefix, lines: [line]))
    }

    private var testHeaderFrameBytes: [UInt8] {
        let header = self.testHeader
        var buffer = ByteBuffer()
        buffer.writeFieldSectionPrefix(header.fieldSection.prefix)
        for line in header.fieldSection.lines {
            buffer.writeFieldLine(line, preferHuffmanEncoding: false)
        }
        let fieldSectionBytes = [UInt8](buffer: buffer)
        let prefix: [UInt8] = [1, UInt8(fieldSectionBytes.count)]  // prefix with frame type and length
        return prefix + fieldSectionBytes
    }

    @Test
    func testFullFrame() {
        var decoder = FrameDecoderDriver()
        // send in a full data frame
        decoder.buffer(.init(bytes: self.testDataFrameBytes))
        let action = decoder.decodeNext()
        #expect(action.returnFrame == .data(.init(bytes: self.testDataFrameContent)))
    }

    @Test
    func testPartialFrame() {
        let testFrame = HTTP3PartialFrame.settings(.init(qpackMaximumTableCapacity: 1024, h3Datagram: false))
        var encodedFrame = ByteBuffer()
        encodedFrame.writeHTTP3PartialFrame(testFrame, preferHuffmanEncoding: false)
        #expect(encodedFrame.readableBytes == 6)
        let bytes = [UInt8](buffer: encodedFrame)
        // drip in a frame, byte by byte, except for the last one
        var decoder = FrameDecoderDriver()
        for byte in bytes.dropLast() {
            decoder.buffer(.init(bytes: [byte]))
            #expect(decoder.decodeNext().needsMoreBytes)
        }
        // Put in the last byte
        decoder.buffer(.init(bytes: [bytes.last!]))
        let action = decoder.decodeNext()
        #expect(action.returnFrame == testFrame)
    }

    @Test
    func testPartialDataFrame() {
        var decoder = FrameDecoderDriver()

        decoder.buffer(.init(bytes: [0]))  // frame type data
        #expect(decoder.decodeNext().needsMoreBytes)  // Nothing useful can come yet

        decoder.buffer(.init(bytes: [4]))  // frame length 4
        #expect(decoder.decodeNext().needsMoreBytes)  // Nothing useful can come yet

        decoder.buffer(.init(bytes: [1, 2]))  // frame payload
        #expect(decoder.decodeNext().returnFrame == .data(.init(bytes: [1, 2])))  // Bytes come out as a frame

        decoder.buffer(.init(bytes: [3]))  // frame payload continued
        #expect(decoder.decodeNext().returnFrame == .data(.init(bytes: [3])))  // Bytes come out as a frame

        decoder.buffer(.init(bytes: [4]))  // frame payload continued
        #expect(decoder.decodeNext().returnFrame == .data(.init(bytes: [4])))  // Bytes come out as a frame

        decoder.buffer(.init(bytes: [0]))  // next frame begins
        #expect(decoder.decodeNext().needsMoreBytes)  // Again nothing is ready yet
    }

    @Test
    func testUnknownFrameType() {
        let bytes: [UInt8] = [12, 0]  // 12 is not a known type
        var decoder = FrameDecoderDriver()
        decoder.buffer(.init(bytes: bytes))

        // There is no action, unknown frames are dropped
        #expect(decoder.decodeNext().isReturnUnknownFrame)

        // Further bytes are processed as usual
        decoder.buffer(.init(bytes: self.testDataFrameBytes))
        let action3 = decoder.decodeNext()
        #expect(action3.returnFrame == .data(.init(bytes: self.testDataFrameContent)))
    }

    /// An unknown frame whose payload doesn't all arrive at once must not spin: the decoder skips what it
    /// has and asks for more, rather than looping on an empty buffer.
    @Test
    func testUnknownFrameTypeWithSplitPayload() {
        // Type 12 is not a known type, and it declares a four byte payload.
        var decoder = FrameDecoderDriver()
        decoder.buffer(.init(bytes: [12, 4] as [UInt8]))

        #expect(decoder.decodeNext().isReturnUnknownFrame)
        // The payload hasn't arrived, so there is nothing more to do yet.
        #expect(decoder.decodeNext().needsMoreBytes)

        // The payload arrives split in two, and neither half is enough to finish the skip.
        decoder.buffer(.init(bytes: [0xde, 0xad] as [UInt8]))
        #expect(decoder.decodeNext().needsMoreBytes)

        decoder.buffer(.init(bytes: [0xbe, 0xef] as [UInt8]))
        #expect(decoder.decodeNext().needsMoreBytes)

        // Once the unknown frame has been skipped over, the frame behind it decodes as usual.
        decoder.buffer(.init(bytes: self.testDataFrameBytes))
        #expect(decoder.decodeNext().returnFrame == .data(.init(bytes: self.testDataFrameContent)))
    }

    @Test
    func testForbiddenFrameType() {
        let forbiddenTypes: [UInt8] = [2, 6, 8, 9]
        for type in forbiddenTypes {
            let bytes: [UInt8] = [type]
            var decoder = FrameDecoderDriver()
            decoder.buffer(.init(bytes: bytes))

            let action1 = decoder.decodeNext()
            switch action1 {
            case .emitConnectionError(let error):
                expectH3ErrorEqual(
                    error: error,
                    expectedCode: .forbiddenFrameType,
                    expectedH3ErrorCode: .frameUnexpected
                )
            case .returnFrame, .needMoreBytes, .previousError, .returnUnknownFrame, .truncated:
                Issue.record("Unexpected action")
            }

            // Every time we call decodeNext, we should get the `previousError` action
            let action2 = decoder.decodeNext()
            #expect(action2.previousError)

            // Further bytes are ignored, even if valid
            decoder.buffer(.init(bytes: self.testDataFrameBytes))
            let action3 = decoder.decodeNext()
            #expect(action3.previousError)
        }
    }

    @Test
    func testPartialHeader() {
        var decoder = FrameDecoderDriver()
        decoder.buffer(.init(bytes: self.testHeaderFrameBytes))
        let action1 = decoder.decodeNext()
        #expect(action1.returnFrame == .headers(self.testHeader))
    }

    @Test
    func testDecodeNoBytes() {
        var decoder = FrameDecoderDriver()
        let action1 = decoder.decodeNext()
        #expect(action1.needsMoreBytes)
    }

    @Test
    func testInputCloseImmediately() {
        var decoder = FrameDecoderDriver()
        decoder.inputClosed()
        // Nothing was truncated, because the decoder has seen no incoming bytes at all
        #expect(decoder.decodeNext().needsMoreBytes)
    }

    @Test
    func testInputCloseCleanly() {
        var decoder = FrameDecoderDriver()

        decoder.buffer(.init(bytes: self.testDataFrameBytes))
        let action = decoder.decodeNext()
        #expect(action.returnFrame == .data(.init(bytes: self.testDataFrameContent)))

        // Nothing was truncated, because the decoder has only seen a full frame
        decoder.inputClosed()
        #expect(decoder.decodeNext().needsMoreBytes)
    }

    @Test(arguments: [
        [0],  // Frame type 0, no length
        [64],  // Partial frame type (64 implies a multi-byte integer)
        [0, 5, 1],  // Frame type + length but incomplete data (only 1 byte of data, expecting 5)
        [0, 1, 1, 0, 1],  // A full frame, followed by a frame with missing payload
    ])
    func testInputCloseUnclean(testData: [UInt8]) {
        var decoder = FrameDecoderDriver()

        decoder.buffer(.init(bytes: testData))
        while case .returnFrame = decoder.decodeNext() {
            // Not interested in what comes out. We just want to consume all the full frames
        }

        // The stream stopped part way through a frame
        decoder.inputClosed()
        #expect(decoder.decodeNext().isTruncated)
    }
}

extension FrameDecoderDriver.DecodeAction {
    fileprivate var returnFrame: HTTP3PartialFrame? {
        switch self {
        case .returnFrame(let f): return f
        case .emitConnectionError, .previousError, .needMoreBytes, .returnUnknownFrame, .truncated: return nil
        }
    }

    fileprivate var isReturnUnknownFrame: Bool {
        switch self {
        case .returnUnknownFrame: return true
        case .returnFrame, .emitConnectionError, .previousError, .needMoreBytes, .truncated: return false
        }
    }

    fileprivate var isTruncated: Bool {
        switch self {
        case .truncated: return true
        case .returnFrame, .emitConnectionError, .previousError, .needMoreBytes, .returnUnknownFrame: return false
        }
    }

    fileprivate var needsMoreBytes: Bool {
        switch self {
        case .needMoreBytes: return true
        case .emitConnectionError, .previousError, .returnFrame, .returnUnknownFrame, .truncated: return false
        }
    }

    fileprivate var previousError: Bool {
        switch self {
        case .previousError: return true
        case .emitConnectionError, .needMoreBytes, .returnFrame, .returnUnknownFrame, .truncated: return false
        }
    }
}
