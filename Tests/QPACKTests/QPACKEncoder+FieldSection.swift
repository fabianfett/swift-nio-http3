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

import HTTPTypes
import NIOCore
import NIOQUICHelpers
import Testing

@_spi(PackageInternal) @testable import QPACK

// The encoders write their field sections straight into an `OutputRawSpan`. These read them back as a `FieldSection`,
// which the tests can compare line by line. They check that the encoders write no more than the room they ask for.

/// The result of encoding a field section.
@available(anyAppleOS 27, *)
struct QPACKEncodeResult: Hashable {
    var fieldSection: FieldSection
    var instructions: [QPACKEncoderInstruction]
}

@available(anyAppleOS 27, *)
extension FieldSection {
    /// Reads the field section which takes up the whole of `buffer`.
    init(readingAll buffer: ByteBuffer) {
        var buffer = buffer
        self = try! buffer.readFieldSection()!
        #expect(buffer.readableBytes == 0)
    }
}

@available(anyAppleOS 27, *)
extension StaticQPACKEncoder {
    func encode(headers: [HTTPField]) -> FieldSection {
        let bound = self.maxEncodedLength(of: headers)
        let buffer = ByteBuffer(initialCapacity: bound) { output in
            self.encode(headers: headers, into: &output, preferHuffmanEncoding: false)
        }
        #expect(buffer.readableBytes <= bound)
        return FieldSection(readingAll: buffer)
    }
}

@available(anyAppleOS 27, *)
extension DynamicQPACKEncoder {
    mutating func encode(headers: [HTTPField], forStream streamID: QUICStreamID) -> QPACKEncodeResult {
        var instructions = [QPACKEncoderInstruction]()
        let bound = self.maxEncodedLength(of: headers)
        let buffer = ByteBuffer(initialCapacity: bound) { output in
            instructions = self.encode(
                headers: headers,
                forStream: streamID,
                into: &output,
                preferHuffmanEncoding: false
            )
        }
        #expect(buffer.readableBytes <= bound)
        return QPACKEncodeResult(fieldSection: FieldSection(readingAll: buffer), instructions: instructions)
    }
}

@available(anyAppleOS 27, *)
extension QPACKStateMachine {
    mutating func encodeHeaders(_ headers: [HTTPField], forStream streamID: QUICStreamID) -> QPACKEncodeResult {
        var instructions = [QPACKEncoderInstruction]()
        let bound = self.maxEncodedLength(of: headers)
        let buffer = ByteBuffer(initialCapacity: bound) { output in
            instructions = self.encodeHeaders(
                headers,
                forStream: streamID,
                into: &output,
                preferHuffmanEncoding: false
            )
        }
        #expect(buffer.readableBytes <= bound)
        return QPACKEncodeResult(fieldSection: FieldSection(readingAll: buffer), instructions: instructions)
    }
}
