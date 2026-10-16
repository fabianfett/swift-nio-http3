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

/// Tests that the encoders write the field sections they describe into an `OutputRawSpan`, within the room they ask for.
struct FieldSectionEncodingTests {
    /// Exact static matches, name only matches, fields absent from the static table, a never indexed field, values
    /// whose length takes more than one byte, and values which are shorter without Huffman coding.
    static let headers: [HTTPField] = {
        var neverIndexed = HTTPField(name: .authorization, value: "Bearer secret")
        neverIndexed.indexingStrategy = .disallow
        return [
            HTTPField(name: .init(parsed: ":method")!, value: "GET"),
            HTTPField(name: .init(parsed: ":scheme")!, value: "https"),
            HTTPField(name: .init(parsed: ":authority")!, value: "www.example.com"),
            HTTPField(name: .init(parsed: ":path")!, value: "/" + String(repeating: "a", count: 300)),
            HTTPField(name: .accept, value: "*/*"),
            HTTPField(name: .init("x-custom-header")!, value: "some-custom-value"),
            HTTPField(name: .init("x-unicode")!, value: "héllo wörld ✓ 🎉"),
            HTTPField(name: .init("x-empty")!, value: ""),
            neverIndexed,
        ]
    }()

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func staticEncoderWritesItsFieldSection(preferHuffmanEncoding: Bool) throws {
        let encoder = StaticQPACKEncoder()
        let bound = encoder.maxEncodedLength(of: Self.headers)
        var buffer = ByteBuffer(initialCapacity: bound) { output in
            encoder.encode(headers: Self.headers, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
        }
        #expect(buffer.readableBytes <= bound)

        let decoded = try buffer.readFieldSection()
        #expect(buffer.readableBytes == 0)
        #expect(decoded == encoder.encode(headers: Self.headers))
    }

    @available(anyAppleOS 27, *)
    @Test func staticEncoderWritesNothingButThePrefixForNoFields() {
        let encoder = StaticQPACKEncoder()
        let buffer = ByteBuffer(initialCapacity: encoder.maxEncodedLength(of: [])) { output in
            encoder.encode(headers: [], into: &output, preferHuffmanEncoding: true)
        }
        // Required Insert Count 0, then Base 0
        #expect(Array(buffer.readableBytesView) == [0, 0])
    }

    /// Two encoders in the same state encode the same fields, one into a ``FieldSection`` and one into an
    /// `OutputRawSpan`. Over enough sections the Base and the Required Insert Count outgrow a byte, and the prefix
    /// usually takes less room than was reserved for it.
    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func dynamicEncoderWritesItsFieldSection(preferHuffmanEncoding: Bool) throws {
        func makeEncoder() -> DynamicQPACKEncoder {
            DynamicQPACKEncoder.create(
                dynamicTableMaxCapacity: 4096,
                dynamicTableInitialCapacity: 4096,
                maxBlockedStreams: 100,
                targetEvictableFraction: 0.5
            ).0
        }
        var reference = makeEncoder()
        var encoder = makeEncoder()

        for round in 0..<200 {
            let streamID = QUICStreamID(rawValue: UInt64(round) * 4)
            // A new field every round, so that every section inserts into the table.
            let headers = Self.headers + [HTTPField(name: .init("x-round")!, value: "\(round)")]
            let expected = reference.encode(headers: headers, forStream: streamID)

            var instructions = [QPACKEncoderInstruction]()
            let bound = encoder.maxEncodedLength(of: headers)
            var buffer = ByteBuffer(initialCapacity: bound) { output in
                instructions = encoder.encode(
                    headers: headers,
                    forStream: streamID,
                    into: &output,
                    preferHuffmanEncoding: preferHuffmanEncoding
                )
            }
            #expect(buffer.readableBytes <= bound, "round \(round)")

            let decoded = try buffer.readFieldSection()
            #expect(buffer.readableBytes == 0, "round \(round)")
            #expect(decoded == expected.fieldSection, "round \(round)")
            #expect(instructions == expected.instructions, "round \(round)")

            // Acknowledge the section, so its entries can be evicted to make room for the next ones.
            if expected.fieldSection.prefix.encodedRequiredInsertCount != 0 {
                try reference.processInstruction(.sectionAcknowledgement(streamID: streamID))
                try encoder.processInstruction(.sectionAcknowledgement(streamID: streamID))
            }
        }
    }

    @available(anyAppleOS 27, *)
    @Test func insertedPrefixMovesTheBytesAfterItBack() {
        let prefix = EncodedFieldSectionPrefix(encodedRequiredInsertCount: 3, deltaBase: 0, signBit: true)
        let buffer = ByteBuffer(initialCapacity: 8) { output in
            output.append(0xAA)
            // Four bytes reserved, of which the prefix needs two.
            output.append(repeating: 0, count: 4, as: UInt8.self)
            output.append(0xBB)
            output.append(0xCC)
            output.insertFieldSectionPrefix(prefix, at: 1, reservedLength: 4)
        }
        #expect(Array(buffer.readableBytesView) == [0xAA, 3, 0b10000000, 0xBB, 0xCC])
    }
}
