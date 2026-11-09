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
@_spi(PackageInternal) import QPACK
import Testing

@_spi(PackageInternal) @testable import HTTP3

struct HTTP3MessageEncodingTests {
    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func requestRoundTrips(preferHuffmanEncoding: Bool) throws {
        var request = HTTPRequest(method: .post, scheme: "https", authority: "example.com", path: "/upload")
        request.headerFields[.contentType] = "application/json"
        request.headerFields.append(HTTPField(name: HTTPField.Name("x-custom")!, value: "a value"))
        var authorization = HTTPField(name: .authorization, value: "secret")
        authorization.indexingStrategy = .disallow
        request.headerFields.append(authorization)

        var buffer = Self.headersFrame(request: request, preferHuffmanEncoding: preferHuffmanEncoding)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.request(fields: fields, extendedConnectSupported: false) == request)
    }

    @available(anyAppleOS 27, *)
    @Test func extendedConnectRequestRoundTrips() throws {
        var request = HTTPRequest(method: .connect, scheme: "https", authority: "example.com", path: "/chat")
        request.extendedConnectProtocol = "websocket"

        var buffer = Self.headersFrame(request: request, preferHuffmanEncoding: true)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.request(fields: fields, extendedConnectSupported: true) == request)
    }

    @available(anyAppleOS 27, *)
    @Test func responseRoundTrips() throws {
        var response = HTTPResponse(status: .ok)
        response.headerFields[.contentLength] = "42"
        response.headerFields[.server] = "swift-nio-http3"

        var buffer = Self.headersFrame(response: response, preferHuffmanEncoding: true)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.response(fields: fields) == response)
    }

    @available(anyAppleOS 27, *)
    @Test func trailersRoundTrip() throws {
        let trailers: HTTPFields = [HTTPField.Name("x-checksum")!: "1234"]

        var buffer = Self.headersFrame(trailers: trailers, preferHuffmanEncoding: false)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.trailers(fields: fields) == trailers)
    }

    @available(anyAppleOS 27, *)
    @Test func longFieldSectionRoundTrips() throws {
        // Long enough for the frame's length to take four bytes.
        var response = HTTPResponse(status: .ok)
        response.headerFields[HTTPField.Name("x-long")!] = String(repeating: "a", count: 20_000)

        var buffer = Self.headersFrame(response: response, preferHuffmanEncoding: false)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.response(fields: fields) == response)
    }

    @available(anyAppleOS 27, *)
    @Test func decodingRejectsDynamicTableReferences() throws {
        // An indexed field line: 1 for indexed, 0 for the dynamic table, then the relative index 0.
        let payload = Self.fieldSectionPayload(lines: [0b1000_0000])

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(payload.readableBytesSpan)
        } throws: { error in
            guard case .connectionError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .qpackDecompressionFailed
        }
    }

    @available(anyAppleOS 27, *)
    @Test func decodingRejectsInvalidFieldNamesAsStreamError() throws {
        // A literal field line with a literal name: 001, no Huffman coding, then the name's length of 10, which
        // overflows the 3 bit prefix. The value follows, without Huffman coding.
        let payload = Self.fieldSectionPayload(
            lines: [0b0010_0111, 10 - 7] + Array("Upper-Case".utf8) + [5] + Array("value".utf8)
        )

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(payload.readableBytesSpan)
        } throws: { error in
            guard case .streamError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .messageError
        }
    }

    @available(anyAppleOS 27, *)
    @Test func decodingRejectsTruncatedFieldLines() throws {
        // An indexed field line of the static table whose index overflows the 6 bit prefix, but then ends.
        let payload = Self.fieldSectionPayload(lines: [0b1111_1111])

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(payload.readableBytesSpan)
        } throws: { error in
            guard case .connectionError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .qpackDecompressionFailed
        }
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [[], [0], [1, 0]] as [[UInt8]])
    func decodingRejectsInvalidPrefixes(prefix: [UInt8]) throws {
        // No prefix, a missing Base, and a Required Insert Count which can't be zero.
        let payload = ByteBuffer(bytes: prefix)

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(payload.readableBytesSpan)
        } throws: { error in
            guard case .connectionError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .qpackDecompressionFailed
        }
    }

    /// The payload of a HEADERS frame carrying the encoded field `lines`, behind a prefix which references no dynamic
    /// table entries.
    private static func fieldSectionPayload(lines: [UInt8]) -> ByteBuffer {
        // Required Insert Count 0, then Base 0
        ByteBuffer(bytes: [0, 0] + lines)
    }

    /// The HEADERS frame of `request`, written into exactly the room the encoder asks for.
    @available(anyAppleOS 27, *)
    private static func headersFrame(request: HTTPRequest, preferHuffmanEncoding: Bool) -> ByteBuffer {
        let maxFieldSectionLength = HTTP3MessageEncoding.maxFieldSectionLength(of: request)
        let bytes = encodeWithExactRoom(HTTP3EncodedLength.headersFrame(maxPayloadLength: maxFieldSectionLength)) {
            HTTP3MessageEncoding.appendHeadersFrame(
                request: request,
                maxFieldSectionLength: maxFieldSectionLength,
                into: &$0,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
        }
        return ByteBuffer(bytes: bytes)
    }

    /// The HEADERS frame of `response`, written into exactly the room the encoder asks for.
    @available(anyAppleOS 27, *)
    private static func headersFrame(response: HTTPResponse, preferHuffmanEncoding: Bool) -> ByteBuffer {
        let maxFieldSectionLength = HTTP3MessageEncoding.maxFieldSectionLength(of: response)
        let bytes = encodeWithExactRoom(HTTP3EncodedLength.headersFrame(maxPayloadLength: maxFieldSectionLength)) {
            HTTP3MessageEncoding.appendHeadersFrame(
                response: response,
                maxFieldSectionLength: maxFieldSectionLength,
                into: &$0,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
        }
        return ByteBuffer(bytes: bytes)
    }

    /// The HEADERS frame of `trailers`, written into exactly the room the encoder asks for.
    @available(anyAppleOS 27, *)
    private static func headersFrame(trailers: HTTPFields, preferHuffmanEncoding: Bool) -> ByteBuffer {
        let maxFieldSectionLength = HTTP3MessageEncoding.maxFieldSectionLength(of: trailers)
        let bytes = encodeWithExactRoom(HTTP3EncodedLength.headersFrame(maxPayloadLength: maxFieldSectionLength)) {
            HTTP3MessageEncoding.appendHeadersFrame(
                trailers: trailers,
                maxFieldSectionLength: maxFieldSectionLength,
                into: &$0,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
        }
        return ByteBuffer(bytes: bytes)
    }

    /// Decodes a HEADERS frame the way the channel based stack does: through ``HTTP3FrameDecoder``.
    @available(anyAppleOS 27, *)
    private static func decodeHeadersFrame(_ buffer: inout ByteBuffer) throws -> [HTTPField] {
        var decoder = HTTP3FrameDecoder()
        let frame = try decoder.decode(buffer: &buffer)
        #expect(buffer.readableBytes == 0)
        guard case .known(.headers(let headers)) = frame else {
            Issue.record("Expected a HEADERS frame, got \(String(describing: frame))")
            return []
        }
        return try StaticQPACKDecoder().decodeFieldSection(headers.fieldSection.readableBytesSpan)
    }
}
