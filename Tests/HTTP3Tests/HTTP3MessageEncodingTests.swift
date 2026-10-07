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

        var buffer = ByteBuffer()
        HTTP3MessageEncoding.writeHeadersFrame(
            request: request,
            into: &buffer,
            preferHuffmanEncoding: preferHuffmanEncoding
        )

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.request(fields: fields) == request)
    }

    @available(anyAppleOS 27, *)
    @Test func extendedConnectRequestRoundTrips() throws {
        var request = HTTPRequest(method: .connect, scheme: "https", authority: "example.com", path: "/chat")
        request.extendedConnectProtocol = "websocket"

        var buffer = ByteBuffer()
        HTTP3MessageEncoding.writeHeadersFrame(request: request, into: &buffer, preferHuffmanEncoding: true)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.request(fields: fields) == request)
    }

    @available(anyAppleOS 27, *)
    @Test func responseRoundTrips() throws {
        var response = HTTPResponse(status: .ok)
        response.headerFields[.contentLength] = "42"
        response.headerFields[.server] = "swift-nio-http3"

        var buffer = ByteBuffer()
        HTTP3MessageEncoding.writeHeadersFrame(response: response, into: &buffer, preferHuffmanEncoding: true)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.response(fields: fields) == response)
    }

    @available(anyAppleOS 27, *)
    @Test func trailersRoundTrip() throws {
        let trailers: HTTPFields = [HTTPField.Name("x-checksum")!: "1234"]

        var buffer = ByteBuffer()
        HTTP3MessageEncoding.writeHeadersFrame(trailers: trailers, into: &buffer, preferHuffmanEncoding: false)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.trailers(fields: fields) == trailers)
    }

    @available(anyAppleOS 27, *)
    @Test func longFieldSectionRoundTrips() throws {
        // Longer than the two byte length the frame is written with up front.
        var response = HTTPResponse(status: .ok)
        response.headerFields[HTTPField.Name("x-long")!] = String(repeating: "a", count: 20_000)

        var buffer = ByteBuffer()
        HTTP3MessageEncoding.writeHeadersFrame(response: response, into: &buffer, preferHuffmanEncoding: false)

        let fields = try Self.decodeHeadersFrame(&buffer)
        #expect(try HTTP3MessageDecoding.response(fields: fields) == response)
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [0, 1, 63, 64, 16_383, 16_384])
    func dataFrameHeaderLengthMatchesWhatIsWritten(payloadLength: Int) throws {
        var buffer = ByteBuffer()
        HTTP3MessageEncoding.writeDataFrameHeader(payloadLength: payloadLength, into: &buffer)
        #expect(buffer.readableBytes == HTTP3MessageEncoding.dataFrameHeaderLength(payloadLength: payloadLength))

        buffer.writeRepeatingByte(0, count: payloadLength)
        var decoder = HTTP3FrameDecoder()
        let frame = try decoder.decode(buffer: &buffer)
        guard case .known(.data(let data)) = frame else {
            Issue.record("Expected a DATA frame, got \(String(describing: frame))")
            return
        }
        #expect(data.payload.readableBytes == payloadLength)
    }

    @available(anyAppleOS 27, *)
    @Test func decodingRejectsDynamicTableReferences() throws {
        var payload = Self.fieldSectionPayload([.indexed(.dynamicTable, index: 0)])

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(&payload)
        } throws: { error in
            guard case .connectionError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .qpackDecompressionFailed
        }
    }

    @available(anyAppleOS 27, *)
    @Test func decodingRejectsInvalidFieldNamesAsStreamError() throws {
        var payload = Self.fieldSectionPayload([
            .literal(requireLiteralRepresentation: false, name: "Upper-Case", value: "value")
        ])

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(&payload)
        } throws: { error in
            guard case .streamError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .messageError
        }
    }

    @available(anyAppleOS 27, *)
    @Test func decodingRejectsTrailingBytes() throws {
        var payload = Self.fieldSectionPayload([.indexed(.staticTable, index: 17)])
        payload.writeInteger(UInt8(0xff))

        #expect {
            try HTTP3MessageDecoding.decodeHeadersFramePayload(&payload)
        } throws: { error in
            guard case .connectionError(let error) = error as? HTTP3MessageDecoding.DecodeError else { return false }
            return error.h3ErrorCode == .frameError
        }
    }

    /// The payload of a HEADERS frame carrying `lines`, behind a prefix which references no dynamic table entries.
    @available(anyAppleOS 27, *)
    private static func fieldSectionPayload(_ lines: [FieldLine]) -> ByteBuffer {
        let fieldSection = FieldSection(
            prefix: .init(encodedRequiredInsertCount: 0, deltaBase: 0, signBit: false),
            lines: lines
        )
        var payload = ByteBuffer()
        payload.writeWithOutputRawSpan(minimumWritableBytes: fieldSection.maxEncodedLength) {
            $0.appendFieldSection(fieldSection, preferHuffmanEncoding: false)
        }
        return payload
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
        return try StaticQPACKDecoder().decodeFieldSection(headers.fieldSection)
    }
}
