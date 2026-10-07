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

public import HTTPTypes
@_spi(PackageInternal) import QPACK

public import struct NIOCore.ByteBuffer

/// Encodes HTTP messages straight into HTTP/3 frames.
///
/// Unlike going through ``HTTP3Frame`` and ``QPACKCoder``, the field lines are written as they are encoded: no
/// intermediate `[HTTPField]` or list of field lines is built.
@_spi(PackageInternal)
@available(anyAppleOS 27, *)
public enum HTTP3MessageEncoding {
    /// Writes a HEADERS frame carrying `request`.
    @_spi(PackageInternal)
    public static func writeHeadersFrame(
        request: HTTPRequest,
        into buffer: inout ByteBuffer,
        preferHuffmanEncoding: Bool
    ) {
        let encoder = StaticQPACKEncoder()
        let pseudo = request.pseudoHeaderFields
        var maxPayloadLength = encoder.fieldSectionPrefixLength &+ encoder.maxEncodedLength(of: pseudo.method)
        if let scheme = pseudo.scheme {
            maxPayloadLength &+= encoder.maxEncodedLength(of: scheme)
        }
        if let authority = pseudo.authority {
            maxPayloadLength &+= encoder.maxEncodedLength(of: authority)
        }
        if let path = pseudo.path {
            maxPayloadLength &+= encoder.maxEncodedLength(of: path)
        }
        if let extendedConnectProtocol = pseudo.extendedConnectProtocol {
            maxPayloadLength &+= encoder.maxEncodedLength(of: extendedConnectProtocol)
        }
        maxPayloadLength &+= Self.maxEncodedLength(of: request.headerFields, encoder: encoder)

        Self.writeHeadersFrame(maxPayloadLength: maxPayloadLength, into: &buffer) { output in
            encoder.appendFieldSectionPrefix(into: &output)
            encoder.append(pseudo.method, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
            if let scheme = pseudo.scheme {
                encoder.append(scheme, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
            }
            if let authority = pseudo.authority {
                encoder.append(authority, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
            }
            if let path = pseudo.path {
                encoder.append(path, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
            }
            if let extendedConnectProtocol = pseudo.extendedConnectProtocol {
                encoder.append(extendedConnectProtocol, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
            }
            Self.append(
                request.headerFields,
                into: &output,
                encoder: encoder,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
        }
    }

    /// Writes a HEADERS frame carrying `response`.
    @_spi(PackageInternal)
    public static func writeHeadersFrame(
        response: HTTPResponse,
        into buffer: inout ByteBuffer,
        preferHuffmanEncoding: Bool
    ) {
        let encoder = StaticQPACKEncoder()
        let status = response.pseudoHeaderFields.status
        let maxPayloadLength =
            encoder.fieldSectionPrefixLength
            &+ encoder.maxEncodedLength(of: status)
            &+ Self.maxEncodedLength(of: response.headerFields, encoder: encoder)

        Self.writeHeadersFrame(maxPayloadLength: maxPayloadLength, into: &buffer) { output in
            encoder.appendFieldSectionPrefix(into: &output)
            encoder.append(status, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
            Self.append(
                response.headerFields,
                into: &output,
                encoder: encoder,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
        }
    }

    /// Writes a HEADERS frame carrying `trailers`.
    @_spi(PackageInternal)
    public static func writeHeadersFrame(
        trailers: HTTPFields,
        into buffer: inout ByteBuffer,
        preferHuffmanEncoding: Bool
    ) {
        let encoder = StaticQPACKEncoder()
        let maxPayloadLength =
            encoder.fieldSectionPrefixLength &+ Self.maxEncodedLength(of: trailers, encoder: encoder)

        Self.writeHeadersFrame(maxPayloadLength: maxPayloadLength, into: &buffer) { output in
            encoder.appendFieldSectionPrefix(into: &output)
            Self.append(trailers, into: &output, encoder: encoder, preferHuffmanEncoding: preferHuffmanEncoding)
        }
    }

    /// The number of bytes ``writeDataFrameHeader(payloadLength:into:)`` writes.
    @_spi(PackageInternal)
    public static func dataFrameHeaderLength(payloadLength: Int) -> Int {
        HTTP3EncodedLength.dataFrameHeader(payloadLength: payloadLength)
    }

    /// Writes the type and length of a DATA frame; the `payloadLength` bytes of payload must follow.
    @_spi(PackageInternal)
    public static func writeDataFrameHeader(payloadLength: Int, into buffer: inout ByteBuffer) {
        buffer.writeWithOutputRawSpan(
            minimumWritableBytes: HTTP3EncodedLength.dataFrameHeader(payloadLength: payloadLength)
        ) {
            $0.appendHTTP3DataFrameHeader(payloadLength: payloadLength)
        }
    }

    /// Writes a frame which carries no field section, such as SETTINGS or GOAWAY.
    ///
    /// - Precondition: `frame` is not HEADERS or PUSH\_PROMISE, those need ``writeHeadersFrame(request:into:preferHuffmanEncoding:)``
    ///   and friends.
    @_spi(PackageInternal)
    public static func writeFrame(_ frame: HTTP3Frame, into buffer: inout ByteBuffer) {
        switch frame {
        case .data(let data):
            Self.writeDataFrameHeader(payloadLength: data.payload.readableBytes, into: &buffer)
            buffer.writeImmutableBuffer(data.payload)
        case .settings(let settings):
            buffer.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.settingsFrame(settings.settings)) {
                $0.appendHTTP3SettingsFrame(settings.settings)
            }
        case .goaway(let goaway):
            buffer.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.goawayFrame(goaway.id)) {
                $0.appendHTTP3GoawayFrame(goaway.id)
            }
        case .maxPushID(let maxPushID):
            buffer.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.maxPushIDFrame(maxPushID.id)) {
                $0.appendHTTP3MaxPushIDFrame(maxPushID.id)
            }
        case .cancelPush(let cancelPush):
            buffer.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.cancelPushFrame(cancelPush.pushID)) {
                $0.appendHTTP3CancelPushFrame(cancelPush.pushID)
            }
        case .headers, .pushPromise:
            preconditionFailure("\(frame.type) frames carry a field section and must be QPACK encoded")
        }
    }

    /// Writes a HEADERS frame whose payload, at most `maxPayloadLength` bytes, `encodePayload` appends.
    private static func writeHeadersFrame(
        maxPayloadLength: Int,
        into buffer: inout ByteBuffer,
        encodePayload: (inout OutputRawSpan) -> Void
    ) {
        buffer.writeWithOutputRawSpan(
            minimumWritableBytes: HTTP3EncodedLength.headersFrame(maxPayloadLength: maxPayloadLength)
        ) {
            $0.appendHTTP3HeadersFrame(maxPayloadLength: maxPayloadLength, encodePayload: encodePayload)
        }
    }

    private static func maxEncodedLength(of fields: HTTPFields, encoder: StaticQPACKEncoder) -> Int {
        var length = 0
        for field in fields {
            length &+= encoder.maxEncodedLength(of: field)
        }
        return length
    }

    private static func append(
        _ fields: HTTPFields,
        into output: inout OutputRawSpan,
        encoder: StaticQPACKEncoder,
        preferHuffmanEncoding: Bool
    ) {
        for field in fields {
            encoder.append(field, into: &output, preferHuffmanEncoding: preferHuffmanEncoding)
        }
    }
}

@available(anyAppleOS 27, *)
extension HTTP3MessageDecoding {
    /// Why a field section couldn't be decoded.
    @_spi(PackageInternal)
    public enum DecodeError: Error {
        /// The connection must be torn down with this error.
        case connectionError(HTTP3Error)
        /// Only the stream the field section arrived on is affected.
        case streamError(HTTP3Error)
    }

    /// Decodes the whole of `payload`, the payload of a HEADERS frame, into its fields.
    ///
    /// Only field sections which don't reference the dynamic table can be decoded: this is for endpoints which
    /// advertise a `SETTINGS_QPACK_MAX_TABLE_CAPACITY` of zero.
    ///
    /// - Throws: A ``DecodeError``, which tells whether the connection or only the stream is at fault.
    @_spi(PackageInternal)
    public static func decodeHeadersFramePayload(
        _ payload: borrowing RawSpan
    ) throws(DecodeError) -> [HTTPField] {
        do {
            // The field section runs to the end of the payload.
            return try StaticQPACKDecoder().decodeFieldSection(payload)
        } catch {
            switch error {
            case .invalidFieldSection, .invalidReference:
                // RFC 9204 § 2.2.3: if the decoder encounters a reference to a dynamic table entry which has
                // been evicted, or whose absolute index is at or above the Required Insert Count, it MUST
                // treat this as a connection error of type QPACK_DECOMPRESSION_FAILED. With no dynamic table
                // any such reference falls into that category, as does a non-zero Required Insert Count. RFC 9204
                // § 6 makes a field section which can't be read one, too.
                throw .connectionError(
                    HTTP3Error(
                        code: .qpackDecoderError,
                        message: "Could not decode QPACK headers",
                        cause: error,
                        errorCode: .qpackDecompressionFailed,
                        location: .here()
                    )
                )
            case .invalidHeaderName:
                // Here we did decode fine, but the header is not valid in HTTP/3 (e.g. it isn't lower case).
                // This is a malformed message, a stream level error. Not a connection error.
                throw .streamError(
                    HTTP3Error(
                        code: .qpackDecoderError,
                        message: "Could not decode QPACK headers",
                        cause: error,
                        errorCode: .messageError,
                        location: .here()
                    )
                )
            }
        }
    }
}
