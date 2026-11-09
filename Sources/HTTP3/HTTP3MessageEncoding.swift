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

/// Encodes the HEADERS frames of HTTP messages into an `OutputRawSpan`.
///
/// Unlike going through ``HTTP3Frame`` and ``QPACKCoder``, the field lines are written as they are encoded: no
/// intermediate `[HTTPField]` or list of field lines is built. The field sections only use the static table.
///
/// Work out the room a frame needs with `maxFieldSectionLength(of:)` and
/// ``HTTP3EncodedLength/headersFrame(maxPayloadLength:)``, then pass the same `maxFieldSectionLength` to
/// `appendHeadersFrame`, so the fields are only measured once.
@_spi(PackageInternal)
@available(anyAppleOS 27, *)
public enum HTTP3MessageEncoding {
    /// The largest number of bytes the field section of `request` takes.
    @_spi(PackageInternal)
    public static func maxFieldSectionLength(of request: HTTPRequest) -> Int {
        let encoder = StaticQPACKEncoder()
        let pseudo = request.pseudoHeaderFields
        var length = encoder.fieldSectionPrefixLength &+ encoder.maxEncodedLength(of: pseudo.method)
        if let scheme = pseudo.scheme {
            length &+= encoder.maxEncodedLength(of: scheme)
        }
        if let authority = pseudo.authority {
            length &+= encoder.maxEncodedLength(of: authority)
        }
        if let path = pseudo.path {
            length &+= encoder.maxEncodedLength(of: path)
        }
        if let extendedConnectProtocol = pseudo.extendedConnectProtocol {
            length &+= encoder.maxEncodedLength(of: extendedConnectProtocol)
        }
        return length &+ Self.maxEncodedLength(of: request.headerFields, encoder: encoder)
    }

    /// The largest number of bytes the field section of `response` takes.
    @_spi(PackageInternal)
    public static func maxFieldSectionLength(of response: HTTPResponse) -> Int {
        let encoder = StaticQPACKEncoder()
        return encoder.fieldSectionPrefixLength
            &+ encoder.maxEncodedLength(of: response.pseudoHeaderFields.status)
            &+ Self.maxEncodedLength(of: response.headerFields, encoder: encoder)
    }

    /// The largest number of bytes the field section of `trailers` takes.
    @_spi(PackageInternal)
    public static func maxFieldSectionLength(of trailers: HTTPFields) -> Int {
        let encoder = StaticQPACKEncoder()
        return encoder.fieldSectionPrefixLength &+ Self.maxEncodedLength(of: trailers, encoder: encoder)
    }

    /// Appends a HEADERS frame carrying `request`.
    ///
    /// - Precondition: `maxFieldSectionLength` is ``maxFieldSectionLength(of:)-(HTTPRequest)`` for `request`, and
    ///   `output` has room for ``HTTP3EncodedLength/headersFrame(maxPayloadLength:)`` of it.
    @_spi(PackageInternal)
    public static func appendHeadersFrame(
        request: HTTPRequest,
        maxFieldSectionLength: Int,
        into output: inout OutputRawSpan,
        preferHuffmanEncoding: Bool
    ) {
        let encoder = StaticQPACKEncoder()
        let pseudo = request.pseudoHeaderFields
        output.appendHTTP3HeadersFrame(maxPayloadLength: maxFieldSectionLength) { output in
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

    /// Appends a HEADERS frame carrying `response`.
    ///
    /// - Precondition: `maxFieldSectionLength` is ``maxFieldSectionLength(of:)-(HTTPResponse)`` for `response`,
    ///   and `output` has room for ``HTTP3EncodedLength/headersFrame(maxPayloadLength:)`` of it.
    @_spi(PackageInternal)
    public static func appendHeadersFrame(
        response: HTTPResponse,
        maxFieldSectionLength: Int,
        into output: inout OutputRawSpan,
        preferHuffmanEncoding: Bool
    ) {
        let encoder = StaticQPACKEncoder()
        output.appendHTTP3HeadersFrame(maxPayloadLength: maxFieldSectionLength) { output in
            encoder.appendFieldSectionPrefix(into: &output)
            encoder.append(
                response.pseudoHeaderFields.status,
                into: &output,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
            Self.append(
                response.headerFields,
                into: &output,
                encoder: encoder,
                preferHuffmanEncoding: preferHuffmanEncoding
            )
        }
    }

    /// Appends a HEADERS frame carrying `trailers`.
    ///
    /// - Precondition: `maxFieldSectionLength` is ``maxFieldSectionLength(of:)-(HTTPFields)`` for `trailers`, and
    ///   `output` has room for ``HTTP3EncodedLength/headersFrame(maxPayloadLength:)`` of it.
    @_spi(PackageInternal)
    public static func appendHeadersFrame(
        trailers: HTTPFields,
        maxFieldSectionLength: Int,
        into output: inout OutputRawSpan,
        preferHuffmanEncoding: Bool
    ) {
        let encoder = StaticQPACKEncoder()
        output.appendHTTP3HeadersFrame(maxPayloadLength: maxFieldSectionLength) { output in
            encoder.appendFieldSectionPrefix(into: &output)
            Self.append(trailers, into: &output, encoder: encoder, preferHuffmanEncoding: preferHuffmanEncoding)
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
