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

import NIOCore

@_spi(PackageInternal) @testable public import HTTP3
@_spi(PackageInternal) @testable import QPACK

extension String {
    @available(anyAppleOS 27, *)
    var huffmanEncodedBytes: [UInt8] {
        let length = self.withContiguousUTF8 { QPACKEncodedLength.huffman(of: $0) }
        let buffer = ByteBuffer(initialCapacity: length) { output in
            self.withContiguousUTF8 { output.appendHuffmanEncoded($0, encodedByteLength: length) }
        }
        return Array(buffer.readableBytesView)
    }
}

@available(anyAppleOS 27, *)
extension HTTP3PushID: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: UInt64) {
        self.init(rawValue: value)
    }
}

@available(anyAppleOS 27, *)
extension HTTP3GoawayID: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: UInt64) {
        self.init(rawValue: value)
    }
}

extension ByteBuffer {
    /// Write `frame`. The stream state machine leaves HEADERS frames to the `QPACKCoder` and never writes PUSH_PROMISE
    /// frames, so this writes their encoded field sections as they are.
    @available(anyAppleOS 27, *)
    mutating func writeTestFrame(_ frame: HTTP3PartialFrame) {
        switch frame {
        case .headers(let payload):
            self.writeEncodedInteger(HTTP3FrameType.headers.rawValue, strategy: .quic)
            self.writeLengthPrefixed(strategy: .quic) { tempBuffer in
                tempBuffer.writeImmutableBuffer(payload.fieldSection)
            }
        case .pushPromise(let payload):
            self.writeEncodedInteger(HTTP3FrameType.pushPromise.rawValue, strategy: .quic)
            self.writeLengthPrefixed(strategy: .quic) { tempBuffer in
                tempBuffer.writeEncodedInteger(payload.pushID.rawValue, strategy: .quic)
                    + tempBuffer.writeImmutableBuffer(payload.fieldSection)
            }
        case .data(let data):
            let payloadLength = data.payload.readableBytes
            self.writeWithOutputRawSpan(
                minimumWritableBytes: HTTP3EncodedLength.dataFrameHeader(payloadLength: payloadLength)
            ) {
                $0.appendHTTP3DataFrameHeader(payloadLength: payloadLength)
            }
            self.writeImmutableBuffer(data.payload)
        case .settings(let settings):
            self.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.settingsFrame(settings.settings)) {
                $0.appendHTTP3SettingsFrame(settings.settings)
            }
        case .goaway(let goaway):
            self.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.goawayFrame(goaway.id)) {
                $0.appendHTTP3GoawayFrame(goaway.id)
            }
        case .maxPushID(let maxPushID):
            self.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.maxPushIDFrame(maxPushID.id)) {
                $0.appendHTTP3MaxPushIDFrame(maxPushID.id)
            }
        case .cancelPush(let cancelPush):
            self.writeWithOutputRawSpan(minimumWritableBytes: HTTP3EncodedLength.cancelPushFrame(cancelPush.pushID)) {
                $0.appendHTTP3CancelPushFrame(cancelPush.pushID)
            }
        }
    }
}
