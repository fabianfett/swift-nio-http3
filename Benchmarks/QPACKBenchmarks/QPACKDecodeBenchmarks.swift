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

import Benchmark
import HTTPTypes
import NIOCore
import NIOQUICHelpers
@_spi(PackageInternal) import QPACK

/// A typical small response, as a server sends it for most requests.
private let responseHeaders: [HTTPField] = [
    HTTPField(name: .init(parsed: ":status")!, value: "200"),
    HTTPField(name: .init(parsed: "content-type")!, value: "text/html; charset=utf-8"),
    HTTPField(name: .init(parsed: "content-length")!, value: "1234"),
    HTTPField(name: .init(parsed: "date")!, value: "Fri, 16 Oct 2026 10:00:00 GMT"),
    HTTPField(name: .init(parsed: "server")!, value: "swift-nio-http3"),
    HTTPField(name: .init(parsed: "cache-control")!, value: "max-age=604800"),
]

private struct DiscardingEncoderStream: QPACKOutboundEncoderStream {
    mutating func sendInstructions(_ instructions: some Collection<QPACKEncoderInstruction>) {}
}

private struct DiscardingDecoderStream: QPACKOutboundDecoderStream {
    mutating func sendInstruction(_ instruction: QPACKDecoderInstruction) {}
    mutating func sendInstructions(_ instruction: some Collection<QPACKDecoderInstruction>) {}
}

private struct NoConnection: QPACKConnectionDelegate {
    func makeOutboundEncoderStream() {}
}

private struct BlackHoleDecodeReceiver: QPACKDecodeReceiver {
    func decodeResult(_ result: Result<[HTTPField], QPACKError>) {
        blackHole(result)
    }
}

/// The field section the static encoder writes for `fields`.
private func staticFieldSection(_ fields: [HTTPField], preferHuffmanEncoding: Bool) -> ByteBuffer {
    let encoder = StaticQPACKEncoder()
    return ByteBuffer(initialCapacity: encoder.maxEncodedLength(of: fields)) {
        encoder.encode(headers: fields, into: &$0, preferHuffmanEncoding: preferHuffmanEncoding)
    }
}

private var decodeConfiguration: Benchmark.Configuration {
    .init(
        metrics: [.mallocCountTotal, .instructions, .wallClock],
        units: [.instructions: .count],
        scalingFactor: .kilo
    )
}

/// Decodes a field section through the ``QPACKCoder``, as the channel based stack does.
private func coderDecodeBenchmark(_ name: String, fields: [HTTPField], preferHuffmanEncoding: Bool) {
    Benchmark(name, configuration: decodeConfiguration) { benchmark in
        let coder = QPACKCoder<DiscardingEncoderStream, DiscardingDecoderStream, NoConnection, BlackHoleDecodeReceiver>(
            encoderTableMaxCapacity: 0,
            decoderTableMaxCapacity: 0,
            decoderMaxBlockedStreams: 0,
            errorDelegate: NoConnection()
        )
        let fieldSection = staticFieldSection(fields, preferHuffmanEncoding: preferHuffmanEncoding)
        let streamID = QUICStreamID(rawValue: 0)

        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            coder.decodeFields(fieldSection, streamID: streamID, decodeReceiver: BlackHoleDecodeReceiver())
        }
    }
}

/// Decodes a field section with the ``StaticQPACKDecoder``, as the lightweight streams do.
private func staticDecodeBenchmark(_ name: String, fields: [HTTPField], preferHuffmanEncoding: Bool) {
    Benchmark(name, configuration: decodeConfiguration) { benchmark in
        let fieldSection = staticFieldSection(fields, preferHuffmanEncoding: preferHuffmanEncoding)
        let decoder = StaticQPACKDecoder()

        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            blackHole(try decoder.decodeFieldSection(fieldSection.readableBytesSpan))
        }
    }
}

func qpackDecodeBenchmarks() {
    coderDecodeBenchmark("QPACKCoderDecode_Static_BrowserGET", fields: requestHeaders, preferHuffmanEncoding: true)
    coderDecodeBenchmark("QPACKCoderDecode_Static_Response", fields: responseHeaders, preferHuffmanEncoding: true)
    coderDecodeBenchmark("QPACKCoderDecode_Plain_BrowserGET", fields: requestHeaders, preferHuffmanEncoding: false)
    staticDecodeBenchmark("StaticQPACKDecode_BrowserGET", fields: requestHeaders, preferHuffmanEncoding: true)
    staticDecodeBenchmark("StaticQPACKDecode_Response", fields: responseHeaders, preferHuffmanEncoding: true)
    staticDecodeBenchmark("StaticQPACKDecode_Plain_BrowserGET", fields: requestHeaders, preferHuffmanEncoding: false)
}
