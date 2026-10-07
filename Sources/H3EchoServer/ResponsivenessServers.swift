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
public import NIOCore
public import NIOHTTP3
public import NIOHTTPTypes
public import NIOQUIC
import NIOQUICHelpers

/// What the performance workloads' `/responsiveness/download/<size>` serves: `size` zero bytes, in chunks of
/// this many.
let responsivenessMaxChunkSize = 64 * 1024

let responsivenessZeros = ByteBuffer(repeating: 0, count: responsivenessMaxChunkSize)

/// Parses `/responsiveness/download/<size>`, returning the size, or `nil` for any other path.
func responsivenessDownloadSize(_ request: HTTPRequest) -> Int? {
    guard request.method == .get, let path = request.path else { return nil }
    let prefix = "/responsiveness/download/"
    guard path.utf8.starts(with: prefix.utf8) else { return nil }
    var size = 0
    var sawDigit = false
    for byte in path.utf8.dropFirst(prefix.utf8.count) {
        if byte == UInt8(ascii: "?") { break }
        guard byte >= UInt8(ascii: "0"), byte <= UInt8(ascii: "9") else { return nil }
        size = size &* 10 &+ Int(byte &- UInt8(ascii: "0"))
        sawDigit = true
    }
    return sawDigit ? size : nil
}

/// The `/responsiveness/download/<size>` server of the `http3-swift-quic` performance workload, on
/// ``HTTP3ServerConsumer``.
///
/// Like the workload's `StreamPerfConsumer`, a download is written in one go: the QUIC stream queues what its
/// flow control doesn't let through yet.
@available(anyAppleOS 27, *)
public struct LightweightResponsivenessServer: HTTP3ServerConsumer {
    public enum RequestState {
        case idle
        case responded
    }

    public let http3Connection: HTTP3Connection<Self>

    public init(http3Connection: HTTP3Connection<Self>) {
        self.http3Connection = http3Connection
    }

    public func makeRequestState() -> RequestState {
        .idle
    }

    public func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        do {
            while let part = try request.readRequestPart(from: &stream, body: { bytes, _ in bytes.byteCount }) {
                switch part {
                case .head(let head):
                    try Self.respond(to: head, &request, stream: &stream)
                case .body, .end:
                    ()
                }
            }
        } catch {
            if !request.isOutboundComplete {
                request.cancel(stream: &stream)
            }
        }
    }

    private static func respond(
        to head: HTTPRequest,
        _ request: inout HTTP3StreamState<RequestState>,
        stream: inout QUICStream<Self>
    ) throws {
        guard case .idle = request.state else { return }
        request.state = .responded

        guard var remaining = responsivenessDownloadSize(head) else {
            let body = ByteBuffer(string: "Not Found")
            let response = HTTPResponse(status: .notFound, headerFields: [.contentLength: "\(body.readableBytes)"])
            try request.writeResponse(response, body: body, to: &stream)
            return
        }

        var response = HTTPResponse(status: .ok)
        response.headerFields[.contentLength] = "\(remaining)"
        try request.writeResponseHead(response, to: &stream)
        while remaining > 0 {
            let size = min(remaining, responsivenessMaxChunkSize)
            try request.writeBody(
                responsivenessZeros.getSlice(at: responsivenessZeros.readerIndex, length: size)!,
                to: &stream
            )
            remaining &-= size
        }
        try request.writeEnd(to: &stream)
    }
}

/// The `ResponsivenessHandler` of the `http3-swift-quic` performance workload, on the `Channel` based API of
/// `NIOHTTP3`, reduced to `/responsiveness/download/<size>`.
public final class ChannelResponsivenessHandler: ChannelInboundHandler {
    public typealias InboundIn = HTTPRequestPart
    public typealias OutboundOut = HTTPResponsePart

    private enum State {
        case idle
        case downloading(remaining: Int, isWritable: Bool)
        case done
    }

    private var state = State.idle
    private var flushPending = false
    private var inReadLoop = false

    public init() {}

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.inReadLoop = true
        switch self.unwrapInboundIn(data) {
        case .head(let head):
            guard case .idle = self.state else { return }
            guard let size = responsivenessDownloadSize(head) else {
                self.state = .done
                let body = context.channel.allocator.buffer(string: "Not Found")
                let response = HTTPResponse(status: .notFound, headerFields: [.contentLength: "\(body.readableBytes)"])
                context.write(self.wrapOutboundOut(.head(response)), promise: nil)
                context.write(self.wrapOutboundOut(.body(body)), promise: nil)
                context.write(self.wrapOutboundOut(.end(nil)), promise: nil)
                self.flushPending = true
                return
            }
            var response = HTTPResponse(status: .ok)
            response.headerFields[.contentLength] = "\(size)"
            context.write(self.wrapOutboundOut(.head(response)), promise: nil)
            self.flushPending = true
            self.state = .downloading(remaining: size, isWritable: true)
            self.resumeDownload(context: context)

        case .body, .end:
            ()
        }
    }

    public func channelReadComplete(context: ChannelHandlerContext) {
        self.inReadLoop = false
        if self.flushPending {
            self.flushPending = false
            context.flush()
        }
        context.fireChannelReadComplete()
    }

    public func channelWritabilityChanged(context: ChannelHandlerContext) {
        if case .downloading(let remaining, _) = self.state {
            let isWritable = context.channel.isWritable
            self.state = .downloading(remaining: remaining, isWritable: isWritable)
            if isWritable {
                self.resumeDownload(context: context)
            }
        }
        context.fireChannelWritabilityChanged()
    }

    private func resumeDownload(context: ChannelHandlerContext) {
        while case .downloading(var remaining, true) = self.state {
            let size = min(remaining, responsivenessMaxChunkSize)
            remaining &-= size
            let slice = responsivenessZeros.getSlice(at: responsivenessZeros.readerIndex, length: size)!
            context.write(self.wrapOutboundOut(.body(slice)), promise: nil)
            if remaining == 0 {
                context.write(self.wrapOutboundOut(.end(nil)), promise: nil)
                self.state = .done
            } else {
                self.state = .downloading(remaining: remaining, isWritable: true)
            }
            self.flushPending = true
        }

        // In a read loop the flush is left to channelReadComplete.
        if !self.inReadLoop && self.flushPending {
            self.flushPending = false
            context.flush()
        }
    }
}

@available(*, unavailable)
extension ChannelResponsivenessHandler: Sendable {}

/// Answers every request stream with the same pre-encoded HTTP/3 response, without parsing the request or
/// running any HTTP/3 connection logic (no control stream, no QPACK): the QUIC floor under the HTTP/3 servers.
///
/// It's what the HTTP/3 servers send for `/responsiveness/download/<size>` for the size it was made with: a
/// HEADERS frame with `:status: 200` and `content-length`, then DATA frames of at most 64 KiB, and a FIN.
@available(anyAppleOS 27, *)
public struct QUICDownloadConsumer: QUICStreamConsumer {
    public struct StreamState {
        var responded: Bool
    }

    private let response: ByteBuffer

    public init(downloadSize: Int) {
        self.response = Self.encodeResponse(size: downloadSize)
    }

    public func makeStreamState(_ stream: inout QUICStream<Self>) -> StreamState {
        StreamState(responded: false)
    }

    public func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let response = self.response
        while var visit = streams.next() {
            if visit.events.contains(.closed) {
                continue
            }

            visit.withStream { stream, state in
                // Everything the client sends is let go of: the request, and its control and QPACK streams.
                var sawBytes = false
                _ = stream.read { bytes in
                    sawBytes = sawBytes || bytes.byteCount > 0
                    return bytes.byteCount
                }

                guard let id = stream.id, !id.type.isUnidirectional, sawBytes, !state.responded else { return }
                state.responded = true
                stream.write(response)
                do {
                    try stream.flush(fin: true)
                } catch {
                    stream.sendResetStream(code: QUICApplicationErrorCode(0)!)
                }
            }
        }
    }

    static func encodeResponse(size: Int) -> ByteBuffer {
        // QPACK, static table only: required insert count and delta base 0, `:status: 200` (static index 25),
        // then `content-length` as a literal with a static name reference (index 4).
        var fields = ByteBuffer()
        fields.writeInteger(UInt8(0x00))
        fields.writeInteger(UInt8(0x00))
        fields.writeInteger(UInt8(0xC0 | 25))
        fields.writeInteger(UInt8(0x50 | 4))
        let length = String(size)
        precondition(length.utf8.count < 0x7F)
        fields.writeInteger(UInt8(length.utf8.count))
        fields.writeString(length)

        var buffer = ByteBuffer()
        Self.writeFrameHeader(type: 0x01, length: fields.readableBytes, into: &buffer)
        buffer.writeImmutableBuffer(fields)

        var remaining = size
        while remaining > 0 {
            let chunk = min(remaining, responsivenessMaxChunkSize)
            Self.writeFrameHeader(type: 0x00, length: chunk, into: &buffer)
            buffer.writeRepeatingByte(0, count: chunk)
            remaining &-= chunk
        }
        return buffer
    }

    private static func writeFrameHeader(type: UInt8, length: Int, into buffer: inout ByteBuffer) {
        buffer.writeInteger(type)
        switch length {
        case ..<(1 << 6):
            buffer.writeInteger(UInt8(length))
        case ..<(1 << 14):
            buffer.writeInteger(UInt16(length) | 0x4000)
        case ..<(1 << 30):
            buffer.writeInteger(UInt32(length) | 0x8000_0000)
        default:
            buffer.writeInteger(UInt64(length) | 0xC000_0000_0000_0000)
        }
    }
}
