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
public import NIOHTTPTypes
public import NIOQUIC
import NIOQUICHelpers

/// The echo server of ``LightweightEchoServer``, built on the `Channel` based API of `NIOHTTP3` instead: one
/// `Channel` per request stream, carrying `HTTPRequestPart`s.
///
/// Like the lightweight one, it streams the body back as it arrives. Back pressure uses the channel's
/// writability: reading stops while the stream isn't writable.
public final class ChannelEchoHandler: ChannelInboundHandler {
    public typealias InboundIn = HTTPRequestPart
    public typealias OutboundOut = HTTPResponsePart

    /// The response head, held back so a response without a body goes out in one flush with its end.
    private var head: HTTPResponse?
    private var needsFlush = false

    public init() {}

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch self.unwrapInboundIn(data) {
        case .head(let request):
            var response = HTTPResponse(status: .ok)
            if let contentLength = request.headerFields[.contentLength] {
                response.headerFields[.contentLength] = contentLength
            }
            self.head = response

        case .body(let body):
            if let head = self.head.take() {
                context.write(self.wrapOutboundOut(.head(head)), promise: nil)
            }
            context.write(self.wrapOutboundOut(.body(body)), promise: nil)
            self.needsFlush = true

        case .end(let trailers):
            if let head = self.head.take() {
                context.write(self.wrapOutboundOut(.head(head)), promise: nil)
            }
            context.write(self.wrapOutboundOut(.end(trailers)), promise: nil)
            self.needsFlush = true
        }
    }

    public func channelReadComplete(context: ChannelHandlerContext) {
        if self.needsFlush {
            self.needsFlush = false
            context.flush()
        }
        context.fireChannelReadComplete()
    }

    public func channelWritabilityChanged(context: ChannelHandlerContext) {
        // Back pressure: stop reading the request while the response can't be written.
        let isWritable = context.channel.isWritable
        try? context.channel.syncOptions?.setOption(ChannelOptions.autoRead, value: isWritable)
        if isWritable {
            context.read()
        }
        context.fireChannelWritabilityChanged()
    }
}

@available(*, unavailable)
extension ChannelEchoHandler: Sendable {}

/// Echoes the bytes of every QUIC stream back on it, without any HTTP/3: the floor for what the HTTP/3 layer
/// adds on top of QUIC.
///
/// Like the HTTP/3 echo servers it streams, reading only as much as the stream will take back.
@available(anyAppleOS 27, *)
public struct QUICEchoConsumer: QUICStreamConsumer {
    public struct StreamState {
        var pending: ByteBuffer
        var finished: Bool
    }

    /// How much a visit of a stream without room may queue: the room a stream reports is advisory, and one
    /// which waited for it to change might never be visited again. See
    /// `HTTP3StreamsConfiguration.minimumWritableBytes`.
    private static var minimumWritableBytes: Int { 16 * 1024 }

    public init() {}

    public func makeStreamState(_ stream: inout QUICStream<Self>) -> StreamState {
        StreamState(pending: ByteBuffer(), finished: false)
    }

    public func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while var visit = streams.next() {
            if visit.events.contains(.closed) {
                continue
            }

            visit.withStream { stream, state in
                guard !state.finished else { return }

                var room = stream.writableBytes
                if room <= 0 {
                    room = Self.minimumWritableBytes
                }
                room -= state.pending.readableBytes
                let outcome = stream.read { bytes in
                    let taken = min(bytes.byteCount, max(room, 0))
                    if taken > 0 {
                        bytes.extracting(0..<taken).withUnsafeBytes { _ = state.pending.writeBytes($0) }
                        room -= taken
                    }
                    return taken
                }

                let fin: Bool
                switch outcome {
                case .endOfStream:
                    fin = true
                case .read, .nothingAvailable:
                    fin = false
                }

                guard fin || state.pending.readableBytes > 0 else { return }
                if state.pending.readableBytes > 0 {
                    stream.write(state.pending)
                    state.pending = ByteBuffer()
                }
                state.finished = fin
                do {
                    try stream.flush(fin: fin)
                } catch {
                    state.finished = true
                    stream.sendResetStream(code: QUICApplicationErrorCode(0)!)
                }
            }
        }
    }
}
