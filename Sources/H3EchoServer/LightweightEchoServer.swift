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
public import NIOHTTP3
public import NIOQUIC

/// An HTTP/3 echo server built on ``HTTP3ServerConsumer``: it responds 200 to every request, with the request's
/// body as the response's body, and the request's trailers as its trailers.
///
/// The body is streamed back as it arrives, with back pressure in both directions: the server only reads as many
/// body bytes as the response stream will take right now, and resumes when it has room again. So a client
/// which sends faster than it reads is held back by QUIC's flow control on the request stream, rather than
/// having the server buffer its body.
@available(anyAppleOS 27, *)
public struct LightweightEchoServer: HTTP3ServerConsumer {
    public struct RequestState {
        /// The response head, until it's written: it's held back so a response without a body goes out in
        /// one frame, together with the end.
        var head: HTTPResponse?
        /// Body bytes read, but not yet written.
        var pending: ByteBuffer
    }

    public let http3Connection: HTTP3Connection<Self>

    public init(http3Connection: HTTP3Connection<Self>) {
        self.http3Connection = http3Connection
    }

    public func makeRequestState() -> RequestState {
        RequestState(head: nil, pending: ByteBuffer())
    }

    public func processRequest(_ request: inout HTTP3StreamState<RequestState>, stream: inout QUICStream<Self>) {
        // The room a QUIC stream reports is only advisory, and it may never say there's more (see
        // `HTTP3StreamState.writableBytes(_:)`): so every visit reads again, up to what the stream allows.
        request.resumeReading()
        do {
            try Self.echo(&request, stream: &stream)
        } catch {
            // Errors reading have already aborted the stream as HTTP/3 requires. A write fails when the
            // client stopped reading: there's nothing left to do with the request then.
            if !request.isOutboundComplete {
                request.cancel(stream: &stream)
            }
        }
    }

    private static func echo(
        _ request: inout HTTP3StreamState<RequestState>,
        stream: inout QUICStream<Self>
    ) throws {
        // Everything read in this visit is echoed in one write: a request which arrived whole is answered with a
        // single frame, which carries the FIN too.
        while !request.isInboundComplete {
            // Back pressure: only read as much of the body as can be sent straight back.
            var room = request.writableBytes(&stream) - request.state.pending.readableBytes
            let part = try request.readRequestPart(from: &stream) { bytes, state in
                let taken = min(bytes.byteCount, max(room, 0))
                if taken > 0 {
                    bytes.extracting(0..<taken).withUnsafeBytes { _ = state.pending.writeBytes($0) }
                    room -= taken
                }
                return taken
            }

            guard let part else { break }

            switch part {
            case .head(let head):
                var response = HTTPResponse(status: .ok)
                if let contentLength = head.headerFields[.contentLength] {
                    response.headerFields[.contentLength] = contentLength
                }
                request.state.head = response

            case .body:
                // Accumulated by the body closure, and sent below.
                ()

            case .end(let trailers):
                if let head = request.state.head.take() {
                    try request.writeResponse(head, body: request.state.pending, trailers: trailers, to: &stream)
                } else {
                    try request.writeBody(request.state.pending, to: &stream)
                    try request.writeEnd(trailers: trailers, to: &stream)
                }
                request.state.pending.clear()
                return
            }
        }

        // The rest of the request is still to come: send what there is of the body so far.
        guard request.state.pending.readableBytes > 0 else { return }
        if let head = request.state.head.take() {
            try request.writeResponseHead(head, to: &stream)
        }
        try request.writeBody(request.state.pending, to: &stream)
        request.state.pending.clear()
        try request.flush(&stream)
    }
}
