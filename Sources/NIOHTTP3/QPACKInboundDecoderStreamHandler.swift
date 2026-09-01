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

@_spi(PackageInternal) import HTTP3
import NIOCore
import NIOQUICHelpers
@_spi(PackageInternal) import QPACK

/// Read decoder instructions from a channel and give them to a callback.
/// This belongs on the incoming decoder stream.
/// The decoder instructions come from the remote decoder and should be fed into the local encoder.
final class QPACKInboundDecoderStreamHandler<
    ConnectionDelegate: HTTP3.ConnectionDelegate,
    StreamDelegate: HTTP3StreamDelegate
>: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let qpackCoder: NIOQPACKCoder<ConnectionDelegate, StreamDelegate>
    private var decoder: NIOSingleStepByteToMessageProcessor<QPACKDecoderInstructionDecoder>

    init(qpackCoder: NIOQPACKCoder<ConnectionDelegate, StreamDelegate>) {
        self.qpackCoder = qpackCoder
        self.decoder = NIOSingleStepByteToMessageProcessor(QPACKDecoderInstructionDecoder())
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        let h3Error = HTTP3Error(
            code: .qpackDecoderStreamError,
            message: "Invalid QPACK instruction",
            cause: error,
            errorCode: .qpackDecoderStreamError,
            location: .here()
        )

        self.qpackCoder.incomingDecoderInstructionStreamFailed(h3Error)
        context.fireErrorCaught(h3Error)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let byteBuffer = self.unwrapInboundIn(data)
        do {
            try self.decoder.process(buffer: byteBuffer) { instruction in
                self.qpackCoder.receivedIncomingDecoderInstruction(instruction)
            }
        } catch {
            let h3Error = HTTP3Error(
                code: .qpackDecoderStreamError,
                message: "Invalid QPACK instruction",
                cause: error,
                errorCode: .qpackDecoderStreamError,
                location: .here()
            )
            self.qpackCoder.incomingDecoderInstructionStreamFailed(h3Error)
        }
    }
}
