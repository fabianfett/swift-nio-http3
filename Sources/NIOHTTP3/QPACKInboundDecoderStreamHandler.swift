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
@_spi(PackageInternal) import QPACK
@_spi(PackageInternal) import HTTP3
import NIOQUICHelpers

/// Read decoder instructions from a channel and give them to a callback.
/// This belongs on the incoming decoder stream.
/// The decoder instructions come from the remote decoder and should be fed into the local encoder.
final class QPACKInboundDecoderStreamHandler<QUICStreamCreator: NIOQUICHelpers.QUICStreamCreator, StreamDelegate: HTTP3StreamDelegate>: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let qpackCoder: NIOQPACKCoder<QUICStreamCreator, StreamDelegate>
    private var decoder: NIOSingleStepByteToMessageProcessor<QPACKDecoderInstructionDecoder>

    init(qpackCoder: NIOQPACKCoder<QUICStreamCreator, StreamDelegate>) {
        self.qpackCoder = qpackCoder
        self.decoder = NIOSingleStepByteToMessageProcessor(QPACKDecoderInstructionDecoder())
    }

    func errorCaught(context: ChannelHandlerContext, error: HTTP3Error) {
        self.qpackCoder.incomingDecoderInstructionStreamFailed(error)
        context.fireErrorCaught(error)
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
