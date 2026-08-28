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
import NIOQUICHelpers

/// Read encoder instructions from a channel and give them to a callback.
/// This belongs on the incoming encoder stream.
/// The encoder instructions come from the remote encoder and should be fed into the local decoder.
final class QPACKInboundEncoderStreamHandler<QUICStreamCreator: NIOQUICHelpers.QUICStreamCreator>: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let qpackCoder: NIOQPACKCoder<QUICStreamCreator>
    private var decoder: NIOSingleStepByteToMessageProcessor<QPACKEncoderInstructionDecoder>

    init(qpackCoder: NIOQPACKCoder<QUICStreamCreator>) {
        self.qpackCoder = qpackCoder
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.qpackCoder.incomingEncoderInstructionStreamFailed(error)
        context.fireErrorCaught(error)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var byteBuffer = self.unwrapInboundIn(data)
        do {
            try self.decoder.process(buffer: byteBuffer) { instruction in
                self.qpackCoder.receivedIncomingEncoderInstruction(instruction)
            }
        } catch {
            self.qpackCoder.incomingEncoderInstructionStreamFailed(error)
        }
    }
}
