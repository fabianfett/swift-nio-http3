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
public import NIOCore
import NIOQUICHelpers
@_spi(PackageInternal) public import QPACK

extension QPACKDecoderInstructionDecoder: NIOSingleStepByteToMessageDecoder {}
@available(anyAppleOS 26.0, *)
extension QPACKEncoderInstructionDecoder: NIOSingleStepByteToMessageDecoder {}

@available(anyAppleOS 26.0, *)
typealias NIOQPACKCoder<
    ConnectionDelegate: QPACK.QPACKConnectionDelegate,
    StreamDelegate: HTTP3StreamDelegate
> = QPACK.QPACKCoder<
    QPACKOutboundEncoderStream,
    QPACKOutboundDecoderStream,
    ConnectionDelegate,
    HTTP3StreamHandler<StreamDelegate, ConnectionDelegate>
>

/// Gives the instructions read from the peer's QPACK streams to the ``QPACKCoder``.
///
/// Failing to read the streams isn't a failure of the coder, so these errors are reported as the ``HTTP3Error`` they
/// are, with all the details the stream handlers know about.
@available(anyAppleOS 26.0, *)
struct QPACKInboundStreamDelegate<
    ConnectionDelegate: QPACK.QPACKConnectionDelegate,
    StreamDelegate: HTTP3StreamDelegate
>: QPACKInboundEncoderStreamDelegate, QPACKInboundDecoderStreamDelegate {
    let qpackCoder: NIOQPACKCoder<ConnectionDelegate, StreamDelegate>
    let reportError: (HTTP3Error) -> Void

    func onReceivedInstruction(_ instruction: QPACKEncoderInstruction) {
        self.qpackCoder.receivedIncomingEncoderInstruction(instruction)
    }

    func onReceivedInstruction(_ instruction: QPACKDecoderInstruction) {
        self.qpackCoder.receivedIncomingDecoderInstruction(instruction)
    }

    func onError(_ error: HTTP3Error) {
        self.reportError(error)
    }
}
