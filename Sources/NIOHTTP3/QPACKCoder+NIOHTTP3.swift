//
//  QPACKCoder+NIOHTTP3.swift
//  swift-nio-http3
//
//  Created by Fabian Fett on 26.08.26.
//

@_spi(PackageInternal) import HTTP3
import HTTPTypes
import NIOCore
import NIOQUICHelpers
@_spi(PackageInternal) import QPACK

struct OutboundQPACKEncoderChannel: HTTP3.QPACKOutboundEncoderStream, ~Copyable {
    let encoder: QPACKEncoderInstructionEncoder

    var channel: any Channel
    var byteBuffer: ByteBuffer

    init(channel: any Channel, preferHuffmanEncoding: Bool) {
        self.encoder = QPACKEncoderInstructionEncoder(preferHuffmanEncoding: preferHuffmanEncoding)
        self.channel = channel
        self.byteBuffer = channel.allocator.buffer(capacity: 64)
    }

    mutating func sendInstructions(_ instructions: some Collection<QPACKEncoderInstruction>) {
        self.byteBuffer.clear()

        for instruction in instructions {
            self.encoder.encode(data: instruction, out: &self.byteBuffer)
        }

        self.channel.writeAndFlush(self.byteBuffer, promise: nil)
    }
}

struct OutboundQPACKDecoderChannel: HTTP3.QPACKOutboundDecoderStream, ~Copyable {
    let encoder: QPACKDecoderInstructionEncoder

    var channel: any Channel
    var byteBuffer: ByteBuffer

    init(channel: any Channel) {
        self.encoder = QPACKDecoderInstructionEncoder()
        self.channel = channel
        self.byteBuffer = channel.allocator.buffer(capacity: 64)
    }

    mutating func sendInstructions(_ instructions: some Collection<QPACKDecoderInstruction>) {
        self.byteBuffer.clear()

        for instruction in instructions {
            self.encoder.encode(data: instruction, out: &self.byteBuffer)
        }

        self.channel.writeAndFlush(self.byteBuffer, promise: nil)
    }

    mutating func sendInstruction(_ instruction: QPACKDecoderInstruction) {
        self.byteBuffer.clear()
        self.encoder.encode(data: instruction, out: &self.byteBuffer)
        self.channel.writeAndFlush(self.byteBuffer, promise: nil)
    }
}

typealias NIOQPACKCoder<
    ConnectionDelegate: HTTP3.ConnectionDelegate,
    StreamDelegate: HTTP3StreamDelegate
> = HTTP3.QPACKCoder<
    OutboundQPACKEncoderChannel,
    OutboundQPACKDecoderChannel,
    ConnectionDelegate,
    HTTP3StreamHandler<StreamDelegate, ConnectionDelegate>
>
