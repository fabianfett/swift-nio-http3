//
//  QPACKCoder.swift
//  swift-nio-http3
//
//  Created by Fabian Fett on 26.08.26.
//

@_spi(PackageInternal) public import QPACK
public import NIOQUICHelpers
public import HTTPTypes

protocol _HTTPField {
    var name: String { get }
    #warning("TODO: This should likely get a RawSpan")
    var value: String { get }
    var indexingStrategy: UInt8 { get }
}

@_spi(PackageInternal)
public protocol QPACKOutboundEncoderStream: ~Copyable {
    mutating func sendInstructions(_ instructions: some Collection<QPACKEncoderInstruction>)
}

@_spi(PackageInternal)
public protocol QPACKOutboundDecoderStream: ~Copyable {
    mutating func sendInstruction(_ instruction: QPACKDecoderInstruction)
    mutating func sendInstructions(_ instruction: some Collection<QPACKDecoderInstruction>)
}

@_spi(PackageInternal)
public protocol ConnectionDelegate {
    func connectionError(_ error: HTTP3Error)
    func makeOutboundEncoderStream()
}

@_spi(PackageInternal)
public protocol QPACKDecodeReceiver {
    func decodeResult(_ result: Result<[HTTPField], HTTP3Error>)
}

public protocol OutboundHTTPFields {

}

public protocol InboundHTTPFields {

}

@_spi(PackageInternal)
public final class QPACKCoder<
    OutboundEncoderStream: QPACKOutboundEncoderStream & ~Copyable,
    OutboundDecoderStream: QPACKOutboundDecoderStream & ~Copyable,
//    OutboundHTTPFields: HTTP3.OutboundHTTPFields,
//    InboundHTTPFields: HTTP3.InboundHTTPFields,
    ConnectionDelegate: HTTP3.ConnectionDelegate,
    DecodeReceiver: QPACKDecodeReceiver
> {

    private var outboundEncoderStream: OutboundEncoderStream?

    private var outboundDecoderStream: OutboundDecoderStream?

    private var stateMachine: QPACKStateMachine<DecodeReceiver>

    private let connection: ConnectionDelegate

    @_spi(PackageInternal)
    public init(
        decoderMaxTableSize: Int,
        decoderMaxBlockedStreams: Int,
        errorDelegate: ConnectionDelegate
    ) {
        self.stateMachine = QPACKStateMachine(
            decoderMaxTableSize: decoderMaxTableSize,
            decoderMaxBlockedStreams: decoderMaxBlockedStreams
        )
        self.connection = errorDelegate
    }

    @_spi(PackageInternal)
    public func receivedRemoteSettings(
        maxQueueSize: Int,
        effectiveDynamicTableSize: Int
    ) {
        let action = self.stateMachine.receivedRemoteSettings(
            maxQueueSize: maxQueueSize,
            effectiveDynamicTableSize: effectiveDynamicTableSize
        )

        switch action {
        case .makeEncoderInstructionStream:
            self.connection.makeOutboundEncoderStream()
        case .none:
            break
        }
    }

    // MARK: Encode

    /// It will handle sending any necessary instructions to the remote, on the dedicated QPACK stream.
    @_spi(PackageInternal)
    public func encodeHeaders(_ fields: [HTTPField], forStream streamID: QUICStreamID) -> HTTP3PartialFrame.Headers {
        let result = self.stateMachine.encodeHeaders(fields, forStream: streamID)

        if !result.instructions.isEmpty {
            self.outboundEncoderStream!.sendInstructions(result.instructions)
        }

        return HTTP3PartialFrame.Headers(fieldSection: result.fieldSection)
    }

    @_spi(PackageInternal)
    public func outboundEncoderStreamReady(_ stream: consuming OutboundEncoderStream) {
        switch self.stateMachine.outboundEncoderStreamReady() {
        case .sendEncoderInstruction(let instruction):
            self.outboundEncoderStream = consume stream
            guard let instruction else { break }
            self.outboundEncoderStream!.sendInstructions(CollectionOfOne(instruction))
        case .none:
            // The connection was shut down while this stream was being created. Drop it: we will never
            // write anything on it.
            break
        }
    }

    @_spi(PackageInternal)
    public func receivedIncomingDecoderInstruction(_ instruction: QPACKDecoderInstruction) {
        switch self.stateMachine.receivedIncomingDecoderInstruction(instruction) {
        case .emitConnectionError(let error):
            self.connection.connectionError(error)
        case .none:
            break
        }
    }

    @_spi(PackageInternal)
    public func incomingDecoderInstructionStreamFailed(_ error: HTTP3Error) {
        self.connection.connectionError(error)
    }

    // MARK: Decode

    /// Tell the connection coordinator that we want to decode a header. It will handle queueing and call back into us when it has a result.
    @_spi(PackageInternal)
    public func decodeHeaders(_ headers: HTTP3PartialFrame.Headers, forStream streamID: QUICStreamID, decodeReceiver: DecodeReceiver) {
        let action = self.stateMachine.decodeHeaders(headers, forStream: streamID, receiver: decodeReceiver)
        self.runDecodeHeaderAction(action)
    }

    @_spi(PackageInternal)
    public func outboundDecoderStreamReady(_ stream: consuming OutboundDecoderStream) {
        self.outboundDecoderStream = consume stream

        let action = self.stateMachine.outboundDecoderStreamReady()
        switch action {
        case .sendDecoderInstructions(let instructions):
            self.outboundDecoderStream!.sendInstructions(instructions)
        case .none:
            break
        }
    }

    @_spi(PackageInternal)
    public func receivedIncomingEncoderInstruction(
        _ instruction: QPACKEncoderInstruction
    ) {
        let action = self.stateMachine.receivedIncomingEncoderInstruction(instruction)
        switch action {
        case .sendDecoderInstruction(let qPACKDecoderInstruction):
            self.outboundDecoderStream!.sendInstruction(qPACKDecoderInstruction)
        case .emitConnectionError(let http3Error):
            self.connection.connectionError(http3Error)
        case .none:
            break
        }

        self.runDecodeHeaderAction(self.stateMachine.checkPendingDecodes())
    }

    private func runDecodeHeaderAction(_ action: QPACKStateMachine<DecodeReceiver>.DecodeHeaderAction?) {
        switch action {
        case .informDecodeResult(let result, let receiver):
            if let instruction = result.instructionToWrite {
                self.outboundDecoderStream!.sendInstruction(instruction)
            }
            receiver.decodeResult(.success(result.fields))

        case .informDecodeError(let informDecodeError, let receiver):
            receiver.decodeResult(.failure(informDecodeError.error))

        case .emitConnectionError(let http3Error, let receiver):
            self.connection.connectionError(http3Error)
            receiver.decodeResult(.failure(http3Error))

        case .none:
            // the required encoder dynamic table update hasn't arrived yet.
            break
        }
    }

    @_spi(PackageInternal)
    public func incomingEncoderInstructionStreamFailed(_ error: HTTP3Error) {
        self.connection.connectionError(error)
    }

    // MARK: Stream Management

    @_spi(PackageInternal)
    public func requestStreamClosed(streamID: QUICStreamID, seenEOF: Bool) {
        switch self.stateMachine.requestStreamClosed(streamID: streamID, seenEOF: seenEOF) {
        case .sendDecoderInstruction(let instruction):
            self.outboundDecoderStream!.sendInstruction(instruction)
        case .none:
            break
        }
    }

    // MARK: Shutdown

    /// Call this when the connection has been shut down.
    ///
    /// Afterwards every inbound QPACK instruction is dropped and no further instructions are written:
    /// the peer is gone, and so are the streams that were waiting on blocked decodes.
    ///
    /// It is safe to call this more than once.
    @_spi(PackageInternal)
    public func shutdownConnection() {
        self.stateMachine.shutdown()
        // Nothing more will be written on these, so let go of the channels behind them.
        self.outboundEncoderStream = nil
        self.outboundDecoderStream = nil
    }
}
