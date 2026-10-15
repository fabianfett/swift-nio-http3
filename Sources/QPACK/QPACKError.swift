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

/// Why the ``QPACKCoder`` failed.
///
/// The owner of the coder translates this into the error type of the protocol it runs QPACK for. Whether a failure
/// takes down the connection or only the stream it happened on is given by ``isConnectionError``.
@_spi(PackageInternal)
@nonexhaustive
public enum QPACKError: Error, Sendable, Hashable {
    /// The peer sent its settings more than once.
    case duplicateSettings

    /// The peer's encoder sent an instruction which can't be processed (RFC 9204 § 6).
    case invalidEncoderInstruction(EncoderInstructionError)

    /// The peer's decoder sent an instruction while no dynamic table is in use.
    case decoderInstructionWithoutDynamicTable

    /// The peer's decoder sent an instruction which can't be processed (RFC 9204 § 6).
    case invalidDecoderInstruction(DecoderInstructionError)

    /// The prefix of a field section could not have been produced by a conformant encoder (RFC 9204 § 4.5.1.1).
    case invalidFieldSectionPrefix

    /// A field section could not be decoded.
    case decodeError(QPACKDecoderError)

    /// More streams are blocked waiting for dynamic table entries than were promised.
    case tooManyBlockedStreams

    /// Whether the failure is fatal to the connection, rather than only to the stream it happened on.
    ///
    /// Everything which may leave the two dynamic tables out of sync is a connection error. The only stream error
    /// is a field section which decoded fine, but whose field names are not valid in HTTP/3, such as upper case
    /// ones: the message is malformed, but the connection is not at fault.
    public var isConnectionError: Bool {
        switch self {
        case .decodeError(.invalidHeaderName):
            return false
        case .duplicateSettings, .invalidEncoderInstruction, .decoderInstructionWithoutDynamicTable,
            .invalidDecoderInstruction, .invalidFieldSectionPrefix, .decodeError(.invalidReference),
            .decodeError(.invalidFieldSection), .tooManyBlockedStreams:
            // RFC 9204 § 2.2.3: a reference to a dynamic table entry that has already been evicted or that has an
            // absolute index greater than or equal to the declared Required Insert Count MUST be treated as a
            // connection error of type QPACK_DECOMPRESSION_FAILED.
            return true
        }
    }
}

extension QPACKError {
    /// Why an instruction from the peer's encoder can't be processed (RFC 9204 § 4.3).
    @nonexhaustive
    public enum EncoderInstructionError: Error, Sendable, Hashable {
        /// The capacity is higher than the maximum we advertised (RFC 9204 § 4.3.1).
        case capacityTooHigh

        /// The new entry is larger than the capacity of the dynamic table (RFC 9204 § 3.2.2).
        case entryTooLarge

        /// Making room would evict an entry which may still be referenced (RFC 9204 § 2.1.1).
        case cannotEvict

        /// The instruction references an entry which doesn't exist.
        case invalidReference

        /// The literal field name is not valid in HTTP/3.
        case invalidHeaderName
    }

    /// Why an instruction from the peer's decoder can't be processed (RFC 9204 § 4.4).
    @nonexhaustive
    public enum DecoderInstructionError: Error, Sendable, Hashable {
        /// The Insert Count Increment is zero (RFC 9204 § 4.4.3).
        case zeroInsertCountIncrement

        /// The Insert Count Increment acknowledges more entries than were inserted (RFC 9204 § 4.4.3).
        case insertCountIncrementTooHigh

        /// The Section Acknowledgment refers to a stream without unacknowledged field sections (RFC 9204 § 4.4.1).
        case unexpectedSectionAcknowledgment
    }
}

extension QPACKError.EncoderInstructionError {
    init(_ error: HeaderTableError) {
        switch error {
        case .insufficientStorage:
            self = .entryTooLarge
        case .cannotPurge:
            self = .cannotEvict
        }
    }
}

extension QPACKError.DecoderInstructionError {
    init(_ error: DynamicHeaderTableError) {
        switch error {
        case .incrementTooLow:
            self = .zeroInsertCountIncrement
        case .incrementTooHigh:
            self = .insertCountIncrementTooHigh
        case .ackCountTooLow, .ackCountTooHigh:
            // A Section Acknowledgment only acknowledges entries our own stream tracker recorded as inserted.
            assertionFailure("Section Acknowledgment acknowledged entries which weren't inserted: \(error)")
            self = .unexpectedSectionAcknowledgment
        }
    }

    init(_ error: QPACKEncoderError) {
        switch error {
        case .unexpectedStreamAck:
            self = .unexpectedSectionAcknowledgment
        case .unknownStream:
            // The stream tracker only forgets a stream once all of its field sections are acknowledged.
            assertionFailure("Section Acknowledgment for a stream the stream tracker doesn't know")
            self = .unexpectedSectionAcknowledgment
        }
    }
}
