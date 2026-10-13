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
/// takes down the connection or only the stream it happened on is up to the coder: see
/// ``QPACKConnectionDelegate/connectionError(_:)`` and ``QPACKDecodeReceiver/decodeResult(_:)``.
@_spi(PackageInternal)
public enum QPACKError: Error, Sendable, Hashable {
    /// The peer sent its settings more than once.
    case duplicateSettings

    /// The peer's encoder sent an instruction which can't be processed (RFC 9204 § 6).
    case invalidEncoderInstruction

    /// The peer's decoder sent an instruction while no dynamic table is in use.
    case decoderInstructionWithoutDynamicTable

    /// The peer's decoder sent an instruction which can't be processed (RFC 9204 § 6).
    case invalidDecoderInstruction

    /// The prefix of a field section could not have been produced by a conformant encoder (RFC 9204 § 4.5.1.1).
    case invalidFieldSectionPrefix

    /// A field section refers to a dynamic table entry which doesn't exist (RFC 9204 § 2.2.3).
    case invalidFieldSection

    /// More streams are blocked waiting for dynamic table entries than were promised.
    case tooManyBlockedStreams

    /// A field section was decoded, but its field names are not valid in HTTP/3, such as upper case ones. The
    /// message is malformed, but the connection is not at fault.
    case invalidFieldName
}
