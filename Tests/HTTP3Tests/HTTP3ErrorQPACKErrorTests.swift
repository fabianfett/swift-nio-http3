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
import NIOQUICHelpers
@_spi(PackageInternal) import QPACK
import Testing

@_spi(PackageInternal) @testable import HTTP3

struct HTTP3ErrorQPACKErrorTests {
    @Test(
        arguments: [
            (QPACKError.duplicateSettings, HTTP3Error.Code.unexpectedFrame, HTTP3ErrorCode.frameUnexpected),
            (.invalidEncoderInstruction, .qpackEncoderStreamError, .qpackEncoderStreamError),
            (.decoderInstructionWithoutDynamicTable, .qpackDecoderStreamError, .qpackDecoderStreamError),
            (.invalidDecoderInstruction, .qpackDecoderStreamError, .qpackDecoderStreamError),
            (.invalidFieldSectionPrefix, .qpackDecoderError, .qpackDecompressionFailed),
            (.invalidFieldSection, .qpackDecoderError, .qpackDecompressionFailed),
            (.tooManyBlockedStreams, .qpackDecoderError, .qpackDecompressionFailed),
            (.invalidFieldName, .qpackDecoderError, .messageError),
        ]
    )
    func translation(qpackError: QPACKError, code: HTTP3Error.Code, h3ErrorCode: HTTP3ErrorCode) {
        let error = HTTP3Error(qpackError)
        #expect(error.code == code)
        #expect(error.h3ErrorCode == h3ErrorCode)
        #expect(error.cause as? QPACKError == qpackError)
    }

    @Test func fieldSectionFailuresNameTheStream() {
        #expect(HTTP3Error(.invalidFieldSection, streamID: 5).message == "Could not decode QPACK headers for stream 5")
        #expect(HTTP3Error(.invalidFieldSection).message == "Could not decode QPACK headers")
    }
}
