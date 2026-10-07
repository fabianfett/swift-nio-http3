// swift-tools-version:6.4
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

import PackageDescription

// Builds `h3-echo-server` and `h3-load` the way the performance workloads build their servers: with the logging of
// SwiftNetwork and swift-log compiled out. Traits can only be set by the root package, hence this one.
//
//   swift build -c release --package-path Benchmarks/Echo --product h3-echo-server
//   swift build -c release --package-path Benchmarks/Echo --product h3-load

#if os(Linux)
let swiftNetworkTraits: Set<Package.Dependency.Trait> = [
    "DisableErrorLogging",
    "DisableDebugLogging",
    "DISABLE_SHIM_CRYPTO_SPAN_APIS",
]
#else
let swiftNetworkTraits: Set<Package.Dependency.Trait> = [
    "DisableErrorLogging",
    "DisableDebugLogging",
]
#endif

let package = Package(
    name: "swift-nio-http3-echo-benchmarks",
    dependencies: [
        .package(name: "swift-nio-http3", path: "../.."),
        .package(
            url: "https://github.com/apple/swift-network-evolution",
            .upToNextMinor(from: "0.4.0"),
            traits: swiftNetworkTraits
        ),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.10.1", traits: ["MaxLogLevelNone"]),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.82.0"),
        .package(url: "https://github.com/apple/swift-nio-quic.git", branch: "main"),
    ],
    targets: [
        // Only here so that the dependencies above, and with them their traits, are part of the build.
        .target(
            name: "EchoBenchmarks",
            dependencies: [
                .product(name: "NIOHTTP3", package: "swift-nio-http3"),
                .product(name: "SwiftNetwork", package: "swift-network-evolution"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .testTarget(
            name: "EchoBenchmarksTests",
            dependencies: [
                .product(name: "H3EchoServer", package: "swift-nio-http3"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOQUIC", package: "swift-nio-quic"),
            ],
            swiftSettings: [.enableExperimentalFeature("AnyAppleOSAvailability")]
        ),
    ]
)
