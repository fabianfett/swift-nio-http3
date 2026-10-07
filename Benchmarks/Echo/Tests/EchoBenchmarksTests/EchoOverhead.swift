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

import Foundation
import H3EchoServer
import Logging
import NIOCore
import NIOPosix
import NIOQUIC
import Testing

/// Compares what a request costs the echo server on each API, in one process: the server runs on an event
/// loop of its own, whose thread's CPU time is measured, and the same load generator drives every server.
///
///   H3_ECHO_CERTS=<dir with ca-chain.pem, private-key.pem, trust-store.pem> \
///     swift test -c release --package-path Benchmarks/Echo --filter EchoOverhead
@Suite(.serialized)
struct EchoOverhead {
    @Test(arguments: [0, 1024])
    @available(anyAppleOS 27, *)
    func serverCostPerRequest(bodySize: Int) async throws {
        guard let certificates = ProcessInfo.processInfo.environment["H3_ECHO_CERTS"] else {
            print("Set H3_ECHO_CERTS to run the echo benchmark")
            return
        }
        let rounds = Int(ProcessInfo.processInfo.environment["H3_ECHO_ROUNDS"] ?? "3")!
        let requests = Int(ProcessInfo.processInfo.environment["H3_ECHO_REQUESTS"] ?? "200000")!
        let apis: [EchoServerAPI] = [.quic, .lightweight, .channel]

        let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        var results: [EchoServerAPI: [(rps: Double, cpu: Double)]] = [:]

        for round in 0..<rounds {
            // Rotate the order, so no API always runs first.
            for index in 0..<apis.count {
                let api = apis[(index + round) % apis.count]
                var quicConfiguration = QUICConfiguration.forServer(
                    serverName: "",
                    authenticationConfiguration: .x509Certificates(
                        certificateChainFilePath: "\(certificates)/ca-chain.pem",
                        privateKeyFilePath: "\(certificates)/private-key.pem"
                    ),
                    applicationProtocols: [api == .quic ? "echo" : "h3"]
                )
                quicConfiguration.initialMaxData = 5_000_000
                quicConfiguration.initialMaxStreamDataBidiLocal = 131_072
                quicConfiguration.initialMaxStreamDataBidiRemote = 5_000_000
                quicConfiguration.initialMaxStreamDataUni = 131_072
                quicConfiguration.initialMaxStreamsBidi = 100
                quicConfiguration.initialMaxStreamsUni = 100
                let serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
                let server = try await EchoServer.bind(
                    api: api,
                    host: "127.0.0.1",
                    port: 0,
                    quicConfiguration: quicConfiguration,
                    group: serverGroup,
                    logger: Logger(label: "echo")
                )

                func load(_ requests: Int) async throws -> TimeAmount {
                    try await EchoLoad.run(
                        mode: api == .quic ? .quic : .http3,
                        address: server.localAddress!,
                        serverName: "localhost",
                        trustRoots: "\(certificates)/trust-store.pem",
                        connections: 4,
                        concurrency: 100,
                        requests: requests,
                        bodySize: bodySize,
                        group: clientGroup
                    )
                }

                // Warm up: a fresh server is slower at first.
                _ = try await load(requests / 4)

                let before = try await serverGroup.next().submit { threadCPUTime() }.get()
                let elapsed = try await load(requests)
                let after = try await serverGroup.next().submit { threadCPUTime() }.get()

                let rps = Double(requests) / (Double(elapsed.nanoseconds) / 1e9)
                let cpu = Double(after - before) / 1e3 / Double(requests)
                results[api, default: []].append((rps, cpu))
                print(
                    "echo body=\(bodySize) round=\(round) api=\(api.rawValue) req/s=\(Int(rps)) server_us_per_req=\(cpu)"
                )

                try await server.close()
                try await serverGroup.shutdownGracefully()
            }
        }
        try await clientGroup.shutdownGracefully()

        func median(_ values: [Double]) -> Double {
            let sorted = values.sorted()
            return sorted[sorted.count / 2]
        }
        let quic = median(results[.quic]!.map(\.cpu))
        for api in apis {
            let cpu = median(results[api]!.map(\.cpu))
            let rps = median(results[api]!.map(\.rps))
            print(
                "echo body=\(bodySize) api=\(api.rawValue) median req/s=\(Int(rps)) server_us_per_req=\(cpu) "
                    + "over_quic_us=\(cpu - quic) x\(cpu / quic)"
            )
        }
    }
}

/// The CPU time of the calling thread, in nanoseconds.
private func threadCPUTime() -> UInt64 {
    var time = timespec()
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time)
    return UInt64(time.tv_sec) * 1_000_000_000 + UInt64(time.tv_nsec)
}
