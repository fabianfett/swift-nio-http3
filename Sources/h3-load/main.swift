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

import H3EchoServer
import NIOCore
import NIOPosix

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

// A load generator for `h3-echo-server`: it keeps a number of requests in flight on each connection, and reports
// the rate at which they complete.
//
//   h3-load --mode h3|quic --port 8443 --trust-roots <pem> [--connections 1] [--concurrency 100]
//           [--requests 200000] [--body-size 0] [--threads 1] [--path /]
//           [--warm-up <seconds>] [--duration <seconds>] [--server-pid <pid>]
//
// `h3` sends HTTP/3 requests (GET without a body, POST with one) with an `HTTP3ClientConsumer`. `quic` sends as many
// bytes as that on a bare QUIC stream, for the `quic` echo server: the difference is what HTTP/3 costs.
//
// With `--duration`, it runs for that long instead of for `--requests`, after `--warm-up` on the same connections.
// With `--server-pid`, it sends that process SIGUSR1 right before and right after the measured phase: the servers
// print their CPU time when they get it.

struct Options {
    var mode = "h3"
    var host = "127.0.0.1"
    var port = 8443
    var serverName = "localhost"
    var trustRoots: String?
    var connections = 1
    var concurrency = 100
    var requests = 200_000
    var bodySize = 0
    var threads = 1
    var path: String?
    var warmUp: Double?
    var duration: Double?
    var serverPID: Int32?

    static func parse(_ arguments: [String]) -> Options {
        var options = Options()
        var iterator = arguments.dropFirst().makeIterator()

        func value(for flag: String) -> String {
            guard let value = iterator.next() else { fatalError("Missing a value for \(flag)") }
            return value
        }

        while let argument = iterator.next() {
            switch argument {
            case "--mode": options.mode = value(for: argument)
            case "--host": options.host = value(for: argument)
            case "--port": options.port = Int(value(for: argument))!
            case "--server-name": options.serverName = value(for: argument)
            case "--trust-roots": options.trustRoots = value(for: argument)
            case "--connections": options.connections = Int(value(for: argument))!
            case "--concurrency": options.concurrency = Int(value(for: argument))!
            case "--requests": options.requests = Int(value(for: argument))!
            case "--body-size": options.bodySize = Int(value(for: argument))!
            case "--threads": options.threads = Int(value(for: argument))!
            case "--path": options.path = value(for: argument)
            case "--warm-up": options.warmUp = Double(value(for: argument))!
            case "--duration": options.duration = Double(value(for: argument))!
            case "--server-pid": options.serverPID = Int32(value(for: argument))!
            default: fatalError("Unknown argument \(argument)")
            }
        }
        return options
    }
}

func seconds(_ value: Double?) -> TimeAmount? {
    value.map { .nanoseconds(Int64($0 * 1e9)) }
}

@available(anyAppleOS 27, *)
func run() async throws {
    let options = Options.parse(CommandLine.arguments)
    guard let trustRoots = options.trustRoots else { fatalError("--trust-roots is required") }
    guard let mode = EchoLoad.Mode(rawValue: options.mode) else { fatalError("Unknown mode \(options.mode)") }
    let serverPID = options.serverPID

    let (elapsed, completed, failed) = try await EchoLoad.run(
        mode: mode,
        address: try SocketAddress(ipAddress: options.host, port: options.port),
        serverName: options.serverName,
        trustRoots: trustRoots,
        connections: options.connections,
        concurrency: options.concurrency,
        requestsPerConnection: options.requests / options.connections,
        warmUp: seconds(options.warmUp),
        duration: seconds(options.duration),
        bodySize: options.bodySize,
        path: options.path,
        group: MultiThreadedEventLoopGroup(numberOfThreads: options.threads),
        timedPhase: { _ in
            if let serverPID {
                kill(serverPID, SIGUSR1)
            }
        }
    )

    let seconds = Double(elapsed.nanoseconds) / 1e9
    print(
        "mode=\(options.mode) requests=\(completed) failed=\(failed) seconds=\(seconds) req/s=\(Int(Double(completed) / seconds))"
    )
}

if #available(anyAppleOS 27, *) {
    try await run()
} else {
    fatalError("Requires macOS 27 or later")
}
