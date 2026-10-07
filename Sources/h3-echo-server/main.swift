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

import Dispatch
import H3EchoServer
import Logging
import NIOCore
import NIOPosix
import NIOQUIC

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

// An HTTP/3 echo server, for benchmarking the APIs it can be built on against each other:
//
//   h3-echo-server --api lightweight|channel|quic --certificate-chain-path <pem> --private-key-path <pem>
//                  [--host 127.0.0.1] [--port 8443] [--threads 1] [--log-level error]
//                  [--workload echo|responsiveness] [--download-size <bytes>]
//
// The responsiveness workload serves `/responsiveness/download/<size>` like the performance workloads do. The `quic`
// API doesn't parse requests: it answers every one with `--download-size` bytes.
//
// It prints the CPU time it has used, in microseconds, whenever it's sent SIGUSR1:
//
//   cpu user=<µs> system=<µs>

struct Options {
    var api = EchoServerAPI.lightweight
    var host = "127.0.0.1"
    var port = 8443
    var threads = 1
    var certificateChainPath: String?
    var privateKeyPath: String?
    var logLevel = Logger.Level.error
    var alpn: String?
    var vectorReadCount = 64
    var workload = EchoServerWorkload.echo
    var httpServerTransport = false

    static func parse(_ arguments: [String]) -> Options {
        var options = Options()
        var iterator = arguments.dropFirst().makeIterator()

        func value(for flag: String) -> String {
            guard let value = iterator.next() else {
                fatalError("Missing a value for \(flag)")
            }
            return value
        }

        while let argument = iterator.next() {
            switch argument {
            case "--api":
                let raw = value(for: argument)
                guard let api = EchoServerAPI(rawValue: raw) else {
                    fatalError("Unknown API \(raw), expected one of \(EchoServerAPI.allCases.map(\.rawValue))")
                }
                options.api = api
            case "--host":
                options.host = value(for: argument)
            case "--port":
                options.port = Int(value(for: argument))!
            case "--threads":
                options.threads = Int(value(for: argument))!
            case "--certificate-chain-path":
                options.certificateChainPath = value(for: argument)
            case "--private-key-path":
                options.privateKeyPath = value(for: argument)
            case "--log-level":
                options.logLevel = Logger.Level(rawValue: value(for: argument))!
            case "--alpn":
                options.alpn = value(for: argument)
            case "--workload":
                switch value(for: argument) {
                case "echo":
                    options.workload = .echo
                case "responsiveness":
                    if case .echo = options.workload {
                        options.workload = .responsiveness(downloadSize: 0)
                    }
                case let other:
                    fatalError("Unknown workload \(other), expected echo or responsiveness")
                }
            case "--transport":
                switch value(for: argument) {
                case "workload": options.httpServerTransport = false
                case "http-server": options.httpServerTransport = true
                case let other: fatalError("Unknown transport \(other), expected workload or http-server")
                }
            case "--download-size":
                options.workload = .responsiveness(downloadSize: Int(value(for: argument))!)
            case "--vector-read-count":
                options.vectorReadCount = Int(value(for: argument))!
            default:
                fatalError("Unknown argument \(argument)")
            }
        }
        return options
    }
}

func printCPUTime() {
    var usage = rusage()
    #if canImport(Glibc)
    getrusage(__rusage_who_t(RUSAGE_SELF.rawValue), &usage)
    #else
    getrusage(RUSAGE_SELF, &usage)
    #endif
    let user = Int(usage.ru_utime.tv_sec) * 1_000_000 + Int(usage.ru_utime.tv_usec)
    let system = Int(usage.ru_stime.tv_sec) * 1_000_000 + Int(usage.ru_stime.tv_usec)
    print("cpu user=\(user) system=\(system)")
    fflush(nil)
}

@available(anyAppleOS 27, *)
func run() async throws {
    let options = Options.parse(CommandLine.arguments)
    guard let certificateChainPath = options.certificateChainPath, let privateKeyPath = options.privateKeyPath else {
        fatalError("--certificate-chain-path and --private-key-path are required")
    }

    var logger = Logger(label: "h3-echo-server")
    logger.logLevel = options.logLevel

    var quicConfiguration = QUICConfiguration.forServer(
        serverName: "",
        authenticationConfiguration: .x509Certificates(
            certificateChainFilePath: certificateChainPath,
            privateKeyFilePath: privateKeyPath
        ),
        applicationProtocols: [options.alpn ?? (options.api == .quic ? "echo" : "h3")]
    )
    if options.httpServerTransport {
        // swift-http-server's `NIOHTTPServerConfiguration.HTTP3.QUICConfiguration.defaults`.
        quicConfiguration.keyExchangeGroup = .x25519
        quicConfiguration.maxIdleTimeout = .seconds(30)
        quicConfiguration.initialMaxData = 1024 * 1024
        quicConfiguration.initialMaxStreamDataBidiLocal = 1024 * 1024
        quicConfiguration.initialMaxStreamDataBidiRemote = 1024 * 1024
        quicConfiguration.initialMaxStreamDataUni = 1024 * 1024
        quicConfiguration.initialMaxStreamsBidi = 100
        quicConfiguration.initialMaxStreamsUni = 100
    } else {
        // The same transport parameters as the http3-swift-quic performance workload.
        quicConfiguration.initialMaxData = 5_000_000
        quicConfiguration.initialMaxStreamDataBidiLocal = 131_072
        quicConfiguration.initialMaxStreamDataBidiRemote = 5_000_000
        quicConfiguration.initialMaxStreamDataUni = 131_072
        quicConfiguration.initialMaxStreamsBidi = 100
        quicConfiguration.initialMaxStreamsUni = 100
    }

    let group = MultiThreadedEventLoopGroup(numberOfThreads: options.threads)
    signal(SIGUSR1, SIG_IGN)
    let cpuTimeSignal = DispatchSource.makeSignalSource(signal: SIGUSR1)
    cpuTimeSignal.setEventHandler { printCPUTime() }
    cpuTimeSignal.resume()

    var channels: [any Channel] = []
    for eventLoop in group.makeIterator() {
        let channel = try await EchoServer.bind(
            api: options.api,
            host: options.host,
            port: options.port,
            quicConfiguration: quicConfiguration,
            group: eventLoop,
            vectorReadCount: options.vectorReadCount,
            workload: options.workload,
            logger: logger
        )
        channels.append(channel)
    }

    print("listening api=\(options.api.rawValue) address=\(channels[0].localAddress!)")
    fflush(nil)

    for channel in channels {
        try await channel.closeFuture.get()
    }
}

if #available(anyAppleOS 27, *) {
    try await run()
} else {
    fatalError("Requires macOS 27 or later")
}
