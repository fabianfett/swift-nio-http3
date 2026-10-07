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

public import HTTP3
public import Logging
public import NIOCore
import NIOHTTP3
public import NIOPosix
public import NIOQUIC
import NIOQUICHelpers

/// The API an echo server is built on.
public enum EchoServerAPI: String, CaseIterable, Sendable {
    /// ``LightweightEchoServer``: `NIOHTTP3`'s `HTTP3ServerConsumer`, on the QUIC stream consumer API.
    case lightweight
    /// ``ChannelEchoHandler``: `NIOHTTP3`, with a `Channel` per stream.
    case channel
    /// ``QUICEchoConsumer``: QUIC only, no HTTP/3.
    case quic
}

/// Starts echo servers.
@available(anyAppleOS 27, *)
public enum EchoServer {
    /// Binds an echo server.
    ///
    /// - Parameters:
    ///   - api: What to build the server on.
    ///   - host: The address to bind to.
    ///   - port: The port to bind to, `0` for any.
    ///   - quicConfiguration: The QUIC configuration. Its application protocols must match the API: `h3` for the
    ///     HTTP/3 ones.
    ///   - settings: The HTTP/3 settings the HTTP/3 servers send.
    ///   - group: The event loops to use.
    ///   - vectorReadCount: How many datagrams to read per system call, on Linux.
    ///   - logger: The logger.
    /// - Returns: The server's UDP channel.
    /// - Throws: If the credentials can't be loaded, or the address can't be bound.
    public static func bind(
        api: EchoServerAPI,
        host: String,
        port: Int,
        quicConfiguration: QUICConfiguration,
        settings: HTTP3Settings = HTTP3Settings(),
        group: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        vectorReadCount: Int = 1,
        logger: Logger
    ) async throws -> any Channel {
        let authenticator: Authenticator? =
            switch quicConfiguration.authenticationConfiguration {
            case .x509Certificates(let certificateChainFilePath, let privateKeyFilePath):
                try Authenticator(certificateFilePath: certificateChainFilePath, privateKeyFilePath: privateKeyFilePath)
            case .rawPublicKeys, .none:
                nil
            }

        var bootstrap = DatagramBootstrap(group: group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.socketOption(.so_reuseport), value: 1)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    let handler: any ChannelHandler
                    switch api {
                    case .lightweight:
                        handler = QUICHandler<LightweightEchoServer>(
                            channel: channel,
                            quicConfiguration: quicConfiguration,
                            asyncVerifier: nil,
                            authenticator: authenticator,
                            logger: logger
                        ) { connection in
                            LightweightEchoServer(
                                http3Connection: .server(connection, settings: settings, logger: logger)
                            )
                        }

                    case .channel:
                        handler = Self.makeChannelServerHandler(
                            channel: channel,
                            quicConfiguration: quicConfiguration,
                            authenticator: authenticator,
                            settings: settings,
                            logger: logger
                        )

                    case .quic:
                        handler = QUICHandler<QUICEchoConsumer>(
                            channel: channel,
                            quicConfiguration: quicConfiguration,
                            asyncVerifier: nil,
                            authenticator: authenticator,
                            logger: logger
                        ) { _ in
                            QUICEchoConsumer()
                        }
                    }
                    try channel.pipeline.syncOperations.addHandler(handler)
                }
            }

        #if os(Linux)
        if vectorReadCount > 1 {
            bootstrap =
                bootstrap
                .channelOption(.datagramVectorReadMessageCount, value: vectorReadCount)
                .channelOption(
                    .recvAllocator,
                    value: FixedSizeRecvByteBufferAllocator(capacity: 1500 * vectorReadCount)
                )
        }
        #else
        _ = vectorReadCount
        #endif

        return try await bootstrap.bind(host: host, port: port).get()
    }

    private static func makeChannelServerHandler(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        authenticator: Authenticator?,
        settings: HTTP3Settings,
        logger: Logger
    ) -> QUICHandler<QUICStreamChannels> {
        QUICHandler(
            channel: channel,
            quicConfiguration: quicConfiguration,
            asyncVerifier: nil,
            authenticator: authenticator,
            logger: logger,
            inboundConnectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeCompletedFuture {
                    let h3Handler = HTTP3ConnectionHandler.server(
                        eventLoop: connectionChannel.eventLoop,
                        configuration: .defaults,
                        settings: settings,
                        streamCreator: streamCreator,
                        logger: logger,
                        inboundRequestStreamInitializer: { parameters in
                            parameters.channel.eventLoop.makeCompletedFuture {
                                try parameters.channel.pipeline.syncOperations.addHandler(ChannelEchoHandler())
                            }
                        }
                    )
                    try connectionChannel.pipeline.syncOperations.addHandler(h3Handler)
                }
            },
            inboundStreamInitializer: { streamChannel in
                do {
                    let h3Handler = try streamChannel.parent!.pipeline.syncOperations.handler(
                        type: HTTP3ConnectionHandler<NIOQUIC.QUICStreamCreator>.self
                    )
                    return h3Handler.inboundStreamReceived(streamChannel)
                } catch {
                    return streamChannel.eventLoop.makeFailedFuture(error)
                }
            },
            noMoreConnections: {}
        )
    }
}
