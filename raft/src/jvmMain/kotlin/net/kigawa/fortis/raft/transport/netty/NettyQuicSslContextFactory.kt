package net.kigawa.fortis.raft.transport.netty

import io.netty.handler.codec.quic.QuicSslContext
import io.netty.handler.codec.quic.QuicSslContextBuilder
import io.netty.handler.ssl.ClientAuth

object NettyQuicSslContextFactory {
    fun client(config: NettyRaftTlsConfig): QuicSslContext =
        QuicSslContextBuilder.forClient()
            .keyManager(
                config.identity.privateKey,
                null,
                *config.identity.certificateChain.toTypedArray(),
            )
            .trustManager(*config.trustedCertificates.toTypedArray())
            .endpointIdentificationAlgorithm(null)
            .applicationProtocols(NettyQuicRaftRpcChannel.ALPN)
            .build()

    fun server(config: NettyRaftTlsConfig): QuicSslContext =
        QuicSslContextBuilder.forServer(
            config.identity.privateKey,
            null,
            *config.identity.certificateChain.toTypedArray(),
        )
            .trustManager(*config.trustedCertificates.toTypedArray())
            .clientAuth(ClientAuth.REQUIRE)
            .applicationProtocols(NettyQuicRaftRpcChannel.ALPN)
            .build()
}
