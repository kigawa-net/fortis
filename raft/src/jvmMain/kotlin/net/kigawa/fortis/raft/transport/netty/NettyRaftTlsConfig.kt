package net.kigawa.fortis.raft.transport.netty

import java.security.cert.X509Certificate

data class NettyRaftTlsConfig(
    val identity: NettyRaftTlsIdentity,
    val trustedCertificates: List<X509Certificate>,
) {
    init {
        require(trustedCertificates.isNotEmpty()) {
            "Raft TLS trust certificates must not be empty"
        }
    }
}
