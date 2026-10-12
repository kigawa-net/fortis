package net.kigawa.fortis.raft.transport.netty

import java.security.PrivateKey
import java.security.cert.X509Certificate

data class NettyRaftTlsIdentity(
    val certificateChain: List<X509Certificate>,
    val privateKey: PrivateKey,
) {
    init {
        require(certificateChain.isNotEmpty()) {
            "Raft TLS certificate chain must not be empty"
        }
    }
}
