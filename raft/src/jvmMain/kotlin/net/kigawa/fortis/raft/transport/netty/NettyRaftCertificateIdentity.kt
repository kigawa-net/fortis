package net.kigawa.fortis.raft.transport.netty

import java.net.URI
import java.security.cert.X509Certificate
import net.kigawa.fortis.raft.transport.RaftTransportException

internal object NettyRaftCertificateIdentity {
    fun uri(peerId: String): String {
        require(peerId.isNotBlank()) { "Raft peer ID must not be blank" }
        return URI(SCHEME, AUTHORITY, "/$peerId", null).toASCIIString()
    }

    fun requirePeer(certificate: X509Certificate, expectedPeerId: String) {
        val actualPeerId = peerId(certificate)
        if (actualPeerId != expectedPeerId) {
            throw RaftTransportException(
                "Raft TLS peer identity mismatch: expected $expectedPeerId, got $actualPeerId",
            )
        }
    }

    fun peerId(certificate: X509Certificate): String {
        val identities = certificate.subjectAlternativeNames.orEmpty()
            .asSequence()
            .filter { it.size >= 2 && it[0] == URI_SAN_TYPE }
            .mapNotNull { it[1] as? String }
            .mapNotNull(::parse)
            .toList()
        if (identities.size != 1) {
            throw RaftTransportException(
                "Raft TLS certificate must contain exactly one $SCHEME identity",
            )
        }
        return identities.single()
    }

    private fun parse(value: String): String? {
        val uri = runCatching { URI(value) }.getOrNull() ?: return null
        if (uri.scheme != SCHEME || uri.host != AUTHORITY) return null
        if (uri.port != -1 || uri.userInfo != null || uri.query != null || uri.fragment != null) {
            return null
        }
        val path = uri.path ?: return null
        return path.removePrefix("/").takeIf { path.startsWith('/') && it.isNotBlank() }
    }

    private const val URI_SAN_TYPE = 6
    private const val SCHEME = "fortis"
    private const val AUTHORITY = "raft"
}
