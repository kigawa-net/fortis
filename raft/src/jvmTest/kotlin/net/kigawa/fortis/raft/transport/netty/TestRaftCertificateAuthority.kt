package net.kigawa.fortis.raft.transport.netty

import java.math.BigInteger
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.cert.X509Certificate
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

internal class TestRaftCertificateAuthority private constructor(
    val certificate: X509Certificate,
    private val keyPair: KeyPair,
) {
    fun identity(peerId: String): NettyRaftTlsIdentity {
        val nodeKeyPair = keyPair()
        val certificateBuilder = JcaX509v3CertificateBuilder(
            X500Name(certificate.subjectX500Principal.name),
            serialNumber(),
            NOT_BEFORE,
            NOT_AFTER,
            X500Name("CN=$peerId"),
            nodeKeyPair.public,
        )
            .addExtension(Extension.basicConstraints, true, BasicConstraints(false))
            .addExtension(
                Extension.keyUsage,
                true,
                KeyUsage(KeyUsage.digitalSignature),
            )
            .addExtension(
                Extension.extendedKeyUsage,
                false,
                ExtendedKeyUsage(
                    arrayOf(
                        KeyPurposeId.id_kp_serverAuth,
                        KeyPurposeId.id_kp_clientAuth,
                    ),
                ),
            )
            .addExtension(
                Extension.subjectAlternativeName,
                false,
                GeneralNames(
                    GeneralName(
                        GeneralName.uniformResourceIdentifier,
                        NettyRaftCertificateIdentity.uri(peerId),
                    ),
                ),
            )
        val nodeCertificate = certificateBuilder.build(signer(keyPair))
            .let(JcaX509CertificateConverter()::getCertificate)
            .also { it.verify(certificate.publicKey) }
        return NettyRaftTlsIdentity(
            certificateChain = listOf(nodeCertificate, certificate),
            privateKey = nodeKeyPair.private,
        )
    }

    companion object {
        private val serial = AtomicLong(1)
        private val NOT_BEFORE = Date.from(Instant.now().minus(1, ChronoUnit.DAYS))
        private val NOT_AFTER = Date.from(Instant.now().plus(1, ChronoUnit.DAYS))

        fun create(): TestRaftCertificateAuthority {
            val keyPair = keyPair()
            val name = X500Name("CN=Fortis Test Raft CA")
            val certificate = JcaX509v3CertificateBuilder(
                name,
                serialNumber(),
                NOT_BEFORE,
                NOT_AFTER,
                name,
                keyPair.public,
            )
                .addExtension(Extension.basicConstraints, true, BasicConstraints(true))
                .addExtension(
                    Extension.keyUsage,
                    true,
                    KeyUsage(KeyUsage.keyCertSign or KeyUsage.cRLSign),
                )
                .build(signer(keyPair))
                .let(JcaX509CertificateConverter()::getCertificate)
                .also { it.verify(keyPair.public) }
            return TestRaftCertificateAuthority(certificate, keyPair)
        }

        private fun keyPair(): KeyPair = KeyPairGenerator.getInstance("RSA").run {
            initialize(2048)
            generateKeyPair()
        }

        private fun serialNumber(): BigInteger =
            BigInteger.valueOf(serial.getAndIncrement())

        private fun signer(keyPair: KeyPair) =
            JcaContentSignerBuilder("SHA256withRSA").build(keyPair.private)
    }
}
