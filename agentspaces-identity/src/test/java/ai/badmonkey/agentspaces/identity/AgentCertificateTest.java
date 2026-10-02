/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * QA4 A4-7 phase 1, the contract, written before the types existed. A
 * subordinate agent identity is a key of its own, certified by its peer; the
 * certificate binds the agent's name and key to the peer and to a lifetime, and
 * verifies only under the certifying peer's key, only for that agent, and only
 * while it lives. Peer-signed agent identities carry no certificate and sign
 * with the peer key exactly as before.
 */
class AgentCertificateTest {

    private final PeerIdentity peer = PeerIdentity.generate();
    private final PeerIdentity other = PeerIdentity.generate();
    private final AgentCertificates certificates = new AgentCertificates();
    private final Instant now = Instant.parse("2026-09-24T12:00:00Z");

    @Test
    void aCertificateForAnotherPeerDoesNotVerify() {
        AgentIdentity worker = peer.subordinate("worker", now, Duration.ofHours(1));
        AgentCertificate cert = worker.certificate().orElseThrow();

        assertThat(certificates.verify(cert, other.rawPublicKey(), worker.id(), now))
                .as("another peer's key did not certify this agent").isFalse();
        assertThat(certificates.verify(cert, peer.rawPublicKey(), worker.id(), now))
                .as("the certifying peer's key does").isTrue();
    }

    @Test
    void aCertificateBindsExactlyOneAgentName() {
        AgentIdentity worker = peer.subordinate("worker", now, Duration.ofHours(1));
        AgentCertificate cert = worker.certificate().orElseThrow();

        assertThat(cert.agent()).isEqualTo(peer.agent("worker"));
        assertThat(certificates.verify(cert, peer.rawPublicKey(), peer.agent("auditor"), now))
                .as("a certificate for 'worker' says nothing about 'auditor'").isFalse();
    }

    @Test
    void anExpiredCertificateDoesNotVerify() {
        AgentIdentity worker = peer.subordinate("worker", now, Duration.ofMinutes(10));
        AgentCertificate cert = worker.certificate().orElseThrow();

        assertThat(cert.expired(now.plusSeconds(599))).isFalse();
        assertThat(cert.expired(now.plusSeconds(600))).isTrue();
        assertThat(certificates.verify(cert, peer.rawPublicKey(), worker.id(), now.plusSeconds(601)))
                .as("a lapsed certificate certifies nothing").isFalse();
    }

    @Test
    void aTamperedCertificateDoesNotVerify() {
        AgentIdentity worker = peer.subordinate("worker", now, Duration.ofHours(1));
        AgentCertificate cert = worker.certificate().orElseThrow();
        AgentCertificate forged = new AgentCertificate(cert.agent(),
                other.rawPublicKey(), cert.issued(), cert.ttl(), cert.peerSignature());

        assertThat(certificates.verify(forged, peer.rawPublicKey(), worker.id(), now))
                .as("swapping the agent key breaks the peer's signature").isFalse();
    }

    @Test
    void subordinateIdentitiesAreDistinctKeysAndSignWithThem() {
        AgentIdentity a = peer.subordinate("worker", now, Duration.ofHours(1));
        AgentIdentity b = peer.subordinate("worker", now, Duration.ofHours(1));
        byte[] message = "hello".getBytes();

        assertThat(a.publicKey()).isNotEqualTo(b.publicKey());
        assertThat(a.publicKey()).isNotEqualTo(peer.rawPublicKey());
        assertThat(a.id()).isEqualTo(peer.agent("worker"));
        assertThat(ai.badmonkey.agentspaces.common.crypto.Ed25519.verifyRaw(
                a.publicKey(), message, a.sign(message))).isTrue();
        assertThat(ai.badmonkey.agentspaces.common.crypto.Ed25519.verifyRaw(
                peer.rawPublicKey(), message, a.sign(message)))
                .as("an agent signature is not a peer signature").isFalse();
    }

    @Test
    void aPeerSignedAgentIdentityIsTodaysBehaviourWithNoCertificate() {
        AgentIdentity worker = peer.agentIdentity("worker");
        byte[] message = "hello".getBytes();

        assertThat(worker.id()).isEqualTo(peer.agent("worker"));
        assertThat(worker.certificate()).isEmpty();
        assertThat(worker.publicKey()).isEqualTo(peer.rawPublicKey());
        assertThat(peer.verify(message, worker.sign(message))).isTrue();
    }

    /** SPEC §4.2 v0.1.13: a certificate is judged at the signing time — the window is issued-inclusive, expiry-exclusive — and a signing time may lead the receiver's clock only by the HLC drift ceiling. */
    @Test
    void verifyAtJudgesTheSigningTimeWithinTheWindowAndTheDriftCeiling() {
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                peer.subordinate("auditor", now, java.time.Duration.ofHours(1));
        ai.badmonkey.agentspaces.api.security.AgentCertificate cert = agent.certificate().orElseThrow();
        byte[] key = peer.rawPublicKey();
        Instant later = now.plus(java.time.Duration.ofDays(30)); // long after expiry
        assertThat(certificates.verifyAt(cert, key, agent.id(), now, later))
                .as("signed at issue, verified a month later").isTrue();
        assertThat(certificates.verifyAt(cert, key, agent.id(), now.plusSeconds(3599), later)).isTrue();
        assertThat(certificates.verifyAt(cert, key, agent.id(), now.plusSeconds(3600), later))
                .as("expiry is exclusive").isFalse();
        assertThat(certificates.verifyAt(cert, key, agent.id(), now.minusMillis(1), later))
                .as("before issue").isFalse();
        long drift = ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock.MAX_DRIFT_MILLIS;
        Instant receiver = now.plusSeconds(10);
        assertThat(certificates.verifyAt(cert, key, agent.id(), now.plusSeconds(20), receiver))
                .as("a signing time a little ahead of the receiver is honest skew").isTrue();
        ai.badmonkey.agentspaces.api.spi.AgentIdentity future = peer.subordinate("auditor",
                receiver.plusMillis(drift + 1), java.time.Duration.ofHours(1));
        assertThat(certificates.verifyAt(future.certificate().orElseThrow(), key, agent.id(),
                receiver.plusMillis(drift + 1), receiver))
                .as("beyond the drift ceiling").isFalse();
        assertThat(certificates.verifyAt(cert, other.rawPublicKey(), agent.id(), now, later))
                .as("the other checks still apply").isFalse();
    }

    /** SPEC §4.2 v0.1.13 (TODO-9-10-11 B3): a renewing identity keeps its key, re-issues at half-life on its clock, retains earlier certificates for signatures made under them, and caps what it retains. */
    @Test
    void aRenewingIdentityReissuesAtHalfLifeAndKeepsEarlierCertificates() {
        ai.badmonkey.agentspaces.test.TestClock clock = ai.badmonkey.agentspaces.test.TestClock.create();
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                peer.renewingSubordinate("worker", java.time.Duration.ofHours(1), clock);
        Instant first = clock.instant();
        ai.badmonkey.agentspaces.api.security.AgentCertificate original = agent.certificate().orElseThrow();
        byte[] key = agent.publicKey();

        clock.advance(java.time.Duration.ofMinutes(29));
        assertThat(agent.certificate()).contains(original);
        clock.advance(java.time.Duration.ofMinutes(1));
        ai.badmonkey.agentspaces.api.security.AgentCertificate renewed = agent.certificate().orElseThrow();
        assertThat(renewed).isNotEqualTo(original);
        assertThat(renewed.issued()).isEqualTo(clock.instant());
        assertThat(renewed.agentPublicKey()).as("the key is fixed").isEqualTo(key);
        assertThat(certificates.verify(renewed, peer.rawPublicKey(), agent.id(), clock.instant())).isTrue();

        clock.advance(java.time.Duration.ofHours(3));
        assertThat(agent.certificateCovering(first.plusSeconds(60)))
                .as("a signature stamped under the first certificate is still certifiable").contains(original);
        assertThat(agent.certificateCovering(clock.instant()))
                .as("a signature now is covered by a fresh certificate").isPresent();
        assertThat(agent.certificateCovering(first.minusSeconds(1))).isEmpty();

        RenewingAgentIdentity renewing = (RenewingAgentIdentity) agent;
        for (int i = 0; i < RenewingAgentIdentity.RETAINED + 20; i++) {
            clock.advance(java.time.Duration.ofMinutes(31));
            agent.certificate();
        }
        assertThat(renewing.heldCertificates()).isEqualTo(RenewingAgentIdentity.RETAINED);
    }

    /**
     * Review M-1 (audit 2026-10-02): past the cap, the least recently used
     * certificate goes, so the certificate covering a take that is renewed for
     * longer than 256 lifetimes stays certifiable while unused ones age out.
     */
    @Test
    void aCertificateStillInUseOutlivesTheRetentionCap() {
        ai.badmonkey.agentspaces.test.TestClock clock = ai.badmonkey.agentspaces.test.TestClock.create();
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                peer.renewingSubordinate("worker", java.time.Duration.ofHours(1), clock);
        Instant claimStamp = clock.instant().plusSeconds(1);
        ai.badmonkey.agentspaces.api.security.AgentCertificate original = agent.certificate().orElseThrow();
        for (int i = 0; i < RenewingAgentIdentity.RETAINED * 2; i++) {
            clock.advance(java.time.Duration.ofMinutes(31));
            // Each renewal of the long-held take asks for the certificate covering its stamp.
            assertThat(agent.certificateCovering(claimStamp)).as("renewal " + i).contains(original);
        }
        assertThat(((RenewingAgentIdentity) agent).heldCertificates()).isEqualTo(RenewingAgentIdentity.RETAINED);
    }

    /** Regression (verify-all, example-05): a certificate issued at sub-millisecond precision covers a signature stamped in the same millisecond, since signing times are whole-millisecond HLC stamps. */
    @Test
    void aCertificateCoversItsOwnIssueMillisecond() {
        Instant precise = Instant.parse("2026-10-02T01:59:39.245731Z");
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                peer.subordinate("auditor", precise, java.time.Duration.ofHours(1));
        Instant stamped = Instant.ofEpochMilli(precise.toEpochMilli()); // 01:59:39.245Z
        assertThat(agent.certificate().orElseThrow().covers(stamped)).isTrue();
        assertThat(agent.certificate().orElseThrow().covers(stamped.minusMillis(1))).isFalse();
    }
}
