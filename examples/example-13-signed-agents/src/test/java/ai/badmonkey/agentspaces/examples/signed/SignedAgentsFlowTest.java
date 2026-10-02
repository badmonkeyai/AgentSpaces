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
package ai.badmonkey.agentspaces.examples.signed;

import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Space.Attestation;
import ai.badmonkey.agentspaces.examples.signed.SignedAgents.Finding;
import ai.badmonkey.agentspaces.examples.signed.SignedAgents.Peer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example 13 end to end over TCP: findings written under subordinate identities
 * are attributed to their agents as {@code AGENT_ATTESTED} at every replica, a
 * peer-signed finding stays {@code PEER_ASSERTED}, and a lapsed certificate
 * certifies nothing.
 */
class SignedAgentsFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(90)
    void eachFindingIsAttributedToItsAgentWithItsAttestationEverywhere() throws Exception {
        int seedPort = freePort();
        Peer auditor = SignedAgents.startAgent("auditor", seedPort, 0);
        Peer clerk = SignedAgents.startAgent("clerk", freePort(), seedPort);
        Peer desk = SignedAgents.startPlainAgent("desk", freePort(), seedPort);
        List<Peer> fleet = List.of(auditor, clerk, desk);
        try {
            // The peer certified its agent's key, for that agent alone.
            AgentCertificate certificate = auditor.certificate().orElseThrow();
            assertThat(certificate.agent()).isEqualTo(auditor.identity().agent("auditor"));
            assertThat(certificate.agentPublicKey())
                    .isNotEqualTo(auditor.identity().rawPublicKey());
            assertThat(desk.certificate()).as("a peer-signed agent has nothing to certify").isEmpty();

            SignedAgents.conclude(auditor, "release-7.4.0", "no hardcoded credentials");
            SignedAgents.conclude(clerk, "invoice-118", "paid in full");
            SignedAgents.conclude(desk, "office", "closes at six");

            for (Peer reader : fleet) {
                Map<String, Space.Issued<Finding>> bySubject = await(() -> {
                    List<Space.Issued<Finding>> all = SignedAgents.ledger(reader);
                    return all.size() >= 3 ? Optional.of(all) : Optional.<List<Space.Issued<Finding>>>empty();
                }, Duration.ofSeconds(30)).orElseThrow().stream()
                        .collect(Collectors.toMap(i -> i.entry().subject(), Function.identity()));

                // Each agent's own key signed its finding: attributed by name, provably.
                assertThat(bySubject.get("release-7.4.0").issuer())
                        .isEqualTo(auditor.identity().agent("auditor"));
                assertThat(bySubject.get("release-7.4.0").attestation())
                        .as("at " + reader.agentName()).isEqualTo(Attestation.AGENT_ATTESTED);
                assertThat(bySubject.get("invoice-118").issuer())
                        .isEqualTo(clerk.identity().agent("clerk"));
                assertThat(bySubject.get("invoice-118").attestation())
                        .isEqualTo(Attestation.AGENT_ATTESTED);
                // The desk wrote as every space always has: on the peer's word.
                assertThat(bySubject.get("office").issuer()).isEqualTo(desk.identity().agent("desk"));
                assertThat(bySubject.get("office").attestation()).isEqualTo(Attestation.PEER_ASSERTED);
            }
        } finally {
            fleet.forEach(Peer::close);
        }
    }

    /**
     * The second half (QA4 A4-7 phase 3): two annotated POJOs on one peer, bound
     * through the facade with a subordinate identity factory, are two provable
     * identities. Each bean's findings are attributed to it, attested, at every
     * replica; their cards carry their keys; and the agent classes hold no
     * identity code at all.
     */
    @Test
    @Timeout(90)
    void twoAnnotatedAgentsOnOnePeerAreTwoProvableIdentities() throws Exception {
        int seedPort = freePort();
        Peer desk = SignedAgents.startPlainAgent("desk", seedPort, 0);
        SignedAgents.AnnotatedPeer both = SignedAgents.startAnnotatedPeer(freePort(), seedPort);
        try {
            assertThat(both.auditorBound().identity().isSubordinate()).isTrue();
            assertThat(both.auditorBound().card().attested()).isTrue();
            assertThat(both.clerkBound().card().attested()).isTrue();
            assertThat(both.auditorBound().card().agentPublicKey())
                    .isNotEqualTo(both.clerkBound().card().agentPublicKey());
            assertThat(both.auditor().findings.writer()).contains(both.identity().agent("auditor"));
            assertThat(both.clerk().findings.writer()).contains(both.identity().agent("clerk"));

            both.auditor().conclude("release-7.5.0", "signed off");
            both.clerk().conclude("invoice-119", "disputed");

            for (java.util.function.Supplier<List<Space.Issued<Finding>>> reader : List.<java.util.function.Supplier<List<Space.Issued<Finding>>>>of(
                    () -> SignedAgents.ledger(desk), () -> both.findings().readAllIssued(
                            ai.badmonkey.agentspaces.api.space.Template.of(Finding.class), 100))) {
                Map<String, Space.Issued<Finding>> bySubject = await(() -> {
                    List<Space.Issued<Finding>> all = reader.get();
                    return all.size() >= 2 ? Optional.of(all) : Optional.<List<Space.Issued<Finding>>>empty();
                }, Duration.ofSeconds(30)).orElseThrow().stream()
                        .collect(Collectors.toMap(i -> i.entry().subject(), Function.identity()));
                assertThat(bySubject.get("release-7.5.0").issuer()).isEqualTo(both.identity().agent("auditor"));
                assertThat(bySubject.get("release-7.5.0").attestation()).isEqualTo(Attestation.AGENT_ATTESTED);
                assertThat(bySubject.get("invoice-119").issuer()).isEqualTo(both.identity().agent("clerk"));
                assertThat(bySubject.get("invoice-119").attestation()).isEqualTo(Attestation.AGENT_ATTESTED);
            }
        } finally {
            both.close();
            desk.close();
        }
    }

    @Test
    @Timeout(90)
    void aLapsedCertificateCertifiesNothingSoItsFindingsAreRefusedElsewhere() throws Exception {
        int seedPort = freePort();
        Peer desk = SignedAgents.startPlainAgent("desk", seedPort, 0);
        Peer stale = SignedAgents.startAgent("stale", freePort(), seedPort,
                Instant.now().minus(Duration.ofHours(2)), Duration.ofHours(1));
        Peer fresh = SignedAgents.startAgent("fresh", freePort(), seedPort);
        List<Peer> fleet = List.of(desk, stale, fresh);
        try {
            assertThat(stale.certificate().orElseThrow().expired(Instant.now())).isTrue();
            // SPEC §4.2 v0.1.13: a certificate is judged at the signing time, so a
            // writer whose certificate does not cover "now" refuses to sign at all.
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                    SignedAgents.conclude(stale, "ledger", "under a certificate that lapsed an hour ago"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("holds no certificate covering");
            SignedAgents.conclude(fresh, "ledger-2", "under a live certificate");

            // The live one arrives; the lapsed agent never wrote anything to arrive.
            List<Space.Issued<Finding>> atDesk = await(() -> {
                List<Space.Issued<Finding>> all = SignedAgents.ledger(desk);
                return all.isEmpty() ? Optional.<List<Space.Issued<Finding>>>empty() : Optional.of(all);
            }, Duration.ofSeconds(30)).orElseThrow();
            Thread.sleep(2000); // a few more gossip rounds and anti-entropy exchanges
            atDesk = SignedAgents.ledger(desk);
            assertThat(atDesk).extracting(i -> i.entry().subject()).containsExactly("ledger-2");
            assertThat(SignedAgents.ledger(stale)).extracting(i -> i.entry().subject())
                    .as("the lapsed writer wrote nothing, not even locally").doesNotContain("ledger");
        } finally {
            fleet.forEach(Peer::close);
        }
    }

    private static <T> Optional<T> await(Supplier<Optional<T>> probe, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<T> value = probe.get();
            if (value.isPresent()) {
                return value;
            }
            Thread.sleep(200);
        }
        return probe.get();
    }
}
