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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.ProvidesCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.capability.AggregateClient;
import ai.badmonkey.agentspaces.agent.capability.VoteClient;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor;
import ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.MembershipAuthorizer;
import ai.badmonkey.agentspaces.peering.node.GroupFounding;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.node.SignedGroupAdvertisement;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.space.SpaceAdmissionException;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.SpaceAdmission;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The autoconfiguration exercised as plain objects, the way Spring will call
 * it: bean methods build the fabric from properties, the post-processor
 * enrolls annotated beans, the lifecycle starts and stops the node. The
 * integration test against a real Spring context runs in a Maven-Central
 * environment; these tests cover every line of our own wiring.
 */
class AgentSpacesAutoConfigurationTest {

    private final AgentSpacesAutoConfiguration autoConfig =
            new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private AgentSpacesProperties fleetProperties(int port, int seedPort) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(200);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(100);
        AgentSpacesProperties.SpaceDef findings = new AgentSpacesProperties.SpaceDef();
        findings.setName("findings");
        findings.setSettleWindowMillis(100);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-fleet-test-v1");
        if (seedPort > 0) {
            group.setSeeds(List.of("127.0.0.1:" + seedPort));
        }
        group.setSpaces(List.of(tasks, findings));
        properties.setGroups(List.of(group));
        return properties;
    }

    private record Wired(AgentSpacesProperties properties, PeerIdentity identity,
                         PeerNode node, AgentSpaces spaces,
                         AgentSpacesBeanPostProcessor postProcessor,
                         AgentSpacesLifecycle lifecycle, Authorizers authorizers) {
    }

    /** Builds the beans exactly as Spring would, in dependency order. */
    private Wired wire(AgentSpacesProperties properties) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder());
        AgentSpacesBeanPostProcessor postProcessor =
                autoConfig.agentSpacesBeanPostProcessor(properties, spaces);
        AgentSpacesLifecycle lifecycle =
                autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
        closeables.add(lifecycle::stop);
        return new Wired(properties, identity, node, spaces, postProcessor, lifecycle,
                authorizers);
    }

    /**
     * agentspaces-springai F5: semantic discovery ranks with the application's
     * Embedder bean; without one, the model-free hashing embedder. The
     * embedder's identity is what the capability advertises.
     */
    @Test
    void semanticDiscoveryRanksWithTheEmbedderBean() throws Exception {
        assertThat(autoConfig.agentSpacesEmbedder())
                .isInstanceOf(ai.badmonkey.agentspaces.capabilities.semantic.HashingEmbedder.class);
        ai.badmonkey.agentspaces.api.spi.Embedder custom = new ai.badmonkey.agentspaces.api.spi.Embedder() {
            @Override
            public double[] embed(String text) {
                return new double[] {1.0};
            }

            @Override
            public int dimensions() {
                return 1;
            }

            @Override
            public String identity() {
                return "custom:unit";
            }
        };
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        closeables.add(node::close);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity,
                autoConfig.agentSpacesAuthorizers(properties, identity), custom);
        ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery semantic =
                (ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery) spaces.group("fleet")
                        .provider(ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery.TYPE)
                        .orElseThrow();
        assertThat(semantic.embedder()).isSameAs(custom);
        assertThat(semantic.describe(spaces.group("fleet").id()).parameters())
                .containsEntry("embedder", "custom:unit").containsEntry("dimensions", "1");
    }

    // ------------------------------------------------------------ TODO-EFG §4: the Authorizer per profile

    private static final String ISSUER = "https://idp.example.com";
    private static final String AUDIENCE = "agentspaces-fleet";
    private static com.nimbusds.jose.jwk.RSAKey idpKey;
    private static com.sun.net.httpserver.HttpServer jwks;

    /** A tiny in-test identity provider: one RSA key served as a JWKS document. */
    @org.junit.jupiter.api.BeforeAll
    static void identityProvider() throws Exception {
        idpKey = new com.nimbusds.jose.jwk.gen.RSAKeyGenerator(2048).keyID("idp-key").generate();
        byte[] document = new com.nimbusds.jose.jwk.JWKSet(idpKey.toPublicJWK())
                .toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        jwks = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        jwks.createContext("/jwks", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, document.length);
            try (java.io.OutputStream out = exchange.getResponseBody()) {
                out.write(document);
            }
        });
        jwks.start();
    }

    @org.junit.jupiter.api.AfterAll
    static void stopIdentityProvider() {
        jwks.stop(0);
    }

    private static String jwksUrl() {
        return "http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks";
    }

    /** Mints a token the way OidcAuthorizerTest does: bound to one peer, carrying scopes. */
    private static String token(PeerId peer, String scope) throws Exception {
        com.nimbusds.jwt.SignedJWT jwt = new com.nimbusds.jwt.SignedJWT(
                new com.nimbusds.jose.JWSHeader.Builder(com.nimbusds.jose.JWSAlgorithm.RS256)
                        .keyID(idpKey.getKeyID()).build(),
                new com.nimbusds.jwt.JWTClaimsSet.Builder()
                        .issuer(ISSUER).audience(AUDIENCE).subject("agent-" + peer.value())
                        .claim("agentspaces_peer", peer.value())
                        .claim("scope", scope)
                        .expirationTime(java.util.Date.from(Instant.now().plusSeconds(3600)))
                        .build());
        jwt.sign(new com.nimbusds.jose.crypto.RSASSASigner(idpKey));
        return jwt.serialize();
    }

    private static void oidcProfile(AgentSpacesProperties properties) {
        properties.getSecurity().setProfile("mtls-oidc");
        properties.getSecurity().getOidc().setIssuer(ISSUER);
        properties.getSecurity().getOidc().setAudience(AUDIENCE);
        properties.getSecurity().getOidc().setJwksUrl(jwksUrl());
    }

    @ai.badmonkey.agentspaces.agent.annotation.AgentSpec(name = "keyed", description = "Has a key")
    public static class KeyedAgent {
        @ai.badmonkey.agentspaces.agent.annotation.SpaceRef("tasks")
        ai.badmonkey.agentspaces.api.space.Space tasks;
    }

    /** SPEC §4.2 v0.1.13 (TODO-9-10-11 B5, R9): with an agent keystore, a subordinate agent keeps its key across a restart of the peer (same peer keystore), so its card carries the same attested key; a keystore under peer-signed agents fails fast. */
    @Test
    void anAgentKeystorePersistsAgentKeysAcrossRestarts(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        AgentSpacesProperties first = fleetProperties(freePort(), 0);
        first.setKeystore(dir.resolve("peer").toString());
        first.getIdentity().setAgentKeys("subordinate");
        first.getIdentity().setAgentKeystore(dir.resolve("agents").toString());
        Wired before = wire(first);
        var boundBefore = before.spaces().group("fleet").bind(new KeyedAgent());
        byte[] keyBefore = boundBefore.identity().publicKey();
        byte[] encryptionBefore = boundBefore.identity().encryptionPublicKey().orElseThrow();
        before.lifecycle().stop();
        before.node().close();

        AgentSpacesProperties second = fleetProperties(freePort(), 0);
        second.setKeystore(dir.resolve("peer").toString());
        second.getIdentity().setAgentKeys("subordinate");
        second.getIdentity().setAgentKeystore(dir.resolve("agents").toString());
        Wired after = wire(second);
        assertThat(after.identity().peerId()).isEqualTo(before.identity().peerId());
        var bound = after.spaces().group("fleet").bind(new KeyedAgent());
        assertThat(bound.identity().publicKey()).as("the agent's key survived the restart")
                .isEqualTo(keyBefore);
        assertThat(bound.card().agentPublicKey()).isEqualTo(keyBefore);
        assertThat(bound.identity().encryptionPublicKey()).as("so did its certified X25519 key (D5)")
                .hasValueSatisfying(key -> assertThat(key).isEqualTo(encryptionBefore));

        AgentSpacesProperties misconfigured = fleetProperties(freePort(), 0);
        misconfigured.getIdentity().setAgentKeystore(dir.resolve("agents").toString());
        assertThatThrownBy(() -> wire(misconfigured)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agent-keystore");
    }

    /**
     * QA4 A4-7 phase 3: {@code agentspaces.identity.agent-keys=subordinate} wires the
     * facade's identity factory, so a bound bean gets a certified key, a per-agent
     * view, and a keyed card; the default leaves everything as it was.
     */
    @Test
    void agentKeysPropertySelectsTheIdentityFactory() throws Exception {
        Wired plain = wire(fleetProperties(freePort(), 0));
        KeyedAgent asBefore = new KeyedAgent();
        var boundBefore = plain.spaces().group("fleet").bind(asBefore);
        assertThat(boundBefore.identity().isSubordinate()).isFalse();
        assertThat(boundBefore.card().agentPublicKey()).isNull();
        assertThat(asBefore.tasks.writer()).contains(plain.identity().agent("app"));

        AgentSpacesProperties keyed = fleetProperties(freePort(), 0);
        keyed.getIdentity().setAgentKeys("subordinate");
        Wired app = wire(keyed);
        KeyedAgent bean = new KeyedAgent();
        var bound = app.spaces().group("fleet").bind(bean);
        assertThat(bound.identity().isSubordinate()).isTrue();
        assertThat(bound.card().agentPublicKey()).isEqualTo(bound.identity().publicKey());
        assertThat(bean.tasks.writer()).contains(app.identity().agent("keyed"));
        bean.tasks.write(new TaskEntry("attested", 1), Lease.of(Duration.ofMinutes(5)));
        assertThat(bean.tasks.readAllIssued(Template.of(TaskEntry.class), 10))
                .singleElement().satisfies(issued -> {
                    assertThat(issued.issuer()).isEqualTo(app.identity().agent("keyed"));
                    assertThat(issued.attestation())
                            .isEqualTo(ai.badmonkey.agentspaces.api.space.Space.Attestation.AGENT_ATTESTED);
                });

        // SPEC §4.2 v0.1.13: the starter's subordinate agents renew their
        // certificates, whose lifetime agent-certificate-ttl sets (24h default).
        ai.badmonkey.agentspaces.api.security.AgentCertificate issued =
                bound.identity().certificate().orElseThrow();
        assertThat(issued.ttl()).isEqualTo(Duration.ofHours(24));
        assertThat(bound.identity().certificateCovering(issued.issued().plus(Duration.ofHours(30))))
                .as("a renewing identity: a fresh certificate covers a time past the first's expiry")
                .isEmpty(); // not yet: renewal follows the clock, which has not moved
        AgentSpacesProperties shortLived = fleetProperties(freePort(), 0);
        shortLived.getIdentity().setAgentKeys("subordinate");
        shortLived.getIdentity().setAgentCertificateTtl(Duration.ofMinutes(5));
        var shortBound = wire(shortLived).spaces().group("fleet").bind(new KeyedAgent());
        assertThat(shortBound.identity().certificate().orElseThrow().ttl())
                .isEqualTo(Duration.ofMinutes(5));

        AgentSpacesProperties bad = fleetProperties(freePort(), 0);
        bad.getIdentity().setAgentKeys("hardware");
        assertThatThrownBy(() -> wire(bad)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agent-keys");
    }

    /**
     * QA4 A4-7 phase 2: an AgentId in {@code agentspaces.security.grants.vote}
     * switches the fleet's vote to AGENT granularity through the starter, and
     * without one it stays PEER, under which one peer is one counted ballot
     * however many names it writes.
     */
    @Test
    void anAgentIdVoteGrantSwitchesTheFleetToAgentGranularity() throws Exception {
        Wired plain = wire(fleetProperties(freePort(), 0));
        Authorizer perPeer = plain.authorizers().forGroup("fleet");
        assertThat(perPeer.granularity(Authorizer.Operation.VOTE, "votes"))
                .isEqualTo(Authorizer.Granularity.PEER);
        assertThat(plain.authorizers().agentGrants()).isEmpty();

        AgentSpacesProperties perAgent = fleetProperties(freePort(), 0);
        PeerId host = PeerIdentity.generate().peerId();
        ai.badmonkey.agentspaces.common.id.AgentId auditor =
                new ai.badmonkey.agentspaces.common.id.AgentId(host, "auditor");
        perAgent.getSecurity().getGrants().setVote(List.of(auditor.encoded()));
        Wired named = wire(perAgent);
        Authorizer authorizer = named.authorizers().forGroup("fleet");
        assertThat(named.authorizers().agentGrants())
                .containsEntry(Authorizer.Operation.VOTE, java.util.Set.of(auditor));
        assertThat(named.authorizers().grants())
                .containsEntry(Authorizer.Operation.VOTE, java.util.Set.of());
        assertThat(authorizer.granularity(Authorizer.Operation.VOTE, "votes"))
                .isEqualTo(Authorizer.Granularity.AGENT);
        // Not admitted yet, so nothing is permitted; the grant shape is what this pins.
        assertThat(authorizer.permits(auditor, Authorizer.Operation.VOTE, "votes")).isFalse();
        assertThat(authorizer.permits(new ai.badmonkey.agentspaces.common.id.AgentId(
                named.identity().peerId(), "self-agent"), Authorizer.Operation.VOTE, "votes"))
                .as("this node's own unnamed agent is not granted").isFalse();
    }

    /** TODO-EFG §4 / TODO item 6 (SPEC §11): under the default mtls profile the Authorizer bean is one MembershipAuthorizer per group, narrowed by agentspaces.security.grants.*. */
    @Test
    void mtlsBuildsAMembershipAuthorizerWithGrants() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        PeerId voter = PeerIdentity.generate().peerId();
        properties.getSecurity().getGrants().setRaftVoter(List.of(voter.value()));
        // gate2-review G2-3: the QUORUM electorate is grantable like the rest.
        PeerId balloter = PeerIdentity.generate().peerId();
        properties.getSecurity().getGrants().setVote(List.of(balloter.value()));
        // agentspaces-springai: model servers are grantable the same way.
        PeerId modelServer = PeerIdentity.generate().peerId();
        properties.getSecurity().getGrants().setModelServe(List.of(modelServer.value()));
        Wired app = wire(properties);
        GroupId group = app.spaces().group("fleet").id();

        Authorizer authorizer = autoConfig.agentSpacesAuthorizer(app.authorizers(), app.spaces());
        // v0.1.13 (M-4): the group's membership authorizer, scoped to refuse what the group revoked.
        assertThat(authorizer).isInstanceOf(ai.badmonkey.agentspaces.peering.membership.GroupScopedAuthorizer.class);
        assertThat(((ai.badmonkey.agentspaces.peering.membership.GroupScopedAuthorizer) authorizer).inner())
                .isInstanceOf(MembershipAuthorizer.class);
        assertThat(app.authorizers().forGroup("fleet")).isSameAs(authorizer);
        assertThat(app.authorizers().profile())
                .isEqualTo(ai.badmonkey.agentspaces.api.security.SecurityProfile.MTLS);
        assertThat(app.authorizers().oidc()).isEmpty();
        assertThat(app.authorizers().grants())
                .containsEntry(Authorizer.Operation.RAFT_VOTER, java.util.Set.of(voter))
                .containsEntry(Authorizer.Operation.VOTE, java.util.Set.of(balloter))
                .containsEntry(Authorizer.Operation.MODEL_SERVE, java.util.Set.of(modelServer))
                .doesNotContainKey(Authorizer.Operation.KEY_HOLDER);

        // Ungranted operations are open to members (this node included); the
        // granted one is narrowed to the listed peer; strangers get nothing.
        PeerId self = app.identity().peerId();
        assertThat(authorizer.permits(self, Authorizer.Operation.KEY_HOLDER, group.value())).isTrue();
        assertThat(authorizer.permits(self, Authorizer.Operation.RAFT_VOTER, group.value())).isFalse();
        assertThat(authorizer.permits(PeerIdentity.generate().peerId(),
                Authorizer.Operation.KEY_HOLDER, group.value())).isFalse();
        assertThatThrownBy(() -> app.authorizers().forGroup("nope"))
                .isInstanceOf(IllegalArgumentException.class);

        // The DirectiveGates bean resolves the group behind a registered space.
        DirectiveGates gates = autoConfig.agentSpacesDirectiveGates(app.authorizers(), app.spaces());
        try (ai.badmonkey.agentspaces.console.DirectiveGate gate =
                     gates.attach(app.spaces().group("fleet").space("tasks"), "w1")) {
            assertThat(gate.paused()).isFalse();
        }
    }

    /**
     * gate2-review G2-1: the CA channel mode is configurable, and configuring
     * it reaches the node. A plaintext transport attests nobody, so the
     * attested node registers TLS alone: dialing a peer's plaintext endpoint
     * would arrive unattested at the far end and get this node evicted.
     */
    @Test
    void requireAttestationReachesTheNodeAndSuppressesPlaintextTcp() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.getTransport().getTls().setEnabled(true);
        properties.getTransport().getTls().setRequireAttestation(true);

        Wired app = wire(properties);

        assertThat(app.node().requiresAttestation())
                .as("the configured posture reached the node").isTrue();
        assertThat(app.node().transportSchemes())
                .containsExactly("tls");
    }

    /**
     * SPEC §5.6 v0.1.13 (item 9): CA-rooted peer revocation is a configured
     * posture that needs the enterprise-CA mode; asking for it without a trust
     * store, or naming an unknown validator, fails at startup.
     */
    @Test
    void theCaRootedRevocationValidatorNeedsTheEnterpriseCaMode() throws Exception {
        AgentSpacesProperties withoutCa = fleetProperties(freePort(), 0);
        withoutCa.getTransport().getTls().setEnabled(true);
        withoutCa.getTransport().getTls().setRevocationValidator("founder-or-ca");
        assertThatThrownBy(() -> wire(withoutCa)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("trust-store");

        AgentSpacesProperties unknown = fleetProperties(freePort(), 0);
        unknown.getTransport().getTls().setRevocationValidator("anyone");
        assertThatThrownBy(() -> wire(unknown)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("founder-or-ca");
    }

    /**
     * SPEC §6.1 v0.1.13 (item 9): {@code founder-or-ca} over a configured trust
     * store installs the CA-rooted validator: a relayed revocation carrying the
     * CA's evidence verifies at this node, where under the default
     * {@code founder} validator the same record is refused.
     */
    @Test
    void founderOrCaInstallsTheCaRootedValidator(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        ai.badmonkey.agentspaces.test.TestCa ca = ai.badmonkey.agentspaces.test.TestCa.create();
        java.nio.file.Path trustStore = dir.resolve("trust.p12");
        java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setCertificateEntry("ca", ca.certificate());
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(trustStore)) {
            store.store(out, "changeit".toCharArray());
        }
        PeerIdentity victim = PeerIdentity.generate();
        PeerIdentity relay = PeerIdentity.generate();
        ai.badmonkey.agentspaces.test.TestCa.Issued leaf = ca.issue(victim.rawPublicKey());
        java.security.cert.X509CRL crl = ca.crl(java.time.Instant.now().minusSeconds(5),
                java.time.Instant.now().plus(Duration.ofDays(1)),
                java.security.cert.CRLReason.KEY_COMPROMISE, leaf.certificate());
        java.util.function.Function<GroupId, ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation>
                caRevocation = group -> {
                    ai.badmonkey.agentspaces.common.codec.CborCodec codec =
                            ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();
                    byte[] evidence = ai.badmonkey.agentspaces.peering.membership.RevocationEvidence
                            .of(leaf.chain(ca), crl).encode(codec);
                    byte[] bytes = codec.toBytes(new ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement(
                            "aspace://" + group.value() + "/revocation/" + victim.peerId().value(),
                            relay.peerId(), group, java.time.Instant.now(), Duration.ofDays(30),
                            victim.peerId(), "ca", null, evidence));
                    return new ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation(
                            bytes, relay.rawPublicKey(), relay.sign(bytes));
                };

        for (String validator : List.of("founder-or-ca", "founder")) {
            AgentSpacesProperties properties = fleetProperties(freePort(), 0);
            properties.getTransport().getTls().setEnabled(true);
            properties.getTransport().getTls().setTrustStore(trustStore.toString());
            properties.getTransport().getTls().setTrustStorePassword("changeit");
            properties.getTransport().getTls().setRevocationValidator(validator);
            Wired app = wire(properties);
            ai.badmonkey.agentspaces.peering.node.GroupRuntime runtime =
                    app.spaces().group("fleet").runtime();
            boolean verified = runtime.revocations().verify(caRevocation.apply(runtime.id())) != null;
            assertThat(verified).as(validator).isEqualTo(validator.equals("founder-or-ca"));
        }
    }

    /**
     * SPEC §4.2 v0.1.13: the starter's agent identities run on the fabric's
     * clock (a subordinate certificate is issued at the context clock's
     * instant), and a declared {@code AgentIdentityFactory} replaces them: the
     * facade signs each bound agent with what the bean returns.
     */
    @Test
    void agentIdentitiesRunOnTheContextClockAndABeanReplacesThem() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.getIdentity().setAgentKeys("subordinate");
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        ai.badmonkey.agentspaces.test.TestClock clock = ai.badmonkey.agentspaces.test.TestClock.create();
        clock.advance(Duration.ofDays(400)); // far from the system clock
        ai.badmonkey.agentspaces.api.spi.AgentIdentity configured =
                autoConfig.agentSpacesAgentIdentities(properties, identity, clock).apply("keyed");
        assertThat(configured.isSubordinate()).isTrue();
        assertThat(configured.certificateCovering(clock.instant()).orElseThrow().issued())
                .as("issued on the context clock").isEqualTo(clock.instant());

        java.util.List<String> asked = new java.util.concurrent.CopyOnWriteArrayList<>();
        ai.badmonkey.agentspaces.agent.AgentIdentityFactory custom = name -> {
            asked.add(name);
            return identity.renewingSubordinate(name, Duration.ofHours(2), clock);
        };
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        closeables.add(node::close);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity,
                autoConfig.agentSpacesAuthorizers(properties, identity, clock),
                autoConfig.agentSpacesEmbedder(), clock, custom);
        closeables.add(spaces::close);
        var bound = spaces.group("fleet").bind(new KeyedAgent());
        assertThat(asked).containsExactly("keyed");
        assertThat(bound.identity().isSubordinate()).isTrue();
        assertThat(bound.identity().certificateCovering(clock.instant()).orElseThrow().ttl())
                .as("the bean's identity, not the configured one").isEqualTo(Duration.ofHours(2));
    }

    /** gate2-review G2-1: the default posture is unchanged, TCP included. */
    @Test
    void theDefaultPostureLeavesAttestationOptionalAndKeepsTcpRegistered() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.getTransport().getTls().setEnabled(true);

        Wired app = wire(properties);

        assertThat(app.node().requiresAttestation()).isFalse();
        assertThat(app.node().transportSchemes())
                .as("plain TCP stays available for dialing peers that advertise it")
                .containsExactlyInAnyOrder("tls", "tcp");
    }

    /**
     * gate2-review G2-1: attestation can only come from an attesting
     * transport, so asking for it without TLS is a misconfiguration the node
     * refuses at startup rather than serving every frame unattested.
     */
    @Test
    void requireAttestationWithoutTlsFailsFast() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.getTransport().getTls().setEnabled(false);
        properties.getTransport().getTls().setRequireAttestation(true);
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);

        assertThatThrownBy(() -> autoConfig.agentSpacesNode(properties, identity))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("require-attestation")
                .hasMessageContaining("agentspaces.transport.tls.enabled");
    }

    /** TODO-EFG §4 / TODO item 6: mtls-oidc fails fast, naming the missing agentspaces.security.oidc.* property. */
    @Test
    void mtlsOidcRequiresIssuerAudienceAndJwks() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.getSecurity().setProfile("mtls-oidc");
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);

        assertThatThrownBy(() -> autoConfig.agentSpacesAuthorizers(properties, identity))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.security.oidc.issuer");
        properties.getSecurity().getOidc().setIssuer(ISSUER);
        assertThatThrownBy(() -> autoConfig.agentSpacesAuthorizers(properties, identity))
                .hasMessageContaining("agentspaces.security.oidc.audience");
        properties.getSecurity().getOidc().setAudience(AUDIENCE);
        assertThatThrownBy(() -> autoConfig.agentSpacesAuthorizers(properties, identity))
                .hasMessageContaining("agentspaces.security.oidc.jwks-url");
        properties.getSecurity().getOidc().setJwksUrl("not a url");
        assertThatThrownBy(() -> autoConfig.agentSpacesAuthorizers(properties, identity))
                .hasMessageContaining("agentspaces.security.oidc.jwks-url");

        properties.getSecurity().getOidc().setJwksUrl(jwksUrl());
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        assertThat(authorizers.oidc()).isPresent();
        assertThat(authorizers.primary()).isSameAs(authorizers.oidc().get());
        assertThat(authorizers.grants()).isEmpty();
        // zero-trust selects the same answerer.
        properties.getSecurity().setProfile("zero-trust");
        assertThat(autoConfig.agentSpacesAuthorizers(properties, identity).oidc()).isPresent();
    }

    /** TODO-EFG §4 / TODO item 6 (SPEC §11): under mtls-oidc a peer's token rides its signed self-advertisement; the other app's OidcAuthorizer permits exactly the token's scopes, and a peer without a token is permitted nothing. */
    @Test
    @Timeout(120)
    void mtlsOidcIngestsTheConfiguredTokenAndAuthorizesByScope(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        int seedPort = freePort();
        // App A's identity is fixed up front so its token can name its PeerID.
        AgentSpacesProperties aProps = fleetProperties(seedPort, 0);
        aProps.setKeystore(dir.resolve("a-keys").toString());
        oidcProfile(aProps);
        PeerId aPeer = autoConfig.agentSpacesIdentity(aProps).peerId();
        aProps.getSecurity().getOidc().setToken(token(aPeer, "openid aspace:space-write:tasks"));

        // App B presents its token from a file, the sidecar-rotated form.
        AgentSpacesProperties bProps = fleetProperties(freePort(), seedPort);
        bProps.setKeystore(dir.resolve("b-keys").toString());
        oidcProfile(bProps);
        PeerId bPeer = autoConfig.agentSpacesIdentity(bProps).peerId();
        java.nio.file.Path tokenFile = dir.resolve("b-token");
        java.nio.file.Files.writeString(tokenFile, token(bPeer, "aspace:raft-voter") + "\n");
        bProps.getSecurity().getOidc().setTokenFile(tokenFile.toString());

        Wired a = wire(aProps);
        a.lifecycle().start();
        Wired b = wire(bProps);
        b.lifecycle().start();
        Authorizer onA = a.authorizers().primary();
        Authorizer onB = b.authorizers().primary();

        // Each node validated its own token locally at wiring time.
        assertThat(onA.permits(aPeer, Authorizer.Operation.SPACE_WRITE, "tasks")).isTrue();
        assertThat(onB.permits(bPeer, Authorizer.Operation.RAFT_VOTER, "")).isTrue();

        // The tokens reach the other node inside the self-advertisements.
        assertThat(await(Duration.ofSeconds(30),
                () -> onB.permits(aPeer, Authorizer.Operation.SPACE_WRITE, "tasks")))
                .as("B ingested A's token from A's advertisement hints").isTrue();
        assertThat(await(Duration.ofSeconds(30),
                () -> onA.permits(bPeer, Authorizer.Operation.RAFT_VOTER, "")))
                .as("A ingested B's token from B's advertisement hints").isTrue();

        // Scopes are exact: A may write tasks and nothing else; B may vote and
        // nothing else. The membership perimeter alone grants nothing here.
        assertThat(onB.permits(aPeer, Authorizer.Operation.SPACE_WRITE, "findings")).isFalse();
        assertThat(onB.permits(aPeer, Authorizer.Operation.SPACE_TAKE, "tasks")).isFalse();
        assertThat(onB.permits(aPeer, Authorizer.Operation.RAFT_VOTER, "")).isFalse();
        assertThat(onA.permits(bPeer, Authorizer.Operation.SPACE_WRITE, "tasks")).isFalse();

        // v0.1.13 (review M-4): the exported authorizer is group-scoped under OIDC
        // too, so an agent its own peer revoked is refused despite a live token.
        ai.badmonkey.agentspaces.common.id.AgentId aApp = new ai.badmonkey.agentspaces.common.id.AgentId(aPeer, "app");
        assertThat(onB.permits(aApp, Authorizer.Operation.SPACE_WRITE, "tasks")).isTrue();
        assertThat(a.node().group(a.spaces().group("fleet").id()).orElseThrow()
                .revokeAgent(aApp, ai.badmonkey.agentspaces.api.ad.CredentialRevocation.RETIRED)).isPresent();
        assertThat(await(Duration.ofSeconds(30),
                () -> !onB.permits(aApp, Authorizer.Operation.SPACE_WRITE, "tasks")))
                .as("B's exported authorizer refuses the revoked agent whatever its peer's token says").isTrue();
        assertThat(onB.permits(aPeer, Authorizer.Operation.SPACE_WRITE, "tasks"))
                .as("the peer itself is not revoked").isTrue();
        assertThat(b.node().group(a.spaces().group("fleet").id()).orElseThrow()
                .membership().member(aPeer)).as("A is an admitted member of B's view").isPresent();

        // Rotating B's file re-presents the new token on the refresh tick.
        java.nio.file.Files.writeString(tokenFile, token(bPeer, "aspace:key-holder"));
        b.authorizers().refreshToken();
        assertThat(onB.permits(bPeer, Authorizer.Operation.KEY_HOLDER, "")).isTrue();
        assertThat(await(Duration.ofSeconds(30),
                () -> onA.permits(bPeer, Authorizer.Operation.KEY_HOLDER, "")))
                .as("A sees B's rotated token").isTrue();
    }

    /** TODO-EFG §4 (the FleetCommander row): with command-and-control enabled the console's own peer is granted DIRECTIVE_ISSUER and that operation is narrowed to it, so a worker's DirectiveGates gate obeys the console and no other member. */
    @Test
    @Timeout(90)
    void theConsolePeerIsGrantedDirectiveIssuer(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        int seedPort = freePort();
        AgentSpacesProperties consoleProps = fleetProperties(seedPort, 0);
        consoleProps.setKeystore(dir.resolve("console-keys").toString());
        AgentSpacesProperties.SpaceDef control = new AgentSpacesProperties.SpaceDef();
        control.setName("fleet-control");
        control.setSettleWindowMillis(100);
        consoleProps.getGroups().get(0).setSpaces(List.of(control));
        consoleProps.getConsole().setEnabled(true);
        consoleProps.getConsole().getCommand().setEnabled(true);
        PeerId consolePeer = autoConfig.agentSpacesIdentity(consoleProps).peerId();

        // A worker node names the console peer as its only directive issuer.
        AgentSpacesProperties workerProps = fleetProperties(freePort(), seedPort);
        workerProps.getGroups().get(0).setSpaces(List.of(control));
        workerProps.getSecurity().getGrants().setDirectiveIssuer(List.of(consolePeer.value()));

        Wired console = wire(consoleProps);
        console.lifecycle().start();
        Wired worker = wire(workerProps);
        worker.lifecycle().start();
        GroupId group = console.spaces().group("fleet").id();
        PeerId workerPeer = worker.identity().peerId();

        assertThat(console.authorizers().grants().get(Authorizer.Operation.DIRECTIVE_ISSUER))
                .containsExactly(consolePeer);
        Authorizer onConsole = console.authorizers().primary();
        assertThat(onConsole.permits(consolePeer, Authorizer.Operation.DIRECTIVE_ISSUER,
                group.value())).isTrue();
        assertThat(await(Duration.ofSeconds(20), () -> onConsole.permits(workerPeer,
                Authorizer.Operation.KEY_HOLDER, group.value())))
                .as("the worker is an admitted member").isTrue();
        assertThat(onConsole.permits(workerPeer, Authorizer.Operation.DIRECTIVE_ISSUER,
                group.value())).as("members are not issuers once C2 narrows it").isFalse();

        // The worker's gate obeys the console's directive and not its own peer's.
        DirectiveGates gates =
                autoConfig.agentSpacesDirectiveGates(worker.authorizers(), worker.spaces());
        ai.badmonkey.agentspaces.api.space.Space workerControl =
                worker.spaces().group("fleet").space("fleet-control");
        try (ai.badmonkey.agentspaces.console.DirectiveGate gate =
                     gates.attach(workerControl, "w1")) {
            workerControl.write(new ai.badmonkey.agentspaces.console.Directive(
                    ai.badmonkey.agentspaces.console.Directive.DRAIN, "*", "rogue",
                    System.currentTimeMillis()), Lease.of(Duration.ofMinutes(10)));
            assertThat(gate.draining()).as("a member's own directive is ignored").isFalse();
            console.spaces().group("fleet").space("fleet-control").write(
                    new ai.badmonkey.agentspaces.console.Directive(
                            ai.badmonkey.agentspaces.console.Directive.PAUSE, "w1", "op",
                            System.currentTimeMillis() + 1), Lease.of(Duration.ofMinutes(10)));
            assertThat(await(Duration.ofSeconds(30), gate::paused))
                    .as("the console's directive replicates and takes effect").isTrue();
            assertThat(gate.draining()).isFalse();
        }
    }

    /** TODO-EFG §3 (SPEC §7.5 CREDENTIAL): admission: credential issues from this node by default, or from the configured credential-issuer. */
    @Test
    void anAdmissionOfCredentialIssuesFromTheConfiguredIssuer() throws Exception {
        AgentSpacesProperties founder = fleetProperties(freePort(), 0);
        AgentSpacesProperties.SpaceDef vault = new AgentSpacesProperties.SpaceDef();
        vault.setName("vault");
        vault.setAdmission("credential");
        founder.getGroups().get(0).setSpaces(List.of(vault));
        Wired issuing = wire(founder);
        ReplicatedSpace issuingVault =
                (ReplicatedSpace) issuing.spaces().group("fleet").space("vault");
        assertThat(issuingVault.admissionRule())
                .isInstanceOfSatisfying(SpaceAdmission.CredentialAdmission.class, rule ->
                        assertThat(rule.issuer()).isEqualTo(issuing.identity().peerId()));
        assertThat(issuingVault.credentialIndex()).isPresent();
        assertThat(issuingVault.grant(PeerIdentity.generate().agent("b"),
                java.util.Set.of(SpaceAdmission.Scope.WRITE), Duration.ofMinutes(5))).isNotNull();
        assertThat(issuing.spaces().group("fleet").discovery().find(SpaceAdvertisement.class,
                ad -> "vault".equals(ad.spaceName())).get(0).admission())
                .isEqualTo(SpaceAdvertisement.Admission.CREDENTIAL);

        // A replica naming another issuer indexes that issuer's credentials
        // and may not grant itself.
        PeerId other = PeerIdentity.generate().peerId();
        AgentSpacesProperties replicaProps = fleetProperties(freePort(), 0);
        AgentSpacesProperties.SpaceDef delegated = new AgentSpacesProperties.SpaceDef();
        delegated.setName("vault");
        delegated.setAdmission("credential");
        delegated.setCredentialIssuer(other.value());
        replicaProps.getGroups().get(0).setSpaces(List.of(delegated));
        Wired replica = wire(replicaProps);
        ReplicatedSpace replicaVault =
                (ReplicatedSpace) replica.spaces().group("fleet").space("vault");
        assertThat(replicaVault.admissionRule())
                .isInstanceOfSatisfying(SpaceAdmission.CredentialAdmission.class, rule ->
                        assertThat(rule.issuer()).isEqualTo(other));
        assertThatThrownBy(() -> replicaVault.grant(PeerIdentity.generate().agent("b"),
                java.util.Set.of(SpaceAdmission.Scope.WRITE), Duration.ofMinutes(5)))
                .isInstanceOf(SpaceAdmissionException.class);
    }

    /** TODO-EFG §4 (SPEC §7.5 AUTHORIZER): admission: authorizer asks the profile's Authorizer SPACE_WRITE in the scope of the space name, so a space-write grant decides who may write. */
    @Test
    void anAdmissionOfAuthorizerUsesTheProfilesAuthorizer(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        AgentSpacesProperties writerProps = fleetProperties(freePort(), 0);
        writerProps.setKeystore(dir.resolve("writer-keys").toString());
        PeerId writerPeer = autoConfig.agentSpacesIdentity(writerProps).peerId();
        AgentSpacesProperties.SpaceDef gated = new AgentSpacesProperties.SpaceDef();
        gated.setName("gated");
        gated.setAdmission("authorizer");
        writerProps.getGroups().get(0).setSpaces(List.of(gated));
        writerProps.getSecurity().getGrants().setSpaceWrite(List.of(writerPeer.value()));

        Wired writer = wire(writerProps);
        ReplicatedSpace writerGated = (ReplicatedSpace) writer.spaces().group("fleet").space("gated");
        assertThat(writerGated.admissionRule().rule())
                .isEqualTo(SpaceAdvertisement.Admission.AUTHORIZER);
        writerGated.write(new TaskEntry("granted", 1), Lease.of(Duration.ofMinutes(10)));
        assertThat(writer.spaces().group("fleet").discovery().find(SpaceAdvertisement.class,
                ad -> "gated".equals(ad.spaceName())).get(0).admission())
                .isEqualTo(SpaceAdvertisement.Admission.AUTHORIZER);

        // Another node with the same grant configuration is not the granted
        // peer: its own authorizer refuses its writes locally.
        AgentSpacesProperties otherProps = fleetProperties(freePort(), 0);
        otherProps.getGroups().get(0).setSpaces(List.of(gated));
        otherProps.getSecurity().getGrants().setSpaceWrite(List.of(writerPeer.value()));
        Wired other = wire(otherProps);
        assertThatThrownBy(() -> other.spaces().group("fleet").space("gated")
                .write(new TaskEntry("refused", 1), Lease.of(Duration.ofMinutes(10))))
                .isInstanceOf(SpaceAdmissionException.class);
        // Reads stay open, and takes follow the (ungranted, so open) take scope.
        assertThat(other.spaces().group("fleet").space("gated")
                .read(Template.of(TaskEntry.class))).isEmpty();
    }

    @AgentSpec(name = "researcher", description = "Researches topics", goals = {"research"})
    public static class Researcher {

        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S",
                resultSpace = "findings")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done: " + task.topic());
        }
    }

    public static class PlainBean {
    }

    /** The one-annotation experience: the stereotype alone makes it an agent. */
    @SpaceAgent(name = "steward", description = "Stereotyped worker", goals = {"steward"})
    public static class Steward {

        /**
         * Works one task.
         *
         * @param task the task
         * @return the finding
         */
        @SpaceTake(space = "tasks", lease = "10m", pollTimeout = "200ms",
                resultSpace = "findings")
        public FindingEntry work(TaskEntry task) {
            return new FindingEntry(task.topic(), "steward: " + task.topic());
        }
    }

    @Test
    @Timeout(60)
    void aSpaceAgentStereotypeAloneEnrollsTheBean() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        app.lifecycle().start();
        app.postProcessor().postProcessAfterInitialization(new Steward(), "steward");

        app.spaces().group("fleet").space("tasks")
                .write(new TaskEntry("stereotyped", 1), Lease.of(Duration.ofMinutes(10)));

        assertThat(app.spaces().group("fleet").space("findings")
                .read(Template.of(FindingEntry.class), Duration.ofSeconds(10)))
                .hasValueSatisfying(finding ->
                        assertThat(finding.summary()).isEqualTo("steward: stereotyped"));

        // The composed annotation supplied the card's whole identity.
        java.util.List<AgentCard> cards = app.spaces().group("fleet").discovery()
                .find(AgentCard.class, card -> card.agent().localName().equals("steward"));
        assertThat(cards).hasSize(1);
        assertThat(cards.get(0).description()).isEqualTo("Stereotyped worker");
        assertThat(cards.get(0).goals()).containsExactly("steward");
    }

    @Test
    @Timeout(60)
    void propertiesWireAWorkingFabricAndBeansEnroll() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        app.lifecycle().start();
        assertThat(app.lifecycle().isRunning()).isTrue();

        // A plain bean passes through; the annotated bean binds and works.
        PlainBean plain = new PlainBean();
        assertThat(app.postProcessor().postProcessAfterInitialization(plain, "plain"))
                .isSameAs(plain);
        app.postProcessor().postProcessAfterInitialization(new Researcher(), "researcher");

        app.spaces().group("fleet").space("tasks")
                .write(new TaskEntry("spring wiring", 1), Lease.of(Duration.ofMinutes(10)));

        assertThat(app.spaces().group("fleet").space("findings")
                .read(Template.of(FindingEntry.class), Duration.ofSeconds(10)))
                .hasValueSatisfying(finding ->
                        assertThat(finding.summary()).isEqualTo("done: spring wiring"));

        // The bound agent's card published into the group's discovery.
        assertThat(app.spaces().group("fleet").discovery()
                .find(AgentCard.class, card -> card.description().contains("Researches")))
                .hasSize(1);

        app.lifecycle().stop();
        assertThat(app.lifecycle().isRunning()).isFalse();
    }

    @Test
    @Timeout(90)
    void twoPropertyWiredAppsFormOneFleetOverTcp() throws Exception {
        int seedPort = freePort();
        Wired first = wire(fleetProperties(seedPort, 0));
        first.lifecycle().start();
        Wired second = wire(fleetProperties(freePort(), seedPort));
        second.lifecycle().start();

        second.postProcessor().postProcessAfterInitialization(new Researcher(), "researcher");

        first.spaces().group("fleet").space("tasks")
                .write(new TaskEntry("cross app", 2), Lease.of(Duration.ofMinutes(10)));

        assertThat(first.spaces().group("fleet").space("findings")
                .read(Template.of(FindingEntry.class), Duration.ofSeconds(20)))
                .hasValueSatisfying(finding ->
                        assertThat(finding.summary()).isEqualTo("done: cross app"));
    }

    @Test
    void tlsAttestedAppsFormAFleetAndElideEnvelopeSignatures() throws Exception {
        int seedPort = freePort();
        AgentSpacesProperties firstProps = fleetProperties(seedPort, 0);
        firstProps.getTransport().setChannelAuth("attested");
        firstProps.getTransport().getTls().setEnabled(true);
        AgentSpacesProperties secondProps = fleetProperties(freePort(), seedPort);
        secondProps.getTransport().setChannelAuth("attested");
        secondProps.getTransport().getTls().setEnabled(true);

        Wired first = wire(firstProps);
        first.lifecycle().start();
        Wired second = wire(secondProps);
        second.lifecycle().start();

        second.postProcessor().postProcessAfterInitialization(new Researcher(), "researcher");

        first.spaces().group("fleet").space("tasks")
                .write(new TaskEntry("over tls", 2), Lease.of(Duration.ofMinutes(10)));

        assertThat(first.spaces().group("fleet").space("findings")
                .read(Template.of(FindingEntry.class), Duration.ofSeconds(20)))
                .hasValueSatisfying(finding ->
                        assertThat(finding.summary()).isEqualTo("done: over tls"));

        // The channels attested and negotiated: the work above rode bare frames.
        assertThat(first.node().bareFramesSent() + second.node().bareFramesSent())
                .isGreaterThan(0);
    }

    @Test
    void beansReferencingUnregisteredSpacesFailFast() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));

        class Misconfigured {
        }
        // The local class has no annotations; it passes through.
        assertThat(app.postProcessor()
                .postProcessAfterInitialization(new Misconfigured(), "m"))
                .isInstanceOf(Misconfigured.class);

        assertThatThrownBy(() -> app.postProcessor().postProcessAfterInitialization(
                new Object() {
                    @SpaceTake(space = "no-such-space", lease = "PT1M")
                    public void work(TaskEntry task) {
                    }
                }, "broken"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no-such-space");
    }

    @Test
    void identityComesFromTheConfiguredKeystore(@org.junit.jupiter.api.io.TempDir
                                                java.nio.file.Path tempDir) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setKeystore(tempDir.toString());

        PeerIdentity first = autoConfig.agentSpacesIdentity(properties);
        PeerIdentity second = autoConfig.agentSpacesIdentity(properties);
        assertThat(first.peerId()).isEqualTo(second.peerId());

        properties.setKeystore(null);
        assertThat(autoConfig.agentSpacesIdentity(properties).peerId())
                .isNotEqualTo(first.peerId());
    }

    /** SPEC §10.1: the configured roles ride the peer's advertisement, so the fleet sees them. */
    @Test
    @Timeout(90)
    void rolesBindToThePeerAdvertisement() throws Exception {
        int seedPort = freePort();
        AgentSpacesProperties firstProps = fleetProperties(seedPort, 0);
        firstProps.setRoles(List.of("rendezvous"));
        Wired first = wire(firstProps);
        first.lifecycle().start();
        Wired second = wire(fleetProperties(freePort(), seedPort));
        second.lifecycle().start();

        List<ai.badmonkey.agentspaces.common.id.PeerId> rendezvous = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (rendezvous.isEmpty() && System.nanoTime() < deadline) {
            rendezvous = second.spaces().group("fleet").runtime().membership()
                    .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS);
            Thread.sleep(100);
        }
        assertThat(rendezvous).containsExactly(first.node().peerId());
        assertThat(first.spaces().group("fleet").runtime().membership()
                .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS))
                .doesNotContain(second.node().peerId());
    }

    /** SPEC §10.1: a role name that is not a PeerRole fails fast at wiring time. */
    @Test
    void aBogusRoleFailsFast() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.setRoles(List.of("bogus"));
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        assertThatThrownBy(() -> autoConfig.agentSpacesNode(properties, identity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BOGUS");
    }

    // ------------------------------------------------------------ §10 additions

    /** Polls a condition on a deadline; the fabric is asynchronous by design. */
    private static boolean await(Duration limit, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    private static AgentSpacesProperties.Group group(String name, String founding,
                                                     int seedPort, String... spaceNames) {
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName(name);
        group.setFounding(founding);
        if (seedPort > 0) {
            group.setSeeds(List.of("127.0.0.1:" + seedPort));
        }
        List<AgentSpacesProperties.SpaceDef> spaces = new ArrayList<>();
        for (String spaceName : spaceNames) {
            AgentSpacesProperties.SpaceDef def = new AgentSpacesProperties.SpaceDef();
            def.setName(spaceName);
            def.setSettleWindowMillis(100);
            spaces.add(def);
        }
        group.setSpaces(spaces);
        return group;
    }

    /** Two groups with the same space names, the §10.3 headline scenario. */
    private AgentSpacesProperties twoGroupProperties(int port) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(200);
        properties.setGroups(List.of(
                group("alpha", "spring-alpha-v1", 0, "tasks", "findings"),
                group("beta", "spring-beta-v1", 0, "tasks", "findings")));
        return properties;
    }

    /** A worker pinned to one group by the annotation's group attribute (§10.3). */
    @AgentSpec(name = "beta-worker", description = "Works beta tasks only", goals = {"beta"})
    public static class BetaWorker {

        @SpaceTake(group = "beta", space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S",
                resultSpace = "findings")
        public FindingEntry work(TaskEntry task) {
            return new FindingEntry(task.topic(), "beta: " + task.topic());
        }
    }

    /** A capability bean: the §10.5 provider SPI plus the annotation. */
    @ProvidesCapability("aspace:cap/echo")
    public static class EchoCapability implements CapabilityProvider {

        private final PeerId self;

        public EchoCapability(PeerId self) {
            this.self = self;
        }

        @Override
        public String capabilityType() {
            return "aspace:cap/echo";
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return new CapabilityAdvertisement("aspace://" + group.value() + "/cap/echo/"
                    + self.value(), self, group, Instant.now(), Duration.ofMinutes(15),
                    "aspace:cap/echo", "0.1", "pipe", Map.of(), Map.of());
        }
    }

    /** An entry whose payload exceeds the 64 KiB inline limit (§6.1a). */
    public record BulkEntry(String topic, String payload) {
    }

    /** SPEC §10.3/§10.4: an annotated bean binds into every group that registers its spaces, one card per group. */
    @Test
    @Timeout(60)
    void anAnnotatedBeanBindsIntoEveryGroupThatRegistersItsSpaces() throws Exception {
        Wired app = wire(twoGroupProperties(freePort()));
        app.lifecycle().start();
        app.postProcessor().postProcessAfterInitialization(new Researcher(), "researcher");

        for (String groupName : List.of("alpha", "beta")) {
            assertThat(app.spaces().group(groupName).discovery().find(AgentCard.class,
                    card -> "researcher".equals(card.agent().localName())))
                    .as("card in " + groupName).hasSize(1);
            app.spaces().group(groupName).space("tasks")
                    .write(new TaskEntry(groupName, 1), Lease.of(Duration.ofMinutes(10)));
            assertThat(app.spaces().group(groupName).space("findings")
                    .read(Template.of(FindingEntry.class), Duration.ofSeconds(10)))
                    .hasValueSatisfying(finding ->
                            assertThat(finding.summary()).isEqualTo("done: " + groupName));
        }
    }

    /** SPEC §10.3: the group attribute routes a method to one group even when another registers the same spaces. */
    @Test
    @Timeout(60)
    void theGroupAttributeRoutesAMethodToOneGroup() throws Exception {
        Wired app = wire(twoGroupProperties(freePort()));
        app.lifecycle().start();
        app.postProcessor().postProcessAfterInitialization(new BetaWorker(), "betaWorker");

        assertThat(app.spaces().group("beta").discovery().find(AgentCard.class,
                card -> "beta-worker".equals(card.agent().localName()))).hasSize(1);
        assertThat(app.spaces().group("alpha").discovery().find(AgentCard.class,
                card -> "beta-worker".equals(card.agent().localName()))).isEmpty();

        app.spaces().group("beta").space("tasks")
                .write(new TaskEntry("routed", 1), Lease.of(Duration.ofMinutes(10)));
        app.spaces().group("alpha").space("tasks")
                .write(new TaskEntry("ignored", 1), Lease.of(Duration.ofMinutes(10)));
        assertThat(app.spaces().group("beta").space("findings")
                .read(Template.of(FindingEntry.class), Duration.ofSeconds(10)))
                .hasValueSatisfying(finding ->
                        assertThat(finding.summary()).isEqualTo("beta: routed"));
        // Nobody works alpha: its task stays, its findings space stays empty.
        assertThat(app.spaces().group("alpha").space("findings")
                .read(Template.of(FindingEntry.class), Duration.ofSeconds(1))).isEmpty();
        assertThat(app.spaces().group("alpha").space("tasks")
                .readAll(Template.of(TaskEntry.class), 10)).hasSize(1);
    }

    /** SPEC §10.5: the starter ships aggregate, vote, semantic-discovery and key-wrap, advertised under the node's PeerId. */
    @Test
    @Timeout(60)
    void theStarterShipsTheDefaultCapabilities() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        app.lifecycle().start();

        AgentSpaces.GroupContext fleet = app.spaces().group("fleet");
        for (String type : List.of(PushSumAggregate.TYPE, VoteCapability.TYPE,
                SemanticDiscovery.TYPE, GroupKeyDistributor.TYPE)) {
            List<CapabilityAdvertisement> ads = fleet.capabilities().providersOf(type);
            assertThat(ads).as(type).hasSize(1);
            assertThat(ads.get(0).issuer()).isEqualTo(app.node().peerId());
            assertThat(fleet.provider(type)).as("local provider " + type).isPresent();
        }
        // Ordered log and gossip learning are off until configured (§10.5).
        assertThat(fleet.discovery().find(CapabilityAdvertisement.class, ad -> true))
                .extracting(CapabilityAdvertisement::capabilityType)
                .containsExactlyInAnyOrder(PushSumAggregate.TYPE, VoteCapability.TYPE,
                        SemanticDiscovery.TYPE, GroupKeyDistributor.TYPE);
        // The vote capability runs over the default votes space, which stays
        // out of the group's registered spaces (sole-space inference intact).
        assertThat(fleet.capabilities().providersOf(VoteCapability.TYPE).get(0).binding())
                .isEqualTo("space:votes");
        assertThat(fleet.spaceNames()).containsExactlyInAnyOrder("tasks", "findings");
    }

    /** SPEC §10.5: spaces.group(...).capability(VoteClient.class) proposes on one app and a second app's ballot decides on both. */
    @Test
    @Timeout(120)
    void typedCapabilityClientsResolve() throws Exception {
        int seedPort = freePort();
        Wired first = wire(fleetProperties(seedPort, 0));
        first.lifecycle().start();
        Wired second = wire(fleetProperties(freePort(), seedPort));
        second.lifecycle().start();
        Lease hour = Lease.of(Duration.ofHours(1));

        VoteClient proposer = first.spaces().group("fleet").capability(VoteClient.class);
        VoteClient voter = second.spaces().group("fleet").capability(VoteClient.class);
        assertThat(first.spaces().group("fleet").capability(VoteClient.class))
                .as("one client per group").isSameAs(proposer);

        proposer.propose("ship", "Ship it?", List.of("approve", "reject"), 2, hour);
        assertThat(await(Duration.ofSeconds(60), () -> voter.proposal("ship").isPresent()))
                .as("proposal replicated").isTrue();

        voter.castBallot("ship", "approve", hour);
        proposer.castBallot("ship", "approve", hour);
        for (VoteClient client : List.of(proposer, voter)) {
            assertThat(await(Duration.ofSeconds(60), () -> client.decision("ship").isPresent()))
                    .as("decision reached").isTrue();
            assertThat(client.decision("ship")).hasValueSatisfying(decision -> {
                assertThat(decision.winner()).isEqualTo("approve");
                assertThat(decision.tally()).containsEntry("approve", 2);
            });
        }
    }

    /**
     * QA3 A3-1 and A3-2: a starter-configured fleet's aggregate <em>works</em>,
     * not merely advertises. Two applications join, each contributes its own
     * number, and both converge on the fleet average with no scheduler, no tick
     * call, and no capability-aware code anywhere in the application. This is
     * the test whose absence let an advertised-but-never-driven capability ship.
     */
    @Test
    @Timeout(120)
    void theStarterDrivesAggregateToConvergenceWithNoApplicationClock() throws Exception {
        int seedPort = freePort();
        Wired first = wire(fleetProperties(seedPort, 0));
        first.lifecycle().start();
        Wired second = wire(fleetProperties(freePort(), seedPort));
        second.lifecycle().start();

        AggregateClient one = first.spaces().group("fleet").capability(AggregateClient.class);
        AggregateClient two = second.spaces().group("fleet").capability(AggregateClient.class);

        // Before anything is contributed there is nothing to report.
        assertThat(one.estimate("backlog")).isEmpty();

        one.start("backlog", 10.0);
        two.start("backlog", 30.0);          // the fleet average is 20

        assertThat(await(Duration.ofSeconds(90), () ->
                one.estimate("backlog").orElse(-1) > 19.9
                        && one.estimate("backlog").orElse(-1) < 20.1
                        && two.estimate("backlog").orElse(-1) > 19.9
                        && two.estimate("backlog").orElse(-1) < 20.1))
                .as("both peers converge on 20 with the application driving nothing")
                .isTrue();
    }

    /** QA3 A3-1: the starter's capability runtime rides the peer tick, and the peer tick alone. */
    @Test
    @Timeout(60)
    void theStarterCapabilityRuntimeIsDrivenByThePeerTick() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        app.lifecycle().start();

        AgentSpaces.GroupContext fleet = app.spaces().group("fleet");
        assertThat(fleet.capabilities().isDrivenByPeerTick())
                .as("registered with the group's clock, so nothing else has to be")
                .isTrue();
        assertThat(app.node().tickPeriod())
                .as("the node knows its configured rate as soon as it starts ticking")
                .hasValue(Duration.ofMillis(200));
        // Providers learn the cadence on the first tick after startTicking, which
        // is one tick period away; the runtime latches it then.
        assertThat(await(Duration.ofSeconds(30), () ->
                fleet.capabilities().cadence().isPresent()))
                .as("the cadence reaches the runtime").isTrue();
        assertThat(fleet.capabilities().cadence()).hasValue(Duration.ofMillis(200));
    }

    /**
     * QA4 A4-1 under Spring: the same misuse must fail the context start, and
     * must say what is actually wrong rather than blaming the space wiring.
     */
    @Test
    @Timeout(60)
    void providesCapabilityWithoutTheInterfaceFailsTheContext() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        app.lifecycle().start();

        assertThatThrownBy(() -> app.postProcessor()
                .postProcessAfterInitialization(new NotAProviderBean(), "broken"))
                .hasMessageContaining("NotAProviderBean")
                .hasMessageContaining("CapabilityProvider")
                .hasMessageNotContaining("no configured group registers them all");
    }

    /** Annotated as a capability but missing the interface. */
    @ProvidesCapability("aspace:cap/broken")
    public static class NotAProviderBean {
    }

    /** SPEC §10.5: a @ProvidesCapability bean is registered on the group's runtime and advertised through discovery. */
    @Test
    @Timeout(60)
    void providesCapabilityBeansAreRegisteredAndDiscoverable() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        app.lifecycle().start();

        EchoCapability echo = new EchoCapability(app.node().peerId());
        assertThat(app.postProcessor().postProcessAfterInitialization(echo, "echo"))
                .isSameAs(echo);

        AgentSpaces.GroupContext fleet = app.spaces().group("fleet");
        assertThat(fleet.provider("aspace:cap/echo")).contains(echo);
        List<CapabilityAdvertisement> ads = fleet.capabilities().providersOf("aspace:cap/echo");
        assertThat(ads).hasSize(1);
        assertThat(ads.get(0).issuer()).isEqualTo(app.node().peerId());
        // Not an agent: no card was published for it.
        assertThat(fleet.discovery().find(AgentCard.class, card -> true)).isEmpty();
    }

    /** SPEC §10.5: agentspaces.capabilities.* toggles switch each shipped provider, and the whole surface, off. */
    @Test
    @Timeout(60)
    void capabilityTogglesAreHonoured() throws Exception {
        AgentSpacesProperties partial = fleetProperties(freePort(), 0);
        partial.getCapabilities().setVote(false);
        partial.getCapabilities().setSemanticDiscovery(false);
        Wired app = wire(partial);
        AgentSpaces.GroupContext fleet = app.spaces().group("fleet");
        assertThat(fleet.discovery().find(CapabilityAdvertisement.class, ad -> true))
                .extracting(CapabilityAdvertisement::capabilityType)
                .containsExactlyInAnyOrder(PushSumAggregate.TYPE, GroupKeyDistributor.TYPE);
        assertThat(fleet.provider(VoteCapability.TYPE)).isEmpty();

        AgentSpacesProperties none = fleetProperties(freePort(), 0);
        none.getCapabilities().setEnabled(false);
        Wired quiet = wire(none);
        assertThat(quiet.spaces().group("fleet").discovery()
                .find(CapabilityAdvertisement.class, ad -> true)).isEmpty();

        // The ordered log needs a member set the starter cannot infer: fail fast.
        AgentSpacesProperties raft = fleetProperties(freePort(), 0);
        raft.getCapabilities().setOrderedLog(true);
        PeerIdentity identity = autoConfig.agentSpacesIdentity(raft);
        PeerNode node = autoConfig.agentSpacesNode(raft, identity);
        closeables.add(node);
        assertThatThrownBy(() -> autoConfig.agentSpaces(raft, node, identity))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ordered-log");
    }

    /** SPEC §8: the lifecycle's refresh re-issues capability advertisements, so they never lapse while the host runs. */
    @Test
    @Timeout(60)
    void theLifecycleRefreshesCardsAndCapabilityAdvertisements() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.setCardRefreshMillis(200);
        Wired app = wire(properties);
        app.lifecycle().start();
        app.postProcessor().postProcessAfterInitialization(new Researcher(), "researcher");

        AgentSpaces.GroupContext fleet = app.spaces().group("fleet");
        Instant firstIssued = fleet.capabilities().providersOf(PushSumAggregate.TYPE)
                .get(0).issued();
        Instant firstCard = fleet.discovery().find(AgentCard.class, card -> true).get(0).issued();

        assertThat(await(Duration.ofSeconds(2), () -> fleet.capabilities()
                .providersOf(PushSumAggregate.TYPE).get(0).issued().isAfter(firstIssued)))
                .as("capability advertisement re-issued within 2 s").isTrue();
        assertThat(await(Duration.ofSeconds(2), () -> fleet.discovery()
                .find(AgentCard.class, card -> true).get(0).issued().isAfter(firstCard)))
                .as("card re-issued within 2 s").isTrue();
    }

    /** SPEC §6.1a/§7.1: auto-configured spaces carry a payload over 64 KiB between two apps through the block exchange. */
    @Test
    @Timeout(120)
    void autoConfiguredSpacesCarryBulkPayloads() throws Exception {
        int seedPort = freePort();
        Wired first = wire(fleetProperties(seedPort, 0));
        first.lifecycle().start();
        Wired second = wire(fleetProperties(freePort(), seedPort));
        second.lifecycle().start();

        String payload = "x".repeat(70 * 1024);
        first.spaces().group("fleet").space("tasks")
                .write(new BulkEntry("bulk", payload), Lease.of(Duration.ofMinutes(10)));

        Optional<BulkEntry> seen = Optional.empty();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (seen.isEmpty() && System.nanoTime() < deadline) {
            seen = second.spaces().group("fleet").space("tasks")
                    .read(Template.of(BulkEntry.class), Duration.ofSeconds(5));
        }
        assertThat(seen).hasValueSatisfying(entry -> {
            assertThat(entry.topic()).isEqualTo("bulk");
            assertThat(entry.payload()).hasSize(70 * 1024);
        });
    }

    /** SPEC §10.1: a rendezvous role sizes the group's ad cache four times the default. */
    @Test
    void aRendezvousRoleSizesTheAdCache() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.setRoles(List.of("rendezvous"));
        Wired rendezvous = wire(properties);
        assertThat(rendezvous.spaces().group("fleet").discovery().cache().maxEntries())
                .isEqualTo(32_768);

        Wired plain = wire(fleetProperties(freePort(), 0));
        assertThat(plain.spaces().group("fleet").discovery().cache().maxEntries())
                .isEqualTo(AdCache.DEFAULT_MAX_ENTRIES);
    }

    /** SPEC §10.1/§4.4: a group configured by join URI fetches the founding advertisement from a seed and verifies it before joining. */
    @Test
    @Timeout(90)
    void aGroupConfiguredByJoinUriFetchesAndVerifiesItsFoundingAdvertisement()
            throws Exception {
        PeerIdentity founderIdentity = PeerIdentity.generate();
        SignedGroupAdvertisement founding = GroupFounding.found(founderIdentity, "fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), Instant.now(),
                Duration.ofDays(1));
        GroupId groupId = founding.advertisement().group();
        int seedPort = freePort();
        PeerNode founder = PeerNode.builder(founderIdentity).build();
        closeables.add(founder);
        founder.listen(new TlsTcpTransport(founderIdentity), "127.0.0.1:" + seedPort);
        founder.joinGroup(founding, GroupMembership.Config.defaults(), List.of());
        founder.startTicking(Duration.ofMillis(200));

        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + freePort());
        properties.setTickMillis(200);
        AgentSpacesProperties.Group group = group("fleet", null, seedPort, "tasks");
        group.setJoin(GroupFounding.URI_PREFIX + groupId.value());
        properties.setGroups(List.of(group));

        Wired joiner = wire(properties);
        AgentSpaces.GroupContext fleet = joiner.spaces().group("fleet");
        assertThat(fleet.id()).isEqualTo(groupId);
        assertThat(fleet.runtime().founding()).hasValueSatisfying(fetched -> {
            assertThat(GroupFounding.verify(fetched)).isTrue();
            assertThat(fetched.advertisement().issuer()).isEqualTo(founderIdentity.peerId());
        });
        assertThat(fleet.spaceNames()).containsExactly("tasks");

        // join and founding identify the group differently: never both.
        AgentSpacesProperties both = new AgentSpacesProperties();
        AgentSpacesProperties.Group conflicting = group("fleet", "literal", seedPort, "tasks");
        conflicting.setJoin(GroupFounding.URI_PREFIX + groupId.value());
        both.setGroups(List.of(conflicting));
        PeerIdentity identity = autoConfig.agentSpacesIdentity(both);
        PeerNode node = autoConfig.agentSpacesNode(both, identity);
        closeables.add(node);
        assertThatThrownBy(() -> autoConfig.agentSpaces(both, node, identity))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("join").hasMessageContaining("founding");
        assertThat(AgentSpacesAutoConfiguration.groupIdOf("aspace://" + groupId.value()
                + "/agent/x")).isEqualTo(groupId);
        assertThat(AgentSpacesAutoConfiguration.groupIdOf(groupId.value())).isEqualTo(groupId);
    }

    /** SPEC §10.1: agentspaces.multicast.enabled turns on the LAN bootstrap beacon at node construction. */
    @Test
    void multicastCanBeEnabledFromProperties() throws Exception {
        AgentSpacesProperties properties = fleetProperties(freePort(), 0);
        properties.getMulticast().setEnabled(true);
        properties.getMulticast().setGroup("239.255.42.99:" + freePort());
        properties.getMulticast().setIntervalMillis(500);
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        closeables.add(node);
        assertThat(node).isNotNull();
        node.tick(); // the beacon announces on the tick without error

        assertThat(AgentSpacesAutoConfiguration.multicastGroup("239.255.42.99:7787"))
                .satisfies(address -> {
                    assertThat(address.getPort()).isEqualTo(7787);
                    assertThat(address.getHostString()).isEqualTo("239.255.42.99");
                });
        assertThatThrownBy(() -> AgentSpacesAutoConfiguration.multicastGroup("no-port"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new AgentSpacesProperties().getMulticast().isEnabled())
                .as("off by default").isFalse();
    }

    /** SPEC §7.5/§6.1: every auto-configured space publishes its SpaceAdvertisement on creation. */
    @Test
    void spacesAdvertiseThemselvesOnStartup() throws Exception {
        Wired app = wire(fleetProperties(freePort(), 0));
        List<SpaceAdvertisement> ads = app.spaces().group("fleet").discovery()
                .find(SpaceAdvertisement.class, ad -> true);
        assertThat(ads).extracting(SpaceAdvertisement::spaceName)
                .contains("tasks", "findings");
        assertThat(ads).allSatisfy(ad -> {
            assertThat(ad.issuer()).isEqualTo(app.node().peerId());
            assertThat(ad.admission()).isEqualTo(SpaceAdvertisement.Admission.GROUP);
            assertThat(ad.strategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
        });
    }

    /** SPEC §7.5: an allowlist-admitted space accepts its listed writer and refuses an unlisted one. */
    @Test
    @Timeout(60)
    void anAllowlistedSpaceRefusesUnlistedWriters(@org.junit.jupiter.api.io.TempDir
                                                  java.nio.file.Path keystore) throws Exception {
        AgentSpacesProperties stewardProps = fleetProperties(freePort(), 0);
        stewardProps.setKeystore(keystore.toString());
        String steward = autoConfig.agentSpacesIdentity(stewardProps).agent("app").encoded();
        AgentSpacesProperties.SpaceDef control = new AgentSpacesProperties.SpaceDef();
        control.setName("control");
        control.setAdmission("allowlist");
        control.setAllowedAgents(List.of(steward));
        stewardProps.getGroups().get(0).setSpaces(List.of(control));

        Wired listed = wire(stewardProps);
        listed.spaces().group("fleet").space("control")
                .write(new TaskEntry("allowed", 1), Lease.of(Duration.ofMinutes(10)));
        assertThat(listed.spaces().group("fleet").discovery().find(SpaceAdvertisement.class,
                ad -> "control".equals(ad.spaceName())).get(0).admission())
                .isEqualTo(SpaceAdvertisement.Admission.ALLOWLIST);

        AgentSpacesProperties otherProps = fleetProperties(freePort(), 0);
        otherProps.getGroups().get(0).setSpaces(List.of(control));
        Wired unlisted = wire(otherProps);
        assertThatThrownBy(() -> unlisted.spaces().group("fleet").space("control")
                .write(new TaskEntry("refused", 1), Lease.of(Duration.ofMinutes(10))))
                .isInstanceOf(SpaceAdmissionException.class);

        AgentSpacesProperties empty = fleetProperties(freePort(), 0);
        AgentSpacesProperties.SpaceDef unlistedDef = new AgentSpacesProperties.SpaceDef();
        unlistedDef.setName("control");
        unlistedDef.setAdmission("allowlist");
        empty.getGroups().get(0).setSpaces(List.of(unlistedDef));
        PeerIdentity identity = autoConfig.agentSpacesIdentity(empty);
        PeerNode node = autoConfig.agentSpacesNode(empty, identity);
        closeables.add(node);
        assertThatThrownBy(() -> autoConfig.agentSpaces(empty, node, identity))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("allowed-agents");
    }
}
