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
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor;
import ai.badmonkey.agentspaces.capabilities.learn.GossipLearning;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.api.spi.Embedder;
import org.springframework.beans.factory.ObjectProvider;
import ai.badmonkey.agentspaces.capabilities.semantic.HashingEmbedder;
import ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.FileKeystore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupFounding;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.space.replicated.CredentialIndex;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.SpaceAdmission;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Boots the AgentSpaces fabric from {@link AgentSpacesProperties} (spec §10.1):
 * one {@link PeerIdentity} (keystore-backed or generated), one {@link PeerNode}
 * listening or dial-only (with the optional multicast beacon), one joined group
 * per configuration entry — founded locally from a string or joined by
 * self-certifying GroupId URI — with rendezvous-sized discovery, a block
 * exchange for bulk payloads (§6.1a), advertised and admission-controlled
 * replicated spaces (§7.5) registered with their writer ids, and the shipped
 * capability providers on a {@link CapabilityRuntime} (§10.5); all navigable
 * through the {@link AgentSpaces} fluent facade, with
 * {@link AgentSpacesLifecycle} driving ticks and card/capability refresh and
 * {@link AgentSpacesBeanPostProcessor} binding annotated beans.
 *
 * <p>The security profile also selects the fleet's {@link Authorizer}
 * (TODO-EFG §4, TODO item 6): the {@link Authorizers} bean holds a
 * membership-rooted authorizer per group under {@code dev-local}/{@code mtls}
 * and one {@code OidcAuthorizer} under {@code mtls-oidc}/{@code zero-trust};
 * it is threaded into the key-wrap provider, the {@code admission: authorizer}
 * spaces, and the {@link DirectiveGates} bean, and exposed as the
 * {@link Authorizer} bean for the enforcement points an application wires
 * itself.
 *
 * <p>Every bean is {@code @ConditionalOnMissingBean}, so an application takes
 * over any piece (its own identity, its own node) by declaring the bean
 * itself; the rest of the wiring follows.
 */
@AutoConfiguration
@EnableConfigurationProperties(AgentSpacesProperties.class)
public class AgentSpacesAutoConfiguration {

    private static final System.Logger LOG =
            System.getLogger(AgentSpacesAutoConfiguration.class.getName());

    /**
     * The profile's authorizer selection (TODO-EFG §4): membership-rooted per
     * group under {@code dev-local} and {@code mtls}, the identity provider's
     * under {@code mtls-oidc} and {@code zero-trust}, where
     * {@code agentspaces.security.oidc.{issuer, audience, jwks-url}} are all
     * required and startup fails fast naming the missing one.
     *
     * @param properties the starter configuration
     * @param identity   the peer identity
     * @return the holder
     */
    public Authorizers agentSpacesAuthorizers(AgentSpacesProperties properties,
                                              PeerIdentity identity) {
        return agentSpacesAuthorizers(properties, identity, InstantSource.system());
    }

    /**
     * The authorizer selection as the bean, on the context's clock.
     *
     * @param properties the starter configuration
     * @param identity   the peer identity
     * @param clock      the fabric's clock
     * @return the holder
     */
    @Bean
    @ConditionalOnMissingBean
    public Authorizers agentSpacesAuthorizers(AgentSpacesProperties properties,
                                              PeerIdentity identity, InstantSource clock) {
        return new Authorizers(properties, identity, clock);
    }

    /**
     * The fabric's clock (review M-8 of TODO-9-10-11): one {@link InstantSource}
     * that the facade, its agents, the authorizers, and the A2A task binding
     * share, so an application or test can substitute its own. The peer node
     * keeps the system clock for its protocol rounds.
     *
     * @return the clock
     */
    @Bean
    @ConditionalOnMissingBean
    public InstantSource agentSpacesClock() {
        return InstantSource.system();
    }

    /**
     * The peer identity: loaded from the configured keystore directory, or
     * generated per process run when none is configured.
     *
     * @param properties the starter configuration
     * @return the identity
     */
    @Bean
    @ConditionalOnMissingBean
    public PeerIdentity agentSpacesIdentity(AgentSpacesProperties properties) {
        // Apply the signature-provider selection before any key material is
        // touched; this is the fabric's first cryptographic act.
        String signatureProvider = properties.getCrypto().getSignatureProvider();
        if (signatureProvider != null && !signatureProvider.isBlank()) {
            ai.badmonkey.agentspaces.common.crypto.Ed25519.select(signatureProvider.trim());
        }
        String keystore = properties.getKeystore();
        if (keystore == null || keystore.isBlank()) {
            return PeerIdentity.generate();
        }
        return FileKeystore.loadOrCreate(Path.of(keystore));
    }

    /**
     * The peer node: listening on the configured bind address, or dial-only
     * (the NAT-restricted posture, spec §5.4) when no bind is configured.
     *
     * @param properties the starter configuration
     * @param identity   the peer identity
     * @return the node
     */
    @Bean
    @ConditionalOnMissingBean
    public PeerNode agentSpacesNode(AgentSpacesProperties properties, PeerIdentity identity) {
        Set<PeerAdvertisement.PeerRole> roles = rolesOf(properties);
        // The security profile decides the posture (remediation plan §4);
        // explicit transport properties are deliberate deviations from it.
        ai.badmonkey.agentspaces.api.security.SecurityProfile profile =
                ai.badmonkey.agentspaces.api.security.SecurityProfile.fromName(
                        properties.getSecurity().getProfile());
        String channelAuthOverride = properties.getTransport().getChannelAuth();
        PeerNode.ChannelAuth channelAuth = channelAuthOverride != null
                ? PeerNode.ChannelAuth.valueOf(channelAuthOverride.trim().toUpperCase())
                : (profile.envelopeSignatureEveryHop()
                        ? PeerNode.ChannelAuth.SIGNED : PeerNode.ChannelAuth.ATTESTED);
        boolean tlsEnabled = tlsEnabled(properties);
        // gate2-review G2-1: the CA channel mode is a configured posture, not
        // only a programmatic one. It needs a transport that can attest, so a
        // fleet that asks for it without TLS is misconfigured and says so at
        // startup rather than serving every frame unattested.
        boolean requireAttestation = properties.getTransport().getTls().isRequireAttestation();
        if (requireAttestation && !tlsEnabled) {
            throw new IllegalStateException(
                    "agentspaces.transport.tls.require-attestation needs an attesting "
                            + "transport: enable agentspaces.transport.tls.enabled, or "
                            + "select a security profile whose transport is TLS");
        }
        PeerNode.Builder builder = PeerNode.builder(identity)
                .roles(roles)
                .channelAuth(channelAuth)
                .requireAttestation(requireAttestation);
        // SPEC §5.6 v0.1.13 (item 9): the CA trust is built before the node, so
        // the node judges presented chains by the same refreshable trust the
        // transport attests with, and may accept CA-rooted revocations.
        AgentSpacesProperties.Transport.Tls tls = properties.getTransport().getTls();
        java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> caTrust =
                tlsEnabled ? caTrust(tls) : null;
        String validator = tls.getRevocationValidator() == null ? "founder"
                : tls.getRevocationValidator().trim().toLowerCase();
        if (!validator.equals("founder") && !validator.equals("founder-or-ca")) {
            throw new IllegalStateException("agentspaces.transport.tls.revocation-validator must be"
                    + " 'founder' or 'founder-or-ca', not '" + tls.getRevocationValidator() + "'");
        }
        if (caTrust != null) {
            builder.channelTrust(caTrust);
        }
        if (validator.equals("founder-or-ca")) {
            if (caTrust == null) {
                throw new IllegalStateException("agentspaces.transport.tls.revocation-validator:"
                        + " founder-or-ca needs the enterprise-CA mode: enable TLS and set"
                        + " agentspaces.transport.tls.trust-store");
            }
            builder.revocationValidator(
                    ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator.anyOf(
                            ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator
                                    .founderRooted(),
                            ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator
                                    .caRooted(caTrust)));
        }
        AgentSpacesProperties.Multicast multicast = properties.getMulticast();
        if (multicast.isEnabled()) {
            builder.multicast(multicastGroup(multicast.getGroup()),
                    Duration.ofMillis(multicast.getIntervalMillis()));
        }
        PeerNode node = builder.build();
        Transport transport;
        if (tlsEnabled) {
            try {
                transport = tlsTransport(identity, tls, caTrust);
            } catch (java.security.GeneralSecurityException e) {
                throw new IllegalStateException(
                        "cannot create the TLS channel credential", e);
            }
            // Plain TCP stays registered for dialing peers that advertise it,
            // except in the attested mode: a plaintext channel attests nobody,
            // so dialing one would evict the honest peer at the far end
            // (gate2-review G2-1).
            if (!requireAttestation) {
                node.transport(new TcpTransport());
            }
        } else {
            transport = new TcpTransport();
        }
        String bind = properties.getBind();
        if (bind == null || bind.isBlank()) {
            node.transport(transport);
        } else {
            try {
                node.listen(transport, bind.trim());
            } catch (IOException e) {
                throw new UncheckedIOException("cannot bind AgentSpaces node to '"
                        + bind + "'", e);
            }
        }
        return node;
    }

    /**
     * The enterprise-CA trust when a trust store is configured (remediation
     * plan §3), its CRL files re-read every {@code crl-refresh} (SPEC §5.6,
     * v0.1.13); null in the self-signed identity-endorsed mode.
     */
    private static java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> caTrust(
            AgentSpacesProperties.Transport.Tls tls) {
        if (tls.getTrustStore() == null || tls.getTrustStore().isBlank()) {
            return null;
        }
        try {
            java.util.List<java.security.cert.X509Certificate> authorities =
                    certificatesIn(java.nio.file.Path.of(tls.getTrustStore().trim()),
                            tls.getTrustStorePassword());
            java.util.List<java.nio.file.Path> crlFiles = new java.util.ArrayList<>();
            for (String path : tls.getCrls()) {
                crlFiles.add(java.nio.file.Path.of(path.trim()));
            }
            return new ai.badmonkey.agentspaces.identity.ReloadingChannelTrust(authorities, crlFiles,
                    tls.getCrlRefresh() == null ? Duration.ofMinutes(5) : tls.getCrlRefresh(),
                    tls.isRevocationStrict(), InstantSource.system());
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("cannot load the TLS trust store or CRLs", e);
        }
    }

    /**
     * Builds the TLS transport: the enterprise-CA mode over the refreshable
     * trust when one is configured, the self-signed identity-endorsed mode
     * otherwise.
     */
    private static Transport tlsTransport(PeerIdentity identity,
                                          AgentSpacesProperties.Transport.Tls tls,
                                          java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> trust)
            throws java.security.GeneralSecurityException {
        if (trust == null) {
            return new TlsTcpTransport(identity);
        }
        ai.badmonkey.agentspaces.identity.ServingCredential serving = null;
        if (tls.getKeyStore() != null && !tls.getKeyStore().isBlank()) {
            serving = ai.badmonkey.agentspaces.identity.ServingCredential.fromKeyStore(
                    java.nio.file.Path.of(tls.getKeyStore().trim()),
                    tls.getKeyStorePassword() == null
                            ? new char[0] : tls.getKeyStorePassword().toCharArray());
        }
        return TlsTcpTransport.withRefreshableTrust(identity, serving, trust);
    }

    private static java.util.List<java.security.cert.X509Certificate> certificatesIn(
            java.nio.file.Path path, String password)
            throws java.security.GeneralSecurityException {
        try (java.io.InputStream in = java.nio.file.Files.newInputStream(path)) {
            java.security.KeyStore store = java.security.KeyStore.getInstance(
                    path.getFileName().toString().endsWith(".jks") ? "JKS" : "PKCS12");
            store.load(in, password == null ? null : password.toCharArray());
            java.util.List<java.security.cert.X509Certificate> certificates =
                    new java.util.ArrayList<>();
            java.util.Enumeration<String> aliases = store.aliases();
            while (aliases.hasMoreElements()) {
                java.security.cert.Certificate certificate =
                        store.getCertificate(aliases.nextElement());
                if (certificate instanceof java.security.cert.X509Certificate x509) {
                    certificates.add(x509);
                }
            }
            if (certificates.isEmpty()) {
                throw new java.security.GeneralSecurityException(
                        "no certificates in trust store " + path);
            }
            return certificates;
        } catch (java.io.IOException e) {
            throw new java.security.GeneralSecurityException(
                    "cannot read trust store " + path, e);
        }
    }

    /** The effective TLS decision: the profile's posture unless overridden. */
    private static boolean tlsEnabled(AgentSpacesProperties properties) {
        Boolean override = properties.getTransport().getTls().getEnabled();
        return override != null ? override
                : ai.badmonkey.agentspaces.api.security.SecurityProfile
                        .fromName(properties.getSecurity().getProfile()).tlsTransport();
    }

    /**
     * The fluent facade over every configured group and space (spec §10.2).
     * Per group: join (by founding string or by verified GroupId URI, §10.1),
     * discovery sized for the node's roles, one {@link BlockExchange} so bulk
     * payloads travel content-addressed (§6.1a), every configured space built
     * over it, advertised, admission-controlled, and registered with its
     * writer id (§7.5), then the enabled capability providers on the group's
     * {@link CapabilityRuntime} (§10.5).
     *
     * @param properties the starter configuration
     * @param node       the peer node
     * @param identity   the peer identity
     * @return the facade, groups joined and spaces attached
     * @see #agentSpaces(AgentSpacesProperties, PeerNode, PeerIdentity, Authorizers)
     */
    public AgentSpaces agentSpaces(AgentSpacesProperties properties, PeerNode node,
                                   PeerIdentity identity) {
        return agentSpaces(properties, node, identity,
                agentSpacesAuthorizers(properties, identity));
    }

    /**
     * The fluent facade, with the profile's {@link Authorizers} threaded in
     * (TODO-EFG §4): each joined group's authorizer is rooted in its
     * membership (or the shared OIDC authorizer), the key-wrap provider serves
     * the content key under {@code KEY_HOLDER}, spaces configured with
     * {@code admission: authorizer} ask {@code SPACE_WRITE}/{@code SPACE_TAKE},
     * and under the OIDC profiles the node's credential-hint listener feeds
     * members' tokens to the authorizer and this node's own token is presented.
     * The three-argument overload builds a fresh holder and is the
     * programmatic convenience; under Spring this is the bean.
     *
     * @param properties  the starter configuration
     * @param node        the peer node
     * @param identity    the peer identity
     * @param authorizers the profile's authorizer selection
     * @return the facade, groups joined and spaces attached
     */
    /**
     * The agent identity factory {@code agentspaces.identity.agent-keys} selects
     * (QA4 A4-7 phase 3): {@code peer} binds agents under the peer key,
     * {@code subordinate} gives each a peer-certified key of its own.
     */
    static java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> agentIdentities(
            AgentSpacesProperties properties, PeerIdentity identity) {
        return agentIdentities(properties, identity, InstantSource.system());
    }

    /**
     * As {@link #agentIdentities(AgentSpacesProperties, PeerIdentity)}, on the
     * fabric's clock: {@code subordinate} agents get renewing certificates
     * (SPEC §4.2, v0.1.13) of {@code agentspaces.identity.agent-certificate-ttl}
     * (default 24h), re-issued at half-life, so they run indefinitely.
     */
    static java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> agentIdentities(
            AgentSpacesProperties properties, PeerIdentity identity, InstantSource clock) {
        String mode = properties.getIdentity().getAgentKeys();
        if (mode == null || mode.isBlank() || "peer".equalsIgnoreCase(mode.trim())) {
            String keystore = properties.getIdentity().getAgentKeystore();
            if (keystore != null && !keystore.isBlank()) {
                throw new IllegalStateException("agentspaces.identity.agent-keystore is set but"
                        + " agentspaces.identity.agent-keys is 'peer': peer-signed agents have no"
                        + " keys to persist; set agent-keys: subordinate or remove agent-keystore");
            }
            return identity::agentIdentity;
        }
        String keystore = properties.getIdentity().getAgentKeystore();
        boolean persisted = keystore != null && !keystore.isBlank();
        if ("subordinate".equalsIgnoreCase(mode.trim())) {
            java.time.Duration ttl = properties.getIdentity().getAgentCertificateTtl();
            if (persisted) {
                // SPEC §4.2 v0.1.13 (B5): each agent's key persists in the keystore,
                // so the agent keeps its attested identity across restarts.
                ai.badmonkey.agentspaces.identity.AgentKeystore agents =
                        ai.badmonkey.agentspaces.identity.AgentKeystore.at(
                                java.nio.file.Path.of(keystore.trim()));
                // The X25519 pair persists too, and is certified alongside, so key-wrap
                // holders can seal content keys to the agent itself (SPEC §11a.2, D5).
                return name -> identity.renewingSubordinate(name,
                        agents.signingKeys(identity.peerId(), name),
                        agents.encryptionKeys(identity.peerId(), name), ttl, clock);
            }
            return name -> identity.renewingSubordinate(name,
                    ai.badmonkey.agentspaces.common.crypto.Ed25519.generate(),
                    ai.badmonkey.agentspaces.common.crypto.X25519.generate(), ttl, clock);
        }
        throw new IllegalStateException("agentspaces.identity.agent-keys must be 'peer' or"
                + " 'subordinate', not '" + mode + "'");
    }

    public AgentSpaces agentSpaces(AgentSpacesProperties properties, PeerNode node,
                                   PeerIdentity identity, Authorizers authorizers) {
        return agentSpaces(properties, node, identity, authorizers, new HashingEmbedder());
    }

    /**
     * The embedder semantic discovery ranks with. The default is the model-free
     * {@link HashingEmbedder}; declare an {@link Embedder} bean (agentspaces-springai
     * provides one over a Spring AI {@code EmbeddingModel}) to rank with a model.
     * Every peer in a group should use the same one (SPEC §8).
     *
     * @return the embedder
     */
    @Bean
    @ConditionalOnMissingBean
    public Embedder agentSpacesEmbedder() {
        return new HashingEmbedder();
    }

    /**
     * The fluent facade as the bean: the four-argument form, with the
     * application's {@link Embedder} behind semantic discovery.
     *
     * @param properties  the starter configuration
     * @param node        the peer node
     * @param identity    the peer identity
     * @param authorizers the profile's authorizer selection
     * @param embedder    the embedder semantic discovery ranks with
     * @return the facade, groups joined and spaces attached
     */
    public AgentSpaces agentSpaces(AgentSpacesProperties properties, PeerNode node,
                                   PeerIdentity identity, Authorizers authorizers, Embedder embedder) {
        return agentSpaces(properties, node, identity, authorizers, embedder, InstantSource.system());
    }

    /**
     * The fluent facade as the bean: the five-argument form on the context's clock.
     *
     * @param properties  the starter configuration
     * @param node        the peer node
     * @param identity    the peer identity
     * @param authorizers the profile's authorizer selection
     * @param embedder    the embedder semantic discovery ranks with
     * @param clock       the fabric's clock
     * @return the facade, groups joined and spaces attached
     */
    public AgentSpaces agentSpaces(AgentSpacesProperties properties, PeerNode node,
                                   PeerIdentity identity, Authorizers authorizers, Embedder embedder,
                                   InstantSource clock) {
        return agentSpaces(properties, node, identity, authorizers, embedder, clock,
                agentSpacesAgentIdentities(properties, identity, clock));
    }

    /**
     * The agent identity factory {@code agentspaces.identity.*} selects (SPEC
     * §4.2, v0.1.13): peer-signed agents, renewing subordinate keys, or
     * renewing keys persisted in {@code agent-keystore}. Declare an
     * {@link ai.badmonkey.agentspaces.agent.AgentIdentityFactory} bean to supply
     * identities another way (an HSM, a secrets manager).
     *
     * @param properties the starter configuration
     * @param identity   the peer identity
     * @param clock      the fabric's clock
     * @return the factory
     */
    @Bean
    @ConditionalOnMissingBean
    public ai.badmonkey.agentspaces.agent.AgentIdentityFactory agentSpacesAgentIdentities(
            AgentSpacesProperties properties, PeerIdentity identity, InstantSource clock) {
        java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> chosen =
                agentIdentities(properties, identity, clock);
        return chosen::apply;
    }

    /**
     * The fluent facade as the bean, with the agent identity factory.
     *
     * @param properties  the starter configuration
     * @param node        the peer node
     * @param identity    the peer identity
     * @param authorizers the profile's authorizer selection
     * @param embedder    the embedder semantic discovery ranks with
     * @param clock       the fabric's clock
     * @param agents      the agent identity factory
     * @return the facade, groups joined and spaces attached
     */
    @Bean
    @ConditionalOnMissingBean
    public AgentSpaces agentSpaces(AgentSpacesProperties properties, PeerNode node,
                                   PeerIdentity identity, Authorizers authorizers, Embedder embedder,
                                   InstantSource clock,
                                   ai.badmonkey.agentspaces.agent.AgentIdentityFactory agents) {
        AgentSpaces spaces = new AgentSpaces(identity, clock, agents);
        CborCodec codec = CborCodec.defaultCodec();
        Set<PeerAdvertisement.PeerRole> roles = rolesOf(properties);
        AgentId writer = identity.agent("app");
        // The token channel first, so members admitted during the joins below
        // have their hints ingested (TODO-EFG §4).
        authorizers.attach(node);
        for (AgentSpacesProperties.Group groupConfig : properties.getGroups()) {
            GroupRuntime runtime = join(node, groupConfig,
                    seedEndpoints(groupConfig.getSeeds(),
                            tlsEnabled(properties) ? "tls" : "tcp"));
            authorizers.register(groupConfig.getName(), runtime);
            Authorizer authorizer = authorizers.forGroup(groupConfig.getName());
            DiscoveryService discovery = DiscoveryService.create(runtime, codec,
                    identity.peerId(), clock, roles);
            AgentSpaces.GroupContext context = spaces.register(
                    groupConfig.getName(), runtime.id(), runtime, discovery);
            BlockExchange blocks = new BlockExchange(runtime, codec);
            GroupKey contentKey = contentKeyOf(groupConfig);
            // SPEC §11a.3 v0.1.13: one ring per group, shared by its spaces, so a
            // rotation reaches every space at once; persisted with the keystore.
            GroupKeyRing ring = contentKeyRing(properties, groupConfig, runtime, contentKey, codec);
            for (AgentSpacesProperties.SpaceDef spaceDef : groupConfig.getSpaces()) {
                context.space(spaceDef.getName(),
                        buildSpace(runtime, identity, spaceDef, blocks, discovery, ring,
                                authorizer, clock),
                        writer);
            }
            if (properties.getCapabilities().isEnabled()) {
                registerCapabilities(properties.getCapabilities(), context, runtime,
                        discovery, identity, codec, clock, blocks, ring, groupConfig,
                        authorizer, embedder, peerEncryptionKeys(properties));
            }
        }
        return spaces;
    }

    /**
     * The selected {@link Authorizer} as a bean (TODO-EFG §4): the
     * {@code OidcAuthorizer} under the OIDC profiles; under the membership
     * profiles the sole group's {@code MembershipAuthorizer}, or a union over
     * the groups when several are configured ({@link Authorizers#primary()};
     * use {@link Authorizers#forGroup(String)} for one group's). Pass it to
     * {@code RaftLog}, {@code DataQueryClient}, and {@code ConnectorRuntime},
     * which the starter does not wire itself.
     *
     * @param authorizers the holder
     * @param spaces      the facade (ordering: the groups must have joined)
     * @return the authorizer
     */
    @Bean
    @ConditionalOnMissingBean
    public Authorizer agentSpacesAuthorizer(Authorizers authorizers, AgentSpaces spaces) {
        return authorizers.primary();
    }

    /**
     * The {@link ai.badmonkey.agentspaces.console.DirectiveGate} factory: workers attach gates
     * that obey whichever peers the group's authorizer permits
     * {@code DIRECTIVE_ISSUER}, without naming the console's PeerID.
     *
     * @param authorizers the holder
     * @param spaces      the facade
     * @return the factory
     */
    @Bean
    @ConditionalOnMissingBean
    public DirectiveGates agentSpacesDirectiveGates(Authorizers authorizers, AgentSpaces spaces) {
        return new DirectiveGates(authorizers, spaces);
    }

    /**
     * Wires the shipped capability providers into one group (spec §10.5):
     * an explicit {@link CapabilityRuntime} registered through
     * {@link AgentSpaces#register(String, CapabilityRuntime)}, then each
     * enabled provider through the context so typed clients resolve locally.
     */
    private static void registerCapabilities(AgentSpacesProperties.Capabilities config,
                                             AgentSpaces.GroupContext context,
                                             GroupRuntime runtime, DiscoveryService discovery,
                                             PeerIdentity identity, CborCodec codec,
                                             InstantSource clock, BlockExchange blocks,
                                             GroupKeyRing contentKey,
                                             AgentSpacesProperties.Group groupConfig,
                                             Authorizer authorizer,
                                             Embedder embedder,
                                             java.security.KeyPair encryptionKeys) {
        if (config.isOrderedLog()) {
            throw new IllegalStateException("agentspaces.capabilities.ordered-log=true: the"
                    + " ordered log runs over a fixed member set the starter cannot infer;"
                    + " register a @ProvidesCapability bean wrapping RaftLog with the"
                    + " members instead");
        }
        AgentSpaces.GroupContext group = context.capabilities(
                new CapabilityRuntime(runtime, discovery, identity));
        CapabilityPipes pipes = group.pipes();
        if (config.isAggregate()) {
            group.provide(new PushSumAggregate(pipes, runtime.sampler(), identity.peerId(),
                    codec, clock));
        }
        if (config.isVote()) {
            String votesName = config.getVotesSpace() == null
                    || config.getVotesSpace().isBlank() ? "votes" : config.getVotesSpace().trim();
            ai.badmonkey.agentspaces.api.space.Space votes;
            if (group.spaceNames().contains(votesName)) {
                votes = group.space(votesName);
            } else {
                // A private space: not registered with the group, so it neither
                // shows in spaceNames() nor disturbs sole-space inference.
                AgentSpacesProperties.SpaceDef def = new AgentSpacesProperties.SpaceDef();
                def.setName(votesName);
                votes = buildSpace(runtime, identity, def, blocks, discovery, contentKey,
                        authorizer, clock);
            }
            // gate2-review G2-3: the profile's authorizer narrows the electorate,
            // so an identity provider names QUORUM voters as it names Raft voters.
            group.provide(new VoteCapability(votes, identity.agent("app"), identity.peerId(),
                    clock, VoteCapability.ANY_AUTHENTICATED_ISSUER, authorizer));
        }
        if (config.isGossipLearn()) {
            group.provide(new GossipLearning(pipes, runtime.sampler(), identity.peerId(),
                    codec, clock));
        }
        if (config.isSemanticDiscovery()) {
            group.provide(new SemanticDiscovery(pipes, runtime, discovery, identity.peerId(),
                    codec, clock, embedder));
        }
        if (config.isKeyWrap()) {
            // The keystore's X25519 pair when one is configured, so keys sealed
            // to this peer stay openable across restarts.
            GroupKeyDistributor keys = encryptionKeys == null
                    ? new GroupKeyDistributor(pipes, identity.peerId(), codec, clock)
                    : new GroupKeyDistributor(pipes, identity.peerId(), codec, clock, encryptionKeys);
            if (contentKey != null) {
                // TODO-EFG §4: the profile's authorizer decides KEY_HOLDER (and
                // KEY_ROTATE) in the scope of the group id (membership plus
                // grants, or the identity provider's scopes).
                keys.serve(contentKey, authorizer, runtime.id());
                keys.follow(contentKey, discovery);
                AgentSpacesProperties.ContentKeyRotation rotation = groupConfig.getContentKeyRotation();
                if (rotation != null && rotation.getRotateEvery() != null) {
                    if (!keys.rotator(identity.peerId())) {
                        throw new IllegalStateException("group '" + groupConfig.getName()
                                + "' sets content-key-rotation.rotate-every but this peer may not"
                                + " rotate: it is not the founder and holds no key-rotator grant");
                    }
                    Duration cutoverDelay = rotation.getCutoverDelay() != null ? rotation.getCutoverDelay()
                            : runtime.advertisement().gossip().period().multipliedBy(2).plusSeconds(30);
                    keys.autoRotate(identity, rotation.getRotateEvery(), cutoverDelay);
                }
            }
            group.provide(keys);
        } else if (contentKey != null && groupConfig.getContentKeyRotation() != null
                && groupConfig.getContentKeyRotation().getRotateEvery() != null) {
            throw new IllegalStateException("group '" + groupConfig.getName() + "' sets"
                    + " content-key-rotation.rotate-every but the key-wrap capability is off;"
                    + " members could never fetch the epochs it mints");
        }
    }

    /**
     * Joins one configured group: by verified founding advertisement fetched
     * from the seeds when {@code join} names the group (spec §10.1, §4.4), by
     * a locally derived founding string otherwise. The two are mutually
     * exclusive because they identify the group differently.
     */
    private static GroupRuntime join(PeerNode node, AgentSpacesProperties.Group config,
                                     List<PeerAdvertisement.Endpoint> seeds) {
        boolean hasJoin = config.getJoin() != null && !config.getJoin().isBlank();
        boolean hasFounding = config.getFounding() != null && !config.getFounding().isBlank();
        if (hasJoin && hasFounding) {
            throw new IllegalStateException("group '" + config.getName() + "' sets both"
                    + " 'join' and 'founding'; a group is either joined by its GroupId URI"
                    + " or founded locally from a string, not both");
        }
        if (!hasJoin) {
            return node.joinGroup(advertisementFor(config), GroupMembership.Config.defaults(),
                    seeds);
        }
        if (seeds.isEmpty()) {
            throw new IllegalStateException("group '" + config.getName() + "' joins by"
                    + " GroupId URI but configures no seeds to fetch the founding"
                    + " advertisement from");
        }
        GroupId groupId = groupIdOf(config.getJoin());
        try {
            return node.joinGroup(groupId, GroupMembership.Config.defaults(), seeds,
                    Duration.ofMillis(config.getJoinTimeoutMillis()));
        } catch (TimeoutException e) {
            throw new IllegalStateException("group '" + config.getName() + "': no seed"
                    + " served a verified founding advertisement for " + groupId
                    + " within " + config.getJoinTimeoutMillis() + " ms; seeds: " + seeds, e);
        }
    }

    /** Parses {@code aspace://<groupId>} or a bare GroupId (spec §10.1). */
    static GroupId groupIdOf(String join) {
        String value = join.trim();
        if (value.startsWith(GroupFounding.URI_PREFIX)) {
            value = value.substring(GroupFounding.URI_PREFIX.length());
        }
        int slash = value.indexOf('/');
        if (slash >= 0) {
            value = value.substring(0, slash);
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("join URI carries no GroupId: '" + join + "'");
        }
        return GroupId.of(value);
    }

    /** Parses the multicast beacon group ({@code host:port}). */
    static InetSocketAddress multicastGroup(String group) {
        if (group == null || group.isBlank()) {
            throw new IllegalArgumentException("agentspaces.multicast.group is blank");
        }
        String value = group.trim();
        int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon == value.length() - 1) {
            throw new IllegalArgumentException("agentspaces.multicast.group must be"
                    + " host:port, got '" + group + "'");
        }
        try {
            return new InetSocketAddress(value.substring(0, colon),
                    Integer.parseInt(value.substring(colon + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("agentspaces.multicast.group port is not a"
                    + " number: '" + group + "'", e);
        }
    }

    private static Set<PeerAdvertisement.PeerRole> rolesOf(AgentSpacesProperties properties) {
        Set<PeerAdvertisement.PeerRole> roles = EnumSet.noneOf(PeerAdvertisement.PeerRole.class);
        for (String role : properties.getRoles()) {
            roles.add(PeerAdvertisement.PeerRole.valueOf(role.trim().toUpperCase()));
        }
        return roles;
    }

    /**
     * The group's content-key ring (SPEC §11a.3, v0.1.13), epoch 0 being the
     * configured key: restored from and saved to
     * {@code <keystore>/content-keys/<group>.ring} when a keystore is set, so
     * rotated epochs survive restarts. Null when the group is unencrypted.
     */
    private static GroupKeyRing contentKeyRing(AgentSpacesProperties properties,
                                               AgentSpacesProperties.Group config,
                                               GroupRuntime runtime, GroupKey contentKey,
                                               CborCodec codec) {
        AgentSpacesProperties.ContentKeyRotation rotation = config.getContentKeyRotation();
        if (contentKey == null) {
            if (rotation != null && rotation.getRotateEvery() != null) {
                throw new IllegalStateException("group '" + config.getName() + "' sets"
                        + " content-key-rotation.rotate-every but no content-key to rotate");
            }
            return null;
        }
        GroupKeyRing ring = GroupKeyRing.of(contentKey);
        if (rotation != null && rotation.getWriterGrace() != null) {
            ring.writerGrace(rotation.getWriterGrace());
        }
        java.security.KeyPair encryption = peerEncryptionKeys(properties);
        if (encryption != null) {
            new ai.badmonkey.agentspaces.capabilities.keywrap.RingStore(
                    Path.of(properties.getKeystore().trim()), runtime.id(), encryption, codec)
                    .attach(ring);
        } else if (rotation != null && rotation.getRotateEvery() != null) {
            LOG.log(System.Logger.Level.WARNING, "group ''{0}'' rotates its content key but"
                    + " agentspaces.keystore is unset: epochs this peer mints are lost on restart"
                    + " unless another member holds them", config.getName());
        }
        return ring;
    }

    /** The peer keystore's X25519 pair, or null without a keystore. */
    private static java.security.KeyPair peerEncryptionKeys(AgentSpacesProperties properties) {
        String keystore = properties.getKeystore();
        return keystore == null || keystore.isBlank() ? null
                : FileKeystore.encryptionKeys(Path.of(keystore.trim()));
    }

    private static GroupKey contentKeyOf(AgentSpacesProperties.Group config) {
        String encoded = config.getContentKey();
        if (encoded == null || encoded.isBlank()) {
            return null;
        }
        try {
            return GroupKey.fromBytes(Base64.getDecoder().decode(encoded.trim()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("group '" + config.getName()
                    + "' content-key is not Base64 of " + GroupKey.KEY_LENGTH + " bytes", e);
        }
    }

    /**
     * The lifecycle driving protocol ticks and leased card refresh.
     *
     * @param properties the starter configuration
     * @param node       the peer node
     * @param spaces     the facade
     * @return the lifecycle
     */
    public AgentSpacesLifecycle agentSpacesLifecycle(AgentSpacesProperties properties,
                                                     PeerNode node, AgentSpaces spaces) {
        return new AgentSpacesLifecycle(node, spaces,
                Duration.ofMillis(properties.getTickMillis()),
                Duration.ofMillis(properties.getCardRefreshMillis()));
    }

    /**
     * The lifecycle, re-presenting this node's OIDC token from
     * {@code agentspaces.security.oidc.token-file} on every card-refresh tick
     * (TODO-EFG §4), so a sidecar can rotate the file. The three-argument
     * overload is the programmatic convenience without the hook.
     *
     * @param properties  the starter configuration
     * @param node        the peer node
     * @param spaces      the facade
     * @param authorizers the profile's authorizer selection
     * @return the lifecycle
     */
    @Bean
    @ConditionalOnMissingBean
    public AgentSpacesLifecycle agentSpacesLifecycle(AgentSpacesProperties properties,
                                                     PeerNode node, AgentSpaces spaces,
                                                     Authorizers authorizers) {
        return new AgentSpacesLifecycle(node, spaces,
                Duration.ofMillis(properties.getTickMillis()),
                Duration.ofMillis(properties.getCardRefreshMillis()),
                authorizers::refreshToken);
    }

    /**
     * The post-processor binding {@code @AgentSpec}/{@code @SpaceTake} beans.
     *
     * @param properties the starter configuration
     * @param spaces     the facade
     * @return the post-processor
     */
    @Bean
    @ConditionalOnMissingBean
    public static AgentSpacesBeanPostProcessor agentSpacesBeanPostProcessor(
            ObjectProvider<AgentSpacesProperties> properties, ObjectProvider<AgentSpaces> spaces) {
        return new AgentSpacesBeanPostProcessor(spaces::getObject,
                () -> groupOrder(properties.getObject()));
    }

    /**
     * The post-processor over an already built facade, for programmatic use.
     *
     * @param properties the starter configuration
     * @param spaces     the facade
     * @return the post-processor
     */
    public static AgentSpacesBeanPostProcessor agentSpacesBeanPostProcessor(
            AgentSpacesProperties properties, AgentSpaces spaces) {
        return new AgentSpacesBeanPostProcessor(spaces, groupOrder(properties));
    }

    private static List<String> groupOrder(AgentSpacesProperties properties) {
        List<String> order = new ArrayList<>();
        properties.getGroups().forEach(group -> order.add(group.getName()));
        return order;
    }

    // ---------------------------------------------------------------- internals

    private static GroupAdvertisement advertisementFor(AgentSpacesProperties.Group config) {
        String founding = config.getFounding() == null || config.getFounding().isBlank()
                ? config.getName()
                : config.getFounding();
        GroupId groupId = GroupId.fromFounding(founding.getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), config.getName(),
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Parses seed strings into endpoints. A seed may carry an explicit scheme
     * ({@code tls://host:port}, {@code tcp://host:port}); a bare
     * {@code host:port} takes {@code defaultScheme}, which follows the node's
     * own transport configuration.
     */
    private static List<PeerAdvertisement.Endpoint> seedEndpoints(List<String> seeds,
                                                                  String defaultScheme) {
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < seeds.size(); i++) {
            String seed = seeds.get(i).trim();
            String scheme = defaultScheme;
            int separator = seed.indexOf("://");
            if (separator > 0) {
                scheme = seed.substring(0, separator);
                seed = seed.substring(separator + 3);
            }
            endpoints.add(new PeerAdvertisement.Endpoint(scheme, seed, i));
        }
        return endpoints;
    }

    /**
     * Builds one replicated space over the group's block exchange (spec
     * §6.1a), advertising itself through discovery and enforcing the
     * configured admission rule (§7.5), encrypted with the group's content
     * key when one is configured (§11a).
     */
    private static ReplicatedSpace buildSpace(GroupRuntime runtime, PeerIdentity identity,
                                              AgentSpacesProperties.SpaceDef spaceDef,
                                              BlockExchange blocks, DiscoveryService discovery,
                                              GroupKeyRing contentKey, Authorizer authorizer,
                                              InstantSource clock) {
        ConflictStrategyType strategy =
                ConflictStrategyType.valueOf(spaceDef.getStrategy().trim().toUpperCase());
        ReplicatedSpace.Builder builder =
                ReplicatedSpace.builder(runtime, spaceDef.getName(), identity, "app")
                        .strategy(strategy)
                        .settleWindow(Duration.ofMillis(spaceDef.getSettleWindowMillis()))
                        .clock(clock) // the fabric's clock (review M-8)
                        .blocks(blocks)
                        .advertise(discovery::publish);
        SpaceAdvertisement.Admission admission = SpaceAdvertisement.Admission.valueOf(
                spaceDef.getAdmission().trim().toUpperCase());
        if (admission == SpaceAdvertisement.Admission.ALLOWLIST) {
            Set<AgentId> allowed = new LinkedHashSet<>();
            for (String agent : spaceDef.getAllowedAgents()) {
                allowed.add(AgentId.parse(agent.trim()));
            }
            if (allowed.isEmpty()) {
                throw new IllegalStateException("space '" + spaceDef.getName()
                        + "' uses allowlist admission but lists no allowed-agents");
            }
            builder.admission(admission, allowed);
        } else if (admission == SpaceAdvertisement.Admission.CREDENTIAL) {
            // SPEC §7.5 CREDENTIAL (TODO-EFG §3): this node issues unless the
            // configuration names the issuing peer.
            String issuer = spaceDef.getCredentialIssuer();
            if (issuer == null || issuer.isBlank()) {
                builder.admission(admission, Set.of());
            } else {
                builder.admission(SpaceAdmission.credentials(PeerId.of(issuer.trim()),
                        new CredentialIndex()));
            }
        } else if (admission == SpaceAdvertisement.Admission.AUTHORIZER) {
            // SPEC §7.5 AUTHORIZER (TODO-EFG §4): the profile's authorizer
            // decides SPACE_WRITE/SPACE_TAKE in the scope of the space name.
            builder.admission(SpaceAdmission.authorizer(authorizer, spaceDef.getName()));
        }
        if (contentKey != null) {
            builder.keyRing(contentKey);
        }
        return builder.build();
    }
}
