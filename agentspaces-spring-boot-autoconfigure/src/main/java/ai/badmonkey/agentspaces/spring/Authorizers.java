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

import ai.badmonkey.agentspaces.api.security.SecurityProfile;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.auth.oidc.OidcAuthorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.MembershipAuthorizer;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.JoinCredentials;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The security profile's answer to "who may act" (TODO-EFG §4, TODO item 6):
 * one {@link Authorizer} selection for the whole starter, exposed as a bean so
 * applications pass it to the enforcement points the starter does not wire
 * itself ({@code RaftLog}, {@code DataQueryClient}, {@code ConnectorRuntime}).
 *
 * <ul>
 *   <li>{@code dev-local} and {@code mtls} are membership-rooted: one
 *   {@link MembershipAuthorizer} per networked group, over that group's
 *   admitted membership, narrowed by {@code agentspaces.security.grants.*}.
 *   {@link #forGroup(String)} returns the group's authorizer;
 *   {@link #primary()} is that authorizer when one group is configured and
 *   otherwise a peer-permitting union over the groups.</li>
 *   <li>{@code mtls-oidc} and {@code zero-trust} defer to the identity
 *   provider: one {@link OidcAuthorizer} built from
 *   {@code agentspaces.security.oidc.{issuer, audience, jwks-url}} answers for
 *   every group, and construction fails fast naming the property that is
 *   missing. Members present their tokens in their signed self-advertisements
 *   under {@link JoinCredentials#OIDC_HINT_KEY}; this holder ingests them
 *   through the node's credential-hint listener and presents this node's own
 *   token from {@code agentspaces.security.oidc.token} or, re-read on every
 *   card-refresh tick, {@code token-file}.</li>
 * </ul>
 *
 * <p>The starter threads the selected authorizer into the key-wrap provider
 * ({@code KEY_HOLDER}), every space configured with {@code admission:
 * authorizer} ({@code SPACE_WRITE}/{@code SPACE_TAKE} in the scope of the space
 * name), and the {@link DirectiveGates} bean ({@code DIRECTIVE_ISSUER} in the
 * scope of the group id).
 */
public final class Authorizers {

    private static final System.Logger LOG = System.getLogger(Authorizers.class.getName());

    private final SecurityProfile profile;
    private final PeerId self;
    private final Map<Authorizer.Operation, Set<PeerId>> grants;
    private final Map<Authorizer.Operation, Set<AgentId>> agentGrants;
    private final OidcAuthorizer oidc;
    private final AgentSpacesProperties.Security.Oidc oidcConfig;
    private final Map<String, Authorizer> byGroup =
            Collections.synchronizedMap(new LinkedHashMap<>());
    /** Each registered group's revocations, so the cross-group union refuses what any group revoked. */
    private final Map<String, ai.badmonkey.agentspaces.api.security.RevocationView> revocationsByGroup =
            Collections.synchronizedMap(new LinkedHashMap<>());
    private volatile PeerNode node;
    private volatile String presentedToken;

    /**
     * Selects the profile's authorizer from the starter configuration.
     *
     * @param properties the starter configuration
     * @param identity   this node's identity
     * @param clock      the time source for OIDC grant expiry
     * @throws IllegalStateException under an OIDC profile with issuer,
     *                               audience, or JWKS URL missing
     */
    public Authorizers(AgentSpacesProperties properties, PeerIdentity identity,
                       InstantSource clock) {
        Objects.requireNonNull(properties, "properties");
        this.self = Objects.requireNonNull(identity, "identity").peerId();
        this.profile = SecurityProfile.fromName(properties.getSecurity().getProfile());
        this.oidcConfig = properties.getSecurity().getOidc();
        if (profile.oidcAuthorization()) {
            requireOidc(oidcConfig.getIssuer(), "issuer");
            requireOidc(oidcConfig.getAudience(), "audience");
            requireOidc(oidcConfig.getJwksUrl(), "jwks-url");
            this.oidc = OidcAuthorizer.fromJwks(oidcConfig.getIssuer().trim(),
                    oidcConfig.getAudience().trim(), jwksUrl(oidcConfig.getJwksUrl()),
                    Objects.requireNonNull(clock, "clock"));
            this.grants = Map.of();
            this.agentGrants = Map.of();
        } else {
            this.oidc = null;
            Map<Authorizer.Operation, Set<AgentId>> agents = new EnumMap<>(Authorizer.Operation.class);
            this.grants = grantsOf(properties, self, agents);
            this.agentGrants = Collections.unmodifiableMap(agents);
        }
    }

    private void requireOidc(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("agentspaces.security.profile="
                    + profile.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-')
                    + " authorizes through the identity provider and requires"
                    + " agentspaces.security.oidc." + property);
        }
    }

    /** Parses a JWKS URL property, naming the property on failure. */
    static URL jwksUrl(String value) {
        try {
            return URI.create(value.trim()).toURL();
        } catch (IllegalArgumentException | MalformedURLException e) {
            throw new IllegalStateException(
                    "agentspaces.security.oidc.jwks-url is not a URL: '" + value + "'", e);
        }
    }

    /**
     * The membership grants: each configured list parsed to PeerIDs, plus the
     * console's own peer under {@code DIRECTIVE_ISSUER} when command-and-control
     * is enabled on this node (TODO-EFG §4, the {@code FleetCommander} row).
     */
    private static Map<Authorizer.Operation, Set<PeerId>> grantsOf(
            AgentSpacesProperties properties, PeerId self,
            Map<Authorizer.Operation, Set<AgentId>> agents) {
        AgentSpacesProperties.Security.Grants config = properties.getSecurity().getGrants();
        Map<Authorizer.Operation, Set<PeerId>> grants = new EnumMap<>(Authorizer.Operation.class);
        put(grants, agents, Authorizer.Operation.RAFT_VOTER, config.getRaftVoter(), "raft-voter");
        put(grants, agents, Authorizer.Operation.DIRECTIVE_ISSUER, config.getDirectiveIssuer(),
                "directive-issuer");
        put(grants, agents, Authorizer.Operation.CONNECTOR_SERVE, config.getConnectorServe(),
                "connector-serve");
        put(grants, agents, Authorizer.Operation.KEY_HOLDER, config.getKeyHolder(), "key-holder");
        put(grants, agents, Authorizer.Operation.KEY_ROTATE, config.getKeyRotator(), "key-rotator");
        put(grants, agents, Authorizer.Operation.SPACE_WRITE, config.getSpaceWrite(), "space-write");
        put(grants, agents, Authorizer.Operation.SPACE_TAKE, config.getSpaceTake(), "space-take");
        put(grants, agents, Authorizer.Operation.VOTE, config.getVote(), "vote");
        put(grants, agents, Authorizer.Operation.MODEL_SERVE, config.getModelServe(), "model-serve");
        if (properties.getConsole().isEnabled() && properties.getConsole().getCommand().isEnabled()) {
            Set<PeerId> issuers = new LinkedHashSet<>(
                    grants.getOrDefault(Authorizer.Operation.DIRECTIVE_ISSUER, Set.of()));
            issuers.add(self);
            grants.put(Authorizer.Operation.DIRECTIVE_ISSUER, Set.copyOf(issuers));
        }
        return Collections.unmodifiableMap(grants);
    }

    /**
     * A grant list mixes bare PeerIds and {@code peer/localName} AgentIds (QA4
     * A4-7 phase 2). A bare PeerId admits every agent on that peer; an AgentId
     * names one agent and puts the operation at AGENT granularity.
     */
    private static void put(Map<Authorizer.Operation, Set<PeerId>> grants,
                            Map<Authorizer.Operation, Set<AgentId>> agents,
                            Authorizer.Operation operation, List<String> values, String property) {
        if (values == null || values.isEmpty()) {
            return;
        }
        Set<PeerId> peers = new LinkedHashSet<>();
        Set<AgentId> named = new LinkedHashSet<>();
        for (String value : values) {
            try {
                String trimmed = value.trim();
                if (trimmed.contains("/")) {
                    named.add(AgentId.parse(trimmed));
                } else {
                    peers.add(PeerId.of(trimmed));
                }
            } catch (RuntimeException e) {
                throw new IllegalStateException("agentspaces.security.grants." + property
                        + " carries an invalid PeerId or AgentId '" + value + "'", e);
            }
        }
        grants.put(operation, Set.copyOf(peers));
        if (!named.isEmpty()) {
            agents.put(operation, Set.copyOf(named));
        }
    }

    /** The profile the selection follows. */
    public SecurityProfile profile() {
        return profile;
    }

    /** The identity-provider authorizer under the OIDC profiles; empty otherwise. */
    public Optional<OidcAuthorizer> oidc() {
        return Optional.ofNullable(oidc);
    }

    /**
     * The explicit membership grants in force (with the console auto-grant
     * applied); empty under the OIDC profiles.
     */
    public Map<Authorizer.Operation, Set<PeerId>> grants() {
        return grants;
    }

    /**
     * The explicit per-agent grants in force (QA4 A4-7 phase 2); an operation
     * present here is answered at AGENT granularity. Empty under the OIDC profiles.
     */
    public Map<Authorizer.Operation, Set<AgentId>> agentGrants() {
        return agentGrants;
    }

    /** The configured group names this holder answers for. */
    public Set<String> groupNames() {
        synchronized (byGroup) {
            return Set.copyOf(byGroup.keySet());
        }
    }

    /**
     * The authorizer for one configured group: the group's
     * {@link MembershipAuthorizer} under the membership profiles, the single
     * {@link OidcAuthorizer} under the OIDC profiles.
     *
     * @param groupName the configured group name
     * @return the authorizer
     * @throws IllegalArgumentException for a group the starter did not configure
     */
    public Authorizer forGroup(String groupName) {
        Objects.requireNonNull(groupName, "groupName");
        Authorizer authorizer = byGroup.get(groupName);
        if (authorizer == null) {
            throw new IllegalArgumentException("no authorizer for group '" + groupName
                    + "'; configured groups: " + groupNames());
        }
        return authorizer;
    }

    /**
     * The one authorizer an application passes around: the sole group's
     * authorizer when one group is configured, otherwise an authorizer
     * permitting what any configured group's authorizer permits (subject to
     * the grants or, under the OIDC profiles, the token's scopes). Every
     * group's authorizer is group-scoped (v0.1.13, review M-4), and the union
     * additionally refuses a peer or agent that any of this node's groups has
     * revoked, so a revocation in one group is not bypassed through another.
     * Before any group is registered under the OIDC profiles, the bare
     * {@link OidcAuthorizer}.
     *
     * @return the authorizer
     */
    public Authorizer primary() {
        synchronized (byGroup) {
            if (byGroup.isEmpty() && oidc != null) {
                return oidc;
            }
            if (byGroup.size() == 1) {
                return byGroup.values().iterator().next();
            }
        }
        return new Authorizer() {
            private List<Authorizer> all() {
                synchronized (byGroup) {
                    return List.copyOf(byGroup.values());
                }
            }

            private List<ai.badmonkey.agentspaces.api.security.RevocationView> views() {
                synchronized (revocationsByGroup) {
                    return List.copyOf(revocationsByGroup.values());
                }
            }

            @Override
            public boolean permits(PeerId peer, Operation operation, String scope) {
                return views().stream().noneMatch(v -> v.revoked(peer))
                        && all().stream().anyMatch(a -> a.permits(peer, operation, scope));
            }

            @Override
            public boolean permits(AgentId agent, Operation operation, String scope) {
                return views().stream().noneMatch(v -> v.revoked(agent, null))
                        && all().stream().anyMatch(a -> a.permits(agent, operation, scope));
            }

            /** The union speaks per agent as soon as any group's authorizer does. */
            @Override
            public Granularity granularity(Operation operation, String scope) {
                return all().stream().anyMatch(a ->
                        a.granularity(operation, scope) == Granularity.AGENT)
                        ? Granularity.AGENT : Granularity.PEER;
            }
        };
    }

    /** Roots one joined group's authorizer in its membership (membership profiles). */
    void register(String groupName, GroupRuntime runtime) {
        Objects.requireNonNull(groupName, "groupName");
        Objects.requireNonNull(runtime, "runtime");
        revocationsByGroup.put(groupName, runtime.revocationView());
        // v0.1.13 (review M-4): each group's authorizer refuses what that group
        // revoked; the shared OIDC authorizer additionally requires membership
        // of the group, since a token is valid in every group a node joins.
        if (oidc == null) {
            byGroup.put(groupName, new ai.badmonkey.agentspaces.peering.membership.GroupScopedAuthorizer(
                    new MembershipAuthorizer(runtime.membership(), self, grants, agentGrants),
                    runtime.revocationView(), null, self));
        } else {
            byGroup.put(groupName, new ai.badmonkey.agentspaces.peering.membership.GroupScopedAuthorizer(
                    oidc, runtime.revocationView(), runtime.membership(), self));
        }
    }

    /**
     * Attaches the token channel to the node under the OIDC profiles: members'
     * hints flow into the {@link OidcAuthorizer} and this node's own token is
     * presented. A no-op under the membership profiles.
     */
    void attach(PeerNode node) {
        this.node = Objects.requireNonNull(node, "node");
        if (oidc == null) {
            return;
        }
        node.onCredentialHints(this::ingest);
        refreshToken();
    }

    /**
     * Ingests one member's credential hints: its {@code aspace:oidc} token, when
     * present, is validated and cached by the {@link OidcAuthorizer}. Under the
     * membership profiles hints carry no authorization and are ignored.
     *
     * @param peer  the member whose signed self-advertisement carried the hints
     * @param hints the hints
     */
    public void ingest(PeerId peer, Map<String, String> hints) {
        if (oidc == null || hints == null) {
            return;
        }
        String token = hints.get(JoinCredentials.OIDC_HINT_KEY);
        if (token == null || token.isBlank()) {
            return;
        }
        if (!oidc.authorize(peer, token)) {
            LOG.log(System.Logger.Level.WARNING, "peer " + peer
                    + " presented an OIDC token that did not validate; it is permitted nothing");
        }
    }

    /**
     * Presents this node's own token (TODO-EFG §4): {@code oidc.token} when set,
     * else the current contents of {@code oidc.token-file}. The token goes into
     * the node's {@code aspace:oidc} hint so the next self-advertisement
     * carries it, and into this node's own authorizer so local decisions
     * ({@code CONNECTOR_SERVE} for its own assets, its own space writes) see
     * the same grants the fleet sees. Called on attach and on every
     * card-refresh tick; an unchanged token is a no-op.
     */
    public void refreshToken() {
        PeerNode current = node;
        if (oidc == null || current == null) {
            return;
        }
        String token = currentToken();
        if (token == null) {
            if (presentedToken != null) {
                current.removeCredentialHint(JoinCredentials.OIDC_HINT_KEY);
                presentedToken = null;
            }
            return;
        }
        if (token.equals(presentedToken)) {
            return;
        }
        current.credentialHint(JoinCredentials.OIDC_HINT_KEY, token);
        presentedToken = token;
        if (!oidc.authorize(self, token)) {
            LOG.log(System.Logger.Level.WARNING, "this node's own OIDC token"
                    + " (agentspaces.security.oidc.token/token-file) did not validate;"
                    + " the fleet will permit this peer nothing until a valid token is presented");
        }
    }

    private String currentToken() {
        String token = oidcConfig.getToken();
        if (token != null && !token.isBlank()) {
            return token.trim();
        }
        String file = oidcConfig.getTokenFile();
        if (file == null || file.isBlank()) {
            return null;
        }
        try {
            String read = Files.readString(Path.of(file.trim()), StandardCharsets.UTF_8).trim();
            return read.isEmpty() ? null : read;
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "cannot read agentspaces.security.oidc.token-file " + file, e);
            return presentedToken;
        }
    }
}
