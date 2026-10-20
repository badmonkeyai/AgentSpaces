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

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the AgentSpaces starter (spec §10.1), bound from the
 * {@code agentspaces} prefix:
 *
 * <pre>{@code
 * agentspaces:
 *   keystore: ./peer-keys           # omit for an ephemeral generated identity
 *   bind: 127.0.0.1:7500            # omit for a dial-only (NAT-restricted) peer
 *   roles: [RENDEZVOUS]             # optional topology roles
 *   tick-millis: 250
 *   groups:
 *     - name: research-fleet
 *       founding: research-fleet-v1 # all members derive the same GroupId from this
 *       seeds: ["hq.example:7500"]
 *       spaces:
 *         - name: tasks
 *         - name: tasks-auction
 *           strategy: AUCTION
 *         - name: control
 *           admission: allowlist    # only the listed AgentIds may write
 *           allowed-agents: ["<peerId>/steward"]
 *     - name: partner-fleet
 *       join: "aspace://z7f3a..."   # fetch and verify the founding ad from the seeds
 *       seeds: ["partner.example:7500"]
 *   capabilities:
 *     vote: true                    # aggregate, vote, semantic-discovery, key-wrap default on
 *   multicast:
 *     enabled: false                # LAN bootstrap beacon, opt-in
 * }</pre>
 *
 * <p>JavaBean-style on purpose: mutable properties bind under every Spring
 * Boot 3 binder configuration with no constructor-binding annotations needed.
 */
@ConfigurationProperties(prefix = "agentspaces")
public class AgentSpacesProperties {

    private String keystore;
    private String bind;
    /**
     * The address other peers dial, carried in this peer's self-advertisement
     * in place of {@code bind}: {@code host:port} or {@code scheme://host:port}.
     * Set it on a host that binds every interface, sits behind a NAT, or runs
     * in a pod whose routable address the platform assigns. Blank: the bind
     * address is advertised, as before.
     */
    private String advertise;
    private List<String> roles = new ArrayList<>();
    private long tickMillis = 250;
    private long cardRefreshMillis = 300_000;
    /** How often a node re-floods its peer advertisement (ms); between refreshes newcomers get it directly. */
    private long peerAdIntervalMillis = 30_000;
    /** The frame coalescing window (ms): rumors and pipe frames to one peer batch into one signed frame; 0 turns it off. */
    private long coalesceMillis = 20;
    /** Lazy push (PERF2 §3.4): rumors ride a self-pruning tree toward members that understand it; off gossips with random fan-out only. */
    private boolean lazyPush = true;
    private List<Group> groups = new ArrayList<>();

    /** Transport and channel-authentication settings (spec §5.6). */
    private Transport transport = new Transport();

    /** Security posture selection ({@code agentspaces.security.*}). */
    private Security security = new Security();

    /** Agent identity settings ({@code agentspaces.identity.*}). */
    private Identity identity = new Identity();

    /** Cryptography settings ({@code agentspaces.crypto.*}). */
    private Crypto crypto = new Crypto();

    /** The served console, off unless enabled. */
    private Console console = new Console();

    /** The A2A gateway ({@code agentspaces.a2a.*}). */
    private A2a a2a = new A2a();

    /** The Embabel bridge settings; active only with Embabel on the classpath. */
    private Embabel embabel = new Embabel();

    /** The shipped capability providers ({@code agentspaces.capabilities.*}, spec §10.5). */
    private Capabilities capabilities = new Capabilities();

    /** The opt-in LAN multicast bootstrap beacon ({@code agentspaces.multicast.*}, spec §10.1). */
    private Multicast multicast = new Multicast();

    /** Relay settings ({@code agentspaces.relay.*}). */
    private Relay relay = new Relay();

    /**
     * What this peer forwards as a {@code RELAY} (SPEC §5.4; ISSUE-BootstrapCore FR-2):
     * a byte budget per group per day, enforced through {@code RelayPolicy.dailyBudget}
     * when no {@code RelayPolicy} bean is defined. Unset: every forward the membership
     * check allows.
     */
    public static class Relay {
        private Long budgetBytesPerDay;

        public Long getBudgetBytesPerDay() {
            return budgetBytesPerDay;
        }

        public void setBudgetBytesPerDay(Long budgetBytesPerDay) {
            this.budgetBytesPerDay = budgetBytesPerDay;
        }
    }

    public Relay getRelay() {
        return relay;
    }

    public void setRelay(Relay relay) {
        this.relay = relay;
    }

    /**
     * One group to join. Exactly one of {@code founding} and {@code join}
     * identifies the group: {@code founding} derives a GroupId every member
     * computes locally from the same string, while {@code join} names an
     * existing self-certifying group by its {@code aspace://<groupId>} URI (or
     * bare GroupId) whose signed founding advertisement is fetched from the
     * seeds and verified against the id before joining (spec §10.1, §4.4).
     * Neither set means the group name doubles as the founding string.
     */
    public static class Group {

        private String name;
        private String founding;
        private String join;
        private long joinTimeoutMillis = 10_000;
        private String contentKey;
        private ContentKeyRotation contentKeyRotation = new ContentKeyRotation();
        private List<String> seeds = new ArrayList<>();
        private List<SpaceDef> spaces = new ArrayList<>();
        /**
         * Who may join (spec §5.1): {@code open} (default) or {@code invite},
         * under which a candidate must present a join credential issued by the
         * founder, or under a delegation the founder signed, in its
         * self-advertisement. With {@code founding}, the founder is the peer
         * named by {@code founder}, or this peer when that is unset; every
         * member must name the same founder or they admit differently.
         */
        private String membership = "open";
        /**
         * The founder's PeerId for an {@code invite} group founded from a
         * string: the issuer whose credentials admit. Unset on the founder
         * itself (its own identity is the founder); required on every other
         * member, which the startup refusal says when a credential is set
         * without it.
         */
        private String founder;
        /**
         * The join credential this peer presents ({@code aspace:join}): a
         * founder-issued or delegated credential string, as
         * {@code JoinCredentials} produces. Blank for the founder and for
         * open groups.
         */
        private String joinCredential;
        /**
         * The signed founding advertisement this peer joins with, as the
         * base64 CBOR a beacon document carries: the self-certifying group
         * this peer founded itself (a social creator), or one it was handed
         * out of band. Exclusive with {@code founding} and {@code join}; the
         * GroupId is the document's own.
         */
        private String foundingDocument;

        public String getFoundingDocument() {
            return foundingDocument;
        }

        public void setFoundingDocument(String foundingDocument) {
            this.foundingDocument = foundingDocument;
        }

        public String getMembership() {
            return membership;
        }

        public void setMembership(String membership) {
            this.membership = membership;
        }

        public String getFounder() {
            return founder;
        }

        public void setFounder(String founder) {
            this.founder = founder;
        }

        public String getJoinCredential() {
            return joinCredential;
        }

        public void setJoinCredential(String joinCredential) {
            this.joinCredential = joinCredential;
        }

        /**
         * Content-key rotation for this group (SPEC §11a.3, v0.1.13). Every
         * member follows rotations whenever {@code content-key} is set and the
         * {@code key-wrap} capability runs; these settings tune it, and
         * {@code rotate-every} makes this peer rotate on a schedule (it must
         * hold the founder's role or a {@code key-rotator} grant).
         */
        public ContentKeyRotation getContentKeyRotation() {
            return contentKeyRotation;
        }

        public void setContentKeyRotation(ContentKeyRotation contentKeyRotation) {
            this.contentKeyRotation = contentKeyRotation;
        }

        public String getJoin() {
            return join;
        }

        public void setJoin(String join) {
            this.join = join;
        }

        public long getJoinTimeoutMillis() {
            return joinTimeoutMillis;
        }

        public void setJoinTimeoutMillis(long joinTimeoutMillis) {
            this.joinTimeoutMillis = joinTimeoutMillis;
        }

        /**
         * The group's content key (spec §11a), Base64 of 32 bytes: when set,
         * every configured space encrypts its payloads with it and the
         * {@code key-wrap} capability serves it to admitted members.
         */
        public String getContentKey() {
            return contentKey;
        }

        public void setContentKey(String contentKey) {
            this.contentKey = contentKey;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getFounding() {
            return founding;
        }

        public void setFounding(String founding) {
            this.founding = founding;
        }

        public List<String> getSeeds() {
            return seeds;
        }

        public void setSeeds(List<String> seeds) {
            this.seeds = seeds;
        }

        public List<SpaceDef> getSpaces() {
            return spaces;
        }

        public void setSpaces(List<SpaceDef> spaces) {
            this.spaces = spaces;
        }
    }

    /**
     * A group's content-key rotation settings (SPEC §11a.3, v0.1.13).
     */
    public static class ContentKeyRotation {

        private java.time.Duration rotateEvery;
        private java.time.Duration cutoverDelay;
        private java.time.Duration writerGrace = java.time.Duration.ofHours(1);

        /**
         * How often this peer rotates the group's content key; unset (the
         * default) means it never rotates on its own. The nonce budget is
         * fleet-wide, so rotate by time (SPEC §11a.3).
         */
        public java.time.Duration getRotateEvery() {
            return rotateEvery;
        }

        public void setRotateEvery(java.time.Duration rotateEvery) {
            this.rotateEvery = rotateEvery;
        }

        /**
         * How long members have to fetch a new epoch before writers switch to
         * it; unset means two gossip periods plus 30s, time for the holder's
         * advertisement to spread and for members to fetch (review §15.3).
         */
        public java.time.Duration getCutoverDelay() {
            return cutoverDelay;
        }

        public void setCutoverDelay(java.time.Duration cutoverDelay) {
            this.cutoverDelay = cutoverDelay;
        }

        /**
         * How long a writer that has not yet fetched an announced epoch keeps
         * sealing under the previous one before refusing to write (1h).
         */
        public java.time.Duration getWriterGrace() {
            return writerGrace;
        }

        public void setWriterGrace(java.time.Duration writerGrace) {
            this.writerGrace = writerGrace;
        }
    }

    /**
     * One replicated space within a group. {@code admission} is {@code group}
     * (every member may write, the default), {@code allowlist}, under which
     * only the {@code allowed-agents} (encoded AgentIds) may write, take, or
     * complete, {@code credential}, under which a live {@code SpaceCredential}
     * entry signed by the {@code credential-issuer} admits an agent, or
     * {@code authorizer}, under which the profile's {@code Authorizer} decides
     * {@code SPACE_WRITE}/{@code SPACE_TAKE} in the scope of the space name;
     * everyone may still read (spec §7.5, TODO-EFG §3–§4).
     */
    public static class SpaceDef {

        private String name;
        private String strategy = "LEASE_RACE";
        private long settleWindowMillis = 200;
        private String admission = "group";
        private List<String> allowedAgents = new ArrayList<>();
        private String credentialIssuer = "";

        public String getAdmission() {
            return admission;
        }

        public void setAdmission(String admission) {
            this.admission = admission;
        }

        /**
         * Under {@code admission: credential}, the PeerID whose signed
         * {@code SpaceCredential} entries admit agents to this space (SPEC
         * §7.5); blank means this node, the founder's configuration, and only
         * that node may {@code grant} and {@code revoke}.
         */
        public String getCredentialIssuer() {
            return credentialIssuer;
        }

        public void setCredentialIssuer(String credentialIssuer) {
            this.credentialIssuer = credentialIssuer;
        }

        public List<String> getAllowedAgents() {
            return allowedAgents;
        }

        public void setAllowedAgents(List<String> allowedAgents) {
            this.allowedAgents = allowedAgents;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStrategy() {
            return strategy;
        }

        public void setStrategy(String strategy) {
            this.strategy = strategy;
        }

        public long getSettleWindowMillis() {
            return settleWindowMillis;
        }

        public void setSettleWindowMillis(long settleWindowMillis) {
            this.settleWindowMillis = settleWindowMillis;
        }
    }

    public String getKeystore() {
        return keystore;
    }

    public void setKeystore(String keystore) {
        this.keystore = keystore;
    }

    public String getAdvertise() {
        return advertise;
    }

    public void setAdvertise(String advertise) {
        this.advertise = advertise;
    }

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public long getTickMillis() {
        return tickMillis;
    }

    public void setTickMillis(long tickMillis) {
        this.tickMillis = tickMillis;
    }

    public long getCardRefreshMillis() {
        return cardRefreshMillis;
    }

    public void setCardRefreshMillis(long cardRefreshMillis) {
        this.cardRefreshMillis = cardRefreshMillis;
    }

    public long getPeerAdIntervalMillis() {
        return peerAdIntervalMillis;
    }

    public long getCoalesceMillis() {
        return coalesceMillis;
    }

    public void setCoalesceMillis(long coalesceMillis) {
        this.coalesceMillis = coalesceMillis;
    }

    public boolean isLazyPush() {
        return lazyPush;
    }

    public void setLazyPush(boolean lazyPush) {
        this.lazyPush = lazyPush;
    }

    public void setPeerAdIntervalMillis(long peerAdIntervalMillis) {
        this.peerAdIntervalMillis = peerAdIntervalMillis;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    public Console getConsole() {
        return console;
    }

    public void setConsole(Console console) {
        this.console = console;
    }

    public A2a getA2a() {
        return a2a;
    }

    public void setA2a(A2a a2a) {
        this.a2a = a2a;
    }

    public Embabel getEmbabel() {
        return embabel;
    }

    public void setEmbabel(Embabel embabel) {
        this.embabel = embabel;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public Security getSecurity() {
        return security;
    }

    public Identity getIdentity() {
        return identity;
    }

    public void setIdentity(Identity identity) {
        this.identity = identity;
    }

    public void setSecurity(Security security) {
        this.security = security;
    }

    public Capabilities getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(Capabilities capabilities) {
        this.capabilities = capabilities;
    }

    public Multicast getMulticast() {
        return multicast;
    }

    public void setMulticast(Multicast multicast) {
        this.multicast = multicast;
    }

    /**
     * The capability providers the starter ships into every networked group
     * ({@code agentspaces.capabilities.*}, spec §10.5). {@code enabled=false}
     * turns the whole surface off; otherwise each provider has its own toggle.
     * {@code aggregate} (push-sum), {@code vote} (QUORUM and MAJORITY_GOSSIP
     * over the {@code votes-space}), {@code semantic-discovery} (the hashing
     * embedder) and {@code key-wrap} (serving the group's {@code content-key}
     * when one is configured) default on. {@code ordered-log} and
     * {@code gossip-learn} default off: the Raft log needs a fixed member set
     * and the learner a model, so an application enables them deliberately —
     * {@code gossip-learn=true} wires the default weight-averaging learner,
     * while {@code ordered-log=true} fails fast asking for a
     * {@code @ProvidesCapability} bean that carries the member set.
     * {@code votes-space} names the space the vote capability runs over: a
     * space the group configures by that name is used as is, otherwise the
     * starter builds a private LEASE_RACE space of that name, which does not
     * count among the group's registered spaces (so single-space inference in
     * {@code @SpaceTake} is unaffected).
     */
    public static class Capabilities {

        private boolean enabled = true;
        private boolean aggregate = true;
        private boolean vote = true;
        private boolean orderedLog;
        private boolean gossipLearn;
        private boolean semanticDiscovery = true;
        private boolean keyWrap = true;
        private String votesSpace = "votes";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isAggregate() {
            return aggregate;
        }

        public void setAggregate(boolean aggregate) {
            this.aggregate = aggregate;
        }

        public boolean isVote() {
            return vote;
        }

        public void setVote(boolean vote) {
            this.vote = vote;
        }

        public boolean isOrderedLog() {
            return orderedLog;
        }

        public void setOrderedLog(boolean orderedLog) {
            this.orderedLog = orderedLog;
        }

        public boolean isGossipLearn() {
            return gossipLearn;
        }

        public void setGossipLearn(boolean gossipLearn) {
            this.gossipLearn = gossipLearn;
        }

        public boolean isSemanticDiscovery() {
            return semanticDiscovery;
        }

        public void setSemanticDiscovery(boolean semanticDiscovery) {
            this.semanticDiscovery = semanticDiscovery;
        }

        public boolean isKeyWrap() {
            return keyWrap;
        }

        public void setKeyWrap(boolean keyWrap) {
            this.keyWrap = keyWrap;
        }

        public String getVotesSpace() {
            return votesSpace;
        }

        public void setVotesSpace(String votesSpace) {
            this.votesSpace = votesSpace;
        }
    }

    /**
     * The LAN multicast bootstrap beacon ({@code agentspaces.multicast.*},
     * spec §10.1 {@code bootstrap: multicast}; off by default). When enabled
     * the node announces its listening endpoints on the multicast
     * {@code group} every {@code interval-millis} and dials peers it hears,
     * so a LAN fleet needs no seeds; every advertisement learned this way is
     * still verified on its own terms. The beacon carries seed addresses only.
     */
    public static class Multicast {

        private boolean enabled;
        private boolean seal;
        private String group = "239.255.42.99:7787";

        /**
         * Seal each beacon datagram under its group's {@code content-key} (SPEC
         * §10.1 v0.1.19): members holding the key find each other and a
         * bystander reads nothing; groups with no content key stay plain.
         */
        public boolean isSeal() {
            return seal;
        }

        public void setSeal(boolean seal) {
            this.seal = seal;
        }
        private long intervalMillis = 5_000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public long getIntervalMillis() {
            return intervalMillis;
        }

        public void setIntervalMillis(long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }
    }

    /**
     * The security posture ({@code agentspaces.security.profile}): one named
     * profile — {@code dev-local}, {@code mtls} (the default),
     * {@code mtls-oidc}, or {@code zero-trust} — sets transport and channel
     * authentication coherently (remediation plan §4). Explicit
     * {@code agentspaces.transport.*} settings override the profile's choices
     * for deployments that need to deviate deliberately.
     */
    /**
     * How bound agents sign (QA4 A4-7 phase 3). {@code agent-keys=peer} (default)
     * signs every agent's records with the peer key, attributing them on the
     * peer's word; {@code subordinate} gives each bound agent an Ed25519 key of
     * its own, certified by the peer, so its records are {@code AGENT_ATTESTED},
     * its card carries the key, and two agents on one peer are two provable
     * identities (SPEC §4.2, §10.3).
     */
    public static class Identity {
        private String agentKeys = "peer";
        /** Each subordinate agent certificate's lifetime; renewed at half-life (SPEC §4.2, v0.1.13). */
        private java.time.Duration agentCertificateTtl = java.time.Duration.ofHours(24);
        /** Directory persisting subordinate agents' keys (SPEC §4.2, v0.1.13); blank keeps keys per process. */
        private String agentKeystore = "";

        public String getAgentKeystore() {
            return agentKeystore;
        }

        public void setAgentKeystore(String agentKeystore) {
            this.agentKeystore = agentKeystore;
        }

        public java.time.Duration getAgentCertificateTtl() {
            return agentCertificateTtl;
        }

        public void setAgentCertificateTtl(java.time.Duration agentCertificateTtl) {
            this.agentCertificateTtl = agentCertificateTtl;
        }

        public String getAgentKeys() {
            return agentKeys;
        }

        public void setAgentKeys(String agentKeys) {
            this.agentKeys = agentKeys;
        }
    }

    public static class Security {

        private String profile = "mtls";

        /** The identity provider the OIDC profiles authorize against ({@code agentspaces.security.oidc.*}). */
        private Oidc oidc = new Oidc();

        /** Per-operation grants for the membership-rooted profiles ({@code agentspaces.security.grants.*}). */
        private Grants grants = new Grants();

        public String getProfile() {
            return profile;
        }

        public void setProfile(String profile) {
            this.profile = profile;
        }

        public Oidc getOidc() {
            return oidc;
        }

        public void setOidc(Oidc oidc) {
            this.oidc = oidc;
        }

        public Grants getGrants() {
            return grants;
        }

        public void setGrants(Grants grants) {
            this.grants = grants;
        }

        /**
         * The identity provider behind the {@code mtls-oidc} and
         * {@code zero-trust} profiles (TODO-EFG §4): {@code issuer},
         * {@code audience}, and {@code jwks-url} build the fleet's
         * {@code OidcAuthorizer} and are all required under those profiles
         * (startup fails fast naming the missing one). {@code token} is this
         * node's own access token, bound to its PeerID by the
         * {@code agentspaces_peer} claim; alternatively {@code token-file}
         * names a file re-read on every card-refresh tick so a sidecar can
         * rotate it. The token travels in this peer's signed
         * self-advertisement under the {@code aspace:oidc} resource hint.
         * When the console's static {@code command.token} is blank, the same
         * issuer, audience, and JWKS also gate the console's command routes
         * with a JWT validator.
         */
        public static class Oidc {

            private String issuer = "";
            private String audience = "";
            private String jwksUrl = "";
            private String token = "";
            private String tokenFile = "";

            public String getIssuer() {
                return issuer;
            }

            public void setIssuer(String issuer) {
                this.issuer = issuer;
            }

            public String getAudience() {
                return audience;
            }

            public void setAudience(String audience) {
                this.audience = audience;
            }

            public String getJwksUrl() {
                return jwksUrl;
            }

            public void setJwksUrl(String jwksUrl) {
                this.jwksUrl = jwksUrl;
            }

            public String getToken() {
                return token;
            }

            public void setToken(String token) {
                this.token = token;
            }

            public String getTokenFile() {
                return tokenFile;
            }

            public void setTokenFile(String tokenFile) {
                this.tokenFile = tokenFile;
            }

            /** Whether issuer, audience, and JWKS URL are all set. */
            public boolean isConfigured() {
                return !blank(issuer) && !blank(audience) && !blank(jwksUrl);
            }

            private static boolean blank(String value) {
                return value == null || value.isBlank();
            }
        }

        /**
         * Explicit per-operation grants for the membership-rooted profiles
         * ({@code dev-local}, {@code mtls}; TODO-EFG §4): each list names the
         * PeerIDs permitted the operation in every networked group; an empty
         * list leaves the operation open to any admitted member. The peers
         * must still be admitted members — a revoked or evicted peer loses
         * its grants with its membership. When command-and-control is enabled
         * on this node, its own peer is added to {@code directive-issuer}
         * automatically, which narrows that operation to the console (plus
         * any peers listed here). Ignored under the OIDC profiles, where the
         * identity provider's scopes decide.
         */
        public static class Grants {

            private List<String> raftVoter = new ArrayList<>();
            private List<String> raftClient = new ArrayList<>();
            private List<String> directiveIssuer = new ArrayList<>();
            private List<String> connectorServe = new ArrayList<>();
            private List<String> keyHolder = new ArrayList<>();
            /** Peers that may rotate the content key (SPEC §11a.3); empty means the founder alone. */
            private List<String> keyRotator = new ArrayList<>();
            private List<String> spaceWrite = new ArrayList<>();
            private List<String> spaceTake = new ArrayList<>();
            private List<String> vote = new ArrayList<>();
            private List<String> modelServe = new ArrayList<>();

            public List<String> getRaftVoter() {
                return raftVoter;
            }

            public void setRaftVoter(List<String> raftVoter) {
                this.raftVoter = raftVoter;
            }

            /**
             * Peers (or agents) that may submit commands to an ordered log without voting
             * ({@code RAFT_CLIENT}, srad/agentspaces-js.md JS-V-05); empty permits every admitted member.
             */
            public List<String> getRaftClient() {
                return raftClient;
            }

            public void setRaftClient(List<String> raftClient) {
                this.raftClient = raftClient;
            }

            public List<String> getDirectiveIssuer() {
                return directiveIssuer;
            }

            public void setDirectiveIssuer(List<String> directiveIssuer) {
                this.directiveIssuer = directiveIssuer;
            }

            public List<String> getConnectorServe() {
                return connectorServe;
            }

            public void setConnectorServe(List<String> connectorServe) {
                this.connectorServe = connectorServe;
            }

            public List<String> getKeyHolder() {
                return keyHolder;
            }

            public void setKeyHolder(List<String> keyHolder) {
                this.keyHolder = keyHolder;
            }

            public List<String> getKeyRotator() {
                return keyRotator;
            }

            public void setKeyRotator(List<String> keyRotator) {
                this.keyRotator = keyRotator;
            }

            /**
             * The peers whose ballots count in a QUORUM tally (gate2-review
             * G2-3); empty leaves voting open to any admitted member of the
             * electorate.
             *
             * @return the permitted voters
             */
            public List<String> getVote() {
                return vote;
            }

            public void setVote(List<String> vote) {
                this.vote = vote;
            }

            /**
             * PeerIDs (or {@code peer/localName} AgentIDs) whose model-server
             * responses a fleet accepts ({@code MODEL_SERVE}); empty leaves the
             * operation open to any admitted member.
             *
             * @return the permitted model servers
             */
            public List<String> getModelServe() {
                return modelServe;
            }

            public void setModelServe(List<String> modelServe) {
                this.modelServe = modelServe;
            }

            public List<String> getSpaceWrite() {
                return spaceWrite;
            }

            public void setSpaceWrite(List<String> spaceWrite) {
                this.spaceWrite = spaceWrite;
            }

            public List<String> getSpaceTake() {
                return spaceTake;
            }

            public void setSpaceTake(List<String> spaceTake) {
                this.spaceTake = spaceTake;
            }
        }
    }

    public Crypto getCrypto() {
        return crypto;
    }

    public void setCrypto(Crypto crypto) {
        this.crypto = crypto;
    }

    /**
     * Cryptography settings. {@code signature-provider} selects the Ed25519
     * implementation by {@code SignatureProvider} name: {@code jdk} (the
     * default) or the name of a provider registered through ServiceLoader on
     * the classpath. Every provider is pinned to the project's golden
     * signature vectors by the conformance test, so a selected provider
     * produces byte-identical signatures or fails the build that ships it.
     */
    public static class Crypto {

        private String signatureProvider = "jdk";

        public String getSignatureProvider() {
            return signatureProvider;
        }

        public void setSignatureProvider(String signatureProvider) {
            this.signatureProvider = signatureProvider;
        }
    }

    /**
     * Transport settings ({@code agentspaces.transport.*}, spec §5.6). Both
     * settings default from the selected {@code agentspaces.security.profile};
     * set them only to deviate from the profile deliberately.
     * {@code channel-auth} selects the frame authentication mode:
     * {@code signed} signs every frame's envelope, and {@code attested}
     * negotiates unsigned frames over channels the transport authenticated,
     * which removes the per-frame Ed25519 cost on those links.
     * {@code tls.enabled} switches the node's TCP listener and dialer to the
     * TLS transport with this peer's identity-endorsed channel certificate,
     * the transport that makes channels attestable in the first place.
     */
    public static class Transport {

        private String channelAuth;
        private Tls tls = new Tls();
        private Listener quic = new Listener();
        private Dtls dtls = new Dtls();
        private Udp udp = new Udp();
        private Http ws = new Http("/aspace");
        private Http wss = new Http("/aspace");
        private Http http = new Http("/aspace/h");
        private Http https = new Http("/aspace/h");
        private Webrtc webrtc = new Webrtc();

        /** The {@code webrtc} block ({@code agentspaces.transport.webrtc.*}); needs agentspaces-transport-webrtc. */
        public Webrtc getWebrtc() {
            return webrtc;
        }

        public void setWebrtc(Webrtc webrtc) {
            this.webrtc = webrtc;
        }

        /** The {@code ws} listener block ({@code agentspaces.transport.ws.*}); needs agentspaces-transport-http. */
        public Http getWs() {
            return ws;
        }

        public void setWs(Http ws) {
            this.ws = ws;
        }

        /** The {@code wss} listener block ({@code agentspaces.transport.wss.*}); needs agentspaces-transport-http. */
        public Http getWss() {
            return wss;
        }

        public void setWss(Http wss) {
            this.wss = wss;
        }

        /** The {@code http} exchange block ({@code agentspaces.transport.http.*}); needs agentspaces-transport-http. */
        public Http getHttp() {
            return http;
        }

        public void setHttp(Http http) {
            this.http = http;
        }

        /** The {@code https} exchange block ({@code agentspaces.transport.https.*}); needs agentspaces-transport-http. */
        public Http getHttps() {
            return https;
        }

        public void setHttps(Http https) {
            this.https = https;
        }

        /**
         * An HTTP binding's block (SRAD agentspaces-js §3.3, JS-H-10): the
         * listener keys plus the path, the browser-origin allowlist, and the
         * binding's tunables. {@code ws} and {@code http} may share a
         * {@code bind}, as may {@code wss} and {@code https}.
         */
        /**
         * The {@code webrtc} block (SRAD agentspaces-js §3.4, §4.3, JS-N-04): {@code enabled}
         * registers the binding and accepts offers, advertising {@code Endpoint("webrtc", "sig",
         * priority)} with the priority defaulting after every stream endpoint (JS-W-08); the
         * address is always {@code sig}, so {@code bind} and {@code advertise} are not used.
         * The ICE servers default to none (JS-W-07, decision 9); every other value is a
         * per-peer rate, a per-session count, or a per-body size (JS-W-10).
         */
        public static class Webrtc extends Listener {

            private java.util.List<IceServer> iceServers = java.util.List.of();
            private java.time.Duration dialTimeout = java.time.Duration.ofSeconds(15);
            private int offersPerPeerPerMinute = 6;
            private int candidatesPerSession = 64;
            private int maxSignalBytes = 64 * 1024;
            private Offload offload = new Offload();

            /**
             * The relay offload (SRAD agentspaces-js JS-W-11, {@code PeerNode.RelayOffload}): a node
             * whose frames to or from a member cross a relay dials the member's {@code webrtc}
             * endpoint from its tick once more than {@code frames-per-minute} of them did in a
             * sliding minute, and after a failed dial waits {@code retry-after} before the next.
             * Settings, not caps; they apply to the node whether or not this block is enabled,
             * since the node dials only a member whose endpoint a registered binding signals.
             */
            public static class Offload {

                private int framesPerMinute = 30;
                private java.time.Duration retryAfter = java.time.Duration.ofMinutes(5);

                public int getFramesPerMinute() {
                    return framesPerMinute;
                }

                public void setFramesPerMinute(int framesPerMinute) {
                    this.framesPerMinute = framesPerMinute;
                }

                public java.time.Duration getRetryAfter() {
                    return retryAfter;
                }

                public void setRetryAfter(java.time.Duration retryAfter) {
                    this.retryAfter = retryAfter;
                }
            }

            /** One ICE server entry: {@code urls}, and the TURN {@code username} and {@code credential}. */
            public static class IceServer {

                private java.util.List<String> urls = java.util.List.of();
                private String username;
                private String credential;

                public java.util.List<String> getUrls() {
                    return urls;
                }

                public void setUrls(java.util.List<String> urls) {
                    this.urls = urls;
                }

                public String getUsername() {
                    return username;
                }

                public void setUsername(String username) {
                    this.username = username;
                }

                public String getCredential() {
                    return credential;
                }

                public void setCredential(String credential) {
                    this.credential = credential;
                }
            }

            /** STUN and TURN servers; empty gathers host and mDNS candidates alone. */
            public java.util.List<IceServer> getIceServers() {
                return iceServers;
            }

            public void setIceServers(java.util.List<IceServer> iceServers) {
                this.iceServers = iceServers;
            }

            /** The bound on a dial (signalling, ICE, DTLS, channel open) and the life of a pending session. */
            public java.time.Duration getDialTimeout() {
                return dialTimeout;
            }

            public void setDialTimeout(java.time.Duration dialTimeout) {
                this.dialTimeout = dialTimeout;
            }

            public int getOffersPerPeerPerMinute() {
                return offersPerPeerPerMinute;
            }

            public void setOffersPerPeerPerMinute(int offersPerPeerPerMinute) {
                this.offersPerPeerPerMinute = offersPerPeerPerMinute;
            }

            public int getCandidatesPerSession() {
                return candidatesPerSession;
            }

            public void setCandidatesPerSession(int candidatesPerSession) {
                this.candidatesPerSession = candidatesPerSession;
            }

            public int getMaxSignalBytes() {
                return maxSignalBytes;
            }

            public void setMaxSignalBytes(int maxSignalBytes) {
                this.maxSignalBytes = maxSignalBytes;
            }

            /** The relay offload settings ({@code agentspaces.transport.webrtc.offload.*}). */
            public Offload getOffload() {
                return offload;
            }

            public void setOffload(Offload offload) {
                this.offload = offload;
            }
        }

        public static class Http extends Listener {

            private String path;
            private java.util.List<String> allowedOrigins = java.util.List.of();
            private java.time.Duration idleTimeout = java.time.Duration.ofSeconds(60);
            private java.time.Duration pingInterval = java.time.Duration.ofSeconds(30);
            private java.time.Duration longPoll = java.time.Duration.ofSeconds(25);
            private int opensPerSourcePerSecond = 4;

            public Http() {
                this("/aspace");
            }

            Http(String path) {
                this.path = path;
            }

            /** The path served: {@code /aspace} for ws and wss, {@code /aspace/h} for http and https. */
            public String getPath() {
                return path;
            }

            public void setPath(String path) {
                this.path = path;
            }

            /** Browser origins allowed to connect ({@code scheme://host[:port]}); empty refuses every browser. */
            public java.util.List<String> getAllowedOrigins() {
                return allowedOrigins;
            }

            public void setAllowedOrigins(java.util.List<String> allowedOrigins) {
                this.allowedOrigins = allowedOrigins == null ? java.util.List.of() : allowedOrigins;
            }

            /** A connection or exchange session silent this long closes (60s). */
            public java.time.Duration getIdleTimeout() {
                return idleTimeout;
            }

            public void setIdleTimeout(java.time.Duration idleTimeout) {
                this.idleTimeout = idleTimeout;
            }

            /** A WebSocket idle this long for writing sends a ping (30s). */
            public java.time.Duration getPingInterval() {
                return pingInterval;
            }

            public void setPingInterval(java.time.Duration pingInterval) {
                this.pingInterval = pingInterval;
            }

            /** How long an exchange download request is held with nothing to send (25s). */
            public java.time.Duration getLongPoll() {
                return longPoll;
            }

            public void setLongPoll(java.time.Duration longPoll) {
                this.longPoll = longPoll;
            }

            /** Exchange sessions one source address may open per second (4); a rate, never a cap. */
            public int getOpensPerSourcePerSecond() {
                return opensPerSourcePerSecond;
            }

            public void setOpensPerSourcePerSecond(int opensPerSourcePerSecond) {
                this.opensPerSourcePerSecond = opensPerSourcePerSecond;
            }
        }

        /**
         * The bare UDP listener block ({@code agentspaces.transport.udp.*}): plaintext,
         * registered only under the dev-local profile or with {@code allow-plaintext};
         * needs agentspaces-transport-dtls.
         */
        public Udp getUdp() {
            return udp;
        }

        public void setUdp(Udp udp) {
            this.udp = udp;
        }

        /** The bare UDP block: the listener keys plus the explicit plaintext opt-in. */
        public static class Udp extends Listener {

            private boolean allowPlaintext;

            /** Permit plaintext UDP outside the dev-local profile (SRAD dtls-support DT-U-04). */
            public boolean isAllowPlaintext() {
                return allowPlaintext;
            }

            public void setAllowPlaintext(boolean allowPlaintext) {
                this.allowPlaintext = allowPlaintext;
            }
        }

        /** The QUIC listener block ({@code agentspaces.transport.quic.*}); needs agentspaces-transport-quic. */
        public Listener getQuic() {
            return quic;
        }

        public void setQuic(Listener quic) {
            this.quic = quic;
        }

        /** The DTLS listener block ({@code agentspaces.transport.dtls.*}); needs agentspaces-transport-dtls. */
        public Dtls getDtls() {
            return dtls;
        }

        public void setDtls(Dtls dtls) {
            this.dtls = dtls;
        }

        /**
         * One optional binding's listener block (SRAD dtls-support §3.6):
         * {@code enabled} registers the binding; with a {@code bind} it also
         * listens and advertises, at {@code priority} (default: after the
         * {@code agentspaces.bind} endpoint, in block order). Attesting
         * bindings take their credential and trust from
         * {@code agentspaces.transport.tls.*}.
         */
        public static class Listener {

            private boolean enabled;
            private String bind;
            private String advertise;
            private Integer priority;

            /** Register the binding. */
            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            /** {@code host:port} to listen on and advertise; blank for a dial-only binding. */
            public String getBind() {
                return bind;
            }

            public void setBind(String bind) {
                this.bind = bind;
            }

            /** {@code host:port} carried in the advertisement in place of {@code bind}. */
            public String getAdvertise() {
                return advertise;
            }

            public void setAdvertise(String advertise) {
                this.advertise = advertise;
            }

            /** Advertised priority (spec §5.5); lower dials first. */
            public Integer getPriority() {
                return priority;
            }

            public void setPriority(Integer priority) {
                this.priority = priority;
            }
        }

        /**
         * The DTLS listener block ({@code agentspaces.transport.dtls.*}, SRAD
         * dtls-support appendix C): the listener keys plus the binding's
         * tunables, each defaulted.
         */
        public static class Dtls extends Listener {

            private String attestation = "identity";
            private java.util.List<String> protocols = java.util.List.of("DTLSv1.2");
            private boolean connectionId = true;
            private int mtu = 1_200;
            private int window = 64;
            private java.time.Duration idleTimeout = java.time.Duration.ofSeconds(60);
            private int maxHalfOpen = 1_024;
            private int helloRatePerSource = 8;

            /**
             * {@code identity} (the default: the identity-endorsed channel
             * certificate, or CA mode when a trust store is set) or {@code none}
             * (encryption only, never attesting).
             */
            public String getAttestation() {
                return attestation;
            }

            public void setAttestation(String attestation) {
                this.attestation = attestation;
            }

            /** DTLS versions offered; DTLSv1.2 is what BouncyCastle negotiates today. */
            public java.util.List<String> getProtocols() {
                return protocols;
            }

            public void setProtocols(java.util.List<String> protocols) {
                this.protocols = protocols;
            }

            /** Negotiate RFC 9146 Connection ID, so associations survive NAT rebinding. */
            public boolean isConnectionId() {
                return connectionId;
            }

            public void setConnectionId(boolean connectionId) {
                this.connectionId = connectionId;
            }

            /** Datagram budget in bytes (576 to 9,000). */
            public int getMtu() {
                return mtu;
            }

            public void setMtu(int mtu) {
                this.mtu = mtu;
            }

            /** DATA PDUs in flight per association (4 to 1,024). */
            public int getWindow() {
                return window;
            }

            public void setWindow(int window) {
                this.window = window;
            }

            /** Silence after which an association fails (10 s to 10 min); keepalive at a quarter of it. */
            public java.time.Duration getIdleTimeout() {
                return idleTimeout;
            }

            public void setIdleTimeout(java.time.Duration idleTimeout) {
                this.idleTimeout = idleTimeout;
            }

            /** Unfinished handshakes per listener (16 to 65,536). */
            public int getMaxHalfOpen() {
                return maxHalfOpen;
            }

            public void setMaxHalfOpen(int maxHalfOpen) {
                this.maxHalfOpen = maxHalfOpen;
            }

            /** ClientHellos accepted per second from one source address (1 to 1,000). */
            public int getHelloRatePerSource() {
                return helloRatePerSource;
            }

            public void setHelloRatePerSource(int helloRatePerSource) {
                this.helloRatePerSource = helloRatePerSource;
            }
        }

        public String getChannelAuth() {
            return channelAuth;
        }

        public void setChannelAuth(String channelAuth) {
            this.channelAuth = channelAuth;
        }

        public Tls getTls() {
            return tls;
        }

        public void setTls(Tls tls) {
            this.tls = tls;
        }

        /**
         * TLS settings for the TCP transport ({@code agentspaces.transport.tls.*}).
         * Without a trust store the transport runs the default self-signed
         * identity-endorsed mode. Configuring {@code trust-store} switches to
         * the enterprise-CA mode (remediation plan §3): peers attest only when
         * their chain validates to the store's CA certificates, with
         * revocation checked against the {@code crls} files (cached documents,
         * refreshed out of band) or fetched live under
         * {@code revocation-strict}. {@code key-store} supplies this node's
         * CA-issued credential (PKCS#12/JKS) from the organization's
         * enrollment pipeline.
         */
        public static class Tls {

            private Boolean enabled;
            private String keyStore;
            private String keyStorePassword;
            private String trustStore;
            private String trustStorePassword;
            private java.util.List<String> crls = java.util.List.of();
            private boolean revocationStrict;
            private boolean requireAttestation;
            private String revocationValidator = "founder";
            private java.time.Duration crlRefresh = java.time.Duration.ofMinutes(5);

            /**
             * Who may revoke a peer fleet-wide (SPEC §6.1, v0.1.13): {@code founder}
             * (the default) or {@code founder-or-ca}, under which any member may
             * relay the CA's revocation of a peer's channel certificate, proven
             * by the chain and CRL it carries, and this node roots one itself
             * when its CRLs revoke a connected peer. Needs a {@code trust-store}.
             */
            public String getRevocationValidator() {
                return revocationValidator;
            }

            public void setRevocationValidator(String revocationValidator) {
                this.revocationValidator = revocationValidator;
            }

            /** How often the {@code crls} files are checked for replacement (5m). */
            public java.time.Duration getCrlRefresh() {
                return crlRefresh;
            }

            public void setCrlRefresh(java.time.Duration crlRefresh) {
                this.crlRefresh = crlRefresh;
            }

            public Boolean getEnabled() {
                return enabled;
            }

            public void setEnabled(Boolean enabled) {
                this.enabled = enabled;
            }

            public String getKeyStore() {
                return keyStore;
            }

            public void setKeyStore(String keyStore) {
                this.keyStore = keyStore;
            }

            public String getKeyStorePassword() {
                return keyStorePassword;
            }

            public void setKeyStorePassword(String keyStorePassword) {
                this.keyStorePassword = keyStorePassword;
            }

            public String getTrustStore() {
                return trustStore;
            }

            public void setTrustStore(String trustStore) {
                this.trustStore = trustStore;
            }

            public String getTrustStorePassword() {
                return trustStorePassword;
            }

            public void setTrustStorePassword(String trustStorePassword) {
                this.trustStorePassword = trustStorePassword;
            }

            public java.util.List<String> getCrls() {
                return crls;
            }

            public void setCrls(java.util.List<String> crls) {
                this.crls = crls;
            }

            public boolean isRevocationStrict() {
                return revocationStrict;
            }

            public void setRevocationStrict(boolean revocationStrict) {
                this.revocationStrict = revocationStrict;
            }

            /**
             * Whether transport attestation is a condition of service (spec
             * §5.6, gate2-review G2-1). With this on, a frame is served only
             * when the connection it arrived on is attested for its signed
             * sender, and a current member whose fresh handshake attests
             * nothing is evicted, which is how a CA's revocation becomes the
             * fleet's eject. It needs an attesting transport, so the node
             * registers only TLS while it is set and refuses to start when TLS
             * is off. Default false.
             *
             * @return whether attestation is required
             */
            public boolean isRequireAttestation() {
                return requireAttestation;
            }

            public void setRequireAttestation(boolean requireAttestation) {
                this.requireAttestation = requireAttestation;
            }
        }
    }

    /**
     * The Embabel bridge ({@code agentspaces.embabel.*}). With Embabel on the
     * classpath and {@code remote-actions-enabled} true (the default), the
     * fleet's discovered AgentCards are exposed as a generated {@code @Agent}
     * whose typed {@code @Action} methods the GOAP planner plans over; each
     * invocation is a leased space round-trip.
     */
    public static class Embabel {

        private boolean remoteActionsEnabled = true;
        private boolean autoDeploy = true;
        private long deployPollMillis = 2_000;
        private long remoteActionTimeoutMillis = 30_000;
        private String remoteAgentName = "remoteFleet";
        private String remoteAgentDescription =
                "Capabilities other fleet members advertise through AgentCards";

        public boolean isRemoteActionsEnabled() {
            return remoteActionsEnabled;
        }

        public void setRemoteActionsEnabled(boolean remoteActionsEnabled) {
            this.remoteActionsEnabled = remoteActionsEnabled;
        }

        /**
         * Whether the generated remote-fleet agent deploys onto an Embabel
         * {@code AgentPlatform} bean automatically when one is present (spec
         * §10.6): a {@code SmartLifecycle} watches the fleet's cards and
         * redeploys as the advertised action set changes. Default on.
         */
        public boolean isAutoDeploy() {
            return autoDeploy;
        }

        public void setAutoDeploy(boolean autoDeploy) {
            this.autoDeploy = autoDeploy;
        }

        /** How often the auto-deployer re-checks the fleet's cards. */
        public long getDeployPollMillis() {
            return deployPollMillis;
        }

        public void setDeployPollMillis(long deployPollMillis) {
            this.deployPollMillis = deployPollMillis;
        }

        public long getRemoteActionTimeoutMillis() {
            return remoteActionTimeoutMillis;
        }

        public void setRemoteActionTimeoutMillis(long remoteActionTimeoutMillis) {
            this.remoteActionTimeoutMillis = remoteActionTimeoutMillis;
        }

        public String getRemoteAgentName() {
            return remoteAgentName;
        }

        public void setRemoteAgentName(String remoteAgentName) {
            this.remoteAgentName = remoteAgentName;
        }

        public String getRemoteAgentDescription() {
            return remoteAgentDescription;
        }

        public void setRemoteAgentDescription(String remoteAgentDescription) {
            this.remoteAgentDescription = remoteAgentDescription;
        }
    }

    /**
     * The A2A gateway ({@code agentspaces.a2a.*}, TODO item 7): off by default.
     * On, the peer serves the fleet's AgentCards as A2A Agent Cards and A2A
     * JSON-RPC tasks bound to {@code space}. The bearer gate follows the
     * console's rule: a non-blank static {@code token} wins; otherwise, when
     * {@code agentspaces.security.oidc.*} is configured, a JWT validator over
     * the identity provider's keys requiring {@code required-scope}; otherwise
     * the gateway is ungated, which startup permits only on a loopback
     * {@code bind}.
     */
    public static class A2a {

        private boolean enabled;
        private String bind = "127.0.0.1";
        private int port = 7594;
        private String group = "";
        private String space = "";
        private String fleetName = "";
        private String provider = "AgentSpaces";
        private String externalBaseUrl = "";
        private String requiredScope = "aspace:a2a:client";
        private String token = "";
        private List<String> allowedWebhookHosts = new ArrayList<>();
        private long taskLeaseMillis = 600_000;
        private long resultLeaseMillis = 3_600_000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getBind() {
            return bind;
        }

        public void setBind(String bind) {
            this.bind = bind;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getSpace() {
            return space;
        }

        public void setSpace(String space) {
            this.space = space;
        }

        public String getFleetName() {
            return fleetName;
        }

        public void setFleetName(String fleetName) {
            this.fleetName = fleetName;
        }

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public String getExternalBaseUrl() {
            return externalBaseUrl;
        }

        public void setExternalBaseUrl(String externalBaseUrl) {
            this.externalBaseUrl = externalBaseUrl;
        }

        public String getRequiredScope() {
            return requiredScope;
        }

        public void setRequiredScope(String requiredScope) {
            this.requiredScope = requiredScope;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public List<String> getAllowedWebhookHosts() {
            return allowedWebhookHosts;
        }

        public void setAllowedWebhookHosts(List<String> allowedWebhookHosts) {
            this.allowedWebhookHosts = allowedWebhookHosts;
        }

        public long getTaskLeaseMillis() {
            return taskLeaseMillis;
        }

        public void setTaskLeaseMillis(long taskLeaseMillis) {
            this.taskLeaseMillis = taskLeaseMillis;
        }

        public long getResultLeaseMillis() {
            return resultLeaseMillis;
        }

        public void setResultLeaseMillis(long resultLeaseMillis) {
            this.resultLeaseMillis = resultLeaseMillis;
        }
    }

    /**
     * The served fleet console ({@code agentspaces.console.*}): off by
     * default, on with {@code agentspaces.console.enabled=true}, at which
     * point the peer serves the console UI, the HAL+JSON API, and the SSE
     * activity stream on the configured port.
     */
    public static class Console {

        private boolean enabled;
        private int port = 7590;
        private String fleetName = "";
        private Command command = new Command();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getFleetName() {
            return fleetName;
        }

        public void setFleetName(String fleetName) {
            this.fleetName = fleetName;
        }

        public Command getCommand() {
            return command;
        }

        public void setCommand(Command command) {
            this.command = command;
        }
    }

    /**
     * Command-and-control settings ({@code agentspaces.console.command.*}). C2
     * turns on when {@code enabled=true} and the command routes have a gate:
     * a non-blank static {@code token}, or — when the token is blank and
     * {@code agentspaces.security.oidc.*} is configured — a JWT validator over
     * the identity provider's keys, under which each request's bearer token
     * is validated, its {@code sub} is the audited operator, and its scopes
     * must include {@code required-scope} (TODO-EFG §5). A static token always
     * wins when set. The gate protects the HTTP edge; commands execute under
     * the console peer's own signed identity.
     */
    public static class Command {

        private boolean enabled;
        private String token = "";
        private String requiredScope = "aspace:console:operate";
        private String controlSpace = "fleet-control";
        private String auditSpace = "c2-audit";

        public String getRequiredScope() {
            return requiredScope;
        }

        public void setRequiredScope(String requiredScope) {
            this.requiredScope = requiredScope;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public String getControlSpace() {
            return controlSpace;
        }

        public void setControlSpace(String controlSpace) {
            this.controlSpace = controlSpace;
        }

        public String getAuditSpace() {
            return auditSpace;
        }

        public void setAuditSpace(String auditSpace) {
            this.auditSpace = auditSpace;
        }
    }
}
