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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.MembershipValidator;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.bootstrap.MulticastBeacon;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * A peer: one process's presence in the AgentSpaces fabric (spec §5). A node
 * holds the peer identity, the configured transports, one {@link GroupRuntime}
 * per joined group, and the frame plumbing between them: every outbound frame is
 * a CBOR envelope carrying an Ed25519 signature, or traveling unsigned on a
 * channel the transport authenticated for exactly its sender (spec §5.6), and
 * every inbound frame passes that same check before any handler sees it.
 *
 * <p>Progress is tick-driven: {@link #tick()} runs one membership probe round
 * and one self-advertisement refresh per group, plus one anti-entropy round
 * per group whenever that group's gossip period has elapsed on the node's
 * clock (spec §5.3). Tests call it explicitly with a test clock; wall-clock
 * deployments call {@code startTicking}.
 */
public final class PeerNode implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(PeerNode.class.getName());

    /**
     * The tick cadence assumed when a host drives {@link #tick()} by hand rather
     * than through {@link #startTicking(Duration)}, and the starter's default
     * for {@code agentspaces.tick-millis}.
     */
    public static final Duration DEFAULT_TICK_PERIOD = Duration.ofMillis(250);

    /**
     * How this node authenticates frames on its connections (spec §5.6).
     * {@code SIGNED} is the classic mode: every outbound frame carries an
     * Ed25519 envelope signature. {@code ATTESTED} additionally announces, on
     * every channel the transport authenticated, that unsigned frames are
     * acceptable there, and elides the signature toward peers that announced
     * the same. Inbound acceptance is identical in both modes: bare frames
     * pass only on a connection whose transport attested their sender.
     */
    public enum ChannelAuth {
        SIGNED, ATTESTED
    }

    /** Wire protocol version this node emits and the only one it accepts
     * (spec §9); see {@link WireCodec#WIRE_VERSION}. */
    static final int WIRE_VERSION = WireCodec.WIRE_VERSION;

    /**
     * How far in the past a frame's stamp may be before it is rejected as stale
     * (ASF-010 replay defence). Generous against clock skew and delivery delay,
     * but far short of a window in which a captured frame is useful to replay.
     */
    private static final long MAX_FRAME_AGE_MILLIS = 300_000L;

    /** Bound on the recent-frame dedup set; oldest entries evict (ASF-019). */
    private static final int RECENT_FRAMES_CAPACITY = 8_192;

    /** The gossip stream peer advertisements travel on. */
    private static final String PEER_AD_STREAM = "peers";

    /** How far in the future an ad's issuer-signed {@code issued} may run
     * before the ad is refused (ASF-027). */
    private static final Duration MAX_AD_ISSUED_SKEW = Duration.ofMinutes(10);

    /** The gossip stream revocations travel on (remediation plan §9). */
    private static final String REVOCATION_STREAM = "revocations";

    /** The gossip stream credential revocations travel on (SPEC §6.1, v0.1.13). */
    static final String CREDENTIAL_REVOCATION_STREAM = "credential-revocations";

    /** How long a revocation keeps re-gossiping; the withdrawal of trust it
     * carries never lapses — anti-entropy converges late joiners regardless. */
    private static final Duration REVOCATION_GOSSIP_TTL = Duration.ofDays(30);

    private final PeerIdentity identity;
    private final InstantSource clock;
    private final Random random;
    private final CborCodec codec;
    private final WireCodec wire;
    private final HybridLogicalClock hlc;
    private final Duration peerAdTtl;
    private final ChannelAuth channelAuth;
    /** The revocation authority check (remediation plan §9): founder-rooted by
     * default; enterprise deployments swap in an IdP- or CA-backed validator. */
    private final RevocationRegistry.RevocationValidator revocationValidator;
    /** The authority ranking for credential revocations (v0.1.13). */
    private final ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry.Validator
            credentialRevocationValidator;
    /** The enterprise-CA trust this node judges presented chains by (v0.1.13), or null. */
    private final java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> channelTrust;
    /** The trust instance connections were last judged under; a new one triggers a re-judge. */
    private volatile ai.badmonkey.agentspaces.identity.ChannelTrust judgedUnder;
    /** Peers this node has rooted a CA revocation for: at most one each, so no storms. */
    private final Set<PeerId> caRevoked = ConcurrentHashMap.newKeySet();
    /** Connections whose remote end presented a chain, for re-judging; weak, so closed ones drop away. */
    private final Set<TransportConnection> chained = java.util.Collections.synchronizedSet(
            java.util.Collections.newSetFromMap(new java.util.WeakHashMap<>()));
    /** Per group, the hash of the join credential each member was admitted under. */
    private final Map<GroupId, Map<PeerId, String>> admittedUnder = new ConcurrentHashMap<>();
    /** v0.1.9 CA channel mode: admit and serve peers only over connections the
     * transport attested for them, and evict a member whose fresh handshake
     * attests nothing (the CA's revocation as the authoritative eject). */
    private final boolean requireAttestation;
    private final double rateCapacity;
    private final double rateRefillPerSecond;
    private final LongSupplier nanos;
    /** Opt-in LAN bootstrap beacon (spec §10.1), or null. */
    private final MulticastBeacon beacon;
    private final Duration beaconInterval;
    private volatile Long lastBeaconMillis;
    /** Frames the wire codec refused (bad signature, unknown version, garbage). */
    private final AtomicLong framesDropped = new AtomicLong();
    /** Join-by-GroupID fetches awaiting a verified founding advertisement. */
    private final Map<GroupId, CompletableFuture<SignedGroupAdvertisement>> pendingFounding =
            new ConcurrentHashMap<>();
    private final Set<PeerAdvertisement.PeerRole> roles;
    private final Map<String, Transport> transports = new ConcurrentHashMap<>();
    private final List<AutoCloseable> listeners = new ArrayList<>();
    private final List<PeerAdvertisement.Endpoint> advertisedEndpoints = new ArrayList<>();
    private final Map<PeerId, TransportConnection> connections = new ConcurrentHashMap<>();
    private final Map<GroupId, GroupRuntime> groups = new ConcurrentHashMap<>();
    /** Per-group resource hints (e.g. an INVITE join credential) put in our self-ad. */
    private final Map<GroupId, Map<String, String>> groupHints = new ConcurrentHashMap<>();
    /** Node-wide credential hints (e.g. this peer's OIDC token under
     * {@link JoinCredentials#OIDC_HINT_KEY}) merged into every group's self-ad;
     * a group's own hints win on a key clash. */
    private final Map<String, String> nodeHints = new ConcurrentHashMap<>();
    /** Listeners told about a member's resource hints on admission and on
     * every refresh that changes them (TODO-EFG §4, token ingestion). */
    private final List<BiConsumer<PeerId, Map<String, String>>> credentialHintListeners =
            new CopyOnWriteArrayList<>();
    /** Bound on remembered hint maps; least-recently-refreshed evict. */
    private static final int HINTS_CAPACITY = 4_096;
    /** The hints last delivered per peer, so an unchanged refresh is not re-delivered. */
    private final Map<PeerId, Map<String, String>> lastHints =
            java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(256, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(
                                Map.Entry<PeerId, Map<String, String>> eldest) {
                            return size() > HINTS_CAPACITY;
                        }
                    });
    /** Per-group admission validators for POLICY groups. */
    private final Map<GroupId, MembershipValidator> groupValidators = new ConcurrentHashMap<>();
    /** Bound on per-issuer bucket state; least-recently-active evict (ASF-019). */
    private static final int RATE_BUCKETS_CAPACITY = 4_096;

    /** Per-issuer inbound token buckets: a coarse backstop against frame floods.
     * Only admitted members reach this map (the dispatch gate runs first), and
     * it is LRU-bounded so departed members' buckets age out (ASF-019). */
    private final Map<PeerId, TokenBucket> rateBuckets =
            java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(256, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(
                                Map.Entry<PeerId, TokenBucket> eldest) {
                            return size() > RATE_BUCKETS_CAPACITY;
                        }
                    });
    /** One shared inbound budget for every peer outside the membership view:
     * bootstrap introductions are rare and cheap, so hostile strangers cannot
     * sum to unbounded work or unbounded per-peer bucket state (ASF-003). */
    private final TokenBucket bootstrapBucket;

    /** Witnessed protocol violations before a peer is quarantined (WS5). Every
     * strike is misbehavior this node saw itself on an authenticated frame —
     * never hearsay — so quarantine cannot be weaponized against honest peers
     * by third parties. The threshold absorbs rare benign anomalies (a clock
     * step tripping the staleness window) without letting an attacker probe
     * freely. */
    static final int STRIKE_THRESHOLD = 16;
    /** How long a quarantined peer's frames are refused before local evidence
     * expires. Long enough for the enterprise revocation path to act, short
     * enough that a misjudged honest peer recovers on its own. */
    static final Duration QUARANTINE = Duration.ofMinutes(10);
    /** Bound on tracked strike counters; least-recently-struck evict. */
    private static final int STRIKES_CAPACITY = 1_024;

    /** Strikes per peer; bounded LRU. Synchronized on the map. */
    private final Map<PeerId, Integer> strikes =
            java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(64, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(
                                Map.Entry<PeerId, Integer> eldest) {
                            return size() > STRIKES_CAPACITY;
                        }
                    });
    /** Quarantined peers and when their local sentence lapses (epoch millis). */
    private final Map<PeerId, Long> quarantinedUntil = new ConcurrentHashMap<>();
    /** Outstanding PING_REQ relays, bounded (timed-out entries age out by
     * eviction), keyed by unguessable nonces (ASF-011). */
    private final Map<Long, Relay> relays = java.util.Collections.synchronizedMap(
            new java.util.LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Relay> eldest) {
                    return size() > 1_024;
                }
            });
    private final java.security.SecureRandom relayNonce = new java.security.SecureRandom();
    /**
     * Recently seen {@code (from, stamp)} frame identities, for exact-replay
     * rejection (ASF-010). Bounded, insertion-order eviction; access is
     * synchronized on the map. Each honest frame has a unique stamp (the HLC
     * advances on every {@code now()}), so this never drops a distinct frame.
     */
    private final Map<String, Boolean> recentFrames =
            java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<>(1024, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > RECENT_FRAMES_CAPACITY;
                }
            });
    /** Connections we already announced CHANNEL_HELLO on (identity keys). */
    private final Set<TransportConnection> helloSent = ConcurrentHashMap.newKeySet();
    /** Attested peer per connection that announced it accepts bare frames. */
    private final Map<TransportConnection, PeerId> bareAccepted = new ConcurrentHashMap<>();
    private final AtomicLong signedFramesSent = new AtomicLong();
    private final AtomicLong bareFramesSent = new AtomicLong();
    private java.util.concurrent.ScheduledExecutorService ticker;
    /** The period {@link #startTicking} was called with; null while hand-driven. */
    private volatile Duration tickPeriod;

    private record Relay(PeerId origin, long originNonce, PeerId target) {
    }

    /**
     * A per-issuer token bucket bounding inbound frames (spec §11). It refills
     * against a monotonic nanosecond source — {@link System#nanoTime} by
     * default, never the injected instant clock — so it throttles a flood in
     * production while never tripping on a deterministic-clock test that
     * bursts ticks; tests that want to pin the limit inject a frozen source
     * through {@link Builder#rateLimit}. The defaults are a coarse backstop: a
     * burst of {@code DEFAULT_CAPACITY} frames and a sustained
     * {@code DEFAULT_REFILL_PER_SEC} per issuer, both far above any honest
     * peer's rate and far below what an unbounded flood would send.
     */
    static final class TokenBucket {
        static final double DEFAULT_CAPACITY = 50_000.0;
        static final double DEFAULT_REFILL_PER_SEC = 10_000.0;

        private final double capacity;
        private final double refillPerSecond;
        private final LongSupplier nanos;
        private double tokens;
        private long lastNanos;

        TokenBucket(double capacity, double refillPerSecond, LongSupplier nanos) {
            this.capacity = capacity;
            this.refillPerSecond = refillPerSecond;
            this.nanos = nanos;
            this.tokens = capacity;
            this.lastNanos = nanos.getAsLong();
        }

        synchronized boolean tryAcquire() {
            long now = nanos.getAsLong();
            double elapsedSeconds = Math.max(0, now - lastNanos) / 1_000_000_000.0;
            lastNanos = now;
            tokens = Math.min(capacity, tokens + elapsedSeconds * refillPerSecond);
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }
    }

    private PeerNode(Builder builder) {
        this.identity = builder.identity;
        this.clock = builder.clock;
        this.random = builder.random;
        this.codec = CborCodec.defaultCodec();
        this.wire = new WireCodec(codec);
        this.hlc = new HybridLogicalClock(builder.clock, builder.identity.peerId().value());
        this.peerAdTtl = builder.peerAdTtl;
        this.channelAuth = builder.channelAuth;
        this.revocationValidator = builder.revocationValidator;
        this.credentialRevocationValidator = builder.credentialRevocationValidator;
        this.channelTrust = builder.channelTrust;
        this.requireAttestation = builder.requireAttestation;
        this.rateCapacity = builder.rateCapacity;
        this.rateRefillPerSecond = builder.rateRefillPerSecond;
        this.nanos = builder.nanos;
        this.bootstrapBucket = new TokenBucket(TokenBucket.DEFAULT_CAPACITY,
                TokenBucket.DEFAULT_REFILL_PER_SEC, nanos);
        this.roles = Set.copyOf(builder.roles);
        this.beaconInterval = builder.beaconInterval;
        MulticastBeacon.Datagrams carrier = builder.beaconCarrier;
        if (carrier == null && builder.multicastGroup != null) {
            try {
                carrier = MulticastBeacon.multicast(builder.multicastGroup);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(
                        "cannot open the multicast beacon on " + builder.multicastGroup, e);
            }
        }
        this.beacon = carrier == null ? null
                : new MulticastBeacon(carrier, this::beaconAnnouncements, this::onBeaconDatagram);
        this.credentialHintListeners.addAll(builder.credentialHintListeners);
    }

    /**
     * Starts building a node.
     *
     * @param identity the peer identity
     * @return the builder
     */
    public static Builder builder(PeerIdentity identity) {
        return new Builder(identity);
    }

    /** Builder for {@link PeerNode}. */
    public static final class Builder {
        private final PeerIdentity identity;
        private InstantSource clock = InstantSource.system();
        private Random random = new Random();
        private Duration peerAdTtl = Duration.ofMinutes(10);
        private ChannelAuth channelAuth = ChannelAuth.SIGNED;
        private RevocationRegistry.RevocationValidator revocationValidator =
                RevocationRegistry.RevocationValidator.founderRooted();
        private java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> channelTrust;
        private ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry.Validator
                credentialRevocationValidator =
                ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry.Validator.defaults();
        private Set<PeerAdvertisement.PeerRole> roles = Set.of();
        private boolean requireAttestation;
        private double rateCapacity = TokenBucket.DEFAULT_CAPACITY;
        private double rateRefillPerSecond = TokenBucket.DEFAULT_REFILL_PER_SEC;
        private LongSupplier nanos = System::nanoTime;
        private java.net.InetSocketAddress multicastGroup;
        private MulticastBeacon.Datagrams beaconCarrier;
        private Duration beaconInterval = Duration.ofSeconds(5);
        private final List<BiConsumer<PeerId, Map<String, String>>> credentialHintListeners =
                new ArrayList<>();

        private Builder(PeerIdentity identity) {
            this.identity = Objects.requireNonNull(identity, "identity");
        }

        /**
         * Registers a listener for members' credential hints (TODO-EFG §4):
         * the resource hints of a peer's verified, admitted self-advertisement
         * are delivered when the peer is admitted and again whenever a refreshed
         * advertisement changes them (an unchanged refresh is not re-delivered).
         * The hints are authentic — they travel inside the signed
         * self-advertisement — but the node interprets none of them; the
         * listener does, typically by handing {@link JoinCredentials#OIDC_HINT_KEY}
         * to an {@code OidcAuthorizer}. Listeners run on the frame-dispatch path
         * and must be quick.
         *
         * @param listener receives the peer and its current hint map
         * @return this builder
         */
        public Builder onCredentialHints(BiConsumer<PeerId, Map<String, String>> listener) {
            credentialHintListeners.add(Objects.requireNonNull(listener, "listener"));
            return this;
        }

        /**
         * Injects the time source.
         *
         * @param clock the time source
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Seeds the node's randomness, for deterministic tests.
         *
         * @param seed the seed
         * @return this builder
         */
        public Builder randomSeed(long seed) {
            this.random = new Random(seed);
            return this;
        }

        /**
         * Sets the TTL of this peer's self-advertisement (spec §6.2 default 10 min).
         *
         * @param ttl the TTL
         * @return this builder
         */
        public Builder peerAdTtl(Duration ttl) {
            this.peerAdTtl = Objects.requireNonNull(ttl, "ttl");
            return this;
        }

        /**
         * The enterprise-CA trust this node judges presented chains by (SPEC
         * §5.6, v0.1.13, item 9): pass the same refreshable supplier the
         * transports attest with. Each new connection's chain, and every live
         * one whenever the supplier yields a new trust (a CRL refresh), is
         * judged; a leaf revoked for a reason that withdraws the identity makes
         * this node root a peer revocation in the CA, with the chain and CRL as
         * evidence, once per peer, and cut the link. Issuance needs a
         * revocation validator that accepts CA-rooted revocations.
         *
         * @param trust the trust supplier
         * @return this builder
         */
        public Builder channelTrust(
                java.util.function.Supplier<ai.badmonkey.agentspaces.identity.ChannelTrust> trust) {
            this.channelTrust = Objects.requireNonNull(trust, "trust");
            return this;
        }

        /**
         * Replaces the credential-revocation authority ranking (SPEC §6.1,
         * v0.1.13): the default ranks the founder, then an agent's own peer
         * for that agent and its keys; a CA-rooted validator adds trust-root
         * evidence.
         *
         * @param validator the ranking
         * @return this builder
         */
        public Builder credentialRevocationValidator(
                ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry.Validator validator) {
            this.credentialRevocationValidator = Objects.requireNonNull(validator, "validator");
            return this;
        }

        /**
         * Replaces the revocation authority check (remediation plan §9): the
         * seam that swaps trust roots — founder key, identity provider, CA —
         * without touching enforcement.
         *
         * @param validator the authority check
         * @return this builder
         */
        public Builder revocationValidator(
                RevocationRegistry.RevocationValidator validator) {
            this.revocationValidator = Objects.requireNonNull(validator, "validator");
            return this;
        }

        /**
         * Sets the channel-authentication mode (spec §5.6). The default,
         * {@link ChannelAuth#SIGNED}, signs every frame. {@link ChannelAuth#ATTESTED}
         * negotiates unsigned frames over transport-authenticated channels,
         * removing the per-frame Ed25519 cost on those links; the evidence
         * signatures inside entries, claims, and advertisements are unaffected.
         *
         * @param channelAuth the mode
         * @return this builder
         */
        public Builder channelAuth(ChannelAuth channelAuth) {
            this.channelAuth = Objects.requireNonNull(channelAuth, "channelAuth");
            return this;
        }

        /**
         * Makes transport attestation a condition of admission and service
         * (spec v0.1.9, the enterprise-CA channel mode). With {@code true},
         * every inbound frame must arrive on a connection whose
         * {@code attestedPeer()} names the frame's sender; frames on
         * unattested connections, or on connections attested for someone
         * else, are logged and dropped before any side effect — including the
         * bootstrap self-introduction, so a peer joins only over a channel the
         * CA vouched for. A current member whose signed frame arrives on an
         * <em>unattested</em> connection has just completed a handshake that
         * attests nothing (its certificate was revoked, expired, or replaced):
         * it is evicted from the membership view, its cached connection is
         * closed, and it is refused at dispatch until a frame of its arrives on
         * a connection attested for it again. Relayed inner frames carry no
         * attestation for their origin and are dropped in this mode. Peer
         * advertisements forwarded by third parties admit nobody new; a peer
         * enters the view only through its own attested channel. Pair this
         * with a {@code TlsTcpTransport} built over a {@code ChannelTrust}.
         * Default {@code false}.
         *
         * @param require whether attestation is required
         * @return this builder
         */
        public Builder requireAttestation(boolean require) {
            this.requireAttestation = require;
            return this;
        }

        /**
         * Tunes the per-issuer inbound rate limit (spec §11) and injects its
         * monotonic time source. A test seam: production keeps the defaults
         * and {@link System#nanoTime}.
         *
         * @param capacity        burst capacity in frames
         * @param refillPerSecond sustained frames per second
         * @param nanos           monotonic nanosecond source
         * @return this builder
         */
        Builder rateLimit(int capacity, int refillPerSecond, LongSupplier nanos) {
            if (capacity <= 0 || refillPerSecond < 0) {
                throw new IllegalArgumentException("capacity must be positive, refill non-negative");
            }
            this.rateCapacity = capacity;
            this.rateRefillPerSecond = refillPerSecond;
            this.nanos = Objects.requireNonNull(nanos, "nanos");
            return this;
        }

        /**
         * Enables the LAN multicast bootstrap beacon (spec §10.1
         * {@code bootstrap: multicast}, roadmap M1; off by default). Every
         * {@code interval} of the node's clock, {@link #tick()} sends one UDP
         * datagram per joined group carrying only this node's signed
         * self-advertisement, and datagrams heard from others are verified and
         * admitted exactly like a stranger's bootstrap rumor, after which the
         * node introduces itself to the advertised endpoints as if they were
         * configured seeds. See {@link MulticastBeacon}.
         *
         * @param group    the multicast group address and port, e.g.
         *                 {@code 239.255.42.99:7787}
         * @param interval how often to announce, on the node's clock
         * @return this builder
         */
        public Builder multicast(java.net.InetSocketAddress group, Duration interval) {
            this.multicastGroup = Objects.requireNonNull(group, "group");
            this.beaconInterval = positive(interval);
            this.beaconCarrier = null;
            return this;
        }

        /**
         * Enables the bootstrap beacon over a custom datagram carrier (a
         * broadcast socket, an in-memory fabric for tests) with the same
         * verification path as {@link #multicast}.
         *
         * @param carrier  the datagram carrier
         * @param interval how often to announce, on the node's clock
         * @return this builder
         */
        public Builder beacon(MulticastBeacon.Datagrams carrier, Duration interval) {
            this.beaconCarrier = Objects.requireNonNull(carrier, "carrier");
            this.beaconInterval = positive(interval);
            this.multicastGroup = null;
            return this;
        }

        private static Duration positive(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("interval must be positive: " + interval);
            }
            return interval;
        }

        /**
         * Declares topology roles this peer serves (spec §5.4), advertised in its
         * leased self-advertisement.
         *
         * @param roles the roles
         * @return this builder
         */
        public Builder roles(Set<PeerAdvertisement.PeerRole> roles) {
            this.roles = Set.copyOf(Objects.requireNonNull(roles, "roles"));
            return this;
        }

        /** Builds the node. */
        public PeerNode build() {
            return new PeerNode(this);
        }
    }

    /** Returns the peer's identity. */
    public PeerIdentity identity() {
        return identity;
    }

    /**
     * Registers a credential-hint listener on a built node; the same contract
     * as {@link Builder#onCredentialHints}, for wiring that learns of the
     * listener after the node exists (the Spring starter's authorizer bean).
     *
     * @param listener receives the peer and its current hint map
     */
    public void onCredentialHints(BiConsumer<PeerId, Map<String, String>> listener) {
        credentialHintListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Sets one of this node's own credential hints (TODO-EFG §4): the value is
     * carried under {@code key} in every group's next signed self-advertisement,
     * so every member's {@linkplain Builder#onCredentialHints hint listener}
     * sees it within one gossip period. Refreshing a token is calling this
     * again with the new value. Hints are bounded only by the advertisement
     * they ride in; keep them small (an OIDC token is typically under 2 KiB).
     * A per-group join hint of the same key takes precedence for that group.
     *
     * @param key   the hint key, e.g. {@link JoinCredentials#OIDC_HINT_KEY}
     * @param value the hint value
     */
    public void credentialHint(String key, String value) {
        nodeHints.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
    }

    /**
     * Removes one of this node's own credential hints; the next
     * self-advertisement no longer carries it.
     *
     * @param key the hint key
     */
    public void removeCredentialHint(String key) {
        nodeHints.remove(Objects.requireNonNull(key, "key"));
    }

    /** Returns the peer's id. */
    /**
     * Whether this node requires transport attestation of every frame's sender
     * (the CA channel mode, spec §5.6). Exposed so a deployment can assert the
     * posture it configured actually reached the node.
     *
     * @return whether attestation is required
     */
    public boolean requiresAttestation() {
        return requireAttestation;
    }

    /**
     * The transport schemes registered on this node, for dialing and for the
     * endpoints it advertises. A node in the attested mode registers only
     * transports that can attest, so an unattestable scheme's absence here is
     * what keeps an honest peer from dialing in on a channel that would get it
     * evicted.
     *
     * @return the registered schemes
     */
    public Set<String> transportSchemes() {
        return Set.copyOf(transports.keySet());
    }

    public PeerId peerId() {
        return identity.peerId();
    }

    /** Returns how many fully signed frames this node has sent. */
    public long signedFramesSent() {
        return signedFramesSent.get();
    }

    /** Returns how many channel-attested (unsigned) frames this node has sent. */
    public long bareFramesSent() {
        return bareFramesSent.get();
    }

    /**
     * Returns how many inbound frames the wire codec refused outright (spec
     * §9): failed signature, half-signed, unknown wire version, or garbage.
     * Such frames name no verified sender, so they count here and strike
     * nobody.
     */
    public long framesDropped() {
        return framesDropped.get();
    }

    /** Witnessed-violation strikes currently held against a peer (WS5), for tests. */
    int strikes(PeerId peer) {
        return strikes.getOrDefault(peer, 0);
    }

    /**
     * Returns the channel-authentication mode per connected peer, for the
     * console and diagnostics: {@code "attested"} when frames to that peer ride
     * an authenticated channel unsigned, {@code "signed"} otherwise.
     *
     * @return peer to mode, a snapshot
     */
    public Map<PeerId, String> channelModes() {
        Map<PeerId, String> modes = new java.util.HashMap<>();
        connections.forEach((peer, connection) -> {
            PeerId announced = bareAccepted.get(connection);
            boolean attested = channelAuth == ChannelAuth.ATTESTED
                    && announced != null
                    && connection.attestedPeer().filter(announced::equals).isPresent();
            modes.put(peer, attested ? "attested" : "signed");
        });
        return modes;
    }

    private void dropConnection(PeerId peer, TransportConnection connection) {
        connections.remove(peer, connection);
        bareAccepted.remove(connection);
        helloSent.remove(connection);
    }

    /**
     * Registers a transport and starts listening on it. The bind address is also
     * advertised to other peers.
     *
     * @param transport   the transport
     * @param bindAddress the transport-specific bind (and advertised) address
     * @throws IOException if the address cannot be bound
     */
    public synchronized void listen(Transport transport, String bindAddress) throws IOException {
        listen(transport, bindAddress, advertisedEndpoints.size());
    }

    /**
     * Registers a transport, starts listening on it, and advertises the bind
     * address at an explicit priority (spec §5.5): dialers try a peer's
     * endpoints in ascending priority, so a node advertising {@code quic} at
     * 0 and {@code tcp} at 1 is reached over QUIC wherever the dialer speaks
     * it. The two-argument overload assigns priorities in listen order.
     *
     * @param transport   the transport
     * @param bindAddress the transport-specific bind (and advertised) address
     * @param priority    the advertised priority; lower dials first
     * @throws IOException if the address cannot be bound
     */
    public synchronized void listen(Transport transport, String bindAddress, int priority)
            throws IOException {
        transports.put(transport.scheme(), transport);
        listeners.add(transport.listen(bindAddress, this::attach));
        advertisedEndpoints.add(new PeerAdvertisement.Endpoint(
                transport.scheme(), bindAddress, priority));
    }

    /**
     * Registers a transport for outbound dialing only, without listening or
     * advertising an endpoint. This is the NAT-restricted posture (spec §5.4): the
     * peer dials out to seeds and members, its self-advertisement carries no
     * endpoints, and peers that need to reach it route through a RELAY-role
     * member instead.
     *
     * @param transport the transport
     */
    public synchronized void transport(Transport transport) {
        transports.put(transport.scheme(), transport);
    }

    /**
     * Joins a locally configured, literal-id group and returns its runtime.
     *
     * <p>This overload performs no self-certification check: the advertisement
     * is this node's own configuration, handed in by the operator, so the
     * network never had a chance to swap its policy. It is the right call for
     * fleets whose members all ship the same configured group, and for tests.
     * A group whose advertisement must be <em>learned</em> — fetched from a
     * seed by GroupID, or received in any other way from the network — must
     * instead go through {@link #joinGroup(SignedGroupAdvertisement,
     * GroupMembership.Config, List)} or {@link #joinGroup(GroupId,
     * GroupMembership.Config, List, Duration)}, which verify the founding
     * document (spec §4.4, §5.1). A literal-id group is never served to a
     * newcomer asking by GroupID.
     *
     * @param groupAd          the group's configured advertisement
     * @param membershipConfig membership tuning
     * @param seeds            bootstrap endpoints of any existing members; empty
     *                         for the first member
     * @return the group runtime
     */
    public GroupRuntime joinGroup(GroupAdvertisement groupAd,
                                  GroupMembership.Config membershipConfig,
                                  List<PeerAdvertisement.Endpoint> seeds) {
        return joinGroup(groupAd, membershipConfig, seeds, Map.of(), null);
    }

    /**
     * Joins a self-certifying group with its signed founding advertisement
     * (spec §4.4, §5.1), verifying it first: the founder's key must hash to
     * the issuer, the signature must verify over the founding fields, and the
     * GroupID must equal the hash of the founding document. A document that
     * fails is refused and nothing is joined. Members joined this way serve the
     * document to newcomers that ask by GroupID.
     *
     * @param founding         the signed founding advertisement
     * @param membershipConfig membership tuning
     * @param seeds            bootstrap endpoints of existing members
     * @return the group runtime
     * @throws IllegalArgumentException when the founding document does not verify
     */
    public GroupRuntime joinGroup(SignedGroupAdvertisement founding,
                                  GroupMembership.Config membershipConfig,
                                  List<PeerAdvertisement.Endpoint> seeds) {
        return joinGroup(founding, membershipConfig, seeds, Map.of(), null);
    }

    /**
     * Joins a self-certifying group, verifying its founding advertisement and
     * presenting membership credentials and a POLICY validator as in
     * {@link #joinGroup(GroupAdvertisement, GroupMembership.Config, List, Map,
     * MembershipValidator)}.
     *
     * @param founding         the signed founding advertisement
     * @param membershipConfig membership tuning
     * @param seeds            bootstrap endpoints of existing members
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @return the group runtime
     * @throws IllegalArgumentException when the founding document does not verify
     */
    public GroupRuntime joinGroup(SignedGroupAdvertisement founding,
                                  GroupMembership.Config membershipConfig,
                                  List<PeerAdvertisement.Endpoint> seeds,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator) {
        Objects.requireNonNull(founding, "founding");
        if (!GroupFounding.verify(founding)) {
            throw new IllegalArgumentException("refusing to join " + founding.advertisement().group()
                    + ": the advertisement is not the self-certifying founding document of that group");
        }
        return join(founding.advertisement(), founding, membershipConfig, seeds,
                membershipHints, validator);
    }

    /**
     * Joins a group knowing only its GroupID and seed endpoints (spec §10.1
     * {@code join: "aspace://<groupID>"}): asks each seed for the founding
     * advertisement ({@code GROUP_AD_WANT}), waits for the first answer that
     * verifies as the self-certifying founding document of exactly that
     * GroupID (spec §4.4, §5.1), and then joins with it as
     * {@link #joinGroup(SignedGroupAdvertisement, GroupMembership.Config,
     * List)} would. Answers that fail verification — a different policy under
     * the same id, a forged signature — are ignored, so a hostile seed can
     * delay the join but never substitute the group; only honest seeds that
     * themselves joined with the founding document ever answer.
     *
     * @param groupId          the self-certifying GroupID to join
     * @param membershipConfig membership tuning
     * @param seeds            seed endpoints to ask and then introduce ourselves to
     * @param timeout          wall-clock bound on the fetch
     * @return the group runtime
     * @throws TimeoutException      when no seed served a verified founding
     *                               advertisement within the timeout
     * @throws IllegalStateException when the group is already joined or a
     *                               fetch for it is already in progress
     */
    public GroupRuntime joinGroup(GroupId groupId,
                                  GroupMembership.Config membershipConfig,
                                  List<PeerAdvertisement.Endpoint> seeds,
                                  Duration timeout) throws TimeoutException {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(seeds, "seeds");
        Objects.requireNonNull(timeout, "timeout");
        if (groups.containsKey(groupId)) {
            throw new IllegalStateException("already a member of " + groupId);
        }
        CompletableFuture<SignedGroupAdvertisement> pending = new CompletableFuture<>();
        if (pendingFounding.putIfAbsent(groupId, pending) != null) {
            throw new IllegalStateException("a join of " + groupId + " is already in progress");
        }
        List<TransportConnection> asked = new ArrayList<>();
        try {
            for (PeerAdvertisement.Endpoint seed : seeds) {
                Transport transport = transports.get(seed.transport());
                if (transport == null) {
                    continue;
                }
                try {
                    TransportConnection connection = transport.dial(seed.address());
                    asked.add(connection);
                    attach(connection);
                    // Unaddressed: the seed's PeerID is not known until it answers.
                    sendOn(connection, groupId, null, Envelope.Kind.GROUP_AD_WANT, new byte[0]);
                } catch (IOException e) {
                    // Seed unreachable; another may answer.
                }
            }
            SignedGroupAdvertisement founding;
            try {
                founding = pending.get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TimeoutException("interrupted while fetching the founding advertisement of "
                        + groupId);
            } catch (java.util.concurrent.ExecutionException e) {
                throw new IllegalStateException(e.getCause());
            }
            return joinGroup(founding, membershipConfig, seeds);
        } finally {
            pendingFounding.remove(groupId, pending);
            asked.forEach(TransportConnection::close);
        }
    }

    /**
     * Joins a group, presenting membership credentials and (for POLICY groups)
     * an admission validator (spec §5.1). The {@code membershipHints} travel in
     * this peer's signed self-advertisement so other members can admit it: an
     * INVITE group expects {@link JoinCredentials#HINT_KEY} to carry a
     * founder-issued credential. The {@code validator}, when non-null, decides
     * admission of other peers into a POLICY group.
     *
     * @param groupAd          the group's founding advertisement
     * @param membershipConfig membership timing configuration
     * @param seeds            seed endpoints to introduce ourselves to
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @return the joined group's runtime
     */
    public GroupRuntime joinGroup(GroupAdvertisement groupAd,
                                  GroupMembership.Config membershipConfig,
                                  List<PeerAdvertisement.Endpoint> seeds,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator) {
        return join(groupAd, null, membershipConfig, seeds, membershipHints, validator);
    }

    /** The common join: wires the runtime and introduces this node to the seeds. */
    private GroupRuntime join(GroupAdvertisement groupAd,
                              SignedGroupAdvertisement founding,
                              GroupMembership.Config membershipConfig,
                              List<PeerAdvertisement.Endpoint> seeds,
                              Map<String, String> membershipHints,
                              MembershipValidator validator) {
        Objects.requireNonNull(groupAd, "groupAd");
        Objects.requireNonNull(membershipHints, "membershipHints");
        GroupId groupId = groupAd.group();
        groupHints.put(groupId, Map.copyOf(membershipHints));
        if (validator != null) {
            groupValidators.put(groupId, validator);
        }
        GroupMembership membership =
                new GroupMembership(peerId(), clock, random, membershipConfig);
        GossipBus.FrameSender sender = new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
                send(groupId, to, Envelope.Kind.RUMOR, rumor);
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digest) {
                send(groupId, to, Envelope.Kind.DIGEST, digest);
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp resp) {
                send(groupId, to, Envelope.Kind.PULL_RESP, resp);
            }
        };
        GossipBus gossip = new GossipBus(membership, sender, codec,
                groupAd.gossip().fanout(), GossipBus.DEFAULT_HOPS);
        RevocationRegistry revocations = new RevocationRegistry(groupAd,
                revocationValidator, codec, clock,
                ad -> onPeerRevoked(membership, ad));
        ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry credentialRevocations =
                new ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry(groupAd,
                        credentialRevocationValidator, codec, clock);
        credentialRevocations.addListener(ad -> onCredentialRevoked(groupId, membership, ad));
        GroupRuntime runtime = new GroupRuntime(groupId, groupAd, membership, gossip,
                revocations, credentialRevocations, this, founding);
        membership.refuse(revocations::revoked); // ASF-047: no re-admission by proxy
        gossip.onStream(REVOCATION_STREAM,
                (from, itemId, payload) -> revocations.accept(payload));
        gossip.forwardFilter(REVOCATION_STREAM,
                payload -> revocations.accept(payload).isPresent()
                        || revocationVerifiable(revocations, payload));
        gossip.reconcile(REVOCATION_STREAM, revocations);
        gossip.onStream(CREDENTIAL_REVOCATION_STREAM,
                (from, itemId, payload) -> credentialRevocations.accept(payload));
        gossip.forwardFilter(CREDENTIAL_REVOCATION_STREAM,
                payload -> credentialRevocations.accept(payload).isPresent()
                        || credentialRevocationVerifiable(credentialRevocations, payload));
        gossip.reconcile(CREDENTIAL_REVOCATION_STREAM, credentialRevocations);
        gossip.onStream(PEER_AD_STREAM,
                (from, itemId, payload) -> onPeerAdItem(runtime, from, payload));
        // ASF-012: this node forwards only peer ads it can verify end-to-end,
        // so its identity and rate budget never amplify unverifiable content.
        gossip.forwardFilter(PEER_AD_STREAM, payload -> forwardsPeerAd(revocations, payload));
        groups.put(groupId, runtime);

        // Introduce ourselves to the seeds directly; gossip takes it from there.
        introduceTo(groupId, seeds);
        return runtime;
    }

    /**
     * The bootstrap self-introduction (spec §5.1): dial each endpoint whose
     * scheme we speak, in ascending priority, and send our signed
     * self-advertisement unaddressed (the seed's PeerID is not known until it
     * answers). Used for configured seeds, for endpoints learned from a
     * verified multicast beacon, and after a join-by-GroupID fetch.
     */
    private void introduceTo(GroupId groupId, List<PeerAdvertisement.Endpoint> endpoints) {
        for (PeerAdvertisement.Endpoint seed : byPriority(endpoints)) {
            Transport transport = transports.get(seed.transport());
            if (transport == null) {
                continue;
            }
            try {
                TransportConnection connection = transport.dial(seed.address());
                attach(connection);
                maybeAnnounceChannel(groupId, connection);
                sendOn(connection, groupId, null, Envelope.Kind.RUMOR, selfAdRumor(groupId));
            } catch (IOException e) {
                // Seed unreachable; others may work, and gossip is redundant.
            }
        }
    }

    /** Endpoints in ascending priority, stable for ties (spec §5.5). */
    private static List<PeerAdvertisement.Endpoint> byPriority(
            List<PeerAdvertisement.Endpoint> endpoints) {
        List<PeerAdvertisement.Endpoint> ordered = new ArrayList<>(endpoints);
        ordered.sort(java.util.Comparator.comparingInt(PeerAdvertisement.Endpoint::priority));
        return ordered;
    }

    /**
     * Returns a joined group's runtime.
     *
     * @param groupId the group
     * @return the runtime, when joined
     */
    public Optional<GroupRuntime> group(GroupId groupId) {
        return Optional.ofNullable(groups.get(groupId));
    }

    /**
     * Runs one protocol round for every joined group: refresh the leased
     * self-advertisement, probe membership, and — once per gossip period of
     * the node's clock (spec §5.3, {@code GossipParameters.period}) —
     * reconcile state with one partner. Rumor forwarding and probes are not
     * paced by the period. Also announces on the bootstrap beacon, when one
     * is configured, once per beacon interval.
     */
    public void tick() {
        long nowMillis = clock.millis();
        rejudgeIfTrustChanged();
        for (GroupRuntime runtime : groups.values()) {
            GossipBus gossip = runtime.gossip();
            Bodies.Rumor selfAd = selfAdRumor(runtime.id());
            gossip.publish(selfAd.streamId(), selfAd.itemId(), selfAd.payload());
            runtime.membership().tick(new GroupMembership.Prober() {
                @Override
                public void ping(PeerId target, long nonce) {
                    send(runtime.id(), target, Envelope.Kind.PING, new Bodies.Ping(nonce));
                }

                @Override
                public void pingReq(PeerId relay, PeerId target, long nonce) {
                    send(runtime.id(), relay, Envelope.Kind.PING_REQ,
                            new Bodies.PingReq(target, nonce));
                }
            });
            if (runtime.claimAntiEntropyRound(nowMillis)) {
                gossip.antiEntropyTick();
            }
            // Layers above peering join this same clock through
            // GroupRuntime.onTick (QA3 A3-1): capability protocols advance here,
            // for the same reason membership and anti-entropy just did.
            runtime.runTickWork(e -> LOG.log(System.Logger.Level.WARNING,
                    "tick work failed in group " + runtime.id().value(), e));
        }
        if (beacon != null) {
            Long last = lastBeaconMillis;
            if (last == null || nowMillis - last >= beaconInterval.toMillis()) {
                lastBeaconMillis = nowMillis;
                beacon.announce();
            }
        }
    }

    /**
     * Starts a background ticker for wall-clock deployments.
     *
     * @param period the tick period
     */
    public synchronized void startTicking(Duration period) {
        if (ticker != null) {
            return;
        }
        this.tickPeriod = Objects.requireNonNull(period, "period");
        ticker = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "peer-tick-" + peerId().display());
            t.setDaemon(true);
            return t;
        });
        long millis = period.toMillis();
        ticker.scheduleAtFixedRate(this::tick, millis, millis,
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Sends a body to a peer in a group, dialing through advertised endpoints when
     * no connection exists, and falling back to a RELAY-role member when the peer
     * cannot be dialed at all (spec §5.4). Remaining failures are dropped
     * silently; gossip redundancy and membership probing absorb them.
     *
     * @param groupId the group
     * @param to      the destination peer
     * @param kind    the frame kind
     * @param body    the body object
     */
    void send(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        TransportConnection connection = connectionTo(groupId, to);
        if (connection == null) {
            sendViaRelay(groupId, to, kind, body);
            return;
        }
        try {
            sendOn(connection, groupId, to, kind, body);
        } catch (IOException e) {
            dropConnection(to, connection);
        }
    }

    /**
     * Wraps the frame in a RELAY_FRAME and hands it to the first reachable
     * RELAY-role member. One relay hop only: the relay dials the target
     * directly, and relay frames themselves are never re-relayed.
     */
    private void sendViaRelay(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        if (kind == Envelope.Kind.RELAY_FRAME) {
            return;
        }
        GroupRuntime runtime = groups.get(groupId);
        if (runtime == null) {
            return;
        }
        // The inner frame is addressed to the ultimate target; the outer
        // RELAY_FRAME is addressed to the relay that carries it.
        byte[] inner = encodeFrame(groupId, to, kind, body);
        for (PeerId relay : runtime.membership()
                .withRole(PeerAdvertisement.PeerRole.RELAY)) {
            if (relay.equals(to) || relay.equals(peerId())) {
                continue;
            }
            TransportConnection connection = connectionTo(groupId, relay);
            if (connection == null) {
                continue;
            }
            try {
                sendOn(connection, groupId, relay, Envelope.Kind.RELAY_FRAME,
                        new Bodies.RelayFrame(to, inner));
                return;
            } catch (IOException e) {
                dropConnection(relay, connection);
            }
        }
    }

    /**
     * The wall-clock period between {@link #tick()} calls, when this node knows
     * it: the period {@link #startTicking(Duration)} was called with. Empty when
     * the host drives {@link #tick()} by hand, because then only the host knows
     * the cadence, and a test advancing a {@code TestClock} a simulated second
     * per tick is not ticking at any wall-clock rate at all.
     *
     * <p>Layers above peering that express their semantics in ticks read this to
     * convert to wall time, and must keep their own declared cadence when it is
     * empty rather than substituting a guess; see
     * {@code CapabilityProvider.driverCadence} (QA3 A3-4).
     *
     * @return the cadence ticks arrive at, or empty when hand-driven
     */
    public Optional<Duration> tickPeriod() {
        return Optional.ofNullable(tickPeriod);
    }

    @Override
    public synchronized void close() {
        if (ticker != null) {
            // Let an in-flight tick finish rather than interrupt it: a tick may be
            // persisting state (a rotated content-key ring), and an interrupted
            // write leaves a half-written file behind. Bounded, and never awaited
            // from the tick thread itself.
            ticker.shutdown();
            if (!Thread.currentThread().getName().startsWith("peer-tick-")) {
                try {
                    ticker.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            ticker.shutdownNow();
        }
        for (AutoCloseable listener : listeners) {
            try {
                listener.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
        connections.values().forEach(TransportConnection::close);
        connections.clear();
        bareAccepted.clear();
        helloSent.clear();
        if (beacon != null) {
            beacon.close();
        }
    }

    // ---------------------------------------------------------------- internals

    private void attach(TransportConnection connection) {
        connection.onReceive(frame -> onFrame(connection, frame));
        java.security.cert.X509Certificate[] chain = connection.remoteChain();
        if (chain.length == 0) {
            return;
        }
        chained.add(connection);
        if (leafRevokedInAnyGroup(chain[0])) {
            chained.remove(connection);
            connection.close();
            return;
        }
        if (channelTrust != null) {
            judgeChannel(connection, channelTrust.get());
        }
    }

    /** Whether a founder's {@code X509_LEAF} revocation names this leaf in any joined group (v0.1.13). */
    private boolean leafRevokedInAnyGroup(java.security.cert.X509Certificate leaf) {
        byte[] fingerprint;
        try {
            fingerprint = ai.badmonkey.agentspaces.common.crypto.Digests.sha256(leaf.getEncoded());
        } catch (java.security.cert.CertificateEncodingException e) {
            return false;
        }
        for (GroupRuntime runtime : groups.values()) {
            if (runtime.credentialRevocations().leafRevoked(fingerprint)) {
                return true;
            }
        }
        return false;
    }

    /** Cuts every live link presenting a leaf an {@code X509_LEAF} revocation names. */
    private void closeRevokedLeaves() {
        List<TransportConnection> live;
        synchronized (chained) {
            live = List.copyOf(chained);
        }
        for (TransportConnection connection : live) {
            java.security.cert.X509Certificate[] chain = connection.remoteChain();
            if (chain.length > 0 && leafRevokedInAnyGroup(chain[0])) {
                chained.remove(connection);
                connection.close();
            }
        }
    }

    /**
     * Judges one connection's presented chain under a trust (SPEC §5.6,
     * v0.1.13): a leaf the CA revoked for a reason withdrawing the identity
     * roots a peer revocation in every joined group, with evidence, and the
     * link is cut.
     */
    private void judgeChannel(TransportConnection connection,
                              ai.badmonkey.agentspaces.identity.ChannelTrust trust) {
        java.security.cert.X509Certificate[] chain = connection.remoteChain();
        if (!(trust.status(chain, clock.instant())
                instanceof ai.badmonkey.agentspaces.identity.TrustStatus.Revoked revoked)
                || !revoked.authorizesPeerRevocation()) {
            return;
        }
        chained.remove(connection);
        connection.close();
        if (!caRevoked.add(revoked.peer())) {
            return; // already rooted here: one per peer per node
        }
        byte[] evidence = ai.badmonkey.agentspaces.peering.membership.RevocationEvidence
                .of(chain, trust.crlRevoking(chain).orElse(null)).encode(codec);
        for (GroupRuntime runtime : groups.values()) {
            if (runtime.revocations().revoked(revoked.peer())) {
                continue;
            }
            ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement ad =
                    new ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement(
                            "aspace://" + runtime.id().value() + "/revocation/" + revoked.peer().value(),
                            peerId(), runtime.id(), clock.instant(), REVOCATION_GOSSIP_TTL,
                            revoked.peer(), "ca:" + revoked.reason().name().toLowerCase(), null, evidence);
            byte[] adBytes = codec.toBytes(ad);
            RevocationRegistry.SignedRevocation signed = new RevocationRegistry.SignedRevocation(
                    adBytes, identity.rawPublicKey(), identity.sign(adBytes));
            if (runtime.revocations().accept(signed).isPresent()) {
                runtime.gossip().publish(REVOCATION_STREAM,
                        "revocation:" + revoked.peer().value() + ":ca", codec.toBytes(signed));
            } else {
                LOG.log(System.Logger.Level.WARNING, "the CA revoked " + revoked.peer().display()
                        + "'s channel certificate (" + revoked.reason() + ") but group "
                        + runtime.id() + " does not accept CA-rooted revocations; link cut only");
            }
        }
    }

    /** Re-judges every live chained connection when the trust has changed (a CRL refresh). */
    private void rejudgeIfTrustChanged() {
        if (channelTrust == null) {
            return;
        }
        ai.badmonkey.agentspaces.identity.ChannelTrust current = channelTrust.get();
        if (current == judgedUnder) {
            return;
        }
        judgedUnder = current;
        List<TransportConnection> live;
        synchronized (chained) {
            live = List.copyOf(chained);
        }
        for (TransportConnection connection : live) {
            judgeChannel(connection, current);
        }
    }

    private void onFrame(TransportConnection connection, byte[] frame) {
        Optional<Envelope> decoded = wire.decode(frame, connection.attestedPeer());
        if (decoded.isEmpty()) {
            // Bad signature, unknown wire version, or malformed: drop before
            // any handler sees it (spec §9). No verified sender, so no strike.
            framesDropped.incrementAndGet();
            return;
        }
        dispatch(decoded.get(), connection);
    }

    /**
     * Dispatches a verified envelope. The connection is null for frames that
     * arrived through a relay: the origin is not on the other end of any local
     * connection, so caching one under its id would misroute later replies.
     */
    private void dispatch(Envelope envelope, TransportConnection connection) {
        GroupRuntime runtime = groups.get(envelope.group());
        if (runtime == null) {
            // Not a member of that group (spec §9). The one exception is the
            // answer to our own join-by-GroupID request, which by definition
            // concerns a group we have not joined yet; it is verified on its
            // own terms (the self-certifying founding document) and has no
            // other side effect.
            if (envelope.kind() == Envelope.Kind.GROUP_AD) {
                onGroupAdAnswer(envelope);
            }
            return;
        }
        // Remediation plan §9: a revoked identity gets nothing, permanently;
        // then WS5's local quarantine, before any other check.
        if (runtime.revocations().revoked(envelope.from())
                || quarantined(envelope.from())) {
            return;
        }
        // ASF-010: an addressed frame is accepted only by its intended recipient,
        // so a captured frame cannot be redirected to, or replayed at, a peer it
        // was never sent to. A null destination is an unaddressed bootstrap frame.
        if (envelope.to() != null && !envelope.to().equals(peerId())) {
            return;
        }
        // ASF-010: reject stale frames and exact replays before any side effect
        // (clock merge, connection caching, liveness) takes hold. Deliberately
        // NOT a strike: a replayed frame is signed by its original author, so
        // striking here would let an attacker replay a victim's captured
        // frames to get the victim quarantined. Later strike sources are safe
        // exactly because this dedup runs first — a frame that reaches them is
        // fresh, and only its signer could have produced it.
        if (isStaleOrReplay(envelope)) {
            return;
        }
        // v0.1.9 CA channel mode: attestation is a condition of everything
        // below, admission included. Runs after the replay dedup so a captured
        // frame replayed over a stranger's connection is not fresh evidence.
        if (requireAttestation && !attests(connection, envelope.from())) {
            onUnattestedFrame(runtime, envelope, connection);
            return;
        }
        // ASF-003: the data plane serves admitted members only. A peer outside
        // the membership view gets exactly one narrow path — introducing itself
        // (bootstrap) — and no side effects (clock merge, connection caching,
        // liveness, per-peer bucket state) before admission.
        if (runtime.membership().member(envelope.from()).isEmpty()) {
            onNonMemberFrame(runtime, envelope, connection);
            return;
        }
        if (!rateBuckets.computeIfAbsent(envelope.from(),
                        p -> new TokenBucket(rateCapacity, rateRefillPerSecond, nanos))
                .tryAcquire()) {
            strike(envelope.from(), "rate limit breach");
            return; // spec §11: per-issuer rate limit; a flooding peer is throttled
        }
        // ASF-020: a stamp near the HLC merge ceiling is a clock-pinning
        // attempt (the merge would clamp it, but sustained near-ceiling
        // traffic still drags every receiver's clock to wall+drift and wins
        // LEASE_RACE tie-breaks). It is a fresh, signed frame — the replay
        // dedup ran already — so it is witnessed, attributable misbehavior.
        if (envelope.stamp().physical()
                > clock.millis() + HybridLogicalClock.MAX_DRIFT_MILLIS) {
            strike(envelope.from(), "far-future stamp");
            return;
        }
        hlc.update(envelope.stamp());
        if (connection != null) {
            connections.putIfAbsent(envelope.from(), connection);
            maybeAnnounceChannel(envelope.group(), connection);
        }
        runtime.membership().recordHeard(envelope.from());
        try {
            dispatchKind(runtime, envelope, connection);
        } catch (RuntimeException e) {
            // A verified member sent a body its kind cannot carry: witnessed
            // misbehavior, and one hostile frame must not kill the reader.
            strike(envelope.from(), "malformed " + envelope.kind() + " body");
        }
    }

    private void dispatchKind(GroupRuntime runtime, Envelope envelope,
                              TransportConnection connection) {
        switch (envelope.kind()) {
            case PING -> {
                Bodies.Ping ping = codec.fromBytes(envelope.body(), Bodies.Ping.class);
                send(runtime.id(), envelope.from(), Envelope.Kind.ACK,
                        new Bodies.Ack(ping.nonce(), null));
            }
            case PING_REQ -> {
                Bodies.PingReq req = codec.fromBytes(envelope.body(), Bodies.PingReq.class);
                long relay = relayNonce.nextLong();
                relays.put(relay, new Relay(envelope.from(), req.nonce(), req.target()));
                send(runtime.id(), req.target(), Envelope.Kind.PING, new Bodies.Ping(relay));
            }
            case ACK -> {
                Bodies.Ack ack = codec.fromBytes(envelope.body(), Bodies.Ack.class);
                Relay relay = relays.get(ack.nonce());
                if (relay != null) {
                    // Vouch for the target only when the target itself answered
                    // this relay's own ping (ASF-011): any other sender is a
                    // forgery and neither consumes the relay nor produces an ACK.
                    if (envelope.from().equals(relay.target())) {
                        relays.remove(ack.nonce());
                        send(runtime.id(), relay.origin(), Envelope.Kind.ACK,
                                new Bodies.Ack(relay.originNonce(), relay.target()));
                    }
                } else {
                    runtime.membership().onAck(ack.nonce(), envelope.from());
                }
            }
            case RUMOR -> runtime.gossip().onRumor(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Rumor.class));
            case DIGEST -> runtime.gossip().onDigest(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Digest.class));
            case PULL_RESP -> runtime.gossip().onPullResp(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.PullResp.class));
            case QUERY, QUERY_HIT, BLOCK_WANT, BLOCK, PIPE_DATA -> {
                var handler = runtime.handlerFor(envelope.kind());
                if (handler != null) {
                    handler.accept(envelope.from(), envelope.body());
                }
            }
            case RELAY_FRAME -> onRelayFrame(runtime, envelope);
            case CHANNEL_HELLO -> onChannelHello(envelope, connection);
            case GROUP_AD_WANT -> runtime.founding().ifPresent(founding ->
                    send(runtime.id(), envelope.from(), Envelope.Kind.GROUP_AD, founding));
            case GROUP_AD -> onGroupAdAnswer(envelope); // only a pending fetch cares
        }
    }

    /**
     * Whether a connection's transport attestation names a peer (spec §5.6,
     * v0.1.9). Relayed frames arrive with no connection and attest nothing.
     */
    private static boolean attests(TransportConnection connection, PeerId peer) {
        return connection != null
                && connection.attestedPeer().filter(peer::equals).isPresent();
    }

    /**
     * A frame that failed the attestation requirement (v0.1.9 CA channel mode).
     * Always dropped and logged. When the connection attests <em>nobody</em>
     * and the signed sender is a current member, the member has just
     * completed a fresh handshake that attests nothing — the CA withdrew its
     * credential — so the CA's decision is enforced here and now: evict from
     * the view and cut the cached link. The peer regains service only by
     * arriving on a connection attested for it again, which a revoked
     * certificate cannot produce. A connection attested for someone
     * <em>else</em> carrying this sender's frame is merely dropped: evicting
     * on it would let the attested party evict anyone whose signed frames it
     * captured.
     */
    private void onUnattestedFrame(GroupRuntime runtime, Envelope envelope,
                                   TransportConnection connection) {
        boolean unattested = connection != null && connection.attestedPeer().isEmpty();
        if (unattested && runtime.membership().member(envelope.from()).isPresent()) {
            runtime.membership().evict(envelope.from());
            TransportConnection cached = connections.remove(envelope.from());
            if (cached != null) {
                cached.close();
                bareAccepted.remove(cached);
                helloSent.remove(cached);
            }
            LOG.log(System.Logger.Level.WARNING,
                    "evicted " + envelope.from().display() + " from " + runtime.id()
                            + ": its fresh handshake attests nothing (CA revocation or"
                            + " credential change); refused until it re-attests");
            return;
        }
        LOG.log(System.Logger.Level.DEBUG,
                "dropped " + envelope.kind() + " from " + envelope.from().display()
                        + " on a connection attested for "
                        + (connection == null ? "nobody (relayed)"
                                : connection.attestedPeer().map(PeerId::display).orElse("nobody"))
                        + "; attestation is required");
    }

    /**
     * Completes a pending join-by-GroupID fetch with a GROUP_AD answer, but
     * only when the answer verifies as the self-certifying founding document
     * of exactly the group we asked about (spec §4.4, §5.1). Anything else —
     * an id that does not re-derive, a forged founder signature, an answer for
     * another group, an unsolicited answer — is ignored, so a hostile seed can
     * stall a join but never substitute a group's policy.
     */
    private void onGroupAdAnswer(Envelope envelope) {
        CompletableFuture<SignedGroupAdvertisement> pending = pendingFounding.get(envelope.group());
        if (pending == null || (envelope.to() != null && !envelope.to().equals(peerId()))) {
            return;
        }
        SignedGroupAdvertisement founding;
        try {
            founding = codec.fromBytes(envelope.body(), SignedGroupAdvertisement.class);
        } catch (RuntimeException e) {
            return;
        }
        if (founding == null || !GroupFounding.verify(founding)
                || !founding.advertisement().group().equals(envelope.group())) {
            LOG.log(System.Logger.Level.WARNING,
                    "seed " + envelope.from().display() + " answered a GROUP_AD_WANT for "
                            + envelope.group() + " with an advertisement that is not its"
                            + " self-certifying founding document; ignored");
            return;
        }
        pending.complete(founding);
    }

    /**
     * Serves the only four frame kinds a peer outside the membership view may
     * send (ASF-003): a RUMOR on the peer-advertisement stream introducing the
     * sender itself (the bootstrap join, spec §5.1); a PING, answered on the
     * incoming connection alone (pure liveness — admission can be momentarily
     * asymmetric between two honest members, and dropping their probes would
     * evict them from each other's views for good); a CHANNEL_HELLO, whose
     * handler independently requires the connection's transport attestation to
     * name exactly the sender; and a GROUP_AD_WANT (spec §10.1 join by
     * GroupID), answered on the incoming connection alone with this group's
     * self-certifying founding document — and with nothing at all for a
     * literal-id group. Everything else — digests, pulls, queries, blocks,
     * pipes, relays — is the data plane, and is dropped before any side
     * effect. All four share the bootstrap rate budget.
     */
    private void onNonMemberFrame(GroupRuntime runtime, Envelope envelope,
                                  TransportConnection connection) {
        if (!bootstrapBucket.tryAcquire()) {
            return;
        }
        if (envelope.kind() == Envelope.Kind.CHANNEL_HELLO) {
            onChannelHello(envelope, connection);
            return;
        }
        if (envelope.kind() == Envelope.Kind.GROUP_AD_WANT) {
            if (connection != null) {
                runtime.founding().ifPresent(founding -> {
                    try {
                        sendOn(connection, envelope.group(), envelope.from(),
                                Envelope.Kind.GROUP_AD, founding);
                    } catch (IOException e) {
                        // Best-effort bootstrap answer; the asker has other seeds.
                    }
                });
            }
            return;
        }
        if (envelope.kind() == Envelope.Kind.PING && connection != null) {
            Bodies.Ping ping;
            try {
                ping = codec.fromBytes(envelope.body(), Bodies.Ping.class);
            } catch (RuntimeException e) {
                return;
            }
            if (ping != null) {
                try {
                    sendOn(connection, envelope.group(), envelope.from(),
                            Envelope.Kind.ACK, new Bodies.Ack(ping.nonce(), null));
                } catch (IOException e) {
                    // Best-effort liveness answer; the prober retries.
                }
            }
            return;
        }
        if (envelope.kind() != Envelope.Kind.RUMOR) {
            return;
        }
        Bodies.Rumor rumor;
        try {
            rumor = codec.fromBytes(envelope.body(), Bodies.Rumor.class);
        } catch (RuntimeException e) {
            return;
        }
        if (rumor == null || !PEER_AD_STREAM.equals(rumor.streamId())) {
            return;
        }
        runtime.gossip().onBootstrapRumor(envelope.from(), rumor);
        if (runtime.membership().member(envelope.from()).isPresent()) {
            // Admitted by its own introduction: adopt the side effects a member
            // frame gets, so replies flow over the connection it dialed in on,
            // and forward the now-verified introduction so the rest of the
            // group learns the newcomer (the pre-gate flow forwarded it too —
            // but only after admission does it deserve fan-out).
            hlc.update(envelope.stamp());
            if (connection != null) {
                connections.putIfAbsent(envelope.from(), connection);
                maybeAnnounceChannel(envelope.group(), connection);
            }
            runtime.membership().recordHeard(envelope.from());
            runtime.gossip().publish(rumor.streamId(), rumor.itemId(), rumor.payload());
        }
    }

    /**
     * Issues (or refuses to issue) a revocation for a group: builds the
     * advertisement, signs it with this node's identity, runs it through the
     * group's own registry — whose validator enforces the trust-root authority
     * check on the issuer, exactly as every receiver will — and gossips it on
     * acceptance.
     */
    Optional<ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement> issueRevocation(
            GroupRuntime runtime, PeerId revoked, String reason, PeerId successor) {
        Objects.requireNonNull(revoked, "revoked");
        ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement ad =
                new ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement(
                        "aspace://" + runtime.id().value() + "/revocation/" + revoked.value(),
                        peerId(), runtime.id(), clock.instant(), REVOCATION_GOSSIP_TTL,
                        revoked, reason, successor);
        byte[] adBytes = codec.toBytes(ad);
        RevocationRegistry.SignedRevocation signed =
                new RevocationRegistry.SignedRevocation(adBytes,
                        identity.rawPublicKey(), identity.sign(adBytes));
        Optional<ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement> issued =
                runtime.revocations().accept(signed);
        if (issued.isPresent()) {
            runtime.gossip().publish(REVOCATION_STREAM,
                    "revocation:" + revoked.value(), codec.toBytes(signed));
        }
        return issued;
    }

    /**
     * Issues a credential revocation for a group (SPEC §6.1, v0.1.13): signed by
     * this node, run through the group's own registry (whose validator ranks
     * this node's authority exactly as every receiver will), and gossiped on
     * acceptance.
     */
    Optional<ai.badmonkey.agentspaces.api.ad.CredentialRevocation> issueCredentialRevocation(
            GroupRuntime runtime, ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target target,
            String reason, Instant effectiveFrom) {
        Objects.requireNonNull(target, "target");
        Instant now = clock.instant();
        if (effectiveFrom != null && effectiveFrom.isAfter(now)) {
            throw new IllegalArgumentException("effectiveFrom " + effectiveFrom
                    + " lies in the future; a revocation takes effect no later than its issue");
        }
        ai.badmonkey.agentspaces.api.ad.CredentialRevocation ad =
                new ai.badmonkey.agentspaces.api.ad.CredentialRevocation(
                        ai.badmonkey.agentspaces.api.ad.CredentialRevocation.idFor(runtime.id(), target),
                        peerId(), runtime.id(), now, REVOCATION_GOSSIP_TTL, target, reason,
                        effectiveFrom, null);
        byte[] adBytes = codec.toBytes(ad);
        RevocationRegistry.SignedRevocation signed = new RevocationRegistry.SignedRevocation(
                adBytes, identity.rawPublicKey(), identity.sign(adBytes));
        if (runtime.credentialRevocations().verify(signed) == null) {
            return Optional.empty(); // not this node's to revoke
        }
        runtime.credentialRevocations().accept(signed);
        // Published even when the target was already revoked: a higher-ranked
        // or newer record replaces the held one everywhere it arrives.
        runtime.gossip().publish(CREDENTIAL_REVOCATION_STREAM,
                "credential-revocation:" + target.key() + ":" + now.toEpochMilli(),
                codec.toBytes(signed));
        return Optional.of(ad);
    }

    /**
     * Enforcement when a credential revocation lands: a member admitted under a
     * revoked join credential is evicted and its link cut; agent revocations
     * are enforced where agents' signatures are checked.
     */
    private void onCredentialRevoked(GroupId groupId, GroupMembership membership,
                                     ai.badmonkey.agentspaces.api.ad.CredentialRevocation ad) {
        ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target target = ad.target();
        if (ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.X509_LEAF.equals(target.kind())) {
            closeRevokedLeaves();
            LOG.log(System.Logger.Level.WARNING, "channel certificate " + target.canonical()
                    + " revoked (" + ad.reason() + "); links presenting it are cut");
            return;
        }
        if (ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.JOIN_CREDENTIAL.equals(target.kind())) {
            String hash = HexFormat.of().formatHex(target.credentialHash());
            Map<PeerId, String> admitted = admittedUnder.getOrDefault(groupId, Map.of());
            for (Map.Entry<PeerId, String> entry : Map.copyOf(admitted).entrySet()) {
                if (entry.getValue().equals(hash)) {
                    PeerId evicted = entry.getKey();
                    membership.evict(evicted);
                    admitted.remove(evicted);
                    // The link is the node's, not the group's: keep it for a peer
                    // still a member of another group joined here.
                    boolean sharedElsewhere = groups.entrySet().stream()
                            .anyMatch(g -> !g.getKey().equals(groupId)
                                    && g.getValue().membership().member(evicted).isPresent());
                    TransportConnection connection = sharedElsewhere ? null : connections.remove(evicted);
                    if (connection != null) {
                        connection.close();
                    }
                    LOG.log(System.Logger.Level.WARNING, "member " + evicted.display()
                            + " evicted: its join credential was revoked (" + ad.reason() + ")");
                }
            }
            return;
        }
        LOG.log(System.Logger.Level.WARNING, target.kind().toLowerCase() + " "
                + target.canonical() + " revoked by " + ad.issuer().display() + " (" + ad.reason() + ")");
    }

    private boolean credentialRevocationVerifiable(
            ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry registry,
            byte[] payload) {
        try {
            return registry.verify(codec.fromBytes(payload,
                    RevocationRegistry.SignedRevocation.class)) != null;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Enforcement when a revocation lands: evict, cut the link, and log. */
    private void onPeerRevoked(GroupMembership membership,
                               ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement ad) {
        membership.evict(ad.revoked());
        TransportConnection connection = connections.remove(ad.revoked());
        if (connection != null) {
            connection.close();
        }
        LOG.log(System.Logger.Level.WARNING,
                "peer " + ad.revoked().display() + " revoked by the trust root ("
                        + ad.reason() + ")"
                        + (ad.successor() == null ? ""
                                : "; succeeded by " + ad.successor().display()));
    }

    private boolean revocationVerifiable(RevocationRegistry registry, byte[] payload) {
        try {
            return registry.verify(codec.fromBytes(payload,
                    RevocationRegistry.SignedRevocation.class)) != null;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Records one witnessed protocol violation by a peer (WS5 containment:
     * detect, then quarantine locally). Past the threshold the peer is
     * quarantined: its frames are refused before dispatch, so its liveness
     * lapses and SWIM evicts it from the membership view. This is per-node
     * containment from per-node evidence; the durable eject is the enterprise
     * revocation channel.
     *
     * @param peer   the misbehaving peer
     * @param reason what this node witnessed
     */
    void strike(PeerId peer, String reason) {
        int count = strikes.merge(peer, 1, Integer::sum);
        if (count >= STRIKE_THRESHOLD) {
            strikes.remove(peer);
            quarantinedUntil.put(peer, clock.millis() + QUARANTINE.toMillis());
            TransportConnection connection = connections.remove(peer);
            if (connection != null) {
                connection.close();
            }
            LOG.log(System.Logger.Level.WARNING,
                    "quarantined " + peer.display() + " after " + count
                            + " witnessed violations; last: " + reason);
        }
    }

    /** Whether this node holds a link to a peer (tests). */
    boolean connectedTo(PeerId peer) {
        return connections.containsKey(peer);
    }

    /** Whether a peer is currently locally quarantined (WS5). */
    boolean quarantined(PeerId peer) {
        Long until = quarantinedUntil.get(peer);
        if (until == null) {
            return false;
        }
        if (clock.millis() >= until) {
            quarantinedUntil.remove(peer);
            return false;
        }
        return true;
    }

    /**
     * Whether a frame is too old to be live, or an exact replay of one already
     * seen (ASF-010). Staleness is judged against real wall-clock time and the
     * frame's own signed HLC stamp; replay is judged by a bounded set of recent
     * {@code (from, stamp)} identities. Together they close the redirection and
     * liveness-forgery replays a captured frame otherwise permits.
     */
    private boolean isStaleOrReplay(Envelope envelope) {
        // Judge staleness against this node's own clock (the injected
        // InstantSource, so deterministic-clock tests stay consistent), which
        // is also the reference the sender's HLC stamp was merged toward.
        long ageMillis = clock.millis() - envelope.stamp().physical();
        if (ageMillis > MAX_FRAME_AGE_MILLIS) {
            return true;
        }
        String id = envelope.from().value() + '|' + envelope.stamp().encoded();
        return recentFrames.put(id, Boolean.TRUE) != null;
    }

    /**
     * Handles a CHANNEL_HELLO announcement (spec §5.6). It counts only when the
     * frame arrived on a connection whose transport attested exactly the
     * announcer, and when the echoed PeerID names us, so the announcement is
     * bound to this very channel; anything else is silently ignored.
     */
    private void onChannelHello(Envelope envelope, TransportConnection connection) {
        if (connection == null
                || connection.attestedPeer().filter(envelope.from()::equals).isEmpty()) {
            return;
        }
        Bodies.ChannelHello hello;
        try {
            hello = codec.fromBytes(envelope.body(), Bodies.ChannelHello.class);
        } catch (RuntimeException e) {
            return;
        }
        if (hello == null || !peerId().equals(hello.attestedRemote())) {
            return;
        }
        if (hello.acceptsBare()) {
            bareAccepted.put(connection, envelope.from());
        } else {
            bareAccepted.remove(connection);
        }
    }

    /**
     * Announces, once per attested connection and only in ATTESTED mode, that
     * this node accepts channel-attested frames there. The announcement itself
     * travels as a fully signed frame.
     */
    private void maybeAnnounceChannel(GroupId groupId, TransportConnection connection) {
        if (channelAuth != ChannelAuth.ATTESTED) {
            return;
        }
        Optional<PeerId> attested = connection.attestedPeer();
        if (attested.isEmpty() || !helloSent.add(connection)) {
            return;
        }
        try {
            connection.send(encodeFrame(groupId, attested.get(), Envelope.Kind.CHANNEL_HELLO,
                    new Bodies.ChannelHello(true, attested.get())));
            signedFramesSent.incrementAndGet();
        } catch (IOException e) {
            helloSent.remove(connection); // retry on the next trigger
        }
    }

    private void onRelayFrame(GroupRuntime runtime, Envelope envelope) {
        Bodies.RelayFrame relayed;
        try {
            relayed = codec.fromBytes(envelope.body(), Bodies.RelayFrame.class);
        } catch (RuntimeException e) {
            return;
        }
        if (relayed == null || relayed.target() == null || relayed.frame() == null) {
            return;
        }
        if (relayed.target().equals(peerId())) {
            // For us: unwrap and dispatch the origin's signed inner frame, with
            // no connection to cache (the origin is behind the relay, not here).
            wire.decode(relayed.frame())
                    .filter(inner -> inner.kind() != Envelope.Kind.RELAY_FRAME)
                    .ifPresent(inner -> dispatch(inner, null));
            return;
        }
        if (!roles.contains(PeerAdvertisement.PeerRole.RELAY)) {
            return; // only declared relays spend bandwidth forwarding
        }
        TransportConnection out = connectionTo(runtime.id(), relayed.target());
        if (out == null) {
            return;
        }
        try {
            sendOn(out, runtime.id(), relayed.target(), Envelope.Kind.RELAY_FRAME, relayed);
        } catch (IOException e) {
            dropConnection(relayed.target(), out);
        }
    }

    private void sendOn(TransportConnection connection, GroupId groupId, PeerId to,
                        Envelope.Kind kind, Object body) throws IOException {
        // Frames built here always originate from this node, so channel
        // attestation can stand in for the envelope signature (spec §5.6):
        // elide only in ATTESTED mode, toward a peer that announced it accepts
        // bare frames, on the connection whose transport attested that peer.
        PeerId announced = channelAuth == ChannelAuth.ATTESTED
                ? bareAccepted.get(connection) : null;
        if (announced != null
                && connection.attestedPeer().filter(announced::equals).isPresent()) {
            byte[] bodyBytes = body instanceof byte[] raw ? raw : codec.toBytes(body);
            connection.send(wire.encodeBare(
                    new Envelope(WIRE_VERSION, groupId, kind, peerId(), to, hlc.now(), bodyBytes)));
            bareFramesSent.incrementAndGet();
            return;
        }
        connection.send(encodeFrame(groupId, to, kind, body));
        signedFramesSent.incrementAndGet();
    }

    private byte[] encodeFrame(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        byte[] bodyBytes = body instanceof byte[] raw ? raw : codec.toBytes(body);
        Envelope envelope =
                new Envelope(WIRE_VERSION, groupId, kind, peerId(), to, hlc.now(), bodyBytes);
        return wire.encode(envelope, identity);
    }

    private TransportConnection connectionTo(GroupId groupId, PeerId to) {
        TransportConnection existing = connections.get(to);
        if (existing != null) {
            return existing;
        }
        GroupRuntime runtime = groups.get(groupId);
        if (runtime == null) {
            return null;
        }
        // Spec §5.5: try endpoints in ascending priority (stable for ties),
        // skipping schemes this node has no transport for.
        return runtime.membership().member(to).map(member -> {
            for (PeerAdvertisement.Endpoint endpoint : byPriority(member.endpoints())) {
                Transport transport = transports.get(endpoint.transport());
                if (transport == null) {
                    continue;
                }
                try {
                    TransportConnection dialed = transport.dial(endpoint.address());
                    attach(dialed);
                    TransportConnection raced = connections.putIfAbsent(to, dialed);
                    if (raced != null) {
                        dialed.close();
                        return raced;
                    }
                    maybeAnnounceChannel(groupId, dialed);
                    return dialed;
                } catch (IOException e) {
                    // Try the next endpoint.
                }
            }
            return null;
        }).orElse(null);
    }

    private void onPeerAdItem(GroupRuntime runtime, PeerId from, byte[] payload) {
        PeerAdvertisement ad = verifiedPeerAd(payload).orElse(null);
        if (ad == null) {
            return;
        }
        if (!admits(runtime.advertisement(), ad)) {
            return; // membership policy (spec §5.1): not admitted, not a member
        }
        // v0.1.9 CA channel mode: a peer enters the view only through its own
        // attested channel (the dispatch gate already required the connection
        // to attest {@code from}); a third party's forwarded copy of its ad
        // refreshes an existing member but admits nobody, so an evicted peer
        // cannot be re-admitted by proxy.
        if (requireAttestation && !from.equals(ad.issuer())
                && runtime.membership().member(ad.issuer()).isEmpty()) {
            return;
        }
        admitAdvertisement(runtime, ad);
    }

    /**
     * Feeds an admitted, verified self-advertisement into the group's view and
     * tells the credential-hint listeners when it introduces a member or
     * changes the hints a member last presented (TODO-EFG §4).
     */
    private void admitAdvertisement(GroupRuntime runtime, PeerAdvertisement ad) {
        if (ad.issuer().equals(peerId())) {
            return; // our own advertisement: membership ignores it and so do listeners
        }
        if (runtime.revocations().revoked(ad.issuer())) {
            return; // ASF-047: a revoked peer is not re-admitted by anyone's copy of its ad
        }
        boolean admission = runtime.membership().member(ad.issuer()).isEmpty();
        runtime.membership().onPeerAdvertisement(ad);
        if (credentialHintListeners.isEmpty()) {
            return;
        }
        Map<String, String> hints = ad.resourceHints();
        Map<String, String> previous = lastHints.put(ad.issuer(), hints);
        if (admission || !hints.equals(previous)) {
            for (BiConsumer<PeerId, Map<String, String>> listener : credentialHintListeners) {
                listener.accept(ad.issuer(), hints);
            }
        }
    }

    /** The datagrams the bootstrap beacon sends each round: one signed self-ad per group. */
    private List<byte[]> beaconAnnouncements() {
        List<byte[]> announcements = new ArrayList<>();
        for (GroupId groupId : groups.keySet()) {
            announcements.add(selfAdRumor(groupId).payload());
        }
        return announcements;
    }

    /**
     * A datagram heard on the bootstrap beacon (spec §10.1): treated exactly
     * like a stranger's bootstrap rumor on the {@code peers} stream. It spends
     * one token of the shared bootstrap budget, must verify end to end as a
     * signed self-advertisement for a group this node has joined, and must
     * pass that group's admission policy; then the sender enters the view as
     * any admitted introduction would and, when this node has no link to it
     * yet, this node introduces itself to the advertised endpoints as if they
     * were configured seeds. Under {@code requireAttestation} the datagram
     * admits nobody — an unauthenticated channel cannot — and serves only as
     * a seed hint for the attested introduction that follows.
     */
    private void onBeaconDatagram(byte[] datagram) {
        if (datagram == null || datagram.length > MulticastBeacon.MAX_DATAGRAM
                || !bootstrapBucket.tryAcquire()) {
            return;
        }
        PeerAdvertisement ad = verifiedPeerAd(datagram).orElse(null);
        if (ad == null || ad.issuer().equals(peerId())) {
            return;
        }
        GroupRuntime runtime = groups.get(ad.group());
        if (runtime == null || !admits(runtime.advertisement(), ad)) {
            return;
        }
        if (!requireAttestation) {
            admitAdvertisement(runtime, ad);
        }
        if (connections.get(ad.issuer()) == null && !ad.endpoints().isEmpty()) {
            introduceTo(runtime.id(), ad.endpoints());
        }
    }

    /**
     * Decodes and verifies one peers-stream payload end-to-end: the embedded
     * key must hash to the ad's issuer, the signature must verify, and the ad
     * must be unexpired and not future-issued beyond clock skew (ASF-027 — a
     * far-future {@code issued} would otherwise make an ad un-evictable and
     * permanently newest in last-writer-wins).
     */
    /** Whether this node forwards a peers-stream payload: verifiable end-to-end, and not a revoked peer's. */
    boolean forwardsPeerAd(RevocationRegistry revocations, byte[] payload) {
        return verifiedPeerAd(payload).filter(ad -> !revocations.revoked(ad.issuer())).isPresent();
    }

    private Optional<PeerAdvertisement> verifiedPeerAd(byte[] payload) {
        SignedPeerAd signed;
        try {
            signed = codec.fromBytes(payload, SignedPeerAd.class);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (signed == null || signed.adBytes() == null || signed.publicKey() == null
                || signed.signature() == null
                || signed.publicKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return Optional.empty();
        }
        PeerAdvertisement ad;
        try {
            ad = codec.fromBytes(signed.adBytes(), PeerAdvertisement.class);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (!PeerId.fromPublicKey(signed.publicKey()).equals(ad.issuer())
                || !Ed25519.verify(Ed25519.publicKeyFromRaw(signed.publicKey()),
                        signed.adBytes(), signed.signature())
                || ad.expired(clock.instant())
                || ad.issued().isAfter(clock.instant().plus(MAX_AD_ISSUED_SKEW))) {
            return Optional.empty();
        }
        return Optional.of(ad);
    }

    /**
     * Applies a group's membership policy to a verified peer advertisement. A
     * verified signature proves the ad's authorship, not that its author may
     * join; OPEN admits everyone, INVITE requires a founder-signed credential,
     * POLICY consults the group's validator. The founder (the group's issuer)
     * always admits itself.
     */
    private boolean admits(GroupAdvertisement groupAd, PeerAdvertisement ad) {
        if (ad.issuer().equals(groupAd.issuer())) {
            return true; // the founder created the group
        }
        return switch (groupAd.membershipPolicy()) {
            case OPEN -> true;
            case INVITE -> admitsInvite(groupAd, ad);
            case POLICY -> {
                MembershipValidator validator = groupValidators.get(groupAd.group());
                yield validator != null
                        && validator.admit(ad.issuer(), groupAd, ad.resourceHints());
            }
        };
    }

    /**
     * INVITE admission: a founder-signed credential for this candidate that no
     * {@code JOIN_CREDENTIAL} revocation names (SPEC §6.1, v0.1.13). Records
     * which credential admitted the member, so revoking it evicts the member.
     */
    private boolean admitsInvite(GroupAdvertisement groupAd, PeerAdvertisement ad) {
        String credential = ad.resourceHints().get(JoinCredentials.HINT_KEY);
        if (!JoinCredentials.verify(credential, ad.issuer(), groupAd, codec)) {
            return false;
        }
        byte[] hash = JoinCredentials.hash(credential);
        GroupRuntime runtime = groups.get(groupAd.group());
        if (runtime != null && runtime.credentialRevocations().joinCredentialRevoked(hash)) {
            return false;
        }
        admittedUnder.computeIfAbsent(groupAd.group(), g -> new ConcurrentHashMap<>())
                .put(ad.issuer(), HexFormat.of().formatHex(hash));
        return true;
    }

    /** This node's self-ad hints for a group: node-wide hints under the group's own. */
    private Map<String, String> selfHints(GroupId groupId) {
        Map<String, String> group = groupHints.getOrDefault(groupId, Map.of());
        if (nodeHints.isEmpty()) {
            return group;
        }
        Map<String, String> merged = new java.util.HashMap<>(nodeHints);
        merged.putAll(group);
        return merged;
    }

    private Bodies.Rumor selfAdRumor(GroupId groupId) {
        PeerAdvertisement ad = new PeerAdvertisement(
                "aspace://" + groupId.value() + "/peer/" + peerId().value(),
                peerId(), groupId, clock.instant(), peerAdTtl,
                List.copyOf(advertisedEndpoints), roles,
                selfHints(groupId));
        byte[] adBytes = codec.toBytes(ad);
        byte[] payload = codec.toBytes(
                new SignedPeerAd(adBytes, identity.rawPublicKey(), identity.sign(adBytes)));
        String itemId = "peer:" + peerId().value() + ":" + ad.issued().toEpochMilli();
        return new Bodies.Rumor(PEER_AD_STREAM, itemId, 6, payload);
    }

    /**
     * The leased, signed self-description that travels on the {@code peers}
     * stream: the canonical bytes of a PeerAdvertisement, the issuer's raw public
     * key, and the signature over those bytes.
     *
     * @param adBytes   canonical CBOR of the PeerAdvertisement
     * @param publicKey the issuer's raw Ed25519 public key
     * @param signature signature over {@code adBytes}
     */
    public record SignedPeerAd(byte[] adBytes, byte[] publicKey, byte[] signature) {
    }
}
