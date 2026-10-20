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
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.api.spi.LocatePolicy;
import ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.peering.bootstrap.SeedsLocator;
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
        /** Every frame carries the identity signature. */
        SIGNED,
        /** Frames toward a peer that announced it accepts them travel bare on a connection the transport attested. */
        ATTESTED,
        /**
         * As ATTESTED where the transport attests, and on a connection it cannot attest a
         * signed SESSION_HELLO on each side establishes a session key, after which frames
         * carry a MAC under it instead of a signature (spec §5.6, v0.1.17). The dev-local
         * profile's mode: plain TCP with one signature per connection rather than per frame.
         */
        SESSION
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
    static final int RECENT_FRAMES_CAPACITY = 8_192;
    /**
     * How old a SESSION_HELLO may be and still be answered (ROBUSTNESS.md §5, the hello replay
     * window). A live hello is seconds old at most, its stamp merged toward what the opener last
     * heard from this node, so thirty seconds covers every honest one; a captured hello replayed
     * on a fresh link after the exact-replay cache has turned over is the one frame kind that can
     * create state at its recipient, and this bound is what makes that capture worthless.
     */
    static final long SESSION_HELLO_MAX_AGE_MILLIS = 30_000L;

    /** The gossip stream peer advertisements travel on. */
    private static final String PEER_AD_STREAM = "peers";
    /** The anti-entropy state carrying the live members' signed self-advertisements. */
    static final String VIEW_STATE = "view";

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
    /** How often the self-advertisement is re-flooded through the group (the refresh); between refreshes
     *  it goes directly, once, to each member this node has not introduced itself to yet. */
    private final Duration peerAdInterval;
    /** The coalescing window, or null when frames go out one by one (PERF2 §3.3). */
    private final Duration coalesceWindow;
    /** Members whose advertisement said they accept BATCH frames. */
    private final Set<PeerId> batchPeers = ConcurrentHashMap.newKeySet();
    private final Map<PeerId, Outbox> outboxes = new ConcurrentHashMap<>();
    private final java.util.concurrent.ScheduledExecutorService flusher;
    /** The hint a node advertises when it accepts BATCH frames. */
    public static final String BATCH_HINT = "wire.batch";
    /** The hint a node advertises when it understands lazy push (HAVE, GRAFT, PRUNE; PERF2 §3.4). */
    public static final String TREE_HINT = "wire.tree";
    /** The hint a node advertises when it establishes sessions on unattested connections (spec §5.6, v0.1.17). */
    public static final String SESSION_HINT = "wire.session";
    /** The hint a node advertises when it answers a DIGEST of hashes (spec §5.3, v0.1.17). */
    public static final String HASHES_HINT = "wire.hashes";
    /** Members whose advertisement carries {@link #HASHES_HINT}. */
    private final Set<PeerId> hashPeers = ConcurrentHashMap.newKeySet();
    /** Members admitted since the last tick, per group: each gets a targeted anti-entropy round on the next tick. */
    private final Map<GroupId, Set<PeerId>> newlyAdmitted = new ConcurrentHashMap<>();
    /** The seeds each group was joined with, re-dialed when the view is empty (ROBUSTNESS.md §3.3, §3.6). */
    private final Map<GroupId, List<PeerAdvertisement.Endpoint>> groupSeeds = new ConcurrentHashMap<>();
    /** The locator each group was joined with, re-asked on an empty view once its last hint has aged. */
    private final Map<GroupId, BeaconLocator> groupLocators = new ConcurrentHashMap<>();
    /** The configured name of each group, for the locator's request. */
    private final Map<GroupId, String> groupNames = new ConcurrentHashMap<>();
    /** The last hint each group's locator answered. */
    private final Map<GroupId, BeaconHint> lastBeaconHints = new ConcurrentHashMap<>();
    /** When each group's last hint expires (its TTL past the time it was answered). */
    private final Map<GroupId, Long> hintExpiryMillis = new ConcurrentHashMap<>();
    /** When each group's locator was last asked. */
    private final Map<GroupId, Long> lastLocateMillis = new ConcurrentHashMap<>();
    /** Groups with a locate in flight on a virtual thread. */
    private final Set<GroupId> locating = ConcurrentHashMap.newKeySet();
    /** The locator is never re-asked faster than this, whatever its hints' TTL says. */
    static final long LOCATE_MIN_INTERVAL_MILLIS = 30_000L;
    /** The node-wide re-ask floor and ceiling (builder-set; a locator's policy overrides per group). */
    private final long locateMinMillis;
    private final long locateMaxMillis;
    /** The trace of each group's last locate, when its locator is a chain. */
    private final Map<GroupId, List<BootstrapChain.Trace>> lastBootstrapTraces = new ConcurrentHashMap<>();
    /** The endpoints learned from verified beacon datagrams, per group and sender, with the ad's expiry. */
    private final Map<GroupId, Map<PeerId, LearnedEndpoints>> beaconLearned = new ConcurrentHashMap<>();

    private record LearnedEndpoints(List<PeerAdvertisement.Endpoint> endpoints, long expiresMillis) {
    }

    /**
     * What the multicast beacon heard for a group (SRAD DB-L-08): the
     * unexpired endpoints, how many senders, and the shortest remaining life.
     *
     * @param endpoints the endpoints, priorities by position
     * @param peers     how many senders contributed
     * @param ttl       the shortest remaining life, or zero when none
     */
    public record BeaconLearned(List<PeerAdvertisement.Endpoint> endpoints, int peers, Duration ttl) {
    }
    /** The locator is re-asked at least this often while the view stays empty, whatever its hints' TTL says. */
    static final long LOCATE_MAX_INTERVAL_MILLIS = 10 * 60_000L;
    /** The endpoints each member last advertised, kept past its departure for the lost-member retry. */
    private final Map<PeerId, List<PeerAdvertisement.Endpoint>> lastEndpoints = new ConcurrentHashMap<>();
    /** Members that left each group's view and when: introduced to again for a while, in case they are back. */
    private final Map<GroupId, Map<PeerId, Long>> lostMembers = new ConcurrentHashMap<>();
    /** The signed self-advertisement last admitted from each live member, for the view's anti-entropy. */
    private final Map<GroupId, Map<PeerId, HeldAd>> heldAds = new ConcurrentHashMap<>();

    private record HeldAd(long issuedMillis, byte[] payload) {
    }
    private final Map<GroupId, Long> lastLostRetryMillis = new ConcurrentHashMap<>();
    /** How long after a member's loss this node keeps introducing itself to its last endpoints. */
    static final long LOST_RETRY_WINDOW_MILLIS = 10 * 60_000L;
    /** How soon after a loss the lost members and (on an empty view) the seeds are first tried. */
    static final long LOST_RETRY_INTERVAL_MILLIS = 5_000L;
    /**
     * The retry interval doubles after each round that admitted nobody, up to this cap, and snaps
     * back on an admission (ROBUSTNESS.md §5, the retry bound): a pair alone under a one-way loss
     * otherwise re-introduces, re-admits and re-drops at full rate for as long as the loss lasts.
     */
    static final long LOST_RETRY_MAX_INTERVAL_MILLIS = 30_000L;
    /** The current retry interval per group: the base after an admission, doubling while nobody answers. */
    private final Map<GroupId, Long> lostRetryIntervalMillis = new ConcurrentHashMap<>();
    /** When a probe's digests last started a targeted pull from a peer, so a persistent mismatch pulls once per period. */
    private final Map<PeerId, Long> probePulls = new ConcurrentHashMap<>();
    /** Members whose advertisement carries {@link #SESSION_HINT}. */
    private final Set<PeerId> sessionPeers = ConcurrentHashMap.newKeySet();
    /** Session state per connection: this node's ephemeral half, the remote half once heard, the derived key. */
    private final Map<TransportConnection, Session> sessions = new ConcurrentHashMap<>();
    private final AtomicLong sessionFramesSent = new AtomicLong();
    private static final java.security.SecureRandom SESSION_RANDOM = new java.security.SecureRandom();
    private static final byte[] SESSION_INFO = "agentspaces session v1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final int SESSION_NONCE_LENGTH = 32;

    private static final class Session {
        final java.security.KeyPair ephemeral = ai.badmonkey.agentspaces.common.crypto.X25519.generate();
        final byte[] nonce = new byte[SESSION_NONCE_LENGTH];
        volatile boolean helloSent;
        volatile long helloSentAtMillis;
        /** Whether this node's hello was the answering one. */
        volatile boolean answered;
        volatile PeerId remote;
        volatile byte[] remoteKey;
        volatile byte[] remoteNonce;
        volatile byte[] key;
        /** Whether the other end provably holds the key: outbound frames carry a MAC only then. */
        volatile boolean confirmed;

        Session() {
            SESSION_RANDOM.nextBytes(nonce);
        }

        WireCodec.Session established() {
            byte[] k = key;
            return k == null ? null : new WireCodec.Session(remote, k);
        }
    }
    /** Members whose advertisement carries {@link #TREE_HINT}: the gossip bus pushes to them lazily. */
    private final Set<PeerId> treePeers = ConcurrentHashMap.newKeySet();
    private final boolean lazyPush;
    private static final int BATCH_MAX_FRAMES = 64;
    private static final int BATCH_MAX_BYTES = 48 * 1024;

    /** Frames waiting for one destination, flushed as one signed BATCH. */
    private static final class Outbox {
        final GroupId group;
        final List<byte[]> frames = new ArrayList<>();
        int bytes;

        Outbox(GroupId group) {
            this.group = group;
        }
    }
    private final Map<GroupId, Long> lastSelfAdMillis = new ConcurrentHashMap<>();
    private final Map<GroupId, Set<PeerId>> selfAdSentTo = new ConcurrentHashMap<>();
    /** The content the last flooded advertisement carried per group; a change floods at once. */
    private final Map<GroupId, Integer> lastSelfAdFingerprint = new ConcurrentHashMap<>();
    /** The membership view at the end of the last tick, per group, to notice drops between ticks. */
    private final Map<GroupId, Set<PeerId>> lastView = new ConcurrentHashMap<>();
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
    /**
     * Per group, the join credential hash and delegation hash each member was admitted under, kept
     * past a lapse and across restarts so a member admitted inside an invite window stays admissible
     * after it (ISSUE-PeerRobustness-2, R2-1).
     */
    private final AdmissionMemory admissionMemory;
    /** Members re-admitted from memory after their delegation expired, logged once each. */
    private final Set<String> readmittedLogged = ConcurrentHashMap.newKeySet();
    /** The host's say over what this node relays (R2-3), asked after the membership check. */
    private final RelayPolicy relayPolicy;
    /** When a relay policy failure was last logged; once a minute at most (R2-S3). */
    private volatile Long lastPolicyFailureLogMillis;
    /** Per group, what this node relayed and dropped (R2-3.3), with the per-target split of ISSUE-BootstrapCore FR-2. */
    private final Map<GroupId, RelayCounters> relayCounters = new ConcurrentHashMap<>();
    /** Bound on per-target relay counters per group; least-recently-relayed-to evict (PR-2, ASF-019). */
    static final int RELAY_TARGETS_CAPACITY = 4_096;
    /** JS-W-11: when this node dials a member it is otherwise relayed to, and how soon it retries. */
    private final RelayOffload relayOffload;
    /**
     * Per group, per far member, the clock stamps of the frames relayed between this node and
     * it in the last minute: sent through a relay, or unwrapped from a RELAY_FRAME addressed
     * here. Bounded as the per-target counters are, least recently relayed-with evicted first.
     */
    private final Map<GroupId, Map<PeerId, java.util.ArrayDeque<Long>>> relayedWith = new ConcurrentHashMap<>();
    /** Per group, per member, when the last offload dial started: the backoff of JS-W-11. */
    private final Map<GroupId, Map<PeerId, Long>> offloadAttempted = new ConcurrentHashMap<>();
    /** Members an offload dial is in flight to, so one dial at a time reaches for each. */
    private final Set<PeerId> offloadInFlight = ConcurrentHashMap.newKeySet();

    private static final class RelayCounters {
        final AtomicLong framesForwarded = new AtomicLong();
        final AtomicLong bytesForwarded = new AtomicLong();
        final AtomicLong droppedNotMember = new AtomicLong();
        final AtomicLong droppedByPolicy = new AtomicLong();
        final AtomicLong offloadAttempts = new AtomicLong();
        final AtomicLong offloadOpened = new AtomicLong();
        final AtomicLong offloadFailed = new AtomicLong();
        /**
         * Per target that passed the membership check, so the key is always a verified member's
         * PeerId (SR-2): a flood naming fake targets reaches neither this map nor the policy.
         */
        final Map<PeerId, RelayCounters> targets;

        RelayCounters(boolean perTarget) {
            targets = !perTarget ? null : java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(64, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(Map.Entry<PeerId, RelayCounters> eldest) {
                            return size() > RELAY_TARGETS_CAPACITY;
                        }
                    });
        }

        RelayCounters target(PeerId target) {
            return targets.computeIfAbsent(target, t -> new RelayCounters(false));
        }

        RelayStats snapshot() {
            return new RelayStats(framesForwarded.get(), bytesForwarded.get(),
                    droppedNotMember.get(), droppedByPolicy.get());
        }
    }
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
    /** Groups whose beacon datagrams are sealed ({@link #beaconKey}). */
    private final Map<GroupId, ai.badmonkey.agentspaces.peering.bootstrap.BeaconSeal> beaconSeals =
            new ConcurrentHashMap<>();
    /** Resources closed with the node ({@link #closeWith}). */
    private final java.util.List<AutoCloseable> ownedResources = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<AutoCloseable> listeners = new ArrayList<>();
    private final List<PeerAdvertisement.Endpoint> advertisedEndpoints = new ArrayList<>();
    private final Map<PeerId, TransportConnection> connections = new ConcurrentHashMap<>();
    /** The scheme each live connection came through (its listener's or its dialer's), for the console (JS-N-07). */
    private final Map<TransportConnection, String> connectionSchemes = new ConcurrentHashMap<>();
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
    private final AtomicLong framesStale = new AtomicLong();
    private final AtomicLong framesReplayed = new AtomicLong();
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
    private final AtomicLong bytesSent = new AtomicLong();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final Map<Envelope.Kind, AtomicLong> sentByKind = new java.util.EnumMap<>(Envelope.Kind.class);
    private final Map<Envelope.Kind, AtomicLong> receivedByKind = new java.util.EnumMap<>(Envelope.Kind.class);

    /** A snapshot of this node's wire traffic since it started (PERF2 §4: frames per kind, and bytes). */
    public record Traffic(long framesSent, long framesReceived, long bytesSent, long bytesReceived,
                          Map<Envelope.Kind, Long> sentByKind, Map<Envelope.Kind, Long> receivedByKind,
                          Map<Envelope.Kind, Long> bytesSentByKind) {
    }

    public Traffic traffic() {
        Map<Envelope.Kind, Long> sent = new java.util.EnumMap<>(Envelope.Kind.class);
        sentByKind.forEach((k, v) -> sent.put(k, v.get()));
        Map<Envelope.Kind, Long> received = new java.util.EnumMap<>(Envelope.Kind.class);
        receivedByKind.forEach((k, v) -> received.put(k, v.get()));
        long framesSent = sent.values().stream().mapToLong(Long::longValue).sum();
        long framesReceived = received.values().stream().mapToLong(Long::longValue).sum();
        Map<Envelope.Kind, Long> bytesByKind = new java.util.EnumMap<>(Envelope.Kind.class);
        bytesSentByKind.forEach((k, v) -> bytesByKind.put(k, v.get()));
        return new Traffic(framesSent, framesReceived, bytesSent.get(), bytesReceived.get(), sent, received, bytesByKind);
    }

    /**
     * What this node relayed for one group since it joined (ISSUE-PeerRobustness-2, R2-3.3):
     * frames and bytes forwarded, frames dropped because the target was no member of the group
     * in this node's view, and frames dropped by the {@link RelayPolicy}.
     */
    public record RelayStats(long framesForwarded, long bytesForwarded, long droppedNotMember,
                             long droppedByPolicy) {
    }

    /**
     * The relay counters per group, a snapshot; a group this node never relayed in has no entry,
     * and leaving a group clears its entry.
     *
     * @return group to counters
     */
    public Map<GroupId, RelayStats> relayStats() {
        Map<GroupId, RelayStats> stats = new java.util.HashMap<>();
        relayCounters.forEach((group, counters) -> stats.put(group, counters.snapshot()));
        return stats;
    }

    /**
     * The relay counters of one group per target (ISSUE-BootstrapCore FR-2, SR-2), a snapshot:
     * keyed by the target's verified PeerId, held only for targets that were members of the group
     * in this node's view when the frame arrived, and bounded per group at
     * {@link #RELAY_TARGETS_CAPACITY} with the least recently relayed-to evicted first. A frame
     * dropped because its target was no member counts in the group's total alone, since such a
     * target is whatever id the sender named.
     *
     * @param groupId the group
     * @return target to counters; empty for a group this node never relayed in
     */
    public Map<PeerId, RelayStats> relayStats(GroupId groupId) {
        RelayCounters counters = relayCounters.get(Objects.requireNonNull(groupId, "groupId"));
        if (counters == null) {
            return Map.of();
        }
        Map<PeerId, RelayStats> stats = new java.util.HashMap<>();
        synchronized (counters.targets) {
            counters.targets.forEach((target, c) -> stats.put(target, c.snapshot()));
        }
        return stats;
    }

    private RelayCounters relayCounters(GroupId groupId) {
        return relayCounters.computeIfAbsent(groupId, g -> new RelayCounters(true));
    }

    /**
     * The relay offload of SRAD srad/agentspaces-js.md JS-W-11: a node whose frames to or from
     * a member travel through a relay dials that member's signalled endpoint (a {@code webrtc}
     * acceptor, {@link Transport#signalled()}) once more than {@code framesPerMinute} of them
     * crossed the relay in a sliding minute, from its tick and never from inside a send, and
     * after a failed dial waits {@code retryAfter} before the next. Both are settings, not
     * caps: nothing here bounds members or connections.
     *
     * @param framesPerMinute the relayed frames in one minute above which the node dials; 30
     * @param retryAfter      the wait after a failed dial before another; 5 minutes
     */
    public record RelayOffload(int framesPerMinute, Duration retryAfter) {

        public RelayOffload {
            Objects.requireNonNull(retryAfter, "retryAfter");
            if (framesPerMinute < 1) {
                throw new IllegalArgumentException("frames-per-minute must be positive: " + framesPerMinute);
            }
            if (retryAfter.isNegative() || retryAfter.isZero()) {
                throw new IllegalArgumentException("retry-after must be positive: " + retryAfter);
            }
        }

        /** The SRAD's defaults: 30 frames a minute, a 5 minute retry. */
        public static RelayOffload defaults() {
            return new RelayOffload(30, Duration.ofMinutes(5));
        }
    }

    /**
     * What the relay offload did for one group since this node joined (JS-W-11): dials started
     * because the relayed traffic with a member passed the threshold, the ones that opened a
     * connection, and the ones that failed and entered the backoff.
     */
    public record OffloadStats(long attempts, long opened, long failed) {
    }

    /** The relay offload settings this node runs with. */
    public RelayOffload relayOffload() {
        return relayOffload;
    }

    /**
     * The relay offload counters per group, a snapshot; a group this node never relayed in
     * has no entry.
     *
     * @return group to counters
     */
    public Map<GroupId, OffloadStats> relayOffloadStats() {
        Map<GroupId, OffloadStats> stats = new java.util.HashMap<>();
        relayCounters.forEach((group, c) -> stats.put(group,
                new OffloadStats(c.offloadAttempts.get(), c.offloadOpened.get(), c.offloadFailed.get())));
        return stats;
    }

    /**
     * How many frames were relayed between this node and a member in the last minute, as the
     * offload counts them (JS-W-11); zero for a member nothing was relayed with.
     *
     * @param groupId the group
     * @param member  the far member
     * @return the frames in the sliding minute
     */
    public int relayedWithInLastMinute(GroupId groupId, PeerId member) {
        Map<PeerId, java.util.ArrayDeque<Long>> perMember = relayedWith.get(Objects.requireNonNull(groupId, "groupId"));
        if (perMember == null) {
            return 0;
        }
        java.util.ArrayDeque<Long> times = perMember.get(Objects.requireNonNull(member, "member"));
        return times == null ? 0 : windowCount(times, clock.millis());
    }

    /** Records one frame relayed between this node and {@code member}, stamped by the node's clock. */
    private void noteRelayed(GroupId groupId, PeerId member) {
        Map<PeerId, java.util.ArrayDeque<Long>> perMember = relayedWith.computeIfAbsent(groupId, g ->
                java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<>(64, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<PeerId, java.util.ArrayDeque<Long>> eldest) {
                        return size() > RELAY_TARGETS_CAPACITY;
                    }
                }));
        java.util.ArrayDeque<Long> times = perMember.computeIfAbsent(member, m -> new java.util.ArrayDeque<>());
        long now = clock.millis();
        synchronized (times) {
            windowCount(times, now);
            times.addLast(now);
        }
    }

    /** Drops stamps older than a minute and returns what remains; the caller holds the deque's lock or owns it. */
    private static int windowCount(java.util.ArrayDeque<Long> times, long now) {
        synchronized (times) {
            while (!times.isEmpty() && now - times.peekFirst() >= 60_000L) {
                times.pollFirst();
            }
            return times.size();
        }
    }

    /**
     * JS-W-11, run from the tick: for each member more than {@code framesPerMinute} frames were
     * relayed with in the last minute, with no connection here, no dial in flight, and no
     * failed dial inside the backoff, dial its first signalled endpoint on a thread of its own,
     * because a signalled dial may take its whole timeout and the tick must not wait for it.
     * A member whose endpoints this node cannot signal is left to the relay.
     */
    private void offloadRelayed(GroupRuntime runtime, long nowMillis) {
        GroupId groupId = runtime.id();
        Map<PeerId, java.util.ArrayDeque<Long>> perMember = relayedWith.get(groupId);
        if (perMember == null) {
            return;
        }
        List<PeerId> members;
        synchronized (perMember) {
            members = List.copyOf(perMember.keySet());
        }
        for (PeerId member : members) {
            java.util.ArrayDeque<Long> times = perMember.get(member);
            if (times == null || windowCount(times, nowMillis) <= relayOffload.framesPerMinute()) {
                continue;
            }
            if (connections.containsKey(member) || offloadInFlight.contains(member)) {
                continue;
            }
            Long attempted = offloadAttempted.getOrDefault(groupId, Map.of()).get(member);
            if (attempted != null && nowMillis - attempted < relayOffload.retryAfter().toMillis()) {
                continue;
            }
            GroupMembership.Member known = runtime.membership().member(member).orElse(null);
            if (known == null) {
                continue;
            }
            PeerAdvertisement.Endpoint signalled = null;
            Transport transport = null;
            for (PeerAdvertisement.Endpoint endpoint : byPriority(known.endpoints())) {
                Transport candidate = transports.get(endpoint.transport());
                if (candidate != null && candidate.signalled()) {
                    signalled = endpoint;
                    transport = candidate;
                    break;
                }
            }
            if (signalled == null || !offloadInFlight.add(member)) {
                continue;
            }
            offloadAttempted.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>()).put(member, nowMillis);
            RelayCounters counters = relayCounters(groupId);
            counters.offloadAttempts.incrementAndGet();
            PeerAdvertisement.Endpoint endpoint = signalled;
            Transport dialer = transport;
            Thread.ofVirtual().name("peer-offload-" + member.display()).start(() -> {
                try {
                    dialEndpoint(groupId, member, dialer, endpoint);
                    counters.offloadOpened.incrementAndGet();
                    offloadAttempted.getOrDefault(groupId, Map.of()).remove(member);
                    synchronized (times) {
                        times.clear(); // the next relayed frame starts a fresh count
                    }
                    LOG.log(System.Logger.Level.DEBUG, "offloaded " + member.display() + " from the relay onto "
                            + dialer.scheme() + " in group " + groupId.value());
                } catch (IOException e) {
                    counters.offloadFailed.incrementAndGet();
                    LOG.log(System.Logger.Level.DEBUG, "offload dial to " + member.display() + " over "
                            + dialer.scheme() + " failed; the relay carries on, retry after "
                            + relayOffload.retryAfter() + ": " + e.getMessage());
                } finally {
                    offloadInFlight.remove(member);
                }
            });
        }
    }

    private void countSent(Envelope.Kind kind, int bytes) {
        sentByKind.computeIfAbsent(kind, k -> new AtomicLong()).incrementAndGet();
        bytesSentByKind.computeIfAbsent(kind, k -> new AtomicLong()).addAndGet(bytes);
        bytesSent.addAndGet(bytes);
    }
    private final Map<Envelope.Kind, AtomicLong> bytesSentByKind = new ConcurrentHashMap<>();
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
        this.peerAdInterval = builder.peerAdInterval.compareTo(builder.peerAdTtl.dividedBy(2)) > 0
                ? builder.peerAdTtl.dividedBy(2) : builder.peerAdInterval;
        this.coalesceWindow = builder.coalesceWindow;
        this.lazyPush = builder.lazyPush;
        if (lazyPush) {
            nodeHints.put(TREE_HINT, "1");
        }
        if (builder.channelAuth == ChannelAuth.SESSION) {
            nodeHints.put(SESSION_HINT, "1");
        }
        nodeHints.put(HASHES_HINT, "1");
        if (coalesceWindow != null) {
            nodeHints.put(BATCH_HINT, "1");
            flusher = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "peer-flush-" + builder.identity.peerId().display());
                t.setDaemon(true);
                return t;
            });
            flusher.scheduleAtFixedRate(this::flushOutboxes, coalesceWindow.toMillis(), coalesceWindow.toMillis(),
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        } else {
            flusher = null;
        }
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
        this.admissionMemory = new AdmissionMemory(builder.stateDirectory, builder.admissionMemoryBound, clock);
        this.relayPolicy = builder.relayPolicy;
        this.relayOffload = builder.relayOffload;
        this.locateMinMillis = builder.locateMinMillis;
        this.locateMaxMillis = builder.locateMaxMillis;
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
        private Duration peerAdInterval = Duration.ofSeconds(30);
        private boolean lazyPush = true;
        private Duration coalesceWindow;
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
        private java.nio.file.Path stateDirectory;
        private int admissionMemoryBound = AdmissionMemory.DEFAULT_BOUND;
        private RelayPolicy relayPolicy = RelayPolicy.ALLOW;
        private RelayOffload relayOffload = RelayOffload.defaults();

        private Builder(PeerIdentity identity) {
            this.identity = Objects.requireNonNull(identity, "identity");
        }

        /**
         * The directory this node keeps state in across restarts (ISSUE-PeerRobustness-2,
         * R2-1.5): today its admission memory, one file per group under {@code admitted/}.
         * The Spring starter and the Micronaut factory pass the keystore directory when one
         * is configured. Without one, a member admitted under a delegation before it
         * expired is forgotten by a restart, and the node says so once per group.
         *
         * @param directory the state directory; created when absent
         * @return this builder
         */
        public Builder stateDirectory(java.nio.file.Path directory) {
            this.stateDirectory = Objects.requireNonNull(directory, "directory");
            return this;
        }

        /**
         * Bounds the admission memory per group (R2-1.6, default 10,000 peers); past the
         * bound the member least recently seen is forgotten first.
         *
         * @param peers the bound
         * @return this builder
         */
        public Builder admissionMemoryBound(int peers) {
            if (peers <= 0) {
                throw new IllegalArgumentException("the admission memory bound must be positive: " + peers);
            }
            this.admissionMemoryBound = peers;
            return this;
        }

        /**
         * Sets the policy this node asks before each frame it relays (SPEC §5.4, R2-3.2),
         * after the membership check that nothing can relax. The default allows every
         * forward the membership check allows.
         *
         * @param policy the policy
         * @return this builder
         */
        public Builder relayPolicy(RelayPolicy policy) {
            this.relayPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /**
         * Sets the relay offload of SRAD srad/agentspaces-js.md JS-W-11 ({@link RelayOffload}):
         * the relayed frames a minute above which this node dials a member's signalled
         * endpoint, and the wait after a failed dial. Defaults: 30 and 5 minutes.
         *
         * @param offload the settings
         * @return this builder
         */
        public Builder relayOffload(RelayOffload offload) {
            this.relayOffload = Objects.requireNonNull(offload, "offload");
            return this;
        }

        /**
         * The node-wide pacing of a locator's re-ask while a group's view is
         * empty (SRAD DB-N-03): never faster than {@code min} whatever a
         * hint's TTL says, and at least once per {@code max}. A locator's own
         * {@link LocatePolicy} overrides per group. Defaults: 30 s and 10 min.
         *
         * @param min the floor
         * @param max the ceiling
         * @return this builder
         */
        public Builder locateInterval(Duration min, Duration max) {
            Objects.requireNonNull(min, "min");
            Objects.requireNonNull(max, "max");
            if (min.isNegative() || max.compareTo(min) < 0) {
                throw new IllegalArgumentException("locateInterval needs 0 <= min <= max");
            }
            this.locateMinMillis = min.toMillis();
            this.locateMaxMillis = max.toMillis();
            return this;
        }
        private long locateMinMillis = LOCATE_MIN_INTERVAL_MILLIS;
        private long locateMaxMillis = LOCATE_MAX_INTERVAL_MILLIS;

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
         * How often this node re-floods its self-advertisement through each group (default 30 s,
         * capped at half the advertisement TTL). The advertisement used to go out on every tick,
         * which at a 250 ms tick with fanout 3 and six hops had every member re-verifying every
         * other member's signature several times a second, growing with the fleet. Between
         * refreshes a member this node has not yet introduced itself to receives the advertisement
         * directly, once, so discovery of a newcomer is not slowed by the interval.
         *
         * @param interval the refresh interval
         * @return this builder
         */
        /**
         * Coalesces rumors, digests, pull responses and pipe frames to one destination into one
         * signed BATCH frame per window (PERF2 §3.3): one signature and one verification where
         * there were many. Only toward peers that advertise {@code wire.batch=1}; other peers get
         * single frames. Off by default (hand-driven tests expect immediate delivery); the starter
         * turns it on with {@code agentspaces.wire.coalesce-millis}. Adds up to one window of
         * latency per hop, which must stay well under a space's settle window.
         *
         * @param window the flush period, or null to send every frame at once
         * @return this builder
         */
        public Builder coalesce(Duration window) {
            if (window != null && (window.isZero() || window.isNegative())) {
                throw new IllegalArgumentException("the coalescing window must be positive");
            }
            this.coalesceWindow = window;
            return this;
        }

        /**
         * Whether this node pushes rumors lazily to members that understand it (PERF2 §3.4,
         * Plumtree): on by default, which advertises the {@link #TREE_HINT}. Off, the node
         * neither advertises the hint nor treats any member as capable, so it gossips with the
         * random fan-out alone and is indistinguishable on the wire from an older node.
         *
         * @param enabled whether to push lazily
         * @return this builder
         */
        public Builder lazyPush(boolean enabled) {
            this.lazyPush = enabled;
            return this;
        }

        public Builder peerAdInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("peerAdInterval must be positive");
            }
            this.peerAdInterval = interval;
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
     * Sets one membership hint of one joined group (SRAD DB-N-12): the join
     * credential this node presents there, replaced when an invite key
     * rotates, without touching the hints of other groups this node hosts.
     * The next self-advertisement carries it.
     *
     * @param groupId the group
     * @param key     the hint key ({@code aspace:join})
     * @param value   the value, or null to remove the hint
     */
    public void groupHint(GroupId groupId, String key, String value) {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(key, "key");
        groupHints.compute(groupId, (g, current) -> {
            Map<String, String> next = new java.util.HashMap<>(current == null ? Map.of() : current);
            if (value == null) {
                next.remove(key);
            } else {
                next.put(key, value);
            }
            return Map.copyOf(next);
        });
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

    /** Returns how many session-authenticated (MAC, unsigned) frames this node has sent. */
    public long sessionFramesSent() {
        return sessionFramesSent.get();
    }

    /** The groups this node has joined, a snapshot (diagnostics). */
    public java.util.Collection<GroupRuntime> groups() {
        return List.copyOf(groups.values());
    }

    /** The live connection this node holds toward a peer, if any (diagnostics and tests). */
    TransportConnection connectionFor(PeerId peer) {
        return connections.get(peer);
    }

    /**
     * Returns how many inbound frames the wire codec refused outright (spec
     * §9): failed signature, half-signed, unknown wire version, or garbage.
     * Such frames name no verified sender, so they count here and strike
     * nobody.
     */
    /**
     * How many verified frames this node dropped as stale, older than five minutes by its own
     * clock (ROBUSTNESS.md §3.3). Not a strike and not in {@link #framesDropped()}: a late frame
     * is the network's doing, not its signer's.
     */
    public long framesStale() {
        return framesStale.get();
    }

    /**
     * How many verified frames this node dropped as exact replays, a sender-and-stamp pair seen
     * before (ROBUSTNESS.md §3.4). Not a strike and not in {@link #framesDropped()}: a duplicate
     * delivery is the network's doing, and a replayed capture its signer never sent twice.
     */
    public long framesReplayed() {
        return framesReplayed.get();
    }

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
    /**
     * Returns the transport scheme of each connected peer's connection ({@code tcp}, {@code tls},
     * {@code ws}, {@code webrtc}, ...), for the console (srad/agentspaces-js.md JS-N-07): a
     * snapshot; a peer whose connection came through a transport registered before this
     * accounting (none in the shipped bindings) shows {@code "?"}.
     *
     * @return peer to scheme
     */
    public Map<PeerId, String> connectionSchemes() {
        Map<PeerId, String> schemes = new java.util.HashMap<>();
        connections.forEach((peer, connection) -> schemes.put(peer, connectionSchemes.getOrDefault(connection, "?")));
        return schemes;
    }

    public Map<PeerId, String> channelModes() {
        Map<PeerId, String> modes = new java.util.HashMap<>();
        connections.forEach((peer, connection) -> {
            PeerId announced = bareAccepted.get(connection);
            boolean attested = channelAuth != ChannelAuth.SIGNED
                    && announced != null
                    && connection.attestedPeer().filter(announced::equals).isPresent();
            Session session = sessions.get(connection);
            boolean inSession = session != null && session.confirmed && peer.equals(session.remote);
            modes.put(peer, attested ? "attested" : inSession ? "session" : "signed");
        });
        return modes;
    }

    /**
     * A connection ended (the transport tells us at once, v0.1.17): forget it under whichever
     * peer it served, so the next tick re-dials and re-introduces this node to that peer
     * rather than a later send discovering the loss. Idempotent, and safe from any thread.
     */
    private void onConnectionClosed(TransportConnection connection) {
        for (Map.Entry<PeerId, TransportConnection> entry : List.copyOf(connections.entrySet())) {
            if (entry.getValue() == connection) {
                dropConnection(entry.getKey(), connection);
            }
        }
        sessions.remove(connection);
        bareAccepted.remove(connection);
        helloSent.remove(connection);
        chained.remove(connection);
    }

    private void dropConnection(PeerId peer, TransportConnection connection) {
        connections.remove(peer, connection);
        connectionSchemes.remove(connection);
        bareAccepted.remove(connection);
        helloSent.remove(connection);
        sessions.remove(connection);
        // A link this node gives up on is closed, not merely forgotten: the other end then learns
        // of it (onClose) and re-dials with a fresh handshake, rather than keeping a session this
        // node no longer holds and sending frames into it that would be dropped here unverified.
        connection.close();
        // A peer whose channel went away is introduced to again on the next tick (publishSelfAd).
        selfAdSentTo.values().forEach(sent -> sent.remove(peer));
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
        listen(transport, bindAddress, bindAddress, priority);
    }

    /**
     * Registers a transport, starts listening on {@code bindAddress}, and
     * advertises {@code advertisedAddress}: the address other peers dial,
     * which differs from the bind address on a host that binds every
     * interface, sits behind a NAT, or runs in a pod whose routable address
     * the platform assigns.
     *
     * @param transport         the transport
     * @param bindAddress       the transport-specific bind address
     * @param advertisedAddress the address carried in this node's self-advertisement
     * @throws IOException if the address cannot be bound
     */
    public synchronized void listen(Transport transport, String bindAddress, String advertisedAddress)
            throws IOException {
        listen(transport, bindAddress, advertisedAddress, advertisedEndpoints.size());
    }

    /**
     * Registers a transport, starts listening, and advertises an explicit
     * address at an explicit priority.
     *
     * @param transport         the transport
     * @param bindAddress       the transport-specific bind address
     * @param advertisedAddress the address carried in this node's self-advertisement
     * @param priority          the advertised priority; lower dials first
     * @throws IOException if the address cannot be bound
     */
    public synchronized void listen(Transport transport, String bindAddress, String advertisedAddress,
                                    int priority) throws IOException {
        Objects.requireNonNull(advertisedAddress, "advertisedAddress");
        requireAttestingIfRequired(transport);
        transports.put(transport.scheme(), transport);
        listeners.add(transport.listen(bindAddress, accepted -> {
            connectionSchemes.put(accepted, transport.scheme());
            attach(accepted);
        }));
        advertisedEndpoints.add(new PeerAdvertisement.Endpoint(
                transport.scheme(), advertisedAddress, priority));
    }

    /** The topology roles this node declares ({@code RENDEZVOUS}, {@code RELAY}). */
    public Set<PeerAdvertisement.PeerRole> roles() {
        return roles;
    }

    /**
     * The wire hints this node advertises ({@code wire.batch}, {@code wire.tree},
     * {@code wire.session}, {@code wire.hashes}), a copy.
     *
     * @return the hints
     */
    public Map<String, String> wireHints() {
        return Map.copyOf(nodeHints);
    }

    /**
     * The endpoints this node advertises, in the order they were registered:
     * the advertised (not the bind) address of each listening transport.
     *
     * @return the endpoints; empty for a dial-only node
     */
    public List<PeerAdvertisement.Endpoint> advertisedEndpoints() {
        return List.copyOf(advertisedEndpoints);
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
    /**
     * Closes a resource when this node closes, after its listeners and
     * connections: for a transport a host built for the node (a DTLS or QUIC
     * binding with sockets and threads of its own) rather than one the
     * application supplied and owns.
     *
     * @param resource the resource
     */
    public void closeWith(AutoCloseable resource) {
        ownedResources.add(Objects.requireNonNull(resource, "resource"));
    }

    public synchronized void transport(Transport transport) {
        requireAttestingIfRequired(transport);
        transports.put(transport.scheme(), transport);
    }

    /**
     * The capabilities of the binding registered for a scheme (spec §5.5),
     * for hosts' startup checks and the console.
     *
     * @param scheme the transport scheme
     * @return the registered binding's capabilities, or empty when none is registered
     */
    public Set<Transport.Capability> transportCapabilities(String scheme) {
        Transport transport = transports.get(scheme);
        return transport == null ? Set.of() : transport.capabilities();
    }

    /**
     * The binding registered for a scheme, when any: a host's capability wiring reaches the
     * {@code webrtc} binding this way to attach its signalling pipe to a group (SRAD
     * srad/agentspaces-js.md JS-N-02), without this layer knowing that binding's type.
     *
     * @param scheme the transport scheme
     * @return the registered binding, or empty when none is
     */
    public Optional<Transport> registeredTransport(String scheme) {
        return Optional.ofNullable(transports.get(scheme));
    }

    /**
     * Spec §5.6, gate-2 review G2-1: a node that requires attestation
     * registers attesting bindings alone. A binding that cannot attest would
     * carry only frames the node then drops, and dialing over it would evict
     * an honest peer at the far end.
     */
    private void requireAttestingIfRequired(Transport transport) {
        if (requireAttestation && !transport.capabilities().contains(Transport.Capability.ATTESTING)) {
            throw new IllegalArgumentException("transport '" + transport.scheme()
                    + "' cannot attest, and this node requires attestation (spec §5.6)");
        }
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
     * Joins a locally configured, literal-id group, finding its first contacts
     * through a locator rather than a fixed list: the locator is asked now and
     * again whenever this node's view of the group is empty and the last hint
     * has aged past its time to live.
     *
     * @param groupAd          the group's configured advertisement
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @return the group runtime
     */
    public GroupRuntime joinGroup(GroupAdvertisement groupAd,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator) {
        return joinGroup(groupAd, membershipConfig, locator, Map.of(), null);
    }

    /**
     * Joins a literal-id group through a locator, presenting membership hints
     * and a POLICY validator.
     *
     * @param groupAd          the group's configured advertisement
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @return the group runtime
     */
    public GroupRuntime joinGroup(GroupAdvertisement groupAd,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator) {
        return joinGroup(groupAd, membershipConfig, locator, membershipHints, validator, null);
    }

    /**
     * Joins a literal-id group through a locator, asked by the configured
     * group name (SRAD DB-N-01, FR-9 of ISSUE-BootstrapCore): the name a host
     * configured the group under, which is what a locator's
     * {@code appliesTo} and a beacon document's group entry go by; the
     * advertisement's own name is used when {@code groupName} is null.
     *
     * @param groupAd          the group's configured advertisement
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @param groupName        the configured group name, or null for the advertisement's
     * @return the group runtime
     */
    public GroupRuntime joinGroup(GroupAdvertisement groupAd,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator,
                                  String groupName) {
        Objects.requireNonNull(groupAd, "groupAd");
        Objects.requireNonNull(locator, "locator");
        String name = nameOf(groupAd, groupName);
        BeaconHint hint = locate(groupAd.group(), name, locator);
        return join(groupAd, null, membershipConfig, locator, hint, membershipHints, validator, name);
    }

    /** The configured name when given, else the advertisement's, else the id. */
    private static String nameOf(GroupAdvertisement groupAd, String groupName) {
        if (groupName != null && !groupName.isBlank()) {
            return groupName;
        }
        return groupAd.name() == null ? groupAd.group().value() : groupAd.name();
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
     * Joins a self-certifying group with its signed founding advertisement,
     * finding its first contacts through a locator.
     *
     * @param founding         the signed founding advertisement
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @return the group runtime
     * @throws IllegalArgumentException when the founding document does not verify
     */
    public GroupRuntime joinGroup(SignedGroupAdvertisement founding,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator) {
        return joinGroup(founding, membershipConfig, locator, Map.of(), null);
    }

    /**
     * Joins a self-certifying group through a locator, presenting membership
     * hints and a POLICY validator.
     *
     * @param founding         the signed founding advertisement
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @return the group runtime
     * @throws IllegalArgumentException when the founding document does not verify
     */
    public GroupRuntime joinGroup(SignedGroupAdvertisement founding,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator) {
        Objects.requireNonNull(founding, "founding");
        Objects.requireNonNull(locator, "locator");
        if (!GroupFounding.verify(founding)) {
            throw new IllegalArgumentException("refusing to join " + founding.advertisement().group()
                    + ": the advertisement is not the self-certifying founding document of that group");
        }
        return joinGroup(founding, membershipConfig, locator, membershipHints, validator, null);
    }

    /**
     * Joins a self-certifying group through a locator, asked by the
     * configured group name.
     *
     * @param founding         the signed founding advertisement
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @param groupName        the configured group name, or null for the advertisement's
     * @return the group runtime
     * @throws IllegalArgumentException when the founding document does not verify
     */
    public GroupRuntime joinGroup(SignedGroupAdvertisement founding,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator,
                                  String groupName) {
        Objects.requireNonNull(founding, "founding");
        Objects.requireNonNull(locator, "locator");
        if (!GroupFounding.verify(founding)) {
            throw new IllegalArgumentException("refusing to join " + founding.advertisement().group()
                    + ": the advertisement is not the self-certifying founding document of that group");
        }
        GroupAdvertisement ad = founding.advertisement();
        String name = nameOf(ad, groupName);
        BeaconHint hint = locate(ad.group(), name, locator);
        return join(ad, founding, membershipConfig, locator, hint, membershipHints, validator, name);
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
        SeedsLocator locator = SeedsLocator.fixed(seeds);
        return join(founding.advertisement(), founding, membershipConfig, locator,
                locator.locate(request(founding.advertisement().group(), founding.advertisement().name())),
                membershipHints, validator, null);
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
        Objects.requireNonNull(seeds, "seeds");
        return joinGroup(groupId, membershipConfig, SeedsLocator.fixed(seeds), timeout);
    }

    /**
     * Joins a group knowing only its GroupID, finding seeds through a locator.
     * When the locator's hint already carries a founding advertisement that
     * verifies as the self-certifying document of exactly this GroupID (a
     * beacon document fetched over HTTPS, say), the fetch is skipped and the
     * join proceeds at once; otherwise the hint's endpoints are asked for it
     * as {@link #joinGroup(GroupId, GroupMembership.Config, List, Duration)}
     * asks seeds.
     *
     * @param groupId          the GroupID
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param timeout          how long to wait for a verified founding advertisement
     * @return the group runtime
     * @throws TimeoutException      when no seed served a verified founding advertisement within the timeout
     * @throws IllegalStateException when the group is already joined or a fetch for it is already in progress
     */
    public GroupRuntime joinGroup(GroupId groupId,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Duration timeout) throws TimeoutException {
        return joinGroup(groupId, membershipConfig, locator, timeout, Map.of(), null);
    }

    /**
     * Joins a group knowing only its GroupID, finding seeds through a locator
     * and presenting membership hints (a join credential) and a POLICY
     * validator once the founding advertisement is in hand.
     *
     * @param groupId          the GroupID
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param timeout          how long to wait for a verified founding advertisement
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @return the group runtime
     * @throws TimeoutException      when no seed served a verified founding advertisement within the timeout
     * @throws IllegalStateException when the group is already joined or a fetch for it is already in progress
     */
    public GroupRuntime joinGroup(GroupId groupId,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Duration timeout,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator) throws TimeoutException {
        return joinGroup(groupId, membershipConfig, locator, timeout, membershipHints, validator, null);
    }

    /**
     * Joins a group knowing only its GroupID, through a locator asked by the
     * configured group name.
     *
     * @param groupId          the GroupID
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param timeout          how long to wait for a verified founding advertisement
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @param groupName        the configured group name, or null for the id
     * @return the group runtime
     * @throws TimeoutException      when no seed served a verified founding advertisement within the timeout
     * @throws IllegalStateException when the group is already joined or a fetch for it is already in progress
     */
    public GroupRuntime joinGroup(GroupId groupId,
                                  GroupMembership.Config membershipConfig,
                                  BeaconLocator locator,
                                  Duration timeout,
                                  Map<String, String> membershipHints,
                                  MembershipValidator validator,
                                  String groupName) throws TimeoutException {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(locator, "locator");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(membershipHints, "membershipHints");
        String name = groupName == null || groupName.isBlank() ? groupId.value() : groupName;
        BeaconHint hint = locate(groupId, name, locator);
        if (hint.founding().isPresent()) {
            SignedGroupAdvertisement carried = decodeFounding(hint.founding().get());
            if (carried != null && GroupFounding.verify(carried)
                    && carried.advertisement().group().equals(groupId)) {
                return join(carried.advertisement(), carried, membershipConfig, locator, hint,
                        membershipHints, validator, name);
            }
            LOG.log(System.Logger.Level.WARNING, "the locator's founding advertisement for " + groupId
                    + " did not verify as that group's; asking the seeds instead");
        }
        List<PeerAdvertisement.Endpoint> seeds = hint.endpoints();
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
            return join(founding.advertisement(), founding, membershipConfig, locator, hint,
                    membershipHints, validator, name);
        } finally {
            pendingFounding.remove(groupId, pending);
            asked.forEach(TransportConnection::close);
        }
    }

    /** Decodes a hint's founding bytes, or null when they are not a signed founding advertisement. */
    private SignedGroupAdvertisement decodeFounding(byte[] bytes) {
        try {
            return codec.fromBytes(bytes, SignedGroupAdvertisement.class);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** The request a locator is asked with: what this node knows and can dial. */
    private BeaconRequest request(GroupId groupId, String groupName) {
        Set<String> schemes = Set.copyOf(transports.keySet());
        String defaultScheme = schemes.contains("tls") ? "tls"
                : schemes.contains("tcp") ? "tcp"
                : advertisedEndpoints.isEmpty() ? (schemes.isEmpty() ? "tcp" : schemes.iterator().next())
                : advertisedEndpoints.get(0).transport();
        return new BeaconRequest(groupName == null ? groupId.value() : groupName,
                Optional.ofNullable(groupId), defaultScheme, schemes);
    }

    /** Asks a locator at join: the hint, its expiry, the trace; a required locator that answers nothing refuses. */
    private BeaconHint locate(GroupId groupId, String groupName, BeaconLocator locator) {
        BeaconHint hint = locate(groupId, groupName, locator, true);
        if (locator.policy().require() && hint.isEmpty()) {
            throw new IllegalStateException("no locator answered for " + groupName + " (" + groupId.value()
                    + ") and bootstrap.require is set; " + traceText(groupId, hint));
        }
        return hint;
    }

    /**
     * Asks a locator now, recording the hint, its expiry under the locator's
     * policy or the node's pacing, and the chain's trace (SRAD DB-N-07): the
     * first answer is logged at INFO, a re-ask at INFO when its endpoints
     * changed, every row at INFO under {@code trace}, otherwise at DEBUG. A
     * locator that throws yields nothing. A hint's {@code wire.*} parameters
     * seed the channel expectations of the beacon it names (DB-N-08).
     */
    private BeaconHint locate(GroupId groupId, String groupName, BeaconLocator locator, boolean joining) {
        long now = clock.millis();
        lastLocateMillis.put(groupId, now);
        BeaconHint hint;
        try {
            hint = locator.locate(request(groupId, groupName));
            if (hint == null) {
                hint = BeaconHint.none();
            }
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "locator " + locator.name() + " failed for " + groupId
                    + ": " + e);
            hint = BeaconHint.none();
        }
        List<PeerAdvertisement.Endpoint> previous = groupSeeds.get(groupId);
        lastBeaconHints.put(groupId, hint);
        if (locator instanceof BootstrapChain chain) {
            lastBootstrapTraces.put(groupId, chain.lastTrace());
        } else {
            lastBootstrapTraces.remove(groupId);
        }
        LocatePolicy policy = locator.policy();
        long min = policy.minInterval().map(Duration::toMillis).orElse(locateMinMillis);
        long max = policy.maxInterval().map(Duration::toMillis).orElse(locateMaxMillis);
        long ttl = hint.ttl().isZero() ? min : hint.ttl().toMillis();
        hintExpiryMillis.put(groupId, now + Math.max(min, Math.min(ttl, Math.max(min, max))));
        boolean changed = previous == null || !previous.equals(hint.endpoints());
        System.Logger.Level level = joining || changed || policy.trace() || hint.isEmpty()
                ? System.Logger.Level.INFO : System.Logger.Level.DEBUG;
        LOG.log(level, "bootstrap " + groupName + (joining ? " (join): " : " (re-ask): ") + traceText(groupId, hint));
        seedChannelHints(hint);
        return hint;
    }

    /** One line: the hint's evidence and endpoints, then the chain's rows when there are any. */
    private String traceText(GroupId groupId, BeaconHint hint) {
        StringBuilder out = new StringBuilder(hint.evidence()).append(' ').append(hint.endpoints());
        for (BootstrapChain.Trace row : lastBootstrapTraces.getOrDefault(groupId, List.of())) {
            out.append("; ").append(row.locator()).append(' ').append(row.millis()).append(" ms ")
                    .append(row.endpoints()).append(" endpoint(s)");
            if (row.error() != null) {
                out.append(" error: ").append(row.error());
            }
        }
        return out.toString();
    }

    /**
     * A beacon document names its beacon ({@code beacon}) and the wire hints
     * it advertises ({@code wire.*}); taking them as expectations before the
     * beacon's advertisement arrives saves the first session hello a retry.
     * The advertisement, when it arrives, is authoritative.
     */
    private void seedChannelHints(BeaconHint hint) {
        String beacon = hint.parameters().get("beacon");
        if (beacon == null || beacon.isBlank()) {
            return;
        }
        PeerId peer;
        try {
            peer = PeerId.of(beacon.trim());
        } catch (RuntimeException e) {
            return;
        }
        if (channelAuth == ChannelAuth.SESSION && "1".equals(hint.parameters().get(SESSION_HINT))) {
            sessionPeers.add(peer);
        }
        if (lazyPush && "1".equals(hint.parameters().get(TREE_HINT))) {
            treePeers.add(peer);
        }
        if ("1".equals(hint.parameters().get(BATCH_HINT))) {
            batchPeers.add(peer);
        }
        if ("1".equals(hint.parameters().get(HASHES_HINT))) {
            hashPeers.add(peer);
        }
    }

    /** Whether this node expects to open a session with the peer (a hint seeded it, or its advertisement said so). */
    boolean expectsSession(PeerId peer) {
        return sessionPeers.contains(peer);
    }

    /**
     * The trace of a group's last locate: one row per locator the chain
     * asked, with its duration, endpoint count, evidence, and error; empty
     * when the group's locator is not a chain or nothing was asked yet.
     *
     * @param groupId the group
     * @return the rows
     */
    public List<BootstrapChain.Trace> lastBootstrapTrace(GroupId groupId) {
        return lastBootstrapTraces.getOrDefault(groupId, List.of());
    }

    /**
     * What the multicast beacon heard for a group: the unexpired endpoints
     * of every sender whose datagram verified, newest sender first.
     *
     * @param groupId the group
     * @return the endpoints, how many senders, and the shortest remaining life
     */
    public BeaconLearned beaconLearned(GroupId groupId) {
        Map<PeerId, LearnedEndpoints> learned = beaconLearned.get(groupId);
        if (learned == null || learned.isEmpty()) {
            return new BeaconLearned(List.of(), 0, Duration.ZERO);
        }
        long now = clock.millis();
        List<PeerAdvertisement.Endpoint> out = new ArrayList<>();
        long shortest = Long.MAX_VALUE;
        int peers = 0;
        for (Map.Entry<PeerId, LearnedEndpoints> entry : learned.entrySet()) {
            if (entry.getValue().expiresMillis() <= now) {
                learned.remove(entry.getKey());
                continue;
            }
            peers++;
            shortest = Math.min(shortest, entry.getValue().expiresMillis() - now);
            for (PeerAdvertisement.Endpoint endpoint : byPriority(entry.getValue().endpoints())) {
                out.add(new PeerAdvertisement.Endpoint(endpoint.transport(), endpoint.address(), out.size()));
            }
        }
        return new BeaconLearned(out, peers, peers == 0 ? Duration.ZERO : Duration.ofMillis(shortest));
    }

    /**
     * The last hint a group's locator answered, with its evidence; empty before
     * the first locate or for a group joined with a fixed seed list (whose
     * hint is the list).
     *
     * @param groupId the group
     * @return the hint
     */
    public Optional<BeaconHint> lastBeaconHint(GroupId groupId) {
        return Optional.ofNullable(lastBeaconHints.get(groupId));
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
        Objects.requireNonNull(groupAd, "groupAd");
        Objects.requireNonNull(seeds, "seeds");
        SeedsLocator locator = SeedsLocator.fixed(seeds);
        return join(groupAd, null, membershipConfig, locator,
                locator.locate(request(groupAd.group(), groupAd.name())), membershipHints, validator, null);
    }

    /** The common join: wires the runtime and introduces this node to the hint's endpoints. */
    private GroupRuntime join(GroupAdvertisement groupAd,
                              SignedGroupAdvertisement founding,
                              GroupMembership.Config membershipConfig,
                              BeaconLocator locator,
                              BeaconHint hint,
                              Map<String, String> membershipHints,
                              MembershipValidator validator,
                              String groupName) {
        Objects.requireNonNull(groupAd, "groupAd");
        Objects.requireNonNull(membershipHints, "membershipHints");
        GroupId groupId = groupAd.group();
        List<PeerAdvertisement.Endpoint> seeds = hint.endpoints();
        groupLocators.put(groupId, locator);
        groupNames.put(groupId, nameOf(groupAd, groupName));
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
            public boolean trySendRumor(PeerId to, Bodies.Rumor rumor) {
                return send(groupId, to, Envelope.Kind.RUMOR, rumor);
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digest) {
                send(groupId, to, Envelope.Kind.DIGEST, digest);
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp resp) {
                send(groupId, to, Envelope.Kind.PULL_RESP, resp);
            }

            @Override
            public void sendHave(PeerId to, Bodies.Have have) {
                send(groupId, to, Envelope.Kind.HAVE, have);
            }

            @Override
            public void sendGraft(PeerId to, Bodies.Graft graft) {
                send(groupId, to, Envelope.Kind.GRAFT, graft);
            }

            @Override
            public void sendPrune(PeerId to, Bodies.Prune prune) {
                send(groupId, to, Envelope.Kind.PRUNE, prune);
            }
        };
        GossipBus gossip = new GossipBus(membership, sender, codec,
                groupAd.gossip().fanout(), GossipBus.DEFAULT_HOPS);
        if (lazyPush) {
            gossip.lazyPush(treePeers::contains);
            gossip.directDelivery(PEER_AD_STREAM); // introduced directly and flooded: never a tree duplicate
            gossip.alwaysEager(PEER_AD_STREAM);
            gossip.alwaysEager(REVOCATION_STREAM);
            gossip.alwaysEager(CREDENTIAL_REVOCATION_STREAM);
        }
        gossip.hashedDigests(hashPeers::contains);
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
        gossip.plainReach(this::connectedTo, peer -> runtime.membership().member(peer)
                .map(m -> m.endpoints().isEmpty()).orElse(false));
        // The view converges by anti-entropy, not by flood luck (ROBUSTNESS.md §6b): two members
        // whose introductions reached each other's random fan-out never would otherwise stay
        // strangers for good once nothing re-floods them.
        gossip.reconcile(VIEW_STATE, new ViewState(runtime));
        // ASF-012: this node forwards only peer ads it can verify end-to-end,
        // so its identity and rate budget never amplify unverifiable content.
        gossip.forwardFilter(PEER_AD_STREAM, payload -> forwardsPeerAd(revocations, payload));
        // The members admitted under a delegation before a restart, back before the first contact.
        admissionMemory.load(groupId);
        if (groupAd.membershipPolicy() == GroupAdvertisement.MembershipPolicy.INVITE
                && !admissionMemory.persistent()) {
            LOG.log(System.Logger.Level.INFO, "the admission memory of group " + groupId.value()
                    + " is held in memory only: no state directory is configured, so a restart forgets which"
                    + " members were admitted under an invite delegation before it expired, and refuses them"
                    + " after it (PeerNode.Builder.stateDirectory; the starter passes agentspaces.keystore)");
        }
        groups.put(groupId, runtime);

        // Introduce ourselves to the seeds directly; gossip takes it from there.
        groupSeeds.put(groupId, List.copyOf(seeds));
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
            if (transport == null || transport.signalled()) {
                continue; // a signalled endpoint names no address to introduce ourselves at
            }
            try {
                TransportConnection connection = transport.dial(seed.address());
                attach(connection);
                maybeAnnounceChannel(groupId, connection, null); // the seed's identity is not known yet
                sendOn(connection, groupId, null, Envelope.Kind.RUMOR, selfAdRumor(groupId));
            } catch (IOException e) {
                // Seed unreachable; others may work, and gossip is redundant.
            }
        }
    }

    /**
     * Two members that dropped each other (a partition longer than the membership TTL, a stale
     * backlog, a one-way loss) have no third party to reintroduce them and nobody dials anybody:
     * they would stay apart for good. So, every few seconds for a while after a member's loss,
     * this node introduces itself to the member's last endpoints as it would to a seed, and when
     * its view is empty it dials its seeds again; a member that is back answers with its own
     * introduction and both re-admit. A revoked peer is never retried.
     */
    private void retryLost(GroupRuntime runtime, long nowMillis) {
        GroupId groupId = runtime.id();
        Long last = lastLostRetryMillis.get(groupId);
        long interval = lostRetryIntervalMillis.getOrDefault(groupId, LOST_RETRY_INTERVAL_MILLIS);
        if (last != null && nowMillis - last < interval) {
            return;
        }
        Map<PeerId, Long> lost = lostMembers.get(groupId);
        boolean tried = false;
        if (lost != null) {
            for (Map.Entry<PeerId, Long> entry : List.copyOf(lost.entrySet())) {
                PeerId peer = entry.getKey();
                if (nowMillis - entry.getValue() > LOST_RETRY_WINDOW_MILLIS || runtime.revocations().revoked(peer)
                        || runtime.membership().member(peer).isPresent()) {
                    lost.remove(peer);
                    continue;
                }
                List<PeerAdvertisement.Endpoint> endpoints = lastEndpoints.get(peer);
                if (endpoints != null && !endpoints.isEmpty()) {
                    tried = true;
                    introduceTo(groupId, endpoints);
                }
            }
        }
        if (runtime.membership().allMembers().isEmpty()) {
            List<PeerAdvertisement.Endpoint> seeds = groupSeeds.get(groupId);
            if (seeds != null && !seeds.isEmpty()) {
                tried = true;
                introduceTo(groupId, seeds);
            }
            relocateIfStale(groupId, nowMillis);
        }
        if (tried) {
            lastLostRetryMillis.put(groupId, nowMillis);
            lostRetryIntervalMillis.put(groupId, Math.min(interval * 2, LOST_RETRY_MAX_INTERVAL_MILLIS));
        }
    }

    /**
     * Re-asks a group's locator, off the tick thread, when the view is empty
     * and the last hint has aged past its time to live: a seed that moved (a
     * rescheduled pod, a changed SRV record) is found again without a restart.
     * Never faster than {@link #LOCATE_MIN_INTERVAL_MILLIS}; the dial back-off
     * of {@code retryLost} is untouched, and a stable view asks nothing.
     */
    private void relocateIfStale(GroupId groupId, long nowMillis) {
        BeaconLocator locator = groupLocators.get(groupId);
        if (locator == null || locator instanceof SeedsLocator) {
            return; // a fixed list never changes
        }
        Long expiry = hintExpiryMillis.get(groupId);
        Long last = lastLocateMillis.get(groupId);
        long min = locator.policy().minInterval().map(Duration::toMillis).orElse(locateMinMillis);
        if ((expiry != null && nowMillis < expiry)
                || (last != null && nowMillis - last < min)
                || !locating.add(groupId)) {
            return;
        }
        lastLocateMillis.put(groupId, nowMillis);
        String name = groupNames.getOrDefault(groupId, groupId.value());
        Thread.ofVirtual().name("peer-locate-" + groupId.value()).start(() -> {
            try {
                BeaconHint hint = locate(groupId, name, locator, false);
                if (!hint.endpoints().isEmpty() && groups.containsKey(groupId)) {
                    groupSeeds.put(groupId, hint.endpoints());
                    introduceTo(groupId, hint.endpoints());
                }
            } finally {
                locating.remove(groupId);
            }
        });
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
     * Leaves one group (ISSUE-PeerRobustness-2, R2-2): everything this node
     * holds for it goes, and nothing held for another group changes. The group
     * leaves the node's tables first, so a frame of the group arriving from
     * now on takes the stranger path and the node sends nothing more in it;
     * then the locate, the lost-member retry, the seeds, the hints, the beacon
     * seal, and the relay counters of the group are dropped, the runtime is
     * released (its periodic work and kind handlers cleared, its
     * {@link GroupRuntime#onClose} listeners told), the admission memory of the
     * group and its file go, and the connections to the members of the group's
     * last view that are members of no remaining group are closed. Other
     * members notice the departure by the membership TTL; no frame announces
     * it. Rejoining starts the group afresh, with an empty admission memory.
     *
     * @param groupId the group
     * @return whether this node was a member
     */
    public synchronized boolean leaveGroup(GroupId groupId) {
        Objects.requireNonNull(groupId, "groupId");
        GroupRuntime runtime = groups.remove(groupId);
        if (runtime == null) {
            return false;
        }
        // The members this node held in the group, from the live view and the last tick's view.
        Set<PeerId> held = new java.util.HashSet<>();
        for (GroupMembership.Member member : runtime.membership().allMembers()) {
            held.add(member.id());
        }
        Set<PeerId> previous = lastView.remove(groupId);
        if (previous != null) {
            held.addAll(previous);
        }
        groupSeeds.remove(groupId);
        groupLocators.remove(groupId);
        groupNames.remove(groupId);
        lastBeaconHints.remove(groupId);
        lastBootstrapTraces.remove(groupId);
        beaconLearned.remove(groupId);
        hintExpiryMillis.remove(groupId);
        lastLocateMillis.remove(groupId);
        locating.remove(groupId); // a locate in flight checks the group is still joined before it acts
        pendingFounding.remove(groupId); // never present for a joined group; cleared for completeness
        lostMembers.remove(groupId);
        lastLostRetryMillis.remove(groupId);
        lostRetryIntervalMillis.remove(groupId);
        heldAds.remove(groupId);
        newlyAdmitted.remove(groupId);
        lastSelfAdMillis.remove(groupId);
        selfAdSentTo.remove(groupId);
        lastSelfAdFingerprint.remove(groupId);
        groupHints.remove(groupId);
        groupValidators.remove(groupId);
        beaconSeals.remove(groupId);
        relayCounters.remove(groupId);
        relayedWith.remove(groupId);
        offloadAttempted.remove(groupId);
        readmittedLogged.removeIf(key -> key.startsWith(groupId.value() + '|'));
        // Frames coalesced for the group and not yet flushed are discarded, never sent.
        for (Map.Entry<PeerId, Outbox> entry : List.copyOf(outboxes.entrySet())) {
            Outbox outbox = entry.getValue();
            if (groupId.equals(outbox.group)) {
                synchronized (outbox) {
                    outbox.frames.clear();
                    outbox.bytes = 0;
                }
                outboxes.remove(entry.getKey(), outbox);
            }
        }
        runtime.release();
        admissionMemory.drop(groupId);
        // The links are the node's: keep one to a peer still a member of another group joined here.
        int closed = 0;
        for (PeerId peer : held) {
            boolean sharedElsewhere = groups.values().stream()
                    .anyMatch(g -> g.membership().member(peer).isPresent());
            if (sharedElsewhere) {
                continue;
            }
            TransportConnection connection = connections.get(peer);
            if (connection != null) {
                dropConnection(peer, connection);
                closed++;
            }
            lastEndpoints.remove(peer);
            lastHints.remove(peer);
            probePulls.remove(peer);
            batchPeers.remove(peer);
            treePeers.remove(peer);
            sessionPeers.remove(peer);
            hashPeers.remove(peer);
        }
        LOG.log(System.Logger.Level.INFO, "left group " + groupId.value() + ": " + held.size()
                + " member(s) forgotten, " + closed + " connection(s) closed");
        return true;
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
            publishSelfAd(runtime, gossip, nowMillis);
            boolean active = gossip.consumeActivity();
            runtime.membership().tick(new GroupMembership.Prober() {
                @Override
                public void ping(PeerId target, long nonce) {
                    // The probe carries the anti-entropy digests and the recent items (v0.1.17).
                    Map<String, byte[]> hashes = gossip.probeHashes();
                    send(runtime.id(), target, Envelope.Kind.PING, new Bodies.Ping(nonce,
                            hashes == null ? gossip.rootDigest() : null, hashes, gossip.piggyback(target)));
                }

                @Override
                public void pingReq(PeerId relay, PeerId target, long nonce) {
                    send(runtime.id(), relay, Envelope.Kind.PING_REQ,
                            new Bodies.PingReq(target, nonce));
                }
            }, !active);
            // A member that left the view since the last tick (the failure detector dropped it, or
            // an eviction did) is outside the data plane: only its own introduction re-admits it,
            // and it sends one on the channel it dials next (see connectionTo), so the stale
            // channel is closed here to make that happen.
            Set<PeerId> view = new java.util.HashSet<>();
            for (GroupMembership.Member member : runtime.membership().allMembers()) {
                view.add(member.id());
            }
            Set<PeerId> previous = lastView.put(runtime.id(), view);
            if (previous != null) {
                for (PeerId dropped : previous) {
                    if (!view.contains(dropped)) {
                        gossip.memberLeft(dropped);
                        probePulls.remove(dropped);
                        boolean ejected = runtime.membership().consumeEjected(dropped);
                        if (!ejected && !runtime.revocations().revoked(dropped)) {
                            lostMembers.computeIfAbsent(runtime.id(), g -> new ConcurrentHashMap<>()).put(dropped, nowMillis);
                        }
                        heldAds.getOrDefault(runtime.id(), Map.of()).remove(dropped);
                        TransportConnection stale = connections.get(dropped);
                        if (stale != null) {
                            dropConnection(dropped, stale);
                            stale.close();
                        }
                    }
                }
            }
            retryLost(runtime, nowMillis);
            gossip.tick(nowMillis); // lazy push: grafts and announcements (PERF2 §3.4)
            PeerId periodicPartner = null;
            if (runtime.claimAntiEntropyRound(nowMillis, active)) {
                periodicPartner = gossip.antiEntropyTick().orElse(null);
            }
            // A member admitted since the last tick is reconciled with now, after this node's own
            // introduction went out above, so it holds this node and answers: the newcomer pulls
            // the cards and space state the fleet already has instead of waiting for the random
            // periodic partner to be one that holds them. The periodic round's partner is not
            // asked twice.
            Set<PeerId> admitted = newlyAdmitted.remove(runtime.id());
            if (admitted != null) {
                for (PeerId member : admitted) {
                    if (!member.equals(periodicPartner) && runtime.membership().member(member).isPresent()) {
                        gossip.antiEntropyWith(member);
                    }
                }
            }
            // Layers above peering join this same clock through
            // GroupRuntime.onTick (QA3 A3-1): capability protocols advance here,
            // for the same reason membership and anti-entropy just did.
            offloadRelayed(runtime, nowMillis); // JS-W-11, off this thread for the dial itself
            runtime.runTickWork(e -> LOG.log(System.Logger.Level.WARNING,
                    "tick work failed in group " + runtime.id().value(), e));
            admissionMemory.flushIfDue(runtime.id(), nowMillis,
                    runtime.advertisement().gossip().period().toMillis());
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
     * The self-advertisement's cadence: a flooded refresh every {@code peerAdInterval}, and between
     * refreshes a direct, non-forwarded copy to each member this node has not introduced itself to
     * since the last refresh (a newcomer, or a member that left and came back). Liveness is the
     * membership protocol's job; the advertisement only has to stay fresher than its TTL and reach
     * every member once per version.
     */
    /**
     * How often this node's advertisement is refreshed to each member of a group. The configured
     * interval, unless the group's probe cycle (one member per period, stretched up to
     * {@link GroupMembership#MAX_QUIET_STRETCH} times when quiet) has outgrown the member TTL: a
     * member is kept by hearing from it, and past that size the probes cannot reach every member
     * inside the TTL, so the refresh takes over as the keepalive at half the TTL (ROBUSTNESS.md
     * §6b, scale). Small groups keep their cadence unchanged.
     */
    long refreshIntervalMillis(GroupRuntime runtime, int members) {
        GroupMembership.Config config = runtime.membership().config();
        long probeCycle = (long) (members + 1) * config.probeInterval().toMillis() * (1L << GroupMembership.MAX_QUIET_STRETCH);
        long ttl = config.memberTtl().toMillis();
        if (probeCycle < ttl) {
            return peerAdInterval.toMillis();
        }
        return Math.min(peerAdInterval.toMillis(), ttl / 2);
    }

    private void publishSelfAd(GroupRuntime runtime, GossipBus gossip, long nowMillis) {
        GroupId groupId = runtime.id();
        Set<PeerId> sentTo = selfAdSentTo.computeIfAbsent(groupId, g -> ConcurrentHashMap.newKeySet());
        List<GroupMembership.Member> members = runtime.membership().allMembers();
        Set<PeerId> current = new java.util.HashSet<>();
        for (GroupMembership.Member member : members) {
            current.add(member.id());
        }
        sentTo.retainAll(current);
        Long last = lastSelfAdMillis.get(groupId);
        int fingerprint = Objects.hash(List.copyOf(advertisedEndpoints), roles, selfHints(groupId));
        boolean changed = !Integer.valueOf(fingerprint).equals(lastSelfAdFingerprint.get(groupId));
        if (last == null || changed) {
            // Flooded: the content is new and has to reach every member, through a third where a
            // direct link is cut. A view change no longer floods (ROBUSTNESS.md §6b, scale): a
            // newcomer gets this node's advertisement directly below, a lost member by the
            // lost-member retry, and at fifty members one expiry made forty-nine floods.
            // Mark before publishing: a member whose send fails is dropped from the set by
            // dropConnection during the publish, and is introduced to again on the next tick.
            sentTo.addAll(current);
            lastSelfAdMillis.put(groupId, nowMillis);
            lastSelfAdFingerprint.put(groupId, fingerprint);
            Bodies.Rumor selfAd = selfAdRumor(groupId);
            gossip.publish(selfAd.streamId(), selfAd.itemId(), selfAd.payload());
            return;
        }
        if (nowMillis - last >= refreshIntervalMillis(runtime, members.size())) {
            // The interval refresh goes to each member directly and unforwarded (v0.1.17): the
            // flood cost every member several copies when nothing else had pruned the tree, and
            // a refresh has to reach each member exactly once anyway.
            sentTo.addAll(current);
            lastSelfAdMillis.put(groupId, nowMillis);
            Bodies.Rumor selfAd = selfAdRumor(groupId);
            Bodies.Rumor refresh = new Bodies.Rumor(selfAd.streamId(), selfAd.itemId(), 1, selfAd.payload());
            for (PeerId member : current) {
                if (!member.equals(peerId()) && !send(groupId, member, Envelope.Kind.RUMOR, refresh)) {
                    gossip.publish(selfAd.streamId(), selfAd.itemId(), selfAd.payload()); // unreachable: flood
                    return;
                }
            }
            return;
        }
        Bodies.Rumor direct = null;
        for (PeerId member : current) {
            if (member.equals(peerId()) || sentTo.contains(member)) {
                continue;
            }
            if (direct == null) {
                Bodies.Rumor selfAd = selfAdRumor(groupId);
                direct = new Bodies.Rumor(selfAd.streamId(), selfAd.itemId(), 1, selfAd.payload());
            }
            if (send(groupId, member, Envelope.Kind.RUMOR, direct)) {
                sentTo.add(member);
                continue;
            }
            // Unreachable directly (a partition, a dial-only peer behind another member): let
            // the group carry the introduction instead, as the per-tick flood used to.
            Bodies.Rumor flooded = selfAdRumor(groupId);
            gossip.publish(flooded.streamId(), flooded.itemId(), flooded.payload());
            lastSelfAdMillis.put(groupId, nowMillis);
            sentTo.addAll(current);
            return;
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
    boolean send(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        if (!groups.containsKey(groupId)) {
            return false; // a group this node left sends nothing more, even over a link it still holds (R2-2.2)
        }
        if (quarantined(to)) {
            return false; // cautious: a sentenced peer is sent nothing, and no link is opened to it
        }
        // Never coalesce an introduction: a peer that has not admitted this node yet discards a
        // BATCH whole, and the peer advertisement inside it is what would have admitted it.
        boolean introduction = body instanceof Bodies.Rumor rumor && PEER_AD_STREAM.equals(rumor.streamId());
        if (coalesceWindow != null && batchable(kind) && !introduction && batchPeers.contains(to)
                && !to.equals(peerId())) {
            enqueue(groupId, to, kind, body);
            return true;
        }
        return sendNow(groupId, to, kind, body);
    }

    private static boolean batchable(Envelope.Kind kind) {
        return kind == Envelope.Kind.RUMOR || kind == Envelope.Kind.DIGEST
                || kind == Envelope.Kind.PULL_RESP || kind == Envelope.Kind.PIPE_DATA
                || kind == Envelope.Kind.HAVE || kind == Envelope.Kind.GRAFT || kind == Envelope.Kind.PRUNE;
    }

    private void enqueue(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        byte[] bodyBytes = body instanceof byte[] raw ? raw : codec.toBytes(body);
        byte[] inner = codec.toBytes(new Envelope(WIRE_VERSION, groupId, kind, peerId(), to, hlc.now(), bodyBytes));
        Outbox full = null;
        Outbox outbox = outboxes.computeIfAbsent(to, p -> new Outbox(groupId));
        synchronized (outbox) {
            outbox.frames.add(inner);
            outbox.bytes += inner.length;
            if (outbox.frames.size() >= BATCH_MAX_FRAMES || outbox.bytes >= BATCH_MAX_BYTES) {
                full = outbox;
            }
        }
        if (full != null) {
            flush(to, full);
        }
    }

    private void flushOutboxes() {
        for (Map.Entry<PeerId, Outbox> e : outboxes.entrySet()) {
            flush(e.getKey(), e.getValue());
        }
    }

    private void flush(PeerId to, Outbox outbox) {
        List<byte[]> frames;
        GroupId group;
        synchronized (outbox) {
            if (outbox.frames.isEmpty()) {
                return;
            }
            frames = List.copyOf(outbox.frames);
            outbox.frames.clear();
            outbox.bytes = 0;
            group = outbox.group;
        }
        if (frames.size() == 1) {
            // Nothing to coalesce: forward the inner frame as itself, re-signed as a plain frame.
            Envelope inner = codec.fromBytes(frames.get(0), Envelope.class);
            sendNow(group, to, inner.kind(), inner.body());
            return;
        }
        sendNow(group, to, Envelope.Kind.BATCH, new Bodies.Batch(frames));
    }

    private void onBatch(GroupRuntime runtime, Envelope envelope, TransportConnection connection) {
        Bodies.Batch batch;
        try {
            batch = codec.fromBytes(envelope.body(), Bodies.Batch.class);
        } catch (RuntimeException e) {
            strike(envelope.from(), "malformed BATCH body");
            return;
        }
        if (batch == null || batch.frames() == null) {
            return;
        }
        for (byte[] bytes : batch.frames()) {
            Envelope inner;
            try {
                inner = codec.fromBytes(bytes, Envelope.class);
            } catch (RuntimeException e) {
                strike(envelope.from(), "malformed frame in BATCH");
                return;
            }
            // An inner frame inherits the outer signature: it must claim the same sender and
            // group, and may not nest. Each is then judged and dispatched as if it had arrived alone.
            if (inner == null || inner.kind() == Envelope.Kind.BATCH || inner.ver() != WIRE_VERSION
                    || !envelope.from().equals(inner.from()) || !envelope.group().equals(inner.group())) {
                strike(envelope.from(), "BATCH frame from another sender or group");
                return;
            }
            receivedByKind.computeIfAbsent(inner.kind(), k -> new AtomicLong()).incrementAndGet();
            dispatch(inner, connection);
        }
    }

    boolean sendNow(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        TransportConnection connection = connectionTo(groupId, to);
        if (connection == null) {
            return sendViaRelay(groupId, to, kind, body);
        }
        try {
            sendOn(connection, groupId, to, kind, body);
            return true;
        } catch (IOException e) {
            // The channel died under us (the peer closed it: a quarantine, an eviction, a
            // restart). Drop it and try once more over a fresh dial, which also carries this
            // node's introduction, so a request is not lost to a stale channel.
            dropConnection(to, connection);
            TransportConnection fresh = connectionTo(groupId, to);
            if (fresh == null || fresh == connection) {
                return sendViaRelay(groupId, to, kind, body);
            }
            try {
                sendOn(fresh, groupId, to, kind, body);
                return true;
            } catch (IOException again) {
                dropConnection(to, fresh);
                return false;
            }
        }
    }

    /**
     * Wraps the frame in a RELAY_FRAME and hands it to the first reachable
     * RELAY-role member. One relay hop only: the relay dials the target
     * directly, and relay frames themselves are never re-relayed.
     */
    private boolean sendViaRelay(GroupId groupId, PeerId to, Envelope.Kind kind, Object body) {
        if (kind == Envelope.Kind.RELAY_FRAME) {
            return false;
        }
        GroupRuntime runtime = groups.get(groupId);
        if (runtime == null) {
            return false;
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
                noteRelayed(groupId, to); // JS-W-11: a frame the relay carried for this pair
                return true;
            } catch (IOException e) {
                dropConnection(relay, connection);
            }
        }
        return false;
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
        if (flusher != null) {
            flusher.shutdownNow();
            try {
                flushOutboxes();
            } catch (RuntimeException e) {
                // closing anyway
            }
        }
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
        admissionMemory.flushAll(); // an admission inside the last gossip period survives the restart
        connections.values().forEach(TransportConnection::close);
        connections.clear();
        for (AutoCloseable resource : ownedResources) {
            try {
                resource.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
        ownedResources.clear();
        bareAccepted.clear();
        helloSent.clear();
        sessions.clear();
        if (beacon != null) {
            beacon.close();
        }
    }

    // ---------------------------------------------------------------- internals

    private void attach(TransportConnection connection) {
        connection.onReceive(frame -> onFrame(connection, frame));
        connection.onClose(() -> onConnectionClosed(connection));
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
        bytesReceived.addAndGet(frame.length);
        Session session = sessions.get(connection);
        Optional<WireCodec.Decoded> decoded = wire.decodeDetailed(frame, connection.attestedPeer(),
                session == null ? null : session.established());
        decoded.ifPresent(d -> receivedByKind.computeIfAbsent(d.envelope().kind(), k -> new AtomicLong()).incrementAndGet());
        if (decoded.isEmpty()) {
            // Bad signature, unknown wire version, or malformed: drop before
            // any handler sees it (spec §9). No verified sender, so no strike.
            framesDropped.incrementAndGet();
            return;
        }
        if (session != null && decoded.get().auth() == WireCodec.Auth.SESSION) {
            session.confirmed = true; // the other end holds the key: key confirmation (spec §5.6)
        }
        dispatch(decoded.get().envelope(), connection);
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
        if (requireAttestation && !attests(connection, envelope.from())
                && !relayedSignalToSelf(envelope, connection)) {
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
            // A session opens only after a non-introduction frame from a member: probes, digests
            // and forwards go to members alone, so such a frame proves the peer admitted this
            // node too, and its answer to a hello will not be ignored as a stranger's.
            maybeAnnounceChannel(envelope.group(), connection, null);
            if (envelope.kind() != Envelope.Kind.RUMOR && envelope.kind() != Envelope.Kind.SESSION_HELLO) {
                // On the connection this node sends over, which with two links between a pair
                // (each dialed the other) is not the one the frame arrived on.
                TransportConnection outbound = connections.get(envelope.from());
                if (outbound != null) {
                    maybeSessionHello(envelope.group(), outbound, envelope.from());
                }
            }
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
                onProbePiggyback(runtime, envelope.from(), ping.root(), ping.hashes(), ping.have());
                GossipBus gossip = runtime.gossip();
                Map<String, byte[]> hashes = gossip.probeHashes();
                send(runtime.id(), envelope.from(), Envelope.Kind.ACK, new Bodies.Ack(ping.nonce(), null,
                        hashes == null ? gossip.rootDigest() : null, hashes, gossip.piggyback(envelope.from())));
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
                    if (ack.onBehalf() == null) {
                        onProbePiggyback(runtime, envelope.from(), ack.root(), ack.hashes(), ack.have());
                    }
                }
            }
            case RUMOR -> runtime.gossip().onRumor(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Rumor.class));
            case DIGEST -> runtime.gossip().onDigest(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Digest.class));
            case PULL_RESP -> runtime.gossip().onPullResp(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.PullResp.class));
            case HAVE -> runtime.gossip().onHave(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Have.class));
            case GRAFT -> runtime.gossip().onGraft(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Graft.class));
            case PRUNE -> runtime.gossip().onPrune(envelope.from(),
                    codec.fromBytes(envelope.body(), Bodies.Prune.class));
            case QUERY, QUERY_HIT, BLOCK_WANT, BLOCK, PIPE_DATA -> {
                var handler = runtime.handlerFor(envelope.kind());
                if (handler != null) {
                    handler.accept(envelope.from(), envelope.body());
                }
            }
            case BATCH -> onBatch(runtime, envelope, connection);
            case RELAY_FRAME -> onRelayFrame(runtime, envelope);
            case CHANNEL_HELLO -> onChannelHello(envelope, connection);
            case SESSION_HELLO -> onSessionHello(envelope, connection);
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

    /** The capability type of WebRTC signalling (SRAD srad/agentspaces-js.md §3.5). */
    static final String RTC_SIGNAL_CAPABILITY = "aspace:cap/rtc-signal";

    /** The head of a {@code PIPE_DATA} body: the capability type alone, every other key ignored. */
    private record PipeHead(String capability) {
    }

    /**
     * The one relayed inner frame required attestation admits (SPEC §5.6, v0.1.21; SRAD
     * srad/agentspaces-js.md JS-N-03, decision 6): a fully signed {@code PIPE_DATA} of the
     * {@code aspace:cap/rtc-signal} capability addressed to this node. A browser member can
     * reach a node it cannot dial only through a relay, and the data channel its signal opens
     * attests it by the fingerprint the signed signal carried, so the exception admits the
     * signal to the signalling capability and nothing else. A null connection is the relayed
     * path alone ({@link #onRelayFrame}), whose inner frame {@code wire.decode} accepted only
     * when fully signed.
     */
    private boolean relayedSignalToSelf(Envelope envelope, TransportConnection connection) {
        if (connection != null || envelope.kind() != Envelope.Kind.PIPE_DATA
                || !peerId().equals(envelope.to())) {
            return false;
        }
        try {
            PipeHead head = codec.fromBytes(envelope.body(), PipeHead.class);
            return head != null && RTC_SIGNAL_CAPABILITY.equals(head.capability());
        } catch (RuntimeException e) {
            return false;
        }
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
                maybeAnnounceChannel(envelope.group(), connection, null); // the newcomer may not hold us yet
                // An introduction is answered with an introduction, on the same connection and
                // before anything else: this node is about to push the newcomer data (the cards
                // it forwards, its own items), and until the newcomer holds this node every such
                // frame is a non-member's and dropped. Waiting for the next tick's direct
                // introduction left a window of a tick in which a dozen frames were lost and
                // recovered only by anti-entropy (found by example 18, 2026-10-12).
                try {
                    Bodies.Rumor selfAd = selfAdRumor(runtime.id());
                    sendOn(connection, runtime.id(), envelope.from(), Envelope.Kind.RUMOR,
                            new Bodies.Rumor(selfAd.streamId(), selfAd.itemId(), 1, selfAd.payload()));
                    selfAdSentTo.computeIfAbsent(runtime.id(), g -> ConcurrentHashMap.newKeySet()).add(envelope.from());
                } catch (IOException e) {
                    // the next tick introduces this node directly, as before
                }
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
            for (Map.Entry<PeerId, String> entry : admissionMemory.forGroup(groupId).entrySet()) {
                if (List.of(entry.getValue().split(",")).contains(hash)) {
                    PeerId evicted = entry.getKey();
                    membership.evict(evicted);
                    admissionMemory.remove(groupId, evicted); // revocation always beats memory (R2-1.2)
                    readmittedLogged.remove(groupId.value() + '|' + evicted.value());
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
            // The peer may have been a gossip-tree forwarder for this node: ask every other
            // member to push eagerly again, so nothing routes through the sentenced peer.
            for (GroupRuntime runtime : groups.values()) {
                if (runtime.membership().member(peer).isPresent()) {
                    runtime.gossip().forwarderLost(peer);
                }
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
            framesStale.incrementAndGet();
            return true;
        }
        String id = envelope.from().value() + '|' + envelope.stamp().encoded();
        if (recentFrames.put(id, Boolean.TRUE) != null) {
            framesReplayed.incrementAndGet();
            return true;
        }
        return false;
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
    private void maybeAnnounceChannel(GroupId groupId, TransportConnection connection, PeerId peer) {
        if (channelAuth == ChannelAuth.SIGNED) {
            return;
        }
        Optional<PeerId> attested = connection.attestedPeer();
        if (attested.isEmpty()) {
            maybeSessionHello(groupId, connection, peer);
            return;
        }
        if (!helloSent.add(connection)) {
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

    /**
     * Opens this node's half of a session on a connection the transport could not attest
     * (spec §5.6, v0.1.17): once per connection, in SESSION mode, toward a member whose
     * advertisement carries the session hint. The hello travels fully signed; the key is
     * derived when the other half arrives.
     */
    private void maybeSessionHello(GroupId groupId, TransportConnection connection, PeerId peer) {
        if (channelAuth != ChannelAuth.SESSION || peer == null || !sessionPeers.contains(peer)
                || peer.equals(peerId()) || connection.attestedPeer().isPresent()) {
            return; // an attested connection keeps the bare form of §5.6
        }
        Session session = sessions.computeIfAbsent(connection, c -> new Session());
        synchronized (session) {
            long now = clock.millis();
            if (session.key != null || session.answered
                    || (session.helloSent && now - session.helloSentAtMillis < SESSION_HELLO_RETRY_MILLIS)) {
                return;
            }
            sendSessionHello(session, groupId, connection, peer, null);
        }
    }

    /** How long an unanswered opening hello stands before the next trigger may repeat it. */
    private static final long SESSION_HELLO_RETRY_MILLIS = 5_000;

    /** Sends this node's half (opening when {@code echo} is null, answering otherwise), fully signed. */
    private void sendSessionHello(Session session, GroupId groupId, TransportConnection connection,
                                  PeerId peer, byte[] echo) {
        Bodies.SessionHello hello = new Bodies.SessionHello(
                ai.badmonkey.agentspaces.common.crypto.X25519.rawPublicKey(session.ephemeral.getPublic()),
                session.nonce, peer, echo);
        // Marked before the send: a synchronous fabric delivers the peer's reaction, which can
        // trigger this node again, before send() returns.
        boolean wasSent = session.helloSent;
        boolean wasAnswered = session.answered;
        long wasAt = session.helloSentAtMillis;
        session.helloSent = true;
        session.helloSentAtMillis = clock.millis();
        session.answered = echo != null;
        try {
            byte[] frame = encodeFrame(groupId, peer, Envelope.Kind.SESSION_HELLO, hello);
            connection.send(frame);
            signedFramesSent.incrementAndGet();
            countSent(Envelope.Kind.SESSION_HELLO, frame.length);
        } catch (IOException e) {
            session.helloSent = wasSent; // the next trigger repeats it
            session.answered = wasAnswered;
            session.helloSentAtMillis = wasAt;
        }
    }

    /**
     * Handles a SESSION_HELLO (spec §5.6, v0.1.17). The codec admits it only fully signed, so
     * the sender's identity stands behind the ephemeral key; it counts only when it names this
     * node as the remote end and arrives on a real connection. An opening hello is answered
     * with this node's half echoing the opener's nonce, and the key is derived at once but
     * used for outbound frames only once the opener's first MAC frame verifies (key
     * confirmation); an answering hello that echoes this node's nonce confirms at once. When
     * both ends open at the same time, the lower PeerID stays the opener. The key is HKDF
     * over the X25519 agreement, bound to both PeerIDs and both nonces.
     */
    private void onSessionHello(Envelope envelope, TransportConnection connection) {
        if (channelAuth != ChannelAuth.SESSION || connection == null || connection.attestedPeer().isPresent()) {
            return;
        }
        Bodies.SessionHello hello;
        try {
            hello = codec.fromBytes(envelope.body(), Bodies.SessionHello.class);
        } catch (RuntimeException e) {
            return;
        }
        if (hello == null || !peerId().equals(hello.remote()) || hello.ephemeralKey() == null
                || hello.ephemeralKey().length != ai.badmonkey.agentspaces.common.crypto.X25519.RAW_PUBLIC_KEY_LENGTH
                || hello.nonce() == null || hello.nonce().length < 16 || hello.nonce().length > 64) {
            return;
        }
        if (clock.millis() - envelope.stamp().physical() > SESSION_HELLO_MAX_AGE_MILLIS) {
            // A live hello is seconds old; a captured one replayed on a fresh link after the
            // exact-replay cache turned over would otherwise be answered. Ignored, never a strike:
            // its signer sent it once, honestly (ROBUSTNESS.md §5, the hello replay window).
            framesStale.incrementAndGet();
            return;
        }
        Session session = sessions.computeIfAbsent(connection, c -> new Session());
        synchronized (session) {
            if (session.key != null || (session.remote != null && !session.remote.equals(envelope.from()))) {
                return; // one session per connection, with one peer
            }
            boolean opening = hello.echo() == null;
            if (opening) {
                if (session.helloSent && !session.answered
                        && peerId().value().compareTo(envelope.from().value()) < 0) {
                    return; // both opened at once: this node is the opener and waits for the answer
                }
            } else if (!session.helloSent || session.answered
                    || !java.security.MessageDigest.isEqual(hello.echo(), session.nonce)) {
                return; // an answer to a hello this node never opened
            }
            session.remote = envelope.from();
            session.remoteKey = hello.ephemeralKey();
            session.remoteNonce = hello.nonce();
            // The key is in place before anything leaves, so the opener's first MAC frame, which
            // a synchronous fabric delivers before the answer's send returns, already verifies.
            session.key = deriveSessionKey(session);
            session.confirmed = !opening;
            if (opening) {
                sendSessionHello(session, envelope.group(), connection, envelope.from(), hello.nonce());
                if (!session.answered) {
                    session.key = null; // the answer did not leave: the opener will open again
                    session.remote = null;
                }
            }
        }
    }

    /** HKDF over the X25519 agreement, bound to both PeerIDs and both nonces in PeerID order. */
    private byte[] deriveSessionKey(Session session) {
        byte[] shared = ai.badmonkey.agentspaces.common.crypto.X25519.agree(
                session.ephemeral.getPrivate(), session.remoteKey);
        PeerId self = peerId();
        boolean selfFirst = self.value().compareTo(session.remote.value()) < 0;
        PeerId first = selfFirst ? self : session.remote;
        PeerId second = selfFirst ? session.remote : self;
        byte[] firstNonce = selfFirst ? session.nonce : session.remoteNonce;
        byte[] secondNonce = selfFirst ? session.remoteNonce : session.nonce;
        java.io.ByteArrayOutputStream info = new java.io.ByteArrayOutputStream();
        info.writeBytes(SESSION_INFO);
        info.writeBytes(first.value().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        info.write(0);
        info.writeBytes(second.value().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        info.write(0);
        info.writeBytes(firstNonce);
        info.writeBytes(secondNonce);
        return ai.badmonkey.agentspaces.common.crypto.Hkdf.derive(
                shared, new byte[0], info.toByteArray(), WireCodec.SESSION_MAC_LENGTH);
    }

    /**
     * Handles what a member's probe carried (v0.1.17): its recently forwarded items go to the
     * lazy-push machinery as an announcement, and a digest that differs from this node's on a
     * shared stream starts a targeted anti-entropy round with that member, at most once per
     * gossip period per member, so a persistent difference (a stream only one side holds
     * behind a root hash) costs one round per period and no more.
     */
    private void onProbePiggyback(GroupRuntime runtime, PeerId from, byte[] root,
                                  Map<String, byte[]> hashes, List<Bodies.HaveItem> have) {
        GossipBus gossip = runtime.gossip();
        if (have != null && !have.isEmpty()) {
            gossip.onHave(from, new Bodies.Have(have));
        }
        if (!runtime.antiEntropyStretched()) {
            return; // the periodic round is on its base period and reconciles what moves
        }
        boolean mismatch = hashes != null ? gossip.differs(hashes) : gossip.differs(root);
        if (!mismatch) {
            return;
        }
        long now = clock.millis();
        long period = runtime.advertisement().gossip().period().toMillis();
        Long last = probePulls.get(from);
        if (last != null && now - last < period) {
            return;
        }
        probePulls.put(from, now);
        gossip.antiEntropyWith(from);
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
                    .ifPresent(inner -> {
                        noteRelayed(runtime.id(), inner.from()); // JS-W-11: the other direction of the pair
                        dispatch(inner, null);
                    });
            return;
        }
        if (!roles.contains(PeerAdvertisement.PeerRole.RELAY)) {
            return; // only declared relays spend bandwidth forwarding
        }
        // A relay forwards only to a member of the group the frame names (SPEC §5.4, R2-3.1): a
        // connection this node holds to the target through another group is no reason to carry
        // a stranger's frame to it.
        RelayCounters counters = relayCounters(runtime.id());
        if (runtime.membership().member(relayed.target()).isEmpty()) {
            counters.droppedNotMember.incrementAndGet();
            return;
        }
        RelayCounters forTarget = counters.target(relayed.target()); // a verified member from here on
        boolean allowed;
        try {
            allowed = relayPolicy.allow(runtime.id(), envelope.from(), relayed.target(), relayed.frame().length);
        } catch (RuntimeException e) {
            logPolicyFailure(e); // fail closed (R2-S3)
            allowed = false;
        }
        if (!allowed) {
            counters.droppedByPolicy.incrementAndGet();
            forTarget.droppedByPolicy.incrementAndGet();
            return;
        }
        TransportConnection out = connectionTo(runtime.id(), relayed.target());
        if (out == null) {
            return;
        }
        try {
            sendOn(out, runtime.id(), relayed.target(), Envelope.Kind.RELAY_FRAME, relayed);
            counters.framesForwarded.incrementAndGet();
            counters.bytesForwarded.addAndGet(relayed.frame().length);
            forTarget.framesForwarded.incrementAndGet();
            forTarget.bytesForwarded.addAndGet(relayed.frame().length);
        } catch (IOException e) {
            dropConnection(relayed.target(), out);
        }
    }

    /** A relay policy that threw: logged at most once a minute, since it runs once per relayed frame. */
    private void logPolicyFailure(RuntimeException e) {
        long now = clock.millis();
        Long last = lastPolicyFailureLogMillis;
        if (last != null && now - last < 60_000L) {
            return;
        }
        lastPolicyFailureLogMillis = now;
        LOG.log(System.Logger.Level.WARNING, "the relay policy failed; the frame was dropped"
                + " (further failures within a minute are not logged)", e);
    }

    private void sendOn(TransportConnection connection, GroupId groupId, PeerId to,
                        Envelope.Kind kind, Object body) throws IOException {
        // Frames built here always originate from this node, so channel
        // attestation can stand in for the envelope signature (spec §5.6):
        // elide only in ATTESTED mode, toward a peer that announced it accepts
        // bare frames, on the connection whose transport attested that peer.
        Session session = channelAuth == ChannelAuth.SESSION ? sessions.get(connection) : null;
        byte[] sessionKey = session == null || !session.confirmed ? null : session.key;
        if (sessionKey != null && to != null && to.equals(session.remote)
                && kind != Envelope.Kind.SESSION_HELLO && kind != Envelope.Kind.CHANNEL_HELLO) {
            byte[] bodyBytes = body instanceof byte[] raw ? raw : codec.toBytes(body);
            byte[] frame = wire.encodeSession(
                    new Envelope(WIRE_VERSION, groupId, kind, peerId(), to, hlc.now(), bodyBytes), sessionKey);
            connection.send(frame);
            sessionFramesSent.incrementAndGet();
            countSent(kind, frame.length);
            return;
        }
        PeerId announced = channelAuth != ChannelAuth.SIGNED
                ? bareAccepted.get(connection) : null;
        if (announced != null
                && connection.attestedPeer().filter(announced::equals).isPresent()) {
            byte[] bodyBytes = body instanceof byte[] raw ? raw : codec.toBytes(body);
            byte[] bare = wire.encodeBare(
                    new Envelope(WIRE_VERSION, groupId, kind, peerId(), to, hlc.now(), bodyBytes));
            connection.send(bare);
            bareFramesSent.incrementAndGet();
            countSent(kind, bare.length);
            return;
        }
        byte[] frame = encodeFrame(groupId, to, kind, body);
        connection.send(frame);
        signedFramesSent.incrementAndGet();
        countSent(kind, frame.length);
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
        // Spec §5.5: try endpoints in ascending priority (stable for ties), skipping schemes
        // this node has no transport for. A signalled endpoint (webrtc's "sig") is skipped
        // here too: its dial may take the binding's whole timeout and its signals route
        // through this very method, so a send takes the relay and the tick dials it once the
        // relayed traffic warrants (offloadRelayed, JS-W-11).
        return runtime.membership().member(to).map(member -> {
            for (PeerAdvertisement.Endpoint endpoint : byPriority(member.endpoints())) {
                Transport transport = transports.get(endpoint.transport());
                if (transport == null || transport.signalled()) {
                    continue;
                }
                try {
                    return dialEndpoint(groupId, to, transport, endpoint);
                } catch (IOException e) {
                    // Try the next endpoint.
                }
            }
            return null;
        }).orElse(null);
    }

    /**
     * Dials one advertised endpoint of a member, attaches the connection, and introduces this
     * node on it; the connection another thread raced in first wins and the new one closes.
     */
    private TransportConnection dialEndpoint(GroupId groupId, PeerId to, Transport transport,
                                             PeerAdvertisement.Endpoint endpoint) throws IOException {
        TransportConnection dialed = transport.dial(endpoint.address(), to);
        connectionSchemes.put(dialed, transport.scheme());
        attach(dialed);
        TransportConnection raced = connections.putIfAbsent(to, dialed);
        if (raced != null) {
            dialed.close();
            return raced;
        }
        maybeAnnounceChannel(groupId, dialed, null); // a session opens once the peer answers
        // Introduce ourselves on the new channel, as to a seed: a peer that forgot
        // this node (its view expired, or it was quarantined) re-admits it from its
        // own advertisement and from nothing else.
        try {
            Bodies.Rumor selfAd = selfAdRumor(groupId);
            sendOn(dialed, groupId, to, Envelope.Kind.RUMOR,
                    new Bodies.Rumor(selfAd.streamId(), selfAd.itemId(), 1, selfAd.payload()));
        } catch (IOException e) {
            // The channel is dead already; the caller's send will notice and drop it.
        }
        return dialed;
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
        hold(runtime, ad, payload);
    }

    /** Keeps the signed advertisement of a live member for the view's anti-entropy, newest issue wins. */
    private void hold(GroupRuntime runtime, PeerAdvertisement ad, byte[] payload) {
        if (ad.issuer().equals(peerId()) || runtime.membership().member(ad.issuer()).isEmpty()) {
            return;
        }
        long issued = ad.issued().toEpochMilli();
        heldAds.computeIfAbsent(runtime.id(), g -> new ConcurrentHashMap<>())
                .merge(ad.issuer(), new HeldAd(issued, payload), (a, b) -> b.issuedMillis() >= a.issuedMillis() ? b : a);
    }

    /**
     * The group's view as anti-entropy state: a digest naming the live members (the set alone:
     * an advertisement's issue time changes at every refresh, at a different moment on every
     * member, and a digest carrying it never agreed on an idle fleet, so deltas flowed every
     * round and nothing backed off), a delta of the signed advertisements of the members the
     * partner lacks, and an application that admits them as a forwarded copy would, with two
     * exceptions: under attestation a copy admits nobody (as above), and a member this node
     * dropped for silence comes back only on an advertisement issued after the drop, which only
     * a member alive since could have made, so a dead member's still-valid advertisement held
     * by a relay cannot bounce it back (the relay's copy may be newer than this node's and still
     * predate the drop). A changed advertisement (endpoints, roles, hints) travels by the flood.
     */
    private final class ViewState implements ai.badmonkey.agentspaces.peering.gossip.ReconcilableState {
        private final GroupRuntime runtime;

        ViewState(GroupRuntime runtime) {
            this.runtime = runtime;
        }

        /** The live set as this node sees it, itself included: two partners' digests then agree. */
        private java.util.TreeMap<String, Long> lines() {
            java.util.TreeMap<String, Long> lines = new java.util.TreeMap<>();
            Map<PeerId, HeldAd> held = heldAds.getOrDefault(runtime.id(), Map.of());
            for (GroupMembership.Member member : runtime.membership().allMembers()) {
                HeldAd ad = held.get(member.id());
                if (ad != null) {
                    lines.put(member.id().value(), ad.issuedMillis());
                }
            }
            lines.put(peerId().value(), 0L);
            return lines;
        }

        @Override
        public byte[] digest() {
            StringBuilder sb = new StringBuilder();
            lines().forEach((issuer, issued) -> sb.append(issuer).append('\n'));
            return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public byte[] deltaFor(byte[] remoteDigest) {
            java.util.Set<String> remote = new java.util.HashSet<>(List.of(
                    new String(remoteDigest, java.nio.charset.StandardCharsets.UTF_8).split("\n")));
            Map<PeerId, HeldAd> held = heldAds.getOrDefault(runtime.id(), Map.of());
            List<byte[]> missing = new ArrayList<>();
            lines().forEach((issuer, issued) -> {
                if (!remote.contains(issuer)) {
                    if (issuer.equals(peerId().value())) {
                        missing.add(selfAdRumor(runtime.id()).payload()); // the partner lacks this node itself
                    } else {
                        HeldAd ad = held.get(PeerId.of(issuer));
                        if (ad != null) {
                            missing.add(ad.payload());
                        }
                    }
                }
            });
            return missing.isEmpty() ? new byte[0] : codec.toBytes(missing);
        }

        @Override
        public void applyDelta(byte[] delta) {
            if (delta.length == 0) {
                return;
            }
            byte[][] payloads;
            try {
                payloads = codec.fromBytes(delta, byte[][].class);
            } catch (RuntimeException e) {
                return;
            }
            for (byte[] payload : payloads) {
                PeerAdvertisement ad = verifiedPeerAd(payload).orElse(null);
                if (ad == null || ad.issuer().equals(peerId()) || !admits(runtime.advertisement(), ad)) {
                    continue;
                }
                boolean member = runtime.membership().member(ad.issuer()).isPresent();
                if (requireAttestation && !member) {
                    continue; // a copy admits nobody under attestation
                }
                Long droppedAt = lostMembers.getOrDefault(runtime.id(), Map.of()).get(ad.issuer());
                if (!member && droppedAt != null && ad.issued().toEpochMilli() <= droppedAt) {
                    continue; // issued before this node dropped it: no evidence it lives
                }
                if (!member && droppedAt == null && lastView.getOrDefault(runtime.id(), Set.of()).contains(ad.issuer())) {
                    // Dropped inside this very tick (a failed probe), its loss not yet recorded:
                    // the probe's own ACK carried the view hash that started this pull.
                    continue;
                }
                admitAdvertisement(runtime, ad);
                hold(runtime, ad, payload);
            }
        }
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
        lastEndpoints.put(ad.issuer(), List.copyOf(ad.endpoints()));
        Map<PeerId, Long> lost = lostMembers.get(runtime.id());
        if (lost != null) {
            lost.remove(ad.issuer());
        }
        if (admission && runtime.membership().member(ad.issuer()).isPresent()) {
            lostRetryIntervalMillis.remove(runtime.id()); // somebody answered: retries start fresh
            newlyAdmitted.computeIfAbsent(runtime.id(), g -> ConcurrentHashMap.newKeySet()).add(ad.issuer());
            runtime.gossip().topologyChanged();
            runtime.gossip().memberAdmitted(ad.issuer());
        }
        if ("1".equals(ad.resourceHints().get(BATCH_HINT))) {
            batchPeers.add(ad.issuer());
        } else {
            batchPeers.remove(ad.issuer());
        }
        if (lazyPush && "1".equals(ad.resourceHints().get(TREE_HINT))) {
            treePeers.add(ad.issuer());
        } else {
            treePeers.remove(ad.issuer());
        }
        if (channelAuth == ChannelAuth.SESSION && "1".equals(ad.resourceHints().get(SESSION_HINT))) {
            sessionPeers.add(ad.issuer());
        } else {
            sessionPeers.remove(ad.issuer());
        }
        if ("1".equals(ad.resourceHints().get(HASHES_HINT))) {
            hashPeers.add(ad.issuer());
        } else {
            hashPeers.remove(ad.issuer());
        }
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
            byte[] signedAd = selfAdRumor(groupId).payload();
            ai.badmonkey.agentspaces.peering.bootstrap.BeaconSeal seal = beaconSeals.get(groupId);
            announcements.add(seal == null ? signedAd : seal.seal(signedAd, clock.instant()));
        }
        return announcements;
    }

    /**
     * Seals this group's bootstrap-beacon datagrams under its content key
     * (SPEC §10.1 v0.1.19 addendum, SRAD dtls-support group M), and opens
     * sealed datagrams that carry its tag. Members holding the same key find
     * each other; a bystander learns nothing but a rotating tag. A group with
     * no beacon key keeps the plain datagram.
     *
     * @param groupId    the group
     * @param contentKey the group's content key
     */
    public void beaconKey(GroupId groupId, byte[] contentKey) {
        beaconSeals.put(Objects.requireNonNull(groupId, "groupId"),
                new ai.badmonkey.agentspaces.peering.bootstrap.BeaconSeal(contentKey));
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
        if (ai.badmonkey.agentspaces.peering.bootstrap.BeaconSeal.isSealed(datagram)) {
            // A sealed beacon (SPEC §10.1 v0.1.19): open it with a held group key, or drop
            // it unread; the opened bytes take the plain path below, verification and all.
            byte[] opened = null;
            for (ai.badmonkey.agentspaces.peering.bootstrap.BeaconSeal seal : beaconSeals.values()) {
                opened = seal.open(datagram, clock.instant()).orElse(null);
                if (opened != null) {
                    break;
                }
            }
            if (opened == null) {
                return;
            }
            datagram = opened;
        }
        PeerAdvertisement ad = verifiedPeerAd(datagram).orElse(null);
        if (ad == null || ad.issuer().equals(peerId())) {
            return;
        }
        GroupRuntime runtime = groups.get(ad.group());
        if (runtime == null || !admits(runtime.advertisement(), ad)) {
            return;
        }
        if (!ad.endpoints().isEmpty()) {
            beaconLearned.computeIfAbsent(ad.group(), g -> new ConcurrentHashMap<>()).put(ad.issuer(),
                    new LearnedEndpoints(List.copyOf(ad.endpoints()), ad.issued().plus(ad.ttl()).toEpochMilli()));
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
        if (ad.issuer().equals(peerId())) {
            return true; // our own advertisement, flooded back: membership ignores it, and it is never recorded
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
     * A delegated credential whose only fault is its delegation's expiry admits
     * a member the admission memory holds under the identical credential
     * (SPEC §5.1 v0.1.20, ISSUE-PeerRobustness-2 R2-1): the invite window
     * bounds first admission; a peer the memory does not hold is judged against
     * the clock as before, and revocation always beats the memory.
     */
    private boolean admitsInvite(GroupAdvertisement groupAd, PeerAdvertisement ad) {
        String credential = ad.resourceHints().get(JoinCredentials.HINT_KEY);
        JoinCredentials.Verdict verdict = JoinCredentials.judge(credential, ad.issuer(), groupAd, codec,
                clock.instant());
        if (verdict == JoinCredentials.Verdict.INVALID) {
            return false;
        }
        byte[] hash = JoinCredentials.hash(credential);
        GroupRuntime runtime = groups.get(groupAd.group());
        if (runtime != null && runtime.credentialRevocations().joinCredentialRevoked(hash)) {
            return false;
        }
        // A delegated credential also dies with its delegation: the founder revokes an invite key
        // by its delegation's hash and every credential issued under it is refused at once.
        byte[] delegationHash = JoinCredentials.delegationHash(credential);
        if (delegationHash != null && runtime != null
                && runtime.credentialRevocations().joinCredentialRevoked(delegationHash)) {
            return false;
        }
        GroupId groupId = groupAd.group();
        String record = HexFormat.of().formatHex(hash)
                + (delegationHash == null ? "" : "," + HexFormat.of().formatHex(delegationHash));
        if (verdict == JoinCredentials.Verdict.EXPIRED) {
            if (!admissionMemory.holds(groupId, ad.issuer(), record)) {
                // A first admission, or another credential: the window has closed (R2-1.3, R2-S1).
                LOG.log(System.Logger.Level.DEBUG, "refused " + ad.issuer().display() + " in group "
                        + groupId.value() + ": its invite delegation has expired and this node holds no"
                        + " admission of it under that credential");
                return false;
            }
            admissionMemory.touch(groupId, ad.issuer());
            if (readmittedLogged.add(groupId.value() + '|' + ad.issuer().value())) {
                LOG.log(System.Logger.Level.INFO, "member " + ad.issuer().display() + " of group "
                        + groupId.value() + " is admitted from memory: its invite delegation has expired,"
                        + " and it was admitted under the same credential while the delegation was valid");
            }
            return true;
        }
        admissionMemory.record(groupId, ad.issuer(), record);
        return true;
    }

    /** The admission memory, for tests. */
    AdmissionMemory admissionMemory() {
        return admissionMemory;
    }

    /** How many members' signed advertisements this node holds for a group's view anti-entropy, for tests. */
    int heldAdCount(GroupId groupId) {
        return heldAds.getOrDefault(groupId, Map.of()).size();
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
