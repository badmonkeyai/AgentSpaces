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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.SpaceListener;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.VerifiedSignatures;
import ai.badmonkey.agentspaces.identity.AgentCertificates;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.ConsistencyHint;
import ai.badmonkey.agentspaces.space.DiscoveryPublisher;
import ai.badmonkey.agentspaces.space.SpaceAdmissionException;
import ai.badmonkey.agentspaces.space.SpaceReadOnlyException;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.space.EntryView;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The replicated AgentSpace (spec §7): the same {@link Space} interface as
 * {@code LocalSpace}, backed by the delta-CRDT replica and the group's gossip
 * bus. Writes, completions, and take claims ride the rumor channel as deltas;
 * anti-entropy reconciles anything rumors missed; and every replica evaluates the
 * same deterministic rules, so replicas that have seen the same updates agree.
 *
 * <p>Exclusive take runs the {@code LEASE_RACE} strategy (spec §7.4): a claim is
 * a {@link TakeClaim} lattice value published as a delta; after the settle window
 * the deterministic merge decides the winner identically on every replica. The
 * settle guarantee (SPEC §7.4, 2026-11-15): while every claim reaches every rival
 * claimant within the settle window (half the window for an AUCTION counter-claim),
 * at most one take of an entry wins. A claim later than that can leave two holders
 * that each judged themselves the winner. A holder that learns of a better claim, or
 * of the entry's completion, before it completes cannot complete; a holder whose
 * claim wins the lattice and that learns of the displaced holder's completion first
 * acknowledges that completion and writes no result. When each learns only after it
 * completed, both did the work: the shared state still keeps one completion (the
 * CRDT's monotone completed flag, and a completion authenticates only under the claim
 * the lattice keeps), and the replica whose claim won counts the duplicate
 * ({@link #duplicateCompletions()}). A result that must exist exactly once needs the
 * ordered log (ORDERED) or an idempotent completion.
 *
 * <p>Entry records are signed over their immutable identity (id, space, type,
 * payload, issuer, issue stamp, tags), and receivers verify the signature and
 * the writer's self-certifying identity before merging a delta.
 *
 * <p>Lifecycle (spec §7.5): a space built with {@link Builder#advertise}
 * publishes its signed {@code SpaceAdvertisement} on creation and on
 * {@link #refreshAdvertisement()}; {@link Builder#admission(SpaceAdmission)}
 * restricts who may write, take, and complete (under {@code CREDENTIAL} the
 * issuer's node hands out leased {@link SpaceCredential} entries with
 * {@link #grant} and withdraws them with {@link #revoke}); and
 * {@link Builder#founderLease} or
 * {@link #markReadOnly()} turns the space read-only. Lease lapses surface as
 * {@code EXPIRED} and {@code REAPPEARED} events from the sweep that the
 * group tick and every anti-entropy digest run when something is due (or
 * {@link #sweepNow()} at once), and the same sweep collects tombstones at
 * their horizon of spec §7.3 ({@link SpaceStateCrdt#keepMillis}): twice the
 * maximum lease after their lease lapsed for a completion, five minutes at
 * most for a lapse, and for a withdrawal the rule the view allows, never
 * sooner than the clock drift ceiling. An expiry index keeps the sweep to the entries that are due, so
 * an unchanged space costs O(1) per probe (reviews/IDLE-WORK-2026-11-13.md). A
 * cancel surfaces as {@code EXPIRED} too, as on a {@code LocalSpace}: at the
 * cancelling replica at once, and at every other replica that held the entry
 * when the withdrawal merges, once per entry with the lapse (core cancel
 * events, 2026-11-09).
 */
public final class ReplicatedSpace implements Space, AutoCloseable {

    private final String name;
    private final SpaceId id;
    private final PeerIdentity identity;
    /**
     * Who signs this handle's records (spec §4.2, QA4 A4-7): the peer-signed
     * identity for {@code agentName} unless the builder was given another. Claims
     * and state transitions stay signed by the peer key in phase 1; the record
     * signature is the writer's.
     */
    private final AgentIdentity writer;
    private final AgentId issuer;
    /** Certificates of agent-attested writers, per entry, carried beside the record. */
    private final Map<EntryId, AgentCertificate> issuerCertificates = new HashMap<>();
    private final AgentCertificates certificates;
    private final GroupRuntime runtime;
    private final InstantSource clock;
    private final HybridLogicalClock hlc;
    private final CborCodec codec;
    private final SchemaRegistry schemas;
    private final Duration settleWindow;
    private final ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy;
    private volatile java.util.function.ToDoubleFunction<Object> bidFunction;
    private final ai.badmonkey.agentspaces.peering.blocks.BlockExchange blocks;
    /** The bulk-payload seam (LB-E-02); null when the space escalates nothing. */
    private final ai.badmonkey.agentspaces.api.spi.BulkPayloads bulk;
    /** Payload bytes above which a write escalates to {@link #bulk}; 0 disables. */
    private final long escalateAbove;
    /**
     * Whether an escalated payload is sealed before it goes to {@link #bulk} (today's
     * rule, the default). A PDT space sets it false (srad/agentspaces-pdt.md PD-E-04):
     * nodes travel sealed per message and the record names the epoch in
     * {@code keyEpoch}; the record signature over {@code payloadRef} binds the tree.
     */
    private final boolean sealBeforeEscalate;
    /**
     * The seam a write takes when {@link #bulk} cannot reach every member right now
     * (PD-E-06: a member lacks the tree exchange); null for today's path (one block,
     * or inline). A payload through it is sealed as today.
     */
    private final ai.badmonkey.agentspaces.api.spi.BulkPayloads bulkFallback;
    /** Whether a write that {@link #bulk} cannot carry is refused naming the members (PD-E-06, {@code require}). */
    private final boolean bulkRequired;
    /** Whether this replica sends recordless renews, cancels, and completes when it may (PD-L-01, PD-L-03). */
    private final boolean recordless;
    /** A payload cap for a space without the content swarm (LB-E-06); 0 leaves the block cap alone. */
    private final long maxPayload;
    /** The group's content keys by epoch (SPEC §11a.3); null for an unencrypted space. */
    private final ai.badmonkey.agentspaces.common.crypto.GroupKeyRing keyRing;
    private final java.util.function.Predicate<Map<String, String>> tagShard;
    private final String streamId;
    /** The bus registrations this handle owns; closed by {@link #close()} (QA4 A4-9). */
    private final AutoCloseable streamRegistration;
    private final AutoCloseable reconcileRegistration;
    /**
     * The space's one participant on the node's loop (srad/agentspaces-event-loop.md EL-T-09): the expiry sweep, the
     * block and bulk retries and the horizon's rounds, each at its own deadline (see {@link #runDue(long)}).
     */
    private final ai.badmonkey.agentspaces.api.spi.DueWork.Registration dueRegistration;
    /**
     * Whether only this replica's writer reads the space ({@link Builder#local()}): it neither gossips its writes
     * nor joins anti-entropy, so the group pays nothing for it (reviews/IDLE-WORK-2026-11-13.md item 15, 5a(f)).
     */
    private final boolean local;

    /** Bounds {@link #shardDroppedOrder} so a sharded replica offered a long stream
     * of out-of-shard entries does not accumulate acknowledgement lines forever. */
    private static final int SHARD_DROPPED_CAP = 8192;

    /** Bounds the acknowledged lines of one space ({@link #refusedAckOrder}). */
    private static final int REFUSED_ACK_CAP = 8192;

    /**
     * How long the acknowledgement of a re-offered collected tombstone is held after its last refusal (RE-H-03): the
     * lapse grace, so a member that keeps the tombstone longer offers it at most once per hold instead of once per
     * round, and the line is gone soon after that member collects it too.
     */
    static final long COLLECTED_ACK_HOLD_MILLIS = SpaceStateCrdt.LAPSED_GRACE_MILLIS;

    /** Bounds {@link #rejectedOffers}. */
    private static final int REJECTED_OFFERS_CAP = 4096;

    /**
     * Ceiling on the lease duration that feeds the tombstone GC horizon (spec
     * §7.3). Tracking the observed maximum lets a hostile member freeze GC
     * fleet-wide by writing one entry with a century-long lease; the cap keeps
     * that damage to two days of retention. A longer lease still protects its
     * own entry, since nothing is collected before its lease has lapsed.
     */
    static final long MAX_TRACKED_LEASE_MILLIS = Duration.ofHours(24).toMillis();

    /** Default TTL of the published space advertisement (spec §7.5). */
    static final Duration DEFAULT_ADVERTISEMENT_TTL = Duration.ofMinutes(15);

    private static final System.Logger LOG = System.getLogger(ReplicatedSpace.class.getName());

    private final Object lock = new Object();
    /**
     * The record, state and claim signatures this replica verified (swap amplification, ROBUSTNESS.md §10.8): a state
     * that came by anti-entropy and again by gossip, and a record that comes again with every later state of its entry,
     * verify once.
     */
    private final VerifiedSignatures signatures = new VerifiedSignatures(VerifiedSignatures.DEFAULT_CAPACITY);
    private SpaceStateCrdt crdt = SpaceStateCrdt.empty();
    /** Bumped under the lock on every change of the reconciled state; the digest cache is keyed on it. */
    private long generation;
    private byte[] digestCache;
    private long digestGeneration = -1;
    private long digestComputations;
    /**
     * Full-digest asks answered from the cache, and asks that had to rebuild it (RE-F-03's signals); hash asks are
     * counted in their own pair below, not here. Under {@link #lock}.
     */
    private long digestCacheHits;
    private long digestCacheMisses;
    /** The same for the hash a probe or a hashed opening carries. Under {@link #lock}. */
    private long digestHashHits;
    private long digestHashMisses;
    /**
     * The SHA-256 of {@link #digestCache}, and the digest it was taken over (IDLE-WORK item 3): a probe and a hashed
     * anti-entropy round ask for the hash of every stream, and re-hashing a cached multi-megabyte digest each time
     * was most of what an unchanged space cost. Kept by identity with the cached digest, so it is stale exactly when
     * the digest is.
     */
    private byte[] digestHashCache;
    private byte[] digestHashOf;

    // ---- the expiry index (IDLE-WORK item 4)
    /**
     * Each entry id with something the sweep will do for it, filed under the earliest time it is due: a write lease
     * lapsing (EXPIRED), a take claim lapsing on a present entry (REAPPEARED), a lapsed claim on an entry this replica
     * does not hold (dropped), or a tombstone reaching its collection horizon. The sweep used to visit every entry on
     * every digest, which made a probe of an idle space cost the whole space; it now visits only what is due, and
     * nothing at all until the earliest time here. Kept in step, under {@link #lock}, by {@link #reindex} at every
     * place an entry's state, its claim, or the sweep's own once-only bookkeeping changes.
     */
    private final TreeMap<Long, Set<EntryId>> dueByTime = new TreeMap<>();
    /** The time each id is filed under in {@link #dueByTime}. Under {@link #lock}. */
    private final Map<EntryId, Long> dueOf = new HashMap<>();
    /** No earlier than the first moment a pending recordless state outlives its TTL; MAX when none waits. Under {@link #lock}. */
    private long pendingDueMillis = Long.MAX_VALUE;
    /** The latest clock reading the index was computed at; a clock that steps back below it rebuilds the index. */
    private long indexedAtMillis = Long.MIN_VALUE;
    /** Entry ids the sweep has evaluated, for the test that an unchanged space visits none. Under {@link #lock}. */
    private long sweepVisits;
    /** What the last sweep did, for the property test against the full scan. Under {@link #lock}. */
    private SweepActions lastSweep = SweepActions.NONE;

    // ---- the change signal a waiting take or read sleeps on (IDLE-WORK 5a(a))
    /** Bumped with every change a waiting take or read could act on. Written under {@link #lock}; read anywhere. */
    private volatile long signals;
    /** Threads waiting on {@link #lock} for a change; a change notifies only when there are any. Under {@link #lock}. */
    private int waiters;
    /**
     * The one take-wait rule (srad/agentspaces-event-loop.md EL-W-07, {@link ai.badmonkey.agentspaces.common.time.Waits}):
     * a waiting take or read waits for this space's change signal or the next time the expiry index says a claim or a
     * lease lapses, and never longer than the backstop between looks. What lets a take succeed signals: a write, a
     * merge, a renewal, a cancel, a claim, a sweep that lapses a claim or collects, a block or artifact landing (the
     * fetch and its retries signal), and a group key installed for a sealed payload (EL-W-07 added it). The space's
     * former 1 s and 30 s slices are this one rule.
     *
     * <p>A compatibility rule (as phase 3's for local periods, §10.16): a space on a plain clock that a test moves by
     * hand looks again every {@link ai.badmonkey.agentspaces.common.time.Waits#HAND_CLOCK_LOOK}, because moving that
     * clock signals nobody, so a lease the test lapses by hand would otherwise wait for the backstop.
     */
    private long lookSliceNanos() {
        return (handClock ? ai.badmonkey.agentspaces.common.time.Waits.HAND_CLOCK_LOOK
                : ai.badmonkey.agentspaces.common.time.Waits.BACKSTOP).toNanos();
    }

    /** Whether only a test's hand moves this space's clock (see {@link #lookSliceNanos}). */
    private final boolean handClock;

    /**
     * The source every wait of this space is reckoned and parked on (EL-C-10, EL-W-01): the space's clock when it is a
     * time source (the system's, or a virtual one a test advances), else the system's.
     */
    private final ai.badmonkey.agentspaces.common.time.TimeSource waits;

    /** The base of this replica's local leases, its subscriptions' (EL-C-04, §10.18): see {@link #localMillis()}. */
    private final ai.badmonkey.agentspaces.common.time.TimeSource localTime;

    /** Unregisters the key ring's install listener on close. */
    private final AutoCloseable keyInstalls;

    /** How many times a blocking read or take has tried, for the test that a waiting take does not poll. */
    private final AtomicLong waitingPolls = new AtomicLong();
    /**
     * The live index (RE-M-04, srad/replication-by-entry.md): every present entry's record under its current lease, by
     * id, beside the map. A read after a change walks the live entries alone, where it used to rebuild the list from
     * every entry the map held, tombstones included. A derived cache of {@link #crdt}, kept in step under
     * {@link #lock} by {@link #setCrdt} at every place an entry's state changes.
     */
    private final LiveIndex liveIndex = new LiveIndex();
    /**
     * Entries whose current claim was decided by the ordered log, with the
     * decided epoch (QA4 A4-5). A log decision is authoritative for its epoch:
     * a gossip-borne claim at the same or a lower epoch cannot displace it,
     * which is the priority the old log-index re-stamp gave committed claims
     * by accident and this gives them by rule. An identical claim may still
     * merge, so an attestation upgrade through a completion delta is allowed,
     * and so may the decided holder's renewal of its claim (same claim, later
     * expiry), so a drain longer than the take lease keeps its hold at every voter.
     */
    private final Map<EntryId, Long> logDecidedEpochs = new HashMap<>();
    private final Map<EntryId, byte[]> issuerKeys = new HashMap<>();
    /** The latest actor-signed state DTO per entry, so anti-entropy forwards the
     * original {@code stateSig} (a relay cannot re-sign state it did not author).
     * Kept in step with {@link #crdt} by {@link #recordSignedState}. */
    private final Map<EntryId, SpaceWire.EntryStateDto> signedStates = new HashMap<>();
    /**
     * The order in which acknowledged lines ({@code e:<id>=<hash>} or {@code c:<id>=<hash>}) were last refused, oldest
     * first, and the entry each names (ASF-047, and the stamp refusals of HLC canonical stamps, 2026-11-08). The lines
     * themselves live in the slots of {@link #crdt} (RE-M-07), so one root covers every digest line; this access-ordered
     * map only bounds them at {@link #REFUSED_ACK_CAP} per space, forgetting the least recently refused first, as the
     * map that held the lines before did. Under {@link #lock}.
     */
    private final java.util.LinkedHashMap<String, EntryId> refusedAckOrder = new java.util.LinkedHashMap<>(16, 0.75f, true);
    /**
     * The acknowledged lines of re-offered collected tombstones (RE-H-03) and the time each is forgotten, in refusal
     * order; each line is also in {@link #refusedAckOrder}, whose cap bounds both. Under {@link #lock}.
     */
    private final java.util.LinkedHashMap<String, Long> collectedAcks = new java.util.LinkedHashMap<>();
    /**
     * When each acknowledgement of a state or claim refused at the door is forgotten (P6, srad/replication-by-entry.md
     * phase 4): once what it refused is past the horizon at which every honest holder collects it. Kept, the line
     * would hold this replica's audited set apart from its partners' for good after they collect, and every round
     * would descend to it and fetch nothing. Line to time, and time to lines for the sweep. Under {@link #lock}.
     */
    private final Map<String, Long> refusedForgetAt = new HashMap<>();
    private final java.util.TreeMap<Long, Set<String>> refusedForgetBy = new java.util.TreeMap<>();
    /** Entries whose claim was refused for a revoked holder, so the completion
     * that would authenticate against that claim is acknowledged, not re-offered. */
    private final Set<EntryId> refusedClaimEntries = java.util.Collections.newSetFromMap(
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<EntryId, Boolean> eldest) {
                    return size() > REFUSED_ACK_CAP;
                }
            });
    /**
     * The order in which out-of-shard entries were last dropped here, oldest first. Their lines live in the slots of
     * {@link #crdt} (RE-M-07); this only bounds them at {@link #SHARD_DROPPED_CAP}, forgetting the oldest first, as
     * the map that held them before did. Under {@link #lock}.
     */
    private final java.util.LinkedHashMap<EntryId, Boolean> shardDroppedOrder =
            new java.util.LinkedHashMap<>(16, 0.75f, true);
    private final Map<EntryId, Object> valueCache = new ConcurrentHashMap<>();
    private final List<Sub> subscriptions = new CopyOnWriteArrayList<>();
    private final AtomicLong dotCounter = new AtomicLong();

    // ---- the record-free renew (srad/agentspaces-pdt.md §3.9, group L)
    /** At most this many recordless states wait here for their record (PD-L-02). */
    static final int MAX_PENDING_STATES = 1_024;
    /** A pending recordless state is dropped after this long without its record (PD-L-02). */
    static final long PENDING_STATE_TTL_MILLIS = 10 * 60 * 1000L;
    /**
     * Entries whose record this replica has evidence a remote replica holds: a remote
     * digest line naming the entry, a remote state or claim for it, a record that came
     * from a remote in the first place, or a quiet anti-entropy round after this
     * replica's own write (PD-L-01). Under {@link #lock}.
     */
    private final Set<EntryId> replicated = new HashSet<>();
    /** This replica's own writes and when they happened, until evidence arrives. Under {@link #lock}. */
    private final Map<EntryId, Long> ownWrites = new HashMap<>();
    /** A recordless state waiting for its record, and when it arrived. */
    private record PendingState(SpaceWire.EntryStateDto dto, long sinceMillis) {
    }
    /** Recordless states whose record this replica lacks, by entry; bounded, insertion order. Under {@link #lock}. */
    private final Map<EntryId, PendingState> pendingStates = new java.util.LinkedHashMap<>();
    private final AtomicLong recordlessSent = new AtomicLong();
    private final AtomicLong recordlessMerged = new AtomicLong();
    private final AtomicLong recordlessHeld = new AtomicLong();
    /** Takes that acknowledged an outbid holder's completion instead of doing the work again. */
    private final AtomicLong outbidAcknowledged = new AtomicLong();
    /** Entries completed here, with work, whose displaced rival's own completion arrived afterwards (SPEC §7.4). */
    private final AtomicLong duplicateCompletions = new AtomicLong();
    /** Displaced claims of this peer's own completions offered again (SPEC §7.4, rule two; see displacedOwn). */
    private final AtomicLong reoffersSent = new AtomicLong();
    /** Distinct states and claims refused for a logical counter above LOGICAL_LIMIT. */
    private final AtomicLong stampsLogicalRefused = new AtomicLong();
    /** Space deltas, by rumor or by anti-entropy, that did not decode (HLC canonical stamps, 2026-11-08). */
    private final AtomicLong deltasUndecodable = new AtomicLong();
    /** Distinct states and claims refused as hostile for a stamp an hour past the far-future margin. */
    private final AtomicLong stampsFarFutureRefused = new AtomicLong();
    /** Distinct states and claims deferred for a stamp past the far-future margin. */
    private final AtomicLong stampsFarFutureDeferred = new AtomicLong();
    /** Signed states and claims this replica offered in anti-entropy deltas, and the states and claims it was offered. */
    private final AtomicLong syncStatesSent = new AtomicLong();
    private final AtomicLong syncClaimsSent = new AtomicLong();
    private final AtomicLong syncStatesReceived = new AtomicLong();
    private final AtomicLong syncClaimsReceived = new AtomicLong();
    /**
     * Renewals that asked for less than the lease had left (RE-H-01 and RE-H-02, srad/replication-by-entry.md §3.6).
     * A renewal never shortens a lease (SPEC §7.2), so each such call kept the longer lease; the count shows which
     * callers still ask for a shorter one.
     */
    private final AtomicLong renewShortenings = new AtomicLong();
    /** Whether the first shortening renewal of this space was logged; each later one is only counted. */
    private final AtomicBoolean renewShorteningLogged = new AtomicBoolean();
    /**
     * Re-offered states of entries this replica no longer holds, refused at the door as collectable tombstones and
     * acknowledged (RE-H-03).
     */
    private final AtomicLong collectedReoffersAcknowledged = new AtomicLong();
    /** Listener calls that threw; each is isolated, so the other listeners and events still run. */
    private final AtomicLong listenerFailures = new AtomicLong();

    // ---- tombstone GC and lease-lapse events (spec §7.2, §7.3)
    /** Builder override for the GC horizon, or null to use the observed maximum. */
    private final Duration maxLeaseOverride;

    // ---- the per-entry horizon (RE-H-04, RE-Q-REL-09; SPEC §7.3, v0.1.35)
    /** Default rounds the view must stay fully hinted before the per-entry horizon engages. */
    public static final int DEFAULT_HORIZON_ENGAGE_ROUNDS = 3;
    /** Bounds of {@link #horizonEngageRounds(int)}. */
    public static final int HORIZON_ENGAGE_ROUNDS_FLOOR = 1;
    /** Bounds of {@link #horizonEngageRounds(int)}. */
    public static final int HORIZON_ENGAGE_ROUNDS_CEILING = 64;
    /** Rounds the view must stay fully hinted before the mode engages; settable at runtime within its bounds. */
    private volatile int horizonEngageRounds = DEFAULT_HORIZON_ENGAGE_ROUNDS;
    /** Guards the mode's hysteresis below; a leaf lock, never held while taking {@link #lock}. */
    private final Object horizonLock = new Object();
    /** The mode as last judged; read without a lock where a stale answer is harmless (the expiry index). */
    private volatile HorizonMode horizonMode = HorizonMode.LEGACY;
    /** Fully hinted rounds counted since the view last held an unhinted member. Under {@link #horizonLock}. */
    private int horizonStreak;
    /** Space-clock time the streak last counted a round, or {@link Long#MIN_VALUE}. Under {@link #horizonLock}. */
    private long horizonCountedAtMillis = Long.MIN_VALUE;
    /** Space-clock time the mode engaged, while it is engaged. Under {@link #horizonLock}. */
    private long horizonEngagedAtMillis;
    /** The mode reached {@link HorizonMode#PER_ENTRY}, so the expiry index must file every tombstone afresh. */
    private volatile boolean horizonRefile;
    /**
     * Withdrawals filed at the legacy horizon under the per-entry mode only because a writer was out of view, by that
     * writer's PeerID text (RE-H-04). When the writer comes back into view its wait becomes known, and they are filed
     * again so that they fall due when it ends, as a pass over every entry would find. Keyed by writer, so a judgement
     * looks once at each absent writer, never at every tombstone. Under {@link #lock}.
     */
    private final Map<String, Set<EntryId>> awaitingWriter = new HashMap<>();
    /**
     * Each writer's current stay in the view, as {node-clock join time, space-clock start} ({@link
     * #writerInViewSince}). Read by the door outside {@link #lock} as well, so concurrent; one small pair per writer
     * in view, dropped when the writer is found out of view and cleared with the index.
     */
    private final Map<String, long[]> writerStays = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicLong horizonEngagements = new AtomicLong();
    private final AtomicLong horizonSettlements = new AtomicLong();
    private final AtomicLong horizonDisengagements = new AtomicLong();
    /** Whether this space logged a disengagement yet; later ones log at debug. */
    private final java.util.concurrent.atomic.AtomicBoolean horizonDisengageLogged =
            new java.util.concurrent.atomic.AtomicBoolean();
    /** Largest lease duration this replica has observed for the space, capped. */
    private volatile long maxLeaseObservedMillis;
    /** Entries whose EXPIRED event already fired here, for a lapse or a cancel. Under {@link #lock}. */
    private final Set<EntryId> expiredFired = new HashSet<>();
    /** Claim epoch per entry whose REAPPEARED event already fired here. Under {@link #lock}. */
    private final Map<EntryId, Long> reappearedEpochs = new HashMap<>();
    /** Claim epoch per entry whose TAKEN event already fired here: a won local take, or a merged remote claim
     *  (a replica that only watches a lane hears of its takes this way). Under {@link #lock}. */
    private final Map<EntryId, Long> takenEpochs = new HashMap<>();
    /**
     * The claim epoch per entry at which a holder that this replica's own claim displaces in the lattice has
     * completed the entry anyway: an AUCTION incumbent a better bid outbid (example 18, 2026-11-15), or, under
     * either strategy, a claimant whose claim reached it later than the settle window, so it judged itself the
     * winner (the settle guarantee, SPEC §7.4, 2026-11-15). That holder judged its take before the better claim
     * reached it, ran its action and completed. Its completion then reaches this replica, where the better claim
     * holds the entry, and fails to authenticate against the held claim, so it is dropped (SPEC §11a.4). Dropping
     * it is right for the shared state, but the work is done: this replica's take, when it is judged, or its
     * completion, when it comes, acknowledges that completion under its own claim, which every replica
     * authenticates, and writes no second result. Only claims held by this peer's agents are noted, so the map
     * holds at most one line per take in flight here; it is bounded besides. Under {@link #lock}.
     */
    private final Map<EntryId, Long> outbidCompletions = new java.util.LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<EntryId, Long> eldest) {
            return size() > OUTBID_COMPLETIONS_CAP;
        }
    };
    /** At most this many {@link #outbidCompletions} lines are kept; past it the oldest goes. */
    private static final int OUTBID_COMPLETIONS_CAP = 1024;
    /**
     * The claim epoch per entry that one of this peer's agents completed here with its own work (not an
     * acknowledgement of a displaced holder's completion), until the entry is collected or a duplicate is counted.
     * A displaced holder's authentic completion that arrives after it is the duplicate completion the settle
     * guarantee leaves possible beyond the window (SPEC §7.4): both holders did the work. It is counted in
     * {@link #duplicateCompletions} so an operator and a test can see it; nothing in the shared state changes.
     * Bounded like {@link #outbidCompletions}. Under {@link #lock}.
     */
    private final Map<EntryId, Long> completedHere = new java.util.LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<EntryId, Long> eldest) {
            return size() > OUTBID_COMPLETIONS_CAP;
        }
    };
    /**
     * The claims that a claim held by one of this peer's agents displaced in the lattice, per entry, kept as
     * evidence for rule two of the settle guarantee (SPEC §7.4; the store race, 2026-11-22). The lattice keeps one
     * claim per entry, so a replica that has merged the better claim offers a displaced holder's completion, by
     * anti-entropy, under the better claim alone, which does not authenticate it; and the displaced holder's own
     * completion delta, the one frame that carries its claim with it, can be lost or come after this peer's take
     * completed. A displaced claim and that holder's completion that reach this replica in different frames, in
     * either order, are judged together ({@link #judgeDisplaced}). Verified claims only, at most
     * {@link #DISPLACED_PER_ENTRY} per entry; bounded like {@link #outbidCompletions}. Under {@link #lock}.
     */
    private final Map<EntryId, List<SpaceWire.SignedClaim>> displacedClaims = boundedLines();
    /**
     * Completions dropped here, per entry, while a claim held by one of this peer's agents held it: each failed to
     * authenticate against that claim, and may be a displaced holder's whose claim reaches this replica in another
     * frame ({@link #displacedClaims}). At most {@link #DISPLACED_PER_ENTRY} per entry. Under {@link #lock}.
     */
    private final Map<EntryId, List<SpaceWire.EntryStateDto>> unprovenCompletions = boundedLines();
    /** At most this many displaced claims, or unproven completions, are kept per entry; past it the oldest goes. */
    private static final int DISPLACED_PER_ENTRY = 4;

    /**
     * One of this peer's completions whose claim a better claim displaced, and its offers so far. Its schedule is on
     * the space's local (monotonic) base, so a sleep or a wall step neither spends the offers at once nor stalls them
     * (srad/agentspaces-event-loop.md §10.36); the claim's own expiry and drift stay on the wall clock.
     */
    private static final class Reoffer {
        final SpaceWire.SignedClaim claim;
        int offers;
        /** When the next offer may go, on {@link #localMillis()}. */
        long nextMillis;

        Reoffer(SpaceWire.SignedClaim claim, long nextMillis) {
            this.claim = claim;
            this.nextMillis = nextMillis;
        }
    }

    /**
     * The entries one of this peer's agents completed with its own work under a claim that a better claim, merged
     * here afterwards, displaced: the losing side of rule two (SPEC §7.4). This replica now holds the better claim,
     * so its anti-entropy offers the completion under that claim only, and the holder of the better claim cannot
     * authenticate it. The displaced claim is offered again, as the claim delta it first travelled in, when the
     * better claim arrives and then whenever a replica still offers the entry present (it has not heard of the
     * completion), with a doubling backoff, at most {@link #REOFFER_MAX} times, while the claim is live and well
     * inside the drift ceiling. The holder of the better claim judges it with the completion, which reaches it by
     * anti-entropy (owner direction 2026-11-09: no guarantees in P2P; what is missing is offered again, not given up
     * on). Bounded like {@link #outbidCompletions}. Under {@link #lock}.
     */
    private final Map<EntryId, Reoffer> displacedOwn = boundedLines();
    /**
     * The claims made here that a better claim displaced, per entry, whether or not an agent here completed under
     * them: what a dial-only member answers a relayed or another member's {@code DIGEST} for (SPEC v0.1.37 §9.4), so a
     * winner's targeted round reaches it. Bounded like {@link #outbidCompletions}. Under {@link #lock}.
     */
    private final Map<EntryId, SpaceWire.SignedClaim> displacedHere = boundedLines();
    /** Set under {@link #lock} when a {@link #displacedOwn} offer is due; {@link #offerDisplaced} sends it after the lock. */
    private volatile boolean reofferDue;
    /** Whether {@link #displacedOwn} may hold a line: read without the lock on every state received, so almost always false. */
    private volatile boolean anyDisplacedOwn;
    /** At most this many offers of one displaced claim. */
    static final int REOFFER_MAX = 6;
    /** The first backoff between offers of a displaced claim; it doubles after each. */
    static final long REOFFER_BACKOFF_MILLIS = 1_000;

    /**
     * The targeted anti-entropy rounds a winner runs toward each holder its claim displaced, while its own take of the
     * entry is open (srad/agentspaces-event-loop.md phase 6, owner decision 4 of 2026-11-23). A displaced holder's
     * completion may have only one source, the holder itself (the writer drops it, SPEC §11a.4), and anti-entropy
     * repairs only a round's opener, so the winner opens rounds with each displaced holder rather than wait for its
     * random partner to be one: at once, then after 1, 2, 4, 8 and 16 s, at most {@link #DISPLACED_PULL_MAX} rounds,
     * ending when it learns the completion, completes the entry itself, or no longer holds the claim. No wire change:
     * a round is the usual digest. Bounded like {@link #displacedClaims}. Under {@link #lock}.
     */
    private final Map<EntryId, DisplacedPull> displacedPulls = boundedLines();
    /**
     * The earliest {@link #displacedPulls} round due, on the space's local (monotonic) base ({@link #localMillis()});
     * {@link Long#MAX_VALUE} for none. The participant converts it to the space's clock as it returns.
     */
    private final java.util.concurrent.atomic.AtomicLong pullDue = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    /**
     * When this replica re-publishes its advertisement, on the space's clock: two thirds of the advertisement TTL
     * after the last one was issued (the TTL rule's 1.5 margin, §3.11), so the advertisement never lapses while the
     * space lives; {@link Long#MAX_VALUE} while it does not advertise (srad/agentspaces-event-loop.md §10.33).
     */
    private final java.util.concurrent.atomic.AtomicLong adDue = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    /** Advertisement renewals the participant ran; package-private, for tests. */
    private final java.util.concurrent.atomic.AtomicLong adRenewals = new java.util.concurrent.atomic.AtomicLong();
    /** Targeted rounds opened toward displaced holders; package-private, for tests. */
    private final java.util.concurrent.atomic.AtomicLong displacedPullRounds = new java.util.concurrent.atomic.AtomicLong();
    /** At most this many targeted rounds per entry. */
    static final int DISPLACED_PULL_MAX = 6;

    /** The displaced holders of one entry this peer's claim won, and the targeted rounds so far. */
    private static final class DisplacedPull {
        final java.util.Set<PeerId> holders = new java.util.LinkedHashSet<>();
        int rounds;
        /** When the next round is due, on {@link #localMillis()}. */
        long nextMillis;

        DisplacedPull(long nextMillis) {
            this.nextMillis = nextMillis;
        }
    }

    /** A map in insertion order that keeps at most {@link #OUTBID_COMPLETIONS_CAP} lines, the oldest going first. */
    private static <K, V> Map<K, V> boundedLines() {
        return new java.util.LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > OUTBID_COMPLETIONS_CAP;
            }
        };
    }
    /** Keys of remote claims, and of states refused for their stamps, already rejected
     * here, so a lagging honest replica re-offering the same contested claim or state is
     * not struck once per round. Each key covers signed content alone (HLC round four,
     * 2026-11-08, decision D5). Under {@link #lock}. */
    private final Set<String> rejectedOffers = java.util.Collections.newSetFromMap(
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REJECTED_OFFERS_CAP;
                }
            });
    /** Keys of states and claims deferred here for a stamp past the far-future margin, so a
     * re-offer before the wall catches up counts once (HLC round four, 2026-11-08, decision D4).
     * Under {@link #lock}. */
    private final Set<String> deferredOffers = java.util.Collections.newSetFromMap(
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REJECTED_OFFERS_CAP;
                }
            });

    // ---- non-blocking block fetches (spec §7.2, P7)
    /** CIDs with a background fetch in flight, so a polling read does not re-send WANTs. */
    private final Set<String> fetching = ConcurrentHashMap.newKeySet();

    /**
     * Block fetches that failed and wait for their next attempt (LB-B-02): the
     * record, how many attempts ran, when the next may start, and the arrival
     * callback. Driven from the group tick, so a {@code TestClock} governs it.
     */
    private final Map<String, BlockRetry> blockRetries = new ConcurrentHashMap<>();
    /** Bulk fetches (a tree or a flat artifact) the seam gave up on, awaiting their next attempt (PD-Q-AVL-01). */
    private final Map<String, BlockRetry> bulkRetries = new ConcurrentHashMap<>();
    /** The arrival callback of a read's poll: nothing to run, so it never joins a fetch in flight. */
    private static final Runnable NO_ARRIVAL = () -> { };
    /**
     * Entries whose merge deferred an event until their payload lands ({@link #payloadArrived}). Whoever removes the key
     * raises the entry's event: the arrival, or a completion that could already decode the value. Bounded by the
     * entries held here: the collection sweep and a retry that is given up drop a key.
     */
    private final Map<EntryId, Boolean> awaitingPayload = new ConcurrentHashMap<>();

    /**
     * The earliest wall deadline the participant was told of since its last run, per kind, each lowered by whoever
     * moves a deadline earlier, which then wakes the participant (DW-2): the sweep's (under {@link #lock}), the
     * retries' (on fetch threads) and the horizon's rounds. A value that is too early costs one look, never a miss.
     */
    private final java.util.concurrent.atomic.AtomicLong sweepDue = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    private final java.util.concurrent.atomic.AtomicLong retryDue = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    private final java.util.concurrent.atomic.AtomicLong horizonDue = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    /** The view epoch the participant last swept at (DW-5: its own copy). */
    private long dueEpoch = Long.MIN_VALUE;
    /**
     * Runs of the participant that took the space's lock, for tests (EL-T-09: an idle space looks only at its
     * deadlines; the loop's backstop asks it every second, and such a run reads three volatile deadlines and returns).
     */
    private final AtomicLong dueLooks = new AtomicLong();

    private record BlockRetry(EntryRecord record, int attempts, long nextAtMillis, Runnable onArrival) {
    }

    /** Default escalation threshold, 4 MiB (srad/large-networks-bulk-data.md decision 7). */
    public static final long DEFAULT_ESCALATE_ABOVE = 4L * 1024 * 1024;

    /** Per-attempt wait for a block, then the interval before the next attempt (LB-B-02). */
    static final long[] BLOCK_FETCH_TIMEOUTS_MILLIS = {800, 2_000, 5_000, 15_000};
    static final long[] BLOCK_RETRY_DELAYS_MILLIS = {2_000, 5_000, 15_000, 60_000};

    // ---- lifecycle: advertisement, admission, read-only (spec §7.5)
    private final DiscoveryPublisher publisher;
    private final Duration advertisementTtl;
    private final AdvertisementSigner adSigner;
    /** Schema names registered through this space, for the advertisement's hints. */
    private final Set<String> schemaNames = ConcurrentHashMap.newKeySet();
    private final SpaceAdmission admission;
    /** The credential rule (issuer and index) when {@link #admission} is CREDENTIAL, else null. */
    private final SpaceAdmission.CredentialAdmission credentialRule;
    /** The schema name of {@link SpaceCredential}, registered on every replica so it decodes everywhere. */
    private final String credentialSchema;
    private volatile long founderDeadlineMillis;
    private volatile boolean readOnly;
    private volatile SignedAdvertisement<SpaceAdvertisement> advertisement;

    private ReplicatedSpace(Builder builder) {
        this.name = builder.name;
        this.runtime = builder.runtime;
        this.identity = builder.identity;
        this.writer = builder.writer != null ? builder.writer
                : builder.identity.agentIdentity(builder.agentName);
        if (!writer.id().peer().equals(builder.identity.peerId())) {
            throw new IllegalArgumentException("writer " + writer.id().encoded()
                    + " does not belong to this peer " + builder.identity.peerId().display());
        }
        this.issuer = writer.id();
        // The same canonical codec the space signs and verifies everything with.
        this.certificates = new AgentCertificates(CborCodec.defaultCodec());
        this.clock = builder.clock;
        this.handClock = ai.badmonkey.agentspaces.common.time.Waits.movedByHand(clock);
        this.waits = ai.badmonkey.agentspaces.common.time.Waits.base(clock);
        this.localTime = ai.badmonkey.agentspaces.common.time.TimeSources.of(clock);
        this.codec = CborCodec.defaultCodec();
        this.schemas = builder.schemas;
        this.settleWindow = builder.settleWindow;
        this.strategy = builder.strategy;
        this.bidFunction = builder.bidFunction;
        this.blocks = builder.blocks;
        this.bulk = builder.bulk;
        this.escalateAbove = builder.bulk == null ? 0 : builder.escalateAbove;
        this.sealBeforeEscalate = builder.sealBeforeEscalate;
        this.bulkFallback = builder.bulkFallback;
        this.bulkRequired = builder.bulkRequired;
        this.recordless = builder.recordless;
        this.maxPayload = builder.maxPayload;
        this.keyRing = builder.keyRing;
        // A key installed for a sealed payload makes an entry readable without any other change (EL-W-07).
        this.keyInstalls = keyRing == null ? () -> { } : keyRing.onInstall(held -> signalArrival());
        this.tagShard = builder.tagShard;
        this.maxLeaseOverride = builder.maxLease;
        this.publisher = builder.publisher;
        this.advertisementTtl = builder.advertisementTtl;
        this.adSigner = new AdvertisementSigner(codec);
        this.admission = builder.admission;
        this.credentialRule = builder.admission instanceof SpaceAdmission.CredentialAdmission rule
                ? rule : null;
        this.credentialSchema = schemas.register(SpaceCredential.class);
        this.founderDeadlineMillis = builder.founderLease == null
                ? Long.MAX_VALUE
                : builder.clock.instant().toEpochMilli() + builder.founderLease.toMillis();
        this.hlc = new HybridLogicalClock(builder.clock, issuer.encoded());
        this.id = SpaceId.local(runtime.id().value() + "/" + builder.name);
        this.streamId = "space:" + builder.name;
        for (Class<?> hint : builder.schemaHints) {
            registerSchema(hint);
        }

        // One replica per space name per node (QA4 A4-9): the bus refuses a
        // second registration under this stream id, so a second handle for a
        // name that is already open fails here, loudly, instead of silently
        // stealing the first handle's replication.
        this.local = builder.local;
        if (local) {
            // A writer-local space (Builder.local): nobody else reads it, so it registers nothing on the bus, no
            // rumor handler and no anti-entropy state. Its sweep runs from the tick alone.
            this.streamRegistration = () -> { };
            this.reconcileRegistration = () -> { };
        } else {
            this.streamRegistration = runtime.gossip().onStream(streamId,
                    (from, itemId, payload) -> onDelta(from, payload));
            this.reconcileRegistration = runtime.gossip().reconcile(streamId, new ReconcilableState() {
                @Override
                public byte[] digest() {
                    return ReplicatedSpace.this.digest();
                }

                @Override
                public byte[] digestHash() {
                    return ReplicatedSpace.this.digestHash();
                }

                @Override
                public boolean applyDeltaChanged(byte[] delta) {
                    return ReplicatedSpace.this.applySyncDelta(delta);
                }

                @Override
                public byte[] deltaFor(byte[] remoteDigest) {
                    return ReplicatedSpace.this.deltaFor(remoteDigest);
                }

                @Override
                public void applyDelta(byte[] delta) {
                    ReplicatedSpace.this.applySyncDelta(delta);
                }

                @Override
                public void matched(PeerId from) {
                    ReplicatedSpace.this.digestMatched(from);
                }

                // A dial-only member answers a relayed or another member's DIGEST for these entries alone (§9.4).
                @Override
                public boolean holdsDisplaced() {
                    return ReplicatedSpace.this.holdsDisplaced();
                }

                @Override
                public ReconcilableState.Bounded displacedFor(byte[] remoteDigest, long budget, boolean first) {
                    return ReplicatedSpace.this.displacedFor(remoteDigest, budget, first);
                }

                // The trie audit of SPEC annex I.6 (srad/replication-by-entry.md phase 4): the bus asks these only
                // toward a member on wire.ae=2.
                @Override
                public byte[] streamHash() {
                    return ReplicatedSpace.this.streamHash();
                }

                @Override
                public ReconcilableState.AuditNode auditNode(byte[] path) {
                    ai.badmonkey.agentspaces.space.crdt.AuditNode node = ReplicatedSpace.this.auditNode(path);
                    return new ReconcilableState.AuditNode(node.count(),
                            node.kind() == ai.badmonkey.agentspaces.space.crdt.AuditNode.Kind.INNER, node.preimage());
                }

                @Override
                public byte[] auditLines(byte[] path) {
                    return ReplicatedSpace.this.auditLines(path);
                }

                @Override
                public byte[] auditLines(byte[] path, long maxBytes) {
                    return ReplicatedSpace.this.auditLines(path, maxBytes);
                }

                @Override
                public java.util.Set<String> auditHeld(byte[] path, java.util.Collection<String> lines) {
                    return ReplicatedSpace.this.auditHeld(path, lines);
                }

                @Override
                public byte[] statesFor(java.util.Collection<String> ids) {
                    return ReplicatedSpace.this.statesFor(ids);
                }

                // The budget of annex I.6 (srad/replication-by-entry.md phase 4, step 4b): a fetch and a get-space
                // cut to the asker's budget, and a merge that names what it carried.
                @Override
                public ReconcilableState.Bounded statesFor(java.util.Collection<String> ids, long budget,
                                                           boolean first) {
                    return ReplicatedSpace.this.statesFor(ids, budget, first);
                }

                @Override
                public ReconcilableState.Bounded deltaFor(byte[] remoteDigest, long budget, boolean first) {
                    return ReplicatedSpace.this.deltaFor(remoteDigest, budget, first);
                }

                @Override
                public ReconcilableState.Applied applyDeltaNamed(byte[] delta) {
                    return ReplicatedSpace.this.applySyncDeltaNamed(delta);
                }

                @Override
                public void linesSeen(PeerId from, String lines) {
                    ReplicatedSpace.this.linesSeen(lines);
                }

                @Override
                public void matchedUnder(PeerId from, byte[] path) {
                    ReplicatedSpace.this.matchedUnder(path);
                }

                // The status figures of RE-Q-MNT-02, for a member's status page.
                @Override
                public ReconcilableState.Figures figures() {
                    return ReplicatedSpace.this.figures();
                }
            });
        }
        // Lease lapses and collections fire from the space's own participant as well as from the digest, so they keep
        // time without gossip traffic; the participant wakes only at the earliest deadline (EL-T-09).
        this.dueRegistration = runtime.onDueWall("space:" + builder.name, clock, this::runDue);
        // Spec §7.5: a member creates a space by publishing its advertisement. A writer-local space publishes none
        // (see refreshAdvertisement).
        refreshAdvertisement();
    }

    /**
     * Starts building a replicated space attached to a group.
     *
     * @param runtime   the group runtime
     * @param name      the space name within the group
     * @param identity  the local peer identity (signs entries)
     * @param agentName the local agent name writes are attributed to
     * @return the builder
     */
    public static Builder builder(GroupRuntime runtime, String name,
                                  PeerIdentity identity, String agentName) {
        return new Builder(runtime, name, identity, agentName);
    }

    /** Builder for {@link ReplicatedSpace}. */
    public static final class Builder {
        private final GroupRuntime runtime;
        private final String name;
        private final PeerIdentity identity;
        private final String agentName;
        private AgentIdentity writer;
        /** The space's clock; null for the node's own time source (EL-C-06), resolved by {@link #build()}. */
        private InstantSource clock;
        private SchemaRegistry schemas = new SimpleSchemaRegistry();
        private Duration settleWindow = Duration.ofMillis(200);
        private ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy =
                ai.badmonkey.agentspaces.api.space.ConflictStrategyType.LEASE_RACE;
        private java.util.function.ToDoubleFunction<Object> bidFunction;
        private ai.badmonkey.agentspaces.peering.blocks.BlockExchange blocks;
        private ai.badmonkey.agentspaces.api.spi.BulkPayloads bulk;
        private long escalateAbove = DEFAULT_ESCALATE_ABOVE;
        private boolean sealBeforeEscalate = true;
        private ai.badmonkey.agentspaces.api.spi.BulkPayloads bulkFallback;
        private boolean bulkRequired;
        private boolean recordless = true;
        private long maxPayload;
        private ai.badmonkey.agentspaces.common.crypto.GroupKeyRing keyRing;
        private java.util.function.Predicate<Map<String, String>> tagShard;
        private Duration maxLease;
        private boolean local;
        private DiscoveryPublisher publisher;
        private Duration advertisementTtl = DEFAULT_ADVERTISEMENT_TTL;
        private final List<Class<?>> schemaHints = new ArrayList<>();
        private SpaceAdmission admission = SpaceAdmission.group();
        private Duration founderLease;

        private Builder(GroupRuntime runtime, String name, PeerIdentity identity, String agentName) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.name = Objects.requireNonNull(name, "name");
            this.identity = Objects.requireNonNull(identity, "identity");
            this.agentName = Objects.requireNonNull(agentName, "agentName");
        }

        /**
         * Who this handle signs records as (spec §4.2, QA4 A4-7). Defaults to the
         * peer-signed identity for the builder's agent name, which is exactly the
         * behaviour every existing caller gets. Give a subordinate identity
         * ({@code identity.subordinate(name)}) and every record this handle writes
         * carries the agent's own signature and the peer's certificate for it, so
         * readers see the entry as {@code AGENT_ATTESTED}. The writer must belong
         * to this peer.
         *
         * @param writer the signing identity
         * @return this builder
         */
        public Builder writer(AgentIdentity writer) {
            this.writer = Objects.requireNonNull(writer, "writer");
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
         * Uses a specific schema registry.
         *
         * @param schemas the registry
         * @return this builder
         */
        public Builder schemaRegistry(SchemaRegistry schemas) {
            this.schemas = Objects.requireNonNull(schemas, "schemas");
            return this;
        }

        /**
         * Sets the LEASE_RACE settle window (default 200 ms, under one 250 ms tick;
         * {@code Duration.ZERO} for deterministic single-threaded tests).
         *
         * @param settleWindow the window
         * @return this builder
         */
        public Builder settleWindow(Duration settleWindow) {
            this.settleWindow = Objects.requireNonNull(settleWindow, "settleWindow");
            return this;
        }

        /**
         * Declares the space's conflict strategy for the space's own
         * {@code take} (spec §7.4). {@code LEASE_RACE} is the default and
         * {@code AUCTION} requires a {@link #bidFunction}; both are decided in
         * the space's claim lattice. {@code ORDERED} is not a space strategy:
         * exactly-once takes are driven by the ordered-log capability's
         * {@code OrderedTakes} coordinator over a Raft quorum, not by
         * {@code Space.take}, so it is rejected here. Build the space with the
         * default strategy and take through {@code OrderedTakes} instead.
         *
         * @param strategy the strategy
         * @return this builder
         */
        public Builder strategy(ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy) {
            if (strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.ORDERED) {
                throw new UnsupportedOperationException(
                        "ORDERED takes are driven by the ordered-log capability's "
                                + "OrderedTakes coordinator, not by a space strategy; "
                                + "build with the default strategy and take through it");
            }
            this.strategy = Objects.requireNonNull(strategy, "strategy");
            return this;
        }

        /**
         * Supplies the cost function for {@code AUCTION} takes: given the entry
         * value, return this agent's cost; the lowest bid across the fleet wins.
         *
         * @param bidFunction the cost function
         * @return this builder
         */
        public Builder bidFunction(java.util.function.ToDoubleFunction<Object> bidFunction) {
            this.bidFunction = Objects.requireNonNull(bidFunction, "bidFunction");
            return this;
        }

        /**
         * Escalates payloads above a threshold to a bulk-payload seam (the content
         * swarm), so an entry of any size travels as a reference (LB-E-02). Between
         * the inline limit and the threshold the block exchange carries the payload
         * as before.
         *
         * @param bulk          the seam
         * @param escalateAbove payload bytes above which a write escalates; 0 disables
         * @return this builder
         */
        /**
         * Refuses a payload above {@code bytes} at write time when no content swarm is attached
         * (srad/large-networks-bulk-data.md LB-E-06), naming the property that would carry it, as
         * a payload above the block cap is refused without a block exchange. 0, the default, keeps
         * 0.4.0's behaviour: a payload travels as one block up to the block cap. Ignored once
         * {@link #bulk} is set, since the swarm carries any size.
         *
         * @param bytes the cap in bytes; 0 disables
         * @return this builder
         */
        public Builder maxPayload(long bytes) {
            this.maxPayload = bytes;
            return this;
        }

        public Builder bulk(ai.badmonkey.agentspaces.api.spi.BulkPayloads bulk, long escalateAbove) {
            this.bulk = Objects.requireNonNull(bulk, "bulk");
            this.escalateAbove = escalateAbove;
            return this;
        }

        /**
         * Whether a payload is sealed under the group key before it goes to the bulk seam
         * (srad/agentspaces-pdt.md PD-E-04). True by default, today's rule for the flat
         * content swarm, which carries opaque bytes. A space over the persistent trees
         * sets it false: the seam spills the plaintext CBOR into a tree so that unchanged
         * sub-trees are shared between versions, the nodes travel sealed per message in
         * {@code group-key} mode, the record still names the write epoch in
         * {@code keyEpoch}, and the record signature over {@code payloadRef} binds the
         * tree. A reader tells the two forms apart by the seam that accepts the reference.
         *
         * @param seal whether to seal before escalating
         * @return this builder
         */
        public Builder sealBeforeEscalate(boolean seal) {
            this.sealBeforeEscalate = seal;
            return this;
        }

        /**
         * The seam a write takes when the primary seam of {@link #bulk} cannot reach every
         * current member (PD-E-06: {@code BulkPayloads.unable} names one); without it the
         * write takes the block path, or inline, as a space without a seam does. A payload
         * through the fallback is sealed as today, whatever {@link #sealBeforeEscalate} says.
         *
         * @param fallback the secondary seam (the flat {@code SpacePayloads})
         * @return this builder
         */
        public Builder bulkFallback(ai.badmonkey.agentspaces.api.spi.BulkPayloads fallback) {
            this.bulkFallback = Objects.requireNonNull(fallback, "fallback");
            return this;
        }

        /**
         * Whether a write the primary seam cannot carry is refused with a message naming
         * the members that lack it, instead of falling back (PD-E-06,
         * {@code agentspaces.pdt.spaces.<name>.require}).
         *
         * @param required whether to refuse
         * @return this builder
         */
        public Builder bulkRequired(boolean required) {
            this.bulkRequired = required;
            return this;
        }

        /**
         * Whether this replica omits the record from a renew, cancel, or complete once it
         * has evidence the record replicated and every member of its view advertises the
         * {@code wire.recordless} hint (srad/agentspaces-pdt.md §3.9, PD-L-01, PD-L-03).
         * On by default; off, every state carries its record as in 0.5.0. A receiver
         * accepts the recordless form either way.
         *
         * @param enabled whether to send recordless states
         * @return this builder
         */
        public Builder recordless(boolean enabled) {
            this.recordless = enabled;
            return this;
        }

        /**
         * Attaches a block exchange so payloads above the inline limit are
         * content-addressed and fetched out of band (spec §7.1).
         *
         * @param blocks the group's block exchange
         * @return this builder
         */
        public Builder blocks(ai.badmonkey.agentspaces.peering.blocks.BlockExchange blocks) {
            this.blocks = Objects.requireNonNull(blocks, "blocks");
            return this;
        }

        /**
         * Encrypts entry payloads with a symmetric group content key (spec §11):
         * gossip and anti-entropy then carry ciphertext, signatures cover the
         * ciphertext (so keyless replicas still verify and relay records), and
         * only replicas holding the key can read entry contents.
         *
         * @param groupKey the group content key
         * @return this builder
         */
        public Builder groupKey(ai.badmonkey.agentspaces.common.crypto.GroupKey groupKey) {
            this.keyRing = ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(
                    Objects.requireNonNull(groupKey, "groupKey"));
            return this;
        }

        /**
         * Encrypts payloads under a content-key ring (SPEC §11a.3, v0.1.13):
         * writers seal under the ring's epoch in effect, readers open with the
         * epoch the record names. Spaces of one group share the group's ring, so
         * a rotation reaches all of them at once.
         *
         * @param keyRing the group's ring
         * @return this builder
         */
        public Builder keyRing(ai.badmonkey.agentspaces.common.crypto.GroupKeyRing keyRing) {
            this.keyRing = Objects.requireNonNull(keyRing, "keyRing");
            return this;
        }

        /**
         * Makes this replica partial (spec §7.5): it stores, matches, and serves
         * only entries whose tags satisfy the shard predicate. Out-of-shard
         * entries seen in deltas are acknowledged in digests but never stored.
         * This replica's own writes are always stored, whatever their tags.
         *
         * @param tagShard the shard predicate over an entry's tags
         * @return this builder
         */
        public Builder tagShard(java.util.function.Predicate<Map<String, String>> tagShard) {
            this.tagShard = Objects.requireNonNull(tagShard, "tagShard");
            return this;
        }

        /**
         * Fixes the maximum lease the tombstone garbage collector reckons with
         * (spec §7.3) instead of the largest lease this replica has observed.
         * Completed, withdrawn, and lapsed entries are dropped once their lease
         * expired more than twice this long ago.
         *
         * @param maxLease the maximum lease
         * @return this builder
         */
        public Builder maxLease(Duration maxLease) {
            this.maxLease = Objects.requireNonNull(maxLease, "maxLease");
            return this;
        }

        /**
         * Makes the space writer-local: a space only its own writer reads, such as a feed's scratch positions or a
         * cache one agent keeps for itself. It does not register with the group's gossip bus, so it has no
         * anti-entropy digest, joins no probe, and its writes, renewals, cancels, takes, and completions publish no
         * rumor; it costs the group nothing (reviews/IDLE-WORK-2026-11-13.md item 15). It publishes no space
         * advertisement either, even with {@link #advertise} set, since an advertisement says the space replicates,
         * and a {@code FRESH} read starts no anti-entropy round. Everything else is as on a
         * replicated space: leases, events, the tombstone sweep (run from the group tick), takes, and every handle
         * of this replica on this node (see {@link ReplicatedSpace#as}). Other members never see its entries, and a
         * member that opens a replicated space of the same name replicates without it. A second handle for the same
         * name on this node is not refused, since the refusal lives in the bus registration a local space skips.
         *
         * @return this builder
         */
        public Builder local() {
            this.local = true;
            return this;
        }

        /**
         * Publishes a signed {@link SpaceAdvertisement} for the space on build
         * and on every {@link ReplicatedSpace#refreshAdvertisement()} (spec
         * §7.5): the space's name, schema hints, strategy, admission rule, and
         * replication mode, signed by the founding peer.
         *
         * @param publisher where advertisements go ({@code DiscoveryService::publish})
         * @return this builder
         */
        public Builder advertise(DiscoveryPublisher publisher) {
            this.publisher = Objects.requireNonNull(publisher, "publisher");
            return this;
        }

        /**
         * Sets the TTL of the published advertisement; the founders' refresh
         * loop must republish within it. Default fifteen minutes.
         *
         * @param ttl the advertisement time-to-live
         * @return this builder
         */
        public Builder advertisementTtl(Duration ttl) {
            this.advertisementTtl = Objects.requireNonNull(ttl, "ttl");
            return this;
        }

        /**
         * Declares entry types up front so the first advertisement already
         * carries their schema hints; types written later are added on refresh.
         *
         * @param types the entry classes
         * @return this builder
         */
        public Builder schemaHints(Class<?>... types) {
            for (Class<?> type : types) {
                schemaHints.add(Objects.requireNonNull(type, "type"));
            }
            return this;
        }

        /**
         * Sets the space's admission rule by name (spec §7.5), delegating to
         * {@link #admission(SpaceAdmission)}. Under
         * {@link SpaceAdvertisement.Admission#ALLOWLIST} only the listed agents
         * may write, take, or complete: a local caller outside the list gets a
         * {@link SpaceAdmissionException}, and an inbound record or claim from an
         * unlisted agent is dropped and reported as misbehavior. Everyone in the
         * group may still read. {@link SpaceAdvertisement.Admission#GROUP}, the
         * default, admits every group member and ignores the list.
         * {@link SpaceAdvertisement.Admission#CREDENTIAL} makes this node the
         * credential issuer (the founder's configuration) and ignores the list;
         * {@link SpaceAdvertisement.Admission#AUTHORIZER} needs an
         * {@code Authorizer} and must be built with
         * {@link SpaceAdmission#authorizer}.
         *
         * @param admission the rule
         * @param allowed   the admitted agents under ALLOWLIST
         * @return this builder
         */
        public Builder admission(SpaceAdvertisement.Admission admission, Set<AgentId> allowed) {
            Objects.requireNonNull(admission, "admission");
            Set<AgentId> agents = Set.copyOf(Objects.requireNonNull(allowed, "allowed"));
            return admission(switch (admission) {
                case GROUP -> SpaceAdmission.group();
                case ALLOWLIST -> SpaceAdmission.allowlist(agents);
                case CREDENTIAL -> SpaceAdmission.credentials(identity.peerId(), new CredentialIndex());
                case AUTHORIZER -> throw new IllegalArgumentException(
                        "AUTHORIZER admission needs an Authorizer: build it with "
                                + "admission(SpaceAdmission.authorizer(authorizer, spaceName))");
            });
        }

        /**
         * Sets the space's admission rule (spec §7.5, TECH-SPEC §7.10). The rule
         * is asked on every local write, take, and completion and on every
         * inbound record or claim after its signature verifies: a refused
         * local caller gets a {@link SpaceAdmissionException}; a refused remote
         * delta is dropped, and struck as misbehavior only when the rule says a
         * refusal cannot be a late credential
         * ({@link SpaceAdmission#strikesOnRefusal()}). Under
         * {@link SpaceAdmission#credentials} this replica indexes the issuer's
         * {@link SpaceCredential} entries as they merge, lapse, and are
         * cancelled, and the issuer's own node may {@link ReplicatedSpace#grant}
         * and {@link ReplicatedSpace#revoke}. Reads stay open to the group.
         *
         * @param admission the rule
         * @return this builder
         */
        public Builder admission(SpaceAdmission admission) {
            this.admission = Objects.requireNonNull(admission, "admission");
            return this;
        }

        /**
         * Leases the space itself to its founders (spec §7.5): once this long
         * has passed on the space's clock the space becomes read-only, so
         * writes and takes throw {@link SpaceReadOnlyException} while reads keep
         * working. {@link ReplicatedSpace#renewFounderLease} extends it.
         *
         * @param founderLease how long the founders hold the space
         * @return this builder
         */
        public Builder founderLease(Duration founderLease) {
            this.founderLease = Objects.requireNonNull(founderLease, "founderLease");
            return this;
        }

        /** Builds and attaches the space. A bid function may also arrive later
         * via {@link ReplicatedSpace#bidFunction}, e.g. from an agent binder. */
        public ReplicatedSpace build() {
            if (clock == null) {
                // A node passes its own time source down (EL-C-06); a node on a plain test clock keeps the system's.
                clock = runtime == null ? InstantSource.system() : runtime.defaultClock();
            }
            return new ReplicatedSpace(this);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public java.util.Optional<AgentId> writer() {
        return java.util.Optional.of(issuer);
    }

    /**
     * A per-agent view of this replica (SPEC §4.2, QA4 A4-7 phase 3): the same
     * space — one replica, one gossip registration, one clock per node — seen
     * through another agent's identity. Records the view writes are issued and
     * signed by {@code actor} (attested when it is a subordinate identity), take
     * claims and completions through it name {@code actor} as the holder, and
     * admission is judged for {@code actor}, so two views of one replica can be
     * admitted differently under ALLOWLIST or CREDENTIAL. Reads and
     * subscriptions are the replica's own. The view has no resources of its own:
     * closing this space ends every view, and views do not close.
     *
     * @param actor the agent the view acts as; must belong to this peer
     * @return the view
     * @throws IllegalArgumentException when the actor is another peer's agent
     */
    public Space as(AgentIdentity actor) {
        Objects.requireNonNull(actor, "actor");
        if (!actor.id().peer().equals(identity.peerId())) {
            throw new IllegalArgumentException("agent " + actor.id().encoded()
                    + " does not belong to this peer " + identity.peerId().display());
        }
        return actor.id().equals(issuer) && actor == writer ? this : new View(actor);
    }

    /** The delegating view: identity differs, everything else is the replica. */
    private final class View implements Space {

        private final AgentIdentity actor;

        private View(AgentIdentity actor) {
            this.actor = actor;
        }

        ReplicatedSpace owner() {
            return ReplicatedSpace.this;
        }

        @Override
        public long changeSignals() {
            return ReplicatedSpace.this.changeSignals();
        }

        @Override
        public void awaitChange(long seen, Duration max) throws InterruptedException {
            ReplicatedSpace.this.awaitChange(seen, max);
        }

        @Override
        public AutoCloseable onChangeSignal(Runnable listener) {
            return ReplicatedSpace.this.onChangeSignal(listener);
        }

        @Override
        public String name() {
            return ReplicatedSpace.this.name();
        }

        @Override
        public SpaceId id() {
            return ReplicatedSpace.this.id();
        }

        @Override
        public java.util.Optional<AgentId> writer() {
            return java.util.Optional.of(actor.id());
        }

        @Override
        public <T> EntryHandle write(T entry, Lease lease) {
            return writeAs(actor, entry, lease, Map.of());
        }

        @Override
        public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
            return writeAs(actor, entry, lease, tags);
        }

        @Override
        public <T> Optional<T> read(Template<T> template) {
            return ReplicatedSpace.this.read(template);
        }

        @Override
        public <T> Optional<T> read(Template<T> template, Duration timeout) {
            return ReplicatedSpace.this.read(template, timeout);
        }

        @Override
        public <T> List<T> readAll(Template<T> template, int limit) {
            return ReplicatedSpace.this.readAll(template, limit);
        }

        @Override
        public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
            return ReplicatedSpace.this.readAllIssued(template, limit);
        }

        @Override
        public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease,
                                                Duration timeout) {
            return takeAs(actor, template, takeLease, timeout);
        }

        @Override
        public <T> List<Entry<T>> readAllEntries(Template<T> template, int limit) {
            return ReplicatedSpace.this.readAllEntries(template, limit);
        }

        @Override
        public List<EntryId> undecodableEntries(Template<?> template, int limit) {
            return ReplicatedSpace.this.undecodableEntries(template, limit);
        }

        @Override
        public void complete(TakenEntry<?> taken) {
            completeInternal(actor, taken, null, null, Map.of());
        }

        @Override
        public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
            return complete(taken, result, resultLease, Map.of());
        }

        @Override
        public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease,
                                        Map<String, String> tags) {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(resultLease, "resultLease");
            Objects.requireNonNull(tags, "tags");
            return completeInternal(actor, taken, result, resultLease, tags);
        }

        @Override
        public <T> Subscription notify(Template<T> template, SpaceListener<T> listener,
                                       Lease lease) {
            return ReplicatedSpace.this.notify(template, listener, lease);
        }

        @Override
        public String toString() {
            return "ReplicatedSpace[" + name + "] as " + actor.id().encoded();
        }
    }

    @Override
    public SpaceId id() {
        return id;
    }

    /** Returns the space's declared conflict strategy. */
    public ai.badmonkey.agentspaces.api.space.ConflictStrategyType strategy() {
        return strategy;
    }

    /**
     * Replaces the AUCTION bid function (for binder-driven wiring).
     *
     * @param bidFunction the cost function
     */
    public void bidFunction(java.util.function.ToDoubleFunction<Object> bidFunction) {
        Objects.requireNonNull(bidFunction, "bidFunction");
        // QA4 A4-8: a space consults one cost function. This used to be a plain
        // setter, so a second bidder on the same peer silently displaced the
        // first and the first never priced anything. Single-assignment; the
        // builder's value counts as the first.
        if (this.bidFunction != null) {
            throw new IllegalStateException("space '" + name + "' already has a bid function;"
                    + " one bid function per space per peer — a second bidder belongs on its"
                    + " own peer");
        }
        this.bidFunction = bidFunction;
    }

    // ------------------------------------------------------------ lifecycle (§7.5)

    /**
     * Re-publishes the space's signed advertisement with a fresh issue instant
     * and the schema hints registered so far (spec §7.5). The founders' refresh
     * loop calls this within the advertisement TTL. A no-op returning empty
     * when the space was built without {@link Builder#advertise} or has become
     * read-only: letting the advertisement lapse is how founders release a
     * space. Also a no-op on a writer-local space ({@link Builder#local()}):
     * every advertisement says the space replicates (FULL or TAG_SHARDED), so
     * advertising a space nobody else can read would send members to a replica
     * that never answers, and refreshing it would cost the group traffic for
     * nothing.
     *
     * @return the advertisement published, when one was
     */
    public Optional<SignedAdvertisement<SpaceAdvertisement>> refreshAdvertisement() {
        if (publisher == null || local || readOnly()) {
            return Optional.empty();
        }
        List<String> hints = new ArrayList<>(schemaNames);
        java.util.Collections.sort(hints);
        SpaceAdvertisement ad = new SpaceAdvertisement(
                "aspace://" + runtime.id().value() + "/" + name, identity.peerId(),
                runtime.id(), clock.instant(), advertisementTtl, name, hints, strategy,
                admission.rule(),
                tagShard == null ? SpaceAdvertisement.Replication.FULL
                        : SpaceAdvertisement.Replication.TAG_SHARDED);
        SignedAdvertisement<SpaceAdvertisement> signed = adSigner.sign(ad, identity);
        advertisement = signed;
        publisher.publish(signed);
        // §10.33: the space's own participant renews it before it lapses; until 0.7.0 nothing did, and a space left
        // every discovery cache (semantic discovery, the console) 15 minutes after it was built
        adDue.set(Long.MAX_VALUE);
        lowerDue(adDue, ad.issued().toEpochMilli() + Math.max(1, advertisementTtl.toMillis() * 2 / 3));
        return Optional.of(signed);
    }

    /** Returns the most recently published advertisement, when the space advertises. */
    public Optional<SignedAdvertisement<SpaceAdvertisement>> advertisement() {
        return Optional.ofNullable(advertisement);
    }

    /** Returns the space's advertised admission rule (spec §7.5). */
    public SpaceAdvertisement.Admission admission() {
        return admission.rule();
    }

    /** Returns the admission rule object the space asks (spec §7.5, TECH-SPEC §7.10). */
    public SpaceAdmission admissionRule() {
        return admission;
    }

    /** Returns the agents admitted under {@code ALLOWLIST}; empty under every other rule. */
    public Set<AgentId> allowedAgents() {
        return admission instanceof SpaceAdmission.AllowlistAdmission allowlist
                ? allowlist.agents() : Set.of();
    }

    /** Returns the credential index this replica maintains under {@code CREDENTIAL} admission. */
    public Optional<CredentialIndex> credentialIndex() {
        return Optional.ofNullable(credentialRule).map(SpaceAdmission.CredentialAdmission::index);
    }

    /**
     * Issues a space credential (spec §7.5): writes a {@link SpaceCredential}
     * entry admitting {@code agent} for {@code scopes}, with a write lease of
     * {@code validity} that is the credential's expiry. Because it is an
     * ordinary signed entry it replicates like any write, and a replica admits
     * the agent once the entry is present there. The returned handle renews or
     * cancels the credential like any entry; a credential nobody renews lapses
     * on its own. Only the configured credential issuer's node may grant.
     *
     * @param agent    the agent to admit
     * @param scopes   the scopes granted
     * @param validity how long the credential lives unless renewed
     * @return the credential entry's handle
     * @throws SpaceAdmissionException when this space does not admit by
     *                                 credential or this node is not its issuer
     */
    public EntryHandle grant(AgentId agent, Set<SpaceAdmission.Scope> scopes, Duration validity) {
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(scopes, "scopes");
        Objects.requireNonNull(validity, "validity");
        requireIssuer("grant");
        return write(new SpaceCredential(agent, scopes), Lease.of(validity));
    }

    /**
     * Withdraws every live credential this replica holds for an agent (spec
     * §7.5) by cancelling the credential entries. Cancellation replicates like
     * any removal, so the revocation is fleet-wide within one gossip round.
     * Only the configured credential issuer's node may revoke.
     *
     * @param agent the agent whose credentials are withdrawn
     * @throws SpaceAdmissionException when this space does not admit by
     *                                 credential or this node is not its issuer
     */
    public void revoke(AgentId agent) {
        Objects.requireNonNull(agent, "agent");
        requireIssuer("revoke");
        List<EntryId> credentials;
        synchronized (lock) {
            credentials = credentialRule.index().entriesFor(agent, nowMillis());
        }
        for (EntryId entryId : credentials) {
            new Handle(entryId, writer).cancel();
        }
    }

    private void requireIssuer(String operation) {
        if (credentialRule == null) {
            throw new SpaceAdmissionException("space '" + name
                    + "' does not admit by credential; " + operation + " refused");
        }
        if (!credentialRule.issuer().equals(identity.peerId())) {
            throw new SpaceAdmissionException("peer " + identity.peerId().display()
                    + " is not the credential issuer " + credentialRule.issuer().display()
                    + " of space '" + name + "'; " + operation + " refused");
        }
    }

    /**
     * Whether the founders' lease on the space has lapsed on this replica's
     * clock (spec §7.5). Always {@code false} when no founder lease was set.
     *
     * @return {@code true} once the deadline has passed
     */
    public boolean founderLeaseExpired() {
        return nowMillis() >= founderDeadlineMillis;
    }

    /** Whether the space refuses writes and takes: founder lease lapsed or marked read-only. */
    public boolean readOnly() {
        return readOnly || founderLeaseExpired();
    }

    /** Makes the space read-only now (spec §7.5); reads keep working. Irreversible. */
    public void markReadOnly() {
        readOnly = true;
    }

    /**
     * Extends the founders' lease on the space from now.
     *
     * @param extension how much longer the founders hold the space
     * @throws SpaceReadOnlyException when the space is already read-only
     */
    public void renewFounderLease(Duration extension) {
        Objects.requireNonNull(extension, "extension");
        if (readOnly()) {
            throw new SpaceReadOnlyException("space '" + name + "' is read-only; its founder lease cannot be renewed");
        }
        founderDeadlineMillis = nowMillis() + extension.toMillis();
    }

    /**
     * Applies lease lapses now (spec §7.2, §7.3): fires {@code EXPIRED} for
     * entries whose write lease lapsed and {@code REAPPEARED} for entries whose
     * take claim lapsed without completion, once per transition, and collects
     * tombstones past their horizon. The group tick and every
     * anti-entropy digest run the same sweep whenever something is due; call
     * this to sweep at once.
     */
    public void sweepNow() {
        List<Runnable> events;
        synchronized (lock) {
            long now = nowMillis();
            refileIfHorizonMoved(now);
            events = sweepLocked(now);
        }
        events.forEach(Runnable::run);
    }

    /**
     * Runs the sweep when something is due, and otherwise only looks at the earliest due time of the expiry index
     * (IDLE-WORK item 4): the digest, the delta, and the group tick call this, so a space with nothing due costs
     * O(1) per probe instead of a pass over every entry it has ever held.
     */
    void sweepIfDue() { // package-private for tests
        List<Runnable> events;
        SweepAudit audit = sweepAudit;
        synchronized (lock) {
            long now = nowMillis();
            refileIfHorizonMoved(now);
            SweepActions planned = audit == null ? null : fullScanPlan(now);
            if (now >= indexedAtMillis && now < nextDueLocked()) {
                if (audit != null) {
                    audit.swept(now, planned, SweepActions.NONE); // a skipped sweep must be one with nothing to do
                }
                return;
            }
            events = sweepLocked(now);
            if (audit != null) {
                audit.swept(now, planned, lastSweep);
            }
        }
        events.forEach(Runnable::run);
    }

    /**
     * A test's view of every sweep the tick, the digest and the delta start: what a full pass would have done at that
     * instant and what the indexed sweep did, {@link SweepActions#NONE} when it skipped. The property test holds the
     * two equal on every such sweep, not only at its own check points, so a sweep skipped or run late in between is
     * caught where it happens. Called under {@link #lock}; it must only record.
     */
    interface SweepAudit {
        void swept(long now, SweepActions planned, SweepActions done);
    }

    /** Set by tests only; null in production, where it costs one volatile read per sweep. */
    private volatile SweepAudit sweepAudit;

    /** Installs the test's sweep audit (see {@link SweepAudit}). */
    void auditSweeps(SweepAudit audit) { // package-private for tests
        this.sweepAudit = audit;
    }

    /** How many take claims this replica holds, its own and its partners'; for tests and the console. */
    public int claimsHeld() {
        synchronized (lock) {
            return crdt.claimCount();
        }
    }

    /** How many block fetches wait for their next attempt (LB-B-02); for tests and the console. */
    public int pendingBlockRetries() {
        return blockRetries.size();
    }

    /** The maximum lease the tombstone collector reckons with right now, in milliseconds. */
    public long maxLeaseMillis() {
        return maxLeaseOverride != null ? maxLeaseOverride.toMillis() : maxLeaseObservedMillis;
    }

    /**
     * Where this space stands on the per-entry horizon of SPEC §7.3 (RE-H-04). The space reads it from its group's
     * membership view and never from a caller.
     */
    public enum HorizonMode {
        /** This node keeps the legacy rule, or some member of the view lacks {@code wire.horizon=1}. */
        LEGACY,
        /** Every member advertises the hint, but not yet for the rounds the hysteresis asks (RE-Q-REL-09). */
        WAITING,
        /**
         * Engaged, and waiting one maximum lease plus the five-minute grace before withdrawals use the per-entry
         * horizon, so that every copy a member renewed while its card in this view lacked the hint (a node that
         * restarts on a release with the hint under the same peer id) has lapsed and is past its own lapse horizon
         * first. The legacy rule still applies. A writer that was not in this view at all is judged per state instead:
         * it must have been in view and hinted as long as this wait before its withdrawals use the per-entry horizon.
         */
        SETTLING,
        /**
         * A withdrawn entry is collectable {@code max(5 min, MAX_DRIFT)} after its own lease expiry, ten minutes with
         * the ceiling of SPEC §11.
         */
        PER_ENTRY
    }

    /**
     * The per-entry horizon's mode as last judged (RE-H-04, RE-F-03); a status figure. The rule a sweep or the door
     * applies is judged afresh at that moment, so an unhinted member that joined since disengages it there at once.
     *
     * @return the mode
     */
    public HorizonMode horizonMode() {
        return horizonMode;
    }

    /** How many times the per-entry horizon engaged here (entered {@link HorizonMode#SETTLING}). */
    public long horizonEngagements() {
        return horizonEngagements.get();
    }

    /** How many times the per-entry horizon came into force here (entered {@link HorizonMode#PER_ENTRY}). */
    public long horizonSettlements() {
        return horizonSettlements.get();
    }

    /** How many times an unhinted member in view disengaged the per-entry horizon here, from settling or in force. */
    public long horizonDisengagements() {
        return horizonDisengagements.get();
    }

    /** The rounds the view must stay fully hinted before the per-entry horizon engages. */
    public int horizonEngageRounds() {
        return horizonEngageRounds;
    }

    /**
     * Sets the rounds the view must stay fully hinted before the per-entry horizon engages (RE-Q-REL-09); a round is
     * one gossip period of the group on this space's clock. Takes effect at the next judgement; a streak already past
     * the new bound engages then.
     *
     * @param rounds from {@link #HORIZON_ENGAGE_ROUNDS_FLOOR} to {@link #HORIZON_ENGAGE_ROUNDS_CEILING}
     * @throws IllegalArgumentException when out of bounds
     */
    public void horizonEngageRounds(int rounds) {
        if (rounds < HORIZON_ENGAGE_ROUNDS_FLOOR || rounds > HORIZON_ENGAGE_ROUNDS_CEILING) {
            throw new IllegalArgumentException("horizon engage rounds must be from " + HORIZON_ENGAGE_ROUNDS_FLOOR
                    + " to " + HORIZON_ENGAGE_ROUNDS_CEILING + ", not " + rounds);
        }
        this.horizonEngageRounds = rounds;
    }

    /**
     * Judges the collection rule this replica may apply at {@code now} (RE-H-04, RE-Q-REL-09), and moves the mode:
     * <ul>
     *   <li>An unhinted member in view, or this node on the legacy setting, disengages at once and resets the streak.
     *       Safety comes first: the per-entry horizon rests on every writer renewing by the rule.</li>
     *   <li>A fully hinted view counts one round per judgement at least one gossip period after the last counted
     *       one, and engages once it has counted {@link #horizonEngageRounds} of them, so a card that flaps does not
     *       move the mode each time it does.</li>
     *   <li>Once engaged, the rule comes into force a maximum lease plus the lapse grace later, and stays until an
     *       unhinted member appears.</li>
     * </ul>
     * Called by the sweep and the door. The lone node, whose view is empty, judges by its own setting. The mode is
     * the view's half of the precondition; the writer's half is judged per state ({@code ruleFor}), since a copy can
     * come from a writer this view does not hold.
     */
    private SpaceStateCrdt.Horizon horizon(long now) {
        if (!runtime.viewAdvertisesHorizon()) {
            if (horizonMode != HorizonMode.LEGACY) {
                disengageHorizon();
            }
            return SpaceStateCrdt.Horizon.LEGACY;
        }
        synchronized (horizonLock) {
            HorizonMode mode = horizonMode;
            if (mode == HorizonMode.LEGACY || mode == HorizonMode.WAITING) {
                long round = Math.max(1, runtime.advertisement().gossip().period().toMillis());
                if (horizonCountedAtMillis == Long.MIN_VALUE || now < horizonCountedAtMillis) {
                    horizonCountedAtMillis = now; // the first hinted judgement, or a clock that stepped back
                } else if (now - horizonCountedAtMillis >= round) {
                    horizonStreak++;
                    horizonCountedAtMillis = now;
                }
                if (horizonStreak >= horizonEngageRounds) {
                    horizonEngagedAtMillis = now;
                    horizonMode = mode = HorizonMode.SETTLING;
                    horizonEngagements.incrementAndGet();
                    LOG.log(System.Logger.Level.DEBUG, () -> "space " + name + ": every member advertises "
                            + "wire.horizon=1 for " + horizonStreak + " rounds; the per-entry horizon engages, in "
                            + "force after one maximum lease and the grace");
                } else {
                    horizonMode = mode = HorizonMode.WAITING;
                }
            }
            if (mode == HorizonMode.SETTLING) {
                long settle = Math.max(0, maxLeaseMillis()) + SpaceStateCrdt.LAPSED_GRACE_MILLIS;
                if (now < horizonEngagedAtMillis) {
                    horizonEngagedAtMillis = now; // the clock stepped back: the wait counts from here
                } else if (now - horizonEngagedAtMillis >= settle) {
                    horizonMode = mode = HorizonMode.PER_ENTRY;
                    horizonSettlements.incrementAndGet();
                    horizonRefile = true; // tombstones are due earlier now than the index filed them
                }
            }
            lowerDue(horizonDue, horizonDeadlineLocked(mode));
            return mode == HorizonMode.PER_ENTRY ? SpaceStateCrdt.Horizon.PER_ENTRY : SpaceStateCrdt.Horizon.LEGACY;
        }
    }

    /**
     * When the horizon next moves by time alone: the next counted round while WAITING, the end of the settle while
     * SETTLING, otherwise never (a view change wakes the participant). Callers hold {@link #horizonLock}.
     */
    private long horizonDeadlineLocked(HorizonMode mode) {
        if (mode == HorizonMode.WAITING && horizonCountedAtMillis != Long.MIN_VALUE) {
            return horizonCountedAtMillis + Math.max(1, runtime.advertisement().gossip().period().toMillis());
        }
        if (mode == HorizonMode.SETTLING) {
            return horizonEngagedAtMillis + Math.max(0, maxLeaseMillis()) + SpaceStateCrdt.LAPSED_GRACE_MILLIS;
        }
        return Long.MAX_VALUE;
    }

    /** Lowers one of the participant's deadlines, and wakes it when that moved the deadline earlier (DW-2). */
    private void lowerDue(java.util.concurrent.atomic.AtomicLong due, long atMillis) {
        if (atMillis == Long.MAX_VALUE) {
            return;
        }
        long before = due.getAndAccumulate(atMillis, Math::min);
        if (atMillis < before && dueRunner != Thread.currentThread()) { // the participant reads it as it returns
            ai.badmonkey.agentspaces.api.spi.DueWork.Registration registration = dueRegistration;
            if (registration != null) {
                registration.wake(); // never blocks, so a caller under the space's lock may call it
            }
        }
    }

    /**
     * The space's participant (srad/agentspaces-event-loop.md EL-T-09, EL-L-03). A direct run ({@code tick()})
     * sweeps and retries as the tick adapters did. On the running loop it takes the space's lock only when something
     * is due: the sweep's deadline passed, the view's epoch moved (the horizon and writer stays read it), or the
     * horizon's round or settle came due; the retries run only when one is due. It returns the earliest of the three
     * deadlines on the space's clock.
     */
    private long runDue(long nowMillis) {
        dueRunner = Thread.currentThread();
        try {
            return runDueOnce(nowMillis);
        } finally {
            dueRunner = null;
        }
    }

    /** The thread running the participant now, whose own deadline moves need no wake; null between runs. */
    private volatile Thread dueRunner;

    private long runDueOnce(long nowMillis) {
        long epoch = runtime.viewEpoch();
        boolean direct = runtime.directRun();
        if (direct || nowMillis >= sweepDue.get() || nowMillis >= horizonDue.get() || epoch != dueEpoch) {
            dueEpoch = epoch;
            dueLooks.incrementAndGet();
            horizonDue.set(Long.MAX_VALUE); // horizon(now), inside the sweep, files the next round afresh
            sweepDue.set(Long.MAX_VALUE);
            // Not under the space lock: the sweep fires its events after its own lock, and a listener that takes a
            // lock of its own which another thread holds while it reads this space (a reduce binding's drain) would
            // otherwise deadlock with this run (2026-11-23). A deadline lowered meanwhile is kept by the minimum.
            sweepIfDue();
            synchronized (lock) {
                sweepDue.accumulateAndGet(nextDueLocked(), Math::min);
            }
            synchronized (horizonLock) {
                horizonDue.accumulateAndGet(horizonDeadlineLocked(horizonMode), Math::min);
            }
        }
        if (direct || nowMillis >= retryDue.get()) {
            retryDue.set(Long.MAX_VALUE); // a retry filed after this lowers it again and wakes the participant
            if (blocks != null) {
                retryBlockFetches();
            }
            if (bulk != null) {
                retryBulkFetches();
            }
            long next = Long.MAX_VALUE;
            for (BlockRetry retry : blockRetries.values()) {
                next = Math.min(next, retry.nextAtMillis());
            }
            for (BlockRetry retry : bulkRetries.values()) {
                next = Math.min(next, retry.nextAtMillis());
            }
            retryDue.accumulateAndGet(next, Math::min);
        }
        long localNow = localMillis();
        if (direct || localNow >= pullDue.get()) {
            pullDue.set(Long.MAX_VALUE); // a holder displaced after this lowers it again and wakes the participant
            pullDue.accumulateAndGet(pullDisplacedHolders(nowMillis, localNow), Math::min);
        }
        if (nowMillis >= adDue.get()) {
            adDue.set(Long.MAX_VALUE); // a no-op refresh (read-only, closed to advertising) lets it lapse, as designed
            adRenewals.incrementAndGet();
            refreshAdvertisement();
        }
        long pull = pullDue.get();
        long pullOnClock = pull == Long.MAX_VALUE ? Long.MAX_VALUE : nowMillis + Math.max(0, pull - localNow);
        return Math.min(Math.min(Math.min(sweepDue.get(), retryDue.get()), Math.min(horizonDue.get(), pullOnClock)),
                adDue.get());
    }

    /**
     * Opens the targeted rounds of {@link #displacedPulls} that are due (owner decision 4) and drops the lines whose
     * take is no longer open here: this peer's agent no longer holds the claim, the entry is no longer present, the
     * displaced completion is already noted, or the rounds are spent. The rounds are opened after the lock.
     *
     * @param nowMillis the space's clock, for the claim's expiry
     * @param localNow  the local base, for the rounds' schedule
     * @return the next round's deadline on the local base, or {@link Long#MAX_VALUE}
     */
    private long pullDisplacedHolders(long nowMillis, long localNow) {
        java.util.Set<PeerId> partners = new java.util.LinkedHashSet<>();
        long next = Long.MAX_VALUE;
        synchronized (lock) {
            if (displacedPulls.isEmpty()) {
                return Long.MAX_VALUE;
            }
            PeerId self = identity.peerId();
            Iterator<Map.Entry<EntryId, DisplacedPull>> lines = displacedPulls.entrySet().iterator();
            while (lines.hasNext()) {
                Map.Entry<EntryId, DisplacedPull> line = lines.next();
                EntryId entryId = line.getKey();
                DisplacedPull pull = line.getValue();
                TakeClaim held = claimOf(entryId);
                EntryState state = crdt.state(entryId).orElse(null);
                boolean open = held != null && held.holder().peer().equals(self) && state != null && state.present()
                        && !held.expired(nowMillis)
                        && !Long.valueOf(held.epoch()).equals(outbidCompletions.get(entryId));
                if (!open || pull.rounds >= DISPLACED_PULL_MAX) {
                    lines.remove();
                    continue;
                }
                if (localNow >= pull.nextMillis) {
                    partners.addAll(pull.holders);
                    pull.nextMillis = localNow + (REOFFER_BACKOFF_MILLIS << pull.rounds);
                    pull.rounds++;
                }
                next = Math.min(next, pull.nextMillis);
            }
        }
        for (PeerId partner : partners) {
            displacedPullRounds.incrementAndGet();
            try {
                runtime.gossip().antiEntropyWith(partner);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG, "a targeted round toward displaced holder " + partner.display()
                        + " failed; the next one is due on its backoff", e);
            }
        }
        return next;
    }

    /**
     * Advertisement renewals the space's participant ran (§10.33); package-private, for tests.
     *
     * @return the count
     */
    long adRenewals() {
        return adRenewals.get();
    }

    /**
     * Targeted anti-entropy rounds this replica opened toward holders its claim displaced (owner decision 4);
     * package-private, for tests.
     *
     * @return the count
     */
    long displacedPullRounds() {
        return displacedPullRounds.get();
    }

    /** How many runs of the space's participant took its lock; for tests. */
    long dueLooks() { // package-private for tests
        return dueLooks.get();
    }

    /** An unhinted member appeared, or this node went legacy: back to the legacy rule at once, streak reset. */
    private void disengageHorizon() {
        HorizonMode was;
        synchronized (horizonLock) {
            was = horizonMode;
            horizonMode = HorizonMode.LEGACY;
            horizonStreak = 0;
            horizonCountedAtMillis = Long.MIN_VALUE;
        }
        if (was == HorizonMode.PER_ENTRY) {
            // Since the drift floor of SPEC §7.3 (v0.1.35), a withdrawal's legacy horizon is never before its
            // per-entry one, so the index is never late here; filing afresh spares the sweep a visit to every
            // tombstone filed for the per-entry horizon that now waits longer.
            horizonRefile = true;
        }
        if (was == HorizonMode.SETTLING || was == HorizonMode.PER_ENTRY) {
            horizonDisengagements.incrementAndGet();
            System.Logger.Level level = horizonDisengageLogged.compareAndSet(false, true)
                    ? System.Logger.Level.INFO : System.Logger.Level.DEBUG;
            LOG.log(level, () -> "space " + name + ": a member without wire.horizon=1 is in view; withdrawals keep "
                    + "the legacy horizon again (was " + was + ")");
        }
    }

    /** The rule as last judged, for filing tombstones in the expiry index; the sweep judges afresh before it acts. */
    private SpaceStateCrdt.Horizon filedHorizon() {
        return horizonMode == HorizonMode.PER_ENTRY ? SpaceStateCrdt.Horizon.PER_ENTRY : SpaceStateCrdt.Horizon.LEGACY;
    }

    /**
     * The rule for one state under the mode this replica judged (RE-H-04). The mode says the view is hinted now, but a
     * copy can come from a writer that is not in the view: across a partition, gone since, or back only lately. This
     * replica cannot vouch for what such a writer renewed out of its sight (a second node on its identity, or an older
     * release that shortens), so the per-entry horizon applies only to a state whose every writing node is this one,
     * or a member that advertises {@code wire.horizon=1} and has been in the view longer than the longest lease plus
     * the grace: whatever it renewed before it came into sight has lapsed and passed its own lapse horizon by then.
     * Any other withdrawal keeps the legacy horizon here.
     *
     * <p>The writing node is the replica of the entry's add dot, set by the write and never changed, so the sweep and
     * the door read the same answer for every copy of one entry. Present and completed states keep the same horizon
     * under both rules, so only a withdrawal is looked up.
     */
    private SpaceStateCrdt.Horizon ruleFor(EntryState state, SpaceStateCrdt.Horizon mode, long now) {
        if (mode != SpaceStateCrdt.Horizon.PER_ENTRY || state.present() || state.completed()) {
            return mode;
        }
        return writerWaitMillis(state, now) == 0 ? SpaceStateCrdt.Horizon.PER_ENTRY : SpaceStateCrdt.Horizon.LEGACY;
    }

    /**
     * How long before every node that wrote a state can be trusted with the per-entry horizon here: 0 when each is
     * this replica or a hinted member in view for the maximum lease plus the grace; the rest of that wait when each is
     * at least a hinted member in view; {@link Long#MAX_VALUE} when one is out of view, unhinted, or unreadable, since
     * nothing here says when that would change.
     */
    private long writerWaitMillis(EntryState state, long now) {
        if (state.adds().isEmpty()) {
            return Long.MAX_VALUE;
        }
        String self = identity.peerId().value();
        long settle = Math.max(0, maxLeaseMillis()) + SpaceStateCrdt.LAPSED_GRACE_MILLIS;
        long wait = 0;
        for (Dot add : state.adds()) {
            if (add.replica().equals(self)) {
                continue; // this replica's own write: its mode already says this node is hinted
            }
            long since = writerInViewSince(add.replica(), now);
            if (since == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            if (since != Long.MIN_VALUE) {
                long trustedAt = since > Long.MAX_VALUE - settle ? Long.MAX_VALUE : since + settle;
                wait = Math.max(wait, trustedAt <= now ? 0 : trustedAt - now);
            }
        }
        return wait;
    }

    /**
     * Since when, on this space's clock, a writer has been a hinted member of the view in its current stay:
     * {@link Long#MIN_VALUE} for this node, {@link Long#MAX_VALUE} for a writer out of view, unhinted, or unreadable.
     * The membership measures the stay on the node's clock; the space converts it once, at the first judgement of
     * the stay, and keeps it, so that its sweep, its door and its expiry index all reckon the writer's wait on the
     * one clock they read, whatever the node's clock does meanwhile. A new stay (the member left and came back) starts
     * afresh.
     */
    private long writerInViewSince(String writer, long now) {
        long inView;
        long joined;
        try {
            PeerId peer = PeerId.of(writer);
            inView = runtime.hintedWriterInViewMillis(peer);
            joined = runtime.membership().joinedAtMillis(peer);
        } catch (IllegalArgumentException malformed) {
            return Long.MAX_VALUE;
        }
        if (inView == Long.MAX_VALUE) {
            return Long.MIN_VALUE;
        }
        if (inView < 0 || joined < 0) {
            writerStays.remove(writer);
            return Long.MAX_VALUE;
        }
        long[] stay = writerStays.get(writer);
        if (stay == null || stay[0] != joined) {
            stay = new long[]{joined, now - inView};
            writerStays.put(writer, stay);
        }
        return stay[1];
    }

    /**
     * Whether this replica collects a state now under the mode it judged (RE-H-04): by the state's rule, except that a
     * withdrawal the per-entry mode cannot vouch for keeps the later of the legacy horizon and the per-entry one, that
     * is {@code L + max(2 x maxLease, MAX_DRIFT)}. Both keep at least the drift floor of SPEC §7.3, so the legacy one
     * is never the earlier today; the later of the two is kept so that a writer leaving the view can never bring its
     * withdrawals' horizon closer whatever the two rules become: the expiry index files a tombstone by its horizon
     * and must never find it due earlier than filed.
     */
    private boolean collectableHere(EntryState state, long now, SpaceStateCrdt.Horizon mode) {
        long maxLease = maxLeaseMillis();
        SpaceStateCrdt.Horizon rule = ruleFor(state, mode, now);
        boolean byRule = SpaceStateCrdt.collectable(state, now, maxLease, rule);
        if (mode == SpaceStateCrdt.Horizon.PER_ENTRY && rule == SpaceStateCrdt.Horizon.LEGACY) {
            return byRule && SpaceStateCrdt.collectable(state, now, maxLease, SpaceStateCrdt.Horizon.PER_ENTRY);
        }
        return byRule;
    }

    /** The horizon {@link #collectableHere} reads for a state whose rule is already judged. */
    private long horizonHere(EntryState state, SpaceStateCrdt.Horizon mode, SpaceStateCrdt.Horizon rule) {
        long maxLease = maxLeaseMillis();
        long byRule = SpaceStateCrdt.collectionHorizon(state, maxLease, rule);
        if (mode == SpaceStateCrdt.Horizon.PER_ENTRY && rule == SpaceStateCrdt.Horizon.LEGACY) {
            return Math.max(byRule, SpaceStateCrdt.collectionHorizon(state, maxLease, SpaceStateCrdt.Horizon.PER_ENTRY));
        }
        return byRule;
    }

    /** The first writer of a state this replica has no wait for (out of view, unhinted, or unreadable), or null. */
    private String absentWriter(EntryState state) {
        String self = identity.peerId().value();
        for (Dot add : state.adds()) {
            if (add.replica().equals(self)) {
                continue;
            }
            if (writerInViewSince(add.replica(), nowMillis()) == Long.MAX_VALUE) {
                return add.replica();
            }
        }
        return null;
    }

    /**
     * The rule this replica applies to a state under the mode as last judged, with the writer judged now; for tests.
     * It does not judge the mode, so reading it never counts a round of the hysteresis.
     */
    SpaceStateCrdt.Horizon ruleFor(EntryState state) { // package-private for tests
        return ruleFor(state, filedHorizon(), nowMillis());
    }

    // ------------------------------------------------------------ operations (§7.2)

    @Override
    public <T> EntryHandle write(T entry, Lease lease) {
        return write(entry, lease, Map.of());
    }

    @Override
    public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
        return writeAs(writer, entry, lease, tags);
    }

    private <T> EntryHandle writeAs(AgentIdentity actor, T entry, Lease lease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(tags, "tags");
        requireWritable(actor, "write", SpaceAdmission.Scope.WRITE);
        return commitWrite(prepareWrite(actor, entry, lease, tags));
    }

    /** Spec §7.5: refuses mutation from an unadmitted local agent or on a read-only space. */
    private void requireWritable(AgentIdentity actor, String operation, SpaceAdmission.Scope scope) {
        if (readOnly()) {
            throw new SpaceReadOnlyException("space '" + name + "' is read-only: " + operation
                    + " refused (founder lease lapsed or space marked read-only)");
        }
        requireAdmitted(actor, operation, scope);
    }

    /** Admission is judged per acting agent (spec §7.5), so two views of one replica can differ. */
    private void requireAdmitted(AgentIdentity actor, String operation, SpaceAdmission.Scope scope) {
        if (!admits(actor.id(), scope, nowMillis())) {
            throw new SpaceAdmissionException("agent " + actor.id().encoded()
                    + " is not admitted to " + operation + " on space '" + name
                    + "' (" + admission + ")");
        }
    }

    /** Whether an agent may perform the scope here now (spec §7.5): WRITE for records, TAKE for claims and completions. */
    private boolean admits(AgentId agent, SpaceAdmission.Scope scope, long nowMillis) {
        Batch running = batch;
        if (running != null && running.owner == Thread.currentThread() && running.admitted != null) {
            // An authorizer's answer, asked before the batch took the lock (RE-M-08).
            Boolean asked = running.admitted.get(admissionKey(agent, scope));
            if (asked != null) {
                return asked;
            }
        }
        return admission.admits(agent, scope, nowMillis);
    }

    private static String admissionKey(AgentId agent, SpaceAdmission.Scope scope) {
        return scope + " " + agent.encoded();
    }

    // ---- the batch of one anti-entropy delta (RE-M-08)

    /**
     * One anti-entropy delta being merged under one hold of {@link #lock} by its {@link #owner}: the generation moves
     * once at its end, and what a merge would deliver outside the lock (events, misbehaviour reports) waits in
     * {@link #after} until the lock is released.
     */
    private static final class Batch {
        final Thread owner = Thread.currentThread();
        final List<Runnable> after = new ArrayList<>();
        /** An AUTHORIZER rule's answers by agent and scope, asked before the lock; null under any other rule. */
        final Map<String, Boolean> admitted;
        boolean changed;

        Batch(Map<String, Boolean> admitted) {
            this.admitted = admitted;
        }
    }

    /**
     * Set on a thread while it delivers the events an anti-entropy batch deferred (RE-M-08). Only there are listeners
     * isolated from each other: the states are merged already and nothing fires those events again, so one listener's
     * throw must not cost the others theirs. A direct write, take or complete fires as it always did, so a listener's
     * exception reaches its caller, as on a LocalSpace.
     */
    private static final ThreadLocal<Boolean> DELIVERING_BATCH = new ThreadLocal<>();

    /** The batch the thread merging an anti-entropy delta runs; null otherwise. Written under {@link #lock}. */
    private volatile Batch batch;
    /**
     * How long the last delta held the lock, the merge alone without the signature checks made before it: RE-Q-PRF-01
     * judges this part, and the benchmark reports it beside the whole. Under {@link #lock}.
     */
    private long lastBatchLockedNanos;

    /** How long the last anti-entropy delta held the lock, in nanoseconds; for the benchmark of RE-Q-PRF-01. */
    long lastBatchLockedNanos() {
        synchronized (lock) {
            return lastBatchLockedNanos;
        }
    }

    /** The generation the digest cache is keyed on; for the test that a delta moves it once (RE-M-08). */
    long generationForTest() {
        synchronized (lock) {
            return generation;
        }
    }

    /**
     * The signature checks of a received state made before the batch's lock. A completion is checked against the claim
     * it is expected to meet under the lock, named in {@code against}; the merge reuses that answer only when the claim
     * then held is that same proof, and checks again otherwise.
     *
     * @param against the claim a completion was checked against, or null (not a completion, or no claim was known)
     */
    private record Prechecked(boolean record, boolean state, SpaceWire.SignedClaim against) {
    }

    /** Runs work that belongs outside the lock now, or after the batch when this thread is merging one. */
    private void later(Runnable work) {
        Batch running = batch;
        if (running != null && running.owner == Thread.currentThread()) {
            running.after.add(work);
        } else {
            work.run();
        }
    }

    /** Reports witnessed misbehaviour, after the batch when this thread is merging one. */
    private void report(PeerId peer, String violation) {
        later(() -> runtime.reportMisbehavior(peer, violation));
    }

    /**
     * Registers an entry type and remembers its schema name for the
     * advertisement. The reserved {@link SpaceCredential} type is registered
     * on every replica but never advertised as a hint: it is admission
     * infrastructure, not the shape of the data the space carries.
     */
    private String registerSchema(Class<?> type) {
        String schemaName = schemas.register(type);
        if (!schemaName.equals(credentialSchema)) {
            schemaNames.add(schemaName);
        }
        return schemaName;
    }

    /**
     * Reads with a consistency hint (spec §7.2): {@link ConsistencyHint#FRESH}
     * performs exactly one anti-entropy pull toward one random group member
     * before the local match; {@link ConsistencyHint#LOCAL} is
     * {@link #read(Template, Duration)}.
     *
     * @param template the template
     * @param hint     how current the read must be
     * @param timeout  how long to wait for a match
     * @param <T>      the entry type
     * @return the first match
     */
    public <T> Optional<T> read(Template<T> template, ConsistencyHint hint, Duration timeout) {
        Objects.requireNonNull(hint, "hint");
        pullIfFresh(hint);
        return read(template, timeout);
    }

    /**
     * Reads all matches with a consistency hint (spec §7.2); see
     * {@link #read(Template, ConsistencyHint, Duration)}.
     *
     * @param template the template
     * @param hint     how current the read must be
     * @param limit    the maximum number of results
     * @param <T>      the entry type
     * @return the matches, up to {@code limit}
     */
    public <T> List<T> readAll(Template<T> template, ConsistencyHint hint, int limit) {
        Objects.requireNonNull(hint, "hint");
        pullIfFresh(hint);
        return readAll(template, limit);
    }

    /**
     * One anti-entropy round (digest to one random member) for FRESH reads. Not on a writer-local space: it holds
     * nothing another member could send, and the round would reconcile the bus's other streams for nothing.
     */
    private void pullIfFresh(ConsistencyHint hint) {
        if (hint == ConsistencyHint.FRESH && !local) {
            runtime.gossip().antiEntropyTick();
        }
    }

    /** A fully-built, signed entry ready to commit; construction may fail, commit does not. */
    private record PreparedWrite(EntryId entryId, EntryRecord signed, Dot dot,
                                 LwwRegister<LeaseInfo> lease, Object value, AgentIdentity actor,
                                 AgentCertificate certificate) {
    }

    /**
     * Builds and signs an entry record without mutating the space. Everything
     * that can fail (serialization, encryption, the oversize-without-blocks
     * check, the block store) happens here, so a caller that must not commit
     * one change until another is guaranteed to succeed (see
     * {@link #completeInternal}) can prepare first and commit last.
     */
    private <T> PreparedWrite prepareWrite(AgentIdentity actor, T entry, Lease lease,
                                           Map<String, String> tags) {
        if (actorRevoked(actor)) {
            // v0.1.13: every receiver would refuse it; fail where the caller can see why.
            throw new IllegalStateException(actor.id().encoded() + " is revoked in this group");
        }
        registerSchema(entry.getClass());
        observeLease(lease.duration().toMillis());
        long expiresAt = nowMillis() + lease.duration().toMillis();
        EntryId entryId = EntryId.newId();
        LeaseInfo writeLease = new LeaseInfo(actor.id(), expiresAt, LeaseKind.WRITE);
        byte[] payload = codec.toBytes(entry);
        Long keyEpoch = null;
        long epoch = keyRing == null ? 0 : keyRing.writeEpoch(clock.instant());
        // PD-E-06: the seam a payload above the threshold takes; the primary when every
        // member can receive it, else the fallback, else the block path as today.
        ai.badmonkey.agentspaces.api.spi.BulkPayloads seam = null;
        if (bulk != null && escalateAbove > 0 && payload.length > escalateAbove) {
            seam = escalationSeam();
        }
        String payloadRef = null;
        if (seam == bulk && seam != null && !sealBeforeEscalate) {
            // PD-E-04: a PDT space hands the seam the plaintext CBOR so unchanged sub-trees
            // are shared between versions; the nodes travel sealed per message (group-key
            // mode), the record names the write epoch, and its signature over payloadRef
            // binds the tree. The writer must still hold the epoch's key, as for a sealed write.
            if (keyRing != null) {
                keyRing.sealingKey(epoch).orElseThrow(() -> new IllegalStateException(
                        "content key epoch " + epoch + " is not held"));
            }
            try {
                payloadRef = seam.publish(payload, lease);
                payload = null;
                keyEpoch = keyRing == null || epoch == 0 ? null : epoch;
            } catch (IllegalArgumentException unspillable) {
                // PD-E-02, PD-N-04: a value the spill rules cannot break up (a text string
                // above the 1 MiB node bound) takes the sealed fallback or the block path,
                // as a space without the trees carries it; the entry is still written.
                seam = bulkFallback;
                if (bulkRequired) {
                    throw new IllegalArgumentException("space '" + name + "' requires the tree exchange for payloads"
                            + " above " + escalateAbove + " bytes and this payload cannot become a tree: "
                            + unspillable.getMessage(), unspillable);
                }
            }
        }
        if (payloadRef == null) {
            if (keyRing != null) {
                // SPEC §11a.3 v0.1.13: seal under the newest epoch in effect; the
                // record names the epoch (absent for 0) and the AAD binds it.
                payload = keyRing.sealingKey(epoch).orElseThrow(() -> new IllegalStateException(
                        "content key epoch " + epoch + " is not held")).encrypt(payload, aadFor(entryId, epoch));
                keyEpoch = epoch == 0 ? null : epoch;
            }
            if (bulk == null && maxPayload > 0 && payload.length > maxPayload) {
                throw new IllegalArgumentException("payload of " + payload.length + " bytes exceeds " + maxPayload
                        + " bytes and this space has no content swarm to carry it; turn on agentspaces.content.enabled"
                        + " (escalate-above) or write it through Content.publish");
            }
            if (seam != null) {
                // LB-E-02: above the threshold the payload becomes an artifact of the content
                // swarm and the record carries its root; the space replicates the reference only.
                payloadRef = seam.publish(payload, lease);
                payload = null;
            }
        }
        if (payload != null && payload.length > EntryRecord.INLINE_PAYLOAD_LIMIT) {
            if (blocks == null) {
                throw new IllegalStateException("payload of " + payload.length
                        + " bytes exceeds the inline limit; configure builder.blocks(...)");
            }
            payloadRef = blocks.put(payload);
            blocks.retain(payloadRef); // LB-B-04: a live entry pins its block
            payload = null;
        }
        EntryRecord unsigned = new EntryRecord(entryId, id,
                schemas.schemaNameOf(entry.getClass()), payload, payloadRef,
                actor.id(), hlc.now(), writeLease, tags, null, keyEpoch);
        // SPEC §4.2 v0.1.13: receivers judge the certificate at the record's
        // issue stamp, so the writer attaches the one covering that stamp.
        AgentCertificate certificate = coveringCertificate(actor, unsigned.issued());
        EntryRecord signed = withSignature(unsigned, actor);
        Dot dot = new Dot(identity.peerId().value(), dotCounter.incrementAndGet());
        return new PreparedWrite(entryId, signed, dot,
                new LwwRegister<>(signed.issued(), writeLease), entry, actor, certificate);
    }

    /** Commits a prepared entry into the space; does not fail. */
    private EntryHandle commitWrite(PreparedWrite prepared) {
        SpaceWire.EntryStateDto dto;
        synchronized (lock) {
            setCrdt(crdt.add(prepared.signed(), prepared.dot(), prepared.lease()), prepared.entryId());
            bump(prepared.entryId());
            issuerKeys.put(prepared.entryId(), identity.rawPublicKey());
            if (prepared.certificate() != null) {
                issuerCertificates.put(prepared.entryId(), prepared.certificate());
            }
            valueCache.put(prepared.entryId(), prepared.value());
            indexCredential(prepared.entryId());
            /**
             * Builds a signed state DTO for the entry: {@link SpaceWire.EntryStateDto}
             * plus a signature by this node over the mutable state. The local node is
             * the authorized actor for every state it publishes (the issuer for its own
             * writes/renewals/cancels, the take-claim holder for its completions), so it
             * always signs with its own key.
             */
            dto = signedDto(prepared.entryId(), identity.rawPublicKey(), prepared.actor());
            ownWrites.put(prepared.entryId(), nowMillis());
        }
        publishDelta(new SpaceWire.Delta(dto, null, null), "w:" + prepared.entryId());
        fire(SpaceEvent.Kind.WRITTEN, prepared.entryId(), prepared.value(), prepared.actor().id(),
                prepared.actor().id(), prepared.signed());
        return new Handle(prepared.entryId(), prepared.actor());
    }

    @Override
    public <T> Optional<T> read(Template<T> template) {
        Objects.requireNonNull(template, "template");
        registerSchema(template.type());
        // Snapshot the candidate records under the lock, then decode outside it.
        // decodeIfMatches never waits on the network (P7): a missing block only
        // starts a background fetch, so nothing here holds the space lock while
        // a frame carrying the block needs it on the transport reader thread.
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(candidateType(template));
        }
        for (EntryRecord record : candidates) {
            Optional<T> value = decodeIfMatches(template, record);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    @Override
    public <T> Optional<T> read(Template<T> template, Duration timeout) {
        return awaiting(timeout, () -> read(template));
    }

    @Override
    public <T> List<T> readAll(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        // Snapshot under the lock, decode outside it; entries whose block has not
        // arrived are skipped this time (see read()).
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(candidateType(template));
        }
        List<T> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            decodeIfMatches(template, record).ifPresent(results::add);
            if (results.size() == limit) {
                break;
            }
        }
        return results;
    }

    @Override
    public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(candidateType(template));
        }
        // The issuer comes off the entry record, whose signature mergeState
        // verified against that issuer's key, so it is the authenticated writer.
        List<Issued<T>> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            decodeIfMatches(template, record).ifPresent(value ->
                    results.add(new Issued<>(value, record.issuer(),
                            issuerCertificates.containsKey(record.entryId())
                                    ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED)));
            if (results.size() == limit) {
                break;
            }
        }
        return results;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Under LEASE_RACE and AUCTION (SPEC §7.4) the take publishes its claim, waits the space's settle window, and
     * wins only if its claim is still live when it judges the race. A take lease no longer than the settle window
     * has always lapsed by then, so such a take is refused at once rather than racing for nothing until its timeout.
     * Leave a margin above the window: a busy machine wakes the take late.
     *
     * <p>The win is exclusive only while every rival claim reaches this replica within the settle window (SPEC §7.4,
     * the settle guarantee). A rival claim that arrives later and beats this one makes {@code complete} throw
     * {@link LeaseExpiredException} if it arrives before the completion, and so does the entry's completion by
     * anyone. If a holder this claim displaced completes the entry first, this take comes back empty, or its
     * {@code complete} throws and writes no result, after acknowledging that completion. A rival that hears of this
     * take only after both completed leaves a duplicate completion, which {@link #duplicateCompletions()} counts.
     *
     * @throws IllegalArgumentException if {@code takeLease} is no longer than the space's settle window
     */
    @Override
    public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease, Duration timeout) {
        return takeAs(writer, template, takeLease, timeout);
    }

    /**
     * Refuses a take whose lease cannot outlast the settle window (SPEC §7.4). The race is judged right after the
     * window and only a live claim wins, so with a lease no longer than the window every claim had lapsed when it was
     * judged: the take came back empty at its timeout as if it had lost every race, while each doomed claim held the
     * entry from other takers for its lease (found 2026-11-15, the TypeScript take refuses the same with a
     * RangeError). The TypeScript take judges after a follow-up wait as well, so its bound is the window plus that
     * wait; this take judges right after the window, so its bound is the window alone.
     */
    private void requireTakeLeaseOutlastsSettle(Lease takeLease) {
        long leaseMillis = takeLease.duration().toMillis();
        long settleMillis = settleWindow.toMillis();
        if (leaseMillis <= settleMillis) {
            throw new IllegalArgumentException("a take lease of " + leaseMillis + " ms cannot outlast the settle "
                    + "window of " + settleMillis + " ms on space '" + name + "': the claim would lapse before the "
                    + "take is judged (the lease must exceed the window)");
        }
    }

    private <T> Optional<TakenEntry<T>> takeAs(AgentIdentity actor, Template<T> template,
                                               Lease takeLease, Duration timeout) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(takeLease, "takeLease");
        Objects.requireNonNull(timeout, "timeout");
        requireTakeLeaseOutlastsSettle(takeLease);
        registerSchema(template.type());
        requireWritable(actor, "take", SpaceAdmission.Scope.TAKE);
        observeLease(takeLease.duration().toMillis());
        // Between attempts the take waits for a change that could let it succeed, not a 20 ms poll that scanned the
        // space fifty times a second for as long as it waited (IDLE-WORK 5a(a)).
        return awaiting(timeout, () -> tryTakeOnce(actor, template, takeLease));
    }

    @Override
    public <T> List<Entry<T>> readAllEntries(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        // Issue #16 §9.2: the readAll visibility rule (live, unclaimed, decodable
        // now) over the records this replica holds; the attestation is read
        // under the lock beside the candidates, as the certificate table is.
        List<EntryRecord> candidates;
        Set<EntryId> attested = new HashSet<>();
        synchronized (lock) {
            candidates = availableRecords(candidateType(template));
            for (EntryRecord record : candidates) {
                if (issuerCertificates.containsKey(record.entryId())) {
                    attested.add(record.entryId());
                }
            }
        }
        List<Entry<T>> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            decodeIfMatches(template, record).ifPresent(value ->
                    results.add(EntryView.of(record, value, attested.contains(record.entryId())
                            ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED)));
            if (results.size() == limit) {
                break;
            }
        }
        return results;
    }

    @Override
    public List<EntryId> undecodableEntries(Template<?> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        registerSchema(template.type());
        // HLC round six, 2026-11-08 (ruling R3): the readAll visibility rule, and of the entries the
        // template selects, those whose payload is here in the clear and holds no value of its type
        List<EntryRecord> candidates;
        synchronized (lock) {
            candidates = availableRecords(candidateType(template));
        }
        List<EntryId> results = new ArrayList<>();
        for (EntryRecord record : candidates) {
            if (!selects(template, record) || template.type().isInstance(valueCache.get(record.entryId()))) {
                continue;
            }
            byte[] payload = clearPayload(record);
            if (payload != null && decodeDocument(payload, template.type()) == null) {
                results.add(record.entryId());
                if (results.size() == limit) {
                    break;
                }
            }
        }
        return results;
    }

    /**
     * The record this replica currently holds for an entry (issue #16 §9.3):
     * present while the CRDT reports the entry present, whether it is available
     * or claimed, and empty once it is completed, withdrawn, or unknown here.
     * The ordered join uses it to read a ticket's tags for an entry id it learned
     * from a committed claim, which a template read cannot see while claimed.
     * The lease on the returned record is the one in force.
     *
     * @param entryId the entry
     * @return the record, when the entry is present here
     */
    public Optional<EntryRecord> recordOf(EntryId entryId) {
        return recordOf(entryId, false);
    }

    /**
     * {@link #recordOf(EntryId)}, optionally answering for a completed entry
     * too, from the tombstone the CRDT keeps until the collection horizon
     * (SPEC §7.3). The ordered join needs this: a member may learn of the first
     * committed ticket claim for a key after that ticket's taker has already
     * completed it, and must still read the ticket's key to know that a later
     * ticket for the same key does not fire.
     *
     * @param entryId      the entry
     * @param completedToo whether a completed entry's record is returned as well
     * @return the record, when the replica holds one
     */
    public Optional<EntryRecord> recordOf(EntryId entryId, boolean completedToo) {
        Objects.requireNonNull(entryId, "entryId");
        synchronized (lock) {
            return crdt.state(entryId)
                    .filter(state -> state.present() || (completedToo && state.completed()))
                    .map(state -> state.record().withLease(state.lease().value()));
        }
    }

    /**
     * The schema name this replica registers {@code type} under (issue #16
     * §10.2), registering it first when it is new, as a read or subscription
     * would. A binder compares this with the name it advertises so a card and
     * the space agree on a type's name.
     *
     * @param type the entry type
     * @return the schema name
     */
    public String schemaNameOf(Class<?> type) {
        Objects.requireNonNull(type, "type");
        return registerSchema(type);
    }

    @Override
    public void complete(TakenEntry<?> taken) {
        completeInternal(writer, taken, null, null, Map.of());
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
        return complete(taken, result, resultLease, Map.of());
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(resultLease, "resultLease");
        Objects.requireNonNull(tags, "tags");
        return completeInternal(writer, taken, result, resultLease, tags);
    }

    @Override
    public <T> Subscription notify(Template<T> template, SpaceListener<T> listener, Lease lease) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(lease, "lease");
        // A subscription declares interest in a type: register it so replicated
        // deltas of that type decode into events on this replica.
        registerSchema(template.type());
        Sub sub = new Sub(template, listener, localMillis() + lease.duration().toMillis());
        subscriptions.add(sub);
        return sub;
    }

    @Override
    public void close() {
        subscriptions.clear();
        // Leave the bus, so the name can be opened again and a delta for this
        // replica is dropped rather than applied to a handle nobody holds.
        try {
            streamRegistration.close();
            reconcileRegistration.close();
            dueRegistration.close();
            keyInstalls.close();
        } catch (Exception e) {
            throw new IllegalStateException("failed deregistering space '" + name + "'", e);
        }
        blockRetries.clear();
    }

    /**
     * Returns the current take claim on an entry, when any replica claim is known
     * here. Exposed for the ordered-log coordinator and for fleet consoles.
     *
     * @param entryId the entry
     * @return the current claim
     */
    public Optional<TakeClaim> currentClaim(EntryId entryId) {
        synchronized (lock) {
            return Optional.ofNullable(claimOf(entryId));
        }
    }

    /**
     * Installs a claim the ordered log committed, <em>with the holder's own
     * attestation</em> (spec §7.4 ORDERED, QA4 A4-5). The coordinator verified
     * {@code signature} over {@code claim} under {@code holderKey} before the
     * command was ever committed; passing them through unchanged is what lets
     * every replica later authenticate the holder's completion, because a
     * completion is verified against the key the stored claim carries. The
     * claim is never re-published as a rumor; the log is its replication
     * channel, and the log's decision is authoritative for the claim's epoch.
     *
     * @param entryId   the entry
     * @param claim     the committed claim, exactly as the holder signed it
     * @param holderKey the holder's raw Ed25519 public key
     * @param signature the holder's signature over the claim's canonical bytes
     * @throws IllegalArgumentException when the key does not belong to the
     *                                  claim's holder or the signature does not
     *                                  verify, since the caller's proof does not
     *                                  prove what it claims, or when the claim's
     *                                  stamp carries a logical counter above
     *                                  {@link HybridLogicalClock#LOGICAL_LIMIT}
     */
    public void applyAuthorizedClaim(EntryId entryId, TakeClaim claim,
                                     byte[] holderKey, byte[] signature) {
        applyAuthorizedClaim(entryId, claim, holderKey, signature, null);
    }

    /**
     * {@link #applyAuthorizedClaim(EntryId, TakeClaim, byte[], byte[])} for a claim
     * the holder signed with a subordinate agent key: the certificate travels with
     * the proof and the two-key rule verifies it (QA4 A4-7 phase 3).
     *
     * @param entryId     the entry
     * @param claim       the committed claim
     * @param holderKey   the holder's peer key
     * @param signature   the holder's signature, under its agent key when a certificate is given
     * @param certificate the peer's certificate for the agent key, or null for a peer-signed claim
     */
    public void applyAuthorizedClaim(EntryId entryId, TakeClaim claim,
                                     byte[] holderKey, byte[] signature,
                                     ai.badmonkey.agentspaces.api.security.AgentCertificate certificate) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(claim, "claim");
        Objects.requireNonNull(holderKey, "holderKey");
        Objects.requireNonNull(signature, "signature");
        SpaceWire.SignedClaim attested = new SpaceWire.SignedClaim(claim, holderKey, signature,
                certificate);
        String refusal = claimRefusal(entryId, attested);
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
        refuseLogClaimStamp(entryId, claim, claim.holder().peer());
        synchronized (lock) {
            mergeHeldClaim(entryId, attested);
            bump(entryId);
            logDecidedEpochs.merge(entryId, claim.epoch(), Math::max);
        }
    }

    /**
     * Whether a log-committed claim would be accepted by
     * {@link #applyAuthorizedClaim(EntryId, TakeClaim, byte[], byte[], AgentCertificate)},
     * without installing it (issue #16): the ordered-log coordinator asks this
     * for a committed claim whose generation this replica already holds through
     * gossip, so that listeners on the committed order still hear of every
     * authentic claim.
     */
    public boolean verifiesAuthorizedClaim(EntryId entryId, TakeClaim claim, byte[] holderKey,
                                           byte[] signature,
                                           ai.badmonkey.agentspaces.api.security.AgentCertificate certificate) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(claim, "claim");
        if (holderKey == null || signature == null) {
            return false;
        }
        return claimRefusal(entryId, new SpaceWire.SignedClaim(claim, holderKey, signature,
                certificate)) == null && HybridLogicalClock.isAcceptable(claim.stamp());
    }

    /** The reason a log-committed claim is refused, or null when it is accepted. */
    private String claimRefusal(EntryId entryId, SpaceWire.SignedClaim attested) {
        TakeClaim claim = attested.claim();
        byte[] holderKey = attested.holderKey();
        if (!attested.holderAttested()) {
            return "claim on " + entryId.value() + " is held by agent '"
                    + claim.holder().localName() + "' of " + claim.holder().peer().display()
                    + " but the key offered as its attestation belongs to "
                    + (holderKey.length == Ed25519.RAW_PUBLIC_KEY_LENGTH
                            ? PeerId.fromPublicKey(holderKey).display() : "no valid peer")
                    + "; a completion could never be authenticated against it";
        }
        if (!verifyClaim(entryId, attested)) {
            return "claim on " + entryId.value()
                    + " does not verify under its holder's key, or is bound to another entry";
        }
        if (runtime.revocationView().refuses(claim.holder(), claimKey(attested), null)) {
            // v0.1.13: the log path refuses a revoked holder exactly as gossip does.
            return "claim on " + entryId.value() + " is held by "
                    + claim.holder().encoded() + ", which is revoked in this group";
        }
        return null;
    }

    /**
     * Installs a claim the ordered log committed <em>without</em> the holder's
     * attestation: the stored proof is signed by this node, so it decides local
     * arbitration but cannot authenticate the holder's completion at any other
     * replica. Prefer {@link #applyAuthorizedClaim(EntryId, TakeClaim, byte[], byte[])}
     * whenever the holder's signature is available, which for a log command it
     * always is. Kept for callers that arbitrate locally and never complete.
     *
     * @param entryId the entry
     * @param claim   the ordered claim
     * @throws IllegalArgumentException when the claim's stamp carries a logical counter
     *                                  above {@link HybridLogicalClock#LOGICAL_LIMIT}
     */
    public void applyAuthorizedClaim(EntryId entryId, TakeClaim claim) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(claim, "claim");
        refuseLogClaimStamp(entryId, claim, null);
        synchronized (lock) {
            mergeHeldClaim(entryId, sign(claim, writer));
            bump(entryId);
            logDecidedEpochs.merge(entryId, claim.epoch(), Math::max);
        }
    }

    /**
     * Adopts an entry whose current claim this agent already holds (placed
     * through the ordered-log coordinator), returning a {@link TakenEntry} that
     * {@link #complete(TakenEntry)} accepts.
     *
     * @param template the template naming the entry type
     * @param entryId  the claimed entry
     * @param <T>      the entry type
     * @return the taken entry, or empty when this agent does not hold the claim
     */
    public <T> Optional<TakenEntry<T>> adoptClaim(Template<T> template, EntryId entryId) {
        return adoptClaimAs(writer, template, entryId);
    }

    /**
     * Adopts a log-decided claim as the given holder identity (v0.1.13), so a
     * completion is signed by the agent that holds the claim, as an
     * agent-attested claim's completion must be (SPEC §11a.4).
     *
     * @param template the entry's template
     * @param entryId  the claimed entry
     * @param holder   the identity the claim names as its holder
     * @param <T>      the entry type
     * @return the taken entry, or empty when the claim is not this holder's
     */
    public <T> Optional<TakenEntry<T>> adoptClaim(Template<T> template, EntryId entryId,
                                                  AgentIdentity holder) {
        return adoptClaimAs(Objects.requireNonNull(holder, "holder"), template, entryId);
    }

    private <T> Optional<TakenEntry<T>> adoptClaimAs(AgentIdentity actor, Template<T> template,
                                                     EntryId entryId) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(entryId, "entryId");
        registerSchema(template.type());
        synchronized (lock) {
            TakeClaim claim = claimOf(entryId);
            if (claim == null || !claim.holder().equals(actor.id())
                    || claim.expired(nowMillis())) {
                return Optional.empty();
            }
            for (EntryRecord record : liveRecords()) {
                if (record.entryId().equals(entryId)) {
                    return decodeIfMatches(template, record)
                            .map(value -> new Taken<>(entryId, value, claim.epoch(), actor));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Reverse-maps a value returned by {@code read}/{@code readAll} to its entry
     * id among currently available entries. Exposed for take coordinators.
     *
     * @param value the entry value
     * @return the entry id, when the value corresponds to an available entry
     */
    public Optional<EntryId> entryIdOf(Object value) {
        Objects.requireNonNull(value, "value");
        synchronized (lock) {
            for (EntryRecord record : availableRecords()) {
                if (value.equals(valueCache.get(record.entryId()))) {
                    return Optional.of(record.entryId());
                }
            }
        }
        return Optional.empty();
    }

    /** How many decoded values this replica holds; live entries at most (package-private, for the leak tests). */
    int cachedValues() {
        return valueCache.size();
    }

    /** Returns how many entries this replica knows about, tombstones included. */
    public int knownEntries() {
        synchronized (lock) {
            return crdt.knownEntries();
        }
    }

    // -------------------------------------------------------------- take / claims

    private <T> Optional<TakenEntry<T>> tryTakeOnce(AgentIdentity actor, Template<T> template,
                                                    Lease takeLease) {
        if (actorRevoked(actor)) {
            return Optional.empty(); // v0.1.13: a revoked agent takes nothing, even locally
        }
        AgentId holder = actor.id();
        EntryId claimedId = null;
        TakeClaim myClaim = null;
        T claimedValue = null;
        EntryRecord claimedRecord = null;
        synchronized (lock) {
            long now = nowMillis();
            // Gather matching candidates; under AUCTION, bid on the candidate this
            // agent is cheapest on first (the CBBA task-selection rule).
            record Candidate<V>(EntryRecord entry, V value, double bid) {
            }
            List<Candidate<T>> candidates = new ArrayList<>();
            for (EntryRecord record : liveRecords(candidateType(template))) {
                Optional<T> value = decodeIfMatches(template, record);
                if (value.isEmpty()) {
                    continue;
                }
                double bid = 0.0;
                if (strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.AUCTION) {
                    if (bidFunction == null) {
                        throw new IllegalStateException(
                                "AUCTION take requires a bidFunction on space '" + name + "'");
                    }
                    bid = bidFunction.applyAsDouble(value.get());
                }
                candidates.add(new Candidate<>(record, value.get(), bid));
            }
            if (strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.AUCTION) {
                candidates.sort(java.util.Comparator.comparingDouble(Candidate::bid));
            }
            for (Candidate<T> candidate : candidates) {
                TakeClaim current = claimOf(candidate.entry().entryId());
                if (current != null && !current.expired(now)) {
                    // Held. Under AUCTION a strictly better bid may still counter-claim
                    // while the incumbent's settle window is open (CBAA-style), but only
                    // in its first half. The incumbent decides it won when its window
                    // ends, so a counter-claim made near that end reached it after the
                    // decision: the incumbent ran its action and completed, and the
                    // counter-claim, winning the lattice, completed the entry a second
                    // time (Party Bus, 2026-11-15: a planner whose own two-second take
                    // had just ended outbid another's claim 1,993 ms after it was
                    // stamped, and the claim arrived 2 ms after that holder had won).
                    // The second half is the counter-claim's time to travel. Travel time has no bound, so
                    // the bound only makes a late counter-claim rare; when one still arrives after the
                    // decision, the incumbent's completion tells this take to stand down (outbidCompletions).
                    boolean canOutbid =
                            strategy == ai.badmonkey.agentspaces.api.space.ConflictStrategyType.AUCTION
                                    && candidate.bid() < current.bid()
                                    && now - current.stamp().physical() <= settleWindow.toMillis() / 2;
                    if (!canOutbid) {
                        continue;
                    }
                    myClaim = new TakeClaim(candidate.entry().entryId(), id,
                            current.epoch(), hlc.now(), holder,
                            candidate.bid(), now + takeLease.duration().toMillis());
                } else {
                    long epoch = current == null ? 1 : current.epoch() + 1;
                    myClaim = new TakeClaim(candidate.entry().entryId(), id,
                            epoch, hlc.now(), holder,
                            candidate.bid(), now + takeLease.duration().toMillis());
                }
                mergeHeldClaim(candidate.entry().entryId(), sign(myClaim, actor));
                bump(candidate.entry().entryId());
                claimedId = candidate.entry().entryId();
                claimedValue = candidate.value();
                claimedRecord = candidate.entry();
                break;
            }
        }
        if (claimedId == null) {
            return Optional.empty();
        }
        publishDelta(new SpaceWire.Delta(null, claimedId, sign(myClaim, actor)),
                "c:" + claimedId + ":" + myClaim.stamp().encoded());
        settle(settleWindow);
        boolean doneByOutbid;
        synchronized (lock) {
            TakeClaim winner = claimOf(claimedId);
            // Core cancel events, 2026-11-09: an entry completed or withdrawn during the settle window is
            // nobody's to take, so a cancel that merged meanwhile leaves the take empty, as on a LocalSpace.
            boolean won = winner != null && winner.holder().equals(holder)
                    && winner.epoch() == myClaim.epoch() && !winner.expired(nowMillis())
                    && crdt.state(claimedId).map(EntryState::present).orElse(false);
            Long outbid = outbidCompletions.get(claimedId);
            // The holder this claim displaced completed the entry during the window (see outbidCompletions):
            // the work is done, so this take acknowledges that completion instead of doing it again. The line
            // stays for completeInternal, which makes the acknowledgement and counts it.
            doneByOutbid = won && outbid != null && outbid == myClaim.epoch();
            if (!doneByOutbid) {
                outbidCompletions.remove(claimedId);
            }
            if (!won) {
                return Optional.empty(); // lost the race, or the entry went; retry elsewhere
            }
            if (!doneByOutbid) {
                takenEpochs.put(claimedId, myClaim.epoch());
            }
        }
        Taken<T> won = new Taken<>(claimedId, claimedValue, myClaim.epoch(), actor);
        if (doneByOutbid) {
            acknowledgeOutbidCompletion(actor, won);
            return Optional.empty();
        }
        fire(SpaceEvent.Kind.TAKEN, claimedId, claimedValue, claimedRecord.issuer(),
                claimedRecord.issuer(), claimedRecord);
        return Optional.of(won);
    }

    /**
     * Completes, with no result, an entry this replica's claim holds after the holder it displaced completed it
     * (see {@link #outbidCompletions}). The completion is signed under the claim every replica holds, so the
     * replicas that dropped the displaced holder's completion converge on this one, and that holder's result stays
     * the only one. completeInternal finds the line, counts the acknowledgement and reports it by throwing; a take
     * that lapsed meanwhile has nothing to acknowledge and throws the same way.
     */
    private void acknowledgeOutbidCompletion(AgentIdentity actor, Taken<?> taken) {
        try {
            completeInternal(actor, taken, null, null, Map.of());
        } catch (LeaseExpiredException acknowledgedOrLapsed) {
            // acknowledged (counted there), or the claim lapsed or a better one arrived: whoever holds it completes it
        }
    }

    private EntryHandle completeInternal(AgentIdentity actor, TakenEntry<?> taken, Object result,
                                         Lease resultLease, Map<String, String> tags) {
        Objects.requireNonNull(taken, "taken");
        if (!(taken instanceof Taken<?> t)) {
            throw new IllegalArgumentException("foreign TakenEntry implementation: " + taken.getClass());
        }
        // Spec §7.5: completion is a mutation the admission rule covers (it
        // needs the TAKE scope, like the claim it finishes); on a read-only
        // space a bare completion of work already held may still land, but its
        // result is a write and is refused like any other.
        requireAdmitted(actor, "complete", SpaceAdmission.Scope.TAKE);
        if (result != null) {
            requireWritable(actor, "complete with result", SpaceAdmission.Scope.WRITE);
        }
        // #2 atomicity: build and validate the result record BEFORE committing
        // the completion, so a result that cannot be written (oversize on a
        // blocks-less space, unserializable, block-store failure) never leaves
        // the task completed-but-resultless and fleet-wide unrecoverable.
        PreparedWrite preparedResult = result == null
                ? null : prepareWrite(actor, result, resultLease, tags);
        SpaceWire.EntryStateDto dto;
        SpaceWire.SignedClaim proof;
        Object completedValue;
        boolean doneByOutbid;
        synchronized (lock) {
            TakeClaim claim = claimOf(t.entryId());
            long now = nowMillis();
            boolean holding = claim != null && claim.holder().equals(actor.id())
                    && claim.epoch() == t.epoch && !claim.expired(now);
            EntryState state = crdt.state(t.entryId()).orElse(null);
            if (!holding || state == null || !state.present()) {
                throw new LeaseExpiredException(
                        "take lease lapsed or lost for entry " + t.entryId());
            }
            // The holder this take displaced completed the entry after this take was judged (see
            // outbidCompletions): the completion below acknowledges that one, and the result is not written.
            Long outbid = outbidCompletions.remove(t.entryId());
            doneByOutbid = outbid != null && outbid == t.epoch;
            if (!doneByOutbid) {
                // This holder does the work, so a displaced holder's completion that arrives later is a duplicate.
                completedHere.put(t.entryId(), t.epoch);
            }
            setCrdt(crdt.complete(t.entryId()), t.entryId());
            bump(t.entryId());
            completedValue = valueCache.get(t.entryId());
            // The completed state is signed by us, the holder; the receiver
            // authenticates it against our winning claim, which we carry along.
            dto = signedDto(t.entryId(),
                    issuerKeys.getOrDefault(t.entryId(), identity.rawPublicKey()), actor);
            proof = heldClaim(t.entryId());
            valueCache.remove(t.entryId()); // #11: stop pinning the completed value
        }
        publishDelta(new SpaceWire.Delta(wireForm(t.entryId(), dto), t.entryId(), proof), "d:" + t.entryId());
        fire(SpaceEvent.Kind.COMPLETED, t.entryId(),
                completedValue == null ? t.entry() : completedValue,
                dto.record().issuer(), actor.id(), dto.record());
        if (doneByOutbid) {
            outbidAcknowledged.incrementAndGet();
            throw new LeaseExpiredException("entry " + t.entryId() + " was completed first by the holder this take"
                    + " outbid or out-raced; this take acknowledged that completion and wrote no result");
        }
        return preparedResult == null ? null : commitWrite(preparedResult);
    }

    // ------------------------------------------------------------ delta handling

    private void onDelta(PeerId from, byte[] payload) {
        SpaceWire.Delta delta;
        try {
            // HLC round six, 2026-11-08 (owner rulings F1 and F3): only the delta's own wire encoding
            delta = codec.fromWire(payload, SpaceWire.Delta.class);
        } catch (RuntimeException e) {
            deltasUndecodable.incrementAndGet(); // HLC canonical stamps, 2026-11-08
            return;
        }
        if (delta == null) {
            deltasUndecodable.incrementAndGet();
            return;
        }
        // Merge the claim first: a completion state authenticates against the
        // holder's claim, which must be present before the state is verified.
        boolean claimVerified = delta.claim() != null && delta.claimEntry() != null
                && verifyClaim(delta.claimEntry(), delta.claim());
        if (claimVerified) {
            mergeClaim(delta.claimEntry(), delta.claim());
            noteReplicated(delta.claimEntry()); // a remote took the entry, so it holds the record (PD-L-01)
        }
        EntryId judged = claimVerified ? delta.claimEntry() : null;
        if (delta.state() != null) {
            boolean merged = mergeState(delta.state());
            if (!merged && claimVerified) {
                noteOutbidCompletion(delta.claimEntry(), delta.claim(), delta.state());
            }
            if (!merged && delta.state().completed() && rememberUnproven(delta.state())) {
                judged = delta.state().entryIdOf();
            }
            staleOffered(delta.state());
        }
        if (judged != null) {
            judgeDisplaced(judged);
        }
        offerDisplaced();
    }

    /**
     * Keeps a completion this replica dropped while one of this peer's agents holds its entry's claim
     * ({@link #unprovenCompletions}): it may be a displaced holder's, whose claim reaches this replica in another frame.
     *
     * @return whether a displaced claim is known for the entry, so the pair is worth judging now
     */
    private boolean rememberUnproven(SpaceWire.EntryStateDto dto) {
        EntryId entryId = dto.entryIdOf();
        if (entryId == null || dto.stateSig() == null) {
            return false;
        }
        synchronized (lock) {
            TakeClaim held = claimOf(entryId);
            if (held == null || !held.holder().peer().equals(identity.peerId())
                    || held.holder().encoded().equals(dto.signer())) {
                return false; // not this peer's claim, or the held claim's own completion coming back
            }
            List<SpaceWire.EntryStateDto> lines = unprovenCompletions.computeIfAbsent(entryId, k -> new ArrayList<>(2));
            if (lines.stream().noneMatch(d -> Arrays.equals(d.stateSig(), dto.stateSig()))) {
                if (lines.size() >= DISPLACED_PER_ENTRY) {
                    lines.remove(0);
                }
                lines.add(dto);
            }
            return displacedClaims.containsKey(entryId);
        }
    }

    /**
     * Judges each completion dropped here for an entry against each claim that this peer's claim displaced for it
     * (rule two of SPEC §7.4, the store race of 2026-11-22): the pair authenticates a displaced holder's completion
     * as the delta that first carried both would have, whichever frames brought them. {@link #noteOutbidCompletion}
     * makes every check it makes for that delta.
     */
    private void judgeDisplaced(EntryId entryId) {
        List<SpaceWire.SignedClaim> claims;
        List<SpaceWire.EntryStateDto> states;
        synchronized (lock) {
            List<SpaceWire.SignedClaim> c = displacedClaims.get(entryId);
            List<SpaceWire.EntryStateDto> d = unprovenCompletions.get(entryId);
            if (c == null || d == null) {
                return;
            }
            claims = List.copyOf(c);
            states = List.copyOf(d);
        }
        for (SpaceWire.EntryStateDto dto : states) {
            for (SpaceWire.SignedClaim claim : claims) {
                if (dto.signer() == null || claim.claim().holder().encoded().equals(dto.signer())) {
                    noteOutbidCompletion(entryId, claim, dto);
                }
            }
        }
    }

    /**
     * A state that shows the entry present, received while one of this peer's completions of it waits in
     * {@link #displacedOwn}: the replica it came from, or one before it, has not heard of the completion, so the
     * displaced claim is due again once its backoff has passed.
     */
    private void staleOffered(SpaceWire.EntryStateDto dto) {
        if (dto == null || dto.completed() || !anyDisplacedOwn) {
            return;
        }
        EntryId entryId = dto.entryIdOf();
        if (entryId == null) {
            return;
        }
        synchronized (lock) {
            Reoffer reoffer = displacedOwn.get(entryId);
            if (reoffer != null && localMillis() >= reoffer.nextMillis) {
                reofferDue = true;
            }
        }
    }

    /**
     * Offers again each displaced claim of {@link #displacedOwn} that is due, as the claim delta it first travelled
     * in, and moves its next offer back; drops a line whose offers are spent, whose claim lapsed, or whose stamp
     * nears the drift ceiling, past which a replica holding the better claim would refuse the claim as a far-past
     * contest (§7.4, §11), or whose epoch the ordered log decided. Called after the lock.
     */
    private void offerDisplaced() {
        if (!reofferDue) {
            return;
        }
        List<Map.Entry<EntryId, SpaceWire.SignedClaim>> due = new ArrayList<>();
        synchronized (lock) {
            reofferDue = false;
            long now = nowMillis();
            long localNow = localMillis();
            Iterator<Map.Entry<EntryId, Reoffer>> lines = displacedOwn.entrySet().iterator();
            while (lines.hasNext()) {
                Map.Entry<EntryId, Reoffer> line = lines.next();
                Reoffer reoffer = line.getValue();
                TakeClaim claim = reoffer.claim.claim();
                Long decided = logDecidedEpochs.get(line.getKey());
                if (reoffer.offers >= REOFFER_MAX || claim.expired(now)
                        || now - claim.stamp().physical() > HybridLogicalClock.MAX_DRIFT_MILLIS / 2
                        || crdt.state(line.getKey()).isEmpty()
                        || (decided != null && claim.epoch() <= decided)) { // the ordered log alone names the holder
                    lines.remove();
                } else if (localNow >= reoffer.nextMillis) {
                    reoffer.nextMillis = localNow + (REOFFER_BACKOFF_MILLIS << reoffer.offers);
                    reoffer.offers++;
                    due.add(Map.entry(line.getKey(), reoffer.claim));
                }
            }
            anyDisplacedOwn = !displacedOwn.isEmpty();
        }
        for (Map.Entry<EntryId, SpaceWire.SignedClaim> line : due) {
            reoffersSent.incrementAndGet();
            publishDelta(new SpaceWire.Delta(null, line.getKey(), line.getValue()),
                    "c:" + line.getKey() + ":" + line.getValue().claim().stamp().encoded());
        }
    }

    /**
     * Whether one of this peer's takes of the entry is to acknowledge a displaced holder's authentic completion
     * rather than do the work again (rule two of SPEC §7.4): its take comes back empty, or its {@code complete}
     * writes no result. For an operator, and for a test that makes the order of a race.
     *
     * @param entryId the entry
     * @return whether such a completion is noted for the claim this replica holds
     */
    public boolean knowsDisplacedCompletion(EntryId entryId) {
        synchronized (lock) {
            TakeClaim held = claimOf(entryId);
            return held != null && Long.valueOf(held.epoch()).equals(outbidCompletions.get(entryId));
        }
    }

    /**
     * For tests: this peer's held claim of an entry is displaced, as a better claim merged here would make it
     * ({@link #noteDisplaced}), and the first offer goes at once.
     */
    void noteDisplacedOwnForTest(EntryId entryId) {
        synchronized (lock) {
            displacedOwn.put(entryId, new Reoffer(heldClaim(entryId), localMillis()));
            anyDisplacedOwn = true;
            reofferDue = true;
        }
        offerDisplaced();
    }

    /** For tests: a replica offered the entry present again ({@link #staleOffered}); a due offer goes. */
    void staleOfferForTest(EntryId entryId) {
        synchronized (lock) {
            Reoffer reoffer = displacedOwn.get(entryId);
            if (reoffer != null && localMillis() >= reoffer.nextMillis) {
                reofferDue = true;
            }
        }
        offerDisplaced();
    }

    /** For tests: a holder this peer's claim displaced, as {@link #noteDisplaced} records one; its round is due now. */
    void noteDisplacedHolderForTest(EntryId entryId, PeerId holder) {
        synchronized (lock) {
            long now = localMillis();
            DisplacedPull pull = displacedPulls.computeIfAbsent(entryId, k -> new DisplacedPull(now));
            pull.holders.add(holder);
            lowerDue(pullDue, pull.nextMillis);
        }
    }

    /** Offers of a displaced claim this replica made again ({@link #displacedOwn}); package-private, for tests. */
    long displacedReoffers() {
        return reoffersSent.get();
    }

    /**
     * Judges a completion this replica dropped because a claim held by one of this peer's agents displaces, in the
     * lattice, the claim the completion travelled with, when that claim authenticates it. The displaced holder
     * judged its take before the better claim reached it: an AUCTION incumbent a better bid outbid, or, under either
     * strategy, a claimant whose rival's claim arrived later than the settle window (SPEC §7.4, the settle
     * guarantee). The state itself stays dropped, as SPEC §11a.4 has every replica do. While this replica still
     * holds the entry present, the completion is noted in {@link #outbidCompletions}, so this peer's take
     * acknowledges it instead of doing the work a second time. When one of this peer's agents has already completed
     * the entry with its own work ({@link #completedHere}), both did the work: the completion is counted as a
     * duplicate ({@link #duplicateCompletions}), once per entry. A revoked or unadmitted holder's completion is no
     * evidence, and neither is one at an epoch the ordered log decided, where the log alone names the holder.
     *
     * @param claimEntry the entry the delta's claim was sent for
     * @param proof      the delta's claim, already verified
     * @param received   the delta's state as received
     */
    private void noteOutbidCompletion(EntryId claimEntry, SpaceWire.SignedClaim proof,
                                      SpaceWire.EntryStateDto received) {
        if (!received.completed()) {
            return;
        }
        EntryId entryId = received.entryIdOf();
        if (entryId == null || !entryId.equals(claimEntry) || !entryId.equals(proof.claim().entryId())) {
            return;
        }
        SpaceWire.EntryStateDto dto = received;
        TakeClaim displaced = proof.claim();
        boolean duplicate;
        synchronized (lock) {
            TakeClaim held = claimOf(entryId);
            EntryState state = crdt.state(entryId).orElse(null);
            long now = nowMillis();
            Long decided = logDecidedEpochs.get(entryId);
            if (held == null || state == null
                    || !held.holder().peer().equals(identity.peerId()) // only this peer's own holders are judged
                    || held.holder().equals(displaced.holder())
                    // A lower epoch is displaced too: its holder never heard of the claim this replica saw lapse,
                    // so it claimed the entry afresh, which only late delivery allows.
                    || displaced.epoch() > held.epoch()
                    || TakeClaim.merge(held, displaced) != held // the held claim displaces this one
                    || (decided != null && displaced.epoch() <= decided)) {
                return;
            }
            if (state.present()) {
                if (held.expired(now) || displaced.expired(now)
                        || Long.valueOf(held.epoch()).equals(outbidCompletions.get(entryId))) {
                    return;
                }
                duplicate = false;
            } else if (state.completed() && Long.valueOf(held.epoch()).equals(completedHere.get(entryId))) {
                duplicate = true;
            } else {
                return;
            }
            if (dto.record() == null) {
                if (dto.issuerPublicKey() == null || dto.adds() == null || dto.removes() == null) {
                    return;
                }
                dto = received.withRecord(state.record()); // PD-L-02: a recordless completion, over the held record
            }
        }
        if (runtime.revocationView().refuses(displaced.holder(), claimKey(proof), stateSigningTime(dto, null))
                || !admits(displaced.holder(), SpaceAdmission.Scope.TAKE, nowMillis())
                || !id.equals(dto.record().spaceId())
                || !verifyState(dto, proof, true)) {
            return;
        }
        boolean counted = false;
        synchronized (lock) {
            TakeClaim held = claimOf(entryId);
            if (held == null || held.epoch() < displaced.epoch()) {
                return;
            }
            if (duplicate) {
                counted = completedHere.remove(entryId, held.epoch());
            } else if (crdt.state(entryId).map(EntryState::present).orElse(false)) {
                outbidCompletions.put(entryId, held.epoch());
            }
        }
        if (counted) {
            duplicateCompletions.incrementAndGet();
            LOG.log(System.Logger.Level.INFO, () -> "space " + name + ": entry " + entryId.value() + " was completed"
                    + " twice: here, and by " + displaced.holder() + ", whose claim lost the race but heard of the"
                    + " winner later than the settle window (SPEC 7.4); the shared state keeps one completion");
        }
    }

    /**
     * Records evidence that a remote replica holds an entry's record (PD-L-01), after
     * which this replica's renews, cancels, and completes of it may omit the record.
     */
    private void noteReplicated(EntryId entryId) {
        synchronized (lock) {
            if (crdt.state(entryId).isPresent()) {
                replicated.add(entryId);
                ownWrites.remove(entryId);
            }
        }
    }

    /**
     * The form a renew, cancel, or complete takes on the wire (srad/agentspaces-pdt.md
     * §3.9, PD-L-01, PD-L-03): without the record when this replica has evidence the
     * record replicated and every member of its view advertises {@code wire.recordless};
     * else the full DTO, as 0.5.0 sends it. The signed state kept for anti-entropy is
     * always the full form.
     */
    private SpaceWire.EntryStateDto wireForm(EntryId entryId, SpaceWire.EntryStateDto dto) {
        if (!recordless || dto.record() == null) {
            return dto;
        }
        if (!replicatedEvidence(entryId) || !runtime.viewAdvertisesRecordless()) {
            return dto;
        }
        recordlessSent.incrementAndGet();
        return dto.withoutRecord();
    }

    /**
     * Whether this replica has evidence that a remote replica holds the entry's record
     * (PD-L-01): a remote state, claim, or digest line for it, or a hashed anti-entropy
     * round (SPEC §5.3, v0.1.17) a partner opened whose hash for this space's stream equaled
     * this replica's while the entry was in its digest ({@link #digestMatched}): the partner
     * then holds exactly what this replica holds, the write included. Bus idleness is no
     * evidence (a lone or partitioned writer is idle too), so a write on a quiet fleet
     * travels in full until a partner's hash matches.
     */
    boolean replicatedEvidence(EntryId entryId) { // package-private for tests
        synchronized (lock) {
            return replicated.contains(entryId);
        }
    }

    /**
     * A partner's hashed digest matched this replica's for the space's stream (PD-L-01):
     * every own write in the digest at that moment is held by the partner, so each of
     * them has the evidence a recordless renew, cancel, or complete needs.
     */
    private void digestMatched(PeerId from) {
        synchronized (lock) {
            if (ownWrites.isEmpty()) {
                return;
            }
            for (EntryId entryId : new ArrayList<>(ownWrites.keySet())) {
                if (crdt.state(entryId).isPresent()) {
                    replicated.add(entryId);
                }
                ownWrites.remove(entryId);
            }
        }
    }

    /** Recordless states this replica published (PD-L-01); for tests and the console. */
    public long recordlessSent() {
        return recordlessSent.get();
    }

    /** Takes here that acknowledged an outbid holder's completion instead of doing the work again (package-private). */
    long outbidAcknowledged() {
        return outbidAcknowledged.get();
    }

    /**
     * Entries this replica completed with its own work whose displaced rival's completion arrived afterwards: the
     * duplicate completion that SPEC §7.4 leaves possible when a claim reaches a claimant later than the settle
     * window. The shared state keeps one completion; the count is how an operator, or a test, sees that the work and
     * any result were done twice. Counted once per entry, at the replica whose claim won the lattice.
     *
     * @return the count since this space was built
     */
    public long duplicateCompletions() {
        return duplicateCompletions.get();
    }

    /** Recordless states this replica merged over a held record (PD-L-02). */
    public long recordlessMerged() {
        return recordlessMerged.get();
    }

    /** Recordless states this replica held pending for their record (PD-L-02). */
    public long recordlessHeld() {
        return recordlessHeld.get();
    }

    /**
     * Distinct folded states, gossip claims, and ordered-log claims this replica refused
     * because a stamp carried a logical counter above {@link HybridLogicalClock#LOGICAL_LIMIT}
     * (HLC logical counter, 2026-11-08, Rust team report). A re-offer of the same state or
     * claim counts once; a state or claim whose signature does not verify never counts.
     */
    public long stampsLogicalRefused() {
        return stampsLogicalRefused.get();
    }

    /**
     * Space deltas this replica dropped because they did not decode, by rumor or by anti-entropy
     * (HLC canonical stamps, 2026-11-08): bytes that are no delta, or a delta holding a stamp that
     * is not canonical text. Such a delta names no verified signer, so it strikes nobody.
     */
    public long deltasUndecodable() {
        return deltasUndecodable.get();
    }

    /**
     * Distinct folded states and gossip claims this replica refused as hostile because a stamp
     * lies more than {@link #FAR_FUTURE_HOSTILE_MILLIS} past its far-future margin (HLC round
     * four, 2026-11-08, decision D4). Each verified first, was acknowledged, and struck its signer.
     */
    public long stampsFarFutureRefused() {
        return stampsFarFutureRefused.get();
    }

    /**
     * Distinct folded states and gossip claims this replica deferred because a stamp lies past
     * its wall time plus {@link HybridLogicalClock#MAX_DRIFT_MILLIS} plus the skew margin, by at
     * most {@link #FAR_FUTURE_HOSTILE_MILLIS} (HLC round four, 2026-11-08, decision D4). Such a
     * state is neither merged nor acknowledged nor struck; it merges when anti-entropy offers it
     * after this replica's wall catches up.
     */
    public long stampsFarFutureDeferred() {
        return stampsFarFutureDeferred.get();
    }

    /**
     * Renewals of a write or take lease on this replica that asked for less than the lease had left (RE-H-01): the
     * count of every such call since the space opened. A renewal never shortens a lease (SPEC §7.2, RE-H-02), so each
     * of these kept the longer lease; the count names callers that still ask for less.
     */
    public long renewShortenings() {
        return renewShortenings.get();
    }

    /**
     * Re-offered states this replica refused at the door because their entry's tombstone is past its horizon and no
     * longer held here, and acknowledged so the member offering them stops (RE-H-03, SPEC §7.3).
     */
    public long collectedReoffersAcknowledged() {
        return collectedReoffersAcknowledged.get();
    }

    /**
     * Counts a renewal that asked for less than its lease had left, and logs the first one of this space unless the
     * node turned the log off ({@code agentspaces.space.renew-shortening-log}).
     */
    private void noteShortening(EntryId entryId, String lease, long currentExpiryMillis, long requestedExpiryMillis) {
        if (requestedExpiryMillis >= currentExpiryMillis) {
            return;
        }
        renewShortenings.incrementAndGet();
        if (runtime.renewShorteningLog() && renewShorteningLogged.compareAndSet(false, true)) {
            LOG.log(System.Logger.Level.INFO, "space " + name + ": the " + lease + " lease of entry " + entryId.value()
                    + " was renewed for " + (currentExpiryMillis - requestedExpiryMillis)
                    + " ms less than it had left, and kept the longer lease: a renewal never shortens a lease"
                    + " (SPEC §7.2). Cancel and write again for a shorter one. Logged once for this space;"
                    + " renewShortenings() counts every such renewal.");
        }
    }

    /**
     * A snapshot of what this replica holds and what its anti-entropy has cost (RE-Q-MNT-02, RE-F-03 in
     * srad/replication-by-entry.md): the source for a member's status figures and for the signals a pacing adjuster
     * reads. Taken under the space lock in one pass over the known entries, so it costs O(n) and is meant for a status
     * page or a test, not for every round.
     *
     * @return the snapshot
     */
    public Stats stats() {
        long now = nowMillis(); // read-only: no sweep runs, so a due tombstone not yet collected is counted as held
        synchronized (lock) {
            int present = 0;
            int lapsed = 0;
            int withdrawn = 0;
            int completed = 0;
            int leaseRunning = 0;
            int[] ages = new int[4];
            for (EntryId entryId : crdt.entryIds()) {
                EntryState state = crdt.state(entryId).orElseThrow();
                long expiry = state.lease().value().expiresAtMillis();
                if (state.present()) {
                    if (expiry > now) {
                        present++;
                        continue;
                    }
                    lapsed++;
                } else if (state.completed()) {
                    completed++;
                } else {
                    withdrawn++;
                }
                if (expiry > now) {
                    leaseRunning++; // withdrawn or completed before its lease ran out: not yet past expiry
                    continue;
                }
                ages[ageBucket(now - expiry)]++;
            }
            return new Stats(present, lapsed, withdrawn, completed,
                    new TombstonesPastExpiry(leaseRunning, ages[0], ages[1], ages[2], ages[3]),
                    crdt.claimCount(), refusedAckOrder.size(), shardDroppedOrder.size(), pendingStates.size(),
                    maxLeaseObservedMillis, maxLeaseMillis(),
                    digestCacheHits, digestCacheMisses, digestComputations, digestHashHits, digestHashMisses,
                    syncStatesSent.get(), syncClaimsSent.get(), syncStatesReceived.get(), syncClaimsReceived.get(),
                    renewShortenings.get());
        }
    }

    /**
     * The space's status figures (srad/replication-by-entry.md RE-Q-MNT-02): live entries, tombstones and their ages
     * past expiry from {@link #stats()}, and the depth of the audit's trie. O(n) under the lock, like {@link #stats()}.
     *
     * @return the figures
     */
    ReconcilableState.Figures figures() {
        Stats stats = stats();
        TombstonesPastExpiry ages = stats.tombstonesPastExpiry();
        Map<String, Long> buckets = new java.util.LinkedHashMap<>();
        buckets.put("leaseRunning", (long) ages.leaseRunning());
        buckets.put("underOneMinute", (long) ages.underOneMinute());
        buckets.put("oneToTenMinutes", (long) ages.oneToTenMinutes());
        buckets.put("tenMinutesToAnHour", (long) ages.tenMinutesToAnHour());
        buckets.put("overAnHour", (long) ages.overAnHour());
        int depth;
        synchronized (lock) {
            depth = crdt.trieDepth();
        }
        return new ReconcilableState.Figures(stats.present(), stats.tombstones(), buckets, depth);
    }

    private static int ageBucket(long ageMillis) {
        if (ageMillis < 60_000L) {
            return 0;
        }
        if (ageMillis < 600_000L) {
            return 1;
        }
        return ageMillis < 3_600_000L ? 2 : 3;
    }

    /**
     * Tombstones by the time since their lease expired, where a tombstone is a known entry no reader sees (lapsed,
     * withdrawn, or completed). This is time past lease expiry, not time held as a tombstone: a withdrawn or completed
     * entry keeps the lease it had, and its state carries dots but no time of withdrawal or completion, so a replica
     * cannot tell how long ago it stopped being served. The collection horizon (SPEC §7.3) is counted from the same
     * expiry, so these buckets say how near the tombstones are to collection. A withdrawn or completed entry whose
     * lease has not yet run out is counted apart in {@code leaseRunning}, not filed as a fresh one.
     *
     * @param leaseRunning       withdrawn or completed entries whose lease has not yet expired
     * @param underOneMinute     expired less than a minute ago
     * @param oneToTenMinutes    one to ten minutes ago
     * @param tenMinutesToAnHour ten minutes to an hour ago
     * @param overAnHour         an hour ago or more
     */
    public record TombstonesPastExpiry(int leaseRunning, int underOneMinute, int oneToTenMinutes,
                                       int tenMinutesToAnHour, int overAnHour) {
        /** Every tombstone counted. */
        public int total() {
            return leaseRunning + underOneMinute + oneToTenMinutes + tenMinutesToAnHour + overAnHour;
        }
    }

    /**
     * What {@link #stats()} returns.
     *
     * @param present                entries a reader sees: present with an unexpired write lease
     * @param lapsedInGrace          present entries whose write lease lapsed, kept for the lapse grace (SPEC §7.3)
     * @param withdrawn              withdrawn entries kept as tombstones
     * @param completed              completed entries kept as tombstones
     * @param tombstonesPastExpiry   the lapsed, withdrawn and completed entries by the time since their lease expired
     * @param claims                 take claims held
     * @param acknowledgementLines   refused-and-acknowledged digest lines ({@code a:}), bounded
     * @param shardDroppedLines      out-of-shard states acknowledged in the digest, bounded
     * @param pendingRecordless      recordless states waiting for their record
     * @param maxLeaseObservedMillis the longest lease seen in this space, capped as for the collection horizon
     * @param maxLeaseMillis         the lease the collection horizon uses: the builder's override or the observed one
     * @param digestCacheHits        full-digest asks (a digest sent whole) answered from the cache; a digest-hash ask
     *                               is not counted here, so the two pairs are disjoint
     * @param digestCacheMisses      full-digest asks that found the cache stale
     * @param digestComputations     digests built, whichever ask found the cache stale: the rebuild count
     * @param digestHashHits         digest-hash asks (a probe, a hashed opening or its answer) answered from the cache
     * @param digestHashMisses       digest-hash asks that hashed the digest again, after rebuilding it when it was
     *                               stale too
     * @param syncStatesSent         signed states offered in anti-entropy deltas
     * @param syncClaimsSent         signed claims offered in anti-entropy deltas
     * @param syncStatesReceived     states received in anti-entropy deltas, merged or not
     * @param syncClaimsReceived     claims received in anti-entropy deltas, merged or not
     * @param renewShortenings       renewals that asked for less than the lease had left ({@link #renewShortenings()})
     */
    public record Stats(int present, int lapsedInGrace, int withdrawn, int completed,
                        TombstonesPastExpiry tombstonesPastExpiry,
                        int claims, int acknowledgementLines, int shardDroppedLines, int pendingRecordless,
                        long maxLeaseObservedMillis, long maxLeaseMillis,
                        long digestCacheHits, long digestCacheMisses, long digestComputations,
                        long digestHashHits, long digestHashMisses,
                        long syncStatesSent, long syncClaimsSent, long syncStatesReceived, long syncClaimsReceived,
                        long renewShortenings) {
        /** Every entry this replica knows, tombstones included. */
        public int known() {
            return present + lapsedInGrace + withdrawn + completed;
        }

        /** The lapsed, withdrawn and completed entries. */
        public int tombstones() {
            return lapsedInGrace + withdrawn + completed;
        }
    }

    /** Recordless states waiting here for their record now. */
    public int pendingRecordless() {
        synchronized (lock) {
            return pendingStates.size();
        }
    }

    /** Most dots a single received state may carry per set (ASF-025): honest
     * entries hold a handful; a validly signed DTO inflated with fabricated
     * dots would otherwise be stored and re-sorted forever. */
    static final int MAX_DOTS_PER_STATE = 1_024;

    /**
     * Merges one received state.
     *
     * @return whether the replica's state changed: false when the state was dropped, refused, deferred, held pending
     *         its record, or already held here (IDLE-WORK item 8: such an offer is no activity)
     */
    private boolean mergeState(SpaceWire.EntryStateDto received) {
        return mergeState(received, null);
    }

    /**
     * Merges one received state, with the checks {@link #applySyncDelta} already made before it took the lock.
     *
     * @param checked the outcome of the signature checks that read nothing of this replica's state, or null to make
     *                them here
     */
    private boolean mergeState(SpaceWire.EntryStateDto received, Prechecked checked) {
        if ((received.adds() != null && received.adds().size() > MAX_DOTS_PER_STATE)
                || (received.removes() != null && received.removes().size() > MAX_DOTS_PER_STATE)) {
            return false; // inflated dot set: hostile, drop before verification cost
        }
        if (received.leaseStamp() == null || received.leaseValue() == null) {
            // HLC round five, 2026-11-08 (decision E7): such a state decodes, as in every
            // implementation, but a lease register holds nothing without its stamp and value.
            // Dropped before any judgement: it merges nothing, counts nothing, strikes nobody.
            return false;
        }
        if (dropsControlId(received.entryIdOf())) {
            return false; // annex I.6: dropped like the state above, uncounted and unacknowledged
        }
        boolean hadRecord = received.record() != null;
        SpaceWire.EntryStateDto dto = received;
        if (!hadRecord) {
            // PD-L-02: a recordless state is merged over the record this replica holds, or
            // waits for the record; it never creates an entry and never fires an event alone.
            EntryId named = dto.entryIdOf();
            if (named == null || dto.issuerPublicKey() == null || dto.adds() == null || dto.removes() == null) {
                return false;
            }
            EntryRecord held;
            synchronized (lock) {
                held = crdt.state(named).map(EntryState::record).orElse(null);
                if (held == null) {
                    keepPending(named, dto);
                    return false;
                }
            }
            dto = received.withRecord(held);
            recordlessMerged.incrementAndGet();
        }
        final SpaceWire.EntryStateDto full = dto;
        // A check made before the lock covers the state as it was received; a recordless state gets its record here.
        Prechecked pre = hadRecord ? checked : null;
        if (dto.record() == null || dto.issuerPublicKey() == null
                || !id.equals(dto.record().spaceId()) // a record from another space is not ours (QA4 A4-11)
                || !dotsBoundToIssuer(dto)
                || !(pre != null ? pre.record() : verifyRecord(dto.record(), dto.issuerPublicKey(),
                        dto.agentCertificate()))) {
            return false;
        }
        // A completion is judged against the claim held now, which an earlier claim in the same delta may have set.
        // The check made before the lock named the claim it expected; when that is the claim held, its answer stands,
        // so a delta of completions verifies no signature under the lock (RE-M-08).
        boolean authentic;
        if (pre == null) {
            authentic = verifyState(dto);
        } else if (!dto.completed()) {
            authentic = pre.state();
        } else {
            SpaceWire.SignedClaim held;
            synchronized (lock) {
                held = heldClaim(dto.record().entryId());
            }
            authentic = sameProof(held, pre.against()) ? pre.state() : verifyState(dto, held, false);
        }
        if (!authentic) {
            // Drop any state whose mutable fields are not authenticated by the
            // party authorized for the transition: this is what makes a forged
            // completion, removal, or lease unable to delete a signed entry.
            return false;
        }
        // After verifyState: the revocation judgement reads the state's signing
        // time (signedAt, the lease stamp), so those must be authenticated, and
        // only an authentic state may earn a refused-and-acknowledged line. HLC
        // round five, 2026-11-08 (decision E6): before the stamp checks, as for a
        // claim, so a revoked actor's state counts in no stamp counter and strikes
        // nobody, whatever its stamps.
        if (refusedForRevocation(dto)) {
            return false; // ASF-047: authentic, but its actor is revoked in this group
        }
        if (!stampsAcceptable(dto)) {
            // HLC logical counter, 2026-11-08 (Rust team report): a record or lease
            // stamp whose logical counter is above LOGICAL_LIMIT froze this space's
            // clock below, and the state then carried the stamp on to every replica
            // through gossip and anti-entropy. The space judges it after
            // verification, so the refusal names the state's signer.
            refuseState(dto, !HybridLogicalClock.isAcceptable(dto.record().issued()), LOGICAL_VIOLATION,
                    stampsLogicalRefused);
            return false;
        }
        // HLC round four, 2026-11-08 (decision D4): a state stamped past the far-future margin
        // waits for this replica's wall, unacknowledged, so anti-entropy offers it again; one
        // stamped an hour beyond the margin is hostile. Judged after verification, so a refusal
        // names the state's signer, and before the stamps reach the space clock.
        // HLC round six, 2026-11-08 (owner ruling F4): an agent-signed state's signedAt is
        // judged with its lease stamp, since the same party signs both.
        Lead issuedLead = lead(dto.record().issued().physical(), nowMillis());
        Lead leaseLead = lead(dto.leaseStamp().physical(), nowMillis());
        Lead signedLead = dto.signedAt() == null ? Lead.WITHIN : lead(dto.signedAt().physical(), nowMillis());
        if (issuedLead == Lead.HOSTILE || leaseLead == Lead.HOSTILE || signedLead == Lead.HOSTILE) {
            refuseState(dto, issuedLead == Lead.HOSTILE, FAR_FUTURE_VIOLATION, stampsFarFutureRefused);
            return false;
        }
        if (issuedLead == Lead.DEFERRED || leaseLead == Lead.DEFERRED || signedLead == Lead.DEFERRED) {
            boolean first;
            synchronized (lock) {
                first = deferredOffers.add(stateOfferKey(dto));
            }
            if (first) {
                stampsFarFutureDeferred.incrementAndGet();
            }
            return false;
        }
        EntryId entryId = dto.record().entryId();
        if (!admitsState(dto)) {
            return false; // spec §7.5: authentic, but its actor is not admitted here
        }
        observeLease(dto.leaseValue().expiresAtMillis() - dto.leaseStamp().physical());
        long doorNow = nowMillis();
        EntryState offered = dto.toState();
        if (collectableHere(offered, doorNow, horizon(doorNow))) {
            // Spec §7.3: a tombstone past the GC horizon, re-offered by a lagging
            // replica. Refusing it at the door is what keeps a collected tombstone
            // from bouncing between replicas that sweep at different moments. The
            // door reads the same rule and the same clock as this replica's sweep
            // (RE-H-04), so under the per-entry horizon a present copy older than
            // a collected withdrawal is past its own lapse horizon here, and refused.
            acknowledgeCollected(entryId, dto);
            return false;
        }
        // Spec §11: remote stamps feed the space clock under the bounded-drift
        // regime (a far-future stamp is clamped, never adopted).
        hlc.update(dto.record().issued());
        hlc.update(dto.leaseStamp());
        if (tagShard != null && !credentialSchema.equals(dto.record().type())
                && !tagShard.test(dto.record().tags())) {
            // Out of shard: acknowledge the state in our digest so anti-entropy
            // partners stop offering it, but never store the record. Credential
            // entries are admission infrastructure and are stored on every shard.
            synchronized (lock) {
                if (!crdt.state(entryId).isPresent()) {
                    dropShard(entryId, stateHash(dto.toState()));
                    bump();
                    return false;
                }
            }
        }
        boolean changed;
        boolean isNew;
        boolean nowCompleted;
        boolean visible;
        Runnable withdrawn;
        AgentId takenBy = null;
        synchronized (lock) {
            boolean wasKnown = crdt.state(entryId).isPresent();
            boolean wasCompleted = crdt.state(entryId).map(EntryState::completed).orElse(false);
            boolean wasPresent = crdt.state(entryId).map(EntryState::present).orElse(false);
            // A state this replica already holds (a re-offer by anti-entropy, a rumor that came twice by two paths)
            // changes nothing: the map is not copied, the digest cache stays good, and the caller counts no
            // activity (IDLE-WORK items 6 and 8). The merge keeps the held record, so equality is exact.
            EntryState before = crdt.state(entryId).orElse(null);
            EntryState incoming = dto.toState();
            changed = before == null || !before.merge(incoming).equals(before);
            if (changed) {
                setCrdt(crdt.merge(entryId, incoming), entryId);
            }
            // An entry known only as a claim until now enters the digest with its issuer key, so that is a change too.
            if (issuerKeys.putIfAbsent(entryId, dto.issuerPublicKey()) == null) {
                changed = true;
            }
            if (changed) {
                bump(entryId);
            }
            if (dto.agentCertificate() != null) {
                issuerCertificates.putIfAbsent(entryId, dto.agentCertificate());
            }
            recordSignedState(entryId, dto);
            indexCredential(entryId);
            // PD-L-01: the sender holds the record, whether it sent it or merged over its own copy.
            replicated.add(entryId);
            ownWrites.remove(entryId);
            isNew = !wasKnown;
            if (isNew && blocks != null && dto.record().payloadRef() != null && !isBulkRef(dto.record())) {
                blocks.retain(dto.record().payloadRef()); // LB-B-04
            }
            EntryState merged = crdt.state(entryId).orElseThrow();
            nowCompleted = !wasCompleted && merged.completed();
            // WRITTEN is for an entry a reader could see: present and unexpired.
            // A withdrawn or lapsed entry learned late is history, not news.
            visible = merged.present() && merged.lease().value().expiresAtMillis() > nowMillis();
            if (isNew && visible) {
                // A watcher's TAKEN (S2-H-01) for a claim that merged before this record: a batch merges its claims
                // before its states, and gossip may bring a claim ahead of its entry. It fires once, after the WRITTEN,
                // while the claim is in force (R1 probe P2, reviews/RBE-REGRESSION-MATRIX.md).
                TakeClaim inForce = claimOf(entryId);
                if (inForce != null && !inForce.expired(nowMillis())
                        && !Long.valueOf(inForce.epoch()).equals(takenEpochs.get(entryId))) {
                    takenEpochs.put(entryId, inForce.epoch());
                    takenBy = inForce.holder();
                }
            }
            // Core cancel events, 2026-11-09: a merged cancel of an entry present here raises EXPIRED once.
            withdrawn = withdrawnEvent(entryId, wasPresent);
            if (!merged.present() && !merged.completed()) {
                // IDLE-WORK 5a(e): a withdrawn entry keeps no decoded value (a completion drops its own below,
                // after its event has used it).
                valueCache.remove(entryId);
            }
        }
        // The events, after the merge and outside the lock; after the whole delta when it is one of many (RE-M-08).
        boolean newAndVisible = isNew && visible;
        boolean completedHere = nowCompleted;
        AgentId takenHolder = takenBy;
        later(() -> {
            if (withdrawn != null) {
                withdrawn.run();
            }
            if (newAndVisible) {
                Optional<Object> value = decodeValue(full.record());
                if (value.isPresent()) {
                    writtenThenTaken(entryId, value.get(), full.record(), takenHolder);
                } else if (full.record().payload() == null && isBulkRef(full.record())) {
                    // LB-E-03: a bulk entry fires its WRITTEN once the artifact is complete here.
                    EntryRecord record = full.record();
                    if (seamFor(record.payloadRef()) != null) {
                        awaitingPayload.put(entryId, Boolean.TRUE);
                        ensureBulk(record, () -> payloadArrived(entryId, record, takenHolder));
                    }
                } else if (full.record().payload() == null && full.record().payloadRef() != null
                        && blocks != null) {
                    // A content-addressed entry whose block has not landed: fetch it now
                    // and fire the deferred WRITTEN when it does, so a notify subscriber
                    // learns of large entries without anyone having to read (a poll was
                    // the only way before; the flagships all polled for this reason).
                    EntryRecord record = full.record();
                    awaitingPayload.put(entryId, Boolean.TRUE);
                    fetchInBackground(record, () -> payloadArrived(entryId, record, takenHolder));
                }
            }
            if (completedHere) {
                AgentId completer;
                synchronized (lock) {
                    SpaceWire.SignedClaim claim = heldClaim(entryId);
                    completer = claim == null ? full.record().issuer() : claim.claim().holder();
                }
                Optional<Object> value = decodeValue(full.record());
                if (value.isPresent()) {
                    // Raised here, so a deferred arrival still owed for this entry raises nothing more.
                    awaitingPayload.remove(entryId);
                    fire(SpaceEvent.Kind.COMPLETED, entryId, value.get(), full.record().issuer(), completer,
                            full.record());
                }
                // Without the value, an entry whose payload fetch is still owed hears its COMPLETED when the payload
                // lands (payloadArrived); one first learned completed raises nothing (SPEC §7.2, completed on arrival).
                valueCache.remove(entryId); // #11: stop pinning a completed entry's decoded value
            }
        });
        if (hadRecord) {
            changed |= applyPending(entryId);
        }
        return changed;
    }

    /**
     * Fires the WRITTEN of an entry learned from a received state and, when a claim on it merged before its record, the
     * TAKEN of that claim right after it.
     *
     * @param takenBy the holder of the claim in force when the record merged, or null when there was none
     */
    private void writtenThenTaken(EntryId entryId, Object value, EntryRecord record, AgentId takenBy) {
        fire(SpaceEvent.Kind.WRITTEN, entryId, value, record.issuer(), record.issuer(), record);
        if (takenBy != null) {
            fire(SpaceEvent.Kind.TAKEN, entryId, value, record.issuer(), takenBy, record);
        }
    }

    /**
     * The event a merge deferred until a content-addressed or bulk payload landed. The entry was present when its state
     * merged, but the fetch takes time, and in that time the entry may have completed, been withdrawn or lapsed. So the
     * state is read again now: a live entry raises its WRITTEN (and the TAKEN of a claim that came first); an entry that
     * completed meanwhile raises COMPLETED alone, since its completion merged while the value could not be decoded and
     * so raised nothing then; an entry that left in another way raises nothing, as no reader here saw it live (decision
     * 21 of srad/replication-by-entry.md). The {@link #awaitingPayload} key makes this run at most once and lets a
     * completion that could decode the value claim the event first.
     *
     * @param takenBy the holder of the claim in force when the record merged, or null when there was none
     */
    private void payloadArrived(EntryId entryId, EntryRecord record, AgentId takenBy) {
        if (awaitingPayload.remove(entryId) == null) {
            return;
        }
        Optional<Object> value = decodeValue(record);
        if (value.isEmpty()) {
            return;
        }
        boolean live;
        boolean completed;
        AgentId completer = null;
        synchronized (lock) {
            EntryState state = crdt.state(entryId).orElse(null);
            live = state != null && state.present() && state.lease().value().expiresAtMillis() > nowMillis();
            completed = state != null && state.completed();
            if (completed) {
                SpaceWire.SignedClaim claim = heldClaim(entryId);
                completer = claim == null ? record.issuer() : claim.claim().holder();
            }
        }
        if (live) {
            writtenThenTaken(entryId, value.get(), record, takenBy);
        } else if (completed) {
            fire(SpaceEvent.Kind.COMPLETED, entryId, value.get(), record.issuer(), completer, record);
            valueCache.remove(entryId); // #11: stop pinning a completed entry's decoded value
        } else {
            valueCache.remove(entryId); // withdrawn or lapsed: no event, and nothing to keep decoded
        }
    }

    /** Keeps a recordless state for an entry this replica lacks, bounded (PD-L-02). Under {@link #lock}. */
    private void keepPending(EntryId entryId, SpaceWire.EntryStateDto dto) {
        if (!pendingStates.containsKey(entryId) && pendingStates.size() >= MAX_PENDING_STATES) {
            EntryId eldest = pendingStates.keySet().iterator().next();
            pendingStates.remove(eldest);
        }
        long now = nowMillis();
        pendingStates.put(entryId, new PendingState(dto, now));
        pendingDueMillis = Math.min(pendingDueMillis, now + PENDING_STATE_TTL_MILLIS + 1); // the sweep's next look
        lowerDue(sweepDue, pendingDueMillis);
        recordlessHeld.incrementAndGet();
    }

    /** Merges the recordless state that waited for an entry's record, once the record is here (PD-L-02). */
    private boolean applyPending(EntryId entryId) {
        PendingState pending;
        EntryRecord held;
        synchronized (lock) {
            pending = pendingStates.remove(entryId);
            if (pending == null) {
                return false;
            }
            held = crdt.state(entryId).map(EntryState::record).orElse(null);
        }
        if (held != null) {
            recordlessMerged.incrementAndGet();
            return mergeState(pending.dto().withRecord(held));
        }
        return false;
    }

    /**
     * Spec §7.5: the admission check for an inbound state, applied to the actor
     * of the transition it carries. A new entry needs its issuer admitted to
     * {@code WRITE}; a completion of a known entry needs the claim holder
     * admitted to {@code TAKE}; a lease renewal (a newer lease register) needs
     * the issuer admitted to {@code WRITE} again; a withdrawal is never a
     * privilege and passes, the state signature having already proved the
     * issuer authored it. Re-judging an entry that merged while its writer was
     * admitted would let a lapsed or revoked credential retroactively delete
     * the writer's work, so a known entry is only re-judged on a privileged
     * transition. A {@link SpaceCredential} record is honoured only from the
     * configured issuer; from anyone else it is dropped and reported, since
     * the signer issued a credential it may not issue. Any other refusal is
     * reported only when the rule says a refusal cannot be a late credential.
     */
    private boolean admitsState(SpaceWire.EntryStateDto dto) {
        EntryRecord record = dto.record();
        if (credentialRule != null && credentialSchema.equals(record.type())
                && !credentialRule.issuer().equals(record.issuer().peer())) {
            report(record.issuer().peer(), "space credential signed by a non-issuer for space '" + name + "'");
            return false;
        }
        AgentId actor = record.issuer();
        SpaceAdmission.Scope scope = SpaceAdmission.Scope.WRITE;
        synchronized (lock) {
            EntryState local = crdt.state(record.entryId()).orElse(null);
            if (local != null) {
                if (dto.completed() && !local.completed()) {
                    SpaceWire.SignedClaim claim = heldClaim(record.entryId());
                    if (claim == null) {
                        return false; // verifyState guarantees a holder; defensive
                    }
                    actor = claim.claim().holder();
                    scope = SpaceAdmission.Scope.TAKE;
                } else if (dto.completed()
                        || dto.leaseStamp().compareTo(local.lease().stamp()) <= 0) {
                    return true; // already-known state, or a withdrawal: no privilege
                }
                // Otherwise a renewal: the issuer must still be admitted to write.
            }
        }
        if (admits(actor, scope, nowMillis())) {
            return true;
        }
        if (admission.strikesOnRefusal()) {
            // The record (or claim) is authentic, so the refusal is attributable,
            // and the rule rests on local configuration that cannot be late.
            report(actor.peer(), (scope == SpaceAdmission.Scope.TAKE
                    ? "completion by unadmitted agent" : "record from unadmitted issuer")
                    + " for space '" + name + "'");
        }
        return false;
    }

    /**
     * Keeps the {@link CredentialIndex} in step with the CRDT for one entry
     * (spec §7.5): a present, unexpired {@link SpaceCredential} is indexed
     * under its current lease; anything else (withdrawn, completed, lapsed,
     * undecodable) is dropped from the index. Call under {@link #lock}; a
     * no-op on replicas that do not admit by credential and for other types.
     */
    private void indexCredential(EntryId entryId) {
        if (credentialRule == null) {
            return;
        }
        EntryState state = crdt.state(entryId).orElse(null);
        if (state == null || !credentialSchema.equals(state.record().type())) {
            return;
        }
        long expiresAt = state.lease().value().expiresAtMillis();
        if (state.present() && expiresAt > nowMillis()) {
            Object value = decodeValue(state.record()).orElse(null);
            if (value instanceof SpaceCredential credential) {
                credentialRule.index().put(entryId, credential, expiresAt);
                return;
            }
        }
        credentialRule.index().remove(entryId);
    }

    /**
     * Largest epoch advance a remote claim may assert over the locally observed
     * epoch (ASF-004). Honest epochs grow by 1 per re-take, so any real history
     * stays far inside the bound; the bound's job is to keep a hostile claim from
     * jumping near {@code Long.MAX_VALUE}, where it would win every merge forever
     * and make the +1 re-take epoch unrepresentable. A generous bound (not the
     * strict +1) is deliberate: a replica that joins late may legitimately first
     * observe an entry at epoch N and must still converge.
     */
    static final long MAX_CLAIM_EPOCH_JUMP = 1L << 20;

    /**
     * Longest TAKE hold a remote claim may assert beyond this replica's clock
     * (ASF-004). Bounds the freeze a hostile claim can cause: however large its
     * epoch, the claim lapses and the entry becomes takeable again.
     */
    static final long MAX_CLAIM_HOLD_MILLIS = Duration.ofHours(24).toMillis();

    /**
     * Whether every dot in a received state names the entry's own writer as its
     * replica (ASF-025). Dots are minted only by the entry's writer (one add
     * per write; a cancellation copies adds into removes), so any other replica
     * string is fabricated — spoofed provenance that would be stored and
     * re-sorted forever.
     */
    private static boolean dotsBoundToIssuer(SpaceWire.EntryStateDto dto) {
        String issuerPeer = dto.record().issuer().peer().value();
        for (Dot dot : dto.adds() == null ? List.<Dot>of() : dto.adds()) {
            if (!issuerPeer.equals(dot.replica())) {
                return false;
            }
        }
        for (Dot dot : dto.removes() == null ? List.<Dot>of() : dto.removes()) {
            if (!issuerPeer.equals(dot.replica())) {
                return false;
            }
        }
        return true;
    }

    /**
     * How far before its entry's issue stamp a claim's stamp may lie (spec
     * §7.4/§11). A claim answers a write, so an honest claimant's HLC has
     * already absorbed the record's stamp and its claim sorts after it; the
     * allowance covers a writer whose clock runs ahead by up to the drift
     * ceiling (whose stamp the claimant's clock clamped). Anchoring on the
     * signed issue stamp rather than on wall time makes the bound identical on
     * every replica and lets anti-entropy re-offer a legitimately old standing
     * claim to a late joiner without penalty.
     */
    static final long MAX_CLAIM_PREDATES_ISSUE_MILLIS = HybridLogicalClock.MAX_DRIFT_MILLIS;

    /**
     * How far past the far-future margin, this replica's wall time plus
     * {@link HybridLogicalClock#MAX_DRIFT_MILLIS} plus {@link PeerNode#FAR_FUTURE_SKEW_MARGIN_MILLIS},
     * a stamp may lie and still be deferred (HLC round four, 2026-11-08,
     * decision D4). An honest replica whose clock a hostile stamp dragged to its ceiling writes
     * ahead of a receiver whose wall runs behind by the receiver's skew, which an hour covers
     * many times over. A stamp further ahead is hostile.
     */
    public static final long FAR_FUTURE_HOSTILE_MILLIS = Duration.ofHours(1).toMillis();

    /**
     * Whether every stamp a verified state carries is acceptable (HLC logical counter,
     * 2026-11-08, Rust team report): the record's issue stamp and the lease stamp, which the
     * state would feed this space's clock, and an agent-signed state's {@code signedAt}, which
     * dates its certificate. HLC canonical stamps, 2026-11-08: the Python and Ruby replicas
     * refused a {@code signedAt} above the limit while this one merged it, so it is judged too.
     */
    private static boolean stampsAcceptable(SpaceWire.EntryStateDto dto) {
        return HybridLogicalClock.isAcceptable(dto.record().issued())
                && HybridLogicalClock.isAcceptable(dto.leaseStamp())
                && (dto.signedAt() == null || HybridLogicalClock.isAcceptable(dto.signedAt()));
    }

    /**
     * Counts a verified state refused for its stamps and strikes its signer, once per
     * distinct state (HLC logical counter, 2026-11-08, Rust team report). The record's
     * issuer signed the issue stamp; the party authorized for the transition signed the
     * lease stamp and {@code signedAt}, the claim holder for a completion and the issuer
     * otherwise. HLC canonical stamps, 2026-11-08: the refusal is acknowledged in the digest,
     * as ASF-047 acknowledges a revoked actor's state, so partners stop re-offering it; only a
     * state whose signatures verified reaches here. HLC round four, 2026-11-08 (decision D5):
     * a distinct state is one distinct signed content, its entry, issue stamp, and signed view.
     *
     * @param issuedAtFault whether the issue stamp, which the record's issuer signed, is refused
     * @param violation     the violation reported against the signer
     * @param counter       the counter a first refusal adds to
     */
    private void refuseState(SpaceWire.EntryStateDto dto, boolean issuedAtFault, String violation,
                             AtomicLong counter) {
        EntryRecord record = dto.record();
        PeerId signer = record.issuer().peer();
        boolean first;
        synchronized (lock) {
            if (!issuedAtFault && dto.completed()) {
                // verifyState found the holder's claim; null only if collected since.
                SpaceWire.SignedClaim claim = heldClaim(record.entryId());
                signer = claim == null ? null : claim.claim().holder().peer();
            }
            first = rejectedOffers.add(stateOfferKey(dto));
            String line = "e:" + record.entryId().value() + "=" + stateHash(dto.toState());
            if (acknowledge(record.entryId(), line)) {
                bump();
            }
            forgetRefusedAfter(line, SpaceStateCrdt.collectionHorizon(dto.toState(), maxLeaseMillis()));
        }
        if (first) {
            counter.incrementAndGet();
            if (signer != null) {
                report(signer, violation + " in a state for entry " + record.entryId());
            }
        }
    }

    /**
     * The key a state offer is judged under, its signed content alone (HLC round four,
     * 2026-11-08, decision D5): the entry, the issue stamp, and the digest of the signed view.
     * The view sorts the dots, so a re-offer with its dots in another order is the same offer,
     * and it holds {@code signedAt}, so two states that differ only there are two.
     */
    private String stateOfferKey(SpaceWire.EntryStateDto dto) {
        return "state:" + dto.record().entryId().value() + "=" + dto.record().issued().encoded() + "/"
                + java.util.HexFormat.of().formatHex(Digests.sha256(stateSignBytes(dto)));
    }

    /**
     * The key a claim offer is judged under, its signed map and its entry (HLC round four,
     * 2026-11-08, decision D5), so two claims that differ only in their holder are two.
     */
    private String claimOfferKey(TakeClaim claim, EntryId entryId) {
        return "claim:" + java.util.HexFormat.of().formatHex(Digests.sha256(codec.toBytes(claim)))
                + "@" + entryId.value();
    }

    /** How a stamp stands against this replica's far-future margin (HLC round four, 2026-11-08, decision D4). */
    private enum Lead {
        /** At or inside the margin: the stamp merges, clamped by the clock. */
        WITHIN,
        /** Past the margin by at most {@link #FAR_FUTURE_HOSTILE_MILLIS}: deferred until the wall catches up. */
        DEFERRED,
        /** Further past it: hostile, refused and struck. */
        HOSTILE
    }

    /**
     * Judges a stamp's physical part against wall time plus {@link HybridLogicalClock#MAX_DRIFT_MILLIS}
     * plus {@link PeerNode#FAR_FUTURE_SKEW_MARGIN_MILLIS}, the margin a frame's stamp is judged
     * by too (HLC round four, 2026-11-08, decision D4).
     */
    private static Lead lead(long physical, long nowMillis) {
        long margin = nowMillis + HybridLogicalClock.MAX_DRIFT_MILLIS + PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS;
        if (physical <= margin) {
            return Lead.WITHIN;
        }
        return physical - margin <= FAR_FUTURE_HOSTILE_MILLIS ? Lead.DEFERRED : Lead.HOSTILE;
    }

    /** The violation every stamp refusal reports (HLC logical counter, 2026-11-08, Rust team report). */
    private static final String LOGICAL_VIOLATION = "logical counter above " + HybridLogicalClock.LOGICAL_LIMIT;

    /** The violation a hostile far-future stamp reports (HLC round four, 2026-11-08, decision D4). */
    private static final String FAR_FUTURE_VIOLATION = "far-future stamp";

    /**
     * Refuses an ordered-log claim whose stamp may not merge, before the lattice (HLC
     * logical counter, 2026-11-08, Rust team report). The space counts it once per
     * distinct claim and strikes its holder when the holder's verified signature attests it.
     *
     * @param holder the holder whose signature verified, or null for a claim this node signs
     * @throws IllegalArgumentException when the stamp's logical counter is above the limit
     */
    private void refuseLogClaimStamp(EntryId entryId, TakeClaim claim, PeerId holder) {
        if (HybridLogicalClock.isAcceptable(claim.stamp())) {
            return;
        }
        boolean first;
        synchronized (lock) {
            first = rejectedOffers.add(claimOfferKey(claim, entryId));
        }
        if (first) {
            stampsLogicalRefused.incrementAndGet();
            if (holder != null) {
                runtime.reportMisbehavior(holder, LOGICAL_VIOLATION + " for entry " + entryId);
            }
        }
        throw new IllegalArgumentException("claim on " + entryId.value() + " carries a "
                + LOGICAL_VIOLATION + ": " + claim.stamp());
    }

    /**
     * Merges one received, verified claim.
     *
     * @return whether the held claim changed: false when the claim was refused, deferred, or already held
     */
    private boolean mergeClaim(EntryId entryId, SpaceWire.SignedClaim claim) {
        if (dropsControlId(entryId)) {
            return false; // annex I.6: an id holding a control character never enters the audited set
        }
        if (runtime.revocationView().refuses(claim.claim().holder(), claimKey(claim), null)) {
            // ASF-047: a revoked peer takes nothing, however its claim arrives.
            // v0.1.13 (review M-1): nor does a revoked agent or agent key, whatever
            // the claim's stamp, since a renewal keeps it; its take lapses.
            synchronized (lock) {
                String line = "c:" + entryId.value() + "=" + claimHash(claim.claim());
                acknowledge(entryId, line);
                // a claim lapses at its expiry and an orphaned one is gone by the lapse grace after it
                forgetRefusedAfter(line, claim.claim().expiresAtMillis()
                        + SpaceStateCrdt.lapsedKeepMillis(maxLeaseMillis()));
                bump();
                refusedClaimEntries.add(entryId);
            }
            return false;
        }
        // Spec §7.5: a claim needs the TAKE scope. Judged before the lock, since
        // an AUTHORIZER rule may consult a cache; under a rule whose evidence
        // can be late (CREDENTIAL, AUTHORIZER) the claim is dropped silently and
        // anti-entropy re-offers it once the evidence is here.
        boolean admitted = admits(claim.claim().holder(), SpaceAdmission.Scope.TAKE, nowMillis());
        if (!admitted && !admission.strikesOnRefusal()) {
            return false;
        }
        String violation = null;
        boolean firstRejection = false;
        boolean firstDeferral = false;
        boolean changed = false;
        EntryRecord takenRecord = null;
        AgentId takenHolder = null;
        synchronized (lock) {
            // ASF-004: bound what a remote claim may assert before it enters the
            // lattice, where higher epochs win by design. Local claims (tryTakeOnce)
            // advance by exactly 1 and are not routed through here.
            TakeClaim offered = claim.claim();
            TakeClaim current = claimOf(entryId);
            long now = nowMillis();
            long observed = current == null ? 0 : current.epoch();
            Lead lead = lead(offered.stamp().physical(), now);
            HlcTimestamp issued = crdt.state(entryId)
                    .map(state -> state.record().issued()).orElse(null);
            if (!admitted) {
                violation = "claim by unadmitted agent"; // spec §7.5, ALLOWLIST strikes
            } else if (!HybridLogicalClock.isAcceptable(offered.stamp())) {
                // HLC logical counter, 2026-11-08 (Rust team report): merged, this
                // stamp froze the space clock below and the claim carried it on to
                // every replica. The holder signed it, so it is reported like the rest.
                violation = LOGICAL_VIOLATION;
            } else if (lead == Lead.HOSTILE) {
                // HLC round four, 2026-11-08 (decision D4): hostile, refused and struck.
                violation = FAR_FUTURE_VIOLATION;
            } else if (offered.epoch() > observed + MAX_CLAIM_EPOCH_JUMP) {
                violation = "claim epoch jump";
            } else if (offered.expiresAtMillis() > now + MAX_CLAIM_HOLD_MILLIS) {
                violation = "claim hold beyond bound";
            } else if (issued != null
                    && offered.stamp().physical() < issued.physical() - MAX_CLAIM_PREDATES_ISSUE_MILLIS) {
                // Spec §7.4/§11: a claim cannot predate the entry it answers.
                violation = "claim stamp predates its entry";
            } else if (current != null && !current.holder().equals(offered.holder())
                    && offered.stamp().physical()
                            < now - HybridLogicalClock.MAX_DRIFT_MILLIS - settleWindow.toMillis()) {
                // Spec §7.4/§11: a claim contesting another holder's claim with a
                // stamp older than the drift ceiling (plus one settle window of
                // race allowance) is a queue-jump: LEASE_RACE picks the lowest
                // stamp, and an honest race is decided within the settle window.
                violation = "far-past claim stamp contesting a held entry";
            }
            // HLC round four, 2026-11-08 (decision D4): a claim past the margin is from an honest
            // holder whose clock a hostile stamp dragged ahead. It waits, unmerged and unstruck,
            // and anti-entropy offers it again once this replica's wall catches up.
            boolean deferred = violation == null && lead == Lead.DEFERRED;
            Long decided = logDecidedEpochs.get(entryId);
            if (!deferred && violation == null && decided != null && offered.epoch() <= decided
                    && !offered.equals(current) && !renews(offered, current)) {
                // QA4 A4-5: the ordered log already decided this epoch. A different
                // gossip claim at the same or a lower epoch is not a contender;
                // only the identical claim (an attestation upgrade) or the decided
                // holder's own renewal of it may merge. Refusing the renewal let a
                // voter's copy of the lease run out while the holder still held the
                // entry, so the log committed a rival's next epoch (the PetClinic
                // booking log, 2026-11-15).
                return false;
            }
            if (deferred) {
                firstDeferral = deferredOffers.add(claimOfferKey(offered, entryId));
            } else if (violation == null) {
                SpaceWire.SignedClaim held = heldClaim(entryId);
                // The merge returns one of its two arguments, so an unchanged claim is the same object.
                changed = mergeHeldClaim(entryId, claim) != held;
                if (changed) {
                    bump(entryId);
                }
                hlc.update(offered.stamp()); // spec §11: bounded-drift regime
                observeLease(offered.expiresAtMillis() - offered.stamp().physical());
                // A watcher's TAKEN (spiral 2 S2-H-01): the claim now in force, once per epoch, so a replica
                // that never takes still hears who took what; a race settles as a later claim at the same
                // epoch, which fires again with the winner as the actor.
                TakeClaim merged = claimOf(entryId);
                EntryState known = crdt.state(entryId).orElse(null);
                // A claim on an entry not held here yet waits for its record: mergeState fires its TAKEN after the
                // WRITTEN, so marking the epoch heard now would lose it (R1 probe P2).
                if (merged != null && !merged.expired(now) && known != null
                        && !Long.valueOf(merged.epoch()).equals(takenEpochs.get(entryId))) {
                    takenEpochs.put(entryId, merged.epoch());
                    takenRecord = known.present() ? known.record() : null;
                    takenHolder = merged.holder();
                }
            } else {
                firstRejection = rejectedOffers.add(claimOfferKey(offered, entryId));
            }
        }
        if (firstDeferral) {
            stampsFarFutureDeferred.incrementAndGet();
        }
        if (takenRecord != null) {
            EntryRecord record = takenRecord;
            AgentId holder = takenHolder;
            later(() -> decodeValue(record).ifPresent(value -> fire(SpaceEvent.Kind.TAKEN, entryId, value,
                    record.issuer(), holder, record)));
        }
        if (violation != null && firstRejection) {
            if (LOGICAL_VIOLATION.equals(violation)) {
                stampsLogicalRefused.incrementAndGet();
            } else if (FAR_FUTURE_VIOLATION.equals(violation)) {
                stampsFarFutureRefused.incrementAndGet();
            }
            // The hostile claim carries its author's own verified signature
            // (verifyClaim ran first), so this is witnessed, attributable
            // misbehavior — report it toward local quarantine (WS5). Once per
            // distinct claim: an honest lagging replica re-offering the same
            // contested claim every round must not quarantine its holder.
            report(claim.claim().holder().peer(), violation + " for entry " + entryId);
        }
        return changed;
    }

    /**
     * Whether {@code offered} renews {@code current}: the same claim in every field (entry, space, epoch, stamp,
     * holder and bid) but a later expiry. {@link TakeClaim#merge} orders such a pair by expiry alone, so a renewal can
     * extend the holder's lease and never displace the holder, which is why the log-decided rule (QA4 A4-5) lets it
     * through. Only the holder can sign one: {@code verifyClaim} binds the signature to the holder's own key before a
     * claim reaches the merge.
     */
    private static boolean renews(TakeClaim offered, TakeClaim current) {
        return current != null
                && offered.expiresAtMillis() > current.expiresAtMillis()
                && offered.epoch() == current.epoch()
                && offered.entryId().equals(current.entryId())
                && offered.spaceId().equals(current.spaceId())
                && offered.stamp().equals(current.stamp())
                && offered.holder().equals(current.holder())
                && Double.compare(offered.bid(), current.bid()) == 0;
    }

    /**
     * ASF-047: whether an authenticated state is refused because the actor of
     * its transition is revoked in this group. The actor is the claim holder
     * for a completion and the record's issuer otherwise, so an honest
     * holder's completion of a revoked peer's entry still lands while the
     * revoked peer can neither write, renew, cancel, nor complete. A refusal
     * is acknowledged in the digest so partners stop re-offering it.
     */
    private boolean refusedForRevocation(SpaceWire.EntryStateDto dto) {
        EntryId entryId = dto.record().entryId();
        boolean refused;
        ai.badmonkey.agentspaces.api.security.RevocationView view = runtime.revocationView();
        synchronized (lock) {
            SpaceWire.SignedClaim claim = heldClaim(entryId);
            if (dto.completed()) {
                refused = claim == null
                        ? refusedClaimEntries.contains(entryId)
                        : view.refuses(claim.claim().holder(), claimKey(claim), stateSigningTime(dto, null));
            } else {
                // v0.1.13 freeze rule: the issuer is judged at the state's own
                // signing time: an agent-signed state's signedAt, else the lease
                // stamp of its last write or renewal (the record's issue stamp for
                // an unrenewed write). A late joiner keeps a retired agent's
                // history, and nothing new from it lands, renewals included.
                AgentCertificate recordCertificate = dto.agentCertificate();
                refused = view.refuses(dto.record().issuer(),
                        recordCertificate == null ? null : recordCertificate.agentPublicKey(),
                        stateSigningTime(dto, Instant.ofEpochMilli(dto.leaseStamp().physical())))
                        || (dto.stateCertificate() != null && dto.signer() != null
                                && view.refuses(dto.record().issuer(),
                                        dto.stateCertificate().agentPublicKey(),
                                        stateSigningTime(dto, null)));
            }
            if (refused) {
                String line = "e:" + entryId.value() + "=" + stateHash(dto.toState());
                acknowledge(entryId, line);
                forgetRefusedAfter(line, SpaceStateCrdt.collectionHorizon(dto.toState(), maxLeaseMillis()));
                bump();
            }
        }
        return refused;
    }

    /** Whether the group has revoked an acting identity (its peer, the agent, or its own key). */
    private boolean actorRevoked(AgentIdentity actor) {
        return runtime.revocationView().revoked(actor.id(), actor.isSubordinate() ? actor.publicKey() : null);
    }

    /** The key a claim was signed with, when its holder signed with an agent key of its own. */
    private static byte[] claimKey(SpaceWire.SignedClaim claim) {
        return claim.holderCertificate() == null ? null : claim.holderCertificate().agentPublicKey();
    }

    /** An agent-signed state's signing time, else the fallback. */
    private static Instant stateSigningTime(SpaceWire.EntryStateDto dto, Instant fallback) {
        return dto.signedAt() == null ? fallback : Instant.ofEpochMilli(dto.signedAt().physical());
    }

    private TakeClaim claimOf(EntryId entryId) {
        SpaceWire.SignedClaim signed = heldClaim(entryId);
        return signed == null ? null : signed.claim();
    }

    // ---- the slots of the map (RE-M-04, RE-M-07): every change to them goes through these, under the lock

    /** The signed claim held for an entry, from its slot, or null. Callers hold {@link #lock}. */
    private SpaceWire.SignedClaim heldClaim(EntryId entryId) {
        return (SpaceWire.SignedClaim) crdt.claim(entryId);
    }

    /**
     * Merges a claim into the one held for its entry by the claim lattice, as the claim map's {@code merge} did, and
     * returns the claim now held: the held one, unchanged, when it wins. Callers hold {@link #lock}.
     */
    private SpaceWire.SignedClaim mergeHeldClaim(EntryId entryId, SpaceWire.SignedClaim claim) {
        SpaceWire.SignedClaim held = heldClaim(entryId);
        SpaceWire.SignedClaim merged = held == null ? claim : SpaceWire.SignedClaim.merge(held, claim);
        if (merged != held) {
            crdt = crdt.withClaim(entryId, merged, claimHash(merged.claim()));
        }
        if (held != null) {
            noteDisplaced(entryId, merged, merged == held ? claim : held);
        }
        return merged;
    }

    /**
     * Keeps what rule two of the settle guarantee (SPEC §7.4) needs once the lattice has kept one claim of two: the
     * displaced claim, when one of this peer's agents holds the kept one ({@link #displacedClaims}), and this peer's
     * own completion, when the displaced claim is the one an agent here completed under ({@link #displacedOwn}).
     * Every claim reaching here was verified or signed here. Callers hold {@link #lock}.
     */
    private void noteDisplaced(EntryId entryId, SpaceWire.SignedClaim kept, SpaceWire.SignedClaim lost) {
        if (kept == lost || kept.claim().holder().equals(lost.claim().holder())) {
            return; // the same holder's renewal or attestation upgrade displaces nobody
        }
        PeerId self = identity.peerId();
        if (lost.claim().holder().peer().equals(self) && !kept.claim().holder().peer().equals(self)) {
            displacedHere.putIfAbsent(entryId, lost); // the claim this peer first lost on the entry
        }
        if (kept.claim().holder().peer().equals(self)) {
            List<SpaceWire.SignedClaim> lines = displacedClaims.computeIfAbsent(entryId, k -> new ArrayList<>(2));
            if (lines.stream().noneMatch(c -> c.claim().equals(lost.claim()))) {
                if (lines.size() >= DISPLACED_PER_ENTRY) {
                    lines.remove(0);
                }
                lines.add(lost);
            }
            PeerId displacedPeer = lost.claim().holder().peer();
            if (!displacedPeer.equals(self)) {
                long now = localMillis();
                DisplacedPull pull = displacedPulls.computeIfAbsent(entryId, k -> new DisplacedPull(now));
                if (pull.holders.add(displacedPeer) && pull.rounds > 0) {
                    pull.nextMillis = Math.min(pull.nextMillis, now); // a newly displaced holder is asked at once
                }
                lowerDue(pullDue, pull.nextMillis);
            }
        } else if (lost.claim().holder().peer().equals(self)
                && Long.valueOf(lost.claim().epoch()).equals(completedHere.get(entryId))
                && !displacedOwn.containsKey(entryId)) {
            displacedOwn.put(entryId, new Reoffer(lost, localMillis()));
            anyDisplacedOwn = true;
            reofferDue = true;
        }
    }

    /**
     * Acknowledges a refused line in its entry's slot, so partners stop re-offering it (ASF-047), and forgets the
     * least recently refused line past {@link #REFUSED_ACK_CAP}. Refusing a line again makes it the most recent.
     * Callers hold {@link #lock}.
     *
     * @return whether the line is new here
     */
    private boolean acknowledge(EntryId entryId, String line) {
        if (refusedAckOrder.put(line, entryId) != null) {
            return false;
        }
        crdt = crdt.withAck(entryId, line);
        if (refusedAckOrder.size() > REFUSED_ACK_CAP) {
            Iterator<Map.Entry<String, EntryId>> eldest = refusedAckOrder.entrySet().iterator();
            Map.Entry<String, EntryId> gone = eldest.next();
            eldest.remove();
            crdt = crdt.withoutAck(gone.getValue(), gone.getKey());
            collectedAcks.remove(gone.getKey());
            unscheduleRefused(gone.getKey());
        }
        return true;
    }

    /**
     * Acknowledges a re-offered state the door refused as a collectable tombstone (RE-H-03, SPEC §7.3), when this
     * replica holds no state for its entry: it collected the entry already, or never held it. Without the line a member
     * that keeps tombstones longer (one that saw a longer lease, or an older implementation) offers the same state on
     * every round until it collects it too. The line goes under {@link #REFUSED_ACK_CAP} with the other refusals, and
     * is forgotten {@link #COLLECTED_ACK_HOLD_MILLIS} after the last refusal, so a lingering line cannot keep this
     * replica's digest apart from its partners' once the offerer has collected too; a member still holding the state
     * then offers it once more and earns the line again. Only an authentic state reaches here.
     */
    private void acknowledgeCollected(EntryId entryId, SpaceWire.EntryStateDto dto) {
        String line = "e:" + entryId.value() + "=" + stateHash(dto.toState());
        synchronized (lock) {
            if (crdt.state(entryId).isPresent()) {
                return; // still held here: its own sweep collects it, and the merge would change nothing
            }
            boolean added = acknowledge(entryId, line);
            collectedAcks.remove(line);
            collectedAcks.put(line, nowMillis() + COLLECTED_ACK_HOLD_MILLIS);
            lowerDue(sweepDue, collectedAcks.values().iterator().next());
            if (added) {
                collectedReoffersAcknowledged.incrementAndGet();
                bump();
            }
        }
    }

    /**
     * Schedules a door acknowledgement to be forgotten once what it refused is past its collection horizon, and never
     * sooner than {@link #COLLECTED_ACK_HOLD_MILLIS} from now: a member that keeps the state longer (it saw a longer
     * lease) then offers it again at most once per hold and earns the line again, as a collected tombstone's re-offer
     * does (RE-H-03). Refusing the line again moves its time. Callers hold {@link #lock}.
     */
    private void forgetRefusedAfter(String line, long horizon) {
        if (!refusedAckOrder.containsKey(line)) {
            return; // the cap already let it go
        }
        long at = Math.max(horizon, nowMillis() + COLLECTED_ACK_HOLD_MILLIS);
        unscheduleRefused(line);
        refusedForgetAt.put(line, at);
        refusedForgetBy.computeIfAbsent(at, k -> new java.util.LinkedHashSet<>()).add(line);
        lowerDue(sweepDue, at);
    }

    /** Drops a door acknowledgement's schedule. Callers hold {@link #lock}. */
    private void unscheduleRefused(String line) {
        Long at = refusedForgetAt.remove(line);
        if (at != null) {
            Set<String> lines = refusedForgetBy.get(at);
            if (lines != null) {
                lines.remove(line);
                if (lines.isEmpty()) {
                    refusedForgetBy.remove(at);
                }
            }
        }
    }

    /** Forgets the door acknowledgements whose time has come (P6). Callers hold {@link #lock}. */
    private void forgetRefusedAcks(long now) {
        boolean dropped = false;
        while (!refusedForgetBy.isEmpty() && refusedForgetBy.firstKey() <= now) {
            for (String line : refusedForgetBy.pollFirstEntry().getValue()) {
                refusedForgetAt.remove(line);
                EntryId entryId = refusedAckOrder.remove(line);
                if (entryId != null) {
                    crdt = crdt.withoutAck(entryId, line);
                    collectedAcks.remove(line);
                    dropped = true;
                }
            }
        }
        if (dropped) {
            generation++;
        }
    }

    /**
     * Forgets the acknowledgements of collected tombstones whose hold has passed (RE-H-03). They are held in refusal
     * order with one hold each, so the oldest come first. Callers hold {@link #lock}.
     */
    private void forgetCollectedAcks(long now) {
        boolean dropped = false;
        Iterator<Map.Entry<String, Long>> held = collectedAcks.entrySet().iterator();
        while (held.hasNext()) {
            Map.Entry<String, Long> next = held.next();
            if (next.getValue() > now) {
                break;
            }
            held.remove();
            EntryId entryId = refusedAckOrder.remove(next.getKey());
            if (entryId != null) {
                crdt = crdt.withoutAck(entryId, next.getKey());
                dropped = true;
            }
        }
        if (dropped) {
            generation++;
        }
    }

    /**
     * Remembers the state line of an out-of-shard entry in its slot, so partners stop offering it, and forgets the
     * oldest past {@link #SHARD_DROPPED_CAP}. Callers hold {@link #lock}.
     */
    private void dropShard(EntryId entryId, String hash) {
        shardDroppedOrder.put(entryId, Boolean.TRUE);
        crdt = crdt.withDropped(entryId, hash);
        if (shardDroppedOrder.size() > SHARD_DROPPED_CAP) {
            Iterator<EntryId> eldest = shardDroppedOrder.keySet().iterator();
            EntryId gone = eldest.next();
            eldest.remove();
            crdt = crdt.withDropped(gone, null);
        }
    }

    /** Forgets an entry's shard-dropped line. Callers hold {@link #lock}. */
    private void forgetShardDropped(EntryId entryId) {
        if (shardDroppedOrder.remove(entryId) != null) {
            crdt = crdt.withDropped(entryId, null);
        }
    }

    /** Sets the map after a change to one entry's state, and keeps the live index in step. Callers hold {@link #lock}. */
    private void setCrdt(SpaceStateCrdt next, EntryId changed) {
        crdt = next;
        refreshLive(changed);
    }

    /** Files an entry in the live index while it is present, and takes it out otherwise. Callers hold {@link #lock}. */
    private void refreshLive(EntryId entryId) {
        EntryState state = crdt.state(entryId).orElse(null);
        if (state != null && state.present()) {
            liveIndex.put(SpaceStateCrdt.liveRecord(state));
        } else {
            liveIndex.remove(entryId);
        }
    }

    /** Whether the live index holds exactly the map's present entries; for the tests of RE-M-04. */
    boolean liveIndexMatchesForTest() {
        synchronized (lock) {
            Map<EntryId, EntryRecord> expected = new HashMap<>();
            for (EntryRecord record : crdt.liveRecords()) {
                expected.put(record.entryId(), record);
            }
            Map<EntryId, EntryRecord> held = new HashMap<>();
            liveIndex.forEach(record -> held.put(record.entryId(), record));
            return held.size() == liveIndex.size() && expected.equals(held);
        }
    }

    /**
     * Signs a claim as the acting identity (QA4 A4-7 phase 3). The peer key
     * always rides as {@code holderKey}, so {@code holderAttested()} and the
     * completion branch of {@code verifyState} are unchanged; a subordinate
     * actor signs with its own key and its certificate rides beside the proof.
     */
    private SpaceWire.SignedClaim sign(TakeClaim claim, AgentIdentity actor) {
        byte[] bytes = codec.toBytes(claim);
        return new SpaceWire.SignedClaim(claim, identity.rawPublicKey(), actor.sign(bytes),
                coveringCertificate(actor, claim.stamp()));
    }

    private boolean verifyClaim(EntryId expectedEntry, SpaceWire.SignedClaim signed) {
        if (signed.claim() == null || signed.holderKey() == null || signed.signature() == null
                || signed.holderKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(signed.holderKey())
                        .equals(signed.claim().holder().peer())) {
            return false;
        }
        // ASF-002: the claim is bound to its entry and space by its signed bytes,
        // so a signed claim cannot be transplanted onto another entry via the
        // unsigned map key it travels under.
        if (!signed.claim().entryId().equals(expectedEntry)
                || !signed.claim().spaceId().equals(id)) {
            return false;
        }
        byte[] bytes = codec.toBytes(signed.claim());
        if (signed.holderCertificate() == null) {
            return signatures.verify(signed.holderKey(), bytes, signed.signature());
        }
        // The two-key rule of TECH-SPEC §7.2, applied to claims (QA4 A4-7 phase 3):
        // the peer key verifies the certificate for exactly this holder, and the
        // certificate's agent key verifies the claim. A claim carrying a
        // certificate but a peer-key signature is refused, as a record would be.
        // SPEC §4.2 v0.1.13: judged at the claim's own stamp, which a renewal keeps. HLC round
        // five, 2026-11-08 (decision E4): by the window alone; mergeClaim judges how far the stamp
        // leads this replica's wall, as it does for a peer-signed claim.
        return certificates.verifyCovering(signed.holderCertificate(), signed.holderKey(),
                signed.claim().holder(), Instant.ofEpochMilli(signed.claim().stamp().physical()))
                && signatures.verify(signed.holderCertificate().agentPublicKey(), bytes, signed.signature());
    }

    /**
     * The stored proof of the current claim on an entry, when one is held: the
     * claim, the holder's peer key, its signature, and — when the holder took
     * through an agent key of its own — the peer's certificate for that key.
     *
     * @param entryId the entry
     * @return the signed claim, or empty
     */
    public Optional<SpaceWire.SignedClaim> claimProof(EntryId entryId) {
        Objects.requireNonNull(entryId, "entryId");
        synchronized (lock) {
            return Optional.ofNullable(heldClaim(entryId));
        }
    }

    /**
     * The most advanced actor-signed state this replica holds for an entry, as
     * anti-entropy would forward it: the record, the issuer's peer key (and
     * certificate, under a subordinate identity), the CRDT fields, and the
     * state signature. Empty for an entry this replica does not know.
     *
     * @param entryId the entry
     * @return the signed state
     */
    public Optional<SpaceWire.EntryStateDto> signedState(EntryId entryId) {
        Objects.requireNonNull(entryId, "entryId");
        synchronized (lock) {
            return Optional.ofNullable(signedStates.get(entryId));
        }
    }

    // ------------------------------------------------------------- anti-entropy

    byte[] digest() { // package-private for tests
        return digest(true);
    }

    /**
     * The digest, counting the ask in the full-digest pair only when {@code fullAsk}: {@link #digestHash()} passes
     * false, so a hash ask is counted once, in its own pair, and a reader that adds the two pairs does not count one
     * rebuild twice. {@link #digestComputations} counts the rebuilds of both.
     */
    private byte[] digest(boolean fullAsk) {
        sweepIfDue(); // spec §7.3: collect tombstones before advertising what we hold, when any are due
        long at;
        synchronized (lock) {
            if (digestCache != null && digestGeneration == generation) {
                if (fullAsk) {
                    digestCacheHits++;
                }
                return digestCache; // nothing changed since the last round (PERF2 §3.2)
            }
            if (fullAsk) {
                digestCacheMisses++;
            }
            at = generation;
        }
        byte[] computed = computeDigest();
        synchronized (lock) {
            if (generation == at) {
                digestCache = computed;
                digestGeneration = at;
                digestHashCache = null; // taken over the digest just replaced
                digestHashOf = null;
            }
            digestComputations++;
        }
        return computed;
    }

    /**
     * The SHA-256 of {@link #digest()}, what a probe and the opening of a hashed anti-entropy round carry per stream
     * (SPEC §5.2, §5.3). Cached beside the digest under the same generation, so an unchanged space answers with the
     * hash it already took (IDLE-WORK item 3). Callers must not modify the returned array.
     */
    byte[] digestHash() { // package-private for tests
        byte[] digest = digest(false);
        synchronized (lock) {
            if (digestHashOf == digest && digestHashCache != null) {
                digestHashHits++;
                return digestHashCache;
            }
            digestHashMisses++;
        }
        byte[] hash = Digests.sha256(digest);
        synchronized (lock) {
            if (digestCache == digest) {
                digestHashCache = hash;
                digestHashOf = digest;
            }
        }
        return hash;
    }

    /**
     * Merges a prepared state in one step, for tests only: a benchmark seeds a hundred thousand entries without
     * paying one copy of the whole map per entry, which is what seeding them through writes or deltas costs.
     */
    void mergeForTest(SpaceStateCrdt state) {
        mergeForTest(state, Map.of(), Set.of());
    }

    /**
     * {@link #mergeForTest(SpaceStateCrdt)} with the side tables a merged state brings, for the test that pins the
     * digest's bytes: an issuer key for every entry, as a merge or a write files one (the digest lists a tombstone
     * only through it), the given take claims, and the given refused-and-acknowledged lines (ASF-047), so the pinned
     * digest carries every kind of line it can carry.
     */
    void mergeForTest(SpaceStateCrdt state, Map<EntryId, SpaceWire.SignedClaim> seededClaims,
                      Set<String> refusedLines) {
        synchronized (lock) {
            crdt = crdt.merge(state);
            for (EntryId entryId : state.entryIds()) {
                issuerKeys.putIfAbsent(entryId, identity.rawPublicKey());
                refreshLive(entryId);
            }
            for (Map.Entry<EntryId, SpaceWire.SignedClaim> e : seededClaims.entrySet()) {
                crdt = crdt.withClaim(e.getKey(), e.getValue(), claimHash(e.getValue().claim()));
            }
            for (String line : refusedLines) {
                // The id runs from after the kind to the last '=': a state hash is hex and a claim hash holds none.
                acknowledge(EntryId.of(line.substring(2, line.lastIndexOf('='))), line);
            }
            long now = nowMillis();
            for (EntryId entryId : state.entryIds()) {
                reindex(entryId, now);
            }
            for (EntryId entryId : seededClaims.keySet()) {
                reindex(entryId, now);
            }
            bump();
        }
    }

    /**
     * {@link #mergeForTest(SpaceStateCrdt)} with the signed state each entry would carry, for the benchmark of
     * {@link #deltaFor}: that method forwards only states held signed, so a seeded space without them offers nothing.
     * The states are stored as given and not verified.
     */
    void mergeForTest(SpaceStateCrdt state, java.util.Collection<SpaceWire.EntryStateDto> signed) {
        mergeForTest(state);
        synchronized (lock) {
            for (SpaceWire.EntryStateDto dto : signed) {
                EntryId entryId = dto.record().entryId();
                signedStates.put(entryId, dto);
                issuerKeys.put(entryId, dto.issuerPublicKey());
            }
            bump();
        }
    }

    /** Marks the digest stale without changing anything, so a benchmark can time a rebuild of an unchanged space. */
    void invalidateDigestForTest() {
        synchronized (lock) {
            bump();
        }
    }

    /** How many times a blocking read or take has tried; for tests. */
    long waitingPolls() {
        return waitingPolls.get();
    }

    /** How many entry ids the sweep has evaluated, for the test that an unchanged space visits none. */
    long sweepVisits() {
        synchronized (lock) {
            return sweepVisits;
        }
    }

    /** How many times the digest was rebuilt (for tests of the cache). */
    long digestComputations() {
        synchronized (lock) {
            return digestComputations;
        }
    }

    /**
     * The text digest, collected from the slots of the map and sorted (RE-M-06): the same lines in the same order as
     * the summary this method used to build from the map, the claims, the shard-dropped lines and the acknowledged
     * lines one table at a time, since each of those now lives in the slot of its entry. The map is immutable, so it is
     * read under the lock and the digest is built outside it.
     */
    private byte[] computeDigest() {
        SpaceStateCrdt snapshot;
        synchronized (lock) {
            snapshot = crdt;
        }
        return snapshot.legacyDigest();
    }

    /**
     * The stream hash of SPEC annex I.6 over this space's audited set: what a probe and the opening of a round carry
     * for this space toward a member on {@code wire.ae=2}. The sweep runs first when anything is due, so an entry
     * whose lease lapsed by this replica's clock is out of the set (RE-T-06) and a collectable tombstone is gone, as
     * the text digest has them. The trie memoizes the hash, so an unchanged space answers with no hashing.
     */
    byte[] streamHash() { // package-private for tests
        sweepIfDue();
        SpaceStateCrdt snapshot;
        synchronized (lock) {
            snapshot = crdt;
        }
        return snapshot.streamHash();
    }

    /** The audited node of annex I.6 at a path, for the audit's {@code tree} answers. */
    ai.badmonkey.agentspaces.space.crdt.AuditNode auditNode(byte[] path) { // package-private for tests
        sweepIfDue();
        SpaceStateCrdt snapshot;
        synchronized (lock) {
            snapshot = crdt;
        }
        return snapshot.node(path);
    }

    /** Every audited line under a path, joined as a leaf's preimage (annex I.6), whatever the count there. */
    byte[] auditLines(byte[] path) { // package-private for tests
        sweepIfDue();
        SpaceStateCrdt snapshot;
        synchronized (lock) {
            snapshot = crdt;
        }
        return snapshot.auditLines(path);
    }

    /** {@link #auditLines(byte[])} when the lines hold at most {@code maxBytes} bytes, else null, gathered no further. */
    byte[] auditLines(byte[] path, long maxBytes) { // package-private for tests
        sweepIfDue();
        SpaceStateCrdt snapshot;
        synchronized (lock) {
            snapshot = crdt;
        }
        return snapshot.auditLines(path, maxBytes);
    }

    /** Which of a partner's lines under a path this replica holds, each read in the one slot it names. */
    java.util.Set<String> auditHeld(byte[] path, java.util.Collection<String> lines) { // package-private for tests
        sweepIfDue();
        SpaceStateCrdt snapshot;
        synchronized (lock) {
            snapshot = crdt;
        }
        return snapshot.auditHeld(path, lines);
    }

    /**
     * The answer to a fetch by id (annex I.6, <em>The fetch</em>): the signed state of each named entry this replica
     * holds and its claim when it holds one, in the delta format of {@link #deltaFor}, present entries before
     * tombstones. An id not held is left out. This is the whole answer, for an asker that sent no budget; one that did
     * is answered by {@link #statesFor(java.util.Collection, long, boolean)}.
     */
    byte[] statesFor(java.util.Collection<String> ids) { // package-private for tests
        sweepIfDue(); // spec §7.3: never offer a tombstone this replica would collect itself
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        List<SpaceWire.EntryStateDto> tombstones = new ArrayList<>();
        Map<String, SpaceWire.SignedClaim> claims = new HashMap<>();
        synchronized (lock) {
            for (String id : ids) {
                EntryId entryId = EntryId.of(id);
                EntryState state = crdt.state(entryId).orElse(null);
                if (state != null) {
                    SpaceWire.EntryStateDto signed = signedStates.get(entryId);
                    if (signed != null) {
                        (state.present() ? states : tombstones).add(signed);
                    }
                }
                SpaceWire.SignedClaim claim = heldClaim(entryId);
                if (claim != null) {
                    claims.put(id, claim);
                }
            }
        }
        states.addAll(tombstones);
        if (states.isEmpty() && claims.isEmpty()) {
            return new byte[0];
        }
        syncStatesSent.addAndGet(states.size());
        syncClaimsSent.addAndGet(claims.size());
        return codec.toBytes(new SpaceWire.SyncDelta(states, claims));
    }

    /**
     * The answer to a fetch by id cut to a budget (annex I.6, <em>The fetch</em> and <em>The budget</em>; RE-I-01,
     * RE-I-06): the signed state of each named entry this replica holds with its claim, in {@link #OFFER_ORDER}, up to
     * {@code budget} bytes of delta, the first item always when {@code first}. An id not held is left out unmarked.
     *
     * @return the delta and the count of held items left out for the budget
     */
    ReconcilableState.Bounded statesFor(java.util.Collection<String> ids, long budget, boolean first) {
        sweepIfDue(); // spec §7.3: never offer a tombstone this replica would collect itself
        List<Offer> offers = new ArrayList<>();
        synchronized (lock) {
            for (String id : ids) {
                EntryId entryId;
                try {
                    entryId = EntryId.of(id);
                } catch (IllegalArgumentException e) {
                    continue; // no such id is held
                }
                EntryState state = crdt.state(entryId).orElse(null);
                SpaceWire.EntryStateDto signed = state == null ? null : signedStates.get(entryId);
                SpaceWire.SignedClaim claim = heldClaim(entryId);
                if (signed != null || claim != null) {
                    offers.add(new Offer(entryId, signed, state != null && state.present(), claim));
                }
            }
        }
        return bounded(offers, budget, first);
    }

    /**
     * Whether a displaced claim made here is still worth sending (SPEC v0.1.37 §9.4): the entry is known, the claim is
     * live, and well inside the drift ceiling, as a re-offer judges. Lines past that are forgotten. Callers hold
     * {@link #lock}.
     */
    private boolean pruneDisplacedHere(long now) {
        displacedHere.entrySet().removeIf(line -> {
            TakeClaim claim = line.getValue().claim();
            return claim.expired(now) || now - claim.stamp().physical() > HybridLogicalClock.MAX_DRIFT_MILLIS / 2
                    || crdt.state(line.getKey()).isEmpty();
        });
        return !displacedHere.isEmpty() || !displacedOwn.isEmpty();
    }

    /** Whether a dial-only member holds anything to answer a member other than its seed with (§9.4). */
    boolean holdsDisplaced() {
        synchronized (lock) {
            return pruneDisplacedHere(nowMillis());
        }
    }

    /**
     * A dial-only member's answer to a relayed or another member's {@code DIGEST} (SPEC v0.1.37 §9.4): the entries
     * whose claim made here a better claim displaced, each with that claim (the one the winner pairs with the
     * completion) and, when the asker's digest lacks it, the entry's state, which carries this peer's completion.
     * Nothing else this replica holds goes, so no member draws its whole state through the seed.
     *
     * @return the delta and the count of items left out for the budget
     */
    ReconcilableState.Bounded displacedFor(byte[] remoteDigest, long budget, boolean first) {
        Set<String> remote = Set.of(new String(remoteDigest, StandardCharsets.UTF_8).split("\n"));
        List<Offer> offers = new ArrayList<>();
        synchronized (lock) {
            pruneDisplacedHere(nowMillis());
            Map<EntryId, SpaceWire.SignedClaim> lines = new java.util.LinkedHashMap<>(displacedHere);
            displacedOwn.forEach((entryId, reoffer) -> lines.putIfAbsent(entryId, reoffer.claim));
            lines.forEach((entryId, claim) -> {
                SpaceWire.EntryStateDto signed = null;
                var state = crdt.state(entryId);
                if (state.isPresent()) {
                    String key = "e:" + entryId.value() + "=" + SpaceStateCrdt.stateHash(state.get());
                    if (!remote.contains(key) && !remote.contains("a:" + key)) {
                        signed = signedStates.get(entryId);
                    }
                }
                offers.add(new Offer(entryId, signed, state.map(st -> st.present()).orElse(false), claim));
            });
        }
        if (offers.isEmpty()) {
            return new ReconcilableState.Bounded(new byte[0], 0);
        }
        displacedAnswers.incrementAndGet();
        return bounded(offers, budget, first);
    }

    private final java.util.concurrent.atomic.AtomicLong displacedAnswers = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Answers this replica gave a relayed or another member's {@code DIGEST} as a dial-only member, each carrying only
     * its displaced claims (SPEC v0.1.37 §9.4).
     *
     * @return the count
     */
    public long displacedAnswers() {
        return displacedAnswers.get();
    }

    /**
     * The answer to a text digest cut to a budget, as {@link #statesFor(java.util.Collection, long, boolean)} cuts a
     * fetch: a budgeted get-space draws the newest present states first (annex I.6, <em>The budget</em>; RE-I-04). The
     * partner's lines are evidence as in {@link #deltaFor(byte[])}.
     *
     * @return the delta and the count of held items left out for the budget
     */
    ReconcilableState.Bounded deltaFor(byte[] remoteDigest, long budget, boolean first) {
        sweepIfDue(); // spec §7.3: never offer a tombstone we would collect ourselves
        Set<String> remote = remoteLines(remoteDigest);
        List<Offer> offers = new ArrayList<>();
        synchronized (lock) {
            crdt.forEachHeld((entryId, state, stateHash, claim, claimHash) -> {
                SpaceWire.EntryStateDto signed = null;
                if (state != null) {
                    String key = "e:" + entryId.value() + "=" + stateHash;
                    if (!remote.contains(key) && !remote.contains("a:" + key)) {
                        signed = signedStates.get(entryId); // a relay forwards only what it holds signed
                    }
                }
                SpaceWire.SignedClaim missing = null;
                if (claim != null) {
                    String key = "c:" + entryId.value() + "=" + claimHash;
                    if (!remote.contains(key) && !remote.contains("a:" + key)) {
                        missing = (SpaceWire.SignedClaim) claim;
                    }
                }
                if (signed != null || missing != null) {
                    offers.add(new Offer(entryId, signed, state != null && state.present(), missing));
                }
            });
        }
        return bounded(offers, budget, first);
    }

    /**
     * One item of a budgeted answer: an entry's signed state with the claim that travels with it (RE-I-06), or a claim
     * alone when the asker holds the state already.
     */
    private record Offer(EntryId entryId, SpaceWire.EntryStateDto state, boolean present,
                         SpaceWire.SignedClaim claim) {

        /** Present states first, then tombstones, then claims alone. */
        int rank() {
            return state == null ? 2 : present ? 0 : 1;
        }

        HlcTimestamp stamp() {
            return state != null ? state.leaseStamp() : claim != null ? claim.claim().stamp() : null;
        }
    }

    /**
     * The order of a budgeted answer (annex I.6; RE-I-06): a present state before a tombstone, so that when a budget
     * splits a replacement (a new revision and the old one's cancel) the new revision goes in the earlier answer and a
     * reader's view is never empty; then newest first by the lease stamp in the stamp order of §7.3; equal stamps in
     * ascending order of their ids' UTF-8 bytes.
     */
    private static final java.util.Comparator<Offer> OFFER_ORDER = (a, b) -> {
        int rank = Integer.compare(a.rank(), b.rank());
        if (rank != 0) {
            return rank;
        }
        HlcTimestamp sa = a.stamp();
        HlcTimestamp sb = b.stamp();
        if (sa != null && sb != null) {
            int newer = sb.compareTo(sa);
            if (newer != 0) {
                return newer;
            }
        } else if (sa != null || sb != null) {
            return sa != null ? -1 : 1;
        }
        return compareUtf8(a.entryId().value(), b.entryId().value());
    };

    /** Compares two texts by their UTF-8 bytes, which is the order of their code points. */
    private static int compareUtf8(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    /** Bytes of a delta beside its items: the record's two field names and the heads of its list and map. */
    private static final long DELTA_BASE = 32;

    /**
     * Fills a delta in {@link #OFFER_ORDER} up to the budget: it stops before the item that would take the delta past
     * it, except that with {@code first} the first item goes in whatever its size (annex I.6, <em>The budget</em>), so
     * a state larger than the budget still travels alone and nobody re-asks for it for ever. Each item's size is its
     * own encoding, which a delta's list and map carry unchanged.
     */
    private ReconcilableState.Bounded bounded(List<Offer> offers, long budget, boolean first) {
        offers.sort(OFFER_ORDER);
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        Map<String, SpaceWire.SignedClaim> claims = new HashMap<>();
        long used = DELTA_BASE;
        int taken = 0;
        for (Offer offer : offers) {
            long size = 0;
            if (offer.state() != null) {
                size += codec.toBytes(offer.state()).length;
            }
            if (offer.claim() != null) {
                // the claim, and its key in the map: the id as CBOR text
                size += codec.toBytes(offer.claim()).length
                        + offer.entryId().value().getBytes(StandardCharsets.UTF_8).length + 9;
            }
            if (used + size > budget && !(first && taken == 0)) {
                break;
            }
            used += size;
            taken++;
            if (offer.state() != null) {
                states.add(offer.state());
            }
            if (offer.claim() != null) {
                claims.put(offer.entryId().value(), offer.claim());
            }
        }
        long left = offers.size() - taken;
        if (taken == 0) {
            return new ReconcilableState.Bounded(new byte[0], left);
        }
        syncStatesSent.addAndGet(states.size());
        syncClaimsSent.addAndGet(claims.size());
        return new ReconcilableState.Bounded(codec.toBytes(new SpaceWire.SyncDelta(states, claims)), left);
    }

    /**
     * A partner's audit answered these lines (PD-L-01): a state line naming an entry this replica holds is evidence the
     * partner holds its record, as a line of its text digest was.
     */
    private void linesSeen(String lines) {
        synchronized (lock) {
            for (String line : lines.split("\n")) {
                if (!line.startsWith("e:")) {
                    continue;
                }
                int eq = line.lastIndexOf('=');
                if (eq <= 2) {
                    continue;
                }
                EntryId listed = EntryId.of(line.substring(2, eq));
                if (crdt.state(listed).isPresent()) {
                    replicated.add(listed);
                    ownWrites.remove(listed);
                }
            }
        }
    }

    /**
     * A partner's audit showed its node at a path equal to this replica's (PD-L-01): it holds every line this replica
     * holds under the path, so each own write whose key lies there has replicated, as {@link #digestMatched} judges a
     * whole stream that matched.
     */
    private void matchedUnder(byte[] path) {
        synchronized (lock) {
            if (ownWrites.isEmpty()) {
                return;
            }
            for (EntryId entryId : new ArrayList<>(ownWrites.keySet())) {
                if (keyStartsWith(entryId, path) && crdt.state(entryId).isPresent()) {
                    replicated.add(entryId);
                    ownWrites.remove(entryId);
                }
            }
        }
    }

    /** Whether an entry's key in the trie, SHA-256 of its id read as nibbles, begins with the path (annex I.6). */
    private static boolean keyStartsWith(EntryId entryId, byte[] path) {
        byte[] key = Digests.sha256(entryId.value().getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < path.length; i++) {
            int nibble = (i & 1) == 0 ? (key[i >> 1] >> 4) & 0xF : key[i >> 1] & 0xF;
            if (nibble != path[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether an entry id holds a control character (U+0000 to U+001F, U+007F), which annex I.6 keeps out of the
     * audited set: under {@code wire.ae=2} such a state or claim is dropped uncounted and unacknowledged (RE-T-06),
     * since an id holding LF would let two different sets join to the same bytes.
     */
    private boolean dropsControlId(EntryId entryId) {
        if (entryId == null || !runtime.gossip().auditing()) {
            return false;
        }
        String value = entryId.value();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch < 0x20 || ch == 0x7F) {
                return true;
            }
        }
        return false;
    }

    /**
     * The lines of a partner's text digest, each a state or claim line or an acknowledgement, after taking the state
     * lines as evidence (PD-L-01): a remote line naming an entry this replica holds shows the remote holds its record,
     * whatever state hash it names.
     */
    private Set<String> remoteLines(byte[] remoteDigest) {
        Set<String> remote = new HashSet<>(List.of(
                new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
        synchronized (lock) {
            for (String line : remote) {
                if (line.startsWith("e:")) {
                    int eq = line.indexOf('=');
                    if (eq > 2) {
                        EntryId listed = EntryId.of(line.substring(2, eq));
                        if (crdt.state(listed).isPresent()) {
                            replicated.add(listed);
                            ownWrites.remove(listed);
                        }
                    }
                }
            }
        }
        return remote;
    }

    byte[] deltaFor(byte[] remoteDigest) { // package-private for tests
        sweepIfDue(); // spec §7.3: never offer a tombstone we would collect ourselves
        Set<String> remote = remoteLines(remoteDigest);
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        Map<String, SpaceWire.SignedClaim> missingClaims = new HashMap<>();
        synchronized (lock) {
            // One walk over the slots: each state and each claim the partner's digest does not list. Present entries
            // go first and tombstones after, the order the walk over the live records and then the known ids gave.
            List<SpaceWire.EntryStateDto> tombstones = new ArrayList<>();
            crdt.forEachHeld((entryId, state, stateHash, claim, claimHash) -> {
                if (state != null) {
                    String key = "e:" + entryId.value() + "=" + stateHash;
                    if (!remote.contains(key) && !remote.contains("a:" + key)) {
                        // Forward the stored actor signature; a relay cannot re-sign
                        // state it did not author, so an entry we never saw a signed
                        // DTO for (should not happen for authentic entries) is skipped.
                        SpaceWire.EntryStateDto signed = signedStates.get(entryId);
                        if (signed != null) {
                            (state.present() ? states : tombstones).add(signed);
                        }
                    }
                }
                if (claim != null) {
                    String key = "c:" + entryId.value() + "=" + claimHash;
                    if (!remote.contains(key) && !remote.contains("a:" + key)) {
                        missingClaims.put(entryId.value(), (SpaceWire.SignedClaim) claim);
                    }
                }
            });
            states.addAll(tombstones);
        }
        if (states.isEmpty() && missingClaims.isEmpty()) {
            return new byte[0];
        }
        syncStatesSent.addAndGet(states.size());
        syncClaimsSent.addAndGet(missingClaims.size());
        return codec.toBytes(new SpaceWire.SyncDelta(states, missingClaims));
    }

    /**
     * Applies an anti-entropy delta.
     *
     * @return whether anything in it changed this replica: a delta whose every state and claim was refused or
     *         already held is no activity, so the gossip bus does not reset its backoff for it (IDLE-WORK item 8)
     */
    boolean applySyncDelta(byte[] delta) { // package-private for tests
        return applySync(delta, null);
    }

    /** Applies an anti-entropy delta and names every state and claim it carried (a fetch's answer, RE-I-03). */
    ReconcilableState.Applied applySyncDeltaNamed(byte[] delta) { // package-private for tests
        Set<String> named = new HashSet<>();
        boolean changed = applySync(delta, named);
        return new ReconcilableState.Applied(changed, named);
    }

    /**
     * Applies an anti-entropy delta, as {@link #applySyncDelta}, and adds to {@code named} the id of every state and
     * claim it carried, taken or refused, which a fetch's asker reads to tell what the partner left out (RE-I-03).
     *
     * @param named where the ids go, or null
     */
    private boolean applySync(byte[] delta, Set<String> named) {
        if (delta.length == 0) {
            return false;
        }
        SpaceWire.SyncDelta sync;
        try {
            // HLC round six, 2026-11-08 (owner rulings F1 and F3): only the delta's own wire encoding
            sync = codec.fromWire(delta, SpaceWire.SyncDelta.class);
        } catch (RuntimeException e) {
            deltasUndecodable.incrementAndGet(); // HLC canonical stamps, 2026-11-08
            return false;
        }
        if (sync == null) {
            deltasUndecodable.incrementAndGet();
            return false;
        }
        syncClaimsReceived.addAndGet(sync.claims() == null ? 0 : sync.claims().size());
        syncStatesReceived.addAndGet(sync.states() == null ? 0 : sync.states().size());
        List<Map.Entry<String, SpaceWire.SignedClaim>> claimList = sync.claims() == null ? List.of()
                : new ArrayList<>(sync.claims().entrySet());
        List<SpaceWire.EntryStateDto> stateList = sync.states() == null ? List.of() : sync.states();
        if (named != null) {
            for (Map.Entry<String, SpaceWire.SignedClaim> claim : claimList) {
                named.add(claim.getKey());
            }
            for (SpaceWire.EntryStateDto dto : stateList) {
                EntryId entryId = dto == null ? null : dto.entryIdOf();
                if (entryId != null) {
                    named.add(entryId.value());
                }
            }
        }

        // Before the lock (RE-M-08): every check that reads nothing of this replica, which is where the time goes
        // (an Ed25519 verification per record and per state), and an AUTHORIZER rule's answers, which may consult
        // something outside this node. Under the lock each merge then judges the rest exactly as one delta at a time
        // did, in the same order, so a claim still lands before the completion it authenticates and a credential
        // before the writes it admits.
        // The checks of a large batch run on the verification pool, in parallel (RE-Q-PRF-03): each reads only its own
        // item and the maps built before it, and the merge below takes the results in the batch's own order.
        boolean[] claimVerified = new boolean[claimList.size()];
        inParallel(claimList.size(), i ->
                claimVerified[i] = verifyClaim(EntryId.of(claimList.get(i).getKey()), claimList.get(i).getValue()));
        // A completion is checked against the claim it should meet under the lock: the merge of the claim held now
        // with a verified claim the delta carries for its entry, which is what mergeClaim keeps unless it refuses or
        // defers that claim. The held claims are read in one short hold; a guess that proves wrong is checked again
        // under the lock, so it costs time, never a wrong answer.
        Map<String, SpaceWire.SignedClaim> deltaClaims = new HashMap<>();
        for (int i = 0; i < claimList.size(); i++) {
            if (claimVerified[i]) {
                deltaClaims.put(claimList.get(i).getKey(), claimList.get(i).getValue());
            }
        }
        Map<EntryId, SpaceWire.SignedClaim> heldNow = heldClaimsOfCompletions(stateList);
        Prechecked[] checked = new Prechecked[stateList.size()];
        inParallel(stateList.size(), i -> {
            SpaceWire.EntryStateDto dto = stateList.get(i);
            SpaceWire.SignedClaim fromDelta = null;
            SpaceWire.SignedClaim expected = null;
            if (dto != null && dto.completed() && dto.record() != null && dto.record().entryId() != null) {
                EntryId entryId = dto.record().entryId();
                fromDelta = deltaClaims.get(entryId.value());
                expected = SpaceWire.SignedClaim.merge(heldNow.get(entryId), fromDelta);
            }
            checked[i] = precheck(dto, expected, expected != null && expected == fromDelta);
        });
        Map<String, Boolean> admitted = askAuthorizer(claimList, claimVerified, stateList, checked);

        // Claims before states: a completed state authenticates against the
        // holder's claim, so the claim must be merged first.
        boolean changed = false;
        Batch running = new Batch(admitted);
        // Completions the merge dropped while the delta carried a verified claim for their entry: an outbid
        // holder's completion may be among them (see outbidCompletions), judged after the lock.
        List<Integer> droppedCompletions = new ArrayList<>();
        // Entries for which a claim this delta carried was displaced by one of this peer's while a completion dropped
        // here earlier waits for it (see displacedClaims), judged after the lock.
        Set<EntryId> displacedPairs = new HashSet<>();
        try {
            synchronized (lock) {
                batch = running;
                long lockedFrom = System.nanoTime();
                try {
                    for (int i = 0; i < claimList.size(); i++) {
                        if (claimVerified[i]) {
                            EntryId claimed = EntryId.of(claimList.get(i).getKey());
                            changed |= mergeClaim(claimed, claimList.get(i).getValue());
                            if (!unprovenCompletions.isEmpty() && unprovenCompletions.containsKey(claimed)
                                    && displacedClaims.containsKey(claimed)) {
                                displacedPairs.add(claimed);
                            }
                        }
                    }
                    for (int i = 0; i < stateList.size(); i++) {
                        boolean merged = mergeState(stateList.get(i), checked[i]);
                        changed |= merged;
                        SpaceWire.EntryStateDto dto = stateList.get(i);
                        staleOffered(dto);
                        // Under either strategy (SPEC §7.4, the settle guarantee): a claim later than the window
                        // displaces a holder that already judged itself the winner, under LEASE_RACE as under AUCTION.
                        if (!merged && dto != null && dto.completed()) {
                            droppedCompletions.add(i);
                        }
                    }
                } finally {
                    batch = null;
                    if (running.changed) {
                        generation++; // once for the whole delta
                        signal();
                    }
                    lastBatchLockedNanos = System.nanoTime() - lockedFrom;
                }
            }
        } finally {
            Boolean outer = DELIVERING_BATCH.get();
            DELIVERING_BATCH.set(Boolean.TRUE);
            try {
                for (Runnable work : running.after) {
                    // The events, in the order the merges raised them, after the batch. Each runs whatever an earlier
                    // one did: the states are merged already, so an event skipped here would never fire. fire()
                    // isolates the listeners themselves while DELIVERING_BATCH is set; this keeps a failure anywhere
                    // else in one event to that event.
                    try {
                        work.run();
                    } catch (RuntimeException e) {
                        LOG.log(System.Logger.Level.WARNING, "delivering an event of space '" + name
                                + "' after an anti-entropy batch failed", e);
                    }
                }
            } finally {
                if (outer == null) {
                    DELIVERING_BATCH.remove();
                } else {
                    DELIVERING_BATCH.set(outer);
                }
            }
        }
        for (int i : droppedCompletions) {
            SpaceWire.EntryStateDto dto = stateList.get(i);
            EntryId entryId = dto.entryIdOf();
            SpaceWire.SignedClaim proof = entryId == null ? null : deltaClaims.get(entryId.value());
            if (proof != null) {
                noteOutbidCompletion(entryId, proof, dto);
            }
            // An answer from a replica that has merged the better claim carries a displaced holder's completion under
            // that claim, or under none: the displaced claim may have come in another frame (store race, 2026-11-22).
            if (rememberUnproven(dto)) {
                displacedPairs.add(entryId);
            }
        }
        for (EntryId entryId : displacedPairs) {
            judgeDisplaced(entryId);
        }
        offerDisplaced();
        return changed;
    }

    /** A batch smaller than this is checked on the thread that applies it: handing it off would cost more. */
    static final int PARALLEL_FROM = 32;

    /**
     * The pool that checks a large batch's signatures before its merge (RE-Q-PRF-03): about 500 microseconds a state
     * of decoding and two Ed25519 verifications, which made a join of 100,000 states take most of a minute on one
     * thread. Sized from the available cores less one, at most 16, shared by every space of the process, and its
     * threads end when idle. The thread that applies the batch, a transport's reader or the tick, waits for it.
     */
    private static final class VerifyPool {
        static final int THREADS = Math.max(1, Math.min(16, Runtime.getRuntime().availableProcessors() - 1));
        static final String PREFIX = "space-verify-";
        /**
         * Set on the pool's own threads: a slice that applies a nested batch checks it inline, judged by this mark,
         * never by the thread's name (srad/agentspaces-event-loop.md EL-X-04, phase 4's "thread-name check gone").
         */
        static final ThreadLocal<Boolean> ON_POOL = ThreadLocal.withInitial(() -> Boolean.FALSE);
        static final java.util.concurrent.ThreadPoolExecutor EXECUTOR;

        static {
            java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger();
            EXECUTOR = new java.util.concurrent.ThreadPoolExecutor(THREADS, THREADS, 30,
                    java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(), r -> {
                        Thread thread = new Thread(() -> {
                            ON_POOL.set(Boolean.TRUE);
                            r.run();
                        }, PREFIX + count.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    });
            EXECUTOR.allowCoreThreadTimeOut(true);
        }

        private VerifyPool() {
        }
    }

    /**
     * Runs {@code work} for each index below {@code n}: on the verification pool in about as many slices as it has
     * threads when the batch is large, here otherwise. Returns when every index ran; a failure in any slice is thrown
     * here once all have ended, so no slice still writes a result the caller reads.
     */
    private static void inParallel(int n, java.util.function.IntConsumer work) {
        int threads = VerifyPool.THREADS;
        if (n < PARALLEL_FROM || threads < 2 || VerifyPool.ON_POOL.get()) {
            for (int i = 0; i < n; i++) {
                work.accept(i);
            }
            return;
        }
        int slices = Math.min(threads, (n + 15) / 16);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>(slices);
        for (int slice = 0; slice < slices; slice++) {
            int from = (int) ((long) n * slice / slices);
            int to = (int) ((long) n * (slice + 1) / slices);
            futures.add(VerifyPool.EXECUTOR.submit(() -> {
                for (int i = from; i < to; i++) {
                    work.accept(i);
                }
            }));
        }
        boolean interrupted = false;
        Throwable failed = null;
        for (java.util.concurrent.Future<?> future : futures) {
            while (true) {
                try {
                    future.get();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true; // the slices write into the caller's arrays: wait for them all regardless
                } catch (java.util.concurrent.ExecutionException e) {
                    if (failed == null) {
                        failed = e.getCause();
                    }
                    break;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (failed instanceof RuntimeException e) {
            throw e;
        }
        if (failed instanceof Error e) {
            throw e;
        }
        if (failed != null) {
            throw new IllegalStateException(failed);
        }
    }

    /** The claims held now for the entries a delta completes, read in one short hold of the lock. */
    private Map<EntryId, SpaceWire.SignedClaim> heldClaimsOfCompletions(List<SpaceWire.EntryStateDto> stateList) {
        Map<EntryId, SpaceWire.SignedClaim> held = new HashMap<>();
        List<EntryId> completed = new ArrayList<>();
        for (SpaceWire.EntryStateDto dto : stateList) {
            if (dto != null && dto.completed() && dto.record() != null && dto.record().entryId() != null) {
                completed.add(dto.record().entryId());
            }
        }
        if (!completed.isEmpty()) {
            synchronized (lock) {
                for (EntryId entryId : completed) {
                    held.put(entryId, heldClaim(entryId));
                }
            }
        }
        return held;
    }

    /**
     * Whether two claim proofs are the same: the same object, or the same claim under the same key, signature and
     * certificate. A completion's check depends on nothing else of the claim, so its answer carries from one to the
     * other.
     */
    private static boolean sameProof(SpaceWire.SignedClaim a, SpaceWire.SignedClaim b) {
        if (a == b) {
            return true;
        }
        return a != null && b != null && Objects.equals(a.claim(), b.claim())
                && Arrays.equals(a.holderKey(), b.holderKey()) && Arrays.equals(a.signature(), b.signature())
                && Objects.equals(a.holderCertificate(), b.holderCertificate());
    }

    /**
     * The signature checks of a received state made before the batch's lock: its record's signature, and the state's
     * own signature, a completion's against the claim it is expected to meet at merge time. Null when the state carries
     * no record (a recordless state is merged over the record held here) or fails a check that {@link #mergeState}
     * makes first anyway, so nothing is verified for a state that would be dropped unverified.
     *
     * @param expected      the claim a completion is checked against, or null
     * @param claimVerified whether that claim's own signature was verified already (one the delta carries)
     */
    private Prechecked precheck(SpaceWire.EntryStateDto dto, SpaceWire.SignedClaim expected, boolean claimVerified) {
        if (dto == null || dto.record() == null || dto.issuerPublicKey() == null
                || dto.leaseStamp() == null || dto.leaseValue() == null
                || (dto.adds() != null && dto.adds().size() > MAX_DOTS_PER_STATE)
                || (dto.removes() != null && dto.removes().size() > MAX_DOTS_PER_STATE)
                || dto.adds() == null || dto.removes() == null
                || !id.equals(dto.record().spaceId()) || !dotsBoundToIssuer(dto)) {
            return null;
        }
        boolean record = verifyRecord(dto.record(), dto.issuerPublicKey(), dto.agentCertificate());
        boolean state = record && verifyState(dto, expected, claimVerified);
        return new Prechecked(record, state, dto.completed() ? expected : null);
    }

    /**
     * An AUTHORIZER rule's answers for the agents a delta names, asked before the batch takes the lock: the issuer of
     * each state for WRITE, and the holder of each claim, and of the claim a completion answers, for TAKE. Null under
     * any other rule, whose answer is local and may move within the batch (a credential merged earlier in it).
     */
    private Map<String, Boolean> askAuthorizer(List<Map.Entry<String, SpaceWire.SignedClaim>> claimList,
                                               boolean[] claimVerified, List<SpaceWire.EntryStateDto> stateList,
                                               Prechecked[] checked) {
        if (admission.rule() != SpaceAdvertisement.Admission.AUTHORIZER) {
            return null;
        }
        long now = nowMillis();
        Map<String, Boolean> answers = new HashMap<>();
        Map<String, AgentId> holders = new HashMap<>();
        for (int i = 0; i < claimList.size(); i++) {
            if (claimVerified[i]) {
                AgentId holder = claimList.get(i).getValue().claim().holder();
                holders.put(claimList.get(i).getKey(), holder);
                answers.computeIfAbsent(admissionKey(holder, SpaceAdmission.Scope.TAKE),
                        k -> admission.admits(holder, SpaceAdmission.Scope.TAKE, now));
            }
        }
        for (int i = 0; i < stateList.size(); i++) {
            SpaceWire.EntryStateDto dto = stateList.get(i);
            if (checked[i] == null || !checked[i].record()) {
                continue;
            }
            AgentId issuer = dto.record().issuer();
            answers.computeIfAbsent(admissionKey(issuer, SpaceAdmission.Scope.WRITE),
                    k -> admission.admits(issuer, SpaceAdmission.Scope.WRITE, now));
            if (dto.completed()) {
                AgentId holder = holders.get(dto.record().entryId().value());
                if (holder == null) {
                    synchronized (lock) {
                        SpaceWire.SignedClaim held = heldClaim(dto.record().entryId());
                        holder = held == null ? null : held.claim().holder();
                    }
                }
                if (holder != null) {
                    AgentId taker = holder;
                    answers.computeIfAbsent(admissionKey(taker, SpaceAdmission.Scope.TAKE),
                            k -> admission.admits(taker, SpaceAdmission.Scope.TAKE, now));
                }
            }
        }
        return answers;
    }

    // ---------------------------------------------------------------- internals

    private List<EntryRecord> liveRecords() {
        return liveRecords(null);
    }

    /**
     * Live records, optionally filtered to one schema type (PERF1 phase 2):
     * the type comparison is one string equality, so records of other types
     * cost nothing beyond it, before any claim lookup, list growth, or decode.
     *
     * @param typeName the schema name to keep, or null for every type
     */
    private List<EntryRecord> liveRecords(String typeName) {
        List<EntryRecord> live = new ArrayList<>();
        java.time.Instant now = clock.instant();
        liveIndex.forEach(record -> { // RE-M-04: the present entries alone, not every entry the map holds
            if (typeName != null && !typeName.equals(record.type())) {
                return;
            }
            LeaseInfo lease = record.lease();
            if (lease.kind() == LeaseKind.WRITE && lease.expired(now)) {
                return;
            }
            live.add(record);
        });
        return live;
    }

    /** Live entries with no active claim: what read and readAll may see. */
    private List<EntryRecord> availableRecords() {
        return availableRecords(null);
    }

    /** Available entries of one schema type, or of every type when null. */
    private List<EntryRecord> availableRecords(String typeName) {
        long now = nowMillis();
        List<EntryRecord> available = new ArrayList<>();
        for (EntryRecord record : liveRecords(typeName)) {
            TakeClaim claim = claimOf(record.entryId());
            if (claim == null || claim.expired(now)) {
                available.add(record);
            }
        }
        return available;
    }

    /**
     * The schema name a template's candidates are filtered by: a typed template's own name; for an Object
     * template (v0.1.24) the wire name of its type-name condition when it is one, else every type.
     */
    private String candidateType(Template<?> template) {
        if (template.type() != Object.class) {
            return schemas.schemaNameOf(template.type());
        }
        String name = template.typeName();
        return name != null && name.contains("#") ? name : null;
    }

    private <T> Optional<T> decodeIfMatches(Template<T> template, EntryRecord record) {
        if (!selects(template, record)) {
            return Optional.empty();
        }
        Object value = valueCache.get(record.entryId());
        if (value == null || !template.type().isInstance(value)) {
            // An Object read decodes as the registry can, often a map before this replica's code has used
            // the type; a typed reader that finds that map decodes again as its own type. A typed value
            // replaces the cached one, and an Object read never replaces a cached value (agentspaces-pws
            // phase 1: a generic read hid entries from typed readers for as long as they waited).
            byte[] payload = clearPayload(record);
            if (payload == null) {
                return Optional.empty();
            }
            Object decoded = decodeDocument(payload, template.type());
            if (decoded == null) {
                return Optional.empty();
            }
            if (value == null || template.type() != Object.class) {
                valueCache.put(record.entryId(), decoded);
            }
            value = decoded;
        }
        if (!template.matches(value)) {
            return Optional.empty();
        }
        return Optional.of(template.type().cast(value));
    }

    /** Whether a template selects a record by its type and tags, before any decode. */
    private boolean selects(Template<?> template, EntryRecord record) {
        // An Object template selects any type (v0.1.24): a watcher reads foreign entries through its
        // registry's decoding; a typed template still selects its own schema name only.
        if (template.type() != Object.class && !record.type().equals(schemas.schemaNameOf(template.type()))) {
            return false;
        }
        if (!template.matchesType(record.type())) {
            return false;
        }
        // Issue #16 §9.2 matching order: type, tags, then decode and fields. The
        // tags sit on the record, so a tagged template never decodes an entry it
        // would not select.
        return template.matchesTags(record.tags());
    }

    /**
     * A record's payload in the clear, or null while its block or bulk artifact is not here (a
     * background fetch starts) or when it is sealed under a key this replica lacks.
     */
    private byte[] clearPayload(EntryRecord record) {
        byte[] payload = record.payload();
        if (payload == null && isBulkRef(record)) {
            // LB-E-03: a bulk reference is served when the artifact is complete here and
            // acquired in the background otherwise, with the same never-block rule.
            ai.badmonkey.agentspaces.api.spi.BulkPayloads seam = seamFor(record.payloadRef());
            payload = seam == null ? null : seam.local(record.payloadRef()).orElse(null);
            if (payload == null && seam != null) {
                ensureBulk(record, NO_ARRIVAL);
            }
        } else if (payload == null && record.payloadRef() != null && blocks != null) {
            // Spec §7.2 / P7: reads never block on the network. Serve the
            // block when it is already here; otherwise start one background
            // fetch and report the entry as not yet visible. A blocking read
            // polls within its own timeout and sees the entry once the block
            // lands; readAll simply skips it this time.
            payload = blocks.local(record.payloadRef()).orElse(null);
            if (payload == null) {
                fetchInBackground(record);
            }
        }
        if (payload == null) {
            return null; // block not fetched yet; invisible for now
        }
        if (keyRing != null && payloadSealed(record)) {
            return open(record, payload); // null when sealed under a key this replica lacks
        }
        return payload;
    }

    /** A payload decoded as a value of the type, or null for bytes that hold none. */
    private Object decodeDocument(byte[] payload, Class<?> type) {
        try {
            // HLC round six, 2026-11-08: a payload is a document in the wire encoding. Foreign
            // bytes that happen to decode as CBOR null (a sealed payload whose nonce starts with
            // 0xF6, once in 256 writes) get the same verdict as a decode failure.
            return codec.fromDocument(payload, type);
        } catch (RuntimeException e) {
            return null; // ciphertext without the key, or foreign bytes
        }
    }

    private Optional<Object> decodeValue(EntryRecord record) {
        return decodeValue(record, true);
    }

    /** Decodes an entry's value, keeping it in the value cache when {@code cache} is set (a lapsed entry's is not). */
    private Optional<Object> decodeValue(EntryRecord record, boolean cache) {
        Object cached = valueCache.get(record.entryId());
        if (cached != null) {
            return Optional.of(cached);
        }
        byte[] payload = record.payload();
        if (payload == null && isBulkRef(record)) {
            ai.badmonkey.agentspaces.api.spi.BulkPayloads seam = seamFor(record.payloadRef());
            payload = seam == null ? null : seam.local(record.payloadRef()).orElse(null);
        } else if (payload == null && record.payloadRef() != null && blocks != null) {
            // A block-stored entry: fire its event once the block is available
            // locally. Consulting the local store only (no network) keeps this
            // event path non-blocking; the block usually arrives moments after
            // the record, and a later merge/read fires the deferred event.
            // Without this, notify() subscribers never saw large entries at all.
            payload = blocks.local(record.payloadRef()).orElse(null);
        }
        if (payload == null) {
            return Optional.empty();
        }
        if (keyRing != null && payloadSealed(record)) {
            payload = open(record, payload);
            if (payload == null) {
                return Optional.empty();
            }
        }
        byte[] plain = payload;
        return schemas.classFor(record.type()).flatMap(type -> {
            Object value;
            try {
                value = codec.fromDocument(plain, type); // HLC round six, 2026-11-08
            } catch (RuntimeException e) {
                return Optional.empty(); // ciphertext without the key, or foreign bytes
            }
            if (value == null) {
                return Optional.empty(); // foreign bytes decoding as CBOR null (see decodeIfMatches)
            }
            if (cache) {
                valueCache.put(record.entryId(), value);
            }
            return Optional.of(value);
        });
    }

    /**
     * Starts one background fetch of a content-addressed payload, at most one
     * per CID at a time. The block exchange stores a verified delivery locally
     * before completing, so the next poll finds it through {@code local()}.
     */
    /** Whether a record references a bulk artifact (multibase {@code b}) rather than one block. */
    private static boolean isBulkRef(EntryRecord record) {
        return ai.badmonkey.agentspaces.api.spi.BulkPayloads.isBulk(record.payloadRef());
    }

    /**
     * The seam that carries a bulk reference (PD-E-01): the primary when it accepts the
     * reference, else the fallback when it does, else null, so a reference of a form no
     * seam here carries (a tree root on a replica that holds only the flat seam) waits for
     * anti-entropy instead of opening a fetch that can never complete. A reader so
     * reassembles a reference through the seam that published it, and the tree seam and
     * the flat seam coexist in one space.
     */
    public ai.badmonkey.agentspaces.api.spi.BulkPayloads seamFor(String ref) { // public for tests and the console
        if (bulk != null && bulk.accepts(ref)) {
            return bulk;
        }
        if (bulkFallback != null && bulkFallback.accepts(ref)) {
            return bulkFallback;
        }
        return null;
    }

    /**
     * Whether a record's payload was sealed by this space before it left (today's rule,
     * and the flat seam's): every inline and block payload, and a bulk payload unless the
     * space escalates unsealed through its primary seam and that seam carries the reference
     * (PD-E-04). Only called when the space has a key ring.
     */
    private boolean payloadSealed(EntryRecord record) {
        if (sealBeforeEscalate || bulk == null || record.payloadRef() == null || !isBulkRef(record)) {
            return true;
        }
        return !bulk.accepts(record.payloadRef());
    }

    /**
     * The seam a write above the threshold takes (PD-E-06): the primary when every current
     * member can receive its payloads, else the fallback, else null for the block path. A
     * space that requires the primary refuses the write naming the members that lack it.
     */
    private ai.badmonkey.agentspaces.api.spi.BulkPayloads escalationSeam() {
        List<PeerId> members = new ArrayList<>();
        for (ai.badmonkey.agentspaces.peering.membership.GroupMembership.Member member
                : runtime.membership().allMembers()) {
            members.add(member.id());
        }
        List<PeerId> lacking = bulk.unable(members);
        if (lacking.isEmpty()) {
            return bulk;
        }
        if (bulkRequired) {
            List<String> names = new ArrayList<>();
            for (PeerId peer : lacking) {
                names.add(peer.display());
            }
            throw new IllegalStateException("space '" + name + "' requires the tree exchange for payloads above "
                    + escalateAbove + " bytes (agentspaces.pdt.spaces." + name + ".require) and these members"
                    + " lack it: " + names);
        }
        return bulkFallback;
    }

    /**
     * Starts acquiring a bulk artifact once; the arrival callback runs when it is complete
     * here. A fetch the seam gives up on (every holder unreachable past its deadline) is
     * retried with the back-off of the block path while the entry lives
     * (srad/agentspaces-pdt.md PD-Q-AVL-01), as a block is; without the retry an entry
     * whose first tree fetch failed stayed invisible here for the life of the space.
     */
    private void ensureBulk(EntryRecord record, Runnable onArrival) {
        ensureBulk(record, onArrival, 0);
    }

    private void ensureBulk(EntryRecord record, Runnable onArrival, int attempt) {
        String ref = record.payloadRef();
        ai.badmonkey.agentspaces.api.spi.BulkPayloads seam = seamFor(ref);
        if (seam == null) {
            return;
        }
        // One fetch per reference at a time: a read's poll adds nothing to a fetch in flight,
        // while a callback that must run (the deferred WRITTEN, a retry's) joins it, since the
        // seam keeps every callback of a reference; otherwise a poll that started the fetch
        // first would have swallowed the event.
        if (!fetching.add(ref) && onArrival == NO_ARRIVAL) {
            return;
        }
        try {
            seam.ensure(ref, record.issuer().peer(), () -> {
                fetching.remove(ref);
                bulkRetries.remove(ref);
                signalArrival(); // a read or take waiting on this artifact may decode it now
                onArrival.run();
            }, () -> {
                fetching.remove(ref);
                long delay = BLOCK_RETRY_DELAYS_MILLIS[Math.min(attempt, BLOCK_RETRY_DELAYS_MILLIS.length - 1)];
                long at = nowMillis() + delay;
                bulkRetries.put(ref, new BlockRetry(record, attempt + 1, at, onArrival));
                lowerDue(retryDue, at);
            });
        } catch (RuntimeException e) {
            fetching.remove(ref);
        }
    }

    /**
     * Tick work (PD-Q-AVL-01): starts the next attempt of every due bulk fetch whose entry
     * is still present here and whose payload is still missing; forgets the rest.
     */
    private void retryBulkFetches() {
        if (bulkRetries.isEmpty()) {
            return;
        }
        long now = nowMillis();
        for (Map.Entry<String, BlockRetry> entry : bulkRetries.entrySet()) {
            BlockRetry retry = entry.getValue();
            if (retry.nextAtMillis() > now) {
                continue;
            }
            String ref = entry.getKey();
            ai.badmonkey.agentspaces.api.spi.BulkPayloads seam = seamFor(ref);
            if (seam != null && seam.local(ref).isPresent()) {
                // Complete here by another way; the arrival this retry carried is still owed, as for a block.
                if (bulkRetries.remove(ref, retry)) {
                    retry.onArrival().run();
                }
                continue;
            }
            if (seam == null || recordOf(retry.record().entryId(), true).isEmpty()) {
                if (bulkRetries.remove(ref, retry)) {
                    awaitingPayload.remove(retry.record().entryId());
                }
                continue;
            }
            if (bulkRetries.remove(ref, retry)) {
                ensureBulk(retry.record(), retry.onArrival(), retry.attempts());
            }
        }
    }

    /** Bulk fetches awaiting a retry, by reference; for tests and the console. */
    public int bulkRetriesPending() {
        return bulkRetries.size();
    }

    private void fetchInBackground(EntryRecord record) {
        fetchInBackground(record, () -> { });
    }

    /**
     * As {@link #fetchInBackground(EntryRecord)}, running {@code onArrival} once
     * the block is stored locally (not on a failed or duplicate fetch). Used to
     * fire the WRITTEN event a merge had to defer.
     */
    private void fetchInBackground(EntryRecord record, Runnable onArrival) {
        fetchInBackground(record, onArrival, 0);
    }

    /**
     * Starts attempt {@code attempt} (zero-based) of a block fetch on a virtual
     * thread. A failed attempt schedules the next one on the group tick with the
     * back-off of {@link #BLOCK_RETRY_DELAYS_MILLIS} while the entry lives
     * (LB-B-02); the first attempt waits 800 ms and later ones longer, so a slow
     * holder on a thin link is given the time its block needs.
     */
    private void fetchInBackground(EntryRecord record, Runnable onArrival, int attempt) {
        String cid = record.payloadRef();
        if (!fetching.add(cid)) {
            return; // a fetch for this block is already in flight
        }
        List<PeerId> candidates = new ArrayList<>();
        candidates.add(record.issuer().peer());
        candidates.addAll(runtime.sampler().randomMembers(2));
        long timeout = BLOCK_FETCH_TIMEOUTS_MILLIS[Math.min(attempt, BLOCK_FETCH_TIMEOUTS_MILLIS.length - 1)];
        // A task of the node's time source, which runs it on a virtual thread (TS-2): the fetch waits on that source
        // (group W), so on a virtual one the advance that passes its deadline settles it before time moves on, and its
        // retry is filed at the virtual instant it failed (phase 6, with BlockExchange's wait).
        runtime.time().schedule(java.time.Duration.ZERO, () -> {
            boolean landed = false;
            try {
                landed = blocks.fetch(cid, candidates, timeout).isPresent();
            } catch (RuntimeException e) {
                // fall through to the retry schedule
            } finally {
                fetching.remove(cid);
            }
            if (landed) {
                blockRetries.remove(cid);
                signalArrival(); // a read or take waiting on this payload may decode it now
                onArrival.run();
            } else {
                long delay = BLOCK_RETRY_DELAYS_MILLIS[Math.min(attempt, BLOCK_RETRY_DELAYS_MILLIS.length - 1)];
                long at = nowMillis() + delay;
                blockRetries.put(cid, new BlockRetry(record, attempt + 1, at, onArrival));
                lowerDue(retryDue, at);
            }
        });
    }

    /**
     * Tick work (LB-B-02): starts the next attempt of every due block fetch whose
     * entry is still present here and whose block is still missing; forgets the
     * rest. A present entry keeps retrying once a minute for as long as it lives.
     */
    private void retryBlockFetches() {
        if (blockRetries.isEmpty()) {
            return;
        }
        long now = nowMillis();
        for (Map.Entry<String, BlockRetry> entry : blockRetries.entrySet()) {
            BlockRetry retry = entry.getValue();
            if (retry.nextAtMillis() > now) {
                continue;
            }
            String cid = entry.getKey();
            if (blocks.local(cid).isPresent()) {
                // The block landed another way, a read's fetch most often; the arrival this retry carried is still
                // owed, or a deferred event would be lost for good.
                if (blockRetries.remove(cid, retry)) {
                    retry.onArrival().run();
                }
                continue;
            }
            if (recordOf(retry.record().entryId(), true).isEmpty()) {
                if (blockRetries.remove(cid, retry)) {
                    awaitingPayload.remove(retry.record().entryId());
                }
                continue;
            }
            if (blockRetries.remove(cid, retry)) {
                fetchInBackground(retry.record(), retry.onArrival(), retry.attempts());
            }
        }
    }

    /**
     * Records a lease duration this replica has observed for the space, for
     * the tombstone GC horizon (spec §7.3). Capped at
     * {@link #MAX_TRACKED_LEASE_MILLIS}; non-positive values are ignored.
     */
    private void observeLease(long leaseMillis) {
        long capped = Math.min(leaseMillis, MAX_TRACKED_LEASE_MILLIS);
        if (capped > maxLeaseObservedMillis) {
            synchronized (lock) {
                if (capped > maxLeaseObservedMillis) {
                    maxLeaseObservedMillis = capped;
                }
            }
        }
    }

    /**
     * The lease sweep (spec §7.2, §7.3). Callers hold {@link #lock}; the
     * returned event deliveries run after it is released.
     *
     * <p>Events fire once per transition: {@code EXPIRED} once per entry whose
     * write lease has lapsed, unless a cancel already raised it (core cancel
     * events, 2026-11-09), {@code REAPPEARED} once per lapsed claim epoch,
     * which is the same per-entry dedup the WRITTEN and COMPLETED paths use.
     * Then tombstones (completed, withdrawn, or lapsed entries past their
     * horizon, {@link SpaceStateCrdt#keepMillis}) are collected, along with
     * every side table keyed by their id, so digests stop advertising them.
     *
     * <p>The sweep visits only the entries the expiry index files at or before
     * {@code now} (IDLE-WORK item 4), in the order the full pass took its steps:
     * lapses and reappearances, then orphaned claims, then collection. What it
     * does is exactly what a pass over every entry would do, since an entry the
     * index does not name has nothing due; {@link #fullScanPlan} is that pass,
     * kept for the property test that holds the two together.
     */
    private List<Runnable> sweepLocked(long now) {
        List<Runnable> events = new ArrayList<>();
        if (now < indexedAtMillis) {
            rebuildIndexLocked(now); // the clock stepped back: what is due was judged from a later time
        }
        // PD-L-02: a recordless state whose record never came is dropped after its TTL.
        if (now >= pendingDueMillis) {
            pendingStates.values().removeIf(pending -> now - pending.sinceMillis() > PENDING_STATE_TTL_MILLIS);
            long next = Long.MAX_VALUE;
            for (PendingState pending : pendingStates.values()) {
                next = Math.min(next, pending.sinceMillis() + PENDING_STATE_TTL_MILLIS + 1);
            }
            pendingDueMillis = next;
        }
        if (credentialRule != null) {
            credentialRule.index().expire(now); // spec §7.5: a lapsed credential stops admitting
        }
        forgetCollectedAcks(now);
        forgetRefusedAcks(now);
        Set<EntryId> due = new java.util.LinkedHashSet<>();
        java.util.NavigableMap<Long, Set<EntryId>> head = dueByTime.headMap(now, true);
        for (Set<EntryId> ids : head.values()) {
            due.addAll(ids);
        }
        head.clear();
        due.forEach(dueOf::remove);
        if (now > indexedAtMillis) {
            indexedAtMillis = now;
        }
        if (due.isEmpty()) {
            lastSweep = SweepActions.NONE;
            return events;
        }
        sweepVisits += due.size();
        Set<EntryId> expired = new HashSet<>();
        Map<EntryId, Long> reappeared = new HashMap<>();
        for (EntryId entryId : due) {
            EntryState state = crdt.state(entryId).orElse(null);
            if (state == null || !state.present()) {
                continue;
            }
            EntryRecord record = state.record().withLease(state.lease().value());
            if (record.lease().expiresAtMillis() <= now) {
                if (expiredFired.add(entryId)) {
                    expired.add(entryId);
                    events.add(expiredEvent(entryId, record));
                }
                continue;
            }
            TakeClaim claim = claimOf(entryId);
            if (claim != null && claim.expired(now)
                    && !Long.valueOf(claim.epoch()).equals(reappearedEpochs.get(entryId))) {
                reappearedEpochs.put(entryId, claim.epoch());
                reappeared.put(entryId, claim.epoch());
                deferredEvent(events, SpaceEvent.Kind.REAPPEARED, record);
            }
        }
        Set<EntryId> orphans = dropOrphanedClaims(due, now);
        Set<EntryId> collected = new HashSet<>();
        long maxLease = maxLeaseMillis();
        SpaceStateCrdt.Horizon rule = horizon(now);
        for (EntryId entryId : due) {
            EntryState state = crdt.state(entryId).orElse(null);
            if (state != null && collectableHere(state, now, rule)) {
                collected.add(entryId);
            }
        }
        if (!collected.isEmpty()) {
            if (blocks != null || bulk != null) {
                for (EntryId entryId : collected) {
                    crdt.state(entryId).map(EntryState::record).map(EntryRecord::payloadRef)
                            .ifPresent(ref -> {
                                if (ai.badmonkey.agentspaces.api.spi.BulkPayloads.isBulk(ref)) {
                                    ai.badmonkey.agentspaces.api.spi.BulkPayloads seam = seamFor(ref);
                                    if (seam != null) {
                                        seam.release(ref); // LB-E-04: a collected entry unpins its artifact
                                    }
                                } else if (blocks != null) {
                                    blocks.release(ref); // LB-B-04: a collected entry unpins its block
                                    blockRetries.remove(ref);
                                }
                            });
                }
            }
            if (!awaitingWriter.isEmpty()) {
                for (EntryId entryId : collected) {
                    crdt.state(entryId).ifPresent(state -> {
                        for (Dot add : state.adds()) {
                            Set<EntryId> waiting = awaitingWriter.get(add.replica());
                            if (waiting != null && waiting.remove(entryId) && waiting.isEmpty()) {
                                awaitingWriter.remove(add.replica());
                            }
                        }
                    });
                }
            }
            crdt = crdt.without(collected);
            for (EntryId entryId : collected) {
                refreshLive(entryId); // a present entry whose lease lapsed past its grace leaves the live index here
            }
            generation++;
            for (EntryId entryId : collected) {
                if (credentialRule != null) {
                    credentialRule.index().remove(entryId);
                }
                issuerKeys.remove(entryId);
                issuerCertificates.remove(entryId);
                logDecidedEpochs.remove(entryId);
                crdt = crdt.withoutClaim(entryId);
                generation++;
                signedStates.remove(entryId);
                valueCache.remove(entryId);
                awaitingPayload.remove(entryId);
                forgetShardDropped(entryId);
                replicated.remove(entryId);
                ownWrites.remove(entryId);
                pendingStates.remove(entryId);
                generation++;
                expiredFired.remove(entryId);
                reappearedEpochs.remove(entryId);
                takenEpochs.remove(entryId);
                outbidCompletions.remove(entryId);
                completedHere.remove(entryId);
                displacedClaims.remove(entryId);
                unprovenCompletions.remove(entryId);
                displacedOwn.remove(entryId);
                displacedPulls.remove(entryId);
            }
        }
        for (EntryId entryId : due) {
            if (!collected.contains(entryId)) {
                reindex(entryId, now); // its next due time, if it has one
            }
        }
        if (!expired.isEmpty() || !reappeared.isEmpty() || !orphans.isEmpty() || !collected.isEmpty()) {
            // a lapsed claim makes its entry takeable, and a waiting take hears of it now; a lapsed entry moves the
            // change signal too, so a participant that keeps something alive in this space (the content swarm's
            // manifest repair, owner answer 5) hears of the lapse at the lapse (srad/agentspaces-event-loop.md §10.14)
            signal();
        }
        lastSweep = new SweepActions(expired, reappeared, orphans, collected);
        return events;
    }

    /**
     * Drops each lapsed claim for an entry this replica does not hold, never held or already
     * collected (HLC round six, 2026-11-08, ruling R10). A claim merges before its entry's state,
     * since a completed state authenticates against it, so a claim may arrive for an entry that
     * never follows; kept, every such claim stayed for good. A live claim stays, and an
     * anti-entropy partner that holds the entry offers its claim again with the state. Callers
     * hold {@link #lock}.
     *
     * @param due the entries the expiry index names as due; a lapsed orphaned claim is filed under its lapse
     * @return the entries whose claim was dropped
     */
    private Set<EntryId> dropOrphanedClaims(Set<EntryId> due, long now) {
        Set<EntryId> dropped = new HashSet<>();
        for (EntryId entryId : due) {
            SpaceWire.SignedClaim claim = heldClaim(entryId);
            if (claim != null && claim.claim().expired(now) && crdt.state(entryId).isEmpty()) {
                crdt = crdt.withoutClaim(entryId);
                logDecidedEpochs.remove(entryId);
                takenEpochs.remove(entryId);
                reappearedEpochs.remove(entryId);
                generation++;
                dropped.add(entryId);
            }
        }
        return dropped;
    }

    // ---------------------------------------------------------------- the expiry index (IDLE-WORK item 4)

    /** The earliest time the sweep has anything to do. Callers hold {@link #lock}. */
    private long nextDueLocked() {
        long next = dueByTime.isEmpty() ? Long.MAX_VALUE : dueByTime.firstKey();
        if (!collectedAcks.isEmpty()) {
            next = Math.min(next, collectedAcks.values().iterator().next()); // RE-H-03: the oldest hold ends first
        }
        if (!refusedForgetBy.isEmpty()) {
            next = Math.min(next, refusedForgetBy.firstKey()); // P6: a door acknowledgement's time to be forgotten
        }
        return Math.min(next, pendingDueMillis);
    }

    /**
     * Files an entry under the earliest time at or after {@code now} the sweep would do something for it, or takes it
     * out of the index when nothing ever would; called under {@link #lock} after every change to the entry's state,
     * its claim, or the sweep's bookkeeping for it.
     */
    private void reindex(EntryId entryId, long now) {
        markLapse(entryId, now);
        long due = dueAt(entryId, now);
        Long filed = dueOf.get(entryId);
        if (filed != null) {
            if (filed == due) {
                return;
            }
            Set<EntryId> at = dueByTime.get(filed);
            if (at != null && at.remove(entryId) && at.isEmpty()) {
                dueByTime.remove(filed);
            }
            dueOf.remove(entryId);
        }
        if (due != Long.MAX_VALUE) {
            dueOf.put(entryId, due);
            dueByTime.computeIfAbsent(due, t -> new HashSet<>()).add(entryId);
            lowerDue(sweepDue, due); // DW-2: an entry merged by gossip wakes no participant by itself
        }
        if (now > indexedAtMillis) {
            indexedAtMillis = now;
        }
    }

    /**
     * The earliest time at or after {@code now} at which the sweep acts on an entry, given its state as it is, or
     * {@link Long#MAX_VALUE} for never. Each case mirrors one step of the sweep:
     * <ul>
     *   <li>a present entry raises EXPIRED once its write lease lapses, unless it already did;</li>
     *   <li>a present entry raises REAPPEARED once per claim epoch when its claim lapses while its lease still runs,
     *       so only a claim that lapses before the lease is due; once the lease lapses first, the lapse wins;</li>
     *   <li>a lapsed claim on an entry this replica does not hold is dropped;</li>
     *   <li>a tombstone is collected at its horizon ({@link SpaceStateCrdt#collectionHorizon}), which is never before
     *       a present entry's lease lapses.</li>
     * </ul>
     * The maximum lease only grows, so a horizon judged earlier is never late: at worst the entry is visited early,
     * found not yet collectable, and filed again.
     */
    private long dueAt(EntryId entryId, long now) {
        EntryState state = crdt.state(entryId).orElse(null);
        TakeClaim claim = claimOf(entryId);
        if (state == null) {
            return claim == null ? Long.MAX_VALUE : Math.max(claim.expiresAtMillis(), now);
        }
        long due = Long.MAX_VALUE;
        long leaseExpiry = state.lease().value().expiresAtMillis();
        if (state.present()) {
            // A present entry is also due at its expiry while its lapse mark is unset (RE-T-06): one renewed after its
            // EXPIRED fired here lapses again at the new expiry, and the sweep's visit then sets the mark.
            if (!expiredFired.contains(entryId) || !crdt.lapsed(entryId)) {
                due = Math.max(leaseExpiry, now);
            }
            if (claim != null && !Long.valueOf(claim.epoch()).equals(reappearedEpochs.get(entryId))) {
                long from = Math.max(claim.expiresAtMillis(), now);
                if (from < leaseExpiry) {
                    due = Math.min(due, from);
                }
            }
        }
        SpaceStateCrdt.Horizon mode = filedHorizon();
        SpaceStateCrdt.Horizon rule = ruleFor(state, mode, now);
        long horizon = horizonHere(state, mode, rule);
        if (mode == SpaceStateCrdt.Horizon.PER_ENTRY && rule == SpaceStateCrdt.Horizon.LEGACY) {
            // A withdrawal whose writer is in view but not yet long enough is due once the writer's wait is over, if
            // that comes before its horizon now. A visit that finds the writer gone files it again, no earlier.
            long wait = writerWaitMillis(state, now);
            if (wait != Long.MAX_VALUE) {
                long perEntry = SpaceStateCrdt.collectionHorizon(state, maxLeaseMillis(), mode);
                long trusted = now > Long.MAX_VALUE - wait ? Long.MAX_VALUE : now + wait;
                horizon = Math.min(horizon, Math.max(perEntry, trusted));
            } else {
                String writer = absentWriter(state);
                if (writer != null) {
                    awaitingWriter.computeIfAbsent(writer, w -> new HashSet<>()).add(entryId);
                }
            }
        }
        if (horizon != Long.MAX_VALUE) {
            due = Math.min(due, Math.max(horizon, now));
        }
        return due;
    }

    /**
     * Keeps the entry's lapse mark in step with this replica's clock (RE-T-06, annex I.6): a present entry whose write
     * lease has run out contributes neither its state line nor its claim line to the audited set, and one renewed or
     * replaced by a state that has not lapsed contributes them again. Run wherever the expiry index is, so the mark
     * follows every change; the sweep visits a present entry at its lease expiry to set it. Callers hold {@link #lock}.
     */
    private void markLapse(EntryId entryId, long now) {
        EntryState state = crdt.state(entryId).orElse(null);
        boolean lapsed = state != null && state.present() && state.lease().value().expiresAtMillis() <= now;
        if (crdt.lapsed(entryId) != lapsed) {
            crdt = crdt.withLapsed(entryId, lapsed);
        }
    }

    /**
     * Judges the horizon's mode at {@code now} and, when the per-entry horizon has just come into force or has just
     * gone out of force, files every entry afresh (RE-H-04): coming into force, its withdrawals fall due earlier than
     * the index filed them, since the per-entry horizon is never later than the legacy one (both keep at least the
     * drift floor of SPEC §7.3); going out of force, they wait longer, and filing afresh spares the sweep early
     * visits. Once per settlement or disengagement, so the pass over every entry is rare. Then it files again the
     * withdrawals of writers that were out of view and are back. Callers hold {@link #lock}.
     */
    private void refileIfHorizonMoved(long now) {
        horizon(now);
        if (horizonRefile) {
            horizonRefile = false;
            rebuildIndexLocked(now);
            return;
        }
        if (awaitingWriter.isEmpty() || filedHorizon() != SpaceStateCrdt.Horizon.PER_ENTRY) {
            return;
        }
        // A writer that was out of view and is back: its withdrawals now have a wait that ends, so file them again.
        for (Iterator<Map.Entry<String, Set<EntryId>>> it = awaitingWriter.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<String, Set<EntryId>> waiting = it.next();
            long inView;
            try {
                inView = runtime.hintedWriterInViewMillis(PeerId.of(waiting.getKey()));
            } catch (IllegalArgumentException malformed) {
                it.remove(); // never readable, so never trusted: the legacy horizon they are filed at stands
                continue;
            }
            if (inView >= 0) {
                it.remove();
                for (EntryId entryId : waiting.getValue()) {
                    reindex(entryId, now);
                }
            }
        }
    }

    /** Files every known entry and claim afresh, judged from {@code now}. Callers hold {@link #lock}. */
    private void rebuildIndexLocked(long now) {
        dueByTime.clear();
        dueOf.clear();
        awaitingWriter.clear(); // the pass below files every withdrawal again, and notes the writers still away
        writerStays.keySet().removeIf(writer -> writerInViewSince(writer, now) == Long.MAX_VALUE);
        indexedAtMillis = now;
        // Every id holding a state or a claim, each once: the slots of the map hold both.
        crdt.forEachHeld((entryId, state, stateHash, claim, claimHash) -> reindex(entryId, now));
    }

    /**
     * The state of an entry or its claim changed: the digest cache is stale, the entry's place in the expiry index
     * moves, and a waiting take or read may now succeed. Callers hold {@link #lock}.
     */
    private void bump(EntryId entryId) {
        reindex(entryId, nowMillis());
        bump();
    }

    /**
     * The reconciled state changed in a way that moves no entry in the expiry index. Callers hold {@link #lock}. Inside
     * a batch ({@link #applySyncDelta}) the generation moves once, when the batch ends (RE-M-08), so no reader of the
     * digest cache can see a delta half applied, and nobody waiting can run before the lock is released anyway.
     */
    private void bump() {
        Batch running = batch;
        if (running != null && running.owner == Thread.currentThread()) {
            running.changed = true;
            return;
        }
        generation++;
        signal();
    }

    /** Wakes the takes and reads waiting for a change. Callers hold {@link #lock}. */
    private void signal() {
        signals++;
        if (waiters > 0) {
            lock.notifyAll();
        }
        for (Runnable listener : changeListeners) {
            try {
                listener.run(); // a wake only (Space.onChangeSignal): never blocks, never re-enters the space
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "space " + name + ": a change listener threw", e);
            }
        }
    }

    /** The {@link #onChangeSignal} listeners (srad/agentspaces-event-loop.md DW-2). */
    private final java.util.List<Runnable> changeListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override
    public AutoCloseable onChangeSignal(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        changeListeners.add(listener);
        return () -> changeListeners.remove(listener);
    }

    /**
     * A counter that moves with every change a reader of this space could act on: a write, a merge, a renewal, a
     * cancel, a claim, a sweep that lapses a claim, a block or artifact landing. A component that derives state from
     * the space recomputes only when it moved (idle-loop O4, reviews/IDLE-LOOP-2026-11-18.md); time alone moves it
     * only through the sweep, so a reader with leases of its own also keeps their due times.
     *
     * @return the change count; only its equality between two reads means anything
     */
    @Override
    public long changeSignals() {
        return signals;
    }

    @Override
    public void awaitChange(long seen, Duration max) throws InterruptedException {
        Objects.requireNonNull(max, "max");
        long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(waits, max); // one base (EL-C-10)
        synchronized (lock) {
            while (signals == seen) {
                waiters++;
                boolean early;
                try {
                    early = waits.waitOn(lock, deadline);
                } finally {
                    waiters--;
                }
                if (!early) {
                    return;
                }
            }
        }
    }

    /** {@link #signal()} from outside the lock: a block or an artifact landed, so a waiting read may decode now. */
    private void signalArrival() {
        synchronized (lock) {
            signal();
        }
    }

    /**
     * What one sweep did, for the property test that holds the indexed sweep to the full pass: the entries it raised
     * EXPIRED for, the claim epochs it raised REAPPEARED for, the orphaned claims it dropped, and the entries it
     * collected.
     */
    record SweepActions(Set<EntryId> expired, Map<EntryId, Long> reappeared, Set<EntryId> orphanClaims,
                        Set<EntryId> collected) {
        static final SweepActions NONE = new SweepActions(Set.of(), Map.of(), Set.of(), Set.of());
    }

    /** What the last sweep did. */
    SweepActions lastSweepActions() { // package-private for tests
        synchronized (lock) {
            return lastSweep;
        }
    }

    /**
     * What a sweep at {@code now} would do if it visited every entry, as the sweep did before the expiry index;
     * changes nothing. The property test compares it with what the indexed sweep then does.
     */
    SweepActions fullScanPlan(long now) { // package-private for tests
        synchronized (lock) {
            Set<EntryId> expired = new HashSet<>();
            Map<EntryId, Long> reappeared = new HashMap<>();
            for (EntryRecord record : crdt.liveRecords()) {
                EntryId entryId = record.entryId();
                if (record.lease().expiresAtMillis() <= now) {
                    if (!expiredFired.contains(entryId)) {
                        expired.add(entryId);
                    }
                    continue;
                }
                TakeClaim claim = claimOf(entryId);
                if (claim != null && claim.expired(now)
                        && !Long.valueOf(claim.epoch()).equals(reappearedEpochs.get(entryId))) {
                    reappeared.put(entryId, claim.epoch());
                }
            }
            Set<EntryId> orphans = new HashSet<>();
            crdt.forEachHeld((entryId, state, stateHash, claim, claimHash) -> {
                if (state == null && ((SpaceWire.SignedClaim) claim).claim().expired(now)) {
                    orphans.add(entryId);
                }
            });
            Set<EntryId> collected = new HashSet<>();
            long maxLease = maxLeaseMillis();
            SpaceStateCrdt.Horizon rule = horizon(now);
            crdt.forEachHeld((entryId, state, stateHash, claim, claimHash) -> {
                if (state != null && collectableHere(state, now, rule)) {
                    collected.add(entryId);
                }
            });
            return new SweepActions(expired, reappeared, orphans, collected);
        }
    }

    /** Queues an event for delivery outside the lock; silent when the value cannot be decoded here. */
    private void deferredEvent(List<Runnable> events, SpaceEvent.Kind kind, EntryRecord record) {
        events.add(() -> decodeValue(record).ifPresent(value ->
                fire(kind, record.entryId(), value, record.issuer(), record.issuer(), record)));
    }

    /**
     * The {@code EXPIRED} event of an entry that left this replica by its lease lapse or by its writer's cancel
     * (core cancel events, 2026-11-09), to deliver outside the lock. Callers hold {@link #lock} and have just added
     * the entry to {@link #expiredFired}. Reads keep to live entries, so nothing reads the decoded value of a gone
     * entry again: the event carries the cached value out rather than decoding it back in, and a stream of large
     * entries stops pinning minutes of values when each entry goes, not at its collection (2026-11-07).
     */
    private Runnable expiredEvent(EntryId entryId, EntryRecord record) {
        Object gone = valueCache.remove(entryId);
        if (gone != null) {
            return () -> fire(SpaceEvent.Kind.EXPIRED, entryId, gone, record.issuer(), record.issuer(), record);
        }
        return () -> decodeValue(record, false).ifPresent(value ->
                fire(SpaceEvent.Kind.EXPIRED, entryId, value, record.issuer(), record.issuer(), record));
    }

    /**
     * The {@code EXPIRED} event a withdrawal raises here (core cancel events, 2026-11-09; SPEC §7.2), or null. A
     * cancel, this replica's own or a merged one, that leaves an entry this replica held present withdrawn and
     * uncompleted raises {@code EXPIRED}, the kind {@code LocalSpace} raises for a cancel. {@link #expiredFired}
     * keeps it to one per entry, whichever comes first of the cancel and the lease sweep, so a redelivered cancel
     * raises nothing. An entry learned first as withdrawn was never present here, and an entry completed first stays
     * completed (completion dominates), so neither raises one. Callers hold {@link #lock}.
     *
     * @param wasPresent whether the entry was present here before the cancel or the merge
     */
    private Runnable withdrawnEvent(EntryId entryId, boolean wasPresent) {
        EntryState state = crdt.state(entryId).orElse(null);
        if (!wasPresent || state == null || state.present() || state.completed() || !expiredFired.add(entryId)) {
            return null;
        }
        return expiredEvent(entryId, state.record().withLease(state.lease().value()));
    }

    /** Binds a ciphertext to its entry within this space (and so this group). */
    private byte[] aadFor(EntryId entryId, long epoch) {
        // Epoch 0 keeps the v0.1.12 binding; a later epoch is bound as well, so
        // a ciphertext cannot be relabelled with another epoch (SPEC §11a.1).
        String bound = epoch == 0 ? id.value() + "|" + entryId.value()
                : id.value() + "|" + entryId.value() + "|" + epoch;
        return bound.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Opens a sealed payload with the keys held for the epoch its record names,
     * or returns null (sealed under a key this replica lacks; the ring reports
     * the epoch missing so the content-key agent can fetch it).
     */
    private byte[] open(EntryRecord record, byte[] sealed) {
        long epoch = record.sealedEpoch();
        byte[] aad = aadFor(record.entryId(), epoch);
        List<ai.badmonkey.agentspaces.common.crypto.GroupKey> keys = keyRing.openingKeys(epoch);
        for (ai.badmonkey.agentspaces.common.crypto.GroupKey key : keys) {
            Optional<byte[]> opened = key.decrypt(sealed, aad);
            if (opened.isPresent()) {
                return opened.get();
            }
        }
        if (!keys.isEmpty()) {
            // Keys are held for the epoch but none opens it: another rotator's
            // key for the same epoch exists (SPEC §11a.3); ask for it again.
            keyRing.reportUnopenable(epoch);
        }
        return null;
    }

    /**
     * The group's revocations as this space enforces them (SPEC §6.1, v0.1.13),
     * for capabilities built over the space that must apply the same view.
     *
     * @return the revocation view
     */
    public ai.badmonkey.agentspaces.api.security.RevocationView revocationView() {
        return runtime.revocationView();
    }

    /**
     * The revocation view behind a space handle: a replicated space's, or the
     * one its per-agent view ({@link #as}) shares; {@code NONE} otherwise.
     *
     * @param space a space handle
     * @return the view it enforces
     */
    public static ai.badmonkey.agentspaces.api.security.RevocationView revocationViewOf(Space space) {
        if (space instanceof ReplicatedSpace replicated) {
            return replicated.revocationView();
        }
        if (space instanceof View view) {
            return view.owner().revocationView();
        }
        return ai.badmonkey.agentspaces.api.security.RevocationView.NONE;
    }

    /**
     * This space's content-key ring (SPEC §11a.3), when the space is encrypted.
     *
     * @return the ring
     */
    public Optional<ai.badmonkey.agentspaces.common.crypto.GroupKeyRing> keyRing() {
        return Optional.ofNullable(keyRing);
    }

    /**
     * The certificate a subordinate actor attaches to a signature stamped
     * {@code stamp} (SPEC §4.2, v0.1.13), or null for a peer-signed actor. An
     * actor holding no certificate that covers the stamp cannot sign: the
     * signature would be refused everywhere, so the write fails here, loudly.
     */
    private static AgentCertificate coveringCertificate(AgentIdentity actor, HlcTimestamp stamp) {
        if (!actor.isSubordinate()) {
            return null;
        }
        Instant signingTime = Instant.ofEpochMilli(stamp.physical());
        return actor.certificateCovering(signingTime).orElseThrow(() ->
                new IllegalStateException("agent " + actor.id().encoded()
                        + " holds no certificate covering " + signingTime
                        + "; re-issue it (a renewing identity does so itself)"));
    }

    private EntryRecord withSignature(EntryRecord unsigned, AgentIdentity actor) {
        byte[] canonical = signableBytes(unsigned);
        return new EntryRecord(unsigned.entryId(), unsigned.spaceId(), unsigned.type(),
                unsigned.payload(), unsigned.payloadRef(), unsigned.issuer(), unsigned.issued(),
                unsigned.lease(), unsigned.tags(), actor.sign(canonical), unsigned.keyEpoch());
    }

    private boolean verifyRecord(EntryRecord record, byte[] issuerKey) {
        return verifyRecord(record, issuerKey, null);
    }

    /**
     * Verifies a record's signature (TECH-SPEC §7.2, QA4 A4-7). The offered key
     * must always hash to the record's issuing peer. With no certificate the
     * record must verify under that peer key, as it always has. With one, the
     * certificate must verify under the peer key, name exactly this issuer, and
     * be unexpired, and the record must then verify under the certificate's agent
     * key: the agent, not merely its peer, signed this.
     */
    private boolean verifyRecord(EntryRecord record, byte[] issuerKey, AgentCertificate certificate) {
        if (record.sig() == null || issuerKey == null
                || issuerKey.length != Ed25519.RAW_PUBLIC_KEY_LENGTH
                || !PeerId.fromPublicKey(issuerKey).equals(record.issuer().peer())) {
            return false;
        }
        if (certificate == null) {
            return signatures.verify(issuerKey, signableBytes(record), record.sig());
        }
        // SPEC §4.2 v0.1.13: judged at the record's issue stamp, not at receipt,
        // so an agent's history stays mergeable after its certificate lapses. HLC round
        // five, 2026-11-08 (decision E4): by the window alone; mergeState judges how far
        // the stamp leads this replica's wall, as it does for a peer-signed record.
        return certificates.verifyCovering(certificate, issuerKey, record.issuer(),
                Instant.ofEpochMilli(record.issued().physical()))
                && signatures.verify(certificate.agentPublicKey(), signableBytes(record), record.sig());
    }

    /** The signature covers the record's immutable identity; leases change freely. */
    private byte[] signableBytes(EntryRecord record) {
        return codec.toBytes(new SignView(record.entryId(), record.spaceId(), record.type(),
                record.payload(), record.payloadRef(), record.issuer(),
                record.issued(), record.tags(), record.keyEpoch()));
    }

    private record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                            String payloadRef, AgentId issuer, HlcTimestamp issued,
                            Map<String, String> tags,
                            // v0.1.13, omitted when null: epoch-0 and unencrypted records
                            // sign exactly the v0.1.12 bytes (golden sign_view_cbor)
                            @com.fasterxml.jackson.annotation.JsonInclude(
                                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                            Long keyEpoch) {
    }

    /**
     * Signs the entry's current state as {@code actor} (SPEC §11a.4, v0.1.13).
     * A subordinate actor signs with its own key: the DTO names it as signer,
     * stamps the signing time, and carries the certificate covering that time.
     * A peer-signed actor signs with the peer key, exactly as before, and the
     * DTO is byte-identical to the v0.1.12 form.
     */
    private SpaceWire.EntryStateDto signedDto(EntryId entryId, byte[] issuerKey, AgentIdentity actor) {
        SpaceWire.EntryStateDto dto = SpaceWire.EntryStateDto.from(
                crdt.state(entryId).orElseThrow(), issuerKey, issuerCertificates.get(entryId));
        if (actor.isSubordinate()) {
            HlcTimestamp signedAt = hlc.now();
            dto = dto.withAgentSigner(actor.id().encoded(), signedAt,
                    coveringCertificate(actor, signedAt));
            dto = dto.withStateSig(actor.sign(stateSignBytes(dto)));
        } else {
            dto = dto.withStateSig(identity.sign(stateSignBytes(dto)));
        }
        recordSignedState(entryId, dto);
        return dto;
    }

    /**
     * SPEC §11a.4 v0.1.13: only the agent that wrote an agent-attested entry
     * renews or cancels it, so a sibling agent on the same peer cannot act on
     * another agent's entry (its signed state would be refused everywhere).
     */
    private void requireIssuerActs(EntryId entryId, EntryRecord record, AgentIdentity actor) {
        if (issuerCertificates.containsKey(entryId) && !actor.id().equals(record.issuer())) {
            throw new IllegalStateException(actor.id().encoded() + " cannot act on "
                    + record.issuer().encoded() + "'s agent-attested entry " + entryId);
        }
    }

    /**
     * Remembers the most advanced actor-signed state DTO seen for an entry, so
     * anti-entropy can forward it verbatim. Call under {@link #lock}.
     */
    private void recordSignedState(EntryId entryId, SpaceWire.EntryStateDto dto) {
        SpaceWire.EntryStateDto current = signedStates.get(entryId);
        if (current == null || advances(dto, current)) {
            signedStates.put(entryId, dto);
        }
    }

    /** Whether {@code a} represents a strictly more advanced state than {@code b}. */
    private static boolean advances(SpaceWire.EntryStateDto a, SpaceWire.EntryStateDto b) {
        if (a.completed() != b.completed()) {
            return a.completed(); // completion is terminal and dominates
        }
        if (a.removes().size() != b.removes().size()) {
            return a.removes().size() > b.removes().size();
        }
        int stampCmp = a.leaseStamp().compareTo(b.leaseStamp());
        if (stampCmp != 0) {
            return stampCmp > 0; // the newer lease register
        }
        return a.adds().size() >= b.adds().size();
    }

    /**
     * Verifies that a received state DTO's mutable fields were signed by the
     * party authorized for the transition: a completed state by the holder of a
     * valid take claim on the entry, any other state by the record's issuer.
     * This is what stops an admitted member from forging a completion, a
     * removal, or a lease that would make a signed entry vanish.
     */
    private boolean verifyState(SpaceWire.EntryStateDto dto) {
        SpaceWire.SignedClaim claim = null;
        if (dto.completed() && dto.stateSig() != null) {
            synchronized (lock) {
                claim = heldClaim(dto.record().entryId());
            }
        }
        return verifyState(dto, claim, false);
    }

    /**
     * Verifies a state as {@link #verifyState(SpaceWire.EntryStateDto)} does, a completion against the given claim
     * rather than the one held now, so the check can run before the lock with the claim it will meet.
     *
     * @param claim         the claim a completion is judged against; ignored for any other state
     * @param claimVerified whether that claim's own signature was verified already
     */
    private boolean verifyState(SpaceWire.EntryStateDto dto, SpaceWire.SignedClaim claim, boolean claimVerified) {
        if (dto.stateSig() == null) {
            return false;
        }
        // The party authorized for this transition (SPEC §11a.4): the claim
        // holder for a completion, the record's issuer otherwise; with that
        // party's peer key and whether that party is agent-attested.
        byte[] signerKey;
        AgentId party;
        boolean partyAttested;
        if (dto.completed()) {
            EntryId entryId = dto.record().entryId();
            if (claim == null || !(claimVerified || verifyClaim(entryId, claim))) {
                return false; // no authenticated holder to attribute the completion to
            }
            signerKey = claim.holderKey();
            party = claim.claim().holder();
            partyAttested = claim.holderCertificate() != null;
        } else {
            signerKey = dto.issuerPublicKey();
            party = dto.record().issuer();
            partyAttested = dto.agentCertificate() != null;
        }
        if (signerKey == null || signerKey.length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return false;
        }
        if (dto.agentSigned()) {
            // v0.1.13 two-key rule for transitions: the certificate verifies under
            // the party's peer key for exactly that party and covers the signing
            // time, and the state verifies under the certificate's agent key. HLC
            // round six, 2026-11-08 (owner ruling F4): the certificate is judged at
            // signedAt with no drift ceiling, and mergeState judges how far signedAt
            // leads by the far-future rule of decision D4, as it judges the lease stamp.
            return dto.signedAt() != null && dto.stateCertificate() != null
                    && party.encoded().equals(dto.signer())
                    && certificates.verifyCovering(dto.stateCertificate(), signerKey, party,
                            Instant.ofEpochMilli(dto.signedAt().physical()))
                    && signatures.verify(dto.stateCertificate().agentPublicKey(), stateSignBytes(dto), dto.stateSig());
        }
        if (partyAttested) {
            // A6: an agent-attested party's transitions must be agent-signed, so
            // its peer (or a sibling agent on it) cannot act in its name.
            return false;
        }
        return signatures.verify(signerKey, stateSignBytes(dto), dto.stateSig());
    }

    /** The bytes an actor signs to authenticate a state transition. */
    private byte[] stateSignBytes(SpaceWire.EntryStateDto dto) {
        return codec.toBytes(new StateSignView(dto.record().spaceId(),
                dto.record().entryId(), sortedDots(dto.adds()), sortedDots(dto.removes()),
                dto.leaseStamp(), dto.leaseValue(), dto.completed(), dto.signer(), dto.signedAt()));
    }

    private static List<Dot> sortedDots(List<Dot> dots) {
        List<Dot> sorted = new ArrayList<>(dots);
        sorted.sort(java.util.Comparator.comparing(Dot::replica)
                .thenComparingLong(Dot::counter));
        return sorted;
    }

    private record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds,
                                 List<Dot> removes, HlcTimestamp leaseStamp,
                                 LeaseInfo leaseValue, boolean completed,
                                 // v0.1.13, omitted when null so a peer-signed state's
                                 // signed bytes are unchanged (golden state_sign_view_cbor)
                                 @com.fasterxml.jackson.annotation.JsonInclude(
                                         com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                                 String signer,
                                 @com.fasterxml.jackson.annotation.JsonInclude(
                                         com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                                 HlcTimestamp signedAt) {
    }

    private void publishDelta(SpaceWire.Delta delta, String itemId) {
        if (local) {
            return; // a writer-local space tells nobody (Builder.local)
        }
        runtime.gossip().publish(streamId, itemId, codec.toBytes(delta));
    }

    /**
     * Delivers an event to the subscriptions whose template accepts the entry's
     * type, tags and fields (issue #16 §9.2). Every caller holds the record the
     * event is about, so the event carries the {@link EntryView} over it; the
     * attestation is read under the lock, as {@code readAllEntries} reads it.
     * Callers do not hold the lock.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void fire(SpaceEvent.Kind kind, EntryId entryId, Object value, AgentId writer,
                      AgentId actor, EntryRecord record) {
        long local = localMillis();
        subscriptions.removeIf(sub -> sub.expiryMillis <= local || sub.closed);
        if (value == null) {
            return;
        }
        boolean isolated = Boolean.TRUE.equals(DELIVERING_BATCH.get()); // an event a batch deferred (RE-M-08)
        EntryView<Object> details = null;
        for (Sub sub : subscriptions) {
            if (!sub.closed && sub.template.matchesTags(record.tags()) && sub.template.matchesType(record.type())
                    && sub.template.matches(value)) {
                if (details == null) {
                    boolean attested;
                    synchronized (lock) {
                        attested = issuerCertificates.containsKey(entryId);
                    }
                    details = EntryView.of(record, value, attested
                            ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED);
                }
                SpaceEvent event = new SpaceEvent(kind, entryId, value, writer, actor, details);
                if (!isolated) {
                    sub.listener.onEvent(event); // a direct call: the listener's exception reaches the caller
                    continue;
                }
                // A listener's own calls into a space are direct calls again, so they fire as direct calls do.
                DELIVERING_BATCH.remove();
                try {
                    sub.listener.onEvent(event);
                } catch (RuntimeException e) {
                    listenerFailed(sub, kind, entryId, e); // an Error, an assertion's included, is not caught
                } finally {
                    DELIVERING_BATCH.set(Boolean.TRUE);
                }
            }
        }
    }

    /**
     * A listener threw on an event an anti-entropy batch deferred (RE-M-08). Its failure is its own: the entry is merged
     * already and nothing fires its event again, so the other listeners of this event, and the later events of the same
     * batch, still get theirs.
     * Each failure is counted; the first of each subscription is logged with its cause, the rest only counted.
     */
    private void listenerFailed(Sub sub, SpaceEvent.Kind kind, EntryId entryId, Throwable failure) {
        listenerFailures.incrementAndGet();
        if (sub.failureLogged.compareAndSet(false, true)) {
            LOG.log(System.Logger.Level.WARNING, "a listener of space '" + name + "' threw on " + kind + " of "
                    + entryId.value() + "; it keeps its subscription, and later failures of it are only counted",
                    failure);
        }
    }

    /** How many times a listener of this space threw; for tests and status. */
    long listenerFailures() {
        return listenerFailures.get();
    }

    /** The hash a digest's state line names; the map keeps it in each slot, so the two never differ. */
    private static String stateHash(EntryState state) {
        return SpaceStateCrdt.stateHash(state);
    }

    private static String claimHash(TakeClaim claim) {
        return claim.epoch() + ":" + claim.stamp().encoded() + ":" + claim.bid()
                + ":" + claim.expiresAtMillis();
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    /**
     * This replica's local now, for the leases nobody else reads (srad/agentspaces-event-loop.md EL-C-04, §10.18): a
     * subscription lives in this process only, and its renewer is a task on the monotonic base (EL-W-02), so its lease
     * is judged on that base too. On wall time a 2-hour laptop sleep or a forward NTP step past half the lease lapsed
     * every subscription at the next event, the renewal then threw, and the agent stopped reacting for good. A plain
     * clock a test moves by hand keeps its subscriptions on that clock
     * ({@link ai.badmonkey.agentspaces.common.time.TimeSources#of}).
     */
    private long localMillis() {
        return localTime.monoMillis();
    }

    /**
     * Waits out the whole settle window, even when interrupted, and keeps the interrupt for the caller. The race is
     * judged when the window ends; a take whose wait an interrupt cut short judged a claim the others had not had
     * time to answer and could win next to a rival, which is the duplicate the window is there to prevent. A closing
     * binding interrupts its take, so this is the path a take in flight at close goes through. The window is short
     * and bounded, so waiting it out keeps the stop prompt.
     */
    private void settle(Duration window) {
        if (window.isZero()) {
            return;
        }
        // On the space's time source (EL-W-01, decision 12): a virtual one releases the window on an advance.
        long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(waits, window);
        boolean interrupted = false;
        try {
            for (long left = window.toNanos(); left > 0; left = deadline - waits.monoNanos()) {
                try {
                    waits.sleep(Duration.ofNanos(left));
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Tries {@code poll} until it answers or the timeout passes, as a blocking read or take does. Between tries it
     * waits on the space's change signal ({@link #signal()}): a write, a merge, a renewal, a cancel, a claim, a sweep
     * that lapses a claim or collects, a block or an artifact landing. Every waiter wakes on a change and tries
     * again, as every poller used to try every 20 ms, so the order in which waiters win is what it was. A wait also
     * ends when a claim lapses by the clock (the expiry index knows when) and after {@link #lookSliceNanos} at
     * most, for what does not signal.
     */
    private <V> Optional<V> awaiting(Duration timeout, java.util.function.Supplier<Optional<V>> poll) {
        // The deadline and every wait on one base, the space's time source (EL-C-10, EL-W-01).
        long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(waits, timeout);
        while (true) {
            long seen;
            synchronized (lock) {
                seen = signals;
            }
            Optional<V> result = poll.get();
            waitingPolls.incrementAndGet();
            // An interrupt ends the wait after the poll in hand, with the status kept, as on a LocalSpace: a closing
            // binding interrupts its take, and a take that waited on through its timeout could still claim and
            // return an entry written after the binding closed.
            if (result.isPresent() || waits.monoNanos() - deadline >= 0 || Thread.currentThread().isInterrupted()) {
                return result;
            }
            awaitSignal(seen, deadline);
        }
    }

    /**
     * Waits until a change is signalled after {@code seen}, the deadline passes, the next due time of the expiry index
     * comes, or the look's slice ends (the one take-wait rule, {@link #lookSliceNanos}). An interrupt ends the wait and
     * is kept for the caller, which then returns after one more poll ({@link #awaiting}) rather than spinning on the
     * kept status. The deadline and the wait are on one base, the space's time source (EL-C-10); the expiry index's
     * next due is wall time, converted to a span at the moment of the wait.
     */
    private void awaitSignal(long seen, long deadlineNanos) {
        boolean interrupted = Thread.interrupted();
        try {
            synchronized (lock) {
                if (signals != seen) {
                    return;
                }
                long now = waits.monoNanos();
                long until = deadlineNanos - now;
                if (until <= 0) {
                    return;
                }
                until = Math.min(until, lookSliceNanos());
                long untilDueMillis = nextDueLocked() - nowMillis();
                if (untilDueMillis > 0 && untilDueMillis < until / 1_000_000L) {
                    until = untilDueMillis * 1_000_000L; // a claim lapsing by the clock makes its entry takeable unsignalled
                }
                waiters++;
                try {
                    waits.waitOn(lock, now + until);
                } finally {
                    waiters--;
                }
            }
        } catch (InterruptedException e) {
            interrupted = true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A handle on an entry held by another actor than its writer, for the tests of the issuer rule (SPEC §7.2): the
     * API hands a handle only to the entry's writer, so no caller reaches this case any other way.
     */
    EntryHandle handleAs(EntryId entryId, AgentIdentity actor) { // package-private for tests
        return new Handle(entryId, actor);
    }

    private final class Handle implements EntryHandle {
        private final EntryId entryId;
        private final AgentIdentity actor;

        private Handle(EntryId entryId, AgentIdentity actor) {
            this.entryId = entryId;
            this.actor = actor;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            SpaceWire.EntryStateDto dto;
            synchronized (lock) {
                EntryState state = crdt.state(entryId).orElse(null);
                if (state == null || !state.present()
                        || state.lease().value().expired(clock.instant())) {
                    throw new LeaseExpiredException("write lease lapsed for entry " + entryId);
                }
                requireIssuerActs(entryId, state.record(), actor);
                long now = nowMillis();
                long current = state.lease().value().expiresAtMillis();
                noteShortening(entryId, "write", current, SpaceStateCrdt.renewedExpiry(Long.MIN_VALUE, now,
                        extension.toMillis()));
                long expiry = SpaceStateCrdt.renewedExpiry(current, now, extension.toMillis());
                if (expiry == current) {
                    // RE-H-02: the lease already runs at least that long, and a renewal never shortens it, so
                    // nothing changes and nothing goes out.
                    return;
                }
                LeaseInfo renewed = new LeaseInfo(actor.id(), expiry, LeaseKind.WRITE);
                setCrdt(crdt.setLease(entryId, new LwwRegister<>(hlc.now(), renewed)), entryId);
                bump(entryId);
                indexCredential(entryId);
                dto = signedDto(entryId,
                        issuerKeys.getOrDefault(entryId, identity.rawPublicKey()), actor);
            }
            publishDelta(new SpaceWire.Delta(wireForm(entryId, dto), null, null), "r:" + entryId + ":" + nowMillis());
        }

        @Override
        public void cancel() {
            SpaceWire.EntryStateDto dto = null;
            Runnable expired = null;
            synchronized (lock) {
                EntryState state = crdt.state(entryId).orElse(null);
                // Core cancel events, 2026-11-09: only a present entry has anything to withdraw. One completed,
                // withdrawn, or unknown here stays as it is, as on a LocalSpace, and nothing goes out: an issuer's
                // signature over a completed state verifies nowhere (SPEC §11a.4), and it would have replaced the
                // holder's completion as the state this replica forwards.
                if (state != null && state.present()) {
                    requireIssuerActs(entryId, state.record(), actor);
                    setCrdt(crdt.remove(entryId), entryId);
                    bump(entryId);
                    indexCredential(entryId);
                    dto = signedDto(entryId,
                            issuerKeys.getOrDefault(entryId, identity.rawPublicKey()), actor);
                    expired = withdrawnEvent(entryId, true);
                    // IDLE-WORK 5a(e): a withdrawn entry is read by nobody again, so its decoded value goes now, not
                    // at its collection two maximum leases on (the EXPIRED above already carries it out).
                    valueCache.remove(entryId);
                }
            }
            if (dto != null) {
                publishDelta(new SpaceWire.Delta(wireForm(entryId, dto), null, null), "x:" + entryId);
            }
            if (expired != null) {
                expired.run();
            }
        }
    }

    private final class Taken<T> implements TakenEntry<T> {
        private final EntryId entryId;
        private final T value;
        private final long epoch;
        private final AgentIdentity actor;

        private Taken(EntryId entryId, T value, long epoch, AgentIdentity actor) {
            this.entryId = entryId;
            this.value = value;
            this.epoch = epoch;
            this.actor = actor;
        }

        @Override
        public T entry() {
            return value;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            TakeClaim renewed;
            synchronized (lock) {
                TakeClaim current = claimOf(entryId);
                if (current == null || !current.holder().equals(actor.id())
                        || current.epoch() != epoch || current.expired(nowMillis())) {
                    throw new LeaseExpiredException("take lease lapsed for entry " + entryId);
                }
                if (actorRevoked(actor)) {
                    // v0.1.13 (M-1): every receiver refuses the renewal; let the take lapse.
                    throw new LeaseExpiredException("take lease of entry " + entryId + " cannot be renewed: "
                            + actor.id().encoded() + " is revoked in this group");
                }
                long now = nowMillis();
                noteShortening(entryId, "take", current.expiresAtMillis(),
                        SpaceStateCrdt.renewedExpiry(Long.MIN_VALUE, now, extension.toMillis()));
                long expiry = SpaceStateCrdt.renewedExpiry(current.expiresAtMillis(), now, extension.toMillis());
                if (expiry == current.expiresAtMillis()) {
                    return; // RE-H-02, as for the write lease: the hold already runs that long, so nothing changes
                }
                renewed = new TakeClaim(current.entryId(), current.spaceId(),
                        current.epoch(), current.stamp(), actor.id(),
                        current.bid(), expiry);
                mergeHeldClaim(entryId, sign(renewed, actor));
                bump(entryId);
            }
            publishDelta(new SpaceWire.Delta(null, entryId, sign(renewed, actor)),
                    "c:" + entryId + ":renew:" + nowMillis());
        }

        @Override
        public boolean held() {
            synchronized (lock) {
                TakeClaim current = claimOf(entryId);
                return current != null && current.holder().equals(actor.id()) && current.epoch() == epoch
                        && !current.expired(nowMillis());
            }
        }
    }

    private final class Sub implements Subscription {
        private final Template<?> template;
        @SuppressWarnings("rawtypes")
        private final SpaceListener listener;
        private volatile long expiryMillis;
        private volatile boolean closed;
        /** Whether a throw of this listener was logged; later ones are only counted. */
        private final AtomicBoolean failureLogged = new AtomicBoolean();

        private Sub(Template<?> template, SpaceListener<?> listener, long expiryMillis) {
            this.template = template;
            this.listener = listener;
            this.expiryMillis = expiryMillis;
        }

        @Override
        public synchronized void renew(Duration extension) {
            // Synchronized so that two renewals at once cannot both read the old expiry and the shorter one write
            // last: the max rule promises the lease never shortens, and the read and the write must be one step.
            Objects.requireNonNull(extension, "extension");
            long now = localMillis();
            if (closed || expiryMillis <= now) {
                throw new LeaseExpiredException("subscription lease already lapsed");
            }
            // The same reading of renew as an entry's lease: at least the extension from now, never shorter. Close the
            // subscription to stop it sooner.
            expiryMillis = SpaceStateCrdt.renewedExpiry(expiryMillis, now, extension.toMillis());
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
