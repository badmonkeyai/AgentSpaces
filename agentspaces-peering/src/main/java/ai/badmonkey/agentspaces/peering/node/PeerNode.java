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
import ai.badmonkey.agentspaces.api.spi.DueWork;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    /** The outboxes and the one flush timer (srad/agentspaces-event-loop.md §3.4), or null when not coalescing. */
    private final Coalescer coalescer;
    /**
     * The node's one time source (srad/agentspaces-event-loop.md §3.1, §3.6, EL-C-04): wall time ({@code millis()},
     * {@code instant()}) for everything a peer reads or that outlives a restart, and monotonic milliseconds
     * ({@link #localMillis()}) for every local period.
     */
    private final ai.badmonkey.agentspaces.common.time.TimeSource time;
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
    /**
     * The hint a node advertises when it accepts a recordless entry state (a renew, cancel, or
     * complete whose {@code EntryStateDto} omits the record; srad/agentspaces-pdt.md §3.9, PD-L-03,
     * ISSUE-RecordlessRenew). A space sends the form only when every member of its view carries it.
     */
    public static final String RECORDLESS_HINT = "wire.recordless";
    /** Members whose advertisement carries {@link #RECORDLESS_HINT}. */
    private final Set<PeerId> recordlessPeers = ConcurrentHashMap.newKeySet();
    /**
     * The hint a node advertises when it audits spaces by their fingerprinted tries and fetches by entry id (SPEC annex
     * I.6, v0.1.35; srad/replication-by-entry.md RE-T-02): {@code wire.ae=2}, a value in the hints map, never a new
     * card field. Toward a member whose current card carries it, a round opens with each space's trie hash and the
     * DIGEST carries the audit's fields; toward any other the forms of v0.1.17 and before go out unchanged.
     */
    public static final String AE_HINT = "wire.ae";
    /** The value of {@link #AE_HINT} this build implements; a later form would be a new value. */
    public static final String AE_LEVEL = "2";
    /** Members whose advertisement carries {@link #AE_HINT} at {@link #AE_LEVEL}. */
    private final Set<PeerId> aePeers = ConcurrentHashMap.newKeySet();
    /**
     * The hint a node advertises when it collects a withdrawn entry by the entry's own lease (SPEC §7.3, v0.1.35;
     * srad/replication-by-entry.md RE-H-04): {@code wire.horizon=1}, a value in the hints map, never a new card field.
     * A space applies the per-entry horizon only while this node and every member of its view carry it, and only to a
     * withdrawal whose writing node is this one or a hinted member that has been in the view longer than the space's
     * longest lease plus five minutes; a writer out of sight keeps the legacy horizon there.
     *
     * <p>The hint is a promise about every writer behind the node: it renews by the rule of SPEC §7.2 (issuer only,
     * present and unexpired, never shorter), and no identity it renews for renews the same entry from a second node at
     * the same time. Two nodes renewing one identity can each hold a longer lease than the expiry the withdrawal
     * carries, and that lease would come back past the shorter horizon (docs/proofs/per-entry-horizon.md, probe P40).
     * A deployment that runs one identity on two nodes at once sets {@code agentspaces.space.horizon=legacy} on them.
     */
    public static final String HORIZON_HINT = "wire.horizon";
    /** The value of {@link #HORIZON_HINT} this build implements. */
    public static final String HORIZON_LEVEL = "1";
    /** Members whose advertisement carries {@link #HORIZON_HINT} at {@link #HORIZON_LEVEL}. */
    private final Set<PeerId> horizonPeers = ConcurrentHashMap.newKeySet();
    /** Moves whenever a peer enters or leaves {@link #horizonPeers} (idle-loop O5). */
    private final AtomicLong horizonHintsVersion = new AtomicLong();
    /**
     * The hint a node advertises once every decoder it runs reads the v0.1.32 schemas of the AppCard and the
     * AgentCard ({@code wire.appcard=2}; SPEC §6.1a.2 and §9; AppCard origin and formats, 2026-11-09): canonical
     * decimal text, read as level N or above. An issuer writes a card's origin, formats, an {@code idp} host, and a
     * spill reference only while every member of its view is at level 2.
     */
    public static final String APPCARD_HINT = "wire.appcard";
    /** The AppCard level this build reads and, by default, advertises. */
    public static final int APPCARD_LEVEL = 2;
    /** The AppCard level each member's advertisement states, for members above level 1. */
    private final Map<PeerId, Integer> appCardLevels = new ConcurrentHashMap<>();
    /** Moves whenever {@link #appCardLevels} changes a level (EL-T-07). */
    private final AtomicLong appCardVersion = new AtomicLong();
    /** The AppCard level this node advertises; 1 advertises no hint. */
    private final int appCardLevel;
    /** Members admitted since the last tick, per group: each gets a targeted anti-entropy round on the next tick. */
    private final Map<GroupId, Set<PeerId>> newlyAdmitted = new ConcurrentHashMap<>();
    /** When each newly admitted member was admitted, on the node's clock, for the newcomer-round series (EL-M-04). */
    private final Map<GroupPeer, Long> admittedAtMillis = new ConcurrentHashMap<>();

    /** A member of a group, as a map key. */
    private record GroupPeer(GroupId group, PeerId peer) {
    }

    /** The loop's counters (srad/agentspaces-event-loop.md EL-M-02, NL-6). */
    private final LoopMeter loopMeter = new LoopMeter();
    /** Interval refreshes of this node's advertisement (the keepalive once the probe cycle outgrows the TTL). */
    private final AtomicLong selfAdRefreshes = new AtomicLong();
    /** Frames those refreshes sent, one per member reached directly. */
    private final AtomicLong selfAdRefreshFrames = new AtomicLong();
    /** Flooded publishes of this node's advertisement (a changed card, or an unreachable member). */
    private final AtomicLong selfAdFloods = new AtomicLong();
    /** Direct introductions to a member this node had not introduced itself to since the last refresh. */
    private final AtomicLong selfAdIntroductions = new AtomicLong();

    /**
     * What this node's loop did since it started (srad/agentspaces-event-loop.md NL-6, EL-M-02). Phase 0 read the
     * old ticker and flusher with the first of these counters; from phase 1 they read the node's loop and its flush
     * timer, so the series before and after the event loop are the same series.
     *
     * @param runs              loop runs: timer, event and direct ({@link #tick()})
     * @param timerWakes        runs the loop's armed timer started (a deadline or the backstop)
     * @param eventWakes        runs a wake started (a frame, a change signal, a registration)
     * @param directRuns        runs of {@link #tick()}
     * @param backstopRuns      runs that swept every participant because the backstop came due
     * @param backstopFoundWork sweeps that found a participant reporting a deadline earlier than the one the loop held
     *                          with no wake (a missed wake, EL-Q-REL-03), over every participant
     * @param backstopFoundBy   the same, by participant name
     * @param participantThrows runs of a participant that threw, by participant name (NL-4)
     * @param flushRuns         runs of the flush timer, armed only by an enqueue into an idle node (EL-B-01)
     * @param flushesWithFrames flush runs that found at least one frame to send
     * @param slowRuns          loop runs over {@link #SLOW_RUN_NANOS} of real time
     * @param failedRuns        participant runs that threw, over every participant
     * @param overruns          timer fires that found a run pending or running (§3.10)
     * @param latenessSamples   timer-started runs whose lateness is in the three figures below (the most recent 4,096)
     * @param latenessP50Nanos  the median of a timer-started run's start minus the deadline it was armed for
     * @param latenessP99Nanos  the 99th percentile of the same
     * @param latenessMaxNanos  the largest lateness since the node started
     * @param busyNanos         the real time loop runs took, summed
     * @param flushWaitSamples  flushes whose oldest frame's wait is in the two figures below (the most recent 4,096)
     * @param flushWaitP99Nanos the 99th percentile of the oldest frame's wait from its enqueue to its flush (EL-B-04)
     * @param flushWaitMaxNanos the largest such wait since the node started
     * @param generation        the loop's generation, bumped by each watchdog re-initialisation (§3.9)
     * @param backstopOnlyRuns  backstop runs in which no participant was due or asked; timer and event wakes less
     *                          these are the runs that had work ({@code core.idle.wakes}, §5.4)
     * @param dueRuns           per participant, scheduled runs that called it because its deadline came
     * @param askedRuns         per participant, scheduled runs that called it because a wake asked it
     * @param wakeCalls         wakes raised, by who raised them: a participant's name, {@code group:<id>[/<kind>]}
     *                          or {@code all} (the per-participant wake counters, srad/agentspaces-event-loop.md
     *                          §10.21)
     * @param stretch           the stretch factor on local periods now, 1 for none (EL-O-04)
     * @param stretches         steps up of the stretch
     * @param restores          steps down of the stretch
     * @param refreshes         refresh actions run before a stretch applied, or before the first run after a
     *                          re-initialisation that found a wire-visible deadline overdue (EL-O-05, WD-4)
     * @param stretchUnrefreshed wire-visible deadlines whose stretch would have outrun their refresh, held to it
     *                          ({@code core.loop.stretch-unrefreshed}, goal 0)
     * @param poolThreads       the loop pool's threads now, 0 on a source with none ({@code core.loop.pool-threads})
     * @param poolMax           the loop pool's max, 0 on a source with none
     * @param wireVisible       the participants whose cadence a peer judges, which carry a refresh action (EL-O-07,
     *                          EL-F-01)
     */
    public record LoopStats(long runs, long timerWakes, long eventWakes, long directRuns, long backstopRuns,
                            long backstopFoundWork, Map<String, Long> backstopFoundBy,
                            Map<String, Long> participantThrows, long flushRuns, long flushesWithFrames,
                            long slowRuns, long failedRuns, long overruns,
                            long latenessSamples, long latenessP50Nanos, long latenessP99Nanos,
                            long latenessMaxNanos, long busyNanos, long flushWaitSamples, long flushWaitP99Nanos,
                            long flushWaitMaxNanos, long generation, long backstopOnlyRuns,
                            Map<String, Long> dueRuns, Map<String, Long> askedRuns, Map<String, Long> wakeCalls,
                            int stretch, long stretches, long restores, long refreshes, long stretchUnrefreshed,
                            int poolThreads, int poolMax, java.util.List<String> wireVisible) {
    }

    /** A loop run longer than this is counted as slow (srad/agentspaces-event-loop.md EL-X-10). */
    public static final long SLOW_RUN_NANOS = 100_000_000L;

    /**
     * Returns the loop's counters (srad/agentspaces-event-loop.md EL-M-02, NL-6).
     *
     * @return the counters as of now
     */
    public LoopStats loopStats() {
        NodeLoop.Stats loopNow = loop.stats();
        ai.badmonkey.agentspaces.common.time.LoopPool pool = time.loopPool();
        LoopMeter.Figures flush = loopMeter.snapshot();
        long failed = loopNow.participantThrows().values().stream().mapToLong(Long::longValue).sum();
        return new LoopStats(loopNow.runs(), loopNow.timerWakes(), loopNow.eventWakes(), loopNow.directRuns(),
                loopNow.backstopRuns(), loopNow.backstopFoundTotal(), loopNow.backstopFoundWork(),
                loopNow.participantThrows(), flush.flushRuns(), flush.flushesWithFrames(), loopNow.slowRuns(), failed,
                loopNow.overruns(), loopNow.latenessSamples(), loopNow.latenessP50Nanos(), loopNow.latenessP99Nanos(),
                loopNow.latenessMaxNanos(), loopNow.busyNanos(), flush.flushWaitSamples(), flush.flushWaitP99Nanos(),
                flush.flushWaitMaxNanos(), loopNow.generation(), loopNow.backstopOnlyRuns(), loopNow.dueRuns(),
                loopNow.askedRuns(), loopNow.wakeCalls(), loopNow.stretch(), loopNow.stretches(), loopNow.restores(),
                loopNow.refreshes(), loopNow.stretchUnrefreshed(), pool == null ? 0 : pool.threads(),
                pool == null ? 0 : pool.options().max(), loopNow.wireVisible());
    }

    /**
     * The node's loop participants in run order, by name (srad/agentspaces-event-loop.md DW-4), for diagnostics.
     *
     * @return their names
     */
    public List<String> loopParticipants() {
        return loop.participants();
    }

    /** What the node does when its watchdog escalates (decision 4, EL-H-08). */
    public enum Escalation {
        /**
         * (i), the default: an ERROR log, {@link #loopHealth()} DOWN and every {@link #onLoopHealth} listener told
         * (the Spring starter publishes {@code LivenessState.BROKEN}), so the supervisor restarts the process.
         */
        REPORT,
        /**
         * (ii), {@code agentspaces.watchdog.escalation=exit}, for hosts with no liveness probe: as REPORT, then the
         * node closes within 2 s and the process exits with {@link #WATCHDOG_EXIT_STATUS}.
         */
        EXIT;

        /**
         * Reads {@code report} or {@code exit}.
         *
         * @param value the text
         * @return the escalation
         */
        public static Escalation parse(String value) {
            return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "report" -> REPORT;
                case "exit" -> EXIT;
                default -> throw new IllegalArgumentException("agentspaces.watchdog.escalation is report or exit: "
                        + value);
            };
        }
    }

    /** The process's exit status under {@link Escalation#EXIT} (decision 4 ii): distinct for the supervisor. */
    public static final int WATCHDOG_EXIT_STATUS = 70;

    /**
     * The node loop's health (srad/agentspaces-event-loop.md EL-H-14): OK; DEGRADED while a participant is backed off
     * after repeated throws or runs in its own process after repeated slow runs; DOWN once the watchdog escalated.
     *
     * @return the health now, with reasons
     */
    public NodeLoop.Health loopHealth() {
        return loop.health();
    }

    /** The flush timer, or null without coalescing; for the seam and combination tests (§10.33). */
    Coalescer coalescer() {
        return coalescer;
    }

    /** Tests and spoilers only: the node's loop, to script a fault on it (EL-SP-20, EL-SP-21). */
    NodeLoop loop() {
        return loop;
    }

    /**
     * What this node does when its watchdog escalates (agentspaces.watchdog.escalation): report, or exit the process.
     *
     * @return the escalation
     */
    public Escalation watchdogEscalation() {
        return escalation;
    }

    /**
     * Tells a listener of each change of {@link #loopHealth()} (EL-H-14). It runs on the loop's or the watchdog's
     * thread and must be quick.
     *
     * @param listener the listener
     */
    public void onLoopHealth(Consumer<NodeLoop.Health> listener) {
        loopHealthListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * The loop's participants that ran over the slow bound (100 ms of real time), with how often (EL-H-11).
     *
     * @return counts by participant name
     */
    public Map<String, Long> slowParticipants() {
        return loop.stats().slowParticipants();
    }

    /**
     * Inbound frame kinds whose handler ran over the slow bound (100 ms of real time), with how often; keyed
     * {@code KIND} (EL-H-11). Moving a slow handler off the reader thread waits for EL-F-06, because per-link order
     * must hold.
     *
     * @return counts by frame kind
     */
    public Map<String, Long> slowFrameHandlers() {
        Map<String, Long> out = new java.util.TreeMap<>();
        slowFrames.forEach((k, v) -> out.put(k.name(), v.get()));
        return Map.copyOf(out);
    }

    private final List<Consumer<NodeLoop.Health>> loopHealthListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<Envelope.Kind, AtomicLong> slowFrames = new ConcurrentHashMap<>();
    private volatile ai.badmonkey.agentspaces.common.time.Watchdog.Registration flushWatch;
    private final java.util.function.IntConsumer exiter;
    private final Escalation escalation;

    private void loopHealthChanged(NodeLoop.Health health) {
        System.Logger.Level level = health.state() == NodeLoop.HealthState.DOWN ? System.Logger.Level.ERROR
                : health.state() == NodeLoop.HealthState.DEGRADED ? System.Logger.Level.WARNING
                : System.Logger.Level.INFO;
        LOG.log(level, "loop health " + health.state() + (health.reasons().isEmpty() ? "" : ": " + health.reasons()));
        for (Consumer<NodeLoop.Health> listener : loopHealthListeners) {
            try {
                listener.accept(health);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "a loop health listener threw", e);
            }
        }
        if (health.state() == NodeLoop.HealthState.DOWN && escalation == Escalation.EXIT) {
            // Decision 4 (ii): close within 2 s, then exit with a distinct status, off the watchdog's thread.
            Thread.ofVirtual().name("agentspaces-watchdog-exit").start(() -> {
                Thread closer = Thread.ofVirtual().start(this::close);
                try {
                    closer.join(Duration.ofSeconds(2));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    LOG.log(System.Logger.Level.ERROR, "watchdog escalation=exit: exiting with status "
                            + WATCHDOG_EXIT_STATUS);
                    exiter.accept(WATCHDOG_EXIT_STATUS);
                }
            });
        }
    }

    private void frameHandled(Envelope.Kind kind, PeerId from, long tookNanos) {
        long n = slowFrames.computeIfAbsent(kind, k -> new AtomicLong()).incrementAndGet();
        LOG.log(n == 1 ? System.Logger.Level.WARNING : System.Logger.Level.DEBUG, "a " + kind + " frame from "
                + (from == null ? "?" : from.display()) + " took " + tookNanos / 1_000_000 + " ms to handle (slow"
                + " bound " + SLOW_RUN_NANOS / 1_000_000 + " ms, EL-H-11)");
    }

    /**
     * How this node kept its advertisement alive (srad/agentspaces-event-loop.md EL-M-03,
     * {@code core.idle.keepalive-frames}).
     *
     * @param refreshes     interval refreshes
     * @param refreshFrames frames the interval refreshes sent directly, one per member
     * @param floods        flooded publishes (a new or changed card, or a member unreachable directly)
     * @param introductions direct copies to members not yet introduced to since the last refresh
     */
    public record KeepaliveStats(long refreshes, long refreshFrames, long floods, long introductions) {
    }

    /**
     * Returns the advertisement keepalive's counters.
     *
     * @return the counters as of now
     */
    public KeepaliveStats keepaliveStats() {
        return new KeepaliveStats(selfAdRefreshes.get(), selfAdRefreshFrames.get(), selfAdFloods.get(),
                selfAdIntroductions.get());
    }

    /**
     * The time from each recent admission to this node's targeted anti-entropy round with the newcomer, on the node's
     * clock, oldest first (srad/agentspaces-event-loop.md EL-M-04, {@code core.newcomer.targeted-round}).
     *
     * @return the most recent 256 figures, in milliseconds
     */
    public List<Long> newcomerRoundMillis() {
        return loopMeter.newcomerRounds();
    }
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
    /**
     * How long after a member's loss this node keeps introducing itself to its last endpoints when retirement is off;
     * it then forgets the member, as before retirement existed.
     */
    static final long LOST_RETRY_WINDOW_MILLIS = 10 * 60_000L;
    /**
     * The longest delay between two introductions to one lost member: {@link #LOST_RETRY_EARLY_CAP_MILLIS} for its
     * first {@link #LOST_RETRY_EARLY_PHASE_MILLIS} lost, so a fleet rejoins within about a minute of a partition, a
     * sleep or a blip healing; this after, for a member that may be gone for good (2026-11-19: a five-minute cap from
     * the start made a twelve-minute partition take minutes to heal).
     */
    static final long LOST_RETRY_CAP_MILLIS = 5 * 60_000L;
    static final long LOST_RETRY_EARLY_CAP_MILLIS = 60_000L;
    static final long LOST_RETRY_EARLY_PHASE_MILLIS = 60 * 60_000L;
    /** At most this many lost members are re-dialed per round, the oldest-due first: no dial storm after a split. */
    static final int LOST_RETRY_BUDGET = 8;
    /** With retirement on, a lost member that never answered retires this long after its loss (the default). */
    static final long RETIRE_AFTER_MILLIS = 24L * 60 * 60_000L;
    /** An empty view this long, the seeds and the locator having found nobody, before the last resort is used. */
    static final long LAST_RESORT_AFTER_MILLIS = 2 * 60_000L;
    /** Retired members tried per last-resort round. */
    static final int LAST_RESORT_BUDGET = 4;
    static final long LAST_RESORT_BASE_MILLIS = 2 * 60_000L;
    static final long LAST_RESORT_MAX_MILLIS = 10 * 60_000L;
    /** Each lost member's retry schedule, per group. */
    private final Map<GroupId, Map<PeerId, Backoff>> lostSchedule = new ConcurrentHashMap<>();
    /** When each group's view became empty, while it is. */
    private final Map<GroupId, Long> emptySince = new ConcurrentHashMap<>();
    private final Map<GroupId, Backoff> lastResortSchedule = new ConcurrentHashMap<>();
    private final Map<GroupId, Integer> lastResortCursor = new ConcurrentHashMap<>();
    private final AtomicLong lostIntroductions = new AtomicLong();
    private final AtomicLong lastResortIntroductions = new AtomicLong();
    private final AtomicLong retirements = new AtomicLong();
    private final boolean retirement;
    private final boolean postRetirement;
    private final long retireAfterMillis;
    private final RetirementList retirementList;
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
    /** How many streams a probe carries per-stream hashes for, in every group ({@code agentspaces.gossip.probe-hash-streams}). */
    private final int probeHashStreams;
    /** Whether this node advertises {@link #AE_HINT} and audits toward members that do. */
    private final boolean audit;
    /** Audit sessions this node keeps open per asker. */
    private final int auditSessions;
    /** The budget this node puts on its audit requests, in bytes. */
    private final long auditBudget;
    /** Whether a space on this node logs its first shortening renewal ({@code agentspaces.space.renew-shortening-log}). */
    private final boolean renewShorteningLog;
    /** Whether this node advertises {@link #HORIZON_HINT} ({@code agentspaces.space.horizon=per-entry}). */
    private final boolean horizonPerEntry;
    /**
     * Whether every probe and acknowledgement carries the hashes, whatever this node's own pace. SPEC §5.2 makes them
     * a MAY, and an older node or another implementation attaches them always; tests stand such a member in with
     * this, so a mixed group can be checked (RE-P-03).
     */
    private volatile boolean probeHashesAlways;
    private final Map<GroupId, Long> lastSelfAdMillis = new ConcurrentHashMap<>();
    private final Map<GroupId, Set<PeerId>> selfAdSentTo = new ConcurrentHashMap<>();
    /** The content the last flooded advertisement carried per group; a change floods at once. */
    private final Map<GroupId, Integer> lastSelfAdFingerprint = new ConcurrentHashMap<>();
    /** The membership view at the end of the last tick, per group, to notice drops between ticks. */
    private final Map<GroupId, Set<PeerId>> lastView = new ConcurrentHashMap<>();
    /** The view version {@link #lastView} was taken at, per group (idle-loop O8). */
    private final Map<GroupId, Long> lastViewVersion = new ConcurrentHashMap<>();
    /** When each member's last PING carrying digest hashes arrived, per group (idle-loop O6); members in view only. */
    private final Map<GroupId, Map<PeerId, Long>> hashedPingFrom = new ConcurrentHashMap<>();
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
     * Connections cut because of the chain they presented (a revoked leaf, or a leaf the CA
     * revoked). A transport hands frames it read before the cut to the node after the cut, so
     * onFrame drops whatever such a connection still delivers. Weak, so a cut connection drops
     * away once its last queued frame is gone.
     */
    private final Set<TransportConnection> chainCut = java.util.Collections.synchronizedSet(
            java.util.Collections.newSetFromMap(new java.util.WeakHashMap<>()));
    /** Whether any link was ever cut for its chain: spares every other frame the set's lock. */
    private volatile boolean anyChainCut;
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

    /** Kinds a group's relay counters keep apart before the rest count as {@code other} (LB-R-09). */
    static final int RELAY_KINDS_CAPACITY = 64;
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

    // ---- relay paths (srad/large-networks-bulk-data.md §3.7, group R) ----

    /** Per group, per member, the relays it declares attachment to (hint {@code relay.via}). */
    private final Map<GroupId, Map<PeerId, Set<PeerId>>> relayVia = new ConcurrentHashMap<>();
    /** Per group, per relay, the relays it declares links to (hint {@code relay.links}). */
    private final Map<GroupId, Map<PeerId, Set<PeerId>>> relayLinks = new ConcurrentHashMap<>();
    /** Per group, per target, the pinned relay path (LB-R-05). */
    private final Map<GroupId, Map<PeerId, PinnedPath>> pinnedPaths = new ConcurrentHashMap<>();
    /** Relays a mesh dial is in flight to, and when each was last attempted (LB-R-02). */
    private final Set<PeerId> meshInFlight = ConcurrentHashMap.newKeySet();
    private final Map<PeerId, Long> meshAttempted = new ConcurrentHashMap<>();
    /** Per origin and target, when the last unreachable signal went out (LB-R-07). */
    private final Map<String, Long> signalSentAt = new ConcurrentHashMap<>();
    /** Relays whose dial as a first hop failed, and when: excluded from path starts for a minute. */
    private final Map<PeerId, Long> firstHopFailedAt = new ConcurrentHashMap<>();
    private final AtomicLong unreachableSignalsReceived = new AtomicLong();
    private final int maxPathHops;
    private final Duration pathTtl;
    private final boolean unreachableSignal;

    private record PinnedPath(List<PeerId> path, long lastUsedMillis) {
    }

    /** The capability type of the unreachable signal a relay sends toward an origin (LB-R-07). */
    public static final String RELAY_SIGNAL_CAPABILITY = "aspace:cap/relay-signal";

    /** Interval between mesh dial attempts to one relay. */
    static final long MESH_RETRY_MILLIS = 60_000;

    /** Interval between unreachable signals for one origin and target. */
    static final long SIGNAL_INTERVAL_MILLIS = 10_000;

    /** The body of a {@code PIPE_DATA} frame, the shape the capability runtime uses. */
    private record RelayPipeFrame(String capability, byte[] payload) {
    }

    /**
     * What a relay tells an origin when it dropped a path frame: the target the
     * frame was for and the hop that could not be reached. No endpoint travels.
     *
     * @param target    the unreachable target
     * @param failedHop the relay or target the dropping relay could not reach
     * @param reason    a short word for the counters
     */
    public record RelaySignal(PeerId target, PeerId failedHop, String reason) {
    }

    private static final class RelayCounters {
        final AtomicLong framesForwarded = new AtomicLong();
        final AtomicLong bytesForwarded = new AtomicLong();
        final AtomicLong droppedNotMember = new AtomicLong();
        final AtomicLong droppedByPolicy = new AtomicLong();
        final AtomicLong forwardedToRelay = new AtomicLong();
        final AtomicLong droppedUnreachable = new AtomicLong();
        final AtomicLong droppedBadPath = new AtomicLong();
        final AtomicLong offloadAttempts = new AtomicLong();
        final AtomicLong offloadOpened = new AtomicLong();
        final AtomicLong offloadFailed = new AtomicLong();
        /**
         * LB-R-09: bytes forwarded per inner frame kind, a pipe frame keyed by its capability
         * ({@code PIPE_DATA:aspace:cap/content-swarm}), so an operator sees how much of a relay's
         * traffic is pieces; bounded at {@link #RELAY_KINDS_CAPACITY} keys, the rest under {@code other}.
         */
        final Map<String, AtomicLong> bytesByKind = new ConcurrentHashMap<>();
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

        void countKind(String kind, long bytes) {
            String key = bytesByKind.containsKey(kind) || bytesByKind.size() < RELAY_KINDS_CAPACITY ? kind : "other";
            bytesByKind.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(bytes);
        }

        RelayCounters target(PeerId target) {
            return targets.computeIfAbsent(target, t -> new RelayCounters(false));
        }

        RelayStats snapshot() {
            return new RelayStats(framesForwarded.get(), bytesForwarded.get(),
                    droppedNotMember.get(), droppedByPolicy.get(),
                    forwardedToRelay.get(), droppedUnreachable.get(), droppedBadPath.get());
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
    /**
     * The beacon's next retry on the local base (EL-O-11), or {@link Long#MIN_VALUE} while none is armed: no group
     * to announce yet. The beacon participant alone writes it.
     */
    private volatile long nextBeaconMillis = Long.MIN_VALUE;
    /** A group was joined since the beacon last ran: announce at once, from position 0 (EL-O-11). */
    private final java.util.concurrent.atomic.AtomicBoolean beaconRestart =
            new java.util.concurrent.atomic.AtomicBoolean();
    /** Frames the wire codec refused (bad signature, unknown version, garbage). */
    private final AtomicLong framesDropped = new AtomicLong();
    /** Join-by-GroupID fetches awaiting a verified founding advertisement. */
    /** The first wait of a join by id before it asks again (ISSUE-BootstrapRobustness FR-4). */
    static final long JOIN_SLICE_FIRST_MILLIS = 1_000;
    /** The longest wait between two asks of a join by id; the slices double up to it. */
    static final long JOIN_SLICE_MAX_MILLIS = 5_000;

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
     * every refresh that changes them (plan EFG §4, token ingestion). */
    private final List<BiConsumer<PeerId, Map<String, String>>> credentialHintListeners =
            new CopyOnWriteArrayList<>();
    /** Bound on remembered hint maps; least-recently-refreshed evict. */
    private static final int HINTS_CAPACITY = 4_096;
    /** The hints each member's newest admitted self-advertisement carries, kept with or without a listener, so an
     * unchanged refresh is not re-delivered and a listener registered later hears them (ISSUE-MemberHintSnapshot FR-2). */
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

    /**
     * How far past the far-future ceiling, this node's wall time plus
     * {@link HybridLogicalClock#MAX_DRIFT_MILLIS}, a frame's stamp may lie and still strike
     * nobody (HLC canonical stamps, 2026-11-08, decision B). A hostile stamp can drag an honest
     * peer's clock to its own ceiling, and a receiver whose wall runs behind that peer's then
     * sees every frame the peer sends past the receiver's ceiling. Strikes never decay, so
     * without the margin such a receiver quarantined the honest peer. Five seconds covers the
     * wall-clock skew of hosts that keep time.
     */
    public static final long FAR_FUTURE_SKEW_MARGIN_MILLIS = 5_000L;
    /** How long a quarantined peer's frames are refused before local evidence
     * expires. Long enough for the enterprise revocation path to act, short
     * enough that a misjudged honest peer recovers on its own. */
    static final Duration QUARANTINE = Duration.ofMinutes(10);
    /** The default time to live of this node's self-advertisement (srad/agentspaces-event-loop.md §3.11). */
    public static final Duration DEFAULT_PEER_AD_TTL = Duration.ofMinutes(10);
    /** The default time to live of an unused pinned relay path (§3.11). */
    public static final Duration DEFAULT_PATH_TTL = Duration.ofMinutes(10);
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
    private final AtomicLong framesLogicalRefused = new AtomicLong();
    private final AtomicLong framesFarFuture = new AtomicLong();
    private final AtomicLong framesFarFutureSkewed = new AtomicLong();
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
                             long droppedByPolicy, long forwardedToRelay, long droppedUnreachable,
                             long droppedBadPath) {

        /** The 0.4.0 form, with no path counters. */
        public RelayStats(long framesForwarded, long bytesForwarded, long droppedNotMember, long droppedByPolicy) {
            this(framesForwarded, bytesForwarded, droppedNotMember, droppedByPolicy, 0, 0, 0);
        }
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
     * The bytes this node forwarded as a relay in a group, per inner frame kind (LB-R-09): a pipe
     * frame under {@code PIPE_DATA:<capability>}, so the content swarm's pieces show as
     * {@code PIPE_DATA:aspace:cap/content-swarm}; empty for a group this node never relayed in.
     *
     * @param groupId the group
     * @return kind to bytes, a snapshot
     */
    public Map<String, Long> relayBytesByKind(GroupId groupId) {
        RelayCounters counters = relayCounters.get(Objects.requireNonNull(groupId, "groupId"));
        if (counters == null) {
            return Map.of();
        }
        Map<String, Long> out = new java.util.TreeMap<>();
        counters.bytesByKind.forEach((kind, bytes) -> out.put(kind, bytes.get()));
        return out;
    }

    /**
     * The kind of a relayed frame, read without verifying it (the target verifies): the envelope's
     * kind, and for a pipe frame its capability. A frame that does not parse is {@code unknown}.
     */
    private String relayedKind(byte[] frame) {
        try {
            ai.badmonkey.agentspaces.peering.wire.WireCodec.Signed signed =
                    codec.fromWire(frame, ai.badmonkey.agentspaces.peering.wire.WireCodec.Signed.class);
            if (signed == null || signed.envelope() == null || signed.envelope().kind() == null) {
                return "unknown";
            }
            Envelope.Kind kind = signed.envelope().kind();
            if (kind == Envelope.Kind.PIPE_DATA && signed.envelope().body() != null) {
                try {
                    RelayPipeFrame pipe = codec.fromWire(signed.envelope().body(), RelayPipeFrame.class);
                    if (pipe != null && pipe.capability() != null) {
                        return kind.name() + ":" + pipe.capability();
                    }
                } catch (RuntimeException e) {
                    // a pipe body that is not a capability frame counts as plain PIPE_DATA
                }
            }
            return kind.name();
        } catch (RuntimeException e) {
            return "unknown";
        }
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
        return times == null ? 0 : windowCount(times, localMillis());
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
        long now = localMillis();
        int count;
        synchronized (times) {
            windowCount(times, now);
            times.addLast(now);
            count = times.size();
        }
        if (count == relayOffload.framesPerMinute() + 1) {
            GroupParticipants gp = participants.get(groupId);
            if (gp != null) {
                gp.offload.wake(); // the frame that takes the member over the threshold (EL-L-03)
            }
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
    /** The node's loop (srad/agentspaces-event-loop.md §3.3): one sequential process that runs due work. */
    private final NodeLoop loop;
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
        this.time = builder.resolveTime();
        this.random = builder.random;
        this.codec = CborCodec.defaultCodec();
        this.wire = new WireCodec(codec);
        this.hlc = new HybridLogicalClock(time, builder.identity.peerId().value());
        this.peerAdTtl = builder.peerAdTtl;
        this.peerAdInterval = builder.peerAdInterval.compareTo(builder.peerAdTtl.dividedBy(2)) > 0
                ? builder.peerAdTtl.dividedBy(2) : builder.peerAdInterval;
        this.coalesceWindow = builder.coalesceWindow;
        this.lazyPush = builder.lazyPush;
        this.probeHashStreams = builder.probeHashStreams;
        this.audit = builder.audit;
        this.auditSessions = builder.auditSessions;
        this.auditBudget = builder.auditBudget;
        this.renewShorteningLog = builder.renewShorteningLog;
        this.horizonPerEntry = builder.horizonPerEntry;
        if (lazyPush) {
            nodeHints.put(TREE_HINT, "1");
        }
        if (builder.channelAuth == ChannelAuth.SESSION) {
            nodeHints.put(SESSION_HINT, "1");
        }
        nodeHints.put(HASHES_HINT, "1");
        if (builder.recordless) {
            nodeHints.put(RECORDLESS_HINT, "1");
        }
        if (builder.audit) {
            nodeHints.put(AE_HINT, AE_LEVEL);
        }
        if (builder.horizonPerEntry) {
            nodeHints.put(HORIZON_HINT, HORIZON_LEVEL);
        }
        this.appCardLevel = builder.appCardLevel;
        if (appCardLevel > 1) {
            nodeHints.put(APPCARD_HINT, Integer.toString(appCardLevel));
        }
        this.loop = new NodeLoop("node:" + builder.identity.peerId().display(), time, NodeLoop.DEFAULT_BACKSTOP);
        this.escalation = builder.escalation;
        this.exiter = builder.exiter;
        loop.onHealth(this::loopHealthChanged);
        if (coalesceWindow != null) {
            nodeHints.put(BATCH_HINT, "1");
            // One flush timer, armed by the first enqueue into an idle node (EL-B-01), in place of the fixed-rate
            // peer-flush thread (EL-B-06).
            coalescer = new Coalescer("flush:" + builder.identity.peerId().display(), time, coalesceWindow,
                    new Coalescer.Sink() {
                @Override
                public void single(GroupId group, PeerId to, Envelope.Kind kind, byte[] body) {
                    sendNow(group, to, kind, body);
                }

                @Override
                public void batch(GroupId group, PeerId to, List<byte[]> frames) {
                    sendNow(group, to, Envelope.Kind.BATCH, new Bodies.Batch(frames));
                }

                @Override
                public void taking(GroupId group, PeerId to) {
                    GroupRuntime runtime = groups.get(group);
                    if (runtime != null) {
                        runtime.gossip().outboxTaking(to); // what this flush carries, for the piggyback (EL-T-15)
                    }
                }
            }, loopMeter);
        } else {
            coalescer = null;
        }
        if (coalescer != null) {
            Coalescer c = coalescer;
            // Refresh before slowdown (EL-O-05): what the refresh actions sent leaves the outboxes before a stretch.
            loop.afterRefresh(c::flushAllNow);
        }
        this.channelAuth = builder.channelAuth;
        this.revocationValidator = builder.revocationValidator;
        this.credentialRevocationValidator = builder.credentialRevocationValidator;
        this.channelTrust = builder.channelTrust;
        this.requireAttestation = builder.requireAttestation;
        this.rateCapacity = builder.rateCapacity;
        this.rateRefillPerSecond = builder.rateRefillPerSecond;
        // EL-C-07: the bucket refills on the node's monotonic base; a node on a plain test clock keeps the system's,
        // since its frozen clock would never refill it.
        this.nanos = builder.nanos != null ? builder.nanos
                : ai.badmonkey.agentspaces.common.time.TimeSources.followsWall(time)
                        ? ai.badmonkey.agentspaces.common.time.TimeSource.system()::monoNanos
                : time::monoNanos;
        this.bootstrapBucket = new TokenBucket(TokenBucket.DEFAULT_CAPACITY,
                TokenBucket.DEFAULT_REFILL_PER_SEC, nanos);
        this.roles = Set.copyOf(builder.roles);
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
                : new MulticastBeacon(carrier, this::beaconAnnouncements, this::onBeaconDatagram,
                        builder.beaconInterval);
        this.credentialHintListeners.addAll(builder.credentialHintListeners);
        this.admissionMemory = new AdmissionMemory(builder.stateDirectory, builder.admissionMemoryBound, time);
        this.retirement = builder.retirement;
        this.postRetirement = builder.postRetirement;
        this.retireAfterMillis = builder.retireAfter.toMillis();
        this.retirementList = new RetirementList(builder.stateDirectory, RetirementList.DEFAULT_BOUND);
        this.relayPolicy = builder.relayPolicy;
        this.relayOffload = builder.relayOffload;
        this.maxPathHops = builder.maxPathHops;
        this.pathTtl = builder.pathTtl;
        this.unreachableSignal = builder.unreachableSignal;
        this.locateMinMillis = builder.locateMinMillis;
        this.locateMaxMillis = builder.locateMaxMillis;
        // A group's state turning dirty asks its persistence participant (EL-L-03).
        admissionMemory.onDirty(this::persistDue);
        retirementList.onDirty(this::persistDue);
        registerNodeParticipants();
        // EL-C-12, EL-C-14: the time source tells this node of a wall step (TS-6); the node asks every participant
        // again, and after a forward step longer than a group's member TTL re-introduces itself there.
        this.wallStepRegistration = time.onWallStep(this::onWallStep);
    }

    // ---- the wall-step signal and the resume probe (srad/agentspaces-event-loop.md §3.6, EL-C-12, EL-C-14)

    private final AutoCloseable wallStepRegistration;
    private final AtomicLong wallStepsSeen = new AtomicLong();
    private final AtomicLong resumeRejoins = new AtomicLong();
    /** The monotonic instant of the last wall step not yet followed by a run of the wall-step marker, or -1. */
    private volatile long wallStepPendingNanos = -1;
    private volatile long lastWallStepRerunNanos = -1;
    private DueWork.Registration wallStepMarker;

    /**
     * What this node saw of wall steps (TS-6, EL-C-12, EL-C-14).
     *
     * @param steps              wall steps the time source raised to this node
     * @param resumeRejoins      groups re-introduced to after a forward step longer than their member TTL (EL-C-14)
     * @param lastRerunNanos     monotonic time from the last step to the end of the first run of the loop that asked
     *                           every participant again ({@code core.wallstep.rerun}), or -1 before the first
     */
    public record WallStepStats(long steps, long resumeRejoins, long lastRerunNanos) {
    }

    /**
     * What this node saw of wall steps.
     *
     * @return the counters
     */
    public WallStepStats wallStepStats() {
        return new WallStepStats(wallStepsSeen.get(), resumeRejoins.get(), lastWallStepRerunNanos);
    }

    /**
     * A wall step (TS-6): every participant is asked again at once, so a wall-anchored deadline (a space's lapse, a
     * lease renewal, a card, credential or retirement expiry, the card refresh) is converted again on the stepped
     * clock instead of waiting out the deadline converted before it (EL-C-12). A forward step longer than a group's
     * member TTL may have been an absence (a suspend, a long freeze), and its peers may have dropped this node, so the
     * node re-introduces itself to the group's seeds and members and probes each member once (EL-C-14). Local periods
     * are on monotonic time and do not move (EL-C-05).
     */
    private void onWallStep(Duration step) {
        if (loop.closed()) {
            return;
        }
        wallStepsSeen.incrementAndGet();
        wallStepPendingNanos = time.monoNanos();
        if (!step.isNegative()) {
            long stepMillis = step.toMillis();
            for (GroupRuntime runtime : groups.values()) {
                if (stepMillis > runtime.membership().memberTtlMillis()) {
                    resumeProbe(runtime);
                }
            }
        }
        loop.wakeAll();
    }

    /** EL-C-14: back from a possible absence, introduce this node again and probe every member once. */
    private void resumeProbe(GroupRuntime runtime) {
        GroupId groupId = runtime.id();
        resumeRejoins.incrementAndGet();
        Set<PeerId> sentTo = selfAdSentTo.get(groupId);
        if (sentTo != null) {
            sentTo.clear(); // the self ad goes to every member directly in the next run
        }
        GroupParticipants gp = participants.get(groupId);
        if (gp != null) {
            gp.introductionsOwed = true;
        }
        List<PeerAdvertisement.Endpoint> seeds = groupSeeds.get(groupId);
        if (seeds != null && !seeds.isEmpty()) {
            introduceTo(groupId, seeds); // as a lost view's retry does, without waiting for its back-off
        }
        runtime.membership().probeEachOnce(); // after the self ads: the self-ad participant runs first
    }

    private void persistDue(GroupId groupId) {
        GroupParticipants gp = participants.get(groupId);
        if (gp != null) {
            gp.persist.wake();
        }
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
        /** The wall clock a caller passed with {@link #clock}, or null. */
        private InstantSource clock;
        private Random random = new Random();
        private Duration peerAdTtl = DEFAULT_PEER_AD_TTL;
        private Duration peerAdInterval = Duration.ofSeconds(30);
        private boolean lazyPush = true;
        private int probeHashStreams = GossipBus.PROBE_DIGEST_STREAMS;
        private boolean audit = true;
        private int auditSessions = 4;
        private long auditBudget = 1L << 20;
        private boolean renewShorteningLog = true;
        private boolean horizonPerEntry = true;
        private boolean recordless = true;
        private int appCardLevel = APPCARD_LEVEL;
        private Duration coalesceWindow;
        /** The time source a caller passed with {@link #time}, or null. */
        private ai.badmonkey.agentspaces.common.time.TimeSource time;
        private Escalation escalation = Escalation.parse(System.getProperty("agentspaces.watchdog.escalation",
                "report"));
        private java.util.function.IntConsumer exiter = System::exit;
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
        /** The token bucket's source, or null for the node's monotonic base (EL-C-07). */
        private LongSupplier nanos;
        private java.net.InetSocketAddress multicastGroup;
        private MulticastBeacon.Datagrams beaconCarrier;
        private Duration beaconInterval = MulticastBeacon.FIRST_RETRY;
        private final List<BiConsumer<PeerId, Map<String, String>>> credentialHintListeners =
                new ArrayList<>();
        private java.nio.file.Path stateDirectory;
        private int admissionMemoryBound = AdmissionMemory.DEFAULT_BOUND;
        private boolean retirement = true;
        private boolean postRetirement;
        private Duration retireAfter = Duration.ofMillis(RETIRE_AFTER_MILLIS);
        private RelayPolicy relayPolicy = RelayPolicy.ALLOW;
        private RelayOffload relayOffload = RelayOffload.defaults();
        private int maxPathHops = 4;
        private Duration pathTtl = DEFAULT_PATH_TTL;
        private boolean unreachableSignal = true;

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
         * Retirement (default on): a lost member is retried on a back-off of its own (5 s doubling to 5 min) and,
         * unanswered after {@link #retireAfter} (24 h), retires to the retirement list, persisted beside the
         * admission memory. Off: a lost member is retried for ten minutes and forgotten, as before.
         *
         * @param on whether lost members retire
         * @return this builder
         */
        public Builder retirement(boolean on) {
            this.retirement = on;
            return this;
        }

        /**
         * How long a lost member is retried before it retires (default 24 h), with retirement on.
         *
         * @param after the time after the loss
         * @return this builder
         */
        public Builder retireAfter(Duration after) {
            if (after == null || after.isNegative() || after.isZero()) {
                throw new IllegalArgumentException("retireAfter must be positive: " + after);
            }
            this.retireAfter = after;
            return this;
        }

        /**
         * Post-retirement (default off): when a group's view has been empty for a while and its lost members, its
         * seeds and its locator brought nobody back, introduce this node to retired members' last endpoints as a last
         * resort before it is out of service in the group. Off by default because a retired member's address may
         * by now belong to another host, which learns of this node from the card it is sent.
         *
         * @param on whether the retirement list is consulted as a last resort
         * @return this builder
         */
        public Builder postRetirement(boolean on) {
            this.postRetirement = on;
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

        /** The longest relay path this node computes or forwards (LB-R-03, decision 10); default 4. */
        public Builder maxPathHops(int hops) {
            if (hops < 1) {
                throw new IllegalArgumentException("max-path-hops must be positive: " + hops);
            }
            this.maxPathHops = hops;
            return this;
        }

        /** Idle time after which a pinned relay path is recomputed (LB-R-05); default 10 minutes. */
        public Builder pathTtl(Duration ttl) {
            this.pathTtl = Objects.requireNonNull(ttl, "ttl");
            return this;
        }

        /** Whether a relay that drops a path frame signals the origin (LB-R-07, decision 11); default on. */
        public Builder unreachableSignal(boolean enabled) {
            this.unreachableSignal = enabled;
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
         * Registers a listener for members' credential hints (plan EFG §4):
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
         * Injects the node's clock. A {@link ai.badmonkey.agentspaces.common.time.TimeSource} is the node's one time
         * source, as {@link #time} sets it; the system clock is {@link
         * ai.badmonkey.agentspaces.common.time.TimeSource#system()}; any other plain clock (a test's) is read for wall
         * time and, as before phase 3, for the node's local periods too, so a hand-driven test that moves it moves
         * them ({@link ai.badmonkey.agentspaces.common.time.TimeSources#of}).
         *
         * @param clock the clock
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * The node's one time source (srad/agentspaces-event-loop.md EL-C-06): {@link #time} when set, with a different
         * {@link #clock} as its wall time; else the clock's time source; else the system's.
         */
        ai.badmonkey.agentspaces.common.time.TimeSource resolveTime() {
            if (time == null) {
                return clock == null ? ai.badmonkey.agentspaces.common.time.TimeSource.system()
                        : ai.badmonkey.agentspaces.common.time.TimeSources.of(clock);
            }
            if (clock == null || clock == time) {
                return time;
            }
            return ai.badmonkey.agentspaces.common.time.TimeSources.withWall(clock, time);
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
         * Coalesces rumors, digests, pull responses and pipe frames to one destination into one
         * signed BATCH frame per window (PERF2 §3.3): one signature and one verification where
         * there were many. Only toward peers that advertise {@code wire.batch=1}; other peers get
         * single frames. Off by default (hand-driven tests expect immediate delivery); the starter
         * turns it on with {@code agentspaces.coalesce-millis}. Adds up to one window of
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
         * The node's one time source (srad/agentspaces-event-loop.md §3.1, EL-C-06): wall time for what peers read and
         * what outlives a restart, monotonic time for every local period, scheduling for the loop and the flush timer,
         * and the wall-step signal. Tests pass a {@code VirtualTime}; {@link #clock(InstantSource)} need not be set as
         * well. Default: the clock's, else {@link ai.badmonkey.agentspaces.common.time.TimeSource#system()}.
         *
         * @param time the time source
         * @return this builder
         */
        public Builder time(ai.badmonkey.agentspaces.common.time.TimeSource time) {
            this.time = Objects.requireNonNull(time, "time");
            return this;
        }

        /**
         * What the node does when the watchdog escalates its loop (srad/agentspaces-event-loop.md decision 4,
         * EL-H-08): {@link Escalation#REPORT} (the default; the system property
         * {@code agentspaces.watchdog.escalation} sets the default) or {@link Escalation#EXIT} for a host with no
         * liveness probe.
         *
         * @param escalation the escalation
         * @return this builder
         */
        public Builder watchdogEscalation(Escalation escalation) {
            this.escalation = Objects.requireNonNull(escalation, "escalation");
            return this;
        }

        /** Tests only: what {@link Escalation#EXIT} calls in place of {@link System#exit(int)}. */
        Builder exiter(java.util.function.IntConsumer exiter) {
            this.exiter = Objects.requireNonNull(exiter, "exiter");
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

        /**
         * How many streams a probe carries per-stream hashes for (RE-T-07 of srad/replication-by-entry.md,
         * {@code agentspaces.gossip.probe-hash-streams}); a node holding more streams sends the root alone. The
         * default, {@link GossipBus#PROBE_DIGEST_STREAMS}, sits above the twenty streams a hub member holding many spaces carries. Nothing new goes on the
         * wire: the hashes map is the one every probe could carry since v0.1.17.
         *
         * @param streams the bound, from {@link GossipBus#PROBE_DIGEST_STREAMS_FLOOR} to
         *                {@link GossipBus#PROBE_DIGEST_STREAMS_CEILING}
         * @return this builder
         * @throws IllegalArgumentException outside those bounds
         */
        public Builder probeHashStreams(int streams) {
            if (streams < GossipBus.PROBE_DIGEST_STREAMS_FLOOR || streams > GossipBus.PROBE_DIGEST_STREAMS_CEILING) {
                throw new IllegalArgumentException("probe-hash-streams must be from "
                        + GossipBus.PROBE_DIGEST_STREAMS_FLOOR + " to " + GossipBus.PROBE_DIGEST_STREAMS_CEILING
                        + ", not " + streams);
            }
            this.probeHashStreams = streams;
            return this;
        }

        /**
         * Whether this node audits spaces by their fingerprinted tries and fetches by entry id (SPEC annex I.6,
         * {@code agentspaces.gossip.audit}): on by default, which advertises {@link #AE_HINT}. Off, the node advertises
         * no such hint and reconciles every stream by the forms of v0.1.17 and before, as a 0.6.0 member does.
         *
         * @param enabled whether to audit
         * @return this builder
         */
        public Builder audit(boolean enabled) {
            this.audit = enabled;
            return this;
        }

        /**
         * How many audit sessions this node keeps open per asker (annex I.6, {@code agentspaces.gossip.audit-sessions}):
         * 4 by default, the most annex I.6 allows. A stream whose root finds the asker's sessions at the bound waits a
         * round.
         *
         * @param sessions from 1 to 4
         * @return this builder
         * @throws IllegalArgumentException outside those bounds
         */
        public Builder auditSessions(int sessions) {
            if (sessions < 1 || sessions > 4) {
                throw new IllegalArgumentException("audit-sessions must be from 1 to 4, not " + sessions);
            }
            this.auditSessions = sessions;
            return this;
        }

        /**
         * The budget this node puts on its audit requests ({@code agentspaces.gossip.audit-budget}): 1 MiB by default,
         * annex I.6's default.
         *
         * @param bytes from 4 KiB to 4 MiB
         * @return this builder
         * @throws IllegalArgumentException outside those bounds
         */
        public Builder auditBudget(long bytes) {
            if (bytes < (4L << 10) || bytes > (4L << 20)) {
                throw new IllegalArgumentException("audit-budget must be from 4096 to 4194304 bytes, not " + bytes);
            }
            this.auditBudget = bytes;
            return this;
        }

        /**
         * Whether each space on this node logs, once, the first renewal that asks for less than its lease has left
         * (RE-H-01, {@code agentspaces.space.renew-shortening-log}); on by default. Spaces count such renewals
         * either way.
         *
         * @param log whether to log it
         * @return this builder
         */
        public Builder renewShorteningLog(boolean log) {
            this.renewShorteningLog = log;
            return this;
        }

        /**
         * Whether this node advertises {@link #HORIZON_HINT} and lets its spaces collect a withdrawn entry by the
         * entry's own lease while every member of their view does too (SPEC §7.3, RE-H-04,
         * {@code agentspaces.space.horizon}): on by default ({@code per-entry}). Off ({@code legacy}), the node
         * advertises no such hint, so every group it is in keeps {@code 2 x maxLease} for withdrawals, as a 0.6.0
         * member does. Set it off on a node that renews for an identity another node also renews for at the same
         * time, since the hint promises one renewing node per identity ({@link #HORIZON_HINT}).
         *
         * @param perEntry whether to advertise the hint
         * @return this builder
         */
        public Builder horizon(boolean perEntry) {
            this.horizonPerEntry = perEntry;
            return this;
        }

        /**
         * Whether this node accepts recordless entry states and says so with the
         * {@link #RECORDLESS_HINT} (srad/agentspaces-pdt.md PD-L-03): on by default. Off, the
         * node does not advertise the hint, so every writer in its groups keeps sending it
         * full states, as a 0.5.0 member receives them.
         *
         * @param enabled whether to advertise the hint
         * @return this builder
         */
        public Builder recordless(boolean enabled) {
            this.recordless = enabled;
            return this;
        }

        /**
         * The AppCard level this node advertises under {@link #APPCARD_HINT} (SPEC §6.1a.2; AppCard origin and formats,
         * 2026-11-09): {@link #APPCARD_LEVEL} by default. Level 1 advertises no hint, so a test builds a mixed fleet in
         * which every issuer keeps a card's gated fields out of the view.
         *
         * @param level 1 or 2
         * @return this builder
         */
        public Builder appCardLevel(int level) {
            if (level < 1 || level > APPCARD_LEVEL) {
                throw new IllegalArgumentException("this build reads AppCard levels 1 to " + APPCARD_LEVEL + ": " + level);
            }
            this.appCardLevel = level;
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
         * {@code bootstrap: multicast}, roadmap M1; off by default). On each
         * retry the node sends one UDP datagram per joined group carrying only
         * its signed self-advertisement: at once when it joins a group, then
         * after {@code interval}, 2, 10 and 20 times it, and from the top again
         * (srad/agentspaces-event-loop.md EL-O-11; 1, 2, 10 and 20 min at the
         * 1 min default). Datagrams heard from others are verified and
         * admitted exactly like a stranger's bootstrap rumor, after which the
         * node introduces itself to the advertised endpoints as if they were
         * configured seeds. See {@link MulticastBeacon}.
         *
         * @param group    the multicast group address and port, e.g.
         *                 {@code 239.255.42.99:7787}
         * @param interval the first retry interval (EL-O-11), on the node's
         *                 monotonic clock
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
         * @param interval the first retry interval (EL-O-11), on the node's
         *                 monotonic clock
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
     * It first hears the current hints of every member the node holds, so
     * registering late misses nobody.
     *
     * @param listener receives the peer and its current hint map
     */
    public void onCredentialHints(BiConsumer<PeerId, Map<String, String>> listener) {
        Objects.requireNonNull(listener, "listener");
        // ISSUE-MemberHintSnapshot FR-3: a listener registered after a member's admission hears the member's current
        // hints now, not at its next change. Under the map's lock, so a change that lands meanwhile reaches the
        // listener after the hints it replaces.
        synchronized (lastHints) {
            credentialHintListeners.add(listener);
            for (Map.Entry<PeerId, Map<String, String>> e : List.copyOf(lastHints.entrySet())) {
                listener.accept(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Takes a credential-hint listener off again (the enterprise platform's FA2 audit §C.3.3: an agent that stops
     * must deregister what it registered, or the node keeps it and everything it captured for the node's life).
     *
     * @param listener the listener given to {@link #onCredentialHints}
     * @return true when the listener was registered and is now gone
     */
    public boolean offCredentialHints(BiConsumer<PeerId, Map<String, String>> listener) {
        return credentialHintListeners.remove(Objects.requireNonNull(listener, "listener"));
    }

    /** The number of credential-hint listeners registered, for lifecycle tests that count what stop() leaves behind. */
    public int credentialHintListeners() {
        return credentialHintListeners.size();
    }

    /**
     * Sets one of this node's own credential hints (plan EFG §4): the value is
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
        selfAdChanged();
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
        selfAdChanged();
    }

    /**
     * Removes one of this node's own credential hints; the next
     * self-advertisement no longer carries it.
     *
     * @param key the hint key
     */
    public void removeCredentialHint(String key) {
        nodeHints.remove(Objects.requireNonNull(key, "key"));
        selfAdChanged();
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

    /**
     * How many verified frames this node refused because their stamp's logical counter is above
     * {@link HybridLogicalClock#LOGICAL_LIMIT} (HLC logical counter, 2026-11-08, Rust team
     * report). Each is also a strike against its signer; none reached this node's clock.
     */
    public long framesLogicalRefused() {
        return framesLogicalRefused.get();
    }

    /**
     * How many verified frames this node dropped because their stamp lies further past its wall
     * time plus {@link HybridLogicalClock#MAX_DRIFT_MILLIS} than {@link #FAR_FUTURE_SKEW_MARGIN_MILLIS}
     * (ASF-020; HLC canonical stamps, 2026-11-08). Each is also a strike against its signer.
     */
    public long framesFarFuture() {
        return framesFarFuture.get();
    }

    /**
     * How many verified frames this node dropped because their stamp lies past its wall time plus
     * {@link HybridLogicalClock#MAX_DRIFT_MILLIS} by at most {@link #FAR_FUTURE_SKEW_MARGIN_MILLIS}
     * (HLC canonical stamps, 2026-11-08, decision B). None is a strike: an honest peer whose clock
     * a hostile stamp dragged to its own ceiling sends such frames when this node's wall runs a
     * little behind its own.
     */
    public long framesFarFutureSkewed() {
        return framesFarFutureSkewed.get();
    }

    /**
     * Returns how many inbound frames the wire codec refused outright (spec
     * §9): failed signature, half-signed, unknown wire version, or garbage.
     * Such frames name no verified sender, so they count here and strike
     * nobody. A frame whose stamp is any CBOR item other than a definite-length,
     * untagged, shortest-form text string is garbage too (HLC round four,
     * 2026-11-08, decision D3). An inner frame of a verified BATCH that does
     * not decode counts here as well, and the outer signer is struck for it.
     */
    public long framesDropped() {
        return framesDropped.get();
    }

    /** Witnessed-violation strikes currently held against a peer (WS5), for tests. */
    int strikes(PeerId peer) {
        return strikes.getOrDefault(peer, 0);
    }

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
    /** Every link attached to this node and not yet closed, so closing the node closes them all. */
    private final Set<TransportConnection> attached = ConcurrentHashMap.newKeySet();

    private void onConnectionClosed(TransportConnection connection) {
        attached.remove(connection);
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
        // A peer whose channel went away is introduced to again on the self ad's next run, which a link change asks.
        selfAdSentTo.values().forEach(sent -> sent.remove(peer));
        for (GroupParticipants gp : participants.values()) {
            gp.introductionsOwed = true;
            gp.selfAd.wake();
        }
        linksChanged();
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
        selfAdChanged();
    }

    /** The topology roles this node declares ({@code RENDEZVOUS}, {@code RELAY}). */
    public Set<PeerAdvertisement.PeerRole> roles() {
        return roles;
    }

    /**
     * The wire hints this node advertises ({@code wire.batch}, {@code wire.tree},
     * {@code wire.session}, {@code wire.hashes}, {@code wire.recordless}, {@code wire.appcard}, {@code wire.ae}, {@code wire.horizon}), a
     * copy.
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
     * for diagnostics and tests; no host or console reads it today.
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
     * configured group name. The join keeps asking while it waits
     * (ISSUE-BootstrapRobustness FR-4): after the first locate it waits in
     * slices of 1 s, 2 s, 4 s, then 5 s each up to the timeout; at each slice
     * it dials again every endpoint that is not connected, sends
     * {@code GROUP_AD_WANT} again on every connection it holds, and asks the
     * locator again when nothing is connected. A first answer that is empty
     * under {@code bootstrap.require} refuses at once, with no slices.
     *
     * @param groupId          the GroupID
     * @param membershipConfig membership tuning
     * @param locator          how to find existing members
     * @param timeout          how long to wait for a verified founding advertisement
     * @param membershipHints  resource hints for our self-ad (join credentials)
     * @param validator        POLICY admission validator, or {@code null}
     * @param groupName        the configured group name, or null for the id
     * @return the group runtime
     * @throws FoundingTimeoutException when no seed served a verified founding advertisement within the
     *                                  timeout; the message reports the locates, the endpoints dialed and
     *                                  connected, and the last hint
     * @throws IllegalStateException    when the group is already joined or a fetch for it is already in progress
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
        // The timeout bounds the whole join, the first locate included (CR-3).
        // The join waits on futures on the node's time source (group W): a virtual one releases it when an advance
        // passes a slice's end; a node on a hand-moved clock waits on the system's.
        ai.badmonkey.agentspaces.common.time.TimeSource waits = ai.badmonkey.agentspaces.common.time.Waits.base(time);
        long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(waits, timeout);
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
        if (groups.containsKey(groupId)) {
            throw new IllegalStateException("already a member of " + groupId);
        }
        CompletableFuture<SignedGroupAdvertisement> pending = new CompletableFuture<>();
        if (pendingFounding.putIfAbsent(groupId, pending) != null) {
            throw new IllegalStateException("a join of " + groupId + " is already in progress");
        }
        // ISSUE-BootstrapRobustness FR-4: the join keeps asking while it waits.
        // Slices of 1 s, 2 s, 4 s, then 5 s each up to the timeout; at the start
        // of each slice every endpoint that is not connected is dialed again,
        // GROUP_AD_WANT goes out again on every connection held, and when nothing
        // is connected the locator is asked again on a virtual thread, so a slow
        // locator never extends the timeout. The first locate above stays
        // synchronous because require (FR-5) decides on it.
        Map<String, TransportConnection> connected = new LinkedHashMap<>();
        Set<String> dialed = new HashSet<>();
        int locates = 1;
        long slice = JOIN_SLICE_FIRST_MILLIS;
        BeaconHint current = hint;
        CompletableFuture<BeaconHint> reask = null;
        try {
            dialAndAsk(groupId, current, dialed, connected);
            while (true) {
                long sliceEnd = Math.min(waits.monoNanos() + slice * 1_000_000L, deadline);
                while (true) {
                    if (sliceEnd - waits.monoNanos() <= 0) {
                        break;
                    }
                    CompletableFuture<?> wake = reask == null ? pending : CompletableFuture.anyOf(pending, reask);
                    try {
                        if (!ai.badmonkey.agentspaces.common.time.Waits.awaitDone(waits, wake, sliceEnd)) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TimeoutException("interrupted while fetching the founding advertisement of "
                                + groupId);
                    }
                    if (wake.isCompletedExceptionally()) {
                        try {
                            wake.join();
                        } catch (java.util.concurrent.CompletionException | java.util.concurrent.CancellationException e) {
                            throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
                        }
                    }
                    if (pending.isDone()) {
                        break;
                    }
                    if (reask != null && reask.isDone()) {
                        BeaconHint answer = reask.join();
                        reask = null;
                        if (!answer.isEmpty()) {
                            current = answer;
                        }
                        dialAndAsk(groupId, current, dialed, connected);
                    }
                }
                if (pending.isDone()) {
                    SignedGroupAdvertisement founding;
                    try {
                        founding = pending.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TimeoutException("interrupted while fetching the founding advertisement of "
                                + groupId);
                    } catch (java.util.concurrent.ExecutionException e) {
                        throw new IllegalStateException(e.getCause());
                    }
                    return join(founding.advertisement(), founding, membershipConfig, locator, current,
                            membershipHints, validator, name);
                }
                if (waits.monoNanos() - deadline >= 0) {
                    throw joinTimeout(groupId, timeout, locates, dialed.size(), connected.size(), current);
                }
                dialAndAsk(groupId, current, dialed, connected);
                if (connected.isEmpty() && reask == null) {
                    locates++;
                    String askedAs = name;
                    reask = CompletableFuture.supplyAsync(() -> locate(groupId, askedAs, locator, true),
                            Thread::startVirtualThread);
                }
                slice = Math.min(slice * 2, JOIN_SLICE_MAX_MILLIS);
            }
        } finally {
            pendingFounding.remove(groupId, pending);
            connected.values().forEach(TransportConnection::close);
        }
    }

    /**
     * Dials every endpoint of the hint that is not connected and sends
     * {@code GROUP_AD_WANT} on every connection held. An endpoint that
     * refused once is dialed again next time, so a seed that starts late at
     * the same address is reached (ISSUE-BootstrapRobustness FR-4).
     */
    private void dialAndAsk(GroupId groupId, BeaconHint hint, Set<String> dialed,
                            Map<String, TransportConnection> connected) {
        for (PeerAdvertisement.Endpoint seed : hint.endpoints()) {
            String key = seed.transport() + "://" + seed.address();
            if (connected.containsKey(key)) {
                continue;
            }
            Transport transport = transports.get(seed.transport());
            if (transport == null) {
                continue;
            }
            dialed.add(key);
            try {
                TransportConnection connection = transport.dial(seed.address());
                connected.put(key, connection);
                attach(connection);
            } catch (IOException e) {
                // Seed unreachable now; it is dialed again at the next slice.
            }
        }
        for (java.util.Iterator<TransportConnection> it = connected.values().iterator(); it.hasNext();) {
            TransportConnection connection = it.next();
            try {
                // Unaddressed: the seed's PeerID is not known until it answers.
                sendOn(connection, groupId, null, Envelope.Kind.GROUP_AD_WANT, new byte[0]);
            } catch (IOException e) {
                // The connection went away; drop it so the endpoint is dialed again.
                it.remove();
                connection.close();
            }
        }
    }

    /** The report of a join by id that ran out of time (ISSUE-BootstrapRobustness FR-6). */
    private FoundingTimeoutException joinTimeout(GroupId groupId, Duration timeout, int locates, int dialed,
                                                 int connected, BeaconHint hint) {
        StringBuilder message = new StringBuilder("no verified founding for ").append(groupId)
                .append(" within ").append(timeout.toMillis()).append(" ms: ")
                .append(locates).append(" locate(s), ")
                .append(connected).append(" of ").append(dialed).append(" endpoint(s) connected");
        if (connected > 0) {
            message.append(", the connected peers sent no founding for this id");
        }
        message.append("; last hint: ").append(traceText(groupId, hint));
        return new FoundingTimeoutException(message.toString(), locates, dialed, connected);
    }

    /** Decodes a hint's founding bytes, or null when they are not a signed founding advertisement. */
    private SignedGroupAdvertisement decodeFounding(byte[] bytes) {
        try {
            return codec.fromWire(bytes, SignedGroupAdvertisement.class);
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
        long now = time.millis();
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
        if ("1".equals(hint.parameters().get(RECORDLESS_HINT))) {
            recordlessPeers.add(peer);
        }
        if (AE_LEVEL.equals(hint.parameters().get(AE_HINT))) {
            aePeers.add(peer);
        }
    }

    /**
     * Whether a member's advertisement carries {@link #RECORDLESS_HINT}, so a space may send it a
     * recordless state (PD-L-03). A member without a known advertisement does not.
     *
     * @param peer the member
     * @return true when the member accepts the recordless form
     */
    public boolean advertisesRecordless(PeerId peer) {
        return peer != null && recordlessPeers.contains(peer);
    }

    /**
     * The AppCard level a hint's text states (SPEC §6.1a.2, §9): canonical decimal text, so {@code 2}, {@code 10}, or a
     * value past every integer reads as at least that level; no hint, any other text, and {@code 0} read as level 1.
     *
     * @param text the hint's text, or null
     * @return the level, at least 1
     */
    public static int appCardLevelOf(String text) {
        if (text == null || text.isEmpty() || text.length() > 1 && text.charAt(0) == '0') {
            return 1;
        }
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) < '0' || text.charAt(i) > '9') {
                return 1;
            }
        }
        if (text.length() > 9) {
            return Integer.MAX_VALUE;
        }
        return Math.max(1, Integer.parseInt(text));
    }

    /**
     * The AppCard level a member's advertisement states (SPEC §6.1a.2): 1 for a member without the hint, with any
     * other text, or whose advertisement this node has not received.
     *
     * @param peer the member
     * @return its level
     */
    public int appCardLevel(PeerId peer) {
        return peer == null ? 1 : appCardLevels.getOrDefault(peer, 1);
    }

    /** The AppCard level this node advertises. */
    public int appCardLevel() {
        return appCardLevel;
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
        long now = time.millis();
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
                new GroupMembership(peerId(), time, random, membershipConfig);
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
        // the bus times its asks for announced items by this node's clock (swarm retries, ROBUSTNESS.md §10.2)
        GossipBus gossip = new GossipBus(membership, sender, codec,
                groupAd.gossip().fanout(), GossipBus.DEFAULT_HOPS, this::localMillis, random);
        gossip.probeDigestStreams(probeHashStreams);
        if (lazyPush) {
            gossip.lazyPush(treePeers::contains);
            gossip.directDelivery(PEER_AD_STREAM); // introduced directly and flooded: never a tree duplicate
            gossip.alwaysEager(PEER_AD_STREAM);
            gossip.alwaysEager(REVOCATION_STREAM);
            gossip.alwaysEager(CREDENTIAL_REVOCATION_STREAM);
        }
        gossip.hashedDigests(hashPeers::contains);
        if (audit) {
            // The form toward each member is read from its current card at every round (RE-T-03, RE-Q-REL-09).
            gossip.audit(aePeers::contains);
            gossip.auditSessions(auditSessions);
            gossip.auditBudget(auditBudget);
        }
        RevocationRegistry revocations = new RevocationRegistry(groupAd,
                revocationValidator, codec, time,
                ad -> onPeerRevoked(membership, ad));
        ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry credentialRevocations =
                new ai.badmonkey.agentspaces.peering.membership.CredentialRevocationRegistry(groupAd,
                        credentialRevocationValidator, codec, time);
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
        retirementList.load(groupId, time.millis());
        if (groupAd.membershipPolicy() == GroupAdvertisement.MembershipPolicy.INVITE
                && !admissionMemory.persistent()) {
            LOG.log(System.Logger.Level.INFO, "the admission memory of group " + groupId.value()
                    + " is held in memory only: no state directory is configured, so a restart forgets which"
                    + " members were admitted under an invite delegation before it expired, and refuses them"
                    + " after it (PeerNode.Builder.stateDirectory; the starter passes agentspaces.keystore)");
        }
        groups.put(groupId, runtime);
        registerParticipants(runtime);
        if (beacon != null) { // the new group is announced at once, from the table's top (EL-O-11)
            beaconRestart.set(true);
            beaconParticipant.wake();
        }

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
        retryLostRound(runtime, nowMillis);
        updateMembershipStatus(runtime, nowMillis);
    }

    private void retryLostRound(GroupRuntime runtime, long nowMillis) {
        GroupId groupId = runtime.id();
        Map<PeerId, Long> lost = lostMembers.get(groupId);
        if (lost != null && !lost.isEmpty()) {
            Map<PeerId, Backoff> schedule = lostSchedule.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>());
            List<Map.Entry<PeerId, Backoff>> due = new ArrayList<>();
            for (Map.Entry<PeerId, Long> entry : List.copyOf(lost.entrySet())) {
                PeerId peer = entry.getKey();
                long lostAt = entry.getValue();
                if (runtime.revocations().revoked(peer) || runtime.membership().member(peer).isPresent()) {
                    lost.remove(peer);
                    schedule.remove(peer);
                    continue;
                }
                if (nowMillis - lostAt > (retirement ? retireAfterMillis : LOST_RETRY_WINDOW_MILLIS)) {
                    lost.remove(peer);
                    schedule.remove(peer);
                    if (retirement) {
                        retire(groupId, peer, lostAt, nowMillis);
                    }
                    continue;
                }
                Backoff backoff = schedule.computeIfAbsent(peer, p -> new Backoff(lostAt));
                if (nowMillis >= backoff.nextDueMillis) {
                    due.add(Map.entry(peer, backoff));
                }
            }
            // The oldest-due first, at most a budget per round: a large split re-dials its lost members over a few
            // rounds rather than all at once (no dial storm; reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md).
            due.sort(java.util.Comparator.comparingLong(e -> e.getValue().nextDueMillis));
            int budget = LOST_RETRY_BUDGET;
            for (Map.Entry<PeerId, Backoff> entry : due) {
                if (budget-- <= 0) {
                    break;
                }
                List<PeerAdvertisement.Endpoint> endpoints = lastEndpoints.get(entry.getKey());
                if (endpoints != null && !endpoints.isEmpty()) {
                    introduceTo(groupId, endpoints);
                    lostIntroductions.incrementAndGet();
                }
                entry.getValue().scheduleNext(nowMillis, random);
            }
        }
        if (!runtime.membership().allMembers().isEmpty()) {
            emptySince.remove(groupId);
            return;
        }
        long empty = emptySince.computeIfAbsent(groupId, g -> nowMillis);
        // An empty view: the seeds and the locator, on the group's own back-off (5 s doubling to 30 s).
        Long last = lastLostRetryMillis.get(groupId);
        long interval = lostRetryIntervalMillis.getOrDefault(groupId, LOST_RETRY_INTERVAL_MILLIS);
        if (last == null || nowMillis - last >= interval) {
            List<PeerAdvertisement.Endpoint> seeds = groupSeeds.get(groupId);
            if (seeds != null && !seeds.isEmpty()) {
                introduceTo(groupId, seeds);
            }
            relocateIfStale(groupId, nowMillis);
            lastLostRetryMillis.put(groupId, nowMillis);
            lostRetryIntervalMillis.put(groupId, Math.min(interval * 2, LOST_RETRY_MAX_INTERVAL_MILLIS));
        }
        if (nowMillis - empty < LAST_RESORT_AFTER_MILLIS) {
            return;
        }
        // Nobody for a while after the lost members, the seeds and the locator: the last resort, when opted in.
        if (postRetirement) {
            lastResort(groupId, nowMillis);
        }
    }

    // ---- the membership status (the console's view of reachability, 2026-11-19)

    /** Where a node stands in one group, from its own view (it is the one place an isolated node can be seen). */
    public enum MembershipState {
        /** No member yet since joining, the seeds and the locator still being tried. */
        JOINING,
        /** Members in view, none lost. */
        CONNECTED,
        /** Members in view, and some lost ones being retried. */
        DEGRADED,
        /** Every member lost, for less than the wait before the last resort; lost members, seeds and locator retried. */
        RECONNECTING,
        /** Nobody for longer than the wait; post-retirement is trying the retired members. */
        LAST_RESORT,
        /** Nobody for longer than the wait, with something to reach and nothing left to try: out of service here. */
        ISOLATED,
        /** No member, and nothing to reach: no member ever, no seed, no locator. A sole founder or a one-node setup. */
        ALONE
    }

    /** A lost member being retried: when it was lost, how many introductions it had, when the next is due. */
    public record LostMember(PeerId peer, long lostAtMillis, int attempts, long nextDueMillis,
                             List<PeerAdvertisement.Endpoint> endpoints) {
    }

    /** A retired member: when it was last seen, when it retired, where it was. */
    public record RetiredMember(PeerId peer, long lastSeenMillis, long retiredAtMillis,
                                List<PeerAdvertisement.Endpoint> endpoints) {
    }

    /**
     * A node's standing in a group (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md).
     *
     * @param group          the group
     * @param state          the state
     * @param sinceMillis    when the state began, on the node's clock
     * @param members        the members in view
     * @param lost           the lost members being retried
     * @param retired        the retired members
     * @param retirement     whether lost members retire
     * @param postRetirement whether retired members are the last resort
     */
    public record MembershipStatus(GroupId group, MembershipState state, long sinceMillis, int members,
                                   List<LostMember> lost, List<RetiredMember> retired, boolean retirement,
                                   boolean postRetirement) {
    }

    private record StateSince(MembershipState state, long sinceMillis) {
    }

    private final Map<GroupId, StateSince> membershipStates = new ConcurrentHashMap<>();
    /** Groups in which this node has had a member: losing them all is a loss, not a start. */
    private final Set<GroupId> everConnected = ConcurrentHashMap.newKeySet();
    private final List<Consumer<MembershipStatus>> membershipListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Tells a listener of every change of a group's membership state, from the tick that sees it (a console's event
     * stream, a health indicator). The listener runs on the tick thread and must be quick.
     *
     * @param listener the listener
     */
    public void onMembershipStatus(Consumer<MembershipStatus> listener) {
        membershipListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * A group's membership status now.
     *
     * @param groupId the group
     * @return the status, or empty when this node has not joined the group
     */
    public Optional<MembershipStatus> membershipStatus(GroupId groupId) {
        GroupRuntime runtime = groups.get(groupId);
        if (runtime == null) {
            return Optional.empty();
        }
        long now = time.millis();
        MembershipState state = membershipState(runtime, now);
        StateSince known = membershipStates.get(groupId);
        long since = known != null && known.state() == state ? known.sinceMillis() : now;
        return Optional.of(status(runtime, state, since, now));
    }

    private MembershipStatus status(GroupRuntime runtime, MembershipState state, long since, long now) {
        GroupId groupId = runtime.id();
        List<LostMember> lostRows = new ArrayList<>();
        Map<PeerId, Backoff> schedule = lostSchedule.getOrDefault(groupId, Map.of());
        lostMembers.getOrDefault(groupId, Map.of()).forEach((peer, lostAt) -> {
            Backoff backoff = schedule.get(peer);
            lostRows.add(new LostMember(peer, lostAt, backoff == null ? 0 : backoff.attempts,
                    backoff == null ? lostAt + LOST_RETRY_INTERVAL_MILLIS : backoff.nextDueMillis,
                    lastEndpoints.getOrDefault(peer, List.of())));
        });
        lostRows.sort(java.util.Comparator.comparingLong(LostMember::lostAtMillis));
        List<RetiredMember> retiredRows = retirementList.list(groupId, now).stream()
                .map(r -> new RetiredMember(r.peer(), r.lastSeenMillis(), r.retiredAtMillis(), r.endpoints()))
                .toList();
        return new MembershipStatus(groupId, state, since, runtime.membership().allMembers().size(),
                List.copyOf(lostRows), retiredRows, retirement, postRetirement);
    }

    private MembershipState membershipState(GroupRuntime runtime, long now) {
        GroupId groupId = runtime.id();
        boolean lost = !lostMembers.getOrDefault(groupId, Map.of()).isEmpty();
        if (!runtime.membership().allMembers().isEmpty()) {
            return lost ? MembershipState.DEGRADED : MembershipState.CONNECTED;
        }
        boolean retiredAny = !retirementList.list(groupId, now).isEmpty();
        List<PeerAdvertisement.Endpoint> seeds = groupSeeds.get(groupId);
        BeaconLocator locator = groupLocators.get(groupId);
        boolean locates = locator != null && !(locator instanceof SeedsLocator) && locator.canLocate();
        boolean reachable = everConnected.contains(groupId) || (seeds != null && !seeds.isEmpty()) || locates
                || lost || retiredAny;
        if (!reachable) {
            return MembershipState.ALONE;
        }
        Long empty = emptySince.get(groupId);
        if (empty == null || now - empty < LAST_RESORT_AFTER_MILLIS) {
            return everConnected.contains(groupId) ? MembershipState.RECONNECTING : MembershipState.JOINING;
        }
        return postRetirement && retiredAny ? MembershipState.LAST_RESORT : MembershipState.ISOLATED;
    }

    private void updateMembershipStatus(GroupRuntime runtime, long nowMillis) {
        GroupId groupId = runtime.id();
        if (!runtime.membership().allMembers().isEmpty()) {
            everConnected.add(groupId);
        }
        MembershipState state = membershipState(runtime, nowMillis);
        StateSince previous = membershipStates.get(groupId);
        if (previous != null && previous.state() == state) {
            return;
        }
        membershipStates.put(groupId, new StateSince(state, nowMillis));
        if (state == MembershipState.ISOLATED) {
            Long empty = emptySince.get(groupId);
            LOG.log(System.Logger.Level.WARNING, "group " + groupId.value() + ": no member for "
                    + (empty == null ? 0 : (nowMillis - empty) / 1000) + " s; no lost member, seed or locator brought"
                    + " one back" + (postRetirement ? ", nor the retirement list" : " (the retirement list is not"
                    + " consulted: post-retirement is off)") + "; this node is isolated in the group");
        } else if (previous != null && (previous.state() == MembershipState.ISOLATED
                || previous.state() == MembershipState.LAST_RESORT)) {
            LOG.log(System.Logger.Level.INFO, "group " + groupId.value() + ": " + state + " again after "
                    + (nowMillis - previous.sinceMillis()) / 1000 + " s " + previous.state());
        }
        if (!membershipListeners.isEmpty()) {
            MembershipStatus status = status(runtime, state, nowMillis, nowMillis);
            for (Consumer<MembershipStatus> listener : membershipListeners) {
                try {
                    listener.accept(status);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "a membership status listener failed", e);
                }
            }
        }
    }

    /**
     * Introduces this node to a few retired members' last endpoints, the most recently seen first, on a slow back-off
     * of its own (two minutes doubling to ten). Opt-in: a retired member's address may by now belong to another host,
     * which learns of this node from the card it is sent; admission still verifies whoever answers.
     *
     * @return whether the list held anyone to try (the node is not out of options yet)
     */
    private boolean lastResort(GroupId groupId, long nowMillis) {
        List<RetirementList.Retired> retired = new ArrayList<>();
        for (RetirementList.Retired r : retirementList.list(groupId, nowMillis)) {
            GroupRuntime runtime = groups.get(groupId);
            if (runtime != null && runtime.revocations().revoked(r.peer())) {
                retirementList.remove(groupId, r.peer());
            } else {
                retired.add(r);
            }
        }
        if (retired.isEmpty()) {
            return false;
        }
        // the first round at once, the next ones on the slow back-off
        Backoff backoff = lastResortSchedule.computeIfAbsent(groupId,
                g -> new Backoff(nowMillis - LAST_RESORT_BASE_MILLIS, LAST_RESORT_BASE_MILLIS, LAST_RESORT_MAX_MILLIS));
        if (nowMillis < backoff.nextDueMillis) {
            return true;
        }
        int cursor = lastResortCursor.getOrDefault(groupId, 0);
        for (int i = 0; i < Math.min(LAST_RESORT_BUDGET, retired.size()); i++) {
            RetirementList.Retired r = retired.get((cursor + i) % retired.size());
            introduceTo(groupId, r.endpoints());
            lastResortIntroductions.incrementAndGet();
        }
        lastResortCursor.put(groupId, (cursor + LAST_RESORT_BUDGET) % retired.size());
        backoff.scheduleNext(nowMillis, random);
        return true;
    }

    /** For tests: records a member as lost at a time, with endpoints to retry, as a departure would. */
    void noteLostForTest(GroupId groupId, PeerId peer, List<PeerAdvertisement.Endpoint> endpoints, long lostAtMillis) {
        lastEndpoints.put(peer, List.copyOf(endpoints));
        lostMembers.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>()).put(peer, lostAtMillis);
        GroupParticipants gp = participants.get(groupId);
        if (gp != null) {
            gp.lost.wake();
        }
    }

    /** Moves a lost member that never answered to the retirement list, with its last endpoints and card time. */
    private void retire(GroupId groupId, PeerId peer, long lostAtMillis, long nowMillis) {
        List<PeerAdvertisement.Endpoint> endpoints = lastEndpoints.get(peer);
        if (endpoints == null || endpoints.isEmpty()) {
            return; // nothing to dial later
        }
        Long issued = lostAdIssued.getOrDefault(groupId, Map.of()).get(peer);
        retirementList.retire(groupId, new RetirementList.Retired(peer, List.copyOf(endpoints),
                issued == null ? 0 : issued, lostAtMillis, nowMillis));
        Map<PeerId, Long> lostIssued = lostAdIssued.get(groupId);
        if (lostIssued != null) {
            lostIssued.remove(peer);
        }
        retirements.incrementAndGet();
        LOG.log(System.Logger.Level.INFO, "group " + groupId.value() + ": retired " + peer.display()
                + ", unanswered since " + (nowMillis - lostAtMillis) / 60_000 + " min"
                + (postRetirement ? "; kept as a last resort" : ""));
    }

    /**
     * A per-target retry schedule: the first try at the base after the loss, then doubling to the cap (a lower cap
     * in the first hour for lost members), each delay spread by a quarter either way so a fleet that lost the same
     * members does not re-dial them in step.
     */
    static final class Backoff {
        final long sinceMillis;
        final long baseMillis;
        final long earlyCapMillis;
        final long capMillis;
        int attempts;
        long nextDueMillis;

        Backoff(long sinceMillis) {
            this(sinceMillis, LOST_RETRY_INTERVAL_MILLIS, LOST_RETRY_EARLY_CAP_MILLIS, LOST_RETRY_CAP_MILLIS);
        }

        Backoff(long sinceMillis, long baseMillis, long capMillis) {
            this(sinceMillis, baseMillis, capMillis, capMillis);
        }

        Backoff(long sinceMillis, long baseMillis, long earlyCapMillis, long capMillis) {
            this.sinceMillis = sinceMillis;
            this.baseMillis = baseMillis;
            this.earlyCapMillis = earlyCapMillis;
            this.capMillis = capMillis;
            this.nextDueMillis = sinceMillis + baseMillis;
        }

        void scheduleNext(long nowMillis, Random random) {
            attempts++;
            long cap = nowMillis - sinceMillis < LOST_RETRY_EARLY_PHASE_MILLIS ? earlyCapMillis : capMillis;
            long delay = Math.min(cap, baseMillis << Math.min(attempts, 20));
            long spread = delay / 4;
            nextDueMillis = nowMillis + delay - spread + (spread == 0 ? 0 : (long) (random.nextDouble() * 2 * spread));
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
        lastViewVersion.remove(groupId);
        GroupParticipants leaving = participants.remove(groupId);
        if (leaving != null) {
            leaving.close();
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
        lostSchedule.remove(groupId);
        lostAdIssued.remove(groupId);
        emptySince.remove(groupId);
        membershipStates.remove(groupId);
        everConnected.remove(groupId);
        lastResortSchedule.remove(groupId);
        lastResortCursor.remove(groupId);
        retirementList.flushAll(time.millis());
        retirementList.forget(groupId);
        lastLostRetryMillis.remove(groupId);
        lostRetryIntervalMillis.remove(groupId);
        heldAds.remove(groupId);
        newlyAdmitted.remove(groupId);
        admittedAtMillis.keySet().removeIf(key -> key.group().equals(groupId));
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
        if (coalescer != null) {
            coalescer.discard(groupId);
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
            recordlessPeers.remove(peer);
            aePeers.remove(peer);
            if (horizonPeers.remove(peer)) {
                horizonHintsVersion.incrementAndGet();
            }
            if (appCardLevels.remove(peer) != null) { // FR-6: a member counts at level 2 only on an ad held now
                appCardVersion.incrementAndGet();
            }
        }
        LOG.log(System.Logger.Level.INFO, "left group " + groupId.value() + ": " + held.size()
                + " member(s) forgotten, " + closed + " connection(s) closed");
        return true;
    }

    /**
     * Runs everything due now, and every participant whether due or not (srad/agentspaces-event-loop.md EL-L-04,
     * DW-1): for every joined group, refresh the leased self-advertisement, probe membership, handle departures,
     * retry lost members, run the lazy push, and reconcile with one partner once per (stretched) gossip period; run
     * every {@code onTick} registrant and every {@link GroupRuntime#onDue} participant; announce on the bootstrap
     * beacon once per beacon interval. Each participant judges for itself whether its work is due, so an early call
     * is harmless. The run is serial with the node's loop (NL-2) and returns when it ends; called from inside the
     * node's own run it runs inline (EL-L-16).
     */
    public void tick() {
        loop.tick();
    }

    // ---- the node's participants (srad/agentspaces-event-loop.md §3.2, §3.3, EL-L-01, appendix C)

    /** The trust participant's deadline until every channel-trust supplier signals its changes (EL-L-14). */
    static final long TRUST_RECHECK_MILLIS = 1_000;
    /** How often a relayed member over the offload threshold is looked at again while it stays over (JS-W-11). */
    static final long OFFLOAD_RECHECK_MILLIS = 1_000;
    /** The built-in participants of one group (EL-L-01) and what each reads between its runs (DW-5). */
    private final class GroupParticipants {
        final GroupRuntime runtime;
        final List<DueWork.Registration> all = new ArrayList<>();
        DueWork.Registration selfAd;
        DueWork.Registration membership;
        DueWork.Registration view;
        DueWork.Registration gossip;
        DueWork.Registration antiEntropy;
        DueWork.Registration newcomer;
        DueWork.Registration lost;
        DueWork.Registration reach;
        DueWork.Registration mesh;
        DueWork.Registration pins;
        DueWork.Registration offload;
        DueWork.Registration persist;
        // The generations each reader last read (DW-5): no participant clears a flag another reads.
        long membershipActivitySeen;
        long membershipTrafficSeen;
        long antiEntropyActivitySeen;
        // The anti-entropy round's partner, and the run it was picked in, for the newcomer round of the same run.
        PeerId periodicPartner;
        long periodicPartnerRun = Long.MIN_VALUE;
        // The self ad's change test (EL-L-09): the card version and view it last judged, and whether a link loss
        // owes a member a fresh introduction.
        long selfAdSeenVersion = -1;
        long selfAdSeenView = -1;
        volatile boolean introductionsOwed = true;

        GroupParticipants(GroupRuntime runtime) {
            this.runtime = runtime;
        }

        DueWork.Registration add(String name, NodeLoop.Kind kind, DueWork work) {
            return add(name, kind, work, NodeLoop.Timing.FIXED);
        }

        DueWork.Registration add(String name, NodeLoop.Kind kind, DueWork work, NodeLoop.Timing timing) {
            DueWork.Registration registration = loop.register(runtime.id().value() + "/" + name, kind,
                    runtime.id().value(), work, timing);
            all.add(registration);
            return registration;
        }

        void close() {
            all.forEach(DueWork.Registration::close);
        }
    }

    private final Map<GroupId, GroupParticipants> participants = new ConcurrentHashMap<>();
    /** The node-wide participants: the trust rejudge (EL-L-14) and the bootstrap beacon. */
    private DueWork.Registration trustParticipant;
    private DueWork.Registration beaconParticipant;
    /** Moves whenever an input of this node's card moves (an endpoint, a hint), for the self ad's change test (EL-L-09). */
    private final AtomicLong selfAdVersion = new AtomicLong();
    /** One deprecation note per {@code onTick} registrant class (EL-L-05). */
    private static final Set<String> TICK_ADAPTER_NOTED = ConcurrentHashMap.newKeySet();

    /** A wall deadline on the node's clock as a deadline on the loop's monotonic base, or NEVER. */
    static long due(long nowNanos, long nowMillis, long deadlineMillis) {
        return ai.badmonkey.agentspaces.common.time.TimeSources.monoDue(nowNanos, nowMillis, deadlineMillis);
    }

    /**
     * A wall deadline as a deadline on the loop's base, converted against both clocks read now, together. A run's own
     * {@code now} was read when the run began, and a participant reads the wall clock after the participants before
     * it ran: converting one against the other made a deadline look earlier by however long they took, and the
     * backstop counted it as a missed wake (6 in 5 minutes over TCP, srad/agentspaces-event-loop.md §10.8).
     */
    private long dueFromWall(long deadlineMillis) {
        return due(time.monoNanos(), time.millis(), deadlineMillis);
    }

    /** A deadline on this node's local base ({@link #localMillis()}) as a deadline on the loop's base, or NEVER. */
    private long dueFromLocal(long deadlineMillis) {
        long nowNanos = time.monoNanos();
        return due(nowNanos, Math.floorDiv(nowNanos, 1_000_000L), deadlineMillis);
    }

    /**
     * This node's local now: the time source's monotonic milliseconds, the base of every local period
     * (srad/agentspaces-event-loop.md §3.6, EL-C-04): probe turns and timeouts, the gossip bus's windows, the
     * self-ad and beacon cadences, relay pins, quarantines and log throttles. A wall step moves none of them.
     */
    private long localMillis() {
        return time.monoMillis();
    }

    /** The earlier of two deadlines on the loop's base. */
    private static long earlier(long a, long b) {
        if (a == DueWork.NEVER) {
            return b;
        }
        if (b == DueWork.NEVER) {
            return a;
        }
        return a - b <= 0 ? a : b;
    }

    /** Registers the node-wide participants; called once, by the constructor. */
    private void registerNodeParticipants() {
        // core.wallstep.rerun (EL-C-12): asked by every wall step with every other participant; its run, after every
        // protocol participant of the run, closes the measure from the step.
        wallStepMarker = loop.register("wall-step", NodeLoop.Kind.OTHER, "", now -> {
            long at = wallStepPendingNanos;
            if (at >= 0) {
                wallStepPendingNanos = -1;
                lastWallStepRerunNanos = Math.max(0, time.monoNanos() - at);
            }
            return DueWork.NEVER;
        });
        trustParticipant = loop.register("trust", NodeLoop.Kind.PROTOCOL, "", now -> {
            rejudgeIfTrustChanged();
            // one reference compare a second until every supplier signals (EL-L-14, EL-Q-SEC-06)
            return channelTrust == null ? DueWork.NEVER : now + TRUST_RECHECK_MILLIS * 1_000_000L;
        }, LOCAL_PERIOD); // a local recheck; a trust change also signals (EL-L-14)
        // The beacon takes no part in the stretch (EL-O-11): its retries follow their own table, so FIXED.
        beaconParticipant = loop.register("beacon", NodeLoop.Kind.PROTOCOL, "", now -> runBeacon(),
                NodeLoop.Timing.FIXED);
    }

    /**
     * The beacon participant (srad/agentspaces-event-loop.md EL-O-11): announces at once after a join, from position
     * 0, and otherwise when its retry falls due; each retry reads the interval at the beacon's position and advances
     * it ({@link MulticastBeacon#nextRetry()}). With no group joined nothing is announced and no retry is spent; the
     * next join asks it again.
     */
    private long runBeacon() {
        if (beacon == null) {
            return DueWork.NEVER;
        }
        boolean restart = beaconRestart.getAndSet(false);
        if (groups.isEmpty()) {
            nextBeaconMillis = Long.MIN_VALUE;
            return DueWork.NEVER;
        }
        long nowMillis = localMillis();
        if (restart) {
            beacon.succeeded(); // a fresh group starts the table from the top
        }
        long next = nextBeaconMillis;
        if (restart || next == Long.MIN_VALUE || nowMillis >= next) {
            beacon.announce();
            next = nowMillis + beacon.nextRetry().toMillis();
            nextBeaconMillis = next;
        }
        return dueFromLocal(next);
    }

    /** The node's beacon, or null when it has none (EL-O-11 tests). */
    MulticastBeacon beacon() {
        return beacon;
    }

    /**
     * The timing of a local period under the loop's stretch (§3.10, EL-O-04): it stretches with the factor and no
     * peer judges it. The node's protocol cadences a peer judges (the self ad, the probes, the gossip push) keep
     * {@link NodeLoop.Timing#FIXED}: they are what this node's keepalive and peers' TTLs rest on (EL-O-06).
     */
    private static final NodeLoop.Timing LOCAL_PERIOD = NodeLoop.Timing.local(Duration.ZERO);

    /**
     * Registers a group's built-in participants (EL-L-01), in today's order within the group, and the change signals
     * that ask them again (EL-L-03, EL-L-13, EL-T-15).
     */
    private void registerParticipants(GroupRuntime runtime) {
        GroupId groupId = runtime.id();
        GroupParticipants gp = new GroupParticipants(runtime);
        // Every built-in participant is the node's own protocol work, so it runs before any registrant (NL-8), and
        // an inbound working frame, which asks a group's registrants (Kind.OTHER), never asks these.
        NodeLoop.Kind protocol = NodeLoop.Kind.PROTOCOL;
        gp.selfAd = gp.add("self-ad", protocol, now -> runSelfAd(gp, now));
        gp.membership = gp.add("membership", protocol, now -> runMembership(gp, now));
        gp.view = gp.add("view", protocol, now -> runView(gp));
        gp.gossip = gp.add("gossip", protocol, now -> {
            long nowMillis = localMillis();
            runtime.gossip().tick(nowMillis); // lazy push: grafts and announcements (PERF2 §3.4)
            long next = runtime.gossip().nextDueMillis(nowMillis);
            return next <= nowMillis ? now + 1_000_000L : dueFromLocal(next);
        });
        // Anti-entropy stretches, never past 32 times the founding period (SPEC §5.3, EL-O-04).
        gp.antiEntropy = gp.add("anti-entropy", protocol, now -> runAntiEntropy(gp, now), NodeLoop.Timing.local(
                runtime.advertisement().gossip().period().multipliedBy(GossipBus.ANTI_ENTROPY_STRETCH_CEILING)));
        gp.newcomer = gp.add("newcomer", protocol, now -> runNewcomer(gp, now));
        gp.lost = gp.add("lost", protocol, now -> runLost(gp, now), LOCAL_PERIOD);
        gp.reach = gp.add("reach", protocol, now -> {
            publishReachHints(runtime);
            return DueWork.NEVER; // asked again on a link or view change
        });
        gp.mesh = gp.add("mesh", protocol, now -> runMesh(gp, now), LOCAL_PERIOD);
        gp.pins = gp.add("pins", protocol, now -> runPins(gp, now), LOCAL_PERIOD);
        gp.offload = gp.add("offload", protocol, now -> runOffload(gp, now), LOCAL_PERIOD);
        gp.persist = gp.add("persist", protocol, now -> {
            long nowMillis = time.millis();
            long period = runtime.advertisement().gossip().period().toMillis();
            admissionMemory.flushIfDue(groupId, nowMillis, period);
            retirementList.flushIfDue(groupId, nowMillis, period);
            long next = Math.min(admissionMemory.nextFlushDueMillis(groupId, period),
                    retirementList.nextFlushDueMillis(groupId, period));
            return next <= nowMillis ? dueFromWall(nowMillis + period) : dueFromWall(next);
        }, LOCAL_PERIOD);
        participants.put(groupId, gp);
        // The change signals (EL-L-03): a view change or a pace moved asks the whole group; the first activity after
        // a quiet stretch asks membership and anti-entropy (EL-L-13); the bus's own work asks the bus (EL-T-15).
        runtime.membership().onChange(() -> loop.wakeGroup(groupId.value()));
        runtime.gossip().loopHooks(new GossipBus.LoopHooks() {
            @Override
            public void activity() {
                gp.membership.wake();
                gp.antiEntropy.wake();
            }

            @Override
            public void gossipDue() {
                gp.gossip.wake();
            }

            @Override
            public void paceChanged() {
                loop.wakeGroup(groupId.value());
            }
        });
        if (coalescer != null) {
            runtime.gossip().outbox(peer -> coalescer.holds(groupId, peer));
        }
    }

    /** The self ad (EL-L-09): its work, then its next refresh. */
    private long runSelfAd(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        long nowMillis = localMillis();
        publishSelfAd(gp, runtime, runtime.gossip(), nowMillis);
        Long last = lastSelfAdMillis.get(runtime.id());
        if (last == null) {
            return now + 1_000_000L;
        }
        return dueFromLocal(last + refreshIntervalMillis(runtime, runtime.membership().size()));
    }

    /** Membership: the probe digests when a probe goes, then SWIM's round (EL-L-06). */
    private long runMembership(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        GossipBus gossip = runtime.gossip();
        long nowMillis = localMillis();
        long activity = gossip.activityGeneration();
        boolean active = activity != gp.membershipActivitySeen;
        gp.membershipActivitySeen = activity;
        // The membership probe's quiet rule (SPEC §5.2) counts all gossip, held here or not, as it did before the
        // honest backoff: a member forwarding a busy stream it does not hold keeps the failure detector at the
        // base period, and only the anti-entropy period reads the honest activity. Each reader keeps its own copy of
        // the generation (DW-5), so membership and anti-entropy never consume each other's signal.
        long trafficSeen = gossip.trafficGeneration();
        boolean traffic = trafficSeen != gp.membershipTrafficSeen;
        gp.membershipTrafficSeen = trafficSeen;
        // The digest hashes a probe carries (v0.1.17) are taken here, before the membership monitor is held, and
        // only when they will be read (IDLE-WORK item 7): a receiver reads them only while its own anti-entropy
        // has backed off, which in a quiet group is when this node's has too. Busy, both run anti-entropy at the
        // base period, the hashes would differ on every probe anyway, and taking them cost every stream's
        // digest twice a second.
        boolean probing = (probeHashesAlways || !active && runtime.antiEntropyStretched())
                && runtime.membership().probeDue(nowMillis); // only on the run a probe goes
        // Two kinds of hashes, taken only when they will be read: a member on wire.ae=2 gets each space's trie
        // hash and any other member the text digests' hashes, so each compares like with like (annex I.6).
        // Each kind is taken only when some member in view would receive it: the text form, which rebuilds a
        // space's sorted digest whenever its generation moved (a lease sweep moves it in a space nobody writes),
        // only when the view holds a member without wire.ae=2, and the trie form only when it holds one with it.
        // Taking the text form for an all-hinted view cost the busy spaces' digests every probing tick and was
        // never sent (a demo console's deployments, 2026-11-18). Both are decided here, outside the membership
        // monitor the probe callbacks run under, since taking a digest takes its space's lock.
        boolean textPartner = false;
        boolean triePartner = false;
        if (probing) {
            for (GroupMembership.Member member : runtime.membership().allMembers()) {
                if (member.id().equals(peerId())) {
                    continue;
                }
                if (gossip.audits(member.id())) {
                    triePartner = true;
                } else {
                    textPartner = true;
                }
                if (textPartner && triePartner) {
                    break;
                }
            }
        }
        ProbeDigests probeText = probing && textPartner ? probeDigests(gossip, false) : ProbeDigests.NONE;
        ProbeDigests probeTrie = probing && triePartner && gossip.auditing() ? probeDigests(gossip, true)
                : ProbeDigests.NONE;
        runtime.membership().tick(new GroupMembership.Prober() {
            @Override
            public void ping(PeerId target, long nonce) {
                // The probe carries the anti-entropy digests, when taken above, and the recent items (v0.1.17).
                ProbeDigests probe = gossip.audits(target) ? probeTrie : probeText;
                send(runtime.id(), target, Envelope.Kind.PING, new Bodies.Ping(nonce,
                        probe.root(), probe.hashes(), gossip.piggyback(target)));
            }

            @Override
            public void pingReq(PeerId relay, PeerId target, long nonce) {
                send(runtime.id(), relay, Envelope.Kind.PING_REQ,
                        new Bodies.PingReq(target, nonce));
            }

            @Override
            public java.util.Collection<PeerId> linkHolders(PeerId target) {
                // TR-1: the members the target names in relay.via, when this node holds no link to it, cannot dial
                // it and has no relay path; a PING would go nowhere (srad/agentspaces-event-loop.md §3.12).
                Set<PeerId> via = viaOf(runtime.id(), target);
                if (via.isEmpty() || hopsTo(runtime.id(), target) >= 0) {
                    return List.of();
                }
                return via;
            }

            @Override
            public java.util.Collection<PeerId> declaredHolders(PeerId target) {
                return viaOf(runtime.id(), target); // TR-2: an escalated round asks these first
            }

            @Override
            public boolean carriesNews(PeerId target, long sinceMillis) {
                if (gossip.piggyback(target) != null) {
                    return true;
                }
                ProbeDigests probe = gossip.audits(target) ? probeTrie : probeText;
                if (probe.root() == null && probe.hashes() == null) {
                    return false;
                }
                // The target's own PING with hashes in this period was answered with this node's: both compared.
                Long hashedAt = hashedPingFrom.getOrDefault(runtime.id(), Map.of()).get(target);
                return hashedAt == null || hashedAt < sinceMillis;
            }
        }, !traffic);
        if (!traffic) {
            gossip.armTrafficEdge(trafficSeen); // quiet: the first traffic from now on asks again (EL-L-13)
        }
        long next = runtime.membership().nextDueMillis();
        return next <= nowMillis ? now + 1_000_000L : dueFromLocal(next);
    }

    /**
     * The view as of this run, kept for leaving the group, and every departure, each once (EL-L-01). Asked on a view
     * change; nothing here has a deadline.
     */
    private long runView(GroupParticipants gp) {
        GroupRuntime runtime = gp.runtime;
        long nowMillis = time.millis();
        // A member that left the view since the last run (the failure detector dropped it, or an eviction did) is
        // outside the data plane: only its own introduction re-admits it, and it sends one on the channel it dials
        // next (see connectionTo), so the stale channel is closed here to make that happen.
        // The view as of this run, kept for leaving the group (the members held then); taken only when the view's
        // version moved (idle-loop O8), since every entry and exit moves it.
        long viewVersion = runtime.membership().viewVersion();
        Long taken = lastViewVersion.put(runtime.id(), viewVersion);
        if (taken == null || taken != viewVersion) {
            Set<PeerId> view = new java.util.HashSet<>();
            for (GroupMembership.Member member : runtime.membership().allMembers()) {
                view.add(member.id());
            }
            lastView.put(runtime.id(), view);
        }
        // Every departure is handled, each once, from the membership's own record of it: a member that left and came
        // back between two runs (re-admitted from a copy of its card, then dropped again by a probe) left all the
        // same, and a loss not recorded is a loss never retried. Comparing views from tick to tick missed it, and a
        // fleet whose other members had stopped retrying stayed split for good (HorizonLaggingCopyTest, 2026-11-18).
        boolean departed = false;
        for (PeerId dropped : runtime.membership().drainDeparted()) {
            if (runtime.membership().member(dropped).isPresent()) {
                continue; // back already: nothing to retry, and its link and state stay
            }
            memberLeft(runtime, runtime.gossip(), dropped, nowMillis);
            departed = true;
        }
        if (departed) {
            gp.lost.wake(); // a loss to retry, later in this same run (NL-7)
        }
        return DueWork.NEVER;
    }

    /** The periodic anti-entropy round, its quiet stretch and the snap-back on the first activity (EL-L-07, EL-L-13). */
    private long runAntiEntropy(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        GossipBus gossip = runtime.gossip();
        long nowMillis = localMillis();
        long activity = gossip.activityGeneration();
        boolean active = activity != gp.antiEntropyActivitySeen;
        gp.antiEntropyActivitySeen = activity;
        if (runtime.claimAntiEntropyRound(nowMillis, active)) {
            gp.periodicPartner = gossip.antiEntropyTick().orElse(null);
            gp.periodicPartnerRun = now;
        }
        if (!active) {
            gossip.armActivityEdge(activity); // quiet: the first activity from now on asks again (EL-L-13)
        }
        long next = runtime.nextAntiEntropyMillis(nowMillis);
        return next <= nowMillis ? now + 1_000_000L : dueFromLocal(next);
    }

    /**
     * A member admitted since the last run is reconciled with now, after this node's own introduction went out (the
     * self ad runs first), so it holds this node and answers: the newcomer pulls the cards and space state the fleet
     * already has instead of waiting for the random periodic partner to be one that holds them. The periodic round's
     * partner of this same run is not asked twice. Asked by the admission (EL-L-07, EL-SP-09).
     */
    private long runNewcomer(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        Set<PeerId> admitted = newlyAdmitted.remove(runtime.id());
        if (admitted == null) {
            return DueWork.NEVER;
        }
        PeerId periodicPartner = gp.periodicPartnerRun == now ? gp.periodicPartner : null;
        for (PeerId member : admitted) {
            boolean present = runtime.membership().member(member).isPresent();
            if (!member.equals(periodicPartner) && present) {
                runtime.gossip().antiEntropyWith(member);
            }
            Long admittedAt = admittedAtMillis.remove(new GroupPeer(runtime.id(), member));
            if (admittedAt != null && present) {
                // reconciled now, by the targeted round or the periodic one that picked it (EL-M-04)
                loopMeter.newcomerRound(Math.max(0, localMillis() - admittedAt));
            }
        }
        return DueWork.NEVER;
    }

    /** Lost members and the empty view: their retries' next due times (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md). */
    private long runLost(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        GroupId groupId = runtime.id();
        long nowMillis = time.millis();
        retryLost(runtime, nowMillis);
        long next = Long.MAX_VALUE;
        Map<PeerId, Long> lost = lostMembers.get(groupId);
        if (lost != null && !lost.isEmpty()) {
            long window = retirement ? retireAfterMillis : LOST_RETRY_WINDOW_MILLIS;
            Map<PeerId, Backoff> schedule = lostSchedule.getOrDefault(groupId, Map.of());
            for (Map.Entry<PeerId, Long> entry : lost.entrySet()) {
                next = Math.min(next, entry.getValue() + window + 1);
                Backoff backoff = schedule.get(entry.getKey());
                next = Math.min(next, backoff == null ? entry.getValue() + LOST_RETRY_INTERVAL_MILLIS
                        : backoff.nextDueMillis);
            }
        }
        if (runtime.membership().size() == 0) {
            Long last = lastLostRetryMillis.get(groupId);
            long interval = lostRetryIntervalMillis.getOrDefault(groupId, LOST_RETRY_INTERVAL_MILLIS);
            next = Math.min(next, last == null ? nowMillis : last + interval);
            Long empty = emptySince.get(groupId);
            if (empty != null && nowMillis - empty < LAST_RESORT_AFTER_MILLIS) {
                next = Math.min(next, empty + LAST_RESORT_AFTER_MILLIS);
            } else if (postRetirement) {
                Backoff backoff = lastResortSchedule.get(groupId);
                if (backoff != null && backoff.nextDueMillis > nowMillis) {
                    next = Math.min(next, backoff.nextDueMillis); // an overdue one found nobody to try
                }
            }
        }
        // A retry already overdue (the round's budget ran out) goes on the next run a moment later.
        return next <= nowMillis ? dueFromWall(nowMillis + 100) : dueFromWall(next);
    }

    /** The relay mesh (relays only): each unlinked relay again a minute after its last dial (LB-R-02). */
    private long runMesh(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        if (!roles.contains(PeerAdvertisement.PeerRole.RELAY)) {
            return DueWork.NEVER;
        }
        long nowMillis = localMillis();
        meshRelays(runtime, nowMillis);
        long next = Long.MAX_VALUE;
        for (PeerId relay : runtime.membership().withRole(PeerAdvertisement.PeerRole.RELAY)) {
            Long attempted = meshAttempted.get(relay);
            if (attempted != null && !relay.equals(peerId()) && !connections.containsKey(relay)) {
                next = Math.min(next, attempted + MESH_RETRY_MILLIS);
            }
        }
        return next <= nowMillis ? dueFromLocal(nowMillis + MESH_RETRY_MILLIS) : dueFromLocal(next);
    }

    /** Pinned relay paths: the earliest idle expiry; a hop leaving the view asks again (LB-R-07). */
    private long runPins(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        long nowMillis = localMillis();
        expirePins(runtime, nowMillis);
        Map<PeerId, PinnedPath> pins = pinnedPaths.get(runtime.id());
        if (pins == null || pins.isEmpty()) {
            return DueWork.NEVER;
        }
        long next = Long.MAX_VALUE;
        for (PinnedPath pinned : pins.values()) {
            next = Math.min(next, pinned.lastUsedMillis() + pathTtl.toMillis() + 1);
        }
        return dueFromLocal(Math.max(next, nowMillis + 1));
    }

    /** The relay offload (JS-W-11): looked at again while a relayed member stays over the threshold. */
    private long runOffload(GroupParticipants gp, long now) {
        GroupRuntime runtime = gp.runtime;
        long nowMillis = localMillis();
        offloadRelayed(runtime, nowMillis); // JS-W-11, off this thread for the dial itself
        Map<PeerId, java.util.ArrayDeque<Long>> perMember = relayedWith.get(runtime.id());
        if (perMember == null) {
            return DueWork.NEVER;
        }
        List<java.util.ArrayDeque<Long>> windows;
        synchronized (perMember) {
            windows = List.copyOf(perMember.values());
        }
        for (java.util.ArrayDeque<Long> times : windows) {
            if (windowCount(times, nowMillis) > relayOffload.framesPerMinute()) {
                return dueFromLocal(nowMillis + OFFLOAD_RECHECK_MILLIS);
            }
        }
        return DueWork.NEVER; // the frame that takes a member over the threshold asks again
    }

    /**
     * Registers due work of a group on the node's loop (GroupRuntime.onDue): a registrant's participant, run after
     * the protocol participants of every group (NL-8).
     */
    DueWork.Registration registerDue(GroupId groupId, String name, DueWork work, NodeLoop.Timing timing) {
        return loop.register(groupId.value() + "/" + name, NodeLoop.Kind.OTHER, groupId.value(), work, timing);
    }

    /**
     * The deprecated {@code onTick} adapter (decision 7, EL-L-05): the runnable runs every {@code tick-millis} while
     * the node's loop runs, and on every {@link #tick()}; hand-driven, only on {@link #tick()}.
     */
    DueWork.Registration registerTickAdapter(GroupId groupId, Runnable work) {
        String owner = work.getClass().getName();
        int lambda = owner.indexOf("$$");
        if (lambda > 0) {
            owner = owner.substring(0, lambda);
        }
        if (TICK_ADAPTER_NOTED.add(owner)) {
            // The core's own registrants move to onDue in phase 2 (EL-T-09); an application's hears it once.
            System.Logger.Level level = owner.startsWith("ai.badmonkey.agentspaces.")
                    && !owner.startsWith("ai.badmonkey.agentspaces.examples.")
                    ? System.Logger.Level.DEBUG : System.Logger.Level.INFO;
            LOG.log(level, "GroupRuntime.onTick is deprecated (registered by " + owner + "); it runs every tick-millis"
                    + " on the node's loop: register GroupRuntime.onDue work that returns its next due time");
        }
        long[] last = {Long.MIN_VALUE};
        String name = "tick:" + owner.substring(owner.lastIndexOf('.') + 1);
        return loop.register(groupId.value() + "/" + name, NodeLoop.Kind.OTHER, groupId.value(), now -> {
            Duration period = tickPeriod;
            long periodNanos = (period == null ? DEFAULT_TICK_PERIOD : period).toNanos();
            boolean dueByTime = period != null && (last[0] == Long.MIN_VALUE || now - last[0] >= periodNanos);
            if (loop.directRun() || dueByTime) {
                last[0] = now;
                work.run();
            }
            return period == null ? DueWork.NEVER : last[0] + periodNanos;
        });
    }

    /** A link opened or closed (EL-L-03): the reach hints, the relay mesh, the offload and owed introductions. */
    private void linksChanged() {
        for (GroupParticipants gp : participants.values()) {
            gp.reach.wake();
            gp.mesh.wake();
            gp.offload.wake();
        }
    }

    /** An input of this node's card moved (EL-L-09): every group's self ad is asked again. */
    private void selfAdChanged() {
        selfAdVersion.incrementAndGet();
        for (GroupParticipants gp : participants.values()) {
            gp.selfAd.wake();
        }
    }

    /**
     * Tells the node that the channel-trust supplier's value changed (a CRL refresh, srad/agentspaces-event-loop.md
     * EL-L-14): every live chained channel is re-judged on the loop's next run, so a revoked channel is cut at once
     * rather than on the trust participant's next look.
     */
    public void trustChanged() {
        DueWork.Registration trust = trustParticipant;
        if (trust != null) {
            trust.wake();
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
     * interval, unless the group's probe cycle (one member per period, stretched four times when
     * quiet, or further while the TTL bound allows: {@link GroupMembership#maxQuietIntervalMillis}) has outgrown the
     * keepalive TTL: a member is kept by hearing from it, and past that size the probes cannot reach every member
     * inside the TTL, so the refresh takes over as the keepalive at half the TTL (ROBUSTNESS.md
     * §6b, scale). Small groups keep their cadence unchanged. The rule reads the keepalive TTL (30 s by default), not
     * the member TTL this node judges others by, so every peer that judges this node at 30 s sees today's cadence
     * (srad/agentspaces-event-loop.md EL-E-06, EL-E-07).
     */
    long refreshIntervalMillis(GroupRuntime runtime, int members) {
        GroupMembership membership = runtime.membership();
        long ttl = membership.config().keepaliveTtl().toMillis();
        long legacy = membership.probeIntervalMillis() * (1L << GroupMembership.LEGACY_QUIET_STRETCH);
        if (membership.maxQuietIntervalMillis(members) > legacy) {
            // stretched past four times by the TTL bound, which keeps every member heard twice inside the TTL
            return peerAdInterval.toMillis();
        }
        // the probe interval in effect (RE-F-03 moves it at run time), stretched as far as a quiet group may
        long probeCycle = (long) (members + 1) * legacy;
        if (probeCycle < ttl) {
            return peerAdInterval.toMillis();
        }
        return Math.min(peerAdInterval.toMillis(), ttl / 2);
    }


    private void publishSelfAd(GroupParticipants gp, GroupRuntime runtime, GossipBus gossip, long nowMillis) {
        GroupId groupId = runtime.id();
        // The change test (srad/agentspaces-event-loop.md EL-L-09): with no input of the card moved, the view as it
        // was, no introduction owed and the refresh not due, the run allocates nothing. A setter moves the version,
        // a view change the view's version, a link loss the owed flag; each also asks this participant again.
        long version = selfAdVersion.get();
        long viewVersion = runtime.membership().viewVersion();
        Long lastRun = lastSelfAdMillis.get(groupId);
        if (lastRun != null && gp.selfAdSeenVersion == version && gp.selfAdSeenView == viewVersion
                && !gp.introductionsOwed
                && nowMillis - lastRun < refreshIntervalMillis(runtime, runtime.membership().size())) {
            return;
        }
        gp.introductionsOwed = false; // before the walk: a link lost during it owes again
        gp.selfAdSeenView = viewVersion;
        Set<PeerId> sentTo = selfAdSentTo.computeIfAbsent(groupId, g -> ConcurrentHashMap.newKeySet());
        List<GroupMembership.Member> members = runtime.membership().allMembers();
        Set<PeerId> current = new java.util.HashSet<>();
        for (GroupMembership.Member member : members) {
            current.add(member.id());
        }
        sentTo.retainAll(current);
        Long last = lastSelfAdMillis.get(groupId);
        boolean changed = false;
        if (gp.selfAdSeenVersion != version || last == null) {
            int fingerprint = Objects.hash(List.copyOf(advertisedEndpoints), roles, selfHints(groupId));
            changed = !Integer.valueOf(fingerprint).equals(lastSelfAdFingerprint.get(groupId));
            lastSelfAdFingerprint.put(groupId, fingerprint);
            gp.selfAdSeenVersion = version;
        }
        if (last == null || changed) {
            // Flooded: the content is new and has to reach every member, through a third where a
            // direct link is cut. A view change no longer floods (ROBUSTNESS.md §6b, scale): a
            // newcomer gets this node's advertisement directly below, a lost member by the
            // lost-member retry, and at fifty members one expiry made forty-nine floods.
            // Mark before publishing: a member whose send fails is dropped from the set by
            // dropConnection during the publish, and is introduced to again on the next run.
            sentTo.addAll(current);
            lastSelfAdMillis.put(groupId, nowMillis);
            Bodies.Rumor selfAd = selfAdRumor(groupId);
            gossip.publish(selfAd.streamId(), selfAd.itemId(), selfAd.payload());
            selfAdFloods.incrementAndGet();
            return;
        }
        if (nowMillis - last >= refreshIntervalMillis(runtime, members.size())) {
            selfAdRefreshes.incrementAndGet();
            // The interval refresh goes to each member directly and unforwarded (v0.1.17): the
            // flood cost every member several copies when nothing else had pruned the tree, and
            // a refresh has to reach each member exactly once anyway.
            sentTo.addAll(current);
            lastSelfAdMillis.put(groupId, nowMillis);
            Bodies.Rumor selfAd = selfAdRumor(groupId);
            Bodies.Rumor refresh = new Bodies.Rumor(selfAd.streamId(), selfAd.itemId(), 1, selfAd.payload());
            for (PeerId member : current) {
                if (member.equals(peerId())) {
                    continue;
                }
                if (!send(groupId, member, Envelope.Kind.RUMOR, refresh)) {
                    gossip.publish(selfAd.streamId(), selfAd.itemId(), selfAd.payload()); // unreachable: flood
                    selfAdFloods.incrementAndGet();
                    return;
                }
                selfAdRefreshFrames.incrementAndGet();
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
                selfAdIntroductions.incrementAndGet();
                continue;
            }
            // Unreachable directly (a partition, a dial-only peer behind another member): let
            // the group carry the introduction instead, as the per-tick flood used to.
            Bodies.Rumor flooded = selfAdRumor(groupId);
            gossip.publish(flooded.streamId(), flooded.itemId(), flooded.payload());
            selfAdFloods.incrementAndGet();
            lastSelfAdMillis.put(groupId, nowMillis);
            sentTo.addAll(current);
            return;
        }
    }

    /**
     * Starts the node's loop for wall-clock deployments (srad/agentspaces-event-loop.md §3.3): from now on each
     * participant runs when it is due or asked, and the node sleeps in between; there is no periodic tick. The period
     * is {@code tick-millis}: the cadence the deprecated {@code onTick} adapters keep (decision 8) and the one
     * {@link #tickPeriod()} reports to layers that convert ticks to time. Idempotent.
     *
     * @param period the tick period
     */
    public synchronized void startTicking(Duration period) {
        if (tickPeriod != null) {
            return;
        }
        this.tickPeriod = Objects.requireNonNull(period, "period");
        loop.start(); // registers the loop with its time source's watchdog (EL-H-02)
        if (coalescer != null) {
            flushWatch = time.watchdog().register(coalescer); // the flush timer too
        }
    }

    /** Introductions this node sent to lost members' last endpoints (the per-member back-off). */
    public long lostIntroductions() {
        return lostIntroductions.get();
    }

    /** Introductions this node sent to retired members as a last resort (post-retirement). */
    public long lastResortIntroductions() {
        return lastResortIntroductions.get();
    }

    /** Lost members this node retired. */
    public long retirements() {
        return retirements.get();
    }

    /**
     * The members retired in a group, the most recently seen first (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md).
     *
     * @param groupId the group
     * @return their peer ids
     */
    public List<PeerId> retired(GroupId groupId) {
        return retirementList.list(groupId, time.millis()).stream().map(RetirementList.Retired::peer).toList();
    }

    /** Whether lost members retire (the retirement setting). */
    public boolean retirement() {
        return retirement;
    }

    /** Whether retired members are tried as a last resort (the post-retirement setting). */
    public boolean postRetirement() {
        return postRetirement;
    }

    /**
     * Whether this node is isolated in a group: its view has been empty for longer than the last resort waits, and
     * neither its lost members, its seeds, its locator, nor (with post-retirement on) its retired members brought
     * anyone back. A status for health checks and the console; the node keeps trying as before.
     *
     * @param groupId the group
     * @return true while isolated
     */
    public boolean isolated(GroupId groupId) {
        StateSince known = membershipStates.get(groupId);
        return known != null && known.state() == MembershipState.ISOLATED;
    }

    /**
     * Asks every participant of a group to run again on the loop's next run (srad/agentspaces-event-loop.md EL-L-15,
     * DW-2): work this node started whose deadline moved earlier calls this when it cannot name its participant.
     *
     * @param groupId the group
     */
    public void wake(GroupId groupId) {
        loop.wakeGroup(groupId.value());
    }

    /**
     * The node's time source: the loop's monotonic base and scheduler.
     *
     * @return the time source
     */
    public ai.badmonkey.agentspaces.common.time.TimeSource time() {
        return time;
    }

    /** Whether the calling thread is inside a run {@link #tick()} started (GroupRuntime.directRun). */
    boolean directRun() {
        return loop.directRun();
    }

    /**
     * Moves whenever a member's AppCard level moves, so a registrant that judges the AppCard gate re-judges only then
     * (srad/agentspaces-event-loop.md EL-T-07).
     *
     * @return the version
     */
    public long appCardVersion() {
        return appCardVersion.get();
    }

    /**
     * Frames that are liveness or time-paced reconciliation, or the gossip bus's own tree upkeep, wake no registrant;
     * any other kind is work a registrant of the group may be driving, and asks the group's registrants again on
     * arrival (EL-L-03). HAVE, GRAFT and PRUNE are the lazy push's announcements and repairs: the bus handles them and
     * wakes its own participant (EL-T-15), and nothing a registrant reads moves. Asking every registrant on each HAVE
     * was the needless wake behind phase 3's rise at ten members (srad/agentspaces-event-loop.md §10.21: HAVE received
     * 2.0 to 9.2 a minute, event wakes 6.4 to 14.6). A BATCH's inner frames are each dispatched and judged by their own
     * kind, so the outer frame asks nothing.
     */
    static boolean wakes(Envelope.Kind kind) {
        return switch (kind) {
            case PING, PING_REQ, ACK, RUMOR, DIGEST, PULL_RESP, HAVE, GRAFT, PRUNE, BATCH -> false;
            default -> true;
        };
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
        if (coalescer != null && !coalescer.stopped() && batchable(kind) && !introduction
                && batchPeers.contains(to) && !to.equals(peerId())) {
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
        coalescer.enqueue(groupId, to, kind, bodyBytes, inner);
    }

    private void onBatch(GroupRuntime runtime, Envelope envelope, TransportConnection connection) {
        Bodies.Batch batch;
        try {
            batch = codec.fromWire(envelope.body(), Bodies.Batch.class);
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
                inner = codec.fromWire(bytes, Envelope.class);
            } catch (RuntimeException e) {
                // HLC round four, 2026-11-08 (decision D5): an inner frame that does not decode, a
                // stamp that is no canonical text among the causes, counts as undecodable like a
                // frame alone, and the outer signature that covers its bytes names the signer.
                framesDropped.incrementAndGet();
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
     * Wraps the frame in a RELAY_FRAME and hands it to the first relay of the
     * path to the target (srad/large-networks-bulk-data.md §3.7): the pinned path
     * when one is held, else the shortest path over the relay graph the view
     * carries, pinned for later sends. A one-relay path travels in the 0.4.0
     * form; a longer one carries {@code path} and {@code hop}. With no path
     * known, the first reachable RELAY-role member carries the frame one hop,
     * as before.
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
        List<PeerId> path = pathTo(groupId, runtime, to);
        if (path != null) {
            PeerId first = path.get(0);
            TransportConnection connection = connectionTo(groupId, first);
            if (connection == null) {
                firstHopFailedAt.put(first, localMillis()); // not a start for a while (LB-R-04)
            } else {
                try {
                    Bodies.RelayFrame frame = path.size() == 1
                            ? new Bodies.RelayFrame(to, inner)
                            : new Bodies.RelayFrame(to, inner, path, 0);
                    sendOn(connection, groupId, first, Envelope.Kind.RELAY_FRAME, frame);
                    noteRelayed(groupId, to); // JS-W-11: a frame the relay carried for this pair
                    return true;
                } catch (IOException e) {
                    dropConnection(first, connection);
                }
            }
            unpin(groupId, to);
        }
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
                noteRelayed(groupId, to);
                return true;
            } catch (IOException e) {
                dropConnection(relay, connection);
            }
        }
        return false;
    }

    // ------------------------------------------------------------ relay paths (group R)

    /** The pinned path to a target, or a freshly computed and pinned one; null when none exists. */
    private List<PeerId> pathTo(GroupId groupId, GroupRuntime runtime, PeerId to) {
        Map<PeerId, PinnedPath> pins = pinnedPaths.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>());
        long now = localMillis();
        PinnedPath pinned = pins.get(to);
        if (pinned != null && now - pinned.lastUsedMillis() <= pathTtl.toMillis()
                && pinned.path().stream().allMatch(hop -> runtime.membership().member(hop).isPresent())) {
            pins.put(to, new PinnedPath(pinned.path(), now));
            return pinned.path();
        }
        List<PeerId> computed = computePath(runtime, to).orElse(null);
        if (computed == null) {
            pins.remove(to);
            return null;
        }
        boolean first = pins.isEmpty();
        pins.put(to, new PinnedPath(computed, now));
        GroupParticipants gp = participants.get(groupId);
        if (first && gp != null) {
            gp.pins.wake(); // a deadline where there was none (EL-L-03)
        }
        return computed;
    }

    private void unpin(GroupId groupId, PeerId to) {
        Map<PeerId, PinnedPath> pins = pinnedPaths.get(groupId);
        if (pins != null) {
            pins.remove(to);
        }
    }

    /** The relay graph of a group as this node's view carries it (LB-R-04). */
    private RelayPaths.Graph relayGraph(GroupRuntime runtime) {
        Set<PeerId> relays = new java.util.HashSet<>(runtime.membership().withRole(PeerAdvertisement.PeerRole.RELAY));
        if (roles.contains(PeerAdvertisement.PeerRole.RELAY)) {
            relays.add(peerId());
        }
        Map<PeerId, Set<PeerId>> links = new java.util.HashMap<>(relayLinks.getOrDefault(runtime.id(), Map.of()));
        if (roles.contains(PeerAdvertisement.PeerRole.RELAY)) {
            links.put(peerId(), attachedRelays(runtime));
        }
        return new RelayPaths.Graph(relays, links, relayVia.getOrDefault(runtime.id(), Map.of()));
    }

    /** The RELAY-role members this node holds a link to. */
    private Set<PeerId> attachedRelays(GroupRuntime runtime) {
        Set<PeerId> attached = new java.util.TreeSet<>(java.util.Comparator.comparing(PeerId::value));
        for (PeerId relay : runtime.membership().withRole(PeerAdvertisement.PeerRole.RELAY)) {
            if (!relay.equals(peerId()) && connections.containsKey(relay)) {
                attached.add(relay);
            }
        }
        return attached;
    }

    /** Whether this node could open a link to a member on its own: it has endpoints on a transport this node runs. */
    private boolean dialable(GroupMembership.Member member) {
        for (PeerAdvertisement.Endpoint endpoint : member.endpoints()) {
            Transport transport = transports.get(endpoint.transport());
            if (transport != null && !transport.signalled()) {
                return true;
            }
        }
        return false;
    }

    private Optional<List<PeerId>> computePath(GroupRuntime runtime, PeerId to) {
        RelayPaths.Graph graph = relayGraph(runtime);
        if (graph.relays().isEmpty()) {
            return Optional.empty();
        }
        // Where this node can start: first the relays it holds links to; only when those
        // reach nothing, the relays it could dial, less any whose dial failed lately. A
        // site relay behind the far NAT has an address and no reachability (LB-R-04).
        Set<PeerId> attached = new java.util.HashSet<>(attachedRelays(runtime));
        attached.remove(to);
        Set<PeerId> dialable = new java.util.HashSet<>();
        long now = localMillis();
        for (PeerId relay : graph.relays()) {
            Long failed = firstHopFailedAt.get(relay);
            if (!relay.equals(peerId()) && !relay.equals(to) && !attached.contains(relay)
                    && (failed == null || now - failed > MESH_RETRY_MILLIS)
                    && runtime.membership().member(relay).map(this::dialable).orElse(false)) {
                dialable.add(relay);
            }
        }
        Set<PeerId> start = attached;
        // Where the target can be reached from: its declared attachments, any relay when it
        // listens, or its relay neighbours when it is itself a relay.
        Set<PeerId> goal = new java.util.HashSet<>(graph.via().getOrDefault(to, Set.of()));
        GroupMembership.Member target = runtime.membership().member(to).orElse(null);
        if (goal.isEmpty() && target != null && !target.endpoints().isEmpty()) {
            goal.addAll(graph.relays());
        }
        if (goal.isEmpty() && graph.relays().contains(to)) {
            goal.addAll(graph.neighbours(to));
        }
        goal.remove(to);
        goal.remove(peerId());
        start.remove(peerId());
        RelayPaths.Graph without = new RelayPaths.Graph(
                graph.relays().stream().filter(r -> !r.equals(to) && !r.equals(peerId())).collect(java.util.stream.Collectors.toSet()),
                graph.links(), graph.via());
        Optional<List<PeerId>> fromAttached = RelayPaths.shortest(without, start, goal, maxPathHops);
        if (fromAttached.isPresent() || dialable.isEmpty()) {
            return fromAttached;
        }
        return RelayPaths.shortest(without, dialable, goal, maxPathHops);
    }

    /** How many relays a frame to a member crosses from here: 0 direct, else the path length, -1 when none. */
    int hopsTo(GroupId groupId, PeerId to) {
        if (connections.containsKey(to)) {
            return 0;
        }
        GroupRuntime runtime = groups.get(groupId);
        if (runtime == null) {
            return -1;
        }
        Map<PeerId, PinnedPath> pins = pinnedPaths.get(groupId);
        PinnedPath pinned = pins == null ? null : pins.get(to);
        if (pinned != null) {
            return pinned.path().size();
        }
        if (runtime.membership().member(to).map(this::dialable).orElse(false)) {
            return 0; // a listener this node has not dialed yet counts as direct
        }
        return computePath(runtime, to).map(List::size).orElse(-1);
    }

    /** The RELAY-role members this node holds links to in a group, sorted by id. */
    List<PeerId> viaRelays(GroupId groupId) {
        GroupRuntime runtime = groups.get(groupId);
        return runtime == null ? List.of() : List.copyOf(attachedRelays(runtime));
    }

    /** The relays a member declared attachment to, as this node last heard (console and tests). */
    Set<PeerId> viaOf(GroupId groupId, PeerId member) {
        return relayVia.getOrDefault(groupId, Map.of()).getOrDefault(member, Set.of());
    }

    /** The relays a relay declared links to, as this node last heard (console and tests). */
    Set<PeerId> relayLinksOf(GroupId groupId, PeerId relay) {
        return relayLinks.getOrDefault(groupId, Map.of()).getOrDefault(relay, Set.of());
    }

    /** Closes and forgets the link this node holds to a peer (tests: a cut wire). */
    void dropLinksTo(PeerId peer) {
        TransportConnection connection = connections.get(peer);
        if (connection != null) {
            dropConnection(peer, connection);
            connection.close();
        }
    }

    /** The pinned path to a target, for the console and tests. */
    public Optional<List<PeerId>> pinnedPath(GroupId groupId, PeerId to) {
        Map<PeerId, PinnedPath> pins = pinnedPaths.get(groupId);
        PinnedPath pinned = pins == null ? null : pins.get(to);
        return pinned == null ? Optional.empty() : Optional.of(pinned.path());
    }

    /** How many unreachable signals this node received as an origin. */
    public long unreachableSignalsReceived() {
        return unreachableSignalsReceived.get();
    }

    /** Puts this node's reach hints in its advertisement for a group (LB-R-01, LB-R-02). */
    private void publishReachHints(GroupRuntime runtime) {
        Set<PeerId> attached = attachedRelays(runtime);
        String via = advertisedEndpoints.isEmpty() && !attached.isEmpty() ? RelayPaths.encode(attached) : null;
        String links = roles.contains(PeerAdvertisement.PeerRole.RELAY) && !attached.isEmpty()
                ? RelayPaths.encode(attached) : null;
        setGroupHintIfChanged(runtime.id(), RelayPaths.VIA_HINT, via);
        setGroupHintIfChanged(runtime.id(), RelayPaths.LINKS_HINT, links);
    }

    private void setGroupHintIfChanged(GroupId groupId, String key, String value) {
        String current = groupHints.getOrDefault(groupId, Map.of()).get(key);
        if (!Objects.equals(current, value)) {
            groupHint(groupId, key, value);
        }
    }

    /** Reads a member's reach hints into the relay graph (LB-R-01, LB-R-02). */
    private void rememberReach(GroupId groupId, PeerAdvertisement ad) {
        Set<PeerId> via = RelayPaths.decode(ad.resourceHints().get(RelayPaths.VIA_HINT));
        Map<PeerId, Set<PeerId>> vias = relayVia.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>());
        if (via.isEmpty()) {
            vias.remove(ad.issuer());
        } else {
            vias.put(ad.issuer(), via);
        }
        Set<PeerId> links = RelayPaths.decode(ad.resourceHints().get(RelayPaths.LINKS_HINT));
        Map<PeerId, Set<PeerId>> adjacency = relayLinks.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>());
        if (links.isEmpty() || !ad.roles().contains(PeerAdvertisement.PeerRole.RELAY)) {
            adjacency.remove(ad.issuer());
        } else {
            adjacency.put(ad.issuer(), links);
        }
    }

    /** A relay dials every other RELAY-role member it can, off the tick thread (LB-R-02). */
    private void meshRelays(GroupRuntime runtime, long nowMillis) {
        if (!roles.contains(PeerAdvertisement.PeerRole.RELAY)) {
            return;
        }
        GroupId groupId = runtime.id();
        for (PeerId relay : runtime.membership().withRole(PeerAdvertisement.PeerRole.RELAY)) {
            if (relay.equals(peerId()) || connections.containsKey(relay) || meshInFlight.contains(relay)
                    || quarantined(relay)) {
                continue;
            }
            Long attempted = meshAttempted.get(relay);
            if (attempted != null && nowMillis - attempted < MESH_RETRY_MILLIS) {
                continue;
            }
            GroupMembership.Member member = runtime.membership().member(relay).orElse(null);
            if (member == null) {
                continue;
            }
            PeerAdvertisement.Endpoint chosen = null;
            Transport transport = null;
            for (PeerAdvertisement.Endpoint endpoint : byPriority(member.endpoints())) {
                Transport candidate = transports.get(endpoint.transport());
                if (candidate != null && !candidate.signalled()) {
                    chosen = endpoint;
                    transport = candidate;
                    break;
                }
            }
            if (chosen == null || !meshInFlight.add(relay)) {
                continue;
            }
            meshAttempted.put(relay, nowMillis);
            PeerAdvertisement.Endpoint endpoint = chosen;
            Transport dialer = transport;
            Thread.ofVirtual().name("peer-mesh-" + relay.display()).start(() -> {
                try {
                    dialEndpoint(groupId, relay, dialer, endpoint);
                } catch (IOException e) {
                    LOG.log(System.Logger.Level.DEBUG, "relay mesh dial to " + relay.display() + " failed: " + e.getMessage());
                } finally {
                    meshInFlight.remove(relay);
                }
            });
        }
    }

    /** Drops pins that idled past the TTL or whose hops left the view. */
    private void expirePins(GroupRuntime runtime, long nowMillis) {
        Map<PeerId, PinnedPath> pins = pinnedPaths.get(runtime.id());
        if (pins == null || pins.isEmpty()) {
            return;
        }
        pins.entrySet().removeIf(e -> nowMillis - e.getValue().lastUsedMillis() > pathTtl.toMillis()
                || e.getValue().path().stream().anyMatch(hop -> runtime.membership().member(hop).isEmpty()));
    }

    /**
     * Handles a {@code PIPE_DATA} that is an unreachable signal addressed to this
     * node: forgets the pinned path to the target and every pin through the
     * failed hop, so the next send computes afresh (LB-R-07).
     *
     * @return whether the frame was a signal, consumed here
     */
    private boolean onRelaySignal(GroupRuntime runtime, Envelope envelope) {
        RelayPipeFrame frame;
        try {
            frame = codec.fromWire(envelope.body(), RelayPipeFrame.class);
        } catch (RuntimeException e) {
            return false;
        }
        if (frame == null || !RELAY_SIGNAL_CAPABILITY.equals(frame.capability()) || frame.payload() == null) {
            return false;
        }
        RelaySignal signal;
        try {
            signal = codec.fromWire(frame.payload(), RelaySignal.class);
        } catch (RuntimeException e) {
            return true;
        }
        if (signal == null || signal.target() == null) {
            return true;
        }
        unreachableSignalsReceived.incrementAndGet();
        Map<PeerId, PinnedPath> pins = pinnedPaths.get(runtime.id());
        if (pins != null) {
            pins.remove(signal.target());
            if (signal.failedHop() != null) {
                // What failed is the step from the signalling relay to the failed hop, not the
                // hop itself: a relay that cannot reach a listening relay says nothing about a
                // path that reaches that relay from elsewhere. Forgetting every pin through the
                // hop dropped working paths whenever a one-hop frame to that relay was refused.
                PeerId signaller = envelope.from();
                pins.entrySet().removeIf(e -> crossesStep(e.getValue().path(), e.getKey(),
                        signaller, signal.failedHop()));
            }
        }
        LOG.log(System.Logger.Level.DEBUG, "relay " + envelope.from().display() + " could not reach "
                + signal.target().display() + " via " + (signal.failedHop() == null ? "?" : signal.failedHop().display())
                + " in group " + runtime.id().value() + "; path forgotten");
        return true;
    }

    /** Whether a pinned path to a target steps straight from one node to another (the target being its last step). */
    static boolean crossesStep(List<PeerId> path, PeerId target, PeerId from, PeerId to) {
        for (int i = 0; i < path.size(); i++) {
            if (path.get(i).equals(from)) {
                PeerId next = i + 1 < path.size() ? path.get(i + 1) : target;
                return next.equals(to);
            }
        }
        return false;
    }

    /** Tells the origin of a dropped path frame which hop failed, at most once per 10 s per pair (LB-R-07). */
    private void signalUnreachable(GroupRuntime runtime, Bodies.RelayFrame relayed, PeerId failedHop) {
        if (!unreachableSignal) {
            return;
        }
        PeerId origin = wire.decode(relayed.frame()).map(Envelope::from).orElse(null);
        if (origin == null || origin.equals(peerId()) || runtime.membership().member(origin).isEmpty()) {
            return;
        }
        String key = runtime.id().value() + "|" + origin.value() + "|" + relayed.target().value();
        long now = localMillis();
        Long last = signalSentAt.get(key);
        if (last != null && now - last < SIGNAL_INTERVAL_MILLIS) {
            return;
        }
        signalSentAt.put(key, now);
        if (signalSentAt.size() > 4_096) {
            signalSentAt.clear(); // a bound, not a policy; the next signal starts the window afresh
        }
        byte[] payload = codec.toBytes(new RelaySignal(relayed.target(), failedHop, "unreachable"));
        send(runtime.id(), origin, Envelope.Kind.PIPE_DATA, new RelayPipeFrame(RELAY_SIGNAL_CAPABILITY, payload));
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
        if (coalescer != null) {
            coalescer.close(); // stop flag, timer cancelled, every outbox flushed once (EL-B-03, FT-6)
        }
        // No run starts after this (NL-5). Let a run in flight finish rather than interrupt it: a run may be
        // persisting state (a rotated content-key ring), and an interrupted write leaves a half-written file behind.
        // Bounded, and never awaited from inside the node's own run, judged by the loop, not a thread name (NL-5).
        loop.close(); // and leaves the watchdog
        try {
            wallStepRegistration.close();
        } catch (Exception e) {
            // a registration's close does not fail
        }
        ai.badmonkey.agentspaces.common.time.Watchdog.Registration fw = flushWatch;
        if (fw != null) {
            fw.close();
        }
        try {
            loop.awaitIdle(Duration.ofSeconds(2));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (AutoCloseable listener : listeners) {
            try {
                listener.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
        admissionMemory.flushAll(); // an admission inside the last gossip period survives the restart
        retirementList.flushAll(time.millis());
        connections.values().forEach(TransportConnection::close);
        connections.clear();
        // Every link this node attached, accepted or dialed, not only the one it sends to each peer on: a peer that
        // dialed this node holds its end open until this end closes, and one left open after this node closed took
        // frames into a reader nobody served. That peer, hearing this identity again from a restart (a new port, a
        // new link), kept sending replies into the dead one (example 34's store restart, 2026-11-19).
        for (TransportConnection link : List.copyOf(attached)) {
            link.close();
        }
        attached.clear();
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

    /**
     * Wires a new connection to this node. A presented chain is judged before the reader starts:
     * the far end writes its first frames the moment its handshake ends, so a reader started
     * first can take a frame off a link presenting a revoked leaf in the instant before the
     * cut. Such a link is closed without ever being read.
     *
     * @return false when the link was cut for its chain
     */
    private boolean attach(TransportConnection connection) {
        attached.add(connection);
        connection.onClose(() -> onConnectionClosed(connection));
        java.security.cert.X509Certificate[] chain = connection.remoteChain();
        if (chain.length > 0) {
            chained.add(connection); // before the checks, so a revocation landing meanwhile still finds it
            if (leafRevokedInAnyGroup(chain[0])) {
                cutForChain(connection);
                return false;
            }
            if (channelTrust != null) {
                judgeChannel(connection, channelTrust.get());
                if (chainCut.contains(connection)) {
                    return false;
                }
            }
        }
        connection.onReceive(frame -> onFrame(connection, frame));
        return true;
    }

    /** A member left a group's view: its per-member state goes, its loss is recorded for retries, its link closes. */
    private void memberLeft(GroupRuntime runtime, GossipBus gossip, PeerId dropped, long nowMillis) {
        Map<PeerId, Long> hashed = hashedPingFrom.get(runtime.id());
        if (hashed != null) {
            hashed.remove(dropped);
        }
        gossip.memberLeft(dropped);
        probePulls.remove(dropped);
        boolean ejected = runtime.membership().consumeEjected(dropped);
        if (!ejected && !runtime.revocations().revoked(dropped)) {
            lostMembers.computeIfAbsent(runtime.id(), g -> new ConcurrentHashMap<>()).put(dropped, nowMillis);
            Long issued = lastAdIssued.get(dropped);
            if (issued != null) {
                lostAdIssued.computeIfAbsent(runtime.id(), g -> new ConcurrentHashMap<>()).put(dropped, issued);
            }
        }
        heldAds.getOrDefault(runtime.id(), Map.of()).remove(dropped);
        TransportConnection stale = connections.get(dropped);
        if (stale != null) {
            dropConnection(dropped, stale);
            stale.close();
        }
    }

    /** The issue time of each peer's newest admitted advertisement, on the issuer's own clock. */
    private final Map<PeerId, Long> lastAdIssued = new ConcurrentHashMap<>();
    /** Per group, the issue time of a lost member's newest advertisement when it was dropped, on its own clock. */
    private final Map<GroupId, Map<PeerId, Long>> lostAdIssued = new ConcurrentHashMap<>();

    /** Cuts a link for the chain it presented; anything it read before the cut is dropped in onFrame. */
    private void cutForChain(TransportConnection connection) {
        anyChainCut = true;
        chainCut.add(connection);
        chained.remove(connection);
        connection.close();
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
                cutForChain(connection);
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
        if (!(trust.status(chain, time.instant())
                instanceof ai.badmonkey.agentspaces.identity.TrustStatus.Revoked revoked)
                || !revoked.authorizesPeerRevocation()) {
            return;
        }
        cutForChain(connection);
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
                            peerId(), runtime.id(), time.instant(), REVOCATION_GOSSIP_TTL,
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
        if (anyChainCut && chainCut.contains(connection)) {
            // Read before the link was cut for its chain, delivered after: the revocation wins.
            framesDropped.incrementAndGet();
            return;
        }
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
        Envelope envelope = decoded.get().envelope();
        long handleStart = System.nanoTime();
        dispatch(envelope, connection);
        long took = System.nanoTime() - handleStart;
        if (took > SLOW_RUN_NANOS) {
            frameHandled(envelope.kind(), envelope.from(), took); // EL-H-11: named by kind and peer
        }
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
        if (refusesFarFuture(envelope)) {
            return;
        }
        if (refusesLogical(envelope)) {
            return;
        }
        hlc.update(envelope.stamp());
        if (connection != null) {
            if (connections.putIfAbsent(envelope.from(), connection) == null) {
                linksChanged(); // a link opened (EL-L-03)
            }
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
        if (envelope.kind() == Envelope.Kind.ACK) {
            runtime.membership().recordAnswered(envelope.from()); // an answer to this node's probe is no contact (O6)
        } else {
            runtime.membership().recordHeard(envelope.from());
        }
        if (wakes(envelope.kind())) {
            loop.wakeGroup(runtime.id().value(), NodeLoop.Kind.OTHER); // a registrant's work (EL-L-03)
        }
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
                Bodies.Ping ping = codec.fromWire(envelope.body(), Bodies.Ping.class);
                onProbePiggyback(runtime, envelope.from(), ping.root(), ping.hashes(), ping.have());
                GossipBus gossip = runtime.gossip();
                // As on the PING: the hashes only while they will be read (IDLE-WORK item 7). That is while this
                // node's anti-entropy has backed off, and also whenever the PING itself carried hashes: a pinger sends
                // them only while its own anti-entropy has backed off, so it will read the ACK's. Without the second
                // case a busy member never answers a quiet one with hashes (the quiet member stretched because the
                // busy stream is not one it holds), and the quiet member's repair of a shared stream it missed waits
                // for its own stretched round instead of the next probe. An older node sends hashes on every PING,
                // so toward it the ACK carries them as before.
                boolean asked = ping.root() != null || ping.hashes() != null;
                if (asked) {
                    hashedPingFrom.computeIfAbsent(runtime.id(), g -> new ConcurrentHashMap<>())
                            .put(envelope.from(), localMillis());
                }
                ProbeDigests probe = asked || probeHashesAlways || runtime.antiEntropyStretched()
                        ? probeDigests(gossip, gossip.audits(envelope.from())) : ProbeDigests.NONE;
                send(runtime.id(), envelope.from(), Envelope.Kind.ACK, new Bodies.Ack(ping.nonce(), null,
                        probe.root(), probe.hashes(), gossip.piggyback(envelope.from())));
            }
            case PING_REQ -> {
                Bodies.PingReq req = codec.fromWire(envelope.body(), Bodies.PingReq.class);
                long relay = relayNonce.nextLong();
                relays.put(relay, new Relay(envelope.from(), req.nonce(), req.target()));
                send(runtime.id(), req.target(), Envelope.Kind.PING, new Bodies.Ping(relay));
            }
            case ACK -> {
                Bodies.Ack ack = codec.fromWire(envelope.body(), Bodies.Ack.class);
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
                    codec.fromWire(envelope.body(), Bodies.Rumor.class));
            case DIGEST -> {
                Bodies.Digest digest = decodeAntiEntropy(runtime, envelope, Bodies.Digest.class);
                if (digest != null) {
                    runtime.gossip().onDigest(envelope.from(), digest);
                }
            }
            case PULL_RESP -> {
                Bodies.PullResp resp = decodeAntiEntropy(runtime, envelope, Bodies.PullResp.class);
                if (resp != null) {
                    runtime.gossip().onPullResp(envelope.from(), resp);
                }
            }
            case HAVE -> runtime.gossip().onHave(envelope.from(),
                    codec.fromWire(envelope.body(), Bodies.Have.class));
            case GRAFT -> runtime.gossip().onGraft(envelope.from(),
                    codec.fromWire(envelope.body(), Bodies.Graft.class));
            case PRUNE -> runtime.gossip().onPrune(envelope.from(),
                    codec.fromWire(envelope.body(), Bodies.Prune.class));
            case QUERY, QUERY_HIT, BLOCK_WANT, BLOCK, PIPE_DATA -> {
                if (envelope.kind() == Envelope.Kind.PIPE_DATA && onRelaySignal(runtime, envelope)) {
                    break; // the node's own signal, consumed here (LB-R-07)
                }
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
            // HLC round six, 2026-11-08 (owner ruling F3): the whole pipe frame, since a record
            // that names one of its fields is no longer the body's encoding
            RelayPipeFrame head = codec.fromWire(envelope.body(), RelayPipeFrame.class);
            return head != null && (RTC_SIGNAL_CAPABILITY.equals(head.capability())
                    || RELAY_SIGNAL_CAPABILITY.equals(head.capability()));
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
                linksChanged();
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
            founding = codec.fromWire(envelope.body(), SignedGroupAdvertisement.class);
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
                ping = codec.fromWire(envelope.body(), Bodies.Ping.class);
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
            rumor = codec.fromWire(envelope.body(), Bodies.Rumor.class);
        } catch (RuntimeException e) {
            return;
        }
        if (rumor == null || !PEER_AD_STREAM.equals(rumor.streamId())) {
            return;
        }
        // An introduction that admits its sender also merges its stamp below,
        // so the stamp is judged before admission: in an OPEN group anyone may
        // introduce itself.
        if (refusesLogical(envelope)) {
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
                if (connections.putIfAbsent(envelope.from(), connection) == null) {
                    linksChanged(); // a link opened (EL-L-03)
                }
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
                        peerId(), runtime.id(), time.instant(), REVOCATION_GOSSIP_TTL,
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
        Instant now = time.instant();
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
                        linksChanged();
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
            return registry.verify(codec.fromWire(payload,
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
            linksChanged();
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
            return registry.verify(codec.fromWire(payload,
                    RevocationRegistry.SignedRevocation.class)) != null;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * The far-future check (ASF-020), with the skew margin of HLC canonical stamps, 2026-11-08
     * (decision B). A stamp past this node's wall time plus {@link HybridLogicalClock#MAX_DRIFT_MILLIS}
     * would drag the clock toward the merge ceiling and win LEASE_RACE tie-breaks, so the frame
     * is dropped. Within {@link #FAR_FUTURE_SKEW_MARGIN_MILLIS} past the ceiling it is what an
     * honest peer sends when a hostile stamp dragged its clock to its own ceiling and this node's
     * wall runs a little behind, so the drop is counted and strikes nobody. Further past it, the
     * frame is fresh and signed (the replay dedup ran already), so it is witnessed, attributable
     * misbehavior: counted and struck.
     *
     * @param envelope a verified, fresh frame from an admitted member
     * @return true when the frame is refused and the caller must drop it
     */
    private boolean refusesFarFuture(Envelope envelope) {
        long ceiling = time.millis() + HybridLogicalClock.MAX_DRIFT_MILLIS;
        long physical = envelope.stamp().physical();
        if (physical <= ceiling) {
            return false;
        }
        if (physical - ceiling <= FAR_FUTURE_SKEW_MARGIN_MILLIS) {
            framesFarFutureSkewed.incrementAndGet();
            return true;
        }
        framesFarFuture.incrementAndGet();
        strike(envelope.from(), "far-future stamp");
        return true;
    }

    /**
     * Refuses a frame whose stamp may not merge into this node's clock, a logical
     * counter above {@link HybridLogicalClock#LOGICAL_LIMIT} (HLC logical counter,
     * 2026-11-08, Rust team report). Such a stamp just under the drift ceiling froze
     * the clock for up to ten minutes, so every frame this node sent read as a
     * replay. The frame is fresh and signed (the replay dedup ran first), so, like
     * the ASF-020 far-future stamp, it is witnessed misbehavior: struck and counted.
     *
     * @param envelope a verified, fresh frame about to merge its stamp
     * @return true when the frame is refused and the caller must drop it
     */
    private boolean refusesLogical(Envelope envelope) {
        if (HybridLogicalClock.isAcceptable(envelope.stamp())) {
            return false;
        }
        framesLogicalRefused.incrementAndGet();
        strike(envelope.from(), "logical counter above " + HybridLogicalClock.LOGICAL_LIMIT);
        return true;
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
            quarantinedUntil.put(peer, localMillis() + QUARANTINE.toMillis());
            TransportConnection connection = connections.remove(peer);
            if (connection != null) {
                linksChanged();
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
        if (localMillis() >= until) {
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
        long ageMillis = time.millis() - envelope.stamp().physical();
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
            hello = codec.fromWire(envelope.body(), Bodies.ChannelHello.class);
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
            long now = localMillis();
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
        session.helloSentAtMillis = localMillis();
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
            hello = codec.fromWire(envelope.body(), Bodies.SessionHello.class);
        } catch (RuntimeException e) {
            return;
        }
        if (hello == null || !peerId().equals(hello.remote()) || hello.ephemeralKey() == null
                || hello.ephemeralKey().length != ai.badmonkey.agentspaces.common.crypto.X25519.RAW_PUBLIC_KEY_LENGTH
                || hello.nonce() == null || hello.nonce().length < 16 || hello.nonce().length > 64) {
            return;
        }
        if (time.millis() - envelope.stamp().physical() > SESSION_HELLO_MAX_AGE_MILLIS) {
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

    /** Whether a space on this node logs its first shortening renewal ({@link Builder#renewShorteningLog}). */
    boolean renewShorteningLog() {
        return renewShorteningLog;
    }

    /** Whether this node advertises {@link #HORIZON_HINT} ({@link Builder#horizon}). */
    public boolean horizonPerEntry() {
        return horizonPerEntry;
    }

    /**
     * Whether a member's current advertisement carries {@link #HORIZON_HINT} at {@link #HORIZON_LEVEL}. A member
     * whose advertisement this node has not received does not, so a newcomer counts as unhinted until its card
     * arrives.
     *
     * @param peer the member
     * @return true when the member advertises the per-entry horizon
     */
    public boolean advertisesHorizon(PeerId peer) {
        return peer != null && horizonPeers.contains(peer);
    }

    /**
     * A counter that moves whenever a peer's {@link #HORIZON_HINT} is learned or lost, so a judgement over the view's
     * hints is recomputed only when it moved (idle-loop O5).
     *
     * @return the hints' version
     */
    public long horizonHintsVersion() {
        return horizonHintsVersion.get();
    }

    /** Makes every probe of this node carry the hashes, as an always-attaching member does (tests of RE-P-03). */
    void probeHashesAlways(boolean always) { // package-private for tests
        this.probeHashesAlways = always;
    }

    /**
     * The digest hashes a probe or its acknowledgement carries (SPEC §5.2, v0.1.17): one per stream, or the root
     * over them above the bus's {@link GossipBus#probeDigestStreams()} streams; both null when the probe carries
     * none, which every receiver reads as no information.
     */
    private record ProbeDigests(byte[] root, Map<String, byte[]> hashes) {
        static final ProbeDigests NONE = new ProbeDigests(null, null);
    }

    /**
     * The probe's hashes of one kind (annex I.6): toward a member on {@code wire.ae=2} each space's trie hash and the
     * root over those, toward any other the text digests' hashes and their root.
     */
    private static ProbeDigests probeDigests(GossipBus gossip, boolean trie) {
        Map<String, byte[]> hashes = gossip.probeHashes(trie);
        return new ProbeDigests(hashes == null ? gossip.rootDigest(trie) : null, hashes);
    }

    /**
     * Decodes an anti-entropy body (RE-T-08, RE-Q-REL-07): one that does not decode is dropped, counted under the
     * bus's {@code undecodable} key and logged once per member, with no strike. The usual cause is a stale card, a
     * sender that read a hint this node no longer carries, which is nobody's misbehaviour; the sender reads the card
     * again before its next round and falls back to the form the card names. Not in {@link #framesDropped()}, which
     * counts the frames that name no verified sender.
     */
    private <T> T decodeAntiEntropy(GroupRuntime runtime, Envelope envelope, Class<T> type) {
        try {
            return codec.fromWire(envelope.body(), type);
        } catch (RuntimeException e) {
            runtime.gossip().undecodable(envelope.from(), envelope.kind().name());
            return null;
        }
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
        boolean trie = gossip.audits(from); // the kind the member used toward this node
        boolean mismatch = hashes != null ? gossip.differs(hashes, trie) : gossip.differs(root, trie);
        if (!mismatch) {
            return;
        }
        long now = localMillis();
        long period = runtime.advertisement().gossip().period().toMillis();
        Long last = probePulls.get(from);
        if (last != null && now - last < period) {
            return;
        }
        probePulls.put(from, now);
        gossip.antiEntropyFromProbe(from, hashes == null);
    }

    private void onRelayFrame(GroupRuntime runtime, Envelope envelope) {
        Bodies.RelayFrame relayed;
        try {
            relayed = codec.fromWire(envelope.body(), Bodies.RelayFrame.class);
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
        RelayCounters counters = relayCounters(runtime.id());
        List<PeerId> path = relayed.path();
        int hop = relayed.hop() == null ? -1 : relayed.hop();
        if (path != null) {
            // A path frame names this relay at its hop; the previous hop sent it; no relay
            // repeats and the target is not a hop (LB-R-03, LB-Q-SEC-09).
            if (!RelayPaths.wellFormed(path, hop, peerId(), relayed.target(), maxPathHops)
                    || (hop > 0 && !envelope.from().equals(path.get(hop - 1)))) {
                counters.droppedBadPath.incrementAndGet();
                return;
            }
        }
        // A relay forwards only to a member of the group the frame names (SPEC §5.4, R2-3.1): a
        // connection this node holds to the target through another group is no reason to carry
        // a stranger's frame to it.
        if (runtime.membership().member(relayed.target()).isEmpty()) {
            counters.droppedNotMember.incrementAndGet();
            return;
        }
        RelayCounters forTarget = counters.target(relayed.target()); // a verified member from here on
        boolean allowed;
        try {
            allowed = relayPolicy.allowKind(runtime.id(), relayedKind(relayed.frame()))
                    && relayPolicy.allow(runtime.id(), envelope.from(), relayed.target(), relayed.frame().length);
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
        if (out != null) {
            try {
                sendOn(out, runtime.id(), relayed.target(), Envelope.Kind.RELAY_FRAME, relayed);
                counters.framesForwarded.incrementAndGet();
                counters.countKind(relayedKind(relayed.frame()), relayed.frame().length);
                counters.bytesForwarded.addAndGet(relayed.frame().length);
                forTarget.framesForwarded.incrementAndGet();
                forTarget.bytesForwarded.addAndGet(relayed.frame().length);
                return;
            } catch (IOException e) {
                dropConnection(relayed.target(), out);
            }
        }
        // Not deliverable from here: the next relay on the path carries it, once (LB-R-03).
        if (path != null && hop + 1 < path.size()) {
            PeerId next = path.get(hop + 1);
            boolean nextIsRelay = runtime.membership().member(next)
                    .map(m -> m.roles().contains(PeerAdvertisement.PeerRole.RELAY)).orElse(false);
            if (nextIsRelay && !quarantined(next)) {
                TransportConnection link = connectionTo(runtime.id(), next);
                if (link != null) {
                    try {
                        sendOn(link, runtime.id(), next, Envelope.Kind.RELAY_FRAME, relayed.nextHop());
                        counters.forwardedToRelay.incrementAndGet();
                        counters.countKind(relayedKind(relayed.frame()), relayed.frame().length);
                        counters.bytesForwarded.addAndGet(relayed.frame().length);
                        forTarget.framesForwarded.incrementAndGet();
                        forTarget.bytesForwarded.addAndGet(relayed.frame().length);
                        return;
                    } catch (IOException e) {
                        dropConnection(next, link);
                    }
                }
            }
            counters.droppedUnreachable.incrementAndGet();
            signalUnreachable(runtime, relayed, next);
            return;
        }
        counters.droppedUnreachable.incrementAndGet();
        signalUnreachable(runtime, relayed, relayed.target());
    }

    /** A relay policy that threw: logged at most once a minute, since it runs once per relayed frame. */
    private void logPolicyFailure(RuntimeException e) {
        long now = localMillis();
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
        if (!attach(dialed)) {
            // The far end presented a revoked chain: never cache the cut link as this peer's channel.
            throw new IOException("the channel certificate " + to.display() + " presented is revoked");
        }
        TransportConnection raced = connections.putIfAbsent(to, dialed);
        if (raced != null) {
            dialed.close();
            return raced;
        }
        linksChanged(); // a link opened (EL-L-03)
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
        /**
         * Decodes and verifies one peers-stream payload end-to-end: the embedded
         * key must hash to the ad's issuer, the signature must verify, and the ad
         * must be unexpired and not future-issued beyond clock skew (ASF-027 — a
         * far-future {@code issued} would otherwise make an ad un-evictable and
         * permanently newest in last-writer-wins).
         */
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
                payloads = codec.fromWire(delta, byte[][].class);
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
                Long issuedAtDrop = lostAdIssued.getOrDefault(runtime.id(), Map.of()).get(ad.issuer());
                Optional<RetirementList.Retired> retiredCard = member ? Optional.empty()
                        : retirementList.get(runtime.id(), ad.issuer());
                if (retiredCard.isPresent() && ad.issued().toEpochMilli() <= retiredCard.get().cardIssuedMillis()) {
                    continue; // a retired member's old card is no evidence it is back
                }
                // Issued no later than the newest card this node held when it dropped the member, both on the
                // issuer's own clock: no evidence it lives. Comparing the issue time with this node's drop time mixed
                // two clocks, so a node whose clock trails the issuer's re-admitted the member from an old copy, again
                // and again (HorizonLaggingCopyTest, 2026-11-18). Without the card's time, the drop time stands in.
                if (!member && droppedAt != null && ad.issued().toEpochMilli()
                        <= (issuedAtDrop != null ? issuedAtDrop : droppedAt)) {
                    continue;
                }
                if (!member && droppedAt == null && runtime.membership().departurePending(ad.issuer())) {
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
     * changes the hints a member last presented (plan EFG §4).
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
        lastAdIssued.merge(ad.issuer(), ad.issued().toEpochMilli(), Math::max);
        Map<PeerId, Long> lost = lostMembers.get(runtime.id());
        if (lost != null) {
            lost.remove(ad.issuer());
        }
        Map<PeerId, Long> lostIssued = lostAdIssued.get(runtime.id());
        if (lostIssued != null) {
            lostIssued.remove(ad.issuer());
        }
        Map<PeerId, Backoff> schedule = lostSchedule.get(runtime.id());
        if (schedule != null) {
            schedule.remove(ad.issuer());
        }
        if (retirementList.remove(runtime.id(), ad.issuer())) {
            LOG.log(System.Logger.Level.INFO, "group " + runtime.id().value() + ": " + ad.issuer().display()
                    + " is back from retirement");
        }
        lastResortSchedule.remove(runtime.id()); // somebody is here: the last resort starts afresh next time
        if (admission && runtime.membership().member(ad.issuer()).isPresent()) {
            lostRetryIntervalMillis.remove(runtime.id()); // somebody answered: retries start fresh
            newlyAdmitted.computeIfAbsent(runtime.id(), g -> ConcurrentHashMap.newKeySet()).add(ad.issuer());
            admittedAtMillis.putIfAbsent(new GroupPeer(runtime.id(), ad.issuer()), localMillis());
            runtime.gossip().topologyChanged();
            runtime.gossip().memberAdmitted(ad.issuer());
            wake(runtime.id()); // the newcomer's introduction and its targeted round (EL-L-07, EL-SP-09)
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
        if ("1".equals(ad.resourceHints().get(RECORDLESS_HINT))) {
            recordlessPeers.add(ad.issuer());
        } else {
            recordlessPeers.remove(ad.issuer());
        }
        // RE-T-03: the form toward a member follows its current card alone, so a card that drops the hint moves the
        // member back to the hashed or text form at the next round, and nothing else moves it (RE-Q-REL-09).
        if (AE_LEVEL.equals(ad.resourceHints().get(AE_HINT))) {
            aePeers.add(ad.issuer());
        } else {
            aePeers.remove(ad.issuer());
        }
        // RE-H-04: the per-entry horizon follows the current card alone, so a card that drops the hint disengages
        // every space whose view holds its member at the next read, and a beacon hint never engages it.
        boolean hintMoved = false;
        if (HORIZON_LEVEL.equals(ad.resourceHints().get(HORIZON_HINT))) {
            if (horizonPeers.add(ad.issuer())) {
                horizonHintsVersion.incrementAndGet();
                hintMoved = true;
            }
        } else {
            if (horizonPeers.remove(ad.issuer())) {
                horizonHintsVersion.incrementAndGet();
                hintMoved = true;
            }
        }
        int level = appCardLevelOf(ad.resourceHints().get(APPCARD_HINT));
        Integer before = level > 1 ? appCardLevels.put(ad.issuer(), level) : appCardLevels.remove(ad.issuer());
        if ((before == null ? 1 : before) != level) {
            appCardVersion.incrementAndGet();
            hintMoved = true;
        }
        if (hintMoved) {
            // A card carried by RUMOR wakes nothing (wakes(kind)), so the spaces' horizon judgement and the AppCard
            // gate hear a moved hint here, not at the backstop (EL-T-07, DW-2).
            loop.wakeGroup(runtime.id().value(), NodeLoop.Kind.OTHER);
        }
        rememberReach(runtime.id(), ad);
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
            announcements.add(seal == null ? signedAd : seal.seal(signedAd, time.instant()));
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
                opened = seal.open(datagram, time.instant()).orElse(null);
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
            // Held past the ad's own expiry for about 1.5 times the longest retry interval (EL-O-11, the TTL rule), so
            // a sender whose beacon has backed off to 20 min never lets this hint lapse between its datagrams.
            // The carrier may deliver before the constructor has assigned the beacon, hence the local read.
            MulticastBeacon heardOn = beacon;
            long now = time.millis();
            List<PeerAdvertisement.Endpoint> endpoints = List.copyOf(ad.endpoints());
            long hold = heardOn == null ? 0 : heardOn.learnedHold().toMillis();
            long expires = Math.max(ad.issued().plus(ad.ttl()).toEpochMilli(), now + hold);
            LearnedEndpoints previous = beaconLearned.computeIfAbsent(ad.group(), g -> new ConcurrentHashMap<>())
                    .put(ad.issuer(), new LearnedEndpoints(endpoints, expires));
            boolean news = previous == null || previous.expiresMillis() <= now
                    || !previous.endpoints().equals(endpoints);
            if (news && heardOn != null) {
                heardOn.succeeded(); // a sender it had no live entry for: the beacon's retries start over (EL-O-11)
            }
        }
        if (!requireAttestation) {
            admitAdvertisement(runtime, ad);
        }
        if (connections.get(ad.issuer()) == null && !ad.endpoints().isEmpty()) {
            introduceTo(runtime.id(), ad.endpoints());
        }
    }

    /** Whether this node forwards a peers-stream payload: verifiable end-to-end, and not a revoked peer's. */
    boolean forwardsPeerAd(RevocationRegistry revocations, byte[] payload) {
        return verifiedPeerAd(payload).filter(ad -> !revocations.revoked(ad.issuer())).isPresent();
    }

    private Optional<PeerAdvertisement> verifiedPeerAd(byte[] payload) {
        return decodeSignedPeerAd(codec, payload)
                .filter(ad -> !ad.expired(time.instant())
                        && !ad.issued().isAfter(time.instant().plus(MAX_AD_ISSUED_SKEW)));
    }

    /**
     * Decodes a signed peer advertisement and verifies it: the wrapper and the advertisement each
     * in their own wire encoding (HLC round six, 2026-11-08, owner rulings F1 and F3), the key the
     * issuer's, and the signature over the raw {@code adBytes}.
     *
     * @param codec   the codec
     * @param payload the signed advertisement as received
     * @return the advertisement, or empty when it does not decode or verify
     */
    static Optional<PeerAdvertisement> decodeSignedPeerAd(CborCodec codec, byte[] payload) {
        SignedPeerAd signed;
        PeerAdvertisement ad;
        try {
            signed = codec.fromWire(payload, SignedPeerAd.class);
            if (signed.adBytes() == null || signed.publicKey() == null || signed.signature() == null
                    || signed.publicKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
                return Optional.empty();
            }
            ad = codec.fromWire(signed.adBytes(), PeerAdvertisement.class);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (!PeerId.fromPublicKey(signed.publicKey()).equals(ad.issuer())
                || !Ed25519.verifyRaw(signed.publicKey(),
                        signed.adBytes(), signed.signature())) {
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
                time.instant());
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

    /**
     * Per group, the content fingerprint and issue millisecond of the last self ad built (§10.16, the same-millisecond
     * audit): a changed self ad never shares its millisecond with the one before it.
     */
    private final Map<GroupId, long[]> lastSelfAdIssue = new ConcurrentHashMap<>();

    /**
     * The issue instant of a self ad (§10.16): now, unless a self ad with different content was already issued in this
     * millisecond or later. Its gossip item id is the issue millisecond, which every peer had seen, and a receiver keeps
     * the card it holds on an equal instant, so such a change reached nobody until the next refresh. The same content
     * in the same millisecond keeps that millisecond: it is the same card.
     */
    private Instant selfAdIssue(GroupId groupId, List<PeerAdvertisement.Endpoint> endpoints,
                                Map<String, String> hints) {
        Instant now = time.instant();
        long fingerprint = Objects.hash(endpoints, roles, hints, peerAdTtl);
        long[] issued = lastSelfAdIssue.compute(groupId, (g, last) -> {
            long ms = now.toEpochMilli();
            if (last == null || ms > last[1]) {
                return new long[] {fingerprint, ms};
            }
            return last[0] == fingerprint ? last : new long[] {fingerprint, last[1] + 1};
        });
        return issued[1] == now.toEpochMilli() ? now : Instant.ofEpochMilli(issued[1]);
    }

    private Bodies.Rumor selfAdRumor(GroupId groupId) {
        List<PeerAdvertisement.Endpoint> endpoints = List.copyOf(advertisedEndpoints);
        Map<String, String> hints = selfHints(groupId);
        PeerAdvertisement ad = new PeerAdvertisement(
                "aspace://" + groupId.value() + "/peer/" + peerId().value(),
                peerId(), groupId, selfAdIssue(groupId, endpoints, hints), peerAdTtl,
                endpoints, roles,
                hints);
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
