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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * The two-channel gossip bus of one group (spec §5.3). The rumor channel pushes
 * new items on named streams with per-item hop decay and id-based deduplication;
 * the anti-entropy channel periodically reconciles registered
 * {@link ReconcilableState}s with one random partner, which guarantees
 * convergence for items the rumor channel missed.
 *
 * <p>Rumor forwarding happens inline on publish and receive; anti-entropy runs on
 * {@link #antiEntropyTick}, which the owning node calls once per
 * {@code GossipParameters.period} of its clock (spec §5.3). Both draw partners
 * from the group's {@link PeerSampler}. Rumors carry the fixed
 * {@link #DEFAULT_HOPS} budget unless a publisher chooses a smaller one.
 *
 * <p>Lazy push (PERF2 §3.4; Leitão, Pereira and Rodrigues' Plumtree): once {@link #lazyPush}
 * is enabled, members that understand it receive payloads eagerly along a spanning tree that
 * prunes itself on duplicates, plus a periodic HAVE announcement of what this node forwarded;
 * a member that hears an announcement for an item it never received asks for it with a GRAFT,
 * which repairs the tree. In steady state each member receives each item once instead of
 * {@code fanout} times. Members that do not understand it, and every member when lazy push is
 * off, get the random fan-out push of before, so the two kinds mix in one group.
 */
public final class GossipBus {

    /** Receives rumor items on a stream. */
    @FunctionalInterface
    public interface RumorHandler {

        /**
         * Handles one deduplicated rumor item.
         *
         * @param from    the forwarding peer (the previous hop, possibly the origin)
         * @param itemId  the item's unique id
         * @param payload the stream-specific payload
         */
        void onItem(PeerId from, String itemId, byte[] payload);
    }

    /** Sends kind-tagged frames to a specific peer; provided by the peer node. */
    public interface FrameSender {

        /**
         * Sends a RUMOR frame.
         *
         * @param to    the destination peer
         * @param rumor the rumor body
         */
        void sendRumor(PeerId to, Bodies.Rumor rumor);

        /**
         * Sends a rumor and says whether it left: a node that cannot reach the peer (no link,
         * no endpoint, a failed dial) returns false, so the fan-out can spend the slot elsewhere.
         */
        default boolean trySendRumor(PeerId to, Bodies.Rumor rumor) {
            sendRumor(to, rumor);
            return true;
        }

        /**
         * Sends a DIGEST frame.
         *
         * @param to      the destination peer
         * @param digests the per-stream digests
         */
        void sendDigest(PeerId to, Bodies.Digest digests);

        /**
         * Sends a PULL_RESP frame.
         *
         * @param to     the destination peer
         * @param deltas the per-stream deltas
         */
        void sendPullResp(PeerId to, Bodies.PullResp deltas);

        /**
         * Sends a HAVE announcement (lazy push). Only called once {@link #lazyPush} is enabled.
         *
         * @param to   the destination
         * @param have the announced items
         */
        default void sendHave(PeerId to, Bodies.Have have) {
        }

        /**
         * Sends a GRAFT request (lazy push).
         *
         * @param to    the announcer being asked
         * @param graft the item asked for
         */
        default void sendGraft(PeerId to, Bodies.Graft graft) {
        }

        /**
         * Sends a PRUNE (lazy push).
         *
         * @param to    the peer being demoted to lazy
         * @param prune the duplicate that caused it
         */
        default void sendPrune(PeerId to, Bodies.Prune prune) {
        }
    }

    private static final int SEEN_CAPACITY = 4096;

    /**
     * The fixed default hop budget of a published rumor (spec §5.3): each
     * forward decrements it and forwarding stops at one. Six hops carry an
     * item across a fleet of several thousand members at fan-out 3
     * (3^6 &gt; 700 reach per origin before dedup), and, because inbound
     * budgets are clamped to this value, it also bounds the amplification any
     * single hostile item can buy (ASF-012). It is a constant, not a
     * {@code GossipParameters} field: the anti-entropy channel guarantees
     * convergence regardless, so the budget only tunes rumor reach.
     */
    public static final int DEFAULT_HOPS = 6;

    private final PeerSampler sampler;
    private final FrameSender sender;
    private final CborCodec codec;
    private final int fanout;
    private final int defaultHops;
    private final java.util.function.LongSupplier clock;
    private final java.util.random.RandomGenerator random;
    private final Map<String, RumorHandler> handlers = new ConcurrentHashMap<>();
    /** Optional per-stream forward filters (ASF-012): an item is re-forwarded
     * only when its payload passes the stream's filter, so an honest node never
     * amplifies content it can already tell is unverifiable. Delivery to the
     * local handler is unaffected — handlers validate for themselves. */
    private final Map<String, java.util.function.Predicate<byte[]>> forwardFilters =
            new ConcurrentHashMap<>();
    /** The partners this node has asked since its last periodic round: the only peers whose
     * PULL_RESP is applied (ASF-003), so an unsolicited delta cannot be injected into
     * replicated state by any other member. The periodic round clears it; a targeted round
     * with a newly admitted member adds to it; a reply consumes its entry. Bounded. */
    private final java.util.Set<PeerId> expectedPullPartners = Collections.synchronizedSet(
            new java.util.LinkedHashSet<>());
    private static final int PULL_PARTNERS_CAPACITY = 16;
    private final Map<String, ReconcilableState> states = new ConcurrentHashMap<>();
    /** The streams in registration order: the order a PULL_RESP's deltas are
     * applied in, whatever order the wire put them in (ISSUE-CanonicalMaps).
     * A node registers its revocation registries before any space attaches,
     * so a late joiner judges the records a pull brings with the revocations
     * the same pull brings, never before them. */
    private final List<String> registrationOrder = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Who delivered an item first and with what hop budget, so a later copy that travelled
     * fewer hops is recognised as the better edge (Plumtree's path optimisation). */
    private record Seen(PeerId from, int hops) {
    }

    private final Map<String, Seen> seen = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Seen> eldest) {
                    return size() > SEEN_CAPACITY;
                }
            });

    // ---- Lazy push (PERF2 §3.4) ----

    /**
     * Announced items this node waits for before it grafts (bounded against a hostile announcer). Each announcer's items
     * count in its own share of this memory, so a flood of announcements pushes out the flooder's own items (swarm
     * retries, flood shares, ROBUSTNESS.md §10.7).
     */
    static final int MISSING_CAPACITY = 256;
    /** Items announced per round at most; older ones are left to anti-entropy. */
    static final int HAVE_CAPACITY = 256;
    /** Forwarded rumors kept to answer a GRAFT. */
    static final int RECENT_CAPACITY = 512;
    /** Items taken from one HAVE at most. */
    static final int HAVE_ITEMS_MAX = 256;
    /**
     * GRAFTs sent per {@link #GRAFT_WINDOW_MILLIS} window at most (per tick before the node's loop, EL-T-01), so an
     * announcer cannot make this node sign thousands of asks however often the bus's participant runs.
     */
    static final int GRAFTS_PER_TICK = 16;
    /*
     * Swarm retries (ROBUSTNESS.md §10.2, sites A1 to A3 of §10.3): the asks for an announced item wait in time, never
     * in ticks, since the scheduler runs missed ticks back to back after a stall. The first ask waits more than an
     * eager push takes, or it would fetch a copy already on its way and the duplicate would prune a sound edge
     * (Plumtree's timer rule); a uniform spread keeps the members that heard one announcement from asking together;
     * later asks back off with the same spread and go round the announcers again while members still announce the item.
     * The base and the spread follow the lateness the node observes (swarm retries, adaptive wait, ROBUSTNESS.md
     * §10.9): the p90 of its graft round trips or the p90 of how long its tree copies came after their first
     * announcement, whichever is longer. Before it has observed either, it waits as the TCP runs of §10.4 chose.
     */
    /**
     * The base of the wait between the first HAVE for an unseen item and its first GRAFT before the node has observed any
     * lateness, and the longest base after: the base is the observed lateness, up to this.
     */
    static final long GRAFT_BASE_MILLIS = 500;
    /** The window the first GRAFT is spread over, uniformly, past the base, before the node has observed any lateness. */
    static final long GRAFT_SPREAD_COLD_MILLIS = 1_000;
    /**
     * The narrowest spread window once the node observes lateness, which widens it to twice the observed lateness: one
     * node tick, so a member that got the item by an earlier ask announces it before the last members ask.
     */
    static final long GRAFT_SPREAD_MIN_MILLIS = 250;
    /** The widest spread window, however late the copies and the answers the node observes. */
    static final long GRAFT_SPREAD_MAX_MILLIS = 4_000;
    /** The wait after the first GRAFT; each later ask doubles it up to {@link #GRAFT_BACKOFF_MAX_MILLIS}. */
    static final long GRAFT_BACKOFF_MILLIS = 1_000;
    /** The longest wait between two asks for one item. */
    static final long GRAFT_BACKOFF_MAX_MILLIS = 8_000;
    /** How long an item stays wanted after its last announcement; a member's probes announce what it forwarded lately. */
    static final long MISSING_HORIZON_MILLIS = 15_000;
    /** Graft round trips, and tree copies' lateness, kept for the p90s that size the wait. */
    static final int GRAFT_RTT_SAMPLES = 64;
    /**
     * Asks in a row an announcer leaves unanswered before a member asks the other announcers first: three, so the rare
     * honest answer that a faster copy overtakes changes nothing.
     */
    static final int UNANSWERED_STRIKES = 3;
    /**
     * A duplicate replaces the first copy's edge only when it travelled this many hops fewer
     * (Plumtree's path optimisation, with its threshold): a one-hop difference is what every
     * triangle produces for items from different sources, and swapping on it leaves the
     * shared partition source-dependent, so a node prunes every edge in turn and never settles.
     */
    static final int PATH_SWAP_THRESHOLD = 2;

    /** An announced item this node lacks, with what its asks need (swarm retries). */
    private static final class Missing {
        final String streamId;
        final byte[] hash;
        /**
         * The first announcer heard: almost always the item's origin, whose announcement reaches every member first. The
         * item counts in this announcer's share of {@link #MISSING_CAPACITY} (flood shares).
         */
        final PeerId firstHeard;
        /** Every member that announced the item, in the order heard. */
        final List<PeerId> announcers = new ArrayList<>();
        /** The announcers asked in the current round; a round ends once each was asked. */
        final java.util.Set<PeerId> asked = new java.util.HashSet<>();
        /** When the first announcement came, from which a tree copy's lateness counts (adaptive wait). */
        final long heardAtMillis;
        long askAtMillis;
        long announcedAtMillis;
        int asks;

        Missing(String streamId, byte[] hash, PeerId firstHeard, long heardAtMillis, long askAtMillis) {
            this.streamId = streamId;
            this.hash = hash;
            this.firstHeard = firstHeard;
            this.heardAtMillis = heardAtMillis;
            this.askAtMillis = askAtMillis;
        }

        /**
         * The announcer to ask next: one not yet asked in this round, never the first heard while another is left, so
         * the members that lack an item spread their asks over its holders instead of all asking its origin; among
         * those, at random, one that has not left {@link #UNANSWERED_STRIKES} asks in a row unanswered while another
         * has not, so an announcer that never answers is asked last (the gate's adversarial review, ROBUSTNESS.md §10.6).
         */
        PeerId next(java.util.random.RandomGenerator random, Map<PeerId, Integer> unanswered) {
            List<PeerId> fresh = new ArrayList<>();
            for (PeerId announcer : announcers) {
                if (!asked.contains(announcer)) {
                    fresh.add(announcer);
                }
            }
            if (fresh.isEmpty()) {
                asked.clear();
                fresh.addAll(announcers);
            }
            if (fresh.size() > 1) {
                fresh.remove(firstHeard);
            }
            List<PeerId> answering = new ArrayList<>();
            for (PeerId announcer : fresh) {
                if (unanswered.getOrDefault(announcer, 0) < UNANSWERED_STRIKES) {
                    answering.add(announcer);
                }
            }
            List<PeerId> pool = answering.isEmpty() ? fresh : answering;
            PeerId target = pool.get(random.nextInt(pool.size()));
            asked.add(target);
            return target;
        }
    }

    /** The asks in flight for an item, by announcer and the clock time each went, until a copy comes (swarm retries). */
    private static final class Grafted {
        final Map<PeerId, Long> sentAtMillis = new HashMap<>();
    }

    /** Whether a member understands HAVE, GRAFT and PRUNE; the owning node judges it from the wire hint. */
    private volatile java.util.function.Predicate<PeerId> lazyCapable;
    /** Members this node holds a live link to, and members with no endpoint of their own (dial-only clients). */
    private volatile java.util.function.Predicate<PeerId> linked = peer -> true;
    private volatile java.util.function.Predicate<PeerId> dialOnly = peer -> false;
    /**
     * For the fan-out's weak slot: the forwarding order stamp ({@link #order}) at which each hint-less member was last
     * pushed to, and the bus clock's millis at which a push to it last failed (srad/agentspaces-event-loop.md
     * EL-T-01). Both were tick counts; the bus's participant no longer runs every 250 ms, so a count of its runs
     * stretched the exclusion to minutes in a quiet group.
     */
    private final Map<PeerId, Long> plainServed = new ConcurrentHashMap<>();
    private final Map<PeerId, Long> plainFailed = new ConcurrentHashMap<>();
    /** How long a failed push keeps a hint-less member out of the strong slots: today's 8 ticks at 250 ms (EL-T-01). */
    static final java.time.Duration PLAIN_FAILURE = java.time.Duration.ofSeconds(2);
    /** Streams whose items are also delivered point to point, so a duplicate on them says nothing about the tree. */
    private final java.util.Set<String> directStreams = ConcurrentHashMap.newKeySet();
    /** Streams pushed to every capable member whatever the tree says: security state never waits on repair. */
    private final java.util.Set<String> eagerStreams = ConcurrentHashMap.newKeySet();

    /**
     * Marks a stream whose items go eagerly to every member, pruned or not (v0.1.17): the
     * revocation streams and the peer advertisement. A revocation that waited on an
     * announcement and a graft would leave a revoked peer connected for a few ticks; these
     * items are rare and small, so every edge carries them.
     *
     * @param streamId the stream
     */
    public void alwaysEager(String streamId) {
        eagerStreams.add(Objects.requireNonNull(streamId, "streamId"));
    }
    /** Capable members this node forwards announcements to instead of payloads: pruned tree edges. */
    private final java.util.Set<PeerId> lazy = ConcurrentHashMap.newKeySet();
    /**
     * An item forwarded, with its place in this bus's forwarding order (srad/agentspaces-event-loop.md EL-T-15): an
     * order, not a time, so two items forwarded in the same millisecond keep theirs, and it moves with every forward
     * however often the bus's own participant runs.
     */
    private record Forwarded(Bodies.HaveItem item, long order, long atMillis) {
    }

    /** The next forwarding order stamp (EL-T-15). */
    private final java.util.concurrent.atomic.AtomicLong order = new java.util.concurrent.atomic.AtomicLong();

    private final Map<String, Forwarded> pendingHaves = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Forwarded> eldest) {
                    return size() > HAVE_CAPACITY;
                }
            });
    private final Map<String, Missing> missing = Collections.synchronizedMap(new LinkedHashMap<>());
    /**
     * The items each announcer's share of {@link #MISSING_CAPACITY} holds, guarded by {@code missing} (swarm retries,
     * flood shares, ROBUSTNESS.md §10.7): a flood of announcements pushes out the flooder's own items.
     */
    private final Map<PeerId, Share> shares = new LinkedHashMap<>();

    /**
     * One announcer's share: the items it alone named, and the items another member named too, each oldest first. The
     * share gives up the second kind last, so a flooder that names an honest member's items cannot carry them out with
     * its flood (the gate of flood shares, ROBUSTNESS.md §10.7).
     */
    private static final class Share {
        final java.util.LinkedHashSet<String> alone = new java.util.LinkedHashSet<>();
        final java.util.LinkedHashSet<String> named = new java.util.LinkedHashSet<>();

        int size() {
            return alone.size() + named.size();
        }

        /** The item the share gives up first: its oldest that no other member named, else its oldest. */
        String first() {
            return (alone.isEmpty() ? named : alone).iterator().next();
        }
    }

    /**
     * The asks in a row each announcer left unanswered, another member's copy coming first, counted up to
     * {@link #UNANSWERED_STRIKES} (swarm retries, the gate's adversarial review, ROBUSTNESS.md §10.6).
     */
    private final Map<PeerId, Integer> unanswered = new ConcurrentHashMap<>();
    private final Map<String, Grafted> grafted = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Grafted> eldest) {
                    return size() > MISSING_CAPACITY;
                }
            });
    /**
     * The members this node asked for an item whose answers are still due after another copy came, by item (asker-only
     * repair, ROBUSTNESS.md §10.9). Such an answer arrives as a duplicate with a budget of one, and it prunes the edge its
     * GRAFT made, as a duplicate answer did when it kept its rumor's budget. Bounded like the asks.
     */
    private final Map<String, java.util.Set<PeerId>> answersDue = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, java.util.Set<PeerId>> eldest) {
                    return size() > MISSING_CAPACITY;
                }
            });
    /** The latest graft round trips in milliseconds, by the member that answered. */
    private final LatenessRing graftRtts = new LatenessRing();
    /**
     * How long the latest tree copies came after their item's first announcement, in milliseconds, by the first announcer
     * (swarm retries, adaptive wait): only copies that came while the item was wanted, from a member this node did not
     * ask, so a copy merely late and a copy that made an ask premature both count, and a repair never does.
     */
    private final LatenessRing copyLateness = new LatenessRing();

    /** The samples of one ring a single member may hold at most: fewer than a tenth of the ring, so one member cannot set its p90. */
    static final int SAMPLES_PER_MEMBER = 6;

    /**
     * The latest lateness samples, each with the member it came from, guarded by itself (swarm retries, adaptive wait, the
     * gate of ROBUSTNESS.md §10.9). A new sample takes the place of the ring's oldest, unless its member already holds
     * {@link #SAMPLES_PER_MEMBER}: then it takes the place of that member's oldest. A member that pushes its items right
     * after announcing them, or answers its asks at once or late on purpose, therefore moves the p90 only together with
     * the other members. The count is a long so it never wraps.
     */
    private static final class LatenessRing {
        private final long[] millis = new long[GRAFT_RTT_SAMPLES];
        private final PeerId[] members = new PeerId[GRAFT_RTT_SAMPLES];
        private final long[] order = new long[GRAFT_RTT_SAMPLES];
        private long count;

        synchronized void add(PeerId member, long sample) {
            int filled = (int) Math.min(count, GRAFT_RTT_SAMPLES);
            int slot = filled < GRAFT_RTT_SAMPLES ? filled : 0;
            int own = -1;
            int held = 0;
            for (int i = 0; i < filled; i++) {
                if (member.equals(members[i])) {
                    held++;
                    own = own < 0 || order[i] < order[own] ? i : own;
                }
                if (filled == GRAFT_RTT_SAMPLES && order[i] < order[slot]) {
                    slot = i;
                }
            }
            if (held >= SAMPLES_PER_MEMBER) {
                slot = own;
            }
            millis[slot] = sample;
            members[slot] = member;
            order[slot] = count++;
        }

        /** The p90 of the samples, or -1 while the ring holds none. */
        synchronized long p90() {
            long[] samples = java.util.Arrays.copyOf(millis, (int) Math.min(count, GRAFT_RTT_SAMPLES));
            if (samples.length == 0) {
                return -1;
            }
            java.util.Arrays.sort(samples);
            return samples[Math.min(samples.length - 1, (int) Math.ceil(samples.length * 0.9) - 1)];
        }
    }
    private final Map<String, Bodies.Rumor> recent = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Bodies.Rumor> eldest) {
                    return size() > RECENT_CAPACITY;
                }
            });
    /**
     * How long after a topology change HAVE goes out every {@link #HAVE_EVERY_MILLIS} (srad/agentspaces-event-loop.md
     * §3.7, EL-T-15): today's 8 ticks at 250 ms, in time, since the bus's own participant no longer runs on a 250 ms
     * grid.
     */
    static final long YOUNG_MILLIS = 2_000;
    /** How long after a topology change HAVE still goes out, once per {@link #SETTLING_EVERY_MILLIS}; then probes carry it. */
    static final long SETTLING_MILLIS = 8_000;
    /** The settling window's pace: once per second of it (today's every fourth tick). */
    static final long SETTLING_EVERY_MILLIS = 1_000;
    /** The young window's pace: at most one HAVE round per 250 ms (today's every tick). */
    static final long HAVE_EVERY_MILLIS = 250;
    /** The window {@link #GRAFTS_PER_TICK} counts in (EL-T-01's 16 per 250 ms window). */
    static final long GRAFT_WINDOW_MILLIS = 250;
    /**
     * The window a probe leaves the items of: today's tick, 250 ms on the bus's clock (srad/agentspaces-event-loop.md
     * §3.7, EL-T-15). Before the event loop a probe left out the items forwarded in the current tick, which gave the
     * tree up to a tick to deliver a fresh item before any probe named it. The outbox rule alone, without that wait,
     * named a busy announcer's burst to members whose tree copies were still on their way:
     * GraftRepairSimulationTest's burst case missed 11,880 small deliveries against its bound of 8,379 (6,000 at
     * HEAD). The window is the same grid of the same width, so the rule is the one the tick gave, in time.
     */
    static final long PIGGYBACK_WINDOW_MILLIS = 250;
    /** Items a probe carries at most. */
    static final int PIGGYBACK_ITEMS = 32;
    /**
     * Streams a probe carries per-stream hashes for by default; above the bound it carries the root hash alone
     * (RE-T-07 of srad/replication-by-entry.md, {@code agentspaces.gossip.probe-hash-streams}). It was sixteen. A
     * hub member that holds many spaces carries about twenty streams, so above sixteen its root never matched that of a
     * member holding fewer streams, and every probe between them opened a targeted round. Per-stream hashes compare
     * only the streams both hold. Every implementation's PING and ACK reader takes a hashes map of any size.
     */
    public static final int PROBE_DIGEST_STREAMS = 32;
    /** The lowest probe-hash bound: zero, the root alone on every probe. */
    public static final int PROBE_DIGEST_STREAMS_FLOOR = 0;
    /**
     * The highest probe-hash bound (RE-F-03): 33 bytes and a stream name per stream, so a probe at the ceiling stays
     * a few tens of kilobytes at most.
     */
    public static final int PROBE_DIGEST_STREAMS_CEILING = 256;
    private volatile int probeDigestStreams = PROBE_DIGEST_STREAMS;
    /** When the tree last changed, on the bus's clock, or {@link Long#MIN_VALUE} before any change (EL-T-15). */
    private volatile long lastTopologyChangeMillis = Long.MIN_VALUE;
    /** Moves with every topology change, so a HAVE round of an earlier window does not pace the new one. */
    private volatile long topologyEpoch;
    // The HAVE rounds' pace; read and written by tick() and nextDueMillis() only, which the node's loop runs serially.
    private long lastHaveMillis = Long.MIN_VALUE;
    private long lastHaveBucket = -1;
    private long lastHaveEpoch = -1;
    private long graftWindowStartMillis = Long.MIN_VALUE;
    private int graftsInWindow;
    /** The forwarding order at each member's admission: a member is told only of items forwarded since. */
    private final Map<PeerId, Long> admittedAtOrder = new ConcurrentHashMap<>();

    /**
     * Notes a member's admission (the owning node calls it): announcements and probe
     * piggybacks toward that member cover only items forwarded after this tick. What this
     * node forwarded before the member joined is history, which the member's anti-entropy
     * brings in registration order, revocations before the records they judge; a graft of an
     * older item would deliver it ahead of that.
     *
     * @param member the member just admitted
     */
    public void memberAdmitted(PeerId member) {
        admittedAtOrder.put(Objects.requireNonNull(member, "member"), order.get());
    }

    /**
     * Notes a member's departure (the owning node calls it): a member gone may have been the
     * tree's only path to others, so every prune is forgotten and items go eagerly to every
     * capable member again until duplicates prune the tree afresh. A burst of duplicates after
     * a rare event, in exchange for delivery that never waits on repair.
     *
     * @param member the member that left the view
     */
    public void memberLeft(PeerId member) {
        admittedAtOrder.remove(Objects.requireNonNull(member, "member"));
        flushedThrough.remove(member);
        forwarderLost(member);
    }

    /**
     * A member this node may have relied on to forward is gone for this node (left the view,
     * or quarantined here): every prune is forgotten, and every other capable member is asked, with
     * a GRAFT that names no item, to make this node eager again, so items flow on every edge
     * at once rather than after the next announcement and graft. One small frame per member,
     * on a rare event.
     *
     * @param lost the member gone, which is asked nothing
     */
    public void forwarderLost(PeerId lost) {
        lazy.clear();
        topologyChanged();
        java.util.function.Predicate<PeerId> capable = lazyCapable;
        if (capable == null) {
            return;
        }
        for (PeerId peer : sampler.members()) {
            if (!peer.equals(lost) && capable.test(peer)) {
                count("GRAFT");
                count("GRAFT_RESET");
                sender.sendGraft(peer, new Bodies.Graft(null, null));
            }
        }
    }

    private long admittedAt(PeerId member) {
        Long stamp = admittedAtOrder.get(member);
        return stamp == null ? Long.MIN_VALUE : stamp;
    }

    /** The items forwarded most recently, with their forwarding order, for the probe piggyback. */
    private final Map<String, Forwarded> recentForwarded = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Forwarded> eldest) {
                    return size() > PIGGYBACK_ITEMS;
                }
            });

    /**
     * Notes a change of the gossip tree or the view (a prune or graft either way, a member
     * admitted or dropped): announcements go out every {@link #HAVE_EVERY_MILLIS} for
     * {@link #YOUNG_MILLIS} after it, then once a second until {@link #SETTLING_MILLIS}, then only
     * on probes, since a member can only be cut off from the tree by such a change. The window
     * starts at the change, on the bus's clock, and a change with announcements queued asks the
     * bus's participant to run (srad/agentspaces-event-loop.md EL-T-15), so a frame-driven prune
     * or graft in a quiet node still announces within {@link #HAVE_EVERY_MILLIS}.
     */
    public void topologyChanged() {
        lastTopologyChangeMillis = clock.getAsLong();
        topologyEpoch++;
        if (!pendingHaves.isEmpty()) {
            hooks.gossipDue();
        }
    }

    /**
     * Sets the test of whether the outbox toward a member holds frames not yet flushed (the node's coalescing,
     * srad/agentspaces-event-loop.md EL-T-15). Without one, every send leaves at once and nothing waits.
     *
     * @param holds true while the outbox toward the member holds frames
     */
    public void outbox(java.util.function.Predicate<PeerId> holds) {
        this.outboxHolds = Objects.requireNonNull(holds, "holds");
    }

    /**
     * Told by the node just before it takes the frames of its outbox toward {@code peer} to send them (EL-T-15):
     * every item stamped before now was enqueued before the take, so it leaves with this flush or left with an
     * earlier one.
     *
     * @param peer the member the outbox goes to
     */
    public void outboxTaking(PeerId peer) {
        flushedThrough.put(peer, order.get());
    }

    private volatile java.util.function.Predicate<PeerId> outboxHolds = peer -> false;
    /** Per member, the forwarding order as of the last take of the outbox toward it (EL-T-15). */
    private final Map<PeerId, Long> flushedThrough = new ConcurrentHashMap<>();

    /**
     * The items this node forwarded since {@code to} was admitted, newest last, for a probe to carry, leaving out
     * any item whose eager copy may still sit in the outbox toward {@code to}: one forwarded after that outbox was
     * last taken, while it holds frames (srad/agentspaces-event-loop.md EL-T-15). The probe is never batched, so it
     * would overtake such a copy and the receiver would graft a copy already on its way. This replaces "before the
     * current tick", which stopped meaning anything once the bus's participant stopped running every 250 ms.
     *
     * <p>Every item stamped before the limit read first was enqueued before it: when the outbox is empty after that
     * read, its copy has left; when it holds frames, only items stamped before its last take have.
     *
     * @param to the member the probe goes to
     */
    public List<Bodies.HaveItem> piggyback(PeerId to) {
        long limit = order.get();
        long since = admittedAt(to);
        if (outboxHolds.test(to)) {
            limit = Math.min(limit, flushedThrough.getOrDefault(to, Long.MIN_VALUE));
        }
        long window = Math.floorDiv(clock.getAsLong(), PIGGYBACK_WINDOW_MILLIS);
        List<Bodies.HaveItem> items = new ArrayList<>();
        synchronized (recentForwarded) {
            for (Forwarded forwarded : recentForwarded.values()) {
                if (forwarded.order() < limit && forwarded.order() >= since
                        && Math.floorDiv(forwarded.atMillis(), PIGGYBACK_WINDOW_MILLIS) < window) {
                    items.add(forwarded.item());
                }
            }
        }
        return items.isEmpty() ? null : items;
    }

    /**
     * Sets how many streams a probe carries per-stream hashes for; above it a probe carries the root alone (RE-T-07,
     * RE-F-03). A runtime setter, so an operator or a later pacing adjuster can move it while the node runs.
     *
     * @param streams the bound, from {@link #PROBE_DIGEST_STREAMS_FLOOR} to {@link #PROBE_DIGEST_STREAMS_CEILING}
     * @throws IllegalArgumentException outside those bounds
     */
    public void probeDigestStreams(int streams) {
        if (streams < PROBE_DIGEST_STREAMS_FLOOR || streams > PROBE_DIGEST_STREAMS_CEILING) {
            throw new IllegalArgumentException("probe-hash-streams must be from " + PROBE_DIGEST_STREAMS_FLOOR
                    + " to " + PROBE_DIGEST_STREAMS_CEILING + ", not " + streams);
        }
        this.probeDigestStreams = streams;
    }

    /** How many streams a probe carries per-stream hashes for ({@link #probeDigestStreams(int)}). */
    public int probeDigestStreams() {
        return probeDigestStreams;
    }

    /**
     * A 32-byte hash per stream for a probe to carry, when the node holds at most
     * {@link #probeDigestStreams()} streams, else null (the root goes instead). A stream's anti-entropy digest is a
     * full state summary that grows with the state (a busy space's runs to a hundred kilobytes), so a probe carries
     * its hash, which is enough to tell two states apart. These are the text digests' hashes, what every member
     * without {@code wire.ae=2} compares.
     */
    public Map<String, byte[]> probeHashes() {
        return probeHashes(false);
    }

    /**
     * {@link #probeHashes()} for one kind of partner (SPEC annex I.6): toward a member whose current card carries
     * {@code wire.ae=2}, each audited stream's hash is its trie's stream hash, and every other stream keeps its text
     * digest's hash, so the receiver compares like with like.
     *
     * @param trie whether the partner is on {@code wire.ae=2}, as {@link #audits} judges it
     */
    public Map<String, byte[]> probeHashes(boolean trie) {
        if (states.isEmpty() || states.size() > probeDigestStreams) {
            return null;
        }
        Map<String, byte[]> hashes = new java.util.TreeMap<>();
        for (Map.Entry<String, ReconcilableState> e : states.entrySet()) {
            hashes.put(e.getKey(), streamHashOf(e.getValue(), trie));
        }
        return hashes;
    }

    /** The per-stream hash toward a partner of one kind: the trie's stream hash where there is one, else the text's. */
    private static byte[] streamHashOf(ReconcilableState state, boolean trie) {
        if (trie) {
            byte[] hash = state.streamHash();
            if (hash != null) {
                return hash;
            }
        }
        return state.digestHash();
    }

    /** The last root hash and what it was taken over, stream ids and their hashes in stream-id order. */
    private record RootHash(List<String> streams, List<byte[]> hashes, byte[] root) {
    }

    /**
     * The last root hash of each kind (IDLE-WORK item 3): while no stream's hash moved, the root is the one already
     * taken, and each stream's hash is itself cached by its state, so a node of twenty unchanged streams answers a probe
     * with twenty cache reads and no hashing at all. A node whose view mixes members with and without
     * {@code wire.ae=2} holds two roots and never compares one kind with the other (annex I.6).
     */
    private volatile RootHash rootHash;
    private volatile RootHash trieRootHash;

    /** SHA-256 over the stream ids and digest hashes in stream-id order; null without streams. */
    public byte[] rootDigest() {
        return rootDigest(false);
    }

    /**
     * {@link #rootDigest()} for one kind of partner: over the hashes {@link #probeHashes(boolean)} would send it.
     *
     * @param trie whether the partner is on {@code wire.ae=2}
     */
    public byte[] rootDigest(boolean trie) {
        if (states.isEmpty()) {
            return null;
        }
        List<String> streams = new ArrayList<>();
        List<byte[]> hashes = new ArrayList<>();
        for (Map.Entry<String, ReconcilableState> e : new java.util.TreeMap<>(states).entrySet()) {
            streams.add(e.getKey());
            hashes.add(streamHashOf(e.getValue(), trie));
        }
        RootHash cached = trie ? trieRootHash : rootHash;
        if (cached != null && cached.streams().equals(streams) && sameHashes(cached.hashes(), hashes)) {
            return cached.root();
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < streams.size(); i++) {
            out.writeBytes(streams.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            out.write(0);
            out.writeBytes(hashes.get(i));
            out.write(0);
        }
        byte[] root = Digests.sha256(out.toByteArray());
        if (trie) {
            trieRootHash = new RootHash(streams, hashes, root);
        } else {
            rootHash = new RootHash(streams, hashes, root);
        }
        return root;
    }

    private static boolean sameHashes(List<byte[]> a, List<byte[]> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!java.util.Arrays.equals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Whether a probe's stream hashes differ from this node's text hashes on any stream both hold. */
    public boolean differs(Map<String, byte[]> theirHashes) {
        return differs(theirHashes, false);
    }

    /**
     * Whether a probe's stream hashes differ from this node's on any stream both hold, compared in the kind the sender
     * used toward this node.
     *
     * @param theirHashes the probe's hashes
     * @param trie        whether the sender is on {@code wire.ae=2}, as {@link #audits} judges it
     */
    public boolean differs(Map<String, byte[]> theirHashes, boolean trie) {
        for (Map.Entry<String, byte[]> e : theirHashes.entrySet()) {
            ReconcilableState state = states.get(e.getKey());
            if (state != null && e.getValue() != null && !sameStream(state, trie, e.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a stream hash a member sent names this node's own set: compared first in the kind this node expects from
     * it, then in the other kind. Which kind the sender used follows from the sender's view of this node's card, not
     * from this node's view of the sender's, and the two disagree until both cards have spread (a member that just
     * joined, a card that changed). Read in one kind alone, two equal sets would compare unequal for as long as the
     * views differ. A hash of one kind never names a different set in the other: a text digest that is not empty
     * ends with an LF and holds no zero byte, while a leaf's preimage never ends with an LF and an inner node's always
     * holds a zero byte (annex I.6), so equal hashes in either kind mean equal sets, short of a SHA-256 collision.
     */
    private static boolean sameStream(ReconcilableState state, boolean trie, byte[] theirs) {
        if (java.util.Arrays.equals(streamHashOf(state, trie), theirs)) {
            return true;
        }
        if (trie) {
            return java.util.Arrays.equals(state.digestHash(), theirs);
        }
        byte[] audited = state.streamHash();
        return audited != null && java.util.Arrays.equals(audited, theirs);
    }

    /** Whether a probe's root hash differs from this node's text root. */
    public boolean differs(byte[] theirRoot) {
        return differs(theirRoot, false);
    }

    /**
     * Whether a probe's root hash differs from this node's root of the kind the sender used toward this node.
     *
     * @param theirRoot the probe's root, or null
     * @param trie      whether the sender is on {@code wire.ae=2}
     */
    public boolean differs(byte[] theirRoot, boolean trie) {
        if (theirRoot == null) {
            return false; // a probe without a root says nothing, so this node's own root is not worth taking
        }
        byte[] own = rootDigest(trie);
        // The other kind's root too, for a sender whose view of this node's card differs from this node's of its card.
        return own != null && !java.util.Arrays.equals(own, theirRoot)
                && !java.util.Arrays.equals(rootDigest(!trie), theirRoot);
    }

    /** Rumor frames sent per stream, for diagnostics ({@code TrafficAudit}). */
    private final Map<String, java.util.concurrent.atomic.AtomicLong> rumorsByStream = new ConcurrentHashMap<>();

    /** How many RUMOR frames this bus has sent per stream, a snapshot. */
    public Map<String, Long> rumorsSentByStream() {
        Map<String, Long> out = new java.util.TreeMap<>();
        rumorsByStream.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    private void countRumor(String streamId) {
        rumorsByStream.computeIfAbsent(streamId, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
    }

    /** Frames the lazy-push machinery sent, by kind name, for tests and diagnostics. */
    private final Map<String, java.util.concurrent.atomic.AtomicLong> lazyCounters = new ConcurrentHashMap<>();

    /**
     * Creates the bus.
     *
     * @param sampler     the group's peer sampler
     * @param sender      the frame sender
     * @param codec       the CBOR codec
     * @param fanout      peers contacted per rumor round
     * @param defaultHops default forward budget for published items
     */
    public GossipBus(PeerSampler sampler, FrameSender sender, CborCodec codec,
                     int fanout, int defaultHops) {
        this(sampler, sender, codec, fanout, defaultHops,
                ai.badmonkey.agentspaces.common.time.TimeSource.system()::monoMillis, new java.util.Random());
    }

    /**
     * Creates the bus with the node's clock and random source, which time and spread the asks for announced items
     * (swarm retries, ROBUSTNESS.md §10.2): a wait reads the clock, never a count of ticks.
     *
     * @param sampler     the group's peer sampler
     * @param sender      the frame sender
     * @param codec       the CBOR codec
     * @param fanout      peers contacted per rumor round
     * @param defaultHops default forward budget for published items
     * @param clock       the node's clock in milliseconds
     * @param random      the node's random source
     */
    public GossipBus(PeerSampler sampler, FrameSender sender, CborCodec codec,
                     int fanout, int defaultHops, java.util.function.LongSupplier clock,
                     java.util.random.RandomGenerator random) {
        this.sampler = Objects.requireNonNull(sampler, "sampler");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        if (fanout <= 0 || defaultHops <= 0) {
            throw new IllegalArgumentException("fanout and defaultHops must be positive");
        }
        this.fanout = fanout;
        this.defaultHops = defaultHops;
        // The audit's own random source spreads the fetch retries' backoff (ROBUSTNESS.md §10). It is seeded apart from
        // the node's source, so drawing from it never moves the partner choices a seeded test relies on.
        this.audit = new TrieAudit(states, sender, codec, clock,
                new java.util.Random(clock.getAsLong() ^ System.identityHashCode(this)));
    }

    /**
     * Registers the handler for a rumor stream. One handler per stream.
     *
     * @param streamId the stream
     * @param handler  the handler
     */
    /**
     * Registers a forward filter for one stream (ASF-012): received items whose
     * payload fails the filter are still delivered locally but never forwarded,
     * so this node's identity and rate budget cannot be borrowed to amplify
     * content that every receiver would reject anyway.
     *
     * @param streamId the stream to filter
     * @param filter   returns whether a payload deserves re-forwarding
     */
    public void forwardFilter(String streamId, java.util.function.Predicate<byte[]> filter) {
        forwardFilters.put(Objects.requireNonNull(streamId, "streamId"),
                Objects.requireNonNull(filter, "filter"));
    }

    /**
     * Registers the handler for a rumor stream and returns the handle that
     * removes it (QA4 A4-9). One handler per stream id per node: a second
     * registration is refused rather than silently replacing the first, which
     * used to leave the earlier registrant deaf with nothing logged. Closing a
     * stale handle after a re-registration removes nothing.
     *
     * @param streamId the stream
     * @param handler  the handler
     * @return a handle that deregisters the handler when closed
     * @throws IllegalStateException when the stream already has a handler
     */
    public AutoCloseable onStream(String streamId, RumorHandler handler) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(handler, "handler");
        if (handlers.putIfAbsent(streamId, handler) != null) {
            throw new IllegalStateException("stream '" + streamId + "' already has a handler on"
                    + " this node; close the earlier registration before registering another");
        }
        return () -> handlers.remove(streamId, handler);
    }

    /**
     * Replaces the handler for a stream, on purpose, returning the one displaced
     * (or null). This is the explicit form of what {@link #onStream} used to do
     * by accident (QA4 A4-9): instrumentation that wants to observe or silence a
     * stream says so here, and production code registers with {@code onStream}
     * and is refused a duplicate. The returned registration removes the
     * replacement only; restoring the original is the caller's business.
     *
     * @param streamId the stream
     * @param handler  the replacement handler
     * @return a handle that removes the replacement when closed
     */
    public AutoCloseable replaceHandler(String streamId, RumorHandler handler) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(handler, "handler");
        handlers.put(streamId, handler);
        return () -> handlers.remove(streamId, handler);
    }

    /**
     * Registers a reconcilable state for anti-entropy under a stream id and
     * returns the handle that removes it (QA4 A4-9). One state per stream id
     * per node; a duplicate is refused, not silently substituted.
     *
     * @param streamId the stream
     * @param state    the state
     * @return a handle that deregisters the state when closed
     * @throws IllegalStateException when the stream already has a state
     */
    public AutoCloseable reconcile(String streamId, ReconcilableState state) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(state, "state");
        if (states.putIfAbsent(streamId, state) != null) {
            throw new IllegalStateException("stream '" + streamId + "' already has a reconcilable"
                    + " state on this node; close the earlier registration before registering"
                    + " another (a second space handle for one name is the usual cause)");
        }
        registrationOrder.add(streamId);
        return () -> {
            if (states.remove(streamId, state)) {
                registrationOrder.remove(streamId);
            }
        };
    }

    /**
     * The stream ids registered on this bus, for handlers or reconcilable
     * states; diagnostics, and the proof a closed registrant is really gone.
     *
     * @return an unmodifiable snapshot
     */
    public java.util.Set<String> streams() {
        java.util.Set<String> all = new java.util.TreeSet<>(handlers.keySet());
        all.addAll(states.keySet());
        return java.util.Collections.unmodifiableSet(all);
    }

    /**
     * Publishes an item: marks it seen locally and pushes it to {@code fanout}
     * sampled members with the default hop budget. The publisher applies the item
     * to its own state before publishing.
     *
     * @param streamId the stream
     * @param itemId   unique item id
     * @param payload  the payload
     */
    /**
     * Set when something this node holds moved since the last anti-entropy round: a rumor published, a first copy
     * of a rumor on a stream it holds, a delta sent, a pulled delta that changed something (IDLE-WORK 5a(b), item 8).
     */
    private volatile boolean activity = true;

    /**
     * Set when any gossip moved since the last tick, held here or not: every first copy of a rumor, every pulled
     * delta, as {@link #activity} was counted before the honest backoff. The membership probe's quiet rule (SPEC
     * §5.2, "no gossip activity since the last tick") reads this one, so a member that only forwards a busy stream
     * it does not hold keeps probing at the base period and the failure detector keeps its speed there; only the
     * anti-entropy period, which is about what this node holds, reads the honest flag.
     */
    private volatile boolean traffic = true;

    /**
     * Whether something this node holds moved since the last call (a rumor published, a first copy received on a
     * stream it holds, a delta sent, or a pulled delta that changed its state). The runtime stretches the
     * anti-entropy period while nothing does.
     */
    public boolean consumeActivity() {
        boolean was = activity;
        activity = false;
        return was;
    }

    /**
     * Whether any gossip moved since the last call, on a stream this node holds or not (see {@link #traffic}). The
     * runtime stretches the membership probe period while nothing does.
     */
    public boolean consumeTraffic() {
        boolean was = traffic;
        traffic = false;
        return was;
    }

    // ---- change signals for the node's loop (srad/agentspaces-event-loop.md DW-5, EL-L-13, EL-T-15)

    /**
     * What the bus tells the owning node's loop. Each hook runs on the thread that raised it (a reader thread, the
     * node's loop, an application thread) and must only wake a participant.
     */
    public interface LoopHooks {
        /** The first activity or traffic after a reader re-armed its edge (EL-L-13). */
        default void activity() {
        }

        /**
         * Work for the bus's own participant that no deadline it reported covers: a topology change with
         * announcements queued, an announcement queued while the tree is young, an announced item to ask for
         * (EL-T-15).
         */
        default void gossipDue() {
        }

        /** A runtime pace moved (RE-F-03): a deadline derived from it may be earlier now. */
        default void paceChanged() {
        }
    }

    private volatile LoopHooks hooks = new LoopHooks() { };
    /** Moves with every mark of {@link #activity}; a reader keeps its own copy and compares (DW-5). */
    private final java.util.concurrent.atomic.AtomicLong activityGeneration =
            new java.util.concurrent.atomic.AtomicLong(1);
    /** Moves with every mark of {@link #traffic} (DW-5). */
    private final java.util.concurrent.atomic.AtomicLong trafficGeneration =
            new java.util.concurrent.atomic.AtomicLong(1);
    /** Armed by a reader that found nothing new: the next activity calls {@link LoopHooks#activity()} once. */
    private final java.util.concurrent.atomic.AtomicBoolean activityEdge = new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicBoolean trafficEdge = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * Sets the hooks the owning node's loop listens on.
     *
     * @param hooks the hooks
     */
    public void loopHooks(LoopHooks hooks) {
        this.hooks = Objects.requireNonNull(hooks, "hooks");
    }

    /**
     * A counter that moves whenever something this node holds moves (see {@link #activity}). A reader keeps the
     * value it last read and compares, so two readers never consume each other's signal (DW-5).
     *
     * @return the generation
     */
    public long activityGeneration() {
        return activityGeneration.get();
    }

    /**
     * A counter that moves whenever any gossip moves (see {@link #traffic}); read as {@link #activityGeneration()}.
     *
     * @return the generation
     */
    public long trafficGeneration() {
        return trafficGeneration.get();
    }

    /**
     * Re-arms the activity edge after a read that found the group quiet (EL-L-13): the next activity wakes the
     * loop once. Activity that came after {@code seen} was read wakes it at once, so none is missed between the read
     * and the arming. A busy group never re-arms, so it pays no wake per rumor.
     *
     * @param seen the generation the reader read
     */
    public void armActivityEdge(long seen) {
        activityEdge.set(true);
        if (activityGeneration.get() != seen && activityEdge.compareAndSet(true, false)) {
            hooks.activity();
        }
    }

    /**
     * Re-arms the traffic edge after a quiet read, as {@link #armActivityEdge} does for activity.
     *
     * @param seen the generation the reader read
     */
    public void armTrafficEdge(long seen) {
        trafficEdge.set(true);
        if (trafficGeneration.get() != seen && trafficEdge.compareAndSet(true, false)) {
            hooks.activity();
        }
    }

    private void markActivity() {
        activity = true;
        activityGeneration.incrementAndGet();
        if (activityEdge.get() && activityEdge.compareAndSet(true, false)) {
            hooks.activity();
        }
    }

    private void markTraffic() {
        traffic = true;
        trafficGeneration.incrementAndGet();
        if (trafficEdge.get() && trafficEdge.compareAndSet(true, false)) {
            hooks.activity();
        }
    }

    /**
     * When the bus's own participant is next due, on the bus's clock (srad/agentspaces-event-loop.md EL-L-01,
     * EL-T-15): the earliest ask for an announced item, the end of a spent graft window, and the next HAVE round
     * while the tree is young or settling and announcements are queued. {@link Long#MAX_VALUE} when nothing is.
     * Read by the node's loop after each run of {@link #tick(long)}, on the same thread.
     *
     * @param nowMillis the bus's clock now
     * @return the next due time in the clock's milliseconds, or {@link Long#MAX_VALUE}
     */
    public long nextDueMillis(long nowMillis) {
        if (lazyCapable == null) {
            return Long.MAX_VALUE;
        }
        long next = Long.MAX_VALUE;
        boolean overdue = false;
        synchronized (missing) {
            for (Missing entry : missing.values()) {
                if (entry.askAtMillis <= nowMillis) {
                    overdue = true;
                } else if (entry.askAtMillis < next) {
                    next = entry.askAtMillis;
                }
            }
        }
        if (overdue) {
            // the window's budget is spent: the rest go when the next window opens
            long open = graftWindowStartMillis == Long.MIN_VALUE ? nowMillis
                    : Math.max(nowMillis, graftWindowStartMillis + GRAFT_WINDOW_MILLIS);
            next = Math.min(next, open);
        }
        long change = lastTopologyChangeMillis;
        if (!pendingHaves.isEmpty() && !lazy.isEmpty() && change != Long.MIN_VALUE) {
            long since = Math.max(0, nowMillis - change);
            if (since < SETTLING_MILLIS) {
                long at;
                boolean sameWindow = lastHaveEpoch == topologyEpoch;
                if (since < YOUNG_MILLIS) {
                    at = !sameWindow || lastHaveMillis == Long.MIN_VALUE ? nowMillis
                            : Math.max(nowMillis, lastHaveMillis + HAVE_EVERY_MILLIS);
                } else {
                    long bucket = since / SETTLING_EVERY_MILLIS;
                    at = !sameWindow || bucket != lastHaveBucket ? nowMillis
                            : change + (bucket + 1) * SETTLING_EVERY_MILLIS;
                }
                next = Math.min(next, at);
            } else {
                next = Math.min(next, nowMillis); // settled: the queue is dropped on the next run
            }
        }
        return next;
    }

    /**
     * Runs one anti-entropy round: send digests to one random partner.
     *
     * @return the partner asked, if any
     */
    public java.util.Optional<PeerId> antiEntropyTick() {
        List<PeerId> partner = sampler.randomMembers(1);
        if (partner.isEmpty() || states.isEmpty()) {
            return java.util.Optional.empty();
        }
        expectedPullPartners.clear();
        syncAuditPaces(); // a lowering whose hold has ended takes effect at the round's start
        digestTo(partner.get(0));
        // The fetch queue's turn (RE-I-03): ids a partner left out go to the next member in view once their backoff
        // passes, and fetch bodies whose answer never came are asked of another member.
        audit.retry(sampler.members());
        return java.util.Optional.of(partner.get(0));
    }

    /**
     * Runs a targeted anti-entropy round with one member, outside the periodic schedule: the
     * owning node calls it for a member it has just admitted, so a newcomer pulls what the
     * fleet already holds (cards, space state) within a tick of joining rather than when the
     * random periodic partner happens to be a member that holds it. Anti-entropy pulls, so
     * the round benefits the asker; each side runs one on admitting the other.
     *
     * @param partner the member to reconcile with
     */
    public void antiEntropyWith(PeerId partner) {
        if (states.isEmpty()) {
            return;
        }
        digestTo(Objects.requireNonNull(partner, "partner"));
    }

    private final java.util.concurrent.atomic.AtomicLong rounds = new java.util.concurrent.atomic.AtomicLong();

    /** How many anti-entropy rounds (periodic and targeted) this bus has started. */
    public long antiEntropyRounds() {
        return rounds.get();
    }

    /**
     * Runs a targeted anti-entropy round that a member's probe asked for (SPEC §5.2): the owning node calls it when a
     * probe's hashes or root differ from this node's, and the bus counts the round by what the probe carried, since a
     * root alone cannot say which stream differs and so costs a round whenever any stream does (RE-F-03).
     *
     * @param partner  the member whose probe differed
     * @param rootOnly whether the probe carried the root hash alone, not a hash per stream
     */
    public void antiEntropyFromProbe(PeerId partner, boolean rootOnly) {
        (rootOnly ? probeRoundsRootOnly : probeRoundsHashes).incrementAndGet();
        antiEntropyWith(partner);
    }

    // ---- pacing signals (RE-F-03, srad/replication-by-entry.md): what anti-entropy costs, for a status page and,
    // later, an adjuster of the paces. Counters only; nothing here changes what the bus sends.
    private final java.util.concurrent.atomic.AtomicLong hashedOpeningsSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong hashedOpeningStreamsSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong wantsReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong wantedStreamsReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong hashedOpeningsReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong wantsSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong probeRoundsHashes = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong probeRoundsRootOnly = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong digestTextBytesSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong digestTextBytesReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespDeltasSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespBytesSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespDeltasReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespBytesReceived = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong pullRespUnsolicited = new java.util.concurrent.atomic.AtomicLong();

    /**
     * The pacing signals so far, a snapshot of counters that each only grow.
     *
     * @param hashedOpeningsSent       hashed anti-entropy openings this bus sent (one per round toward a hinted member)
     * @param hashedOpeningStreamsSent stream hashes those openings carried
     * @param wantsReceived            answers to them naming streams that differ, from the member the round was opened
     *                                 with
     * @param wantedStreamsReceived    streams those answers named
     * @param hashedOpeningsReceived   hashed openings from members
     * @param wantsSent                of those, the ones this bus answered with a want
     * @param probeRoundsHashes        targeted rounds opened because a probe's per-stream hashes differed
     * @param probeRoundsRootOnly      targeted rounds opened because a probe's root differed (it carried no hash per
     *                                 stream)
     * @param digestTextBytesSent      bytes of full digest text sent, in plain rounds and in answer to a want
     * @param digestTextBytesReceived  bytes of full digest text received
     * @param pullRespSent             PULL_RESP frames sent
     * @param pullRespDeltasSent       stream deltas they carried
     * @param pullRespBytesSent        delta bytes they carried
     * @param pullRespReceived         PULL_RESP frames applied (from a member a round was opened with)
     * @param pullRespDeltasReceived   stream deltas they carried
     * @param pullRespBytesReceived    delta bytes they carried
     * @param pullRespUnsolicited      PULL_RESP frames dropped as unsolicited (ASF-003)
     */
    public record PacingSignals(long hashedOpeningsSent, long hashedOpeningStreamsSent,
                                long wantsReceived, long wantedStreamsReceived,
                                long hashedOpeningsReceived, long wantsSent,
                                long probeRoundsHashes, long probeRoundsRootOnly,
                                long digestTextBytesSent, long digestTextBytesReceived,
                                long pullRespSent, long pullRespDeltasSent, long pullRespBytesSent,
                                long pullRespReceived, long pullRespDeltasReceived, long pullRespBytesReceived,
                                long pullRespUnsolicited) {
        /** The share of this bus's hashed openings that were answered with a want; zero before the first opening. */
        public double wantFraction() {
            return hashedOpeningsSent == 0 ? 0.0 : (double) wantsReceived / hashedOpeningsSent;
        }

        /** Every targeted round opened from a probe. */
        public long probeRounds() {
            return probeRoundsHashes + probeRoundsRootOnly;
        }
    }

    /** A snapshot of the pacing signals (RE-F-03). */
    public PacingSignals pacingSignals() {
        return new PacingSignals(hashedOpeningsSent.get(), hashedOpeningStreamsSent.get(),
                wantsReceived.get(), wantedStreamsReceived.get(),
                hashedOpeningsReceived.get(), wantsSent.get(),
                probeRoundsHashes.get(), probeRoundsRootOnly.get(),
                digestTextBytesSent.get(), digestTextBytesReceived.get(),
                pullRespSent.get(), pullRespDeltasSent.get(), pullRespBytesSent.get(),
                pullRespReceived.get(), pullRespDeltasReceived.get(), pullRespBytesReceived.get(),
                pullRespUnsolicited.get());
    }

    private static long byteCount(Map<String, byte[]> digests) {
        long bytes = 0;
        for (byte[] digest : digests.values()) {
            bytes += digest == null ? 0 : digest.length;
        }
        return bytes;
    }

    /** Which members understand the hashed DIGEST form; the owning node judges it from the wire hint. */
    private volatile java.util.function.Predicate<PeerId> hashedDigests = p -> false;

    /**
     * Enables the root-hash-first anti-entropy exchange toward the members {@code capable}
     * accepts (v0.1.17): a round opens with a hash per stream and the full digests follow only
     * for the streams that differ.
     *
     * @param capable which members understand a DIGEST of hashes
     */
    public void hashedDigests(java.util.function.Predicate<PeerId> capable) {
        this.hashedDigests = Objects.requireNonNull(capable, "capable");
    }

    // ---- the form per partner, the paces, and the status figures (srad/replication-by-entry.md RE-F-03,
    // RE-Q-MNT-02, RE-Q-REL-09)

    /** The anti-entropy form a round toward a member takes, read from the member's current card. */
    public enum Form {
        /** {@code wire.ae=2}: trie hashes, the audit, and the fetch by id (annex I.6). */
        TREE("ae"),
        /** {@code wire.hashes=1} alone: a hash per stream, then the text of the streams that differ (v0.1.17). */
        HASHED("hashes"),
        /** Neither hint: the full text digest of every stream (§5.3). */
        TEXT("text");

        private final String label;

        Form(String label) {
            this.label = label;
        }

        /** The short name a status page shows. */
        public String label() {
            return label;
        }
    }

    /** The form of each partner's last round, bounded like the other per-partner maps. */
    private final Map<PeerId, Form> lastForm = java.util.Collections.synchronizedMap(
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<PeerId, Form> eldest) {
                    return size() > 4096;
                }
            });
    private final java.util.concurrent.atomic.AtomicLong formChanges = new java.util.concurrent.atomic.AtomicLong();

    private void noteForm(PeerId partner, Form form) {
        Form before = lastForm.put(partner, form);
        if (before != null && before != form) {
            formChanges.incrementAndGet();
        }
    }

    /**
     * The form a round toward a member would take now, read from its current card: what {@link #audits} and the
     * hashed-digest predicate say. It changes only when the card does (RE-Q-REL-09).
     *
     * @param peer the member
     * @return the form
     */
    public Form formToward(PeerId peer) {
        return audit.audits(peer) ? Form.TREE : hashedDigests.test(peer) ? Form.HASHED : Form.TEXT;
    }

    /** The form of the last round this bus opened with a member, or null before the first. */
    public Form lastFormToward(PeerId peer) {
        return lastForm.get(peer);
    }

    /**
     * How many times a round toward a member took another form than the round before it. Each change follows a change
     * of that member's card, so this never grows faster than the cards do (RE-Q-REL-09).
     */
    public long formChanges() {
        return formChanges.get();
    }

    /** The members in view by the form a round toward each would take now. Every form is listed, zero included. */
    public Map<Form, Integer> partnersByForm() {
        Map<Form, Integer> out = new java.util.EnumMap<>(Form.class);
        for (Form form : Form.values()) {
            out.put(form, 0);
        }
        for (PeerId member : sampler.members()) {
            out.merge(formToward(member), 1, Integer::sum);
        }
        return out;
    }

    /** The least base period of anti-entropy a setter may ask for. */
    public static final long ANTI_ENTROPY_PERIOD_FLOOR_MILLIS = 100;
    /** The greatest base period of anti-entropy a setter may ask for. */
    public static final long ANTI_ENTROPY_PERIOD_CEILING_MILLIS = 60_000;
    /** The greatest stretch of an idle group's anti-entropy, as a multiple of the base period (SPEC §5.3). */
    public static final int ANTI_ENTROPY_STRETCH_CEILING = 32;

    /**
     * The anti-entropy base period: the group's founding names it, a runtime setter moves it (RE-F-03). Its own hold
     * is its value, so a raised period serves at least one period at that value before it comes down.
     */
    private volatile Pace antiEntropyPeriod = new Pace("anti-entropy-period", ANTI_ENTROPY_PERIOD_FLOOR_MILLIS,
            ANTI_ENTROPY_PERIOD_CEILING_MILLIS, 1_000, this::clockMillis, this::paceHoldMillis);
    /** The most an idle group's anti-entropy period stretches to, as a multiple of the base: 1 never stretches. */
    private final Pace antiEntropyStretch = new Pace("anti-entropy-stretch", 1, ANTI_ENTROPY_STRETCH_CEILING,
            ANTI_ENTROPY_STRETCH_CEILING, this::clockMillis, this::paceHoldMillis);
    private final Pace auditBudgetPace = new Pace("audit-budget", TrieAudit.BUDGET_FLOOR, TrieAudit.BUDGET_CEILING,
            TrieAudit.BUDGET_DEFAULT, this::clockMillis, this::paceHoldMillis);
    private final Pace auditSessionsPace = new Pace("audit-sessions", TrieAudit.SESSIONS_FLOOR,
            TrieAudit.SESSIONS_CEILING, TrieAudit.SESSIONS_DEFAULT, this::clockMillis, this::paceHoldMillis);

    private long clockMillis() {
        return clock.getAsLong();
    }

    /** One anti-entropy period, the hold of every pace here (RE-Q-REL-09). */
    private long paceHoldMillis() {
        Pace period = antiEntropyPeriod;
        return period == null ? 1_000 : period.get();
    }

    /** Applies a lowering of the audit's budget or sessions whose hold has ended. */
    private void syncAuditPaces() {
        long budget = auditBudgetPace.get();
        if (audit.budget() != budget) {
            audit.budget(budget);
        }
        int sessions = (int) auditSessionsPace.get();
        if (audit.sessionsMax() != sessions) {
            audit.sessionsMax(sessions);
        }
    }

    /**
     * Starts the anti-entropy period at the group's founding value; the owning runtime calls it once, before any
     * round. The founding value may lie outside the setter's bounds.
     *
     * @param millis the founding's gossip period
     */
    public void antiEntropyBase(long millis) {
        antiEntropyPeriod = new Pace("anti-entropy-period", ANTI_ENTROPY_PERIOD_FLOOR_MILLIS,
                ANTI_ENTROPY_PERIOD_CEILING_MILLIS, millis, this::clockMillis, this::paceHoldMillis);
    }

    /**
     * Sets the anti-entropy base period (RE-F-03); a runtime setter. A raise takes effect at once; a lowering waits
     * until the raised period has served one period (RE-Q-REL-09).
     *
     * @param period from {@value #ANTI_ENTROPY_PERIOD_FLOOR_MILLIS} ms to {@value #ANTI_ENTROPY_PERIOD_CEILING_MILLIS}
     *               ms
     * @throws IllegalArgumentException outside those bounds
     */
    public void antiEntropyPeriod(java.time.Duration period) {
        antiEntropyPeriod.set(period.toMillis());
        hooks.paceChanged();
    }

    /** The anti-entropy base period in effect. */
    public java.time.Duration antiEntropyPeriod() {
        return java.time.Duration.ofMillis(antiEntropyPeriod.get());
    }

    /**
     * Sets how far an idle group's anti-entropy period may stretch, as a multiple of the base (RE-F-03); a runtime
     * setter. The rounds never stop: SPEC §5.3's floor keeps at least one round per 32 periods.
     *
     * @param multiple from 1 (never stretch) to {@value #ANTI_ENTROPY_STRETCH_CEILING}
     * @throws IllegalArgumentException outside those bounds
     */
    public void antiEntropyStretch(int multiple) {
        antiEntropyStretch.set(multiple);
        hooks.paceChanged();
    }

    /** How far an idle group's anti-entropy period may stretch, as a multiple of the base. */
    public int antiEntropyStretch() {
        return (int) antiEntropyStretch.get();
    }

    /** Every pace this bus keeps, as a status page or an adjuster reads them. */
    public List<Pace.Snapshot> paces() {
        syncAuditPaces();
        return List.of(antiEntropyPeriod.snapshot(), antiEntropyStretch.snapshot(), auditBudgetPace.snapshot(),
                auditSessionsPace.snapshot(), new Pace.Snapshot("fetch-window", fetchWindow(),
                        TrieAudit.FETCH_WINDOW_FLOOR, TrieAudit.FETCH_WINDOW_CEILING, null, null, null, 0, 0, 0),
                new Pace.Snapshot("fetch-wait-floor", fetchWaitFloor(), TrieAudit.FETCH_WAIT_FLOOR_MIN_MILLIS,
                        TrieAudit.LATE_COPY_MILLIS, null, null, null, 0, 0, 0));
    }

    /**
     * One stream's status (RE-Q-MNT-02).
     *
     * @param stream        the stream id
     * @param figures       what the stream's state reports, or null for a state that keeps no figures
     * @param auditRounds   audit rounds of the stream as either side
     * @param auditBytes    preimage bytes of the audit's answers for the stream, both ways
     * @param fetchRequeued the stream's fetched ids queued for another member
     */
    public record StreamStatus(String stream, ReconcilableState.Figures figures, long auditRounds, long auditBytes,
                               long fetchRequeued) {
    }

    /**
     * A member's replication status for one group (RE-Q-MNT-02, RE-F-03): the members by form, each stream's figures,
     * the paces, and the signals an adjuster reads. {@link #toMap()} gives it in the shape a feed's {@code /healthz} and
     * the console's panel render.
     *
     * @param partnersByForm the members in view by the form a round toward each takes now
     * @param formChanges    rounds that took another form toward a member than the round before
     * @param streams        each stream's status, in stream-id order
     * @param paces          every pace
     * @param rounds         anti-entropy rounds started
     * @param audit          the audit's signals
     * @param pacing         the pacing signals
     */
    public record ReplicationStatus(Map<Form, Integer> partnersByForm, long formChanges, List<StreamStatus> streams,
                                    List<Pace.Snapshot> paces, long rounds, AuditSignals audit,
                                    PacingSignals pacing) {

        /** Text-digest and {@code PULL_RESP} bytes sent per anti-entropy round, and the audit's answer bytes. */
        public Map<String, Double> costPerRound() {
            double n = Math.max(1, rounds);
            Map<String, Double> out = new java.util.LinkedHashMap<>();
            out.put("digestTextBytes", pacing.digestTextBytesSent() / n);
            out.put("pullRespBytes", pacing.pullRespBytesSent() / n);
            out.put("auditAnswerBytes", (audit.answerBytesSent() + audit.answerBytesReceived()) / n);
            return out;
        }

        /** The status as nested maps and lists of plain values, for JSON. */
        public Map<String, Object> toMap() {
            Map<String, Object> doc = new java.util.LinkedHashMap<>();
            Map<String, Integer> forms = new java.util.LinkedHashMap<>();
            partnersByForm.forEach((form, n) -> forms.put(form.label(), n));
            doc.put("partnersByForm", forms);
            doc.put("formChanges", formChanges);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (StreamStatus stream : streams) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("stream", stream.stream());
                if (stream.figures() != null) {
                    row.put("live", stream.figures().live());
                    row.put("tombstones", stream.figures().tombstones());
                    row.put("tombstonesPastExpiry", new java.util.LinkedHashMap<>(stream.figures().tombstonesPastExpiry()));
                    row.put("trieDepth", stream.figures().trieDepth());
                }
                row.put("auditRounds", stream.auditRounds());
                row.put("auditBytes", stream.auditBytes());
                row.put("fetchRequeued", stream.fetchRequeued());
                rows.add(row);
            }
            doc.put("streams", rows);
            List<Map<String, Object>> paceRows = new ArrayList<>();
            for (Pace.Snapshot pace : paces) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("name", pace.name());
                row.put("value", pace.value());
                row.put("floor", pace.floor());
                row.put("ceiling", pace.ceiling());
                if (pace.pending() != null) {
                    row.put("pending", pace.pending());
                    row.put("heldUntil", pace.heldUntil());
                }
                paceRows.add(row);
            }
            doc.put("paces", paceRows);
            Map<String, Object> signals = new java.util.LinkedHashMap<>();
            signals.put("rounds", rounds);
            signals.put("costPerRound", costPerRound());
            signals.put("wantFraction", pacing.wantFraction());
            signals.put("auditRounds", audit.roundsAudited());
            signals.put("idsFetched", audit.idsFetched());
            signals.put("fetchRequeued", audit.fetchRequeued());
            signals.put("fetchTimedOut", audit.fetchTimedOut());
            signals.put("fetchParked", audit.fetchParked());
            signals.put("fetchQueued", audit.fetchQueued());
            signals.put("refusals", new java.util.TreeMap<>(audit.refusals()));
            signals.put("pullRespUnsolicited", pacing.pullRespUnsolicited());
            doc.put("signals", signals);
            return doc;
        }
    }

    /**
     * This bus's replication status now (RE-Q-MNT-02). Each stream's figures come from its state, which may walk what
     * it holds to answer, so this is for a status page, not for every round.
     *
     * @return the status
     */
    public ReplicationStatus replicationStatus() {
        List<StreamStatus> rows = new ArrayList<>();
        for (Map.Entry<String, ReconcilableState> e : new java.util.TreeMap<>(states).entrySet()) {
            TrieAudit.StreamAudit counts = audit.streamAudit(e.getKey());
            ReconcilableState.Figures figures;
            try {
                figures = e.getValue().figures();
            } catch (RuntimeException failed) {
                figures = null; // a status page shows what it can; a state that cannot answer shows its counters
            }
            rows.add(new StreamStatus(e.getKey(), figures, counts.rounds(),
                    counts.bytesSent() + counts.bytesReceived(), counts.requeued()));
        }
        return new ReplicationStatus(partnersByForm(), formChanges.get(), rows, paces(), rounds.get(),
                auditSignals(), pacingSignals());
    }

    /** The trie audit of annex I.6, both halves; idle until {@link #audit} enables it. */
    private final TrieAudit audit;

    /**
     * Enables the trie audit of SPEC annex I.6 (v0.1.35, {@code wire.ae=2}) toward the members {@code capable}
     * accepts, which the owning node judges from each member's current card, so the form a partner gets changes only
     * when its card does (RE-T-03, RE-Q-REL-09). Toward such a member a round opens with each audited stream's trie
     * hash, the partner answers the streams that differ with their roots, and the two descend to the entries that
     * differ. Null turns the audit off: this node then reads a body carrying the audit's fields as a member without
     * the hint does, and drops it.
     *
     * @param capable which members are on {@code wire.ae=2}, or null for none
     */
    public void audit(java.util.function.Predicate<PeerId> capable) {
        audit.capable(capable);
    }

    /** The trie audit itself, for the tests of its fetch queue in this package. */
    TrieAudit auditForTest() {
        return audit;
    }

    /** Whether this node takes part in the trie audit at all ({@link #audit}). */
    public boolean auditing() {
        return audit.enabled();
    }

    /**
     * Whether this node audits with a member: it takes part, and the member's current card carries
     * {@code wire.ae=2}.
     *
     * @param peer the member
     */
    public boolean audits(PeerId peer) {
        return audit.audits(peer);
    }

    /**
     * Sets how many audit sessions this node keeps open per asker (RE-F-03, {@code agentspaces.gossip.audit-sessions});
     * a runtime setter.
     *
     * @param sessions from 1 to 4, the bound of annex I.6
     * @throws IllegalArgumentException outside those bounds
     */
    public void auditSessions(int sessions) {
        audit.sessionsMax((int) auditSessionsPace.set(sessions));
    }

    /** How many audit sessions this node keeps open per asker. */
    public int auditSessions() {
        syncAuditPaces();
        return audit.sessionsMax();
    }

    /**
     * Sets the budget this node puts on its audit requests (RE-F-03, {@code agentspaces.gossip.audit-budget}); a
     * runtime setter.
     *
     * @param bytes from 4 KiB to 4 MiB
     * @throws IllegalArgumentException outside those bounds
     */
    public void auditBudget(long bytes) {
        audit.budget(auditBudgetPace.set(bytes));
    }

    /** The budget this node puts on its audit requests, in bytes. */
    public long auditBudget() {
        syncAuditPaces();
        return audit.budget();
    }

    /**
     * Sets how many fetch bodies this node keeps in flight per partner (RE-F-03): the next goes out as each answer
     * lands. A runtime setter; the default is four.
     *
     * @param bodies from 1 to 16
     * @throws IllegalArgumentException outside those bounds
     */
    public void fetchWindow(int bodies) {
        audit.fetchWindow(bodies);
    }

    /** How many fetch bodies this node keeps in flight per partner. */
    public int fetchWindow() {
        return audit.fetchWindow();
    }

    /**
     * Sets the shortest wait for a fetch's answer before its ids are asked of another member (RE-F-03). The wait is
     * four times the partner's last answer time when that is longer, up to the time a late copy is still taken. A
     * runtime setter; the default is five seconds.
     *
     * @param millis from 1,000 to 60,000
     * @throws IllegalArgumentException outside those bounds
     */
    public void fetchWaitFloor(long millis) {
        audit.fetchWaitFloor(millis);
    }

    /** The shortest wait for a fetch's answer, in milliseconds. */
    public long fetchWaitFloor() {
        return audit.fetchWaitFloor();
    }

    /**
     * The trie audit's signals: rounds, sessions, nodes and bytes each way, ids fetched, and the refusals and
     * fallbacks by reason (RE-F-03, RE-Q-MNT-02, RE-Q-MNT-03). A snapshot.
     *
     * @return the signals
     */
    public AuditSignals auditSignals() {
        TrieAudit.Signals s = audit.signals();
        return new AuditSignals(s.roundsAudited(), s.sessionsOpened(), s.rootsAnswered(), s.requestsAnswered(),
                s.nodesAnswered(), s.answerBytesSent(), s.requestsSent(), s.answersReceived(),
                s.answerBytesReceived(), s.idsFetched(), s.fetchesAnswered(), s.openSessions(), s.refusals(),
                s.fetchContinued(), s.fetchRequeued(), s.fetchTimedOut(), s.fetchParked(), s.fetchQueueFull(),
                s.fetchQueued(), pullContinued.get(), budgetedAnswers.get(), remainderSent.get(),
                remainderReceived.get(), s.fingerprintsChecked());
    }

    /**
     * The trie audit's signals (see {@link #auditSignals()}).
     *
     * @param roundsAudited       openings this node answered with at least one root
     * @param sessionsOpened      sessions it opened as the partner
     * @param rootsAnswered       roots it answered
     * @param requestsAnswered    request bodies it answered within a session
     * @param nodesAnswered       nodes those answers carried
     * @param answerBytesSent     preimage bytes of every answer it sent
     * @param requestsSent        request bodies it sent as the asker
     * @param answersReceived     answer nodes it accepted as the asker
     * @param answerBytesReceived preimage bytes of those answers
     * @param idsFetched          entry ids it asked for in fetch bodies, each ask counted
     * @param fetchesAnswered     fetch bodies it answered
     * @param openSessions        sessions open now, as the partner
     * @param refusals            refusals and fallbacks by reason, one key each
     * @param fetchContinued      fetched ids a budgeted answer left out, asked of the same partner at once
     * @param fetchRequeued       fetched ids a partner did not hold, or whose answer never came, queued for another
     *                            member after a jittered backoff (RE-I-03)
     * @param fetchTimedOut       fetch bodies whose answer did not come within the wait
     * @param fetchParked         ids every member in view left out, waiting for an audit to name them again
     * @param fetchQueueFull      ids the fetch queue's bound turned away or pushed out
     * @param fetchQueued         ids the fetch queue holds now
     * @param pullContinued       budgeted text pulls continued at once because an answer carried a remainder
     * @param budgetedAnswers     {@code PULL_RESP} answers this node cut to an asker's budget
     * @param remainderSent       states those answers left out for the budget, summed
     * @param remainderReceived   states the answers this node received left out, summed
     * @param fingerprintsChecked fingerprints this node computed as an asker, checking answers and comparing its own
     *                            children with them
     */
    public record AuditSignals(long roundsAudited, long sessionsOpened, long rootsAnswered, long requestsAnswered,
                               long nodesAnswered, long answerBytesSent, long requestsSent, long answersReceived,
                               long answerBytesReceived, long idsFetched, long fetchesAnswered, int openSessions,
                               Map<String, Long> refusals, long fetchContinued, long fetchRequeued,
                               long fetchTimedOut, long fetchParked, long fetchQueueFull, long fetchQueued,
                               long pullContinued, long budgetedAnswers, long remainderSent,
                               long remainderReceived, long fingerprintsChecked) {
    }

    private final java.util.concurrent.atomic.AtomicLong pullContinued = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong budgetedAnswers = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong remainderSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong remainderReceived =
            new java.util.concurrent.atomic.AtomicLong();
    /** Budgeted text pulls continued per partner since this node last opened a round with it; bounded per round. */
    private final Map<PeerId, Integer> continuations = new ConcurrentHashMap<>();

    /**
     * Counts a {@code DIGEST} or {@code PULL_RESP} from a member that did not decode, and logs it once per member
     * (RE-T-08, RE-Q-REL-07, RE-Q-MNT-03). The usual cause is a stale card: the sender read a hint this node no longer
     * carries, or this node carries one its reader does not know. Neither side is at fault, so nobody is struck; the
     * sender reads the card again before its next round and falls back to the form it names.
     *
     * @param from the sender
     * @param kind the frame's kind
     */
    public void undecodable(PeerId from, String kind) {
        audit.refuse(from, kind, "undecodable");
    }

    /** Notes that a partner's PULL_RESP is expected, the eldest expectation going first past the bound. */
    private void expectPull(PeerId partner) {
        synchronized (expectedPullPartners) {
            while (expectedPullPartners.size() >= PULL_PARTNERS_CAPACITY) {
                java.util.Iterator<PeerId> eldest = expectedPullPartners.iterator();
                eldest.next();
                eldest.remove();
            }
            expectedPullPartners.add(partner);
        }
    }

    private void digestTo(PeerId partner) {
        rounds.incrementAndGet();
        continuations.remove(partner);
        expectPull(partner);
        boolean trie = audit.audits(partner);
        noteForm(partner, trie ? Form.TREE : hashedDigests.test(partner) ? Form.HASHED : Form.TEXT);
        if (trie || hashedDigests.test(partner)) {
            // Toward a member on wire.ae=2 each audited stream's hash is its trie's (annex I.6); every other stream,
            // and every stream toward a member on wire.hashes=1 alone, carries its text digest's hash as since v0.1.17.
            Map<String, byte[]> hashes = new HashMap<>();
            for (Map.Entry<String, ReconcilableState> e : states.entrySet()) {
                hashes.put(e.getKey(), streamHashOf(e.getValue(), trie));
            }
            if (trie) {
                audit.opening(partner);
            }
            hashedOpeningsSent.incrementAndGet();
            hashedOpeningStreamsSent.addAndGet(hashes.size());
            sender.sendDigest(partner, new Bodies.Digest(null, hashes, null));
            return;
        }
        Map<String, byte[]> digests = fullDigests(states.keySet());
        digestTextBytesSent.addAndGet(byteCount(digests));
        sender.sendDigest(partner, new Bodies.Digest(digests));
    }

    private Map<String, byte[]> fullDigests(java.util.Collection<String> streams) {
        Map<String, byte[]> digests = new HashMap<>();
        for (String streamId : streams) {
            ReconcilableState state = states.get(streamId);
            if (state != null) {
                digests.put(streamId, state.digest());
            }
        }
        return digests;
    }

    /**
     * Handles an inbound DIGEST. Each field is handled on its own (annex I.6): {@code hashes} opens a round and is
     * answered with {@code want} for the streams compared by their text and with {@code tree} roots for the streams
     * audited by their trie; {@code want} asks this node, which opened the round, for full digests; {@code tree}
     * carries an audit's requests or answers; {@code digests} and {@code ids} draw one {@code PULL_RESP} between them.
     * A body carrying a field of annex I.6 reaches a node that does not audit only from a sender that read a stale
     * card; it is dropped whole and counted, as a member without the hint drops it.
     *
     * @param from   the asking peer
     * @param digest the asker's digests
     */
    public void onDigest(PeerId from, Bodies.Digest digest) {
        if (digest.carriesAudit() && !audit.enabled()) {
            audit.refuse(from, null, "lacks-hint");
            return;
        }
        // Taken before want is answered, since a synchronous transport delivers that answer's PULL_RESP, which settles
        // the round, before this body's tree is read.
        boolean textRound = !audit.audits(from) && expectedPullPartners.contains(from);
        if (digest.hashes() != null) {
            onOpening(from, digest.hashes());
        }
        if (digest.want() != null) {
            // The partner this node opened a round with asks for the full digests of the streams
            // that differ; nobody else is answered, so a stranger cannot make this node send them.
            if (expectedPullPartners.contains(from) && !digest.want().isEmpty()) {
                wantsReceived.incrementAndGet();
                wantedStreamsReceived.addAndGet(digest.want().size());
                Map<String, byte[]> digests = fullDigests(digest.want());
                if (!digests.isEmpty()) {
                    digestTextBytesSent.addAndGet(byteCount(digests));
                    sender.sendDigest(from, textPull(from, digests));
                }
            }
        }
        if (digest.tree() != null && !digest.tree().isEmpty()) {
            List<String> fallback = audit.onTree(from, digest.tree(), digest.budget(), textRound);
            if (!fallback.isEmpty()) {
                // A stream this node keeps no trie for came back as a root: pull it by the text form, as a want would.
                Map<String, byte[]> digests = fullDigests(fallback);
                if (!digests.isEmpty()) {
                    expectPull(from);
                    digestTextBytesSent.addAndGet(byteCount(digests));
                    sender.sendDigest(from, textPull(from, digests));
                }
            }
        }
        // sketch is reserved (annex I.6, RE-F-01): never sent, and ignored when received.
        if (digest.digests() == null && digest.ids() == null) {
            return;
        }
        answerPull(from, digest);
    }

    /** Bytes of a {@code PULL_RESP} body beside its deltas: the record's field names and its map and list heads. */
    private static final long PULL_RESP_BASE = 32;

    /** Bytes one stream adds to a {@code PULL_RESP} body beside its delta: its name twice, a byte head and a count. */
    private static long streamOverhead(String streamId) {
        return 2L * (streamId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 9) + 9;
    }

    /**
     * The {@code PULL_RESP} a body's {@code digests} and {@code ids} draw, in one answer (annex I.6). With no
     * {@code budget} it is as before v0.1.35: every delta whole, no remainder. With one, the answer is cut to the
     * smaller of the asker's budget and this node's own cap: the streams in registration order, so a revocation registry
     * goes before the spaces whose records it judges, each stream's states in the annex's order, the first item always
     * carried; and {@code remainder} counts per stream what was held and left out. A budgeted fetch is always answered,
     * even with nothing held, so the asker learns at once to ask another member.
     */
    private void answerPull(PeerId from, Bodies.Digest digest) {
        Long asked = digest.budget();
        long cap = asked == null ? -1 : Math.min(asked, TrieAudit.BUDGET_CEILING);
        Map<String, byte[]> deltas = new HashMap<>();
        Map<String, Long> remainder = asked == null ? null : new java.util.TreeMap<>();
        long[] used = {PULL_RESP_BASE};
        Map<String, byte[]> texts = digest.digests() == null ? Map.of() : digest.digests();
        Map<String, List<String>> ids = digest.ids() == null ? Map.of() : digest.ids();
        if (!texts.isEmpty()) {
            digestTextBytesReceived.addAndGet(byteCount(texts));
        }
        for (String streamId : answerOrder(texts.keySet(), ids.keySet())) {
            ReconcilableState state = states.get(streamId);
            byte[] text = texts.get(streamId);
            List<String> named = ids.get(streamId);
            boolean fetch = state != null && named != null && !named.isEmpty() && state.streamHash() != null;
            if (state == null || (text == null && !fetch)) {
                if (remainder != null && named != null) {
                    // Nothing of this stream is held here: a zero remainder tells the asker so at once, and it asks
                    // the next member rather than waiting out its fetch.
                    remainder.put(streamId, 0L);
                }
                continue;
            }
            ReconcilableState.Bounded answer;
            if (cap < 0) {
                byte[] delta = text != null ? state.deltaFor(text) : state.statesFor(named);
                answer = new ReconcilableState.Bounded(delta, 0);
            } else {
                long room = cap - used[0] - streamOverhead(streamId);
                answer = text != null ? state.deltaFor(text, room, deltas.isEmpty())
                        : state.statesFor(named, room, deltas.isEmpty());
                remainder.put(streamId, Math.max(0, answer.left()));
                remainderSent.addAndGet(Math.max(0, answer.left()));
            }
            if (answer.delta().length > 0) {
                deltas.put(streamId, answer.delta());
                used[0] += answer.delta().length + streamOverhead(streamId);
            }
            if (text == null) {
                audit.fetchAnswered();
            }
        }
        boolean fetched = !ids.isEmpty();
        if (deltas.isEmpty() && !(fetched && asked != null)) {
            return;
        }
        markActivity();
        markTraffic();
        pullRespSent.incrementAndGet();
        pullRespDeltasSent.addAndGet(deltas.size());
        pullRespBytesSent.addAndGet(byteCount(deltas));
        if (asked != null) {
            budgetedAnswers.incrementAndGet();
        }
        sender.sendPullResp(from, new Bodies.PullResp(deltas, remainder));
    }

    /**
     * The streams of a pull's answer: those this node registered, in registration order, then any other it was asked
     * for. A stream asked both by text and by ids is answered by its text, which covers the ids.
     */
    private List<String> answerOrder(java.util.Set<String> texts, java.util.Set<String> ids) {
        List<String> order = new ArrayList<>();
        for (String streamId : registrationOrder) {
            if (texts.contains(streamId) || ids.contains(streamId)) {
                order.add(streamId);
            }
        }
        java.util.Set<String> rest = new java.util.TreeSet<>(texts);
        rest.addAll(ids);
        rest.removeAll(order);
        order.addAll(rest);
        return order;
    }

    /**
     * A body asking for full text digests: under this node's budget toward a member on {@code wire.ae=2}, which reads a
     * {@code PULL_RESP} carrying {@code remainder}, and as since v0.1.17 toward any other.
     */
    private Bodies.Digest textPull(PeerId to, Map<String, byte[]> digests) {
        if (!audit.audits(to)) {
            return new Bodies.Digest(digests);
        }
        return new Bodies.Digest(digests, null, null, null, null, audit.budget(), null);
    }

    /**
     * The opening of a hashed round: name the streams both hold whose hashes differ. A stream this node audits with
     * the asker is compared by its trie's stream hash and answered with its root, a session permitting; any other is
     * compared by its text digest's hash and named in {@code want}, as since v0.1.17. Both go in one body.
     */
    private void onOpening(PeerId from, Map<String, byte[]> theirs) {
        hashedOpeningsReceived.incrementAndGet();
        boolean trie = audit.audits(from);
        List<String> want = new ArrayList<>();
        List<String> differs = new ArrayList<>();
        List<String> equal = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : new java.util.TreeMap<>(theirs).entrySet()) {
            ReconcilableState state = states.get(e.getKey());
            if (state == null || e.getValue() == null) {
                continue;
            }
            boolean audited = trie && state.streamHash() != null;
            if (!sameStream(state, audited, e.getValue())) {
                (audited ? differs : want).add(e.getKey());
            } else {
                state.matched(from); // the partner holds this stream whole (PD-L-01 evidence)
                if (audited) {
                    equal.add(e.getKey());
                }
            }
        }
        Map<String, List<Bodies.TreeNode>> tree = differs.isEmpty() && equal.isEmpty() ? Map.of()
                : audit.roots(from, differs, equal);
        if (want.isEmpty() && tree.isEmpty()) {
            return;
        }
        if (!want.isEmpty()) {
            wantsSent.incrementAndGet();
        }
        sender.sendDigest(from, new Bodies.Digest(null, null, want.isEmpty() ? null : want,
                tree.isEmpty() ? null : tree, null, null, null));
    }

    /**
     * Handles an inbound PULL_RESP: apply the deltas, in the order the streams
     * were registered, but only from a partner this bus sent a digest or a fetch to and has not heard
     * back from. Anti-entropy is strictly request-response; an unsolicited PULL_RESP is a
     * delta-injection attempt and is dropped (ASF-003). A fetch's answer is awaited for
     * {@link TrieAudit#LATE_COPY_MILLIS}, so a copy that arrives late is still applied, and the merge keeps
     * whichever state is newer (RE-Q-REL-02).
     *
     * @param from the sending peer
     * @param resp the deltas
     */
    public void onPullResp(PeerId from, Bodies.PullResp resp) {
        Map<String, byte[]> deltas = resp.deltas() == null ? Map.of() : resp.deltas();
        java.util.Set<String> named = new java.util.HashSet<>(deltas.keySet());
        if (resp.remainder() != null) {
            named.addAll(resp.remainder().keySet());
        }
        // A fetch's answer first: it names the stream of a body in flight, which no round's answer does, since this
        // node asks a space it audits by ids alone. Anything else must answer a round this node opened.
        String fetchStream = audit.fetchAnswer(from, named);
        if (fetchStream == null && !expectedPullPartners.remove(from)) {
            pullRespUnsolicited.incrementAndGet();
            return;
        }
        pullRespReceived.incrementAndGet();
        pullRespDeltasReceived.addAndGet(deltas.size());
        pullRespBytesReceived.addAndGet(byteCount(deltas));
        if (resp.remainder() != null) {
            for (Long left : resp.remainder().values()) {
                remainderReceived.addAndGet(left);
            }
        }
        java.util.Set<String> received = java.util.Set.of();
        // In registration order, not wire order: the canonical map order of wire
        // v3 sorts shorter stream ids first, which would apply "space:x" before
        // "credential-revocations"; the revocation registries registered first.
        for (String streamId : registrationOrder) {
            byte[] delta = deltas.get(streamId);
            ReconcilableState state = delta == null ? null : states.get(streamId);
            // Activity only when the delta changed something (IDLE-WORK item 8): a partner re-offering states
            // this replica refuses or already holds is no news, and counting it held a quiet group's anti-entropy
            // at its base period for good.
            if (state != null) {
                markTraffic();
                boolean changed;
                if (streamId.equals(fetchStream)) {
                    ReconcilableState.Applied applied = state.applyDeltaNamed(delta);
                    changed = applied.changed();
                    received = applied.ids();
                } else {
                    changed = state.applyDeltaChanged(delta);
                }
                if (changed) {
                    markActivity();
                }
            }
        }
        if (fetchStream != null) {
            // Which body the answer settles is read from what it carried (TrieAudit.settle), not from arrival order.
            TrieAudit.Batch fetch = audit.settle(from, fetchStream, received);
            if (fetch != null) {
                Long left = resp.remainder() == null ? null : resp.remainder().get(fetchStream);
                audit.fetched(from, fetch, received, left == null ? 0 : left);
            }
        } else if (resp.remainder() != null) {
            continuePull(from, resp.remainder());
        }
    }

    /** Budgeted text pulls a partner continues at once in one round, at most: the session bound of annex I.6. */
    static final int PULL_CONTINUATIONS_MAX = 64;

    /**
     * A budgeted text pull came back with a remainder: continue it at once rather than a round later (RE-Q-PRF-03),
     * with this node's own text digest under the same budget, which now lists what the answer brought, so the partner
     * sends the next states. A stream this node audits continues by an audit instead, whose fetch is bounded per body,
     * since the text of a large space could pass the frame. At most {@link #PULL_CONTINUATIONS_MAX} per partner per
     * round, after which the next round carries on.
     */
    private void continuePull(PeerId from, Map<String, Long> remainder) {
        List<String> text = new ArrayList<>();
        boolean reopen = false;
        for (Map.Entry<String, Long> e : remainder.entrySet()) {
            ReconcilableState state = states.get(e.getKey());
            if (state == null || e.getValue() == null || e.getValue() <= 0) {
                continue;
            }
            if (audit.audits(from) && state.streamHash() != null) {
                reopen = true;
            } else {
                text.add(e.getKey());
            }
        }
        if (text.isEmpty() && !reopen) {
            return;
        }
        if (continuations.merge(from, 1, Integer::sum) > PULL_CONTINUATIONS_MAX) {
            return; // the next round carries on
        }
        pullContinued.incrementAndGet();
        if (!text.isEmpty()) {
            Map<String, byte[]> digests = fullDigests(text);
            expectPull(from);
            digestTextBytesSent.addAndGet(byteCount(digests));
            sender.sendDigest(from, textPull(from, digests));
        }
        if (reopen) {
            int sofar = continuations.getOrDefault(from, 0);
            digestTo(from); // an audit opening; it resets the count, so keep it
            continuations.put(from, sofar);
        }
    }

    /**
     * Delivers a rumor from a peer outside the membership view: hand it to the
     * stream's handler, but never forward it, and never mark it seen — a
     * delivery the handler rejects must not dedup away a later legitimate copy
     * of the same item from an admitted member. This is the one gossip path
     * open to non-members (the bootstrap join, ASF-003); the caller rate-limits
     * it, and the peer-ad handler verifies each ad's own signature and
     * admission policy, so the sender's standing adds nothing.
     *
     * @param from  the sending peer
     * @param rumor the rumor body
     */
    public void onBootstrapRumor(PeerId from, Bodies.Rumor rumor) {
        RumorHandler handler = handlers.get(rumor.streamId());
        if (handler != null) {
            handler.onItem(from, rumor.itemId(), rumor.payload());
        }
    }

    public void publish(String streamId, String itemId, byte[] payload) {
        if (!directStreams.contains(streamId)) {
            markActivity(); // a direct stream (the peer advertisement) is liveness, not data
            markTraffic();
        }
        byte[] hash = Digests.sha256(payload);
        String key = key(streamId, hash);
        seen.put(key, new Seen(null, defaultHops));
        Bodies.Rumor rumor = new Bodies.Rumor(streamId, itemId, defaultHops, payload);
        forward(key, hash, rumor, null);
    }

    /**
     * Handles an inbound RUMOR frame: deduplicate, deliver, and forward while the
     * hop budget lasts. Under lazy push a duplicate from a capable member demotes that member
     * to lazy with a PRUNE (the edge it came over is redundant), and a first copy from a lazy
     * member makes it eager again.
     *
     * @param from  the sending peer
     * @param rumor the rumor body
     */
    public void onRumor(PeerId from, Bodies.Rumor rumor) {
        byte[] hash = Digests.sha256(rumor.payload());
        String key = key(rumor.streamId(), hash);
        Seen first = seen.putIfAbsent(key, new Seen(from, rumor.hopsRemaining()));
        if (first != null) {
            if (!treeWorthwhile()) {
                return; // a small group: the fan-out reaches everyone in one hop, and the tree would only add fragility
            }
            // A duplicate over a tree edge means an edge is redundant (Plumtree PRUNE). A copy
            // delivered point to point (hop budget 1: an introduction, a refresh) and a stream the
            // node also delivers directly are not evidence about the tree, and neither is the
            // asker's forward of a GRAFT answer, which carries a budget of one too (asker-only
            // repair, ROBUSTNESS.md §10.9): a repaired item comes late by nature, and those copies
            // say nothing about which edge is redundant. An answer that comes after another copy
            // does prune the edge its GRAFT made, as it did when it kept its rumor's budget. Which
            // edge goes: the duplicate's, unless it travelled markedly fewer hops than the first
            // copy (PATH_SWAP_THRESHOLD), in which case the first sender's edge is the longer
            // path and is pruned instead. The partition is shared by every source, so a node
            // keeps whichever edge delivered first and only swaps for a clearly shorter path,
            // with one exception: the origin's own push is the shortest path there is, so a
            // relayed copy that beat it (an asynchronous fabric does that at both receivers at
            // once, which would orphan the origin) never prunes the origin's edge.
            java.util.function.Predicate<PeerId> capable = lazyCapable;
            if (first.from() == null) {
                // An echo of this node's own item: the sender's path back here is a cycle, but
                // this node's direct edge to the sender is the shortest path there is, and the
                // sender is about to receive (or has received) this node's own push, which it
                // will judge the better copy. Pruning here would cut the one-hop edge in favour
                // of the cycle, which a synchronous fabric produces before the fan-out finishes.
                return;
            }
            boolean dueAnswer = rumor.hopsRemaining() == 1 && answerDue(key, from);
            if (capable != null && capable.test(from) && (rumor.hopsRemaining() > 1 || dueAnswer)
                    && !directStreams.contains(rumor.streamId())) {
                PeerId pruned = from;
                boolean directFromOrigin = rumor.hopsRemaining() >= defaultHops && first.hops() < defaultHops;
                if (first.from() != null && !first.from().equals(from) && capable.test(first.from())
                        && (directFromOrigin || rumor.hopsRemaining() - first.hops() >= PATH_SWAP_THRESHOLD)) {
                    pruned = first.from();
                    lazy.remove(from);
                    seen.put(key, new Seen(from, rumor.hopsRemaining()));
                }
                lazy.add(pruned);
                topologyChanged();
                count("PRUNE");
                sender.sendPrune(pruned, new Bodies.Prune(rumor.streamId(), hash));
            }
            return;
        }
        // A first copy of an item on a stream this node holds is news. One on a stream it does not hold (another
        // member's space this node never opened) is still forwarded below exactly as before, but nothing here
        // changed, so it does not keep this node's anti-entropy at its base period (IDLE-WORK 5a(b)): a single
        // busy space held by a few members used to hold every member of the group at full rate.
        if (!directStreams.contains(rumor.streamId())) {
            markTraffic();
            if (handlers.containsKey(rumor.streamId()) || states.containsKey(rumor.streamId())) {
                markActivity();
            }
        }
        Missing arrived;
        synchronized (missing) {
            arrived = missing.remove(key);
            if (arrived != null) {
                leaveShare(key, arrived);
            }
        }
        boolean treePush = rumor.hopsRemaining() > 1 && !directStreams.contains(rumor.streamId());
        boolean asked = noteCopy(key, from, arrived, treePush);
        // A first copy over a pruned edge restores it, when it was a tree push: a copy delivered
        // point to point (hop budget one: a refresh, an introduction, a graft answer) or on a
        // direct stream says nothing about the tree, just as such a duplicate never prunes. The
        // GRAFT itself made the asker's edge to the member it asked eager at both ends.
        if (treePush) {
            lazy.remove(from);
        }
        RumorHandler handler = handlers.get(rumor.streamId());
        if (handler != null) {
            handler.onItem(from, rumor.itemId(), rumor.payload());
        }
        java.util.function.Predicate<byte[]> filter = forwardFilters.get(rumor.streamId());
        if (filter != null && !filter.test(rumor.payload())) {
            return; // ASF-012: never amplify content this node can reject itself
        }
        // ASF-012: the inbound hop budget is clamped, so a hostile
        // hopsRemaining cannot buy amplification beyond this bus's own default.
        int hops = Math.min(defaultHops, rumor.hopsRemaining());
        if (hops > 1) {
            forward(key, hash, new Bodies.Rumor(rumor.streamId(), rumor.itemId(),
                    hops - 1, rumor.payload()), from);
        } else if ((asked || arrived != null && arrived.asks > 0) && !directStreams.contains(rumor.streamId())) {
            // Asker-only repair (ROBUSTNESS.md §10.9): the answer to this node's GRAFT ended its
            // budget here, and this node forwards the item along its own eager edges as usual.
            // Each copy carries the same budget of one, so it ends at the member it reaches and
            // prunes nothing there, and the lazy members hear of the item from this node. The
            // record of the asks in flight is bounded, and a flood's asks can push one out before
            // a slow answer comes, so the item's own entry among the wanted items counts too (the gate).
            forward(key, hash, new Bodies.Rumor(rumor.streamId(), rumor.itemId(), 1, rumor.payload()), from);
        }
    }

    /**
     * Enables lazy push toward the members {@code capable} accepts (the owning node passes the
     * members whose advertisement carries the {@code wire.tree} hint). Announcements of what
     * this node forwarded go out on every {@link #tick} to a few lazy members.
     *
     * @param capable which members understand HAVE, GRAFT and PRUNE
     */
    public void lazyPush(java.util.function.Predicate<PeerId> capable) {
        this.lazyCapable = Objects.requireNonNull(capable, "capable");
    }

    /**
     * Tells the fan-out toward hint-less members what the node knows about reaching them:
     * {@code linked} holds a live link now, {@code dialOnly} has no endpoint of its own (a
     * client behind its seed), so only a linked node can push to it at all.
     */
    public void plainReach(java.util.function.Predicate<PeerId> linked, java.util.function.Predicate<PeerId> dialOnly) {
        this.linked = Objects.requireNonNull(linked, "linked");
        this.dialOnly = Objects.requireNonNull(dialOnly, "dialOnly");
    }

    /**
     * Marks a stream whose items this node also delivers point to point (the peer
     * advertisement: introduced directly on a dial and flooded on the interval), so a
     * duplicate on it never prunes a tree edge.
     *
     * @param streamId the stream
     */
    public void directDelivery(String streamId) {
        directStreams.add(Objects.requireNonNull(streamId, "streamId"));
    }

    /**
     * Whether the tree is worth pruning for: only once the group has more members than the
     * fan-out reaches in one hop. Below that every member pushes to every other, each item
     * arrives at most {@code fanout} times, and no bid or claim ever waits on a repair; a
     * settle window of a few ticks then means what it says (ROBUSTNESS.md).
     */
    boolean treeWorthwhile() {
        return sampler.members().size() > fanout;
    }

    /** The capable members this node currently announces to rather than pushes to. */
    public java.util.Set<PeerId> lazyPeers() {
        return java.util.Set.copyOf(lazy);
    }

    /** How many lazy-push frames of a kind ({@code HAVE}, {@code GRAFT}, {@code PRUNE}) this bus has sent. */
    public long lazyFramesSent(String kind) {
        java.util.concurrent.atomic.AtomicLong counter = lazyCounters.get(kind);
        return counter == null ? 0 : counter.get();
    }

    /** The counters of the asks for announced items (swarm retries, ROBUSTNESS.md §10.2), by name. */
    static final List<String> REPAIR_COUNTERS = List.of("GRAFT_FIRST_HEARD", "GRAFT_RETRY", "GRAFT_PREMATURE",
            "GRAFT_RESET", "GRAFT_DROPPED", "GRAFT_EVICTED", "GRAFT_EVICTED_OWN", "GRAFT_EVICTED_OTHER");

    /**
     * How this node asked for announced items it lacked, a snapshot (swarm retries, ROBUSTNESS.md §10.2):
     * {@code GRAFT_FIRST_HEARD} asks sent to the first announcer heard, almost always the item's origin;
     * {@code GRAFT_RETRY} asks after the first; {@code GRAFT_PREMATURE} asks whose item came from another member
     * anyway; {@code GRAFT_RESET} the GRAFTs naming no item that a lost forwarder sent; {@code GRAFT_DROPPED} items
     * given up once no member announced them any more; {@code GRAFT_EVICTED} items given up because newer
     * announcements filled the {@link #MISSING_CAPACITY} items this node tracks, by cause: {@code GRAFT_EVICTED_OWN}
     * when an item of the same share took the place, {@code GRAFT_EVICTED_OTHER} when an announcer with a smaller share
     * took it (flood shares, ROBUSTNESS.md §10.7).
     *
     * @return every counter, zero included
     */
    public Map<String, Long> repairCounters() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String name : REPAIR_COUNTERS) {
            java.util.concurrent.atomic.AtomicLong counter = lazyCounters.get(name);
            out.put(name, counter == null ? 0 : counter.get());
        }
        return out;
    }

    private void count(String kind) {
        lazyCounters.computeIfAbsent(kind, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
    }

    /**
     * Handles an inbound HAVE: items this node has seen are ignored; the rest are noted with
     * their announcer and asked for once the spread wait of swarm retries ends, if they are
     * still missing then (ROBUSTNESS.md §10.2). Each announcement keeps its item wanted for
     * {@link #MISSING_HORIZON_MILLIS} more.
     *
     * @param from the announcing member
     * @param have the announced items
     */
    public void onHave(PeerId from, Bodies.Have have) {
        long now = clock.getAsLong();
        int taken = 0;
        boolean asksAdded = false;
        for (Bodies.HaveItem item : have.items()) {
            if (++taken > HAVE_ITEMS_MAX || item.streamId() == null || item.hash() == null) {
                break;
            }
            String key = key(item.streamId(), item.hash());
            if (seen.containsKey(key)) {
                continue;
            }
            synchronized (missing) {
                Missing entry = missing.get(key);
                if (entry == null) {
                    if (missing.size() >= MISSING_CAPACITY) {
                        makeRoom(from);
                    }
                    entry = new Missing(item.streamId(), item.hash(), from, now, now + base() + spread());
                    missing.put(key, entry);
                    joinShare(key, entry);
                    asksAdded = true;
                } else if (!entry.announcers.contains(from) && shares.get(entry.firstHeard).alone.remove(key)) {
                    shares.get(entry.firstHeard).named.add(key); // another member named it: its share gives it up last
                }
                entry.announcedAtMillis = now;
                if (!entry.announcers.contains(from)) {
                    entry.announcers.add(from);
                }
            }
        }
        if (asksAdded) {
            hooks.gossipDue(); // a first ask the bus's participant has no deadline for yet (EL-T-15)
        }
    }

    /**
     * Makes room for one more item from {@code announcer} in the full memory of announced items (swarm retries, flood
     * shares, ROBUSTNESS.md §10.7). An announcer that would then hold more than an even split of
     * {@link #MISSING_CAPACITY} among the announcers holding items gives up an item of its own share. Any other
     * announcer takes the room from the largest share. Either share gives up its oldest item that no other member named,
     * and its oldest item only when another member named every one. A flood of announcements then pushes out the
     * flooder's own items, one announcer alone may fill the whole memory, and every announcer may grow to an even split
     * whoever else floods. The caller holds the lock of {@code missing}.
     */
    private void makeRoom(PeerId announcer) {
        int announcers = shares.size() + (shares.containsKey(announcer) ? 0 : 1);
        PeerId giver = announcer;
        if (held(announcer) == 0 || (long) (held(announcer) + 1) * announcers <= MISSING_CAPACITY) {
            int most = 0;
            for (Map.Entry<PeerId, Share> share : shares.entrySet()) {
                if (!share.getKey().equals(announcer) && share.getValue().size() > most) {
                    giver = share.getKey();
                    most = share.getValue().size();
                }
            }
        }
        String given = shares.get(giver).first();
        leaveShare(given, missing.remove(given));
        count("GRAFT_EVICTED");
        count(giver.equals(announcer) ? "GRAFT_EVICTED_OWN" : "GRAFT_EVICTED_OTHER");
    }

    /** How many items an announcer's share holds; the caller holds the lock of {@code missing}. */
    private int held(PeerId announcer) {
        Share share = shares.get(announcer);
        return share == null ? 0 : share.size();
    }

    /** Counts an item in its first announcer's share, as its newest; the caller holds the lock of {@code missing}. */
    private void joinShare(String key, Missing entry) {
        shares.computeIfAbsent(entry.firstHeard, a -> new Share()).alone.add(key);
    }

    /** Takes an item out of its share; the caller holds the lock of {@code missing}. */
    private void leaveShare(String key, Missing entry) {
        Share share = shares.get(entry.firstHeard);
        if (!share.alone.remove(key)) {
            share.named.remove(key);
        }
        if (share.size() == 0) {
            shares.remove(entry.firstHeard);
        }
    }

    /** A uniform random wait over the spread window. */
    private long spread() {
        return random.nextLong(spreadWindow() + 1);
    }

    /**
     * The lateness this node observes (swarm retries, adaptive wait, ROBUSTNESS.md §10.9): the p90 of its latest graft
     * round trips or the p90 of how long its latest tree copies came after their first announcement, whichever is
     * longer, or -1 before it has observed either. Each member holds a few samples of each ring at most, so one member
     * cannot pull the wait to its floor or push it to its ceiling alone (the gate).
     */
    private long lateness() {
        return Math.max(graftRtts.p90(), copyLateness.p90());
    }

    /** The base of the first ask's wait: the observed lateness, up to {@link #GRAFT_BASE_MILLIS}, which is also the base before any. */
    private long base() {
        long lateness = lateness();
        return lateness < 0 ? GRAFT_BASE_MILLIS : Math.min(GRAFT_BASE_MILLIS, lateness);
    }

    /**
     * The spread window: twice the observed lateness, between its floor and ceiling, or {@link #GRAFT_SPREAD_COLD_MILLIS}
     * before the node has observed any.
     */
    private long spreadWindow() {
        long lateness = lateness();
        return lateness < 0 ? GRAFT_SPREAD_COLD_MILLIS
                : Math.max(GRAFT_SPREAD_MIN_MILLIS, Math.min(GRAFT_SPREAD_MAX_MILLIS, 2 * lateness));
    }

    /**
     * How long a member that lacks an announced item may take before it has asked two announcers (swarm retries,
     * ROBUSTNESS.md §10.2): the base, a spread window, the first backoff, and a second spread window. A stream with a
     * fallback of its own (a pushed delta's {@code TREE_WANT}) waits at least this long before it gives up on the bus.
     *
     * @return the window in milliseconds
     */
    public long repairWindowMillis() {
        long window = spreadWindow();
        return base() + window + GRAFT_BACKOFF_MILLIS + window;
    }

    /** The wait after an item's {@code asks}-th ask: 1, 2, 4, 8 s, then 8 s. */
    private static long backoff(int asks) {
        return Math.min(GRAFT_BACKOFF_MAX_MILLIS, GRAFT_BACKOFF_MILLIS << Math.min(asks - 1, 16));
    }

    /**
     * Notes the first copy of an item (swarm retries). For an item this node asked for, a copy from a member asked gives
     * a round trip and clears that member's unanswered asks, a copy from any other member makes the asks premature,
     * counted once, and each member asked whose copy did not come first has one more unanswered ask. A later copy, a late
     * answer among them, repaired nothing, so it neither widens the spread nor counts (the gate's adversarial review,
     * ROBUSTNESS.md §10.6). A tree copy of a wanted item from a member not asked gives its lateness after the item's
     * first announcement (adaptive wait, §10.9).
     *
     * @param arrived  the item's entry among the wanted items, or null when nobody announced it first
     * @param treePush whether the copy came as a tree push, its budget above one
     * @return whether this node had asked for the item
     */
    private boolean noteCopy(String key, PeerId from, Missing arrived, boolean treePush) {
        Long sentAt = null;
        boolean asked;
        synchronized (grafted) {
            Grafted asks = grafted.remove(key);
            asked = asks != null;
            if (asked) {
                sentAt = asks.sentAtMillis.get(from);
                java.util.Set<PeerId> due = new java.util.HashSet<>();
                for (PeerId member : asks.sentAtMillis.keySet()) {
                    if (member.equals(from)) {
                        unanswered.remove(member);
                    } else {
                        unanswered.merge(member, 1, (held, one) -> Math.min(UNANSWERED_STRIKES, held + one));
                        due.add(member);
                    }
                }
                if (!due.isEmpty()) {
                    answersDue.put(key, due);
                }
            }
        }
        long now = clock.getAsLong();
        if (sentAt != null) {
            graftRtts.add(from, Math.max(0, now - sentAt));
            return true;
        }
        if (asked) {
            count("GRAFT_PREMATURE");
        }
        if (arrived != null && treePush) {
            copyLateness.add(arrived.firstHeard, Math.max(0, now - arrived.heardAtMillis));
        }
        return asked;
    }

    /** Whether a copy from {@code from} is the answer to an ask of this node that another copy overtook; consumes it. */
    private boolean answerDue(String key, PeerId from) {
        synchronized (answersDue) {
            java.util.Set<PeerId> due = answersDue.get(key);
            if (due == null || !due.remove(from)) {
                return false;
            }
            if (due.isEmpty()) {
                answersDue.remove(key);
            }
            return true;
        }
    }

    /**
     * Handles an inbound GRAFT: the asker is an eager member again, and gets the item if this node still holds it, as a
     * copy for the asker alone whose hop budget ends there (asker-only repair, ROBUSTNESS.md §10.9). The asker forwards
     * the item along its own eager edges. One answer per ask bounds what an asker can make this node send.
     *
     * @param from  the asking member
     * @param graft the item asked for
     */
    public void onGraft(PeerId from, Bodies.Graft graft) {
        if (lazy.remove(from)) {
            topologyChanged();
        }
        if (graft.streamId() == null || graft.hash() == null) {
            return; // an edge restored, no item asked for
        }
        Bodies.Rumor cached = recent.get(key(graft.streamId(), graft.hash()));
        if (cached != null) {
            sender.sendRumor(from, new Bodies.Rumor(cached.streamId(), cached.itemId(), 1, cached.payload()));
        }
    }

    /**
     * Handles an inbound PRUNE: the sender takes announcements from now on, not payloads.
     *
     * @param from  the member that received a duplicate from this node
     * @param prune the duplicate
     */
    public void onPrune(PeerId from, Bodies.Prune prune) {
        if (lazy.add(from)) {
            topologyChanged();
        }
    }

    /**
     * One lazy-push tick, driven by the owning node on every peer tick: forgets members that
     * left the view, grafts announced items that never arrived once their wait ends (a bounded
     * number per tick, each from the next announcer by the rule of swarm retries), and tells
     * the lazy members what this node forwarded since the last tick, so a member cut off from
     * the tree is grafted back within the spread wait. An idle group announces nothing. The
     * waits read {@code nowMillis}, so the catch-up ticks after a stall shorten none of them.
     *
     * @param nowMillis the node's clock
     */
    public void tick(long nowMillis) {
        java.util.function.Predicate<PeerId> capable = lazyCapable;
        if (capable == null) {
            return;
        }
        List<PeerId> members = sampler.members();
        if (!lazy.isEmpty()) {
            lazy.retainAll(members);
        }
        List<Missing> asks = new ArrayList<>();
        List<PeerId> askers = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        java.util.Set<PeerId> view = new java.util.HashSet<>(members);
        unanswered.keySet().retainAll(view);
        // At most GRAFTS_PER_TICK asks per 250 ms window (EL-T-01), however often the bus's participant runs.
        if (graftWindowStartMillis == Long.MIN_VALUE || nowMillis - graftWindowStartMillis >= GRAFT_WINDOW_MILLIS
                || nowMillis < graftWindowStartMillis) {
            graftWindowStartMillis = nowMillis;
            graftsInWindow = 0;
        }
        int budget = Math.max(0, GRAFTS_PER_TICK - graftsInWindow);
        synchronized (missing) {
            // first asks before retries (swarm retries, the gate's adversarial review): the retries of items a flood
            // of announcements named and nobody serves must not spend the budget a newly announced item needs
            for (int pass = 0; pass < 2 && asks.size() < budget; pass++) {
                boolean firstAsks = pass == 0;
                java.util.Iterator<Map.Entry<String, Missing>> it = missing.entrySet().iterator();
                while (it.hasNext() && asks.size() < budget) {
                    Map.Entry<String, Missing> next = it.next();
                    Missing entry = next.getValue();
                    if (entry.askAtMillis > nowMillis || firstAsks != (entry.asks == 0)) {
                        continue;
                    }
                    entry.announcers.retainAll(view);
                    if (entry.announcers.isEmpty() || nowMillis - entry.announcedAtMillis > MISSING_HORIZON_MILLIS) {
                        it.remove(); // no member announces it any more: anti-entropy, or the stream's own fallback, has it
                        leaveShare(next.getKey(), entry);
                        count("GRAFT_DROPPED");
                        continue;
                    }
                    PeerId announcer = entry.next(random, unanswered);
                    entry.asks++;
                    entry.askAtMillis = nowMillis + backoff(entry.asks) + spread();
                    if (entry.asks > 1) {
                        count("GRAFT_RETRY");
                    }
                    if (announcer.equals(entry.firstHeard)) {
                        count("GRAFT_FIRST_HEARD");
                    }
                    asks.add(entry);
                    askers.add(announcer);
                    keys.add(next.getKey());
                }
            }
        }
        graftsInWindow += asks.size();
        for (int i = 0; i < asks.size(); i++) {
            if (lazy.remove(askers.get(i))) {
                topologyChanged();
            }
            synchronized (grafted) {
                grafted.computeIfAbsent(keys.get(i), k -> new Grafted()).sentAtMillis.put(askers.get(i), nowMillis);
            }
            count("GRAFT");
            sender.sendGraft(askers.get(i), new Bodies.Graft(asks.get(i).streamId, asks.get(i).hash));
        }
        if (pendingHaves.isEmpty() || lazy.isEmpty()) {
            if (!pendingHaves.isEmpty() && lazy.isEmpty()) {
                pendingHaves.clear(); // every capable member got the payload
            }
            return;
        }
        // Standalone announcements only while the tree is young: a member is cut off only by a
        // topology change, so a settled tree needs none, and the probes carry the recent items.
        // The windows are time from the change (EL-T-15): every 250 ms for 2 s, then once a second until 8 s.
        long change = lastTopologyChangeMillis;
        long since = change == Long.MIN_VALUE ? Long.MAX_VALUE : Math.max(0, nowMillis - change);
        if (since >= SETTLING_MILLIS) {
            pendingHaves.clear();
            return;
        }
        long epoch = topologyEpoch;
        boolean sameWindow = lastHaveEpoch == epoch;
        if (since < YOUNG_MILLIS) {
            if (sameWindow && lastHaveMillis != Long.MIN_VALUE && nowMillis - lastHaveMillis < HAVE_EVERY_MILLIS
                    && nowMillis >= lastHaveMillis) {
                return;
            }
        } else if (sameWindow && since / SETTLING_EVERY_MILLIS == lastHaveBucket) {
            return;
        }
        lastHaveMillis = nowMillis;
        lastHaveBucket = since / SETTLING_EVERY_MILLIS;
        lastHaveEpoch = epoch;
        List<Forwarded> pending;
        synchronized (pendingHaves) {
            pending = List.copyOf(pendingHaves.values());
            pendingHaves.clear();
        }
        for (PeerId peer : members) {
            if (!lazy.contains(peer) || !capable.test(peer)) {
                continue;
            }
            long admitted = admittedAt(peer);
            List<Bodies.HaveItem> items = new ArrayList<>();
            for (Forwarded forwarded : pending) {
                if (forwarded.order() >= admitted) {
                    items.add(forwarded.item());
                }
            }
            if (!items.isEmpty()) {
                count("HAVE");
                sender.sendHave(peer, new Bodies.Have(items));
            }
        }
    }

    /**
     * Pushes a rumor on: to every capable member this node has not pruned (the tree), and to
     * {@code fanout} of the other members at random (the fan-out of spec §5.3, unchanged for a
     * group with no capable member). The item is remembered for a GRAFT and queued for the next
     * announcement when any capable member exists.
     */
    private void forward(String key, byte[] hash, Bodies.Rumor rumor, PeerId exclude) {
        java.util.function.Predicate<PeerId> capable = lazyCapable;
        boolean anyCapable = false;
        // In a group too small for the tree every capable member is pushed to, whatever a PRUNE
        // said: nothing prunes there, and a prune honoured there could only cost a repair.
        boolean eager = eagerStreams.contains(rumor.streamId()) || !treeWorthwhile();
        List<PeerId> plain = new ArrayList<>();
        for (PeerId peer : sampler.members()) {
            if (peer.equals(exclude)) {
                continue;
            }
            if (capable != null && capable.test(peer)) {
                anyCapable = true;
                if (eager || !lazy.contains(peer)) {
                    countRumor(rumor.streamId());
                    sender.sendRumor(peer, rumor);
                }
            } else {
                plain.add(peer);
            }
        }
        pushToPlain(plain, rumor, exclude == null || !anyCapable);
        if (anyCapable && (!eager || !treeWorthwhile() && !eagerStreams.contains(rumor.streamId()))) {
            recent.put(key, rumor);
            // stamped after the eager copies were enqueued, so an item stamped before an outbox's take left with it
            Forwarded forwarded = new Forwarded(new Bodies.HaveItem(rumor.streamId(), hash), order.getAndIncrement(),
                    clock.getAsLong());
            boolean firstQueued = pendingHaves.isEmpty();
            pendingHaves.put(key, forwarded);
            recentForwarded.put(key, forwarded);
            long change = lastTopologyChangeMillis;
            if (firstQueued && !lazy.isEmpty() && change != Long.MIN_VALUE
                    && clock.getAsLong() - change < SETTLING_MILLIS) {
                hooks.gossipDue(); // an announcement queued while the tree is young (EL-T-15)
            }
        }
    }

    /**
     * The fan-out toward members without the tree hint (older nodes, dial-only clients), PERF2
     * §3 item 4's other half. At the origin, or anywhere in a group with no tree at all, the
     * {@code fanout} slots go two ways: all but one to <em>strong</em> members, those this node
     * holds a live link to and has not failed to reach lately, in the sampler's random order;
     * the last to the <em>weak</em> one, the member pushed to least recently (never, first),
     * ties in that same random order. The strong keep the push reliable, the weak slot sees
     * that nobody is left out for long, and the randomness keeps it from settling on one pair.
     * A forwarder along the tree pushes only to dial-only members it is linked to, which nobody
     * else can reach: every other hint-less member had its chance at the origin and gets the rest
     * by anti-entropy, where every tree member pushing to every one of them cost each of them
     * four copies of every item. A push that could not leave frees its slot for the next pick.
     */
    private void pushToPlain(List<PeerId> plain, Bodies.Rumor rumor, boolean origin) {
        if (plain.isEmpty()) {
            return;
        }
        long now = clock.getAsLong();
        if (!origin) {
            for (PeerId peer : plain) {
                if (dialOnly.test(peer) && linked.test(peer)) {
                    push(peer, rumor, now);
                }
            }
            return;
        }
        List<PeerId> strong = new ArrayList<>();
        List<PeerId> weak = new ArrayList<>(plain);
        for (PeerId peer : plain) {
            Long failed = plainFailed.get(peer);
            if (linked.test(peer) && (failed == null || now - failed > PLAIN_FAILURE.toMillis())) {
                strong.add(peer);
            }
        }
        weak.sort(java.util.Comparator.comparingLong(peer -> plainServed.getOrDefault(peer, -1L)));
        int sent = 0;
        java.util.Set<PeerId> chosen = new java.util.HashSet<>();
        int strongSlots = Math.max(0, fanout - 1);
        for (PeerId peer : strong) {
            if (sent >= strongSlots) {
                break;
            }
            if (push(peer, rumor, now)) {
                sent++;
            }
            chosen.add(peer);
        }
        for (PeerId peer : weak) {
            if (sent >= fanout) {
                break;
            }
            if (chosen.add(peer) && push(peer, rumor, now)) {
                sent++;
            }
        }
    }

    private boolean push(PeerId peer, Bodies.Rumor rumor, long now) {
        if (sender.trySendRumor(peer, rumor)) {
            countRumor(rumor.streamId());
            plainServed.put(peer, order.getAndIncrement()); // the weak slot's order, not a time (EL-T-01)
            return true;
        }
        plainFailed.put(peer, now);
        return false;
    }

    /**
     * The seen-cache key: the stream and a hash of the payload itself, never the
     * sender-chosen itemId (ASF-012): a predictable itemId let an attacker pre-seed the cache
     * with a victim's <em>future</em> ids, so honest nodes dropped the victim's
     * genuine items unread — targeted eviction from the fabric. A payload hash
     * cannot be precomputed for content the victim has not produced yet.
     */
    private static String key(String streamId, byte[] hash) {
        return streamId + "\u0000" + java.util.HexFormat.of().formatHex(hash);
    }
}
