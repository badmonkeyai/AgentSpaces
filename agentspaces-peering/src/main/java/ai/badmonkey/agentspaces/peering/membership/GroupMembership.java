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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.time.TimeSource;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Leased membership for one group (spec §5.2): the membership view is fed by
 * leased PeerAdvertisements arriving over gossip and by direct liveness contact,
 * and a member that neither gossips nor answers within its TTL is dropped without
 * ceremony (spec P2). A SWIM-style probe cycle runs on {@link #tick}: ping a
 * random member, and on timeout ask {@code indirectPeers} others to probe the
 * target before dropping it.
 *
 * <p>Deterministic by construction: time comes from the injected clock, randomness
 * from the injected {@link Random}, and probing advances only on {@code tick}.
 *
 * <p>Every time this view keeps is a local period (srad/agentspaces-event-loop.md §3.6, EL-C-04): when a member was
 * last heard, its probes' deadlines, the probe turns. Given a {@link TimeSource}, the view reckons them on its
 * monotonic milliseconds ({@link TimeSource#monoMillis()}), so a wall step neither expires a member early nor keeps
 * one past its TTL (EL-SP-04); given a plain {@link InstantSource}, on that clock's wall time, as before. The
 * {@code *Millis} values it reports ({@link #nextDueMillis}, {@link #joinedAtMillis}) are on the same base.
 */
public final class GroupMembership implements PeerSampler {

    /**
     * The default member TTL's floor (srad/agentspaces-event-loop.md EL-E-05, decision 17): 1.5 times the 30 s
     * self-advertisement refresh, and never less than 1.5 times the quiet probe cycle ({@link #memberTtlMillis}).
     */
    public static final Duration MEMBER_TTL_FLOOR = Duration.ofSeconds(45);
    /**
     * The keepalive bound of the defaults (EL-E-06): this node's own probe stretch and self-advertisement refresh keep
     * every member that judges it at 30 s (a 0.6.0 member, another implementation) as today, whatever TTL this node
     * judges others by.
     */
    public static final Duration KEEPALIVE_TTL = Duration.ofSeconds(30);
    /** A member heard only through probes is kept for this many quiet probe cycles (the owner's 1.5 margin, EL-E-05). */
    public static final double TTL_CYCLE_FACTOR = 1.5;

    /**
     * Membership tuning.
     *
     * @param memberTtl     how long a silent member stays in the view: the floor, or the configured value. A member that
     *                      keeps itself alive (a newer advertisement it issued, or a frame it signed, reached this node
     *                      since it was admitted) is judged by it; one heard only through probes by the larger of it and
     *                      {@code cycleFactor} quiet probe cycles ({@link GroupMembership#memberTtlMillis})
     * @param pingTimeout   how long to wait for an ACK before indirect probing
     * @param indirectPeers how many peers to ask in the indirect round
     * @param probeInterval the base probe interval
     * @param keepaliveTtl  the TTL this node's own keepalive (the quiet probe bound and the self-advertisement refresh)
     *                      is kept inside; never feeds back from the member TTL (EL-E-06)
     * @param cycleFactor   how many quiet probe cycles a member heard only through probes is kept for, at least 1
     */
    public record Config(Duration memberTtl, Duration pingTimeout, int indirectPeers, Duration probeInterval,
                         Duration keepaliveTtl, double cycleFactor) {

        /** The SWIM protocol period: one probe per member per interval (memberlist's default, 1 s). */
        public static final Duration DEFAULT_PROBE_INTERVAL = Duration.ofSeconds(1);

        public Config {
            Objects.requireNonNull(memberTtl, "memberTtl");
            if (probeInterval == null) {
                probeInterval = DEFAULT_PROBE_INTERVAL;
            }
            if (keepaliveTtl == null) {
                keepaliveTtl = memberTtl;
            }
            if (!(cycleFactor >= 1.0) || Double.isInfinite(cycleFactor)) {
                throw new IllegalArgumentException("cycleFactor must be at least 1: " + cycleFactor);
            }
        }

        /**
         * The four-argument form: the keepalive is kept inside the configured member TTL, as before (owner answer 10,
         * 2026-11-22), and a member heard only through probes gets the 1.5 cycle margin above it.
         */
        public Config(Duration memberTtl, Duration pingTimeout, int indirectPeers, Duration probeInterval) {
            this(memberTtl, pingTimeout, indirectPeers, probeInterval, memberTtl, TTL_CYCLE_FACTOR);
        }

        /** The three-argument form, probing once per second. */
        public Config(Duration memberTtl, Duration pingTimeout, int indirectPeers) {
            this(memberTtl, pingTimeout, indirectPeers, DEFAULT_PROBE_INTERVAL);
        }

        /**
         * Returns defaults: a member TTL floor of 45 s ({@link #MEMBER_TTL_FLOOR}), ping timeout 2 s, 2 indirect peers,
         * one probe per second, this node's keepalive inside 30 s ({@link #KEEPALIVE_TTL}) and the 1.5 cycle margin.
         */
        public static Config defaults() {
            return new Config(MEMBER_TTL_FLOOR, Duration.ofSeconds(2), 2, DEFAULT_PROBE_INTERVAL, KEEPALIVE_TTL,
                    TTL_CYCLE_FACTOR);
        }
    }

    /** One member's view state. */
    public static final class Member {
        private final PeerId id;
        private volatile List<PeerAdvertisement.Endpoint> endpoints;
        private volatile java.util.Set<PeerAdvertisement.PeerRole> roles = java.util.Set.of();
        private volatile long lastHeardMillis;
        /** When a verified frame this member signed last reached this node, or MIN_VALUE before any (idle-loop O6). */
        private volatile long lastContactMillis = Long.MIN_VALUE;
        private volatile boolean suspect;
        /** When this member entered the view, on the node's clock; a member that leaves and returns starts afresh. */
        private final long joinedMillis;
        /**
         * The TTL a fall of the cycle-bound TTL left this member with (§3.11): the old TTL still bounds a judgement
         * that started before the fall. Cleared when the member is heard.
         */
        private volatile long ttlHoldMillis;
        /**
         * Until when this member, found past its TTL by a late wake of this node (a frozen process), is judged by the
         * probe that wake sent rather than by its TTL; {@link Long#MIN_VALUE} for none. Cleared when it is heard.
         */
        private volatile long reprobeUntilMillis = Long.MIN_VALUE;

        private Member(PeerId id, List<PeerAdvertisement.Endpoint> endpoints, long lastHeardMillis) {
            this.id = id;
            this.endpoints = endpoints;
            this.lastHeardMillis = lastHeardMillis;
            this.joinedMillis = lastHeardMillis;
        }

        /**
         * Whether the member keeps itself alive (EL-E-05, owner answer 9, 2026-11-22): a frame it signed reached this
         * node directly within the member TTL of its last hearing. A Java member's own probes and its direct
         * self-advertisement refresh do this every 30 s at most. Such a member is judged at the configured TTL (the
         * 45 s floor by default); one heard only through probes, as a dial-only peer is through its holder, by the
         * larger cycle-bound TTL ({@link GroupMembership#memberTtlMillis}). A relayed copy of its advertisement is
         * not contact: a dial-only peer's card reaches every member through gossip, but the peer never reissues it.
         *
         * @param memberTtlMillis the configured member TTL
         * @return whether it is judged at the configured TTL
         */
        boolean keepsItselfAlive(long memberTtlMillis) {
            long contact = lastContactMillis;
            return contact != Long.MIN_VALUE && lastHeardMillis - contact <= memberTtlMillis;
        }

        private void heard(long now) {
            lastHeardMillis = now;
            suspect = false;
            ttlHoldMillis = 0;
            reprobeUntilMillis = Long.MIN_VALUE;
        }

        /** Returns the topology roles the member advertises (spec §5.4). */
        public java.util.Set<PeerAdvertisement.PeerRole> roles() {
            return roles;
        }

        /** Returns the member's PeerID. */
        public PeerId id() {
            return id;
        }

        /** Returns the member's advertised endpoints, priority-ordered. */
        public List<PeerAdvertisement.Endpoint> endpoints() {
            return endpoints;
        }

        /** Returns whether the member is currently suspected. */
        public boolean suspect() {
            return suspect;
        }
    }

    /** Callback used by the probe cycle to send PING and PING_REQ frames. */
    public interface Prober {

        /**
         * Sends a PING to a member.
         *
         * @param target the member to probe
         * @param nonce  the correlation nonce
         */
        void ping(PeerId target, long nonce);

        /**
         * Asks a relay to probe a target on this peer's behalf.
         *
         * @param relay  the peer asked to probe
         * @param target the member to probe
         * @param nonce  the correlation nonce
         */
        void pingReq(PeerId relay, PeerId target, long nonce);

        /**
         * Whether a PING to this target would carry something beside liveness that the target has not already
         * exchanged with this node in this period: the recently forwarded items it piggybacks, or digest hashes the
         * target's own hash-carrying PING (answered with this node's) did not already compare. A probe of a member
         * heard from in this period is skipped only when it would not.
         *
         * @param target      the member about to be probed
         * @param sinceMillis this node's previous probe turn, on the node's clock
         * @return true when the probe carries news
         */
        default boolean carriesNews(PeerId target, long sinceMillis) {
            return false;
        }

        /**
         * The members holding the only link to a target (srad/agentspaces-event-loop.md §3.12, TR-1): those the
         * target names in its {@code relay.via} hint, when this node can reach it no other way (no link, no endpoint
         * it can dial, no relay path). A probe of such a target skips the direct PING, which would go nowhere, and
         * asks these holders (TR-2). Empty when the target is reachable or names none. The view filters the answer.
         *
         * @param target the member about to be probed
         * @return the holders, or empty
         */
        default java.util.Collection<PeerId> linkHolders(PeerId target) {
            return List.of();
        }

        /**
         * The members a target names in its {@code relay.via} hint, whether or not this node can reach it otherwise:
         * an indirect round escalated from a failed probe asks them first (TR-2). The view filters the answer.
         *
         * @param target the member in doubt
         * @return the declared holders, or empty
         */
        default java.util.Collection<PeerId> declaredHolders(PeerId target) {
            return List.of();
        }

        /**
         * Sends the PING a wake asks of the seed this node is linked through ({@link #probeAtWake}); the node counts
         * it apart from the probe's own PINGs. The default sends it as any PING.
         *
         * @param target the seed
         * @param nonce  the correlation nonce
         */
        default void wakePing(PeerId target, long nonce) {
            ping(target, nonce);
        }

        /**
         * Whether bytes of a frame from the target arrived on its link since an instant, the frame not yet whole
         * (SPEC v0.1.37 §9.4): a seed writing a large frame over a slow link is answering, and a wake's PING it has
         * not yet answered with a whole frame is not judged unanswered. The default sees no bytes.
         *
         * @param target      the member
         * @param sinceMillis the instant, on this view's clock
         * @return whether bytes arrived since
         */
        default boolean progressSince(PeerId target, long sinceMillis) {
            return false;
        }
    }

    /**
     * Where a probe stands (TR-2): a direct PING, a routine round through the target's holders, an indirect round, or
     * the PING a wake sent the seed (owner decision 3 of 2026-11-23), whose silence drops the seed at once.
     */
    private enum Stage { DIRECT, HOLDER, INDIRECT, WAKE }

    /**
     * An outstanding probe.
     *
     * @param fromHolder whether the probe of this target started as a routine holder round (for the drop counters)
     */
    private record Pending(PeerId target, long deadlineMillis, Stage stage, java.util.Set<PeerId> relays,
                           boolean fromHolder) {
    }

    /**
     * The probe's counters (srad/agentspaces-event-loop.md §3.12, EL-SP-31).
     *
     * @param directPings              direct PINGs sent at a probe turn
     * @param holderRounds             probe turns that went to the target's holders instead of a direct PING (TR-2)
     * @param holderPingReqs           PING_REQs sent to holders, in routine and escalated rounds
     * @param escalations              failed holder rounds escalated to a second round of holders and random members
     * @param randomPingReqs           PING_REQs sent to random members in an indirect round (TR-5)
     * @param dropsAfterHolderRound    members dropped by a probe that began as a holder round
     * @param dropsAfterIndirectRound  members dropped by a probe that began as a direct PING
     * @param holderPingReqsByRelay    holder PING_REQs per relay, for members still in view
     */
    public record ProbeStats(long directPings, long holderRounds, long holderPingReqs, long escalations,
                             long randomPingReqs, long dropsAfterHolderRound, long dropsAfterIndirectRound,
                             Map<PeerId, Long> holderPingReqsByRelay) {
    }

    private final java.util.concurrent.atomic.AtomicLong directPings = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong holderRounds = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong holderPingReqs = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong escalations = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong randomPingReqs = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong dropsAfterHolderRound =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong dropsAfterIndirectRound =
            new java.util.concurrent.atomic.AtomicLong();
    /** Holder PING_REQs per relay; bounded by the view, a relay's entry goes when it leaves the view. */
    private final Map<PeerId, java.util.concurrent.atomic.AtomicLong> holderPingReqsByRelay = new ConcurrentHashMap<>();

    /**
     * Returns the probe's counters.
     *
     * @return the counters as of now
     */
    public ProbeStats probeStats() {
        Map<PeerId, Long> byRelay = new java.util.HashMap<>();
        holderPingReqsByRelay.forEach((relay, count) -> byRelay.put(relay, count.get()));
        return new ProbeStats(directPings.get(), holderRounds.get(), holderPingReqs.get(), escalations.get(),
                randomPingReqs.get(), dropsAfterHolderRound.get(), dropsAfterIndirectRound.get(), Map.copyOf(byRelay));
    }

    private final PeerId self;
    private volatile java.util.function.Predicate<PeerId> refused = peer -> false;
    private final InstantSource clock;
    /** The base every period here is reckoned on: the time source's monotonic millis, or a plain clock's wall. */
    private final java.util.function.LongSupplier localMillis;
    private final Random random;
    private final Config config;
    private final Map<PeerId, Member> members = new ConcurrentHashMap<>();
    /** Peers removed by {@link #evict} rather than by silence, until the owner consumes the mark. */
    private final java.util.Set<PeerId> ejected = ConcurrentHashMap.newKeySet();

    /**
     * Whether a peer left this view by an authoritative {@link #evict} rather than by lapsing,
     * consuming the mark: the owner retries lost members' endpoints for a while, and an ejected
     * peer is not lost but refused, so it is not courted (v0.1.17).
     *
     * @param peer the peer that left the view
     * @return true once, if it was ejected
     */
    public boolean consumeEjected(PeerId peer) {
        return ejected.remove(peer);
    }
    private final Map<Long, Pending> pendingProbes = new ConcurrentHashMap<>();
    /** Probe nonces are unguessable (ASF-011): a sequential counter let any
     * member cancel or satisfy probes it never saw by guessing the next value. */
    private final java.security.SecureRandom nonceRandom = new java.security.SecureRandom();

    /**
     * Creates the membership view.
     *
     * @param self   the local peer (never a member of its own view)
     * @param clock  the time source
     * @param random the randomness source (seed it in tests)
     * @param config membership tuning
     */
    public GroupMembership(PeerId self, InstantSource clock, Random random, Config config) {
        this.self = Objects.requireNonNull(self, "self");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.localMillis = clock instanceof TimeSource source ? source::monoMillis
                : () -> clock.instant().toEpochMilli();
        this.random = Objects.requireNonNull(random, "random");
        this.config = Objects.requireNonNull(config, "config");
        // RE-F-03: the probe's paces start where the configuration puts them and move by runtime setters; each holds
        // a raise for one probe interval before a lowering takes effect (RE-Q-REL-09).
        this.probeIntervalPace = new ai.badmonkey.agentspaces.peering.gossip.Pace("probe-interval",
                PROBE_INTERVAL_FLOOR_MILLIS, PROBE_INTERVAL_CEILING_MILLIS, config.probeInterval().toMillis(),
                this::nowMillis, this::probeHoldMillis);
        this.probeStretchPace = new ai.badmonkey.agentspaces.peering.gossip.Pace("probe-stretch", 0,
                MAX_QUIET_STRETCH, MAX_QUIET_STRETCH, this::nowMillis, this::probeHoldMillis);
    }

    /** The least probe interval a setter may ask for. */
    public static final long PROBE_INTERVAL_FLOOR_MILLIS = 100;
    /** The greatest probe interval a setter may ask for. */
    public static final long PROBE_INTERVAL_CEILING_MILLIS = 60_000;
    private final ai.badmonkey.agentspaces.peering.gossip.Pace probeIntervalPace;
    private final ai.badmonkey.agentspaces.peering.gossip.Pace probeStretchPace;

    private long probeHoldMillis() {
        ai.badmonkey.agentspaces.peering.gossip.Pace interval = probeIntervalPace;
        return interval == null ? config.probeInterval().toMillis() : interval.get();
    }

    /**
     * Sets the probe interval, the base of SWIM's protocol period (RE-F-03); a runtime setter. A raise takes effect at
     * once; a lowering waits until the raised interval has served one interval (RE-Q-REL-09).
     *
     * @param interval from {@value #PROBE_INTERVAL_FLOOR_MILLIS} ms to {@value #PROBE_INTERVAL_CEILING_MILLIS} ms
     * @throws IllegalArgumentException outside those bounds
     */
    public void probeInterval(Duration interval) {
        probeIntervalPace.set(interval.toMillis());
        changed();
    }

    /** The probe interval in effect, in milliseconds. */
    public long probeIntervalMillis() {
        return probeIntervalPace.get();
    }

    /**
     * Sets how many doublings a quiet group's probe interval may stretch by (RE-F-03); a runtime setter. Zero never
     * stretches; the ceiling is {@link #MAX_QUIET_STRETCH}, so a member's keepalive reasoning holds at every setting.
     *
     * @param doublings from 0 to {@link #MAX_QUIET_STRETCH}
     * @throws IllegalArgumentException outside those bounds
     */
    public void probeStretch(int doublings) {
        probeStretchPace.set(doublings);
        changed();
    }

    /** How many doublings a quiet group's probe interval may stretch by now. */
    public int probeStretch() {
        return (int) probeStretchPace.get();
    }

    /** The probe's paces, as a status page or an adjuster reads them. */
    public java.util.List<ai.badmonkey.agentspaces.peering.gossip.Pace.Snapshot> paces() {
        return java.util.List.of(probeIntervalPace.snapshot(), probeStretchPace.snapshot());
    }

    /**
     * Sets the peers this view must never admit, consulted on every
     * advertisement: the group's revocations (ASF-047). Late-bound because the
     * view is built before the group's revocation registry.
     *
     * @param refused the peers to refuse
     */
    public void refuse(java.util.function.Predicate<PeerId> refused) {
        this.refused = Objects.requireNonNull(refused, "refused");
    }

    /**
     * Feeds a verified PeerAdvertisement into the view, refreshing endpoints and
     * liveness.
     *
     * @param ad the advertisement (already signature-verified by the caller)
     */
    public void onPeerAdvertisement(PeerAdvertisement ad) {
        Objects.requireNonNull(ad, "ad");
        if (ad.issuer().equals(self) || refused.test(ad.issuer())) {
            return; // ourselves, or a peer the group has revoked (ASF-047)
        }
        long now = nowMillis();
        long before = viewVersion.get();
        // Nothing in this lambda takes this view's monitor: tick holds it while it walks the map's bins, so a call to
        // a synchronized method from here could deadlock. The probe order learns of a newcomer through a lock-free
        // queue that the probe turn drains (TR-4).
        members.compute(ad.issuer(), (id, existing) -> {
            if (existing == null) {
                activeSinceProbe = true; // a newcomer: probe on the base interval again
                quietCycles = 0;
                viewVersion.incrementAndGet();
                admitted.add(id);
            }
            Member member = existing == null
                    ? new Member(id, ad.endpoints(), now) : existing;
            member.endpoints = ad.endpoints();
            member.roles = ad.roles();
            member.heard(now);
            return member;
        });
        if (viewVersion.get() != before) {
            changed();
        }
    }

    /**
     * Removes a peer from the view immediately (a revocation's enforcement, or
     * any other authoritative eject). Idempotent.
     *
     * @param peer the peer to evict
     */
    public void evict(PeerId peer) {
        ejected.add(Objects.requireNonNull(peer, "peer"));
        dropMember(peer);
    }

    /**
     * Records direct contact from a peer (any verified frame counts as liveness).
     *
     * @param peer the peer heard from
     */
    /** This group's failure-detection parameters. */
    public Config config() {
        return config;
    }

    public void recordHeard(PeerId peer) {
        Member member = members.get(peer);
        if (member != null) {
            long now = nowMillis();
            member.lastContactMillis = now;
            member.heard(now);
        }
    }

    /**
     * Liveness without contact (idle-loop O6): an ACK, which answers this node's own probe, or an ACK a relay this
     * node asked brought for the member. Neither is a frame the member sent of its own accord, so neither lets a later
     * probe of it rest.
     *
     * @param peer the member
     */
    public void recordAnswered(PeerId peer) {
        creditRelayed(peer);
    }

    private void creditRelayed(PeerId peer) {
        Member member = members.get(peer);
        if (member != null) {
            member.heard(nowMillis());
        }
    }

    /** Whether a probe awaits its answer, so its timeout is judged on the coming ticks (idle-loop O8). */
    public boolean probesPending() {
        return !pendingProbes.isEmpty();
    }

    /**
     * Whether the next {@link #tick} at {@code nowMillis} sends a probe, so a caller takes what the probe carries only
     * then (idle-loop O8). An approximation the tick does not depend on: the tick decides again.
     *
     * @param nowMillis the node clock's now
     * @return true when the probe period has run out
     */
    public synchronized boolean probeDue(long nowMillis) {
        long interval = stretchedIntervalMillis(quietCycles);
        return lastProbeMillis == Long.MIN_VALUE || nowMillis - lastProbeMillis >= interval;
    }

    /** Probes skipped because the member's own verified frame reached this node within the probe period (O6). */
    public long probesRested() {
        return probesRested.get();
    }

    private final java.util.concurrent.atomic.AtomicLong probesRested = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Handles an ACK correlated to an outstanding probe. The liveness credit
     * always goes to the probe's own recorded target — never to any identity
     * the wire supplies — and the ACK counts only when its authenticated
     * sender is that target (a direct probe) or a relay this node actually
     * asked (an indirect probe). A sprayed or forged ACK neither cancels the
     * probe nor marks anyone alive (ASF-011).
     *
     * @param nonce the echoed nonce
     * @param from  the frame's authenticated sender
     */
    public void onAck(long nonce, PeerId from) {
        Pending pending = pendingProbes.get(nonce);
        if (pending == null) {
            return;
        }
        boolean fromTarget = from.equals(pending.target());
        boolean fromAskedRelay = pending.relays().contains(from);
        if (!fromTarget && !fromAskedRelay) {
            return; // not who we asked: the probe stands
        }
        pendingProbes.remove(nonce);
        creditRelayed(pending.target()); // an answer to this node's own probe: liveness, not contact
    }

    /**
     * Runs one membership round: expire silent members, escalate or drop timed-out
     * probes, and probe one random member.
     *
     * @param prober the frame sender
     */
    private long lastProbeMillis = Long.MIN_VALUE;
    /** How far the probe interval has stretched on a quiet fleet: the interval is the base times two to this. */
    private volatile int quietCycles;
    /** Whether anything moved (gossip activity, a member admitted) since the last probe. */
    private volatile boolean activeSinceProbe = true;
    /**
     * The probe interval stretches to the base times two to this at most (v0.1.17: 2; idle-loop item 3, 2026-11-18: 4).
     * Past {@link #LEGACY_QUIET_STRETCH} doublings the stretch is also bounded by the member TTL
     * ({@link #maxQuietIntervalMillis}), so every member is still heard twice inside its TTL.
     */
    public static final int MAX_QUIET_STRETCH = 4;
    /** The stretch every quiet group may reach whatever its size: four times the base, as before item 3. */
    public static final int LEGACY_QUIET_STRETCH = 2;

    /**
     * The longest probe interval a quiet group may stretch to with {@code others} members besides this node (idle-loop
     * item 3): the base times two to the stretch ceiling, but past four times the base no longer than the keepalive TTL
     * over twice the others, so this node's probe cycle reaches every member twice inside the TTL. Groups too large
     * for that keep the four-times stretch, and the advertisement refresh is their keepalive (ROBUSTNESS.md §6b). The
     * bound reads the keepalive TTL ({@link Config#keepaliveTtl}, 30 s by default), never the member TTL this node
     * judges others by, so the cadence peers judge this node by does not feed back from it (EL-E-06).
     *
     * @param others the members besides this node
     * @return the interval in milliseconds
     */
    public long maxQuietIntervalMillis(int others) {
        int stretch = (int) probeStretchPace.get();
        long base = probeIntervalPace.get();
        long legacy = base * (1L << Math.min(stretch, LEGACY_QUIET_STRETCH));
        long full = base * (1L << stretch);
        long ttlBound = config.keepaliveTtl().toMillis() / (2L * Math.max(1, others));
        return Math.min(full, Math.max(legacy, ttlBound));
    }

    /**
     * This node's quiet probe cycle (EL-E-05): the members besides this node times the quiet probe interval, the
     * longest one node waits between two probes of the same member while the group is quiet and its order kept (TR-4).
     *
     * @return the cycle in milliseconds, 0 with an empty view
     */
    public long quietProbeCycleMillis() {
        int others = members.size();
        return others * maxQuietIntervalMillis(others);
    }

    /**
     * The member TTL this node judges a member heard only through probes by (decision 17 as re-decided, EL-E-05): the
     * larger of the configured TTL (the 45 s floor by default) and {@link Config#cycleFactor} quiet probe cycles: 45 s
     * up to eight members, 48 s at nine, 54 s at ten, 66 s at twelve, 90 s at sixteen under the defaults. Computed
     * from the view's size and the paces on every read, so a view or cadence change applies at once; a fall applies
     * only to judgements that start after it (§3.11). A member that keeps itself alive is judged at the configured
     * TTL.
     *
     * @return the TTL in milliseconds
     */
    public long memberTtlMillis() {
        long cycle = quietProbeCycleMillis();
        return Math.max(config.memberTtl().toMillis(), (long) Math.ceil(config.cycleFactor() * cycle));
    }

    /** The cycle-bound TTL as of the last settle; a fall below it holds for judgements already started. */
    private long lastTtlMillis;

    /**
     * Settles the cycle-bound TTL before a judgement (§3.11): on a fall, every member heard only through probes keeps
     * the old TTL until it is heard again, so no member heard within the old TTL is dropped by the change.
     *
     * @return the cycle-bound TTL now
     */
    private synchronized long settleTtl() {
        long ttl = memberTtlMillis();
        long last = lastTtlMillis;
        if (ttl < last) {
            for (Member m : members.values()) {
                if (m.ttlHoldMillis < last) {
                    m.ttlHoldMillis = last;
                }
            }
        }
        lastTtlMillis = ttl;
        return ttl;
    }

    /** The TTL a member is judged by now (EL-E-05). */
    private long ttlFor(Member m, long cycleTtl) {
        long configured = config.memberTtl().toMillis();
        long ttl = m.keepsItselfAlive(configured) ? configured : cycleTtl;
        return Math.max(ttl, m.ttlHoldMillis);
    }

    /** The probe interval in effect after {@code cycles} quiet probe cycles. */
    private long stretchedIntervalMillis(int cycles) {
        int stretch = (int) probeStretchPace.get();
        long stretched = probeIntervalPace.get() * (1L << Math.min(cycles, stretch));
        return Math.min(stretched, Math.max(probeIntervalPace.get(), maxQuietIntervalMillis(members.size())));
    }
    /**
     * The probe order, kept across cycles (TR-4, EL-P-15): shuffled once on its first fill, a newcomer inserted at a
     * random place, a departed member removed when the cursor reaches it. With a reshuffle at every refill the gap
     * between two probes of one member reached almost two cycles; kept, it is one cycle plus one turn per member
     * admitted since. Touched only under this view's monitor.
     */
    private final ArrayList<PeerId> probeOrder = new ArrayList<>();
    private final java.util.HashSet<PeerId> inOrder = new java.util.HashSet<>();
    private int cursor;
    /** Members admitted since the last probe turn, fed from the admission without a lock and drained by the turn. */
    private final java.util.concurrent.ConcurrentLinkedQueue<PeerId> admitted =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    private void drainAdmitted() {
        if (probeOrder.isEmpty()) {
            // the first fill (or a refill after everyone left): one shuffle of the view
            admitted.clear();
            List<PeerId> ids = new ArrayList<>(members.keySet());
            inOrder.clear();
            if (ids.isEmpty()) {
                return;
            }
            Collections.shuffle(ids, random);
            probeOrder.addAll(ids);
            inOrder.addAll(ids);
            cursor = 0;
            return;
        }
        for (PeerId id; (id = admitted.poll()) != null; ) {
            if (inOrder.add(id)) {
                int at = random.nextInt(probeOrder.size() + 1);
                probeOrder.add(at, id);
                if (at < cursor) {
                    cursor++;
                }
            }
        }
    }

    /** At the end of a cycle: any member in view but missing from the order (an admission raced a removal) is added. */
    private void reconcileOrder() {
        if (inOrder.size() == members.size() && inOrder.containsAll(members.keySet())) {
            return;
        }
        for (PeerId id : members.keySet()) {
            if (inOrder.add(id)) {
                probeOrder.add(random.nextInt(probeOrder.size() + 1), id);
            }
        }
    }

    private PeerId nextProbeTarget() {
        drainAdmitted();
        boolean wrapped = false;
        while (!probeOrder.isEmpty()) {
            if (cursor >= probeOrder.size()) {
                if (wrapped) {
                    return null; // nobody in the order is in view
                }
                wrapped = true;
                cursor = 0; // the next cycle, in the same order
                reconcileOrder();
                continue;
            }
            PeerId next = probeOrder.get(cursor);
            if (!members.containsKey(next)) {
                probeOrder.remove(cursor); // departed: removed lazily, the cursor stays on the next one
                inOrder.remove(next);
                continue;
            }
            cursor++;
            return next;
        }
        return null;
    }

    /** The holders of a target's link the view can use (TR-1): in view, not this node, not the target, distinct, capped. */
    private List<PeerId> usableHolders(java.util.Collection<PeerId> named, PeerId target) {
        if (named == null || named.isEmpty()) {
            return List.of();
        }
        List<PeerId> out = new ArrayList<>(Math.min(named.size(), config.indirectPeers()));
        for (PeerId holder : named) {
            if (out.size() >= config.indirectPeers()) {
                break;
            }
            if (holder != null && !holder.equals(self) && !holder.equals(target) && members.containsKey(holder)
                    && !out.contains(holder)) {
                out.add(holder);
            }
        }
        return out;
    }

    private void askHolders(Prober prober, List<PeerId> holders, PeerId target, long nonce) {
        for (PeerId holder : holders) {
            holderPingReqs.incrementAndGet();
            holderPingReqsByRelay.computeIfAbsent(holder, h -> new java.util.concurrent.atomic.AtomicLong())
                    .incrementAndGet();
            prober.pingReq(holder, target, nonce);
        }
    }

    /**
     * An indirect round for a target in doubt (TR-2, TR-5): its declared holders first, then random members other than
     * those. After a failed direct PING the random members fill the round up to {@code indirectPeers}; after a failed
     * holder round (owner answer 11) up to {@code indirectPeers} random members join the holders, so one lost relayed
     * ACK does not drop a dial-only peer, which only an advertisement it issues could bring back. Nobody is asked yet:
     * the caller records the pending probe first, because an answer can arrive before the send returns.
     *
     * @return the holders and the random members, either possibly empty
     */
    private Round indirectRound(Prober prober, PeerId target, boolean afterHolderRound) {
        List<PeerId> holders = usableHolders(prober.declaredHolders(target), target);
        java.util.Set<PeerId> exclude = new java.util.HashSet<>(holders);
        exclude.add(target);
        int randomCap = afterHolderRound ? config.indirectPeers()
                : Math.max(0, config.indirectPeers() - holders.size());
        return new Round(holders, randomMembersExcluding(randomCap, exclude));
    }

    /** One indirect round's relays: the target's holders, then random members. */
    private record Round(List<PeerId> holders, List<PeerId> others) {
        boolean empty() {
            return holders.isEmpty() && others.isEmpty();
        }

        java.util.Set<PeerId> all() {
            java.util.Set<PeerId> asked = new java.util.HashSet<>(holders);
            asked.addAll(others);
            return java.util.Set.copyOf(asked);
        }
    }

    private void ask(Prober prober, Round round, PeerId target, long nonce) {
        askHolders(prober, round.holders(), target, nonce);
        for (PeerId relay : round.others()) {
            randomPingReqs.incrementAndGet();
            prober.pingReq(relay, target, nonce);
        }
    }

    public synchronized void tick(Prober prober) {
        tick(prober, false);
    }

    /**
     * One membership tick. While the fleet is quiet (no gossip activity reported by the caller
     * since the last tick, no member admitted) the probe interval doubles at each probe up to
     * {@link #MAX_QUIET_STRETCH} doublings, and snaps back to the base on the first active
     * tick, so an idle fleet probes at a quarter of the rate and a busy one as before. Probe
     * timeouts are judged on every tick regardless.
     *
     * @param prober the prober
     * @param quiet  whether nothing moved since the last tick
     */
    public synchronized void tick(Prober prober, boolean quiet) {
        Objects.requireNonNull(prober, "prober");
        long now = nowMillis();
        if (!quiet) {
            activeSinceProbe = true;
            quietCycles = 0;
        }
        // A late wake (SPEC v0.1.37 §9.4): this tick runs more than the 30 s bound after the deadline it was due at,
        // with no wall step (that would be the node's own wake): the process was frozen, or its monotonic clock counted
        // a sleep. Every member went on meanwhile; one past its TTL only because this node did not run is probed, not
        // dropped.
        long armed = armedDueMillis;
        boolean late = armed != Long.MAX_VALUE && now - armed > LATE_WAKE_MILLIS;
        if (late) {
            lateTicks.incrementAndGet();
        }

        // Expire members whose lease of our attention ran out. The maps are walked in place, without a copy
        // (srad/agentspaces-event-loop.md EL-L-06): a concurrent map's iterator tolerates the removals below. A member
        // that keeps itself alive is judged at the configured TTL, one heard only through probes at the cycle-bound
        // TTL, and a fall of that TTL holds for a judgement already started (EL-E-05, §3.11).
        long cycleTtl = settleTtl();
        for (Member m : members.values()) {
            if (now - m.lastHeardMillis > ttlFor(m, cycleTtl)) {
                if (now < m.reprobeUntilMillis) {
                    continue; // the late wake's probe judges it
                }
                if (late && m.reprobeUntilMillis == Long.MIN_VALUE) {
                    reprobe(prober, m, now);
                    continue;
                }
                dropMember(m.id);
            }
        }

        // Resolve timed-out probes: a direct PING or a holder round escalates to an indirect round; a drop follows
        // only an indirect round.
        for (Map.Entry<Long, Pending> e : pendingProbes.entrySet()) {
            Pending pending = e.getValue();
            if (now < pending.deadlineMillis()) {
                continue;
            }
            pendingProbes.remove(e.getKey());
            Member target = members.get(pending.target());
            if (target == null) {
                continue;
            }
            if (pending.stage() == Stage.WAKE
                    && prober.progressSince(pending.target(), pending.deadlineMillis() - config.pingTimeout().toMillis())) {
                // Bytes of a frame from the seed are still arriving: it is answering (SPEC v0.1.37 §9.4).
                wakeBytesAnswers.incrementAndGet();
                target.heard(now);
                continue;
            }
            if (pending.stage() == Stage.WAKE) {
                // The seed did not answer the wake's PING within the ping timeout: it is dropped at once, with no
                // indirect round, however young its link (the monotonic clock did not count the sleep), and the node
                // fails over to its other seeds (owner decision 3 of 2026-11-23).
                wakeDrops.incrementAndGet();
                wakeDropped.add(pending.target());
                dropMember(pending.target());
                continue;
            }
            if (pending.stage() != Stage.INDIRECT) {
                target.suspect = true;
                activeSinceProbe = true; // doubt about a member: probe on the base interval again
                quietCycles = 0;
                boolean afterHolders = pending.stage() == Stage.HOLDER;
                Round round = indirectRound(prober, pending.target(), afterHolders);
                if (round.empty()) {
                    dropAfterProbe(pending.target(), pending.fromHolder());
                    continue;
                }
                if (afterHolders) {
                    escalations.incrementAndGet();
                }
                long nonce = nextNonce();
                pendingProbes.put(nonce, new Pending(pending.target(),
                        now + config.pingTimeout().toMillis(), Stage.INDIRECT, round.all(), pending.fromHolder()));
                ask(prober, round, pending.target(), nonce); // after the record: an answer may come back inline
            } else {
                dropAfterProbe(pending.target(), pending.fromHolder());
            }
        }

        PeerId wokeSeed = null;
        if (wakeTarget != null) {
            wokeSeed = wakeTarget;
            wakeTarget = null;
            wakeProbe(prober, wokeSeed, now); // before the resume probes, which then skip the seed
        }
        if (probeEachRequested) {
            probeEachRequested = false;
            probeEach(prober, now, wokeSeed); // EL-C-14: back from a possible absence, every member at once
        }

        // Probe one random member once per protocol period (SWIM): probing on every 250 ms tick
        // was four fifths of an idle peer's frames, and detection speed comes from the suspicion
        // timeout, not the probe rate. Timeouts above are still judged on every tick.
        int stretch = (int) probeStretchPace.get();
        long interval = stretchedIntervalMillis(quietCycles);
        if (lastProbeMillis != Long.MIN_VALUE && now - lastProbeMillis < interval) {
            return;
        }
        long previousTurn = lastProbeMillis;
        lastProbeMillis = now;
        if (!activeSinceProbe) {
            quietCycles = Math.min(quietCycles + 1, stretch);
        }
        activeSinceProbe = false;
        // Round-robin over a shuffled order (SWIM's protocol period), so every member is probed
        // once per cycle and detection time is bounded, rather than a random draw each period.
        PeerId target = nextProbeTarget();
        Member probed = target == null ? null : members.get(target);
        if (probed != null && previousTurn != Long.MIN_VALUE && probed.lastContactMillis != Long.MIN_VALUE
                && probed.lastContactMillis >= previousTurn && !prober.carriesNews(target, previousTurn)) {
            // Idle-loop O6 (reviews/IDLE-LOOP-2026-11-18.md): the member's own verified frame reached this node since
            // this node's previous probe turn, so a probe would learn nothing the frame did not show; with nothing new
            // to carry, it rests. Measured from the previous turn, not a sliding interval: two members whose turns fall
            // together each see the other's probe of the same instant as news since their last turn, so one of them
            // (or both, on alternate turns) rests; a sliding interval saw it one interval old and neither ever rested.
            // Its turn is spent as if probed: one that falls silent now is found as it would have been after an answer.
            probesRested.incrementAndGet();
            return;
        }
        if (target != null) {
            long nonce = nextNonce();
            List<PeerId> holders = usableHolders(prober.linkHolders(target), target);
            if (!holders.isEmpty()) {
                // TR-2: this node cannot reach the target, whose PING would go nowhere; its holders are asked at once,
                // with one ping timeout. A routine holder round is no doubt about the target: it neither suspects it
                // nor snaps the quiet stretch back, or a dial-only peer would hold the group at the base interval.
                holderRounds.incrementAndGet();
                pendingProbes.put(nonce, new Pending(target, now + config.pingTimeout().toMillis(), Stage.HOLDER,
                        java.util.Set.copyOf(holders), true));
                askHolders(prober, holders, target, nonce);
                return;
            }
            directPings.incrementAndGet();
            pendingProbes.put(nonce, new Pending(target,
                    now + config.pingTimeout().toMillis(), Stage.DIRECT, java.util.Set.of(), false));
            prober.ping(target, nonce);
        }
    }

    private void dropAfterProbe(PeerId target, boolean fromHolder) {
        (fromHolder ? dropsAfterHolderRound : dropsAfterIndirectRound).incrementAndGet();
        dropMember(target);
    }

    /**
     * Looks up a member.
     *
     * @param peer the member id
     * @return the member, when present in the view
     */
    public Optional<Member> member(PeerId peer) {
        return Optional.ofNullable(members.get(peer));
    }

    /**
     * How long a peer has been in this view without a break, on the node's clock (RE-H-04): a writer the per-entry
     * horizon trusts must have been in sight for as long as any copy it renewed out of sight could stay live.
     *
     * @param peer the member id
     * @return the time since it entered the view, or -1 when it is not in the view
     */
    public long inViewForMillis(PeerId peer) {
        Member member = members.get(peer);
        return member == null ? -1 : Math.max(0, nowMillis() - member.joinedMillis);
    }

    /**
     * When a peer entered this view for its current stay, on the node's clock (RE-H-04). A member that leaves and
     * returns gets a new time, so a reader can tell one stay from the next.
     *
     * @param peer the member id
     * @return the node-clock time it entered the view, or -1 when it is not in the view
     */
    public long joinedAtMillis(PeerId peer) {
        Member member = members.get(peer);
        return member == null ? -1 : member.joinedMillis;
    }

    /**
     * Takes a member out of the view: the view's version moves, the member joins the departures the node drains, and
     * the probes awaiting its answer go with it, so one re-admitted soon after is not dropped again by a probe of its
     * earlier stay (found by HorizonLaggingCopyTest, 2026-11-18).
     */
    private void dropMember(PeerId peer) {
        if (members.remove(peer) != null) {
            viewVersion.incrementAndGet();
            departed.add(peer);
            pendingProbes.values().removeIf(pending -> pending.target().equals(peer));
            holderPingReqsByRelay.remove(peer);
            changed();
        }
    }

    // ---- the node loop's view of the membership (srad/agentspaces-event-loop.md EL-L-06)

    private volatile Runnable onChange = () -> { };

    /**
     * Sets what runs when the view changes (a member enters or leaves) or a runtime pace moves: the owning node asks
     * the group's participants again (EL-L-03). It runs on the thread that made the change and must only wake.
     *
     * @param listener the listener
     */
    public void onChange(Runnable listener) {
        this.onChange = Objects.requireNonNull(listener, "listener");
    }

    private void changed() {
        onChange.run();
    }

    /**
     * When {@link #tick} next has work, on the node's clock (EL-L-06): the earliest of a member's TTL expiry, a
     * pending probe's timeout and the next probe turn. {@link Long#MAX_VALUE} when the view is empty and no probe
     * waits. Reads in place, without a copy.
     *
     * @return the next due time in the clock's milliseconds, or {@link Long#MAX_VALUE}
     */
    public synchronized long nextDueMillis() {
        long next = nextDue();
        armedDueMillis = next;
        return next;
    }

    /** {@link #nextDueMillis()} without noting it as the deadline the next tick is judged late against. */
    private long nextDue() {
        long next = Long.MAX_VALUE;
        long cycleTtl = settleTtl();
        boolean anyone = false;
        for (Member m : members.values()) {
            anyone = true;
            // a member a late wake re-probes is judged by its probe until the probe's span ends
            next = Math.min(next, Math.max(m.lastHeardMillis + ttlFor(m, cycleTtl) + 1, m.reprobeUntilMillis));
        }
        for (Pending pending : pendingProbes.values()) {
            next = Math.min(next, pending.deadlineMillis());
        }
        if (wakeTarget != null || probeEachRequested) {
            next = Math.min(next, nowMillis()); // a wake's probes go on the next run
        }
        if (anyone) {
            next = Math.min(next, lastProbeMillis == Long.MIN_VALUE ? nowMillis()
                    : lastProbeMillis + stretchedIntervalMillis(quietCycles));
        }
        return next;
    }

    /**
     * The probe interval in effect now, after the quiet stretch so far: the base on a busy group, up to the stretch
     * ceiling on a quiet one (SPEC §5.2; srad/agentspaces-event-loop.md EL-L-13, {@code core.activity.snapback}).
     *
     * @return the interval in milliseconds
     */
    public synchronized long currentProbeIntervalMillis() {
        return stretchedIntervalMillis(quietCycles);
    }

    /**
     * The members in view, counted without a copy.
     *
     * @return how many
     */
    public int size() {
        return members.size();
    }

    private final java.util.concurrent.ConcurrentLinkedQueue<PeerId> departed =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    /**
     * The members that left the view since the last call, each once per departure, oldest first. The node handles a
     * loss from here rather than by comparing views from tick to tick: a member that left and came back between two
     * ticks is a departure all the same, and its loss is recorded (and retried) even though both views hold it.
     *
     * @return the departures since the last drain
     */
    public List<PeerId> drainDeparted() {
        List<PeerId> out = new ArrayList<>();
        for (PeerId peer; (peer = departed.poll()) != null; ) {
            out.add(peer);
        }
        return out;
    }

    /**
     * Whether a member left the view and the node has not drained that departure yet: its loss is not recorded yet.
     *
     * @param peer the member
     * @return true while the departure waits to be drained
     */
    public boolean departurePending(PeerId peer) {
        return departed.contains(peer);
    }

    private final java.util.concurrent.atomic.AtomicLong viewVersion = new java.util.concurrent.atomic.AtomicLong();

    /**
     * A counter that moves whenever a member enters or leaves this view (idle-loop O5/O8): what is derived from the
     * view is recomputed only when it moved. Only its equality between two reads means anything.
     *
     * @return the view's version
     */
    public long viewVersion() {
        return viewVersion.get();
    }

    /** Returns a snapshot of all current members. */
    public List<Member> allMembers() {
        return List.copyOf(members.values());
    }

    /**
     * Returns the members currently advertising a topology role.
     *
     * @param role the role
     * @return members offering it
     */
    public List<PeerId> withRole(PeerAdvertisement.PeerRole role) {
        List<PeerId> matching = new ArrayList<>();
        for (Member member : members.values()) {
            if (member.roles.contains(role)) {
                matching.add(member.id);
            }
        }
        return matching;
    }

    @Override
    public List<PeerId> randomMembers(int n) {
        return randomMembersExcluding(n, java.util.Set.of());
    }

    private List<PeerId> randomMembersExcluding(int n, java.util.Set<PeerId> excluded) {
        List<PeerId> ids = new ArrayList<>();
        for (Member m : members.values()) {
            if (!excluded.contains(m.id)) {
                ids.add(m.id);
            }
        }
        Collections.shuffle(ids, random);
        return ids.size() <= n ? ids : ids.subList(0, n);
    }

    private synchronized long nextNonce() {
        return nonceRandom.nextLong();
    }

    private long nowMillis() {
        return localMillis.getAsLong();
    }

    /**
     * This view's now, on the base its periods are reckoned on (see the class comment).
     *
     * @return the local millis
     */
    public long localMillis() {
        return nowMillis();
    }

    /** Set by {@link #probeEachOnce()}; the next {@link #tick} probes every member once. Guarded by this. */
    private boolean probeEachRequested;
    private final java.util.concurrent.atomic.AtomicLong resumeProbes = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Asks the next {@link #tick} to probe every member once, each by its own direct PING or holder round, with the
     * ordinary ping timeout and escalation, instead of waiting out a probe cycle (srad/agentspaces-event-loop.md
     * EL-C-14): a node back from a suspend or a long freeze learns at once which members are still there.
     */
    public synchronized void probeEachOnce() {
        probeEachRequested = true;
    }

    /**
     * Probes sent by {@link #probeEachOnce()} (EL-C-14).
     *
     * @return the count
     */
    public long resumeProbes() {
        return resumeProbes.get();
    }

    /** The seed a wake asked to PING at the next {@link #tick} ({@link #probeAtWake}), or null. Guarded by this. */
    private PeerId wakeTarget;
    /** Seeds dropped because they left the wake's PING unanswered, until the node consumes the mark. */
    private final java.util.Set<PeerId> wakeDropped = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong wakeDrops = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong wakeAdopted = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Asks the next {@link #tick} to PING the seed this node is linked through, after a wake (owner decision 3 of
     * 2026-11-23): an answer within the ping timeout keeps it; silence drops it at once, with no indirect round. A
     * direct PING already waiting for the seed is not sent again: it becomes the wake's, with its own deadline.
     *
     * @param seed the member this node's seed link answered from
     */
    public synchronized void probeAtWake(PeerId seed) {
        wakeTarget = Objects.requireNonNull(seed, "seed");
    }

    /**
     * A resume's two asks at once (srad/agentspaces-event-loop.md §10.51): the seed's wake PING ({@link #probeAtWake},
     * when {@code seed} is not null) and one probe of every other member ({@link #probeEachOnce}). Set under one lock,
     * so the same {@link #tick} sends the wake's PING first and the resume probes then skip the seed; set apart, a tick
     * between them could take the PING's answer before the resume probes ran, and the seed was probed twice.
     *
     * @param seed the seed link's member, or null for none
     */
    public synchronized void probeAtResume(PeerId seed) {
        if (seed != null) {
            wakeTarget = seed;
        }
        probeEachRequested = true;
    }

    /**
     * Whether a peer left this view because it left a wake's PING unanswered ({@link #probeAtWake}), consuming the
     * mark: the node then fails over to its other seeds at once.
     *
     * @param peer the peer that left the view
     * @return true once, if a wake's PING dropped it
     */
    public boolean consumeWakeDropped(PeerId peer) {
        return wakeDropped.remove(peer);
    }

    /**
     * Seeds dropped by a wake's unanswered PING ({@link #probeAtWake}).
     *
     * @return the count
     */
    public long wakeDrops() {
        return wakeDrops.get();
    }

    /**
     * Wakes that found a direct PING already waiting for the seed and made it theirs rather than send another.
     *
     * @return the count
     */
    public long wakePingsAdopted() {
        return wakeAdopted.get();
    }

    /** The wake's PING to the seed, or the adoption of one already waiting. Called from {@link #tick}, under this. */
    private void wakeProbe(Prober prober, PeerId seed, long now) {
        if (!members.containsKey(seed) || seed.equals(self)) {
            return;
        }
        for (Map.Entry<Long, Pending> e : pendingProbes.entrySet()) {
            Pending p = e.getValue();
            if (!p.target().equals(seed)) {
                continue;
            }
            if (p.stage() == Stage.DIRECT) {
                wakeAdopted.incrementAndGet(); // the PING already out becomes the wake's; its deadline stands
                pendingProbes.put(e.getKey(), new Pending(seed, p.deadlineMillis(), Stage.WAKE, p.relays(),
                        p.fromHolder()));
            }
            return; // a holder or indirect round already judges it
        }
        long nonce = nextNonce();
        pendingProbes.put(nonce, new Pending(seed, now + config.pingTimeout().toMillis(), Stage.WAKE,
                java.util.Set.of(), false));
        prober.wakePing(seed, nonce); // after the record: an answer may come back inline
    }

    /** A tick more than this late past its deadline is a late wake (SPEC v0.1.37 §9.4, the 30 s keepalive bound). */
    public static final long LATE_WAKE_MILLIS = KEEPALIVE_TTL.toMillis();
    /** The deadline {@link #nextDueMillis()} last gave, which the node's loop armed; {@link Long#MAX_VALUE} for none. */
    private long armedDueMillis = Long.MAX_VALUE;
    private final java.util.concurrent.atomic.AtomicLong lateTicks = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong lateReprobes = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong wakeBytesAnswers = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Ticks that ran more than {@link #LATE_WAKE_MILLIS} past their deadline (a frozen process, SPEC v0.1.37 §9.4).
     *
     * @return the count
     */
    public long lateTicks() {
        return lateTicks.get();
    }

    /**
     * Members a late tick found past their TTL and probed rather than dropped (SPEC v0.1.37 §9.4).
     *
     * @return the count
     */
    public long lateReprobes() {
        return lateReprobes.get();
    }

    /**
     * Wake PINGs to the seed answered by bytes of a frame still arriving rather than a whole frame (§9.4).
     *
     * @return the count
     */
    public long wakeBytesAnswers() {
        return wakeBytesAnswers.get();
    }

    /**
     * Probes a member a late tick found past its TTL (SPEC v0.1.37 §9.4): a probe already out for it judges it; else a
     * direct PING or a holder round, with the ordinary timeout and escalation. Until that probe's whole span (the
     * direct or holder stage, then the indirect one) has passed, the TTL does not drop it. Under this.
     */
    private void reprobe(Prober prober, Member m, long now) {
        lateReprobes.incrementAndGet();
        m.reprobeUntilMillis = now + 2 * config.pingTimeout().toMillis() + 1;
        for (Pending p : pendingProbes.values()) {
            if (p.target().equals(m.id)) {
                return;
            }
        }
        long nonce = nextNonce();
        List<PeerId> holders = usableHolders(prober.linkHolders(m.id), m.id);
        if (!holders.isEmpty()) {
            holderRounds.incrementAndGet();
            pendingProbes.put(nonce, new Pending(m.id, now + config.pingTimeout().toMillis(), Stage.HOLDER,
                    java.util.Set.copyOf(holders), true));
            askHolders(prober, holders, m.id, nonce);
            return;
        }
        directPings.incrementAndGet();
        pendingProbes.put(nonce, new Pending(m.id, now + config.pingTimeout().toMillis(), Stage.DIRECT,
                java.util.Set.of(), false));
        prober.ping(m.id, nonce); // after the record: an answer may come back inline
    }

    /** Probes every member not already under a probe, once (EL-C-14). Called from {@link #tick}, under this. */
    private void probeEach(Prober prober, long now, PeerId wokeSeed) {
        java.util.Set<PeerId> pending = new java.util.HashSet<>();
        // The seed this tick's wake already PINGed is skipped by name, not only while its probe is pending: onAck takes
        // no lock, so the seed's answer may remove that probe on another thread between the wake's PING and this
        // loop, and the seed was then probed twice (EL-SP-16, the core reactor of 2026-11-23).
        if (wokeSeed != null) {
            pending.add(wokeSeed);
        }
        for (Pending p : pendingProbes.values()) {
            pending.add(p.target());
        }
        for (Member m : members.values()) {
            if (m.id.equals(self) || pending.contains(m.id)) {
                continue;
            }
            long nonce = nextNonce();
            List<PeerId> holders = usableHolders(prober.linkHolders(m.id), m.id);
            resumeProbes.incrementAndGet();
            if (!holders.isEmpty()) {
                holderRounds.incrementAndGet();
                pendingProbes.put(nonce, new Pending(m.id, now + config.pingTimeout().toMillis(), Stage.HOLDER,
                        java.util.Set.copyOf(holders), true));
                askHolders(prober, holders, m.id, nonce);
                continue;
            }
            directPings.incrementAndGet();
            pendingProbes.put(nonce, new Pending(m.id, now + config.pingTimeout().toMillis(), Stage.DIRECT,
                    java.util.Set.of(), false));
            prober.ping(m.id, nonce); // after the record: an answer may come back inline
        }
    }
}
