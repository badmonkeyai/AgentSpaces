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
 */
public final class GroupMembership implements PeerSampler {

    /**
     * Membership tuning.
     *
     * @param memberTtl     how long a silent member stays in the view
     * @param pingTimeout   how long to wait for an ACK before indirect probing
     * @param indirectPeers how many peers to ask in the indirect round
     */
    public record Config(Duration memberTtl, Duration pingTimeout, int indirectPeers, Duration probeInterval) {

        /** The SWIM protocol period: one probe per member per interval (memberlist's default, 1 s). */
        public static final Duration DEFAULT_PROBE_INTERVAL = Duration.ofSeconds(1);

        public Config {
            if (probeInterval == null) {
                probeInterval = DEFAULT_PROBE_INTERVAL;
            }
        }

        /** The three-argument form, probing once per second. */
        public Config(Duration memberTtl, Duration pingTimeout, int indirectPeers) {
            this(memberTtl, pingTimeout, indirectPeers, DEFAULT_PROBE_INTERVAL);
        }

        /** Returns defaults: TTL 30s, ping timeout 2s, 2 indirect peers, one probe per second. */
        public static Config defaults() {
            return new Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2, DEFAULT_PROBE_INTERVAL);
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

        private Member(PeerId id, List<PeerAdvertisement.Endpoint> endpoints, long lastHeardMillis) {
            this.id = id;
            this.endpoints = endpoints;
            this.lastHeardMillis = lastHeardMillis;
            this.joinedMillis = lastHeardMillis;
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
    }

    private record Pending(PeerId target, long deadlineMillis, boolean indirectStage,
                           java.util.Set<PeerId> relays) {
    }

    private final PeerId self;
    private volatile java.util.function.Predicate<PeerId> refused = peer -> false;
    private final InstantSource clock;
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
        members.compute(ad.issuer(), (id, existing) -> {
            if (existing == null) {
                activeSinceProbe = true; // a newcomer: probe on the base interval again
                quietCycles = 0;
                viewVersion.incrementAndGet();
            }
            Member member = existing == null
                    ? new Member(id, ad.endpoints(), now) : existing;
            member.endpoints = ad.endpoints();
            member.roles = ad.roles();
            member.lastHeardMillis = now;
            member.suspect = false;
            return member;
        });
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
            member.lastHeardMillis = now;
            member.lastContactMillis = now;
            member.suspect = false;
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
            member.lastHeardMillis = nowMillis();
            member.suspect = false;
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
     * item 3): the base times two to the stretch ceiling, but past four times the base no longer than the member TTL
     * over twice the others, so this node's probe cycle reaches every member twice inside the TTL. Groups too large
     * for that keep the four-times stretch, and the advertisement refresh is their keepalive (ROBUSTNESS.md §6b).
     *
     * @param others the members besides this node
     * @return the interval in milliseconds
     */
    public long maxQuietIntervalMillis(int others) {
        int stretch = (int) probeStretchPace.get();
        long base = probeIntervalPace.get();
        long legacy = base * (1L << Math.min(stretch, LEGACY_QUIET_STRETCH));
        long full = base * (1L << stretch);
        long ttlBound = config.memberTtl().toMillis() / (2L * Math.max(1, others));
        return Math.min(full, Math.max(legacy, ttlBound));
    }

    /** The probe interval in effect after {@code cycles} quiet probe cycles. */
    private long stretchedIntervalMillis(int cycles) {
        int stretch = (int) probeStretchPace.get();
        long stretched = probeIntervalPace.get() * (1L << Math.min(cycles, stretch));
        return Math.min(stretched, Math.max(probeIntervalPace.get(), maxQuietIntervalMillis(members.size())));
    }
    /** The members still to probe in this cycle, in a shuffled order; refilled when exhausted. */
    private final java.util.ArrayDeque<PeerId> probeCycle = new java.util.ArrayDeque<>();

    private PeerId nextProbeTarget() {
        while (true) {
            PeerId next = probeCycle.pollFirst();
            if (next == null) {
                List<PeerId> ids = new ArrayList<>();
                for (PeerId id : members.keySet()) {
                    if (!id.equals(self)) {
                        ids.add(id);
                    }
                }
                if (ids.isEmpty()) {
                    return null;
                }
                Collections.shuffle(ids, random);
                probeCycle.addAll(ids);
                continue;
            }
            if (members.containsKey(next)) {
                return next;
            }
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

        // Expire members whose lease of our attention ran out.
        for (Member m : List.copyOf(members.values())) {
            if (now - m.lastHeardMillis > config.memberTtl().toMillis()) {
                dropMember(m.id);
            }
        }

        // Resolve timed-out probes: escalate direct to indirect, drop after indirect.
        for (Map.Entry<Long, Pending> e : List.copyOf(pendingProbes.entrySet())) {
            Pending pending = e.getValue();
            if (now < pending.deadlineMillis()) {
                continue;
            }
            pendingProbes.remove(e.getKey());
            Member target = members.get(pending.target());
            if (target == null) {
                continue;
            }
            if (!pending.indirectStage()) {
                target.suspect = true;
                activeSinceProbe = true; // doubt about a member: probe on the base interval again
                quietCycles = 0;
                List<PeerId> relays = randomMembersExcluding(config.indirectPeers(), pending.target());
                if (relays.isEmpty()) {
                    dropMember(pending.target());
                    continue;
                }
                long nonce = nextNonce();
                pendingProbes.put(nonce, new Pending(pending.target(),
                        now + config.pingTimeout().toMillis(), true,
                        java.util.Set.copyOf(relays)));
                for (PeerId relay : relays) {
                    prober.pingReq(relay, pending.target(), nonce);
                }
            } else {
                dropMember(pending.target());
            }
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
            pendingProbes.put(nonce, new Pending(target,
                    now + config.pingTimeout().toMillis(), false, java.util.Set.of()));
            prober.ping(target, nonce);
        }
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
        }
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
        return randomMembersExcluding(n, null);
    }

    private List<PeerId> randomMembersExcluding(int n, PeerId excluded) {
        List<PeerId> ids = new ArrayList<>();
        for (Member m : members.values()) {
            if (!m.id.equals(excluded)) {
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
        return clock.instant().toEpochMilli();
    }
}
