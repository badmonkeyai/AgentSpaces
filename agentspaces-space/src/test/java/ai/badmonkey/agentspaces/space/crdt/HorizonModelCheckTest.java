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
package ai.badmonkey.agentspaces.space.crdt;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RE-H-05 and RE-Q-REL-04 of srad/replication-by-entry.md: an exhaustive explicit-state check of the per-entry
 * tombstone horizon of §3.5, the Java twin of {@code docs/proofs/HorizonPerEntry.tla}. The two describe the same
 * model with the same variables and actions, so TLC's count of distinct states for {@code HorizonPerEntry.cfg} should
 * equal {@link #MAIN}'s count printed here; a difference means the two models differ and is worth a look.
 *
 * <p>The model: three replicas and one entry. Replica 1 (index 0) is the issuer: it writes, renews and cancels.
 * Replicas 2 and 3 may claim and complete. A copy travels as a whole state through a set of messages that loses,
 * reorders and delays them, and a partition cuts one replica off. Each replica has its own clock, any two within
 * {@code maxDrift}. A replica collects a state once its horizon has passed on its own clock, and its door refuses a
 * copy whose horizon has passed on that same clock. Breadth-first search visits every reachable state up to the clock
 * bound and checks the invariants in each one.
 *
 * <p>Time is in abstract units, and {@code grace} stands for the five minutes of SPEC §7.3. The argument of §3.5 is
 * parametric in both: it needs only that a withdrawal's horizon is never shorter than the lapse horizon the door
 * applies to a present copy. NoLateLive, the step property that a replica which never heard of the withdrawal does not
 * turn the entry live once another has collected it, needs one thing more: a withdrawal keeps at least
 * {@code maxDrift} (the drift floor of SPEC §7.3, v0.1.35, owner decision 22), so that every clock reads L or later
 * when the first replica collects. {@link #theModelsLegacyRuleIsTheRealOne} ties the model's unhinted rule to
 * {@link SpaceStateCrdt#collectionHorizon} at the real five minutes.
 */
class HorizonModelCheckTest {

    /**
     * One run's constants, the TLA+ model's CONSTANTS.
     *
     * @param maxTime    bound on every local clock
     * @param maxDrift   bound on the difference between any two local clocks
     * @param maxLease   the longest extension a write or renewal asks for, and the maxLease of the horizon
     * @param grace      the five-minute grace of SPEC §7.3
     * @param maxStamp   bound on the lease writes, which keeps a shortening writer's state space finite
     * @param maxMsgs    bound on the copies in flight
     * @param perEntry   every member advertises {@code wire.horizon=1}: a withdrawal is collectable at L + grace, and
     *                   with the floor no sooner than L + maxDrift
     * @param shortening break 1: a renewal sets {@code now + extension} even when that is shorter
     * @param twoIssuers break 2: the issuer's identity renews from replicas 1 and 2
     * @param driftFloor a withdrawal keeps at least {@code maxDrift} under either rule (SPEC §7.3, v0.1.35); off only
     *                   to show what the floor is for
     */
    record Config(int maxTime, int maxDrift, int maxLease, int grace, int maxStamp, int maxMsgs,
                  boolean perEntry, boolean shortening, boolean twoIssuers, boolean driftFloor) {

        /** A run under the rule as specified, with the drift floor. */
        Config(int maxTime, int maxDrift, int maxLease, int grace, int maxStamp, int maxMsgs,
               boolean perEntry, boolean shortening, boolean twoIssuers) {
            this(maxTime, maxDrift, maxLease, grace, maxStamp, maxMsgs, perEntry, shortening, twoIssuers, true);
        }

        Config legacy() {
            return new Config(maxTime, maxDrift, maxLease, grace, maxStamp, maxMsgs, false, shortening, twoIssuers,
                    driftFloor);
        }

        Config withShortening() {
            return new Config(maxTime, maxDrift, maxLease, grace, maxStamp, maxMsgs, perEntry, true, twoIssuers,
                    driftFloor);
        }

        Config withTwoIssuers() {
            return new Config(maxTime, maxDrift, maxLease, grace, maxStamp, maxMsgs, perEntry, shortening, true,
                    driftFloor);
        }

        /** The rule before owner decision 22: a withdrawal keeps the grace or 2 x maxLease alone. */
        Config withoutDriftFloor() {
            return new Config(maxTime, maxDrift, maxLease, grace, maxStamp, maxMsgs, perEntry, shortening, twoIssuers,
                    false);
        }
    }

    /**
     * The constants of {@code HorizonPerEntry.cfg}. A lease of up to 3 units beside a grace of 1 keeps the legacy
     * horizon (6) apart from the per-entry one (1), and lets a shortening writer or a second renewer push a copy's
     * expiry past L + grace, which the break runs must be able to reach. One copy in flight at a time: a copy still
     * waits as long as any run likes, and a replica that holds an old state can send it at any later step.
     */
    static final Config MAIN = new Config(4, 1, 3, 1, 3, 1, true, false, false);

    /**
     * The constants of {@code HorizonPerEntryReorder.cfg}: two copies in flight, so two deliveries can arrive in
     * either order. Leases of up to 2 units over a clock of 3 let a renewed entry be withdrawn and collected inside the
     * bound (write with expiry 1, renew to 2, cancel, collect at 3), with an older copy of the first lease still in
     * flight to the replica that collected: the case reordering is about. A clock of 2 with leases of 3 never got
     * there, since any renewal moved the expiry past the last collectable one.
     */
    static final Config REORDER = new Config(3, 1, 2, 1, 3, 2, true, false, false);

    /**
     * The constants of {@code HorizonPerEntryDrift.cfg}: the main run with clocks up to two units apart, twice the
     * grace, as SPEC's MAX_DRIFT of ten minutes is twice the five-minute grace. With the drift floor a withdrawal keeps
     * two units, and every invariant and {@link Inv#NO_LATE_LIVE} hold; without it ({@code
     * HorizonPerEntryDriftNoFloor.cfg}) NoLateLive breaks, the trailing-clock window the floor closes.
     */
    static final Config DRIFT = new Config(4, 2, 3, 1, 3, 1, true, false, false);

    /**
     * The constants of {@code HorizonPerEntryLegacyDrift.cfg}: the legacy rule with a maximum lease of one unit and
     * clocks up to three apart, so that {@code 2 x maxLease} is shorter than the drift, as two minutes are against
     * SPEC's ten. With the floor a withdrawal keeps three units and NoLateLive holds; without it ({@code
     * HorizonPerEntryLegacyDriftNoFloor.cfg}) the legacy rule has the same window as the per-entry one had.
     */
    static final Config LEGACY_DRIFT = new Config(4, 3, 1, 1, 3, 1, false, false, false);

    /** The invariants of the TLA+ model. */
    enum Inv {
        /** A replica that held the entry withdrawn never again holds it present and unexpired by its own clock. */
        NO_RESURRECTION,
        /** Every copy a replica's collected state subsumes is refused at that replica's door. */
        REFUSED_AT_EVERY_DOOR,
        /** Once withdrawn, no copy anywhere carries an expiry past the withdrawal's L (the lemma of §3.5). */
        LEASE_BOUND,
        /** A replica that collected a completed state never holds the entry present and unexpired again. */
        COMPLETION_STAYS_DONE,
        /**
         * A step property, checked on every transition: once some replica has collected a withdrawal, no replica
         * turns the entry live, including one that never heard of the withdrawal. It holds when a withdrawal keeps at
         * least the drift between clocks, which the drift floor ensures, and breaks without the floor once the drift
         * exceeds what the rule keeps (the TLA+ model's {@code NoLateLive}).
         */
        NO_LATE_LIVE
    }

    static final Set<Inv> SAFETY = EnumSet.of(Inv.NO_RESURRECTION, Inv.REFUSED_AT_EVERY_DOOR, Inv.LEASE_BOUND,
            Inv.NO_LATE_LIVE);

    private static final int N = 3;
    private static final int ISSUER = 0;
    private static final int[] NONE = new int[0];

    // ---- one replica's state of the entry, packed: rm | cp << 1 | exp << 2 | stp << 8; 0 is "holds nothing" ----

    static int pack(boolean rm, boolean cp, int exp, int stp) {
        return (rm ? 1 : 0) | (cp ? 2 : 0) | (exp << 2) | (stp << 8);
    }

    static boolean rm(int s) {
        return (s & 1) != 0;
    }

    static boolean cp(int s) {
        return (s & 2) != 0;
    }

    static int exp(int s) {
        return (s >> 2) & 63;
    }

    static int stp(int s) {
        return (s >> 8) & 31;
    }

    static boolean held(int s) {
        return stp(s) > 0;
    }

    static boolean present(int s) {
        return !rm(s) && !cp(s);
    }

    /** Merge of two copies: removal and completion are monotone, the lease is the one with the higher stamp. */
    static int merge(int a, int b) {
        int lease = stp(a) >= stp(b) ? a : b;
        return pack(rm(a) || rm(b), cp(a) || cp(b), exp(lease), stp(lease));
    }

    /** What a state keeps past its lease expiry: the horizon rule under test. */
    static int keep(Config c, int s) {
        if (cp(s)) {
            // Completed entries keep 2 x maxLease in both modes; one withdrawn as well keeps the drift floor too, so it
            // never goes before a copy that is withdrawn alone (RefusedAtEveryDoor).
            return rm(s) && c.driftFloor() ? Math.max(2 * c.maxLease(), c.maxDrift()) : 2 * c.maxLease();
        }
        if (rm(s)) {
            int keep = c.perEntry() ? c.grace() : 2 * c.maxLease();
            return c.driftFloor() ? Math.max(keep, c.maxDrift()) : keep; // the drift floor of SPEC §7.3, v0.1.35
        }
        return Math.min(2 * c.maxLease(), c.grace()); // the lapse horizon, normative since v0.1.35
    }

    /** {@link SpaceStateCrdt#collectable(EntryState, long, long)} in the model's units, with the horizon rule above. */
    static boolean collectable(Config c, int s, int now) {
        return (!present(s) || exp(s) <= now) && exp(s) + keep(c, s) <= now;
    }

    /** Merging {@code c} into {@code k} changes nothing: {@code c} is a copy of what {@code k} already says. */
    static boolean subsumed(int c, int k) {
        return (!rm(c) || rm(k)) && (!cp(c) || cp(k)) && stp(c) <= stp(k);
    }

    // ---- the whole model state ----

    static final class S {
        final int[] st = new int[N];
        final int[][] gone = {NONE, NONE, NONE};
        final boolean[] knew = new boolean[N];
        final boolean[] claimed = new boolean[N];
        final int[] clock = new int[N];
        int[] msgs = NONE; // sorted, each to << 13 | state
        int lease; // the TLA+ model's L: the issuer's expiry when it cancelled, 0 before
        int ctr;

        S copy() {
            S t = new S();
            System.arraycopy(st, 0, t.st, 0, N);
            System.arraycopy(gone, 0, t.gone, 0, N); // the inner arrays are never written in place
            System.arraycopy(knew, 0, t.knew, 0, N);
            System.arraycopy(claimed, 0, t.claimed, 0, N);
            System.arraycopy(clock, 0, t.clock, 0, N);
            t.msgs = msgs;
            t.lease = lease;
            t.ctr = ctr;
            return t;
        }

        boolean live(int r) {
            return held(st[r]) && present(st[r]) && exp(st[r]) > clock[r];
        }

        /** Every copy of the entry: what each replica holds and what is in flight. */
        int[] copies() {
            int[] out = new int[N + msgs.length];
            int n = 0;
            for (int r = 0; r < N; r++) {
                if (held(st[r])) {
                    out[n++] = st[r];
                }
            }
            for (int m : msgs) {
                out[n++] = m & 0x1FFF;
            }
            return Arrays.copyOf(out, n);
        }

        /** A canonical encoding: one char per small field, sets sorted, so equal states give equal keys. */
        String key() {
            StringBuilder b = new StringBuilder(24);
            for (int r = 0; r < N; r++) {
                b.append((char) st[r]).append((char) clock[r]);
            }
            int flags = 0;
            for (int r = 0; r < N; r++) {
                flags |= (knew[r] ? 1 : 0) << r;
                flags |= (claimed[r] ? 1 : 0) << (r + N);
            }
            b.append((char) flags).append((char) lease).append((char) ctr);
            b.append((char) msgs.length);
            for (int m : msgs) {
                b.append((char) m);
            }
            for (int r = 0; r < N; r++) {
                b.append((char) gone[r].length);
                for (int g : gone[r]) {
                    b.append((char) g);
                }
            }
            return b.toString();
        }

        static S decode(String k) {
            S s = new S();
            int i = 0;
            for (int r = 0; r < N; r++) {
                s.st[r] = k.charAt(i++);
                s.clock[r] = k.charAt(i++);
            }
            int flags = k.charAt(i++);
            for (int r = 0; r < N; r++) {
                s.knew[r] = (flags & (1 << r)) != 0;
                s.claimed[r] = (flags & (1 << (r + N))) != 0;
            }
            s.lease = k.charAt(i++);
            s.ctr = k.charAt(i++);
            s.msgs = new int[k.charAt(i++)];
            for (int j = 0; j < s.msgs.length; j++) {
                s.msgs[j] = k.charAt(i++);
            }
            for (int r = 0; r < N; r++) {
                s.gone[r] = new int[k.charAt(i++)];
                for (int j = 0; j < s.gone[r].length; j++) {
                    s.gone[r][j] = k.charAt(i++);
                }
            }
            return s;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            for (int r = 0; r < N; r++) {
                b.append(" r").append(r + 1).append("{t=").append(clock[r]).append(' ').append(show(st[r]));
                if (knew[r]) {
                    b.append(" knew");
                }
                if (claimed[r]) {
                    b.append(" claimed");
                }
                if (gone[r].length > 0) {
                    b.append(" gone=");
                    for (int g : gone[r]) {
                        b.append(show(g));
                    }
                }
                b.append('}');
            }
            b.append(" msgs=[");
            for (int m : msgs) {
                b.append("to r").append((m >> 13) + 1).append(':').append(show(m & 0x1FFF)).append(' ');
            }
            return b.append("] L=").append(lease).toString();
        }
    }

    static String show(int s) {
        if (!held(s)) {
            return "-";
        }
        return "(" + (rm(s) ? "rm," : "") + (cp(s) ? "cp," : "") + "e=" + exp(s) + ",s=" + stp(s) + ")";
    }

    private static int[] withAdded(int[] set, int v) {
        int at = Arrays.binarySearch(set, v);
        if (at >= 0) {
            return set;
        }
        int ins = -at - 1;
        int[] out = new int[set.length + 1];
        System.arraycopy(set, 0, out, 0, ins);
        out[ins] = v;
        System.arraycopy(set, ins, out, ins + 1, set.length - ins);
        return out;
    }

    private static int[] withRemoved(int[] set, int index) {
        int[] out = new int[set.length - 1];
        System.arraycopy(set, 0, out, 0, index);
        System.arraycopy(set, index + 1, out, index, set.length - index - 1);
        return out;
    }

    /** Receives each successor with the name of the action that produced it. */
    interface Out {
        void next(String action, S t);
    }

    /** The Next relation of the TLA+ model, action by action and in the same terms. */
    static void successors(Config c, S s, Out out) {
        // Write: the issuer writes the entry once.
        if (s.ctr == 0) {
            for (int ext = 1; ext <= c.maxLease(); ext++) {
                S t = s.copy();
                t.st[ISSUER] = pack(false, false, s.clock[ISSUER] + ext, 1);
                t.ctr = 1;
                out.next("Write(ext=" + ext + ")", t);
            }
        }
        // Cancel: the issuer withdraws its present entry; L is the expiry its replica holds.
        if (held(s.st[ISSUER]) && present(s.st[ISSUER]) && s.lease == 0) {
            S t = s.copy();
            t.st[ISSUER] = s.st[ISSUER] | 1;
            t.knew[ISSUER] = true;
            t.lease = exp(s.st[ISSUER]);
            out.next("Cancel", t);
        }
        for (int r = 0; r < N; r++) {
            // Renew: an issuer replica holding the entry present and unexpired; the clamp is the real one.
            boolean renewer = r == ISSUER || (c.twoIssuers() && r == 1);
            if (renewer && s.live(r) && s.ctr < c.maxStamp()) {
                for (int ext = 1; ext <= c.maxLease(); ext++) {
                    int asked = s.clock[r] + ext;
                    int now = c.shortening() ? asked
                            : (int) SpaceStateCrdt.renewedExpiry(exp(s.st[r]), s.clock[r], ext);
                    if (now != exp(s.st[r])) {
                        S t = s.copy();
                        t.st[r] = pack(false, false, now, s.ctr + 1);
                        t.ctr = s.ctr + 1;
                        out.next("Renew(r" + (r + 1) + ",ext=" + ext + ")", t);
                    }
                }
            }
            // Claim and Complete: a taker holds a claim and completes while the entry is present there.
            if (r != ISSUER && s.live(r) && !s.claimed[r]) {
                S t = s.copy();
                t.claimed[r] = true;
                out.next("Claim(r" + (r + 1) + ")", t);
            }
            if (s.claimed[r] && held(s.st[r]) && present(s.st[r])) {
                S t = s.copy();
                t.st[r] = s.st[r] | 2;
                out.next("Complete(r" + (r + 1) + ")", t);
            }
            // Collect: the sweep drops a state whose horizon passed on this replica's clock.
            if (held(s.st[r]) && collectable(c, s.st[r], s.clock[r])) {
                S t = s.copy();
                t.gone[r] = withAdded(s.gone[r], s.st[r]);
                t.st[r] = 0;
                t.claimed[r] = false;
                out.next("Collect(r" + (r + 1) + ")", t);
            }
            // Tick: a clock moves on, never more than maxDrift ahead of another.
            boolean canTick = s.clock[r] < c.maxTime();
            for (int o = 0; o < N && canTick; o++) {
                canTick = s.clock[r] + 1 - s.clock[o] <= c.maxDrift();
            }
            if (canTick) {
                S t = s.copy();
                t.clock[r]++;
                out.next("Tick(r" + (r + 1) + ")", t);
            }
            // Send: any copy to any other replica, while there is room in flight. Nothing ever forces a send, so a
            // partition of any length is already a run of the model: the sends across it are the ones not taken.
            for (int to = 0; to < N; to++) {
                int m = (to << 13) | s.st[r];
                if (to != r && held(s.st[r]) && s.msgs.length < c.maxMsgs()
                        && Arrays.binarySearch(s.msgs, m) < 0) {
                    S t = s.copy();
                    t.msgs = withAdded(s.msgs, m);
                    out.next("Send(r" + (r + 1) + "->r" + (to + 1) + ")", t);
                }
            }
        }
        // Deliver or Lose any message in flight, in any order.
        for (int i = 0; i < s.msgs.length; i++) {
            int m = s.msgs[i];
            int to = m >> 13;
            int copy = m & 0x1FFF;
            S t = s.copy();
            t.msgs = withRemoved(s.msgs, i);
            if (!collectable(c, copy, s.clock[to])) {
                int now = held(s.st[to]) ? merge(s.st[to], copy) : copy;
                t.st[to] = now;
                t.knew[to] = s.knew[to] || rm(now);
                out.next("Deliver(" + show(copy) + "->r" + (to + 1) + ")", t);
            } else {
                out.next("Refuse(" + show(copy) + "->r" + (to + 1) + ")", t);
            }
            S lost = s.copy();
            lost.msgs = t.msgs;
            out.next("Lose(" + show(copy) + "->r" + (to + 1) + ")", lost);
        }
    }

    /** The invariants a state breaks, empty when it breaks none. */
    static Set<Inv> broken(Config c, S s, Set<Inv> checked) {
        Set<Inv> out = EnumSet.noneOf(Inv.class);
        int[] copies = s.copies();
        for (int r = 0; r < N; r++) {
            if (checked.contains(Inv.NO_RESURRECTION) && s.knew[r] && s.live(r)) {
                out.add(Inv.NO_RESURRECTION);
            }
            for (int k : s.gone[r]) {
                if (checked.contains(Inv.COMPLETION_STAYS_DONE) && cp(k) && s.live(r)) {
                    out.add(Inv.COMPLETION_STAYS_DONE);
                }
                if (checked.contains(Inv.REFUSED_AT_EVERY_DOOR)) {
                    for (int copy : copies) {
                        if (subsumed(copy, k) && !collectable(c, copy, s.clock[r])) {
                            out.add(Inv.REFUSED_AT_EVERY_DOOR);
                        }
                    }
                }
            }
        }
        if (checked.contains(Inv.LEASE_BOUND) && s.lease > 0) {
            for (int copy : copies) {
                if (exp(copy) > s.lease) {
                    out.add(Inv.LEASE_BOUND);
                }
            }
        }
        return out;
    }

    /**
     * What a run found.
     *
     * @param states      distinct reachable states visited
     * @param transitions successor states generated, a revisit included
     * @param depth       the deepest breadth-first level reached
     * @param broken      the invariants the first violating state breaks, empty when none
     * @param trace       the shortest path to that state, one action and the state after it per line
     * @param danger      states where a replica has collected a withdrawn tombstone while a present copy still exists
     * @param refusedLive deliveries the door refused of a present copy to a replica that had collected the withdrawal
     * @param staleBehind states where a replica has collected a renewed withdrawal (stamp above 1) while a copy with a
     *                    lower stamp is in flight to it: reordered lease versions meeting a collection
     */
    record Result(long states, long transitions, int depth, Set<Inv> broken, List<String> trace,
                  long danger, long refusedLive, long staleBehind) {
    }

    /**
     * Breadth-first search over every reachable state; stops at the first violation when one is found, and then
     * searches again keeping each state's parent, so the shortest path to the violation can be printed. A run that
     * breaks nothing keeps no parents, which halves its memory.
     */
    static Result check(Config c, Set<Inv> checked) {
        Result first = search(c, checked, false);
        return first.broken().isEmpty() ? first : search(c, checked, true);
    }

    private static Result search(Config c, Set<Inv> checked, boolean traces) {
        Set<String> seen = new HashSet<>(1 << 20);
        Map<String, String> parent = new HashMap<>();
        Map<String, String> via = new HashMap<>();
        ArrayDeque<String> frontier = new ArrayDeque<>();
        String init = new S().key();
        seen.add(init);
        frontier.add(init);
        long[] transitions = {0};
        long[] danger = {0};
        long[] refusedLive = {0};
        long[] staleBehind = {0};
        String[] bad = {null};
        String[] badFrom = {null};
        String[] badVia = {null};
        Set<Inv> badSet = EnumSet.noneOf(Inv.class);
        int depth = 0;
        while (!frontier.isEmpty() && bad[0] == null) {
            ArrayDeque<String> next = new ArrayDeque<>();
            for (String k : frontier) {
                S s = S.decode(k);
                successors(c, s, (action, t) -> {
                    transitions[0]++;
                    if (bad[0] != null) {
                        return;
                    }
                    if (action.startsWith("Refuse(") && present(refusedCopy(s, t))
                            && hasCollectedWithdrawal(s, refusedTo(s, t))) {
                        refusedLive[0]++;
                    }
                    String tk = t.key();
                    // A step property is judged on the transition itself, before a state reached another way is
                    // skipped; its trace is the path to the state before it, then this step.
                    if (checked.contains(Inv.NO_LATE_LIVE) && turnsLiveAfterCollection(s, t)) {
                        bad[0] = tk;
                        badFrom[0] = k;
                        badVia[0] = action;
                        badSet.add(Inv.NO_LATE_LIVE);
                        return;
                    }
                    if (!seen.add(tk)) {
                        return;
                    }
                    if (traces) {
                        parent.put(tk, k);
                        via.put(tk, action);
                    }
                    if (isDangerous(t)) {
                        danger[0]++;
                    }
                    if (hasStaleBehindCollection(t)) {
                        staleBehind[0]++;
                    }
                    Set<Inv> b = broken(c, t, checked);
                    if (!b.isEmpty()) {
                        bad[0] = tk;
                        badSet.addAll(b);
                        return;
                    }
                    next.add(tk);
                });
                if (bad[0] != null) {
                    break;
                }
            }
            if (!next.isEmpty() || bad[0] != null) {
                depth++;
            }
            frontier = next;
        }
        List<String> trace = new ArrayList<>();
        if (bad[0] != null) {
            String from = bad[0];
            if (badFrom[0] != null) {
                trace.add(badVia[0] + " ->" + S.decode(bad[0]));
                from = badFrom[0];
            }
            for (String k = from; k != null; k = parent.get(k)) {
                trace.add(0, (via.containsKey(k) ? via.get(k) : "Init") + " ->" + S.decode(k));
            }
        }
        return new Result(seen.size(), transitions[0], depth, badSet, trace, danger[0], refusedLive[0],
                staleBehind[0]);
    }

    /** Some replica had collected a withdrawal, and this step made a replica hold the entry live. */
    private static boolean turnsLiveAfterCollection(S before, S after) {
        boolean collected = false;
        for (int r = 0; r < N && !collected; r++) {
            collected = hasCollectedWithdrawal(before, r);
        }
        if (!collected) {
            return false;
        }
        for (int r = 0; r < N; r++) {
            if (!before.live(r) && after.live(r)) {
                return true;
            }
        }
        return false;
    }

    /** A replica collected a renewed withdrawal while a copy carrying an older lease is in flight to it. */
    private static boolean hasStaleBehindCollection(S s) {
        for (int m : s.msgs) {
            int to = m >> 13;
            int copy = m & 0x1FFF;
            for (int g : s.gone[to]) {
                if (rm(g) && !cp(g) && stp(g) > 1 && stp(copy) < stp(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The copy a refused delivery carried: the one message that left the set. */
    private static int refusedCopy(S before, S after) {
        for (int m : before.msgs) {
            if (Arrays.binarySearch(after.msgs, m) < 0) {
                return m & 0x1FFF;
            }
        }
        return 0;
    }

    private static int refusedTo(S before, S after) {
        for (int m : before.msgs) {
            if (Arrays.binarySearch(after.msgs, m) < 0) {
                return m >> 13;
            }
        }
        return 0;
    }

    private static boolean hasCollectedWithdrawal(S s, int r) {
        for (int g : s.gone[r]) {
            if (rm(g) && !cp(g)) {
                return true;
            }
        }
        return false;
    }

    /** A replica collected the withdrawal while a present copy of the entry still exists somewhere. */
    private static boolean isDangerous(S s) {
        boolean collected = false;
        for (int r = 0; r < N && !collected; r++) {
            collected = hasCollectedWithdrawal(s, r);
        }
        if (!collected) {
            return false;
        }
        for (int copy : s.copies()) {
            if (present(copy)) {
                return true;
            }
        }
        return false;
    }

    private static void report(String name, Config c, Result r) {
        System.out.printf("HorizonModelCheck %s %s: %d states, %d transitions, depth %d, broken %s,"
                        + " danger states %d, present copies refused after a collected withdrawal %d,"
                        + " states with an older lease in flight to a replica that collected a renewed withdrawal %d%n",
                name, c, r.states(), r.transitions(), r.depth(), r.broken(), r.danger(), r.refusedLive(),
                r.staleBehind());
        r.trace().forEach(line -> System.out.println("  " + line));
    }

    // ---- the runs ----

    @Test
    void thePerEntryHorizonHoldsInEveryReachableState() {
        Result r = check(MAIN, SAFETY);
        report("per-entry", MAIN, r);
        assertThat(r.broken()).as("invariants broken; trace: %s", r.trace()).isEmpty();
        // Not vacuous: the model reaches the case the argument is about, a present copy still around after a
        // replica collected the withdrawal, and the door then turns such a copy away.
        assertThat(r.danger()).isPositive();
        assertThat(r.refusedLive()).isPositive();
        assertThat(r.states()).isGreaterThan(10_000);
    }

    @Test
    void thePerEntryHorizonHoldsWhenTwoCopiesTravelAndArriveInEitherOrder() {
        Result r = check(REORDER, SAFETY);
        report("per-entry, two in flight", REORDER, r);
        assertThat(r.broken()).as("invariants broken; trace: %s", r.trace()).isEmpty();
        assertThat(r.refusedLive()).isPositive();
        // Not vacuous for reordering: a renewed entry's withdrawal is collected while a copy of its older lease is
        // still on its way to that replica.
        assertThat(r.staleBehind()).isPositive();
    }

    /**
     * Clocks further apart than the grace (owner decision 22). With drift at twice the grace, as SPEC's ten minutes is
     * twice the five, a withdrawal keeps the drift, and every invariant and NoLateLive hold: when the first replica
     * collects at L + MAX_DRIFT by its clock, every other clock reads L or later, so no copy is live anywhere.
     */
    @Test
    void aClockTrailingByTheWholeDriftNeverShowsACollectedWithdrawalLive() {
        Result r = check(DRIFT, SAFETY);
        report("per-entry, drift twice the grace", DRIFT, r);
        assertThat(r.broken()).as("invariants broken; trace: %s", r.trace()).isEmpty();
        // Not vacuous: a present copy is still around after a replica collected the withdrawal.
        assertThat(r.danger()).isPositive();
    }

    /**
     * What the floor is for, shown by taking it away: the per-entry rule of v0.1.35 before owner decision 22. The
     * claim of §3.5 still holds, but once every replica that held the withdrawal has collected it at L + grace, a
     * replica that never heard of it and whose clock trails can be handed an old present copy that is unexpired by its
     * own clock, and show it live until that copy's expiry: up to MAX_DRIFT minus the grace, five minutes.
     */
    @Test
    void withoutTheDriftFloorATrailingClockShowsAnUnheardWithdrawalLive() {
        Config noFloor = DRIFT.withoutDriftFloor();
        Result safe = check(noFloor, EnumSet.of(Inv.NO_RESURRECTION, Inv.REFUSED_AT_EVERY_DOOR, Inv.LEASE_BOUND));
        report("per-entry without the floor, drift twice the grace", noFloor, safe);
        assertThat(safe.broken()).as("invariants broken; trace: %s", safe.trace()).isEmpty();
        Result late = check(noFloor, EnumSet.of(Inv.NO_LATE_LIVE));
        report("per-entry without the floor, drift twice the grace, late live", noFloor, late);
        assertThat(late.broken()).containsExactly(Inv.NO_LATE_LIVE);
        assertThat(late.trace()).isNotEmpty();
    }

    /**
     * The legacy rule's own window, closed by the same floor: with a maximum lease shorter than half the drift,
     * {@code 2 x maxLease} alone lets a trailing clock show a collected withdrawal live, as the per-entry rule did;
     * with the floor every invariant and NoLateLive hold.
     */
    @Test
    void theDriftFloorClosesTheLegacyWindowWhenTwoLeasesAreShorterThanTheDrift() {
        Result r = check(LEGACY_DRIFT, SAFETY);
        report("legacy, two leases under the drift", LEGACY_DRIFT, r);
        assertThat(r.broken()).as("invariants broken; trace: %s", r.trace()).isEmpty();
        Config noFloor = LEGACY_DRIFT.withoutDriftFloor();
        Result late = check(noFloor, EnumSet.of(Inv.NO_LATE_LIVE));
        report("legacy without the floor, two leases under the drift, late live", noFloor, late);
        assertThat(late.broken()).containsExactly(Inv.NO_LATE_LIVE);
        assertThat(late.trace()).isNotEmpty();
    }

    @Test
    void theLegacyHorizonHoldsInEveryReachableState() {
        Config legacy = MAIN.legacy();
        Result r = check(legacy, SAFETY);
        report("legacy", legacy, r);
        assertThat(r.broken()).as("invariants broken; trace: %s", r.trace()).isEmpty();
    }

    @Test
    void aShorteningWriterBreaksThePerEntryHorizon() {
        Config broken = MAIN.withShortening();
        Result r = check(broken, EnumSet.of(Inv.NO_RESURRECTION));
        report("break: shortening writer", broken, r);
        assertThat(r.broken()).as("a writer that shortens its lease must let a lagging copy back")
                .containsExactly(Inv.NO_RESURRECTION);
        assertThat(r.trace()).isNotEmpty();
        // The lemma and the door invariant break with it.
        assertThat(check(broken, EnumSet.of(Inv.LEASE_BOUND)).broken()).containsExactly(Inv.LEASE_BOUND);
        assertThat(check(broken, EnumSet.of(Inv.REFUSED_AT_EVERY_DOOR)).broken())
                .containsExactly(Inv.REFUSED_AT_EVERY_DOOR);
    }

    @Test
    void twoNodesRenewingOneIdentityBreakThePerEntryHorizon() {
        Config broken = MAIN.withTwoIssuers();
        Result r = check(broken, EnumSet.of(Inv.NO_RESURRECTION));
        report("break: two nodes renew one identity", broken, r);
        assertThat(r.broken()).as("a second renewer must let a lagging copy back")
                .containsExactly(Inv.NO_RESURRECTION);
        assertThat(check(broken, EnumSet.of(Inv.LEASE_BOUND)).broken()).containsExactly(Inv.LEASE_BOUND);
    }

    /**
     * Why completed entries keep 2 x maxLease, and why that is a delivery assumption rather than a proof: the
     * completer is not the issuer, so the issuer may go on renewing a copy the completion has not reached, and a
     * replica that collected the completion can take that copy back once the partition heals. The model shows it
     * under the legacy rule itself, with leases of two units so the bounded clock reaches past 2 x maxLease.
     */
    @Test
    void aCompletionsHorizonRestsOnDeliveryNotOnTheLease() {
        Config tiny = new Config(5, 1, 2, 1, 6, 1, false, false, false);
        Result r = check(tiny, EnumSet.of(Inv.COMPLETION_STAYS_DONE));
        report("completion under partition, legacy rule", tiny, r);
        assertThat(r.broken()).containsExactly(Inv.COMPLETION_STAYS_DONE);
    }

    /**
     * The model's two rules are the real ones: for lapsed, withdrawn and completed states the model's keep, read in
     * minutes with the grace at five, equals {@link SpaceStateCrdt#collectionHorizon(EntryState, long)} minus the
     * expiry for the unhinted rule, and {@link SpaceStateCrdt#collectionHorizon(EntryState, long,
     * SpaceStateCrdt.Horizon)} under {@link SpaceStateCrdt.Horizon#PER_ENTRY} for the hinted one (step 5b). The
     * collectable forms agree with the horizons one minute before and at them.
     */
    @Test
    void theModelsLegacyRuleIsTheRealOne() {
        AgentId agent = AgentId.parse("zP/writer");
        EntryRecord record = new EntryRecord(EntryId.of("entry-model"), SpaceId.local("tasks"), "T#v1",
                new byte[]{1}, null, agent, new HlcTimestamp(1L, 0, "n"),
                new LeaseInfo(agent, 1_000, LeaseKind.WRITE), Map.of(), null);
        long minute = 60_000L;
        for (int maxLease = 1; maxLease <= 12; maxLease++) {
            // A clock drift of 10: MAX_DRIFT is ten minutes, the floor of a withdrawal under both rules.
            Config c = new Config(30, 10, maxLease, 5, 4, 2, false, false, false);
            for (int kind = 0; kind < 4; kind++) {
                long expiry = 10 * minute;
                EntryState state = EntryState.added(record, new Dot("a", 1),
                        new LwwRegister<>(new HlcTimestamp(1L, 0, "n"), new LeaseInfo(agent, expiry, LeaseKind.WRITE)));
                int model = pack(false, false, 10, 1);
                if (kind == 1) {
                    state = state.withObservedRemoved();
                    model = pack(true, false, 10, 1);
                } else if (kind == 2) {
                    state = state.withCompleted();
                    model = pack(false, true, 10, 1);
                } else if (kind == 3) {
                    state = state.withObservedRemoved().withCompleted();
                    model = pack(true, true, 10, 1);
                }
                long real = SpaceStateCrdt.collectionHorizon(state, maxLease * minute) - expiry;
                assertThat(real).as("kind %d at maxLease %d", kind, maxLease).isEqualTo(keep(c, model) * minute);
                assertThat(SpaceStateCrdt.collectionHorizon(state, maxLease * minute, SpaceStateCrdt.Horizon.LEGACY))
                        .isEqualTo(real + expiry);
                Config hinted = new Config(30, 10, maxLease, 5, 4, 2, true, false, false);
                long perEntry = SpaceStateCrdt.collectionHorizon(state, maxLease * minute,
                        SpaceStateCrdt.Horizon.PER_ENTRY);
                assertThat(perEntry - expiry).as("hinted, kind %d at maxLease %d", kind, maxLease)
                        .isEqualTo(keep(hinted, model) * minute);
                assertThat(SpaceStateCrdt.collectable(state, perEntry - minute, maxLease * minute,
                        SpaceStateCrdt.Horizon.PER_ENTRY)).isFalse();
                assertThat(SpaceStateCrdt.collectable(state, perEntry, maxLease * minute,
                        SpaceStateCrdt.Horizon.PER_ENTRY)).isTrue();
            }
        }
    }
}
