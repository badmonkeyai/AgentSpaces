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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fetch and the budget at the bus (srad/replication-by-entry.md phase 4, step 4b; SPEC annex I.6, <em>The
 * fetch</em> and <em>The budget</em>): a fetch's answer is cut to the asker's budget and carries a remainder, the asker
 * continues it at once, a first item larger than the budget travels alone, an id a partner left out goes to the next
 * member after a jittered backoff and waits for an audit when every member left it out, a lost answer is asked of
 * another member while its late copy is still applied, a budgeted get-space of a text stream finishes within one round,
 * and an asker that sent no budget is answered as before v0.1.35.
 *
 * <p>The buses are wired by a queue that delivers each body through the codec's strict reader, so every body sent is
 * one the wire would carry, and a test decides what is lost and when the rest arrives. A stand-in state holds items with
 * a stamp and a payload of a chosen size, and answers in the annex's order, newest first.
 */
class BudgetedFetchTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final String STREAM = "space:t";
    private static final long BUDGET = 4096;

    private final AtomicLong now = new AtomicLong(1_000_000);
    private final Net net = new Net();

    /** One item of the stand-in state. */
    record Item(String id, long stamp, byte[] pad) {
    }

    /** The stand-in state's delta. */
    record Delta(List<Item> items) {
    }

    /**
     * A stand-in for a space: items by id, a text digest of {@code e:<id>=<stamp>} lines, and budgeted answers newest
     * first. With {@code trie} it reports a stream hash, which makes the bus treat its stream as one it fetches by id.
     */
    static final class KvState implements ReconcilableState {
        final Map<String, Item> items = new TreeMap<>();
        final boolean trie;
        final List<Integer> answerSizes = new ArrayList<>();

        KvState(boolean trie) {
            this.trie = trie;
        }

        void put(String id, long stamp, int size) {
            items.put(id, new Item(id, stamp, new byte[size]));
        }

        @Override
        public synchronized byte[] digest() {
            List<String> lines = new ArrayList<>();
            items.values().forEach(item -> lines.add("e:" + item.id() + "=" + item.stamp()));
            return String.join("\n", lines).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] streamHash() {
            return trie ? Digests.sha256(digest()) : null;
        }

        private synchronized List<Item> missing(byte[] remote) {
            Set<String> lines = new HashSet<>(List.of(new String(remote, StandardCharsets.UTF_8).split("\n")));
            List<Item> out = new ArrayList<>();
            for (Item item : items.values()) {
                if (!lines.contains("e:" + item.id() + "=" + item.stamp())) {
                    out.add(item);
                }
            }
            return out;
        }

        private synchronized List<Item> held(Collection<String> ids) {
            List<Item> out = new ArrayList<>();
            for (String id : ids) {
                Item item = items.get(id);
                if (item != null) {
                    out.add(item);
                }
            }
            return out;
        }

        private Bounded bounded(List<Item> offers, long budget, boolean first) {
            offers.sort(Comparator.comparingLong(Item::stamp).reversed().thenComparing(Item::id));
            List<Item> taken = new ArrayList<>();
            long used = 16;
            for (Item item : offers) {
                long size = CODEC.toBytes(item).length;
                if (used + size > budget && !(first && taken.isEmpty())) {
                    break;
                }
                used += size;
                taken.add(item);
            }
            answerSizes.add(taken.size());
            return new Bounded(taken.isEmpty() ? new byte[0] : CODEC.toBytes(new Delta(taken)),
                    offers.size() - taken.size());
        }

        @Override
        public byte[] deltaFor(byte[] remoteDigest) {
            return bounded(missing(remoteDigest), Long.MAX_VALUE, true).delta();
        }

        @Override
        public Bounded deltaFor(byte[] remoteDigest, long budget, boolean first) {
            return bounded(missing(remoteDigest), budget, first);
        }

        @Override
        public byte[] statesFor(Collection<String> ids) {
            return bounded(held(ids), Long.MAX_VALUE, true).delta();
        }

        @Override
        public Bounded statesFor(Collection<String> ids, long budget, boolean first) {
            return bounded(held(ids), budget, first);
        }

        @Override
        public void applyDelta(byte[] delta) {
            applyDeltaNamed(delta);
        }

        @Override
        public boolean applyDeltaChanged(byte[] delta) {
            return applyDeltaNamed(delta).changed();
        }

        @Override
        public synchronized Applied applyDeltaNamed(byte[] delta) {
            Set<String> named = new HashSet<>();
            boolean changed = false;
            if (delta.length > 0) {
                for (Item item : CODEC.fromBytes(delta, Delta.class).items()) {
                    named.add(item.id());
                    Item held = items.get(item.id());
                    if (held == null || held.stamp() < item.stamp()) {
                        items.put(item.id(), item);
                        changed = true;
                    }
                }
            }
            return new Applied(changed, named);
        }
    }

    /** One body on its way, and the bytes its encoding takes. */
    record Frame(PeerId from, PeerId to, Object body, int bytes) {
    }

    /** The fabric: a queue of bodies, each delivered through the strict reader, with loss decided by a test. */
    final class Net {
        final Map<PeerId, GossipBus> buses = new HashMap<>();
        final ArrayDeque<Frame> queue = new ArrayDeque<>();
        final List<Frame> log = new ArrayList<>();
        final List<Frame> dropped = new ArrayList<>();
        Predicate<Frame> drop = f -> false;

        GossipBus.FrameSender senderFor(PeerId self) {
            return new GossipBus.FrameSender() {
                @Override
                public void sendRumor(PeerId to, Bodies.Rumor rumor) {
                }

                @Override
                public void sendDigest(PeerId to, Bodies.Digest digest) {
                    enqueue(self, to, digest);
                }

                @Override
                public void sendPullResp(PeerId to, Bodies.PullResp resp) {
                    enqueue(self, to, resp);
                }
            };
        }

        void enqueue(PeerId from, PeerId to, Object body) {
            byte[] bytes = CODEC.toBytes(body);
            Object read = CODEC.fromWire(bytes, body.getClass()); // what a strict reader takes off the wire
            Frame frame = new Frame(from, to, read, bytes.length);
            log.add(frame);
            if (drop.test(frame)) {
                dropped.add(frame);
                return;
            }
            queue.add(frame);
        }

        void deliver(Frame frame) {
            GossipBus bus = buses.get(frame.to());
            if (frame.body() instanceof Bodies.Digest digest) {
                bus.onDigest(frame.from(), digest);
            } else {
                bus.onPullResp(frame.from(), (Bodies.PullResp) frame.body());
            }
        }

        void run() {
            while (!queue.isEmpty()) {
                deliver(queue.poll());
            }
        }

        List<Frame> pullResps(PeerId from, PeerId to) {
            return log.stream().filter(f -> f.from().equals(from) && f.to().equals(to)
                    && f.body() instanceof Bodies.PullResp).toList();
        }

        List<Bodies.Digest> fetches(PeerId from, PeerId to) {
            return log.stream().filter(f -> f.from().equals(from) && f.to().equals(to)
                            && f.body() instanceof Bodies.Digest d && d.ids() != null)
                    .map(f -> (Bodies.Digest) f.body()).toList();
        }
    }

    /** A member: its id, its bus on the shared fabric, and its state for the stream. */
    record Member(PeerId id, GossipBus bus, KvState state) {
        TrieAudit audit() {
            return bus.auditForTest();
        }
    }

    private final List<Member> members = new ArrayList<>();

    private Member member(boolean trie, long seed) {
        PeerId id = PeerIdentity.generate().peerId();
        KvState state = new KvState(trie);
        // Every member sees every other one; the order the fabric was built in is the view's.
        GossipBus bus = new GossipBus(n -> {
            List<PeerId> others = new ArrayList<>();
            for (Member m : members) {
                if (!m.id().equals(id)) {
                    others.add(m.id());
                }
            }
            return others.subList(0, Math.min(n, others.size()));
        }, net.senderFor(id), CODEC, 3, 6, now::get, new java.util.Random(seed));
        bus.audit(p -> true);
        bus.auditBudget(BUDGET);
        bus.reconcile(STREAM, state);
        net.buses.put(id, bus);
        Member member = new Member(id, bus, state);
        members.add(member);
        return member;
    }

    private static List<String> ids(String prefix, int count) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(prefix + i);
        }
        return out;
    }

    /** The members a member sees: every other one, as a sampler lists them. */
    private List<PeerId> viewOf(Member self) {
        return members.stream().map(Member::id).filter(id -> !id.equals(self.id())).toList();
    }

    /**
     * RE-I-01, RE-I-02, RE-F-02, RE-Q-PRF-03: a fetch of a hundred items of about 230 bytes under a 4 KiB budget comes
     * back as several answers, each within the budget and carrying a remainder, newest first, and the asker continues
     * each at once, so every item lands with no round in between and nothing goes to another member.
     */
    @Test
    void aFetchIsCutToTheBudgetCarriesARemainderAndContinuesAtOnce() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        for (int i = 0; i < 100; i++) {
            a.state().put("x" + i, 1_000 + i, 200);
        }

        b.audit().want(a.id(), STREAM, ids("x", 100));
        net.run();

        assertThat(b.state().items).hasSize(100);
        List<Frame> answers = net.pullResps(a.id(), b.id());
        assertThat(answers.size()).as("several bounded answers").isGreaterThanOrEqualTo(6);
        for (Frame answer : answers) {
            Bodies.PullResp resp = (Bodies.PullResp) answer.body();
            assertThat(answer.bytes()).as("an answer's body stays within the budget").isLessThanOrEqualTo((int) BUDGET);
            assertThat(resp.remainder()).as("a budgeted answer counts what it left out").containsKey(STREAM);
        }
        Bodies.PullResp first = (Bodies.PullResp) answers.get(0).body();
        List<Item> firstItems = CODEC.fromBytes(first.deltas().get(STREAM), Delta.class).items();
        assertThat(firstItems.get(0).stamp()).as("newest first").isEqualTo(1_099);
        assertThat(first.remainder().get(STREAM)).isEqualTo(100L - firstItems.size());
        Bodies.PullResp last = (Bodies.PullResp) answers.get(answers.size() - 1).body();
        assertThat(last.remainder().get(STREAM)).isZero();
        assertThat(net.fetches(b.id(), a.id())).allSatisfy(d -> assertThat(d.budget()).isEqualTo(BUDGET));

        GossipBus.AuditSignals signals = b.bus().auditSignals();
        assertThat(signals.fetchContinued()).as("the left-out ids were asked again at once").isPositive();
        assertThat(signals.fetchRequeued()).isZero();
        assertThat(signals.fetchQueued()).isZero();
        assertThat(signals.remainderReceived()).isPositive();
        assertThat(a.bus().auditSignals().budgetedAnswers()).isEqualTo(answers.size());
    }

    /** A fetch of more ids than a few bodies hold keeps the window's bodies in flight and sends the next as each lands. */
    @Test
    void aLargeFetchKeepsItsWindowInFlight() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        int n = 5 * Bodies.Digest.IDS_MAX + 7;
        for (int i = 0; i < n; i++) {
            a.state().put("w" + i, i, 0);
        }

        b.audit().want(a.id(), STREAM, ids("w", n));
        assertThat(net.queue).as("the window, not every body at once").hasSize(TrieAudit.FETCH_WINDOW);
        assertThat(net.queue).allSatisfy(f -> assertThat(((Bodies.Digest) f.body()).ids().get(STREAM))
                .hasSize(Bodies.Digest.IDS_MAX));
        net.run();

        assertThat(b.state().items).hasSize(n);
        assertThat(b.bus().auditSignals().fetchQueued()).isZero();
    }

    /**
     * P29 and annex I.6's one exception: an item larger than the budget goes alone as its answer's first item, so it
     * travels within the frame and nobody re-asks for it for ever; the rest follow under the budget.
     */
    @Test
    void aFirstItemLargerThanTheBudgetTravelsAlone() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        a.state().put("big", 9_999, 3 * (int) BUDGET);
        for (int i = 0; i < 5; i++) {
            a.state().put("small" + i, i, 100);
        }

        b.audit().want(a.id(), STREAM, List.of("big", "small0", "small1", "small2", "small3", "small4"));
        net.run();

        assertThat(b.state().items).hasSize(6);
        List<Frame> answers = net.pullResps(a.id(), b.id());
        Frame first = answers.get(0);
        assertThat(first.bytes()).as("the one answer past the budget").isGreaterThan((int) BUDGET);
        assertThat(CODEC.fromBytes(((Bodies.PullResp) first.body()).deltas().get(STREAM), Delta.class).items())
                .extracting(Item::id).containsExactly("big");
        assertThat(answers.subList(1, answers.size())).allSatisfy(f ->
                assertThat(f.bytes()).isLessThanOrEqualTo((int) BUDGET));
        long bigs = answers.stream().filter(f -> ((Bodies.PullResp) f.body()).deltas().containsKey(STREAM)
                && CODEC.fromBytes(((Bodies.PullResp) f.body()).deltas().get(STREAM), Delta.class).items().stream()
                .anyMatch(item -> item.id().equals("big"))).count();
        assertThat(bigs).as("sent once, never re-asked").isEqualTo(1);
    }

    /**
     * RE-I-03, RE-Q-REL-03: an id the partner does not hold comes back in an empty answer whose remainder is zero; the
     * asker waits a jittered backoff and asks the next member in view, not the same one again; an id that every member
     * leaves out is parked, and asked again when an audit names it, so no repair is given up for good.
     */
    @Test
    void anIdLeftOutGoesToTheNextMemberAfterABackoffAndWaitsForAnAuditWhenEveryMemberLeftItOut() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        Member c = member(true, 3);
        c.state().put("x", 7, 50);

        b.audit().want(a.id(), STREAM, List.of("x"));
        net.run();
        List<Frame> fromA = net.pullResps(a.id(), b.id());
        assertThat(fromA).hasSize(1);
        assertThat(((Bodies.PullResp) fromA.get(0).body()).deltas()).as("a holds nothing of x").isEmpty();
        assertThat(((Bodies.PullResp) fromA.get(0).body()).remainder()).containsEntry(STREAM, 0L);
        assertThat(b.bus().auditSignals().fetchRequeued()).isEqualTo(1);

        b.audit().retry(viewOf(b));
        net.run();
        assertThat(net.fetches(b.id(), c.id())).as("not before the backoff").isEmpty();

        now.addAndGet(TrieAudit.RETRY_BASE_MILLIS * 3 / 2 + 1);
        b.audit().retry(viewOf(b));
        net.run();
        assertThat(net.fetches(b.id(), a.id())).as("a is not asked twice").hasSize(1);
        assertThat(net.fetches(b.id(), c.id())).hasSize(1);
        assertThat(b.state().items).containsKey("x");
        assertThat(b.bus().auditSignals().fetchQueued()).isZero();

        // Nobody holds y: each member leaves it out in turn, and then it waits for an audit.
        b.audit().want(a.id(), STREAM, List.of("y"));
        net.run();
        for (int round = 0; round < 4; round++) {
            now.addAndGet(TrieAudit.RETRY_CEILING_MILLIS);
            b.audit().retry(viewOf(b));
            net.run();
        }
        GossipBus.AuditSignals signals = b.bus().auditSignals();
        assertThat(signals.fetchParked()).isEqualTo(1);
        assertThat(signals.fetchQueued()).as("kept, not given up").isEqualTo(1);
        long asksOfY = net.log.stream().filter(f -> f.body() instanceof Bodies.Digest d && d.ids() != null
                && d.ids().get(STREAM).contains("y")).count();
        assertThat(asksOfY).as("each member once, then parked").isEqualTo(2);

        a.state().put("y", 9, 50);
        b.audit().want(a.id(), STREAM, List.of("y")); // the next audit with a names it again
        net.run();
        assertThat(b.state().items).containsKey("y");
        assertThat(b.bus().auditSignals().fetchQueued()).isZero();
    }

    /**
     * RE-Q-REL-02, RE-Q-REL-03: an answer lost on the way leaves its ids in flight until the wait passes; they are then
     * asked of another member, and the lost answer, arriving late, is still taken and applied, never refused as
     * unsolicited.
     */
    @Test
    void aLostAnswerIsAskedOfAnotherMemberAndItsLateCopyIsStillApplied() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        Member c = member(true, 3);
        a.state().put("z", 5, 50);
        c.state().put("z", 5, 50);
        a.state().put("z2", 6, 50);
        net.drop = f -> f.from().equals(a.id()) && f.body() instanceof Bodies.PullResp;

        b.audit().want(a.id(), STREAM, List.of("z", "z2"));
        net.run();
        assertThat(b.state().items).isEmpty();
        assertThat(net.dropped).hasSize(1);
        net.drop = f -> false;

        b.audit().retry(viewOf(b));
        assertThat(net.queue).as("still waiting for a's answer").isEmpty();

        now.addAndGet(TrieAudit.FETCH_WAIT_FLOOR_MILLIS + 1);
        b.audit().retry(viewOf(b)); // the wait passed: the ids wait their backoff to go elsewhere
        assertThat(b.bus().auditSignals().fetchTimedOut()).isEqualTo(1);
        now.addAndGet(TrieAudit.RETRY_BASE_MILLIS * 3 / 2 + 1);
        b.audit().retry(viewOf(b));
        net.run();
        assertThat(net.fetches(b.id(), c.id())).as("asked of the next member").hasSize(1);
        assertThat(b.state().items).containsKey("z").doesNotContainKey("z2");

        long unsolicited = b.bus().pacingSignals().pullRespUnsolicited();
        net.deliver(net.dropped.get(0)); // the late copy
        assertThat(b.bus().pacingSignals().pullRespUnsolicited()).isEqualTo(unsolicited);
        assertThat(b.state().items).as("the late copy is applied").containsKeys("z", "z2");
    }

    /**
     * RE-I-04, RE-Q-PRF-03: a budgeted get-space of a stream compared by its text is a run of bounded pulls that the
     * asker continues at once with its own grown digest, so it finishes within the round that began it; toward a member
     * off {@code wire.ae=2} the same round sends no budget and draws one whole answer with no remainder, as before
     * v0.1.35 (RE-Q-REL-06).
     */
    @Test
    void aBudgetedGetSpaceIsARunOfBoundedPullsWithinOneRoundAndAnUnhintedAskerGetsTheWholeAnswer() {
        Member a = member(false, 1);
        Member b = member(false, 2);
        for (int i = 0; i < 60; i++) {
            a.state().put("g" + i, i, 200);
        }

        b.bus().antiEntropyWith(a.id());
        net.run();

        assertThat(b.state().items).hasSize(60);
        List<Frame> answers = net.pullResps(a.id(), b.id());
        assertThat(answers.size()).isGreaterThan(1);
        assertThat(answers).allSatisfy(f -> {
            assertThat(f.bytes()).isLessThanOrEqualTo((int) BUDGET);
            assertThat(((Bodies.PullResp) f.body()).remainder()).containsKey(STREAM);
        });
        assertThat(b.bus().auditSignals().pullContinued()).isEqualTo(answers.size() - 1);

        Member c = member(false, 3);
        c.bus().audit(null); // a member off wire.ae=2: no budget goes out, none comes back
        a.bus().audit(p -> !p.equals(c.id()));
        c.bus().antiEntropyWith(a.id());
        net.run();
        List<Frame> whole = net.pullResps(a.id(), c.id());
        assertThat(whole).hasSize(1);
        assertThat(((Bodies.PullResp) whole.get(0).body()).remainder()).isNull();
        assertThat(net.log.stream().filter(f -> f.from().equals(c.id()) && f.body() instanceof Bodies.Digest d
                && d.carriesAudit()).count()).isZero();
        assertThat(c.state().items).hasSize(60);
    }

    /** A fetch that carries no budget, the form before step 4b, draws the whole answer and no remainder. */
    @Test
    void aFetchWithoutABudgetIsAnsweredWhole() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        for (int i = 0; i < 40; i++) {
            a.state().put("u" + i, i, 200);
        }

        a.bus().onDigest(b.id(), new Bodies.Digest(null, null, null, null, Map.of(STREAM, ids("u", 40)), null, null));
        List<Frame> answers = net.pullResps(a.id(), b.id());
        assertThat(answers).hasSize(1);
        Bodies.PullResp resp = (Bodies.PullResp) answers.get(0).body();
        assertThat(resp.remainder()).isNull();
        assertThat(CODEC.fromBytes(resp.deltas().get(STREAM), Delta.class).items()).hasSize(40);
        assertThat(answers.get(0).bytes()).isGreaterThan((int) BUDGET);
    }

    /** RE-F-03: the fetch's paces are runtime settings within bounds; a window of one keeps one body in flight. */
    @Test
    void theFetchPacesAreBoundedAndSettable() {
        Member a = member(true, 1);
        Member b = member(true, 2);
        assertThat(b.bus().fetchWindow()).isEqualTo(TrieAudit.FETCH_WINDOW);
        assertThatThrownBy(() -> b.bus().fetchWindow(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.bus().fetchWindow(17)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.bus().fetchWaitFloor(999)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.bus().fetchWaitFloor(TrieAudit.LATE_COPY_MILLIS + 1))
                .isInstanceOf(IllegalArgumentException.class);
        b.bus().fetchWaitFloor(2_000);
        assertThat(b.bus().fetchWaitFloor()).isEqualTo(2_000);
        b.bus().fetchWindow(1);
        int n = 3 * Bodies.Digest.IDS_MAX;
        for (int i = 0; i < n; i++) {
            a.state().put("p" + i, i, 0);
        }

        b.audit().want(a.id(), STREAM, ids("p", n));
        assertThat(net.queue).hasSize(1);
        net.run();
        assertThat(b.state().items).hasSize(n);
    }
}
