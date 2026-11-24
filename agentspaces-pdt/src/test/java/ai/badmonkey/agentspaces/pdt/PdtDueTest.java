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
package ai.badmonkey.agentspaces.pdt;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.NodeLoop;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The PDT facade as due work on a started loop (srad/agentspaces-event-loop.md EL-T-08, EL-T-09): {@link Pdt#joinLoop}
 * ticks the ref engine only at its quiet deadlines or when the ref space changed, so an idle ref with a 10-minute head
 * lease ticks about every 5 minutes, where the {@code onTick} adapter called it every 250 ms; and a head another member
 * writes, which arrives by gossip and wakes nothing by kind, is loaded through the ref space's change signal.
 */
@Timeout(180)
class PdtDueTest {

    private static final Codec<String> STR = Codec.of(String.class);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPdtDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zPdtDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "pdt-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closers = new ArrayList<>();

    private record Member(PeerNode node, GroupRuntime runtime, Pdt pdt, Content content, ReplicatedSpace refs) {
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : closers) {
            c.close();
        }
        nodes.forEach(PeerNode::close);
    }

    private Member member(String name, String seed) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        BlockExchange blocks = new BlockExchange(runtime, codec, time, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content").clock(time)
                .settleWindow(Duration.ZERO).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "refs", identity, "refs").clock(time)
                .settleWindow(Duration.ZERO).build();
        Pdt pdt = Pdt.builder(refSpace).store(new NodeStore(new MemoryBlockStore(), time)).clock(time)
                .loadTimeout(Duration.ofSeconds(30)).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace,
                        identity.peerId()).codec(codec).clock(time).store(new MemoryPieceStore(time, 0)).blocks(blocks)
                .random(new Random(name.hashCode())).treeStore(pdt.treeStore()).build();
        content.joinLoop(runtime);
        content.start();
        pdt.attach(content).start();
        pdt.joinLoop(runtime);
        closers.add(pdt);
        closers.add(content);
        node.startTicking(Duration.ofMillis(250));
        return new Member(node, runtime, pdt, content, refSpace);
    }

    private void form(Member a, Member b) {
        long waited = 0;
        while (a.runtime().membership().allMembers().isEmpty() || b.runtime().membership().allMembers().isEmpty()) {
            time.advance(Duration.ofMillis(250));
            waited += 250;
            assertThat(waited).isLessThan(60_000);
        }
    }

    @Test
    void anIdleRefTicksAtItsHalfLifeAndARemoteHeadLoadsThroughTheChangeSignal() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        form(a, b);
        Ref<PMap<String, String>> onA = a.pdt().mapRef("facts", STR, STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef("facts", STR, STR);
        onA.swap(m -> m.assoc("a", "one"));
        long waited = 0;
        while (onB.deref() == null || onB.deref().get("a").isEmpty()) {
            time.advance(Duration.ofMillis(50));
            waited += 50;
            assertThat(waited).as("b loads a's head").isLessThan(10_000);
        }
        System.out.printf("[pdt-due] b loaded a's first head %d ms after the swap%n", waited);

        time.advance(Duration.ofMinutes(2));
        long ticksA = a.pdt().dueTicks();
        long ticksB = b.pdt().dueTicks();
        time.advance(Duration.ofMinutes(20));
        long idleA = a.pdt().dueTicks() - ticksA;
        long idleB = b.pdt().dueTicks() - ticksB;
        System.out.printf("[pdt-due] idle 20 min: ref engine ticks a=%d b=%d (the adapter: 4,800 each)%n", idleA, idleB);
        assertThat(idleA).as("about every 5 minutes, plus the changes its renewals make").isBetween(1L, 30L);
        assertThat(idleB).isBetween(1L, 30L);

        long before = time.monoNanos();
        onA.swap(m -> m.assoc("b", "two"));
        waited = 0;
        while (!"two".equals(onB.deref().get("b").orElse(null))) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
            assertThat(waited).as("the remote head is loaded without the backstop").isLessThan(5_000);
        }
        System.out.printf("[pdt-due] b loaded a's next head %d ms after the swap%n",
                (time.monoNanos() - before) / 1_000_000);
        assertThat(b.node().loopStats().backstopFoundBy()).as("no missed wake on b").isEmpty();
        assertThat(a.node().loopStats().backstopFoundBy()).as("no missed wake on a").isEmpty();
    }

    /**
     * EL-Q-REL-03, the final gate (srad/agentspaces-event-loop.md §10.49): a ref space raises its change signal under
     * its lock and fires the entry's event after it, so the loop, on its own thread, can run the engine between the
     * two. That tick sees the space change but not yet the head the event brings, and the event then puts the head's
     * root to be rechecked: work in flight that nothing signalled. The loop held the engine's quiet deadline (585 s
     * away in the module run's example 33) and the backstop's sweep found the recheck 15 s later. Here the engine's
     * own subscription is set aside, the head's signal runs the engine, and the event is handed to the engine after
     * that run, as the late event is: it must wake the engine, which takes the recheck up at once, and no sweep finds
     * work. (That the head then loads over TCP is example 33's, whose module run found the missed wake.)
     */
    @Test
    @SuppressWarnings("unchecked")
    void aRecheckTheEventPutsAfterTheSignalsRunWakesTheEngine() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        form(a, b);
        Ref<PMap<String, String>> onA = a.pdt().mapRef("facts", STR, STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef("facts", STR, STR);
        onA.swap(m -> m.assoc("a", "one"));
        long waited = 0;
        while (onB.deref() == null || onB.deref().get("a").isEmpty()) {
            time.advance(Duration.ofMillis(50));
            waited += 50;
            assertThat(waited).as("b loads a's first head").isLessThan(10_000);
        }
        time.advance(Duration.ofMinutes(2)); // quiet: b's engine's next due is minutes away
        assertThat(b.node().loopStats().backstopFoundBy()).isEmpty();

        ai.badmonkey.agentspaces.pdt.refs.Refs refs = b.pdt().refs();
        java.lang.reflect.Field statesField = ai.badmonkey.agentspaces.pdt.refs.Refs.class.getDeclaredField("states");
        statesField.setAccessible(true);
        Object state = ((java.util.Map<?, ?>) statesField.get(refs)).get("facts");
        java.lang.reflect.Field subscriptionField = state.getClass().getDeclaredField("subscription");
        subscriptionField.setAccessible(true);
        ((ai.badmonkey.agentspaces.api.space.Subscription) subscriptionField.get(state)).close();
        java.lang.reflect.Field templateField = state.getClass().getDeclaredField("template");
        templateField.setAccessible(true);
        List<ai.badmonkey.agentspaces.api.space.SpaceEvent<PRef>> held = new java.util.concurrent.CopyOnWriteArrayList<>();
        closers.add(b.refs().notify((ai.badmonkey.agentspaces.api.space.Template<PRef>) templateField.get(state),
                event -> held.add(event), ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofHours(1)))::close);

        String padding = "x".repeat(200);
        onA.swap(m -> {
            PMap<String, String> next = m;
            for (int i = 0; i < 400; i++) { // 80 KB: past the 32 KiB inline delta, so b fetches the tree
                next = next.assoc("k" + i, padding + i);
            }
            return next;
        });
        waited = 0;
        while (held.stream().noneMatch(e -> e.kind() == ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.WRITTEN)) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
            assertThat(waited).as("the head reaches b").isLessThan(10_000);
        }
        time.advance(Duration.ofSeconds(1)); // the signal's run of the engine is over, and the engine is quiet again
        long ticks = b.pdt().dueTicks();
        java.lang.reflect.Method onEvent = ai.badmonkey.agentspaces.pdt.refs.Refs.class.getDeclaredMethod("onEvent",
                state.getClass(), ai.badmonkey.agentspaces.api.space.SpaceEvent.class);
        onEvent.setAccessible(true);
        for (ai.badmonkey.agentspaces.api.space.SpaceEvent<PRef> event : held) {
            onEvent.invoke(refs, state, event); // the event, late, on its own thread's turn
        }
        time.advance(Duration.ofMillis(1));
        assertThat(b.pdt().dueTicks() - ticks).as("the event's recheck woke the engine at once").isGreaterThan(0);
        java.lang.reflect.Field recheckField = state.getClass().getDeclaredField("recheck");
        recheckField.setAccessible(true);
        assertThat((java.util.Map<?, ?>) recheckField.get(state)).as("the engine took the recheck up").isEmpty();
        time.advance(NodeLoop.DEFAULT_BACKSTOP.plusSeconds(5)); // no deref meanwhile: only the engine works
        assertThat(b.node().loopStats().backstopFoundBy()).as("no sweep found the recheck").isEmpty();
        time.advance(Duration.ofSeconds(40)); // past the load timeout, so nothing the fetch started is left in flight
    }

    /**
     * EL-C-04 (§10.35): an own head's renewal at half-life is a local period, so a wall step back of an hour does not
     * hold it for an hour: the head is renewed five minutes of real time after its write, as the other members, whose
     * clocks did not step, judge its 10-minute lease. On the wall clock (and the engine's quiet deadline with it) the
     * renewal waited for the wall to catch up.
     */
    @Test
    void aWallStepBackDoesNotHoldAnOwnHeadsRenewal() throws Exception {
        Member a = member("a", null);
        Ref<PMap<String, String>> ref = a.pdt().mapRef("facts", STR, STR);
        ref.swap(m -> m.assoc("a", "one"));
        time.advance(Duration.ofSeconds(5));
        long ticks = a.pdt().dueTicks();
        long renewals = a.pdt().refs().ownRenewals();
        time.stepWall(Duration.ofHours(-1));
        time.advance(Duration.ofMinutes(4));
        assertThat(a.pdt().refs().ownRenewals() - renewals).as("not before its half-life").isZero();
        time.advance(Duration.ofMinutes(2)); // past the 5-minute half-life of the 10-minute head lease
        System.out.printf("[pdt-due] own head after a wall step back of an hour: %d engine ticks, %d renewals in 6 min%n",
                a.pdt().dueTicks() - ticks, a.pdt().refs().ownRenewals() - renewals);
        assertThat(a.pdt().dueTicks() - ticks).as("the engine's quiet deadline is local: it ticked").isPositive();
        assertThat(a.pdt().refs().ownRenewals() - renewals).as("renewed at half-life of real time").isEqualTo(1);
        assertThat(a.node().loopStats().backstopFoundBy()).as("no missed wake").isEmpty();
    }

    /**
     * EL-Q-REL-03 (close-out gate, srad/agentspaces-event-loop.md §10.39): the engine's quiet check reads inputs the ref
     * space does not signal: the heads a deref on the caller's thread read, this peer's own entry after a swap (recorded
     * after the write's signal), and the remembered completeness a collection drops ({@code forgetHeld}). Each change
     * now wakes the participant ({@code Refs.onLocalChange}), so the engine ticks in the same instant. Before, its next
     * look was the backstop's sweep, up to 15 s later, and a sweep whose tick then found work due sooner counted a
     * missed wake (once and twice in {@code IdleCostIT}'s two shapes at the gate).
     */
    @Test
    void aChangeTheRefSpaceDoesNotSignalWakesTheEngineAtOnce() throws Exception {
        Member a = member("a", null);
        Ref<PMap<String, String>> ref = a.pdt().mapRef("facts", STR, STR);
        ref.swap(m -> m.assoc("a", "one"));
        time.advance(Duration.ofMillis(1));
        assertThat(a.pdt().refs().nextDueMillis()).as("after a swap the engine is not left due").isGreaterThan(time.millis());
        time.advance(Duration.ofMinutes(1)); // quiet: the next due is minutes away
        for (int i = 0; i < 3; i++) {
            assertThat(ref.deref().get("a")).contains("one");
            time.advance(Duration.ofMillis(1));
            assertThat(a.pdt().refs().nextDueMillis()).as("after a deref on the caller's thread the engine is not left"
                    + " due for the sweep").isGreaterThan(time.millis());
            time.advance(Duration.ofSeconds(20));
        }
        long ticks = a.pdt().dueTicks();
        a.pdt().refs().forgetHeld(); // what a collection that removed nodes does
        time.advance(Duration.ofMillis(1));
        assertThat(a.pdt().dueTicks() - ticks).as("dropped completeness: the engine ticks at once").isEqualTo(1);
        assertThat(a.pdt().refs().nextDueMillis()).isGreaterThan(time.millis());
        time.advance(Duration.ofMinutes(1));
        assertThat(a.node().loopStats().backstopFoundBy()).as("no missed wake").isEmpty();
    }

    /**
     * EL-Q-REL-03, §10.39's blind spot (§10.47): with the engine's local-change wake cut, a change it does not signal is
     * left for the backstop's sweep. The sweep runs the engine's overdue tick and the participant returns a deadline no
     * earlier than the one the loop held, which the counter did not count. The participant now reports the tick's own
     * due ({@code NodeLoop.foundOverdue}), so the sweep counts as one that found work.
     */
    @Test
    void aSweepThatRunsTheEnginesOverdueTickCountsAsFindingWork() throws Exception {
        Member a = member("a", null);
        java.lang.reflect.Method loopOf = PeerNode.class.getDeclaredMethod("loop");
        loopOf.setAccessible(true);
        ((ai.badmonkey.agentspaces.peering.node.NodeLoop) loopOf.invoke(a.node())).markScripted(); // this test's find
        Ref<PMap<String, String>> ref = a.pdt().mapRef("facts", STR, STR);
        ref.swap(m -> m.assoc("a", "one"));
        time.advance(Duration.ofMinutes(1)); // quiet: the next due is minutes away
        assertThat(a.node().loopStats().backstopFoundBy()).isEmpty();
        java.lang.reflect.Field listeners = ai.badmonkey.agentspaces.pdt.refs.Refs.class
                .getDeclaredField("localChangeListeners");
        listeners.setAccessible(true);
        ((List<?>) listeners.get(a.pdt().refs())).clear(); // the cut: a local change wakes nobody
        long ticks = a.pdt().dueTicks();
        a.pdt().refs().forgetHeld(); // due now, unsignalled
        time.advance(ai.badmonkey.agentspaces.peering.node.NodeLoop.DEFAULT_BACKSTOP.plusSeconds(1));
        assertThat(a.pdt().dueTicks() - ticks).as("the sweep ran the overdue tick").isEqualTo(1);
        assertThat(a.node().loopStats().backstopFoundBy().keySet()).as("the sweep found the engine's late work")
                .anySatisfy(name -> assertThat(name).endsWith("/pdt"));
    }

    /**
     * EL-X-07, example 33's flake on the loop (srad/agentspaces-event-loop.md §10.47): while a ref has work in flight
     * (a fetch, a recheck, a chain batch) the engine's next due was "now + 250 ms" read afresh on every call, so the
     * participant ran every 250 ms and never found the engine due, and the work waited for the next change of the ref
     * space (a head's renewal, minutes away). A recheck put after the tick that read the space's signal, a fetch that
     * failed, or a completed fetch was left alone for the 120 s of the example's load timeout. The step is now a fixed
     * deadline after the engine's last tick.
     */
    @Test
    void anEngineWithWorkInFlightTicksAtItsStepWithNoChangeOfTheSpace() throws Exception {
        Member a = member("a", null);
        Ref<PMap<String, String>> ref = a.pdt().mapRef("facts", STR, STR);
        ref.swap(m -> m.assoc("a", "one"));
        time.advance(Duration.ofMinutes(1)); // quiet: the next due is minutes away
        java.lang.reflect.Field statesField = ai.badmonkey.agentspaces.pdt.refs.Refs.class.getDeclaredField("states");
        statesField.setAccessible(true);
        Object state = ((java.util.Map<?, ?>) statesField.get(a.pdt().refs())).get("facts");
        java.lang.reflect.Field wantedField = state.getClass().getDeclaredField("chainWanted");
        wantedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, ai.badmonkey.agentspaces.common.id.PeerId> wanted =
                (java.util.Map<String, ai.badmonkey.agentspaces.common.id.PeerId>) wantedField.get(state);
        String missing = "bafyreigsqqzxdzehb6y2egvajzolvxk5hgfoj4jhpotomh4xzxovxxlv7a"; // a node nobody holds
        long ticks = a.pdt().dueTicks();
        wanted.put(missing, a.node().peerId()); // work in flight, as a walk leaves it for the tick
        a.pdt().refs().forgetHeld(); // one wake of the participant, with no change of the ref space
        time.advance(Duration.ofSeconds(2));
        assertThat(a.pdt().dueTicks() - ticks).as("the engine ticked at its in-flight step").isGreaterThanOrEqualTo(1);
        assertThat(wanted).as("the tick took the wanted node into a batch").doesNotContainKey(missing);
    }

    /**
     * EL-C-04, EL-C-05: the collection period is local, on the monotonic base, so a wall step back of an hour does not
     * hold the collector for an hour: it still looks every minute of real time.
     */
    @Test
    void aWallStepBackDoesNotHoldTheCollector() throws Exception {
        Member a = member("a", null);
        time.advance(Duration.ofMinutes(2));
        time.stepWall(Duration.ofHours(-1));
        time.advance(Duration.ofSeconds(1));
        long looks = a.pdt().collectLooks();
        time.advance(Duration.ofMinutes(5));
        long ran = a.pdt().collectLooks() - looks;
        System.out.printf("[pdt-due] collection looks in 5 minutes after a wall step back of an hour: %d%n", ran);
        assertThat(ran).as("every minute of the monotonic base").isBetween(4L, 6L);
    }
}
