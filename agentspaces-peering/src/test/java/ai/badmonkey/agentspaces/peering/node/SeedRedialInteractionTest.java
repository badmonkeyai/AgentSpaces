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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.LinkFilter;
import ai.badmonkey.agentspaces.test.LinkFrame;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Feature interactions of the seed redial that no row of srad/agentspaces-event-loop.md names (§10.38, the close-out
 * review): each pairs the redial of §10.36 with an older feature of the node.
 *
 * <ul>
 *   <li>Two groups that share a seed, joined together: one group's introduction must not close the other's.</li>
 *   <li>A seed that is also a lost member: the lost-member retry must not dial it off the table while the redial
 *       does, and the link it answers on is still the seed link.</li>
 *   <li>A locator that answers new seeds while a round of the old ones is under way: the new seeds are dialled in
 *       that round, not a table step later.</li>
 * </ul>
 *
 * <p>Virtual time over the in-memory fabric, as {@link SeedRedialTest}.
 */
@Timeout(120)
class SeedRedialInteractionTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final SimNetwork network = new SimNetwork();
    private final Wire wire = new Wire();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> listeners = new ArrayList<>();
    private final Map<String, GroupRuntime> runtimes = new ConcurrentHashMap<>();

    private static GroupAdvertisement group(String name) {
        return new GroupAdvertisement("aspace://" + name, PeerIdentity.generate().peerId(), GroupId.of(name),
                Instant.EPOCH, Duration.ofDays(1), name, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    private final GroupAdvertisement groupA = group("zInteractA");
    private final GroupAdvertisement groupB = group("zInteractB");

    /** Every frame of the fabric by link and kind with its instants; a silenced address neither sends nor hears. */
    private final class Wire implements LinkFilter {
        final Map<String, List<Long>> times = new ConcurrentHashMap<>();
        final Set<String> silent = new CopyOnWriteArraySet<>();

        @Override
        public Verdict decide(LinkFrame frame) {
            times.computeIfAbsent(frame.from() + ">" + frame.to() + ":" + frame.kind(),
                    k -> new CopyOnWriteArrayList<>()).add(time.monoMillis());
            return silent.contains(frame.from()) || silent.contains(frame.to()) ? Verdict.drop() : Verdict.pass();
        }

        List<Long> instants(String from, String to, String kind, long fromMillis) {
            return times.getOrDefault(from + ">" + to + ":" + kind, List.of()).stream()
                    .filter(t -> t >= fromMillis).distinct().toList();
        }
    }

    @BeforeEach
    void wire() {
        network.filter(null, null, wire);
    }

    @AfterEach
    void tearDown() throws Exception {
        nodes.forEach(PeerNode::close);
        for (AutoCloseable listener : listeners) {
            listener.close();
        }
    }

    private static PeerAdvertisement.Endpoint mem(String address) {
        return new PeerAdvertisement.Endpoint("mem", address, 0);
    }

    private PeerNode bare(String name, boolean listening, PeerNode.Builder builder) throws IOException {
        PeerNode node = builder.build();
        if (listening) {
            node.listen(network.register(name), name);
        } else {
            node.transport(network.register(name)); // dial-only
        }
        nodes.add(node);
        return node;
    }

    private PeerNode.Builder builder(String name) {
        return PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(name.hashCode());
    }

    private PeerNode node(String name, boolean listening, String... seeds) throws IOException {
        PeerNode node = bare(name, listening, builder(name));
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(mem(seed));
        }
        runtimes.put(name, node.joinGroup(groupA, GroupMembership.Config.defaults(), endpoints));
        node.startTicking(TICK);
        return node;
    }

    private void silentSeed(String name) throws IOException {
        listeners.add(network.register(name).listen(name, connection -> { }));
    }

    private static boolean sees(GroupRuntime runtime, PeerNode other) {
        return runtime.membership().member(other.peerId()).isPresent();
    }

    private long until(Duration limit, Duration step, BooleanSupplier condition) {
        time.runDue();
        long start = time.monoMillis();
        while (!condition.getAsBoolean()) {
            if (time.monoMillis() - start > limit.toMillis()) {
                return -1;
            }
            time.advance(step);
        }
        return time.monoMillis() - start;
    }

    // ---- 1. two groups, one seed

    /**
     * A node that joins two groups at one seed dials the seed once per group. Before this review the second group's
     * introduction closed the first group's link while the seed's answer was still on its way (the leak rule of
     * §10.36 (f) keyed the link by endpoint alone), so the first group waited a whole table step (5 s) to join.
     */
    @Test
    void twoGroupsJoinedTogetherAtOneSeedBothJoinOnTheSeedsFirstAnswers() throws Exception {
        PeerNode seed = node("s", true);
        runtimes.put("s/B", seed.joinGroup(groupB, GroupMembership.Config.defaults(), List.of()));
        network.hold("s", "h"); // the seed's answers are on their way while the second group joins
        PeerNode host = bare("h", false, builder("h"));
        GroupRuntime a = host.joinGroup(groupA, GroupMembership.Config.defaults(), List.of(mem("s")));
        GroupRuntime b = host.joinGroup(groupB, GroupMembership.Config.defaults(), List.of(mem("s")));
        host.startTicking(TICK);
        time.advance(Duration.ofMillis(100));
        assertThat(network.heldCount("s", "h")).as("the seed's answers are held").isPositive();
        network.release("s", "h", SimNetwork.Order.FIFO, 0);
        long joined = until(Duration.ofSeconds(10), Duration.ofMillis(10), () -> sees(a, seed) && sees(b, seed));
        System.out.printf("[seed-interaction] two groups at one seed: both joined %d ms after the answers (A %s, B %s)%n",
                joined, sees(a, seed), sees(b, seed));
        assertThat(joined).as("both groups join on the seed's first answers, not a table step later")
                .isBetween(0L, 1_000L);
        assertThat(host.seedLinkStats().seedDials()).as("one dial per group at the join").isEqualTo(2);
    }

    // ---- 2. a seed that is also a lost member

    /**
     * The seed link's member goes silent and is dropped. With nobody in view the redial dials the seeds on the
     * table; the lost-member retry, which dials a lost member's endpoints on its own doubling backoff, must not dial
     * the same seed between the rounds (it did: twice the dials, and each closed the round's waiting link), and when
     * the seed answers again its link is the seed link, so a later wake PINGs it.
     */
    @Test
    void aLostSeedMemberIsDialledOnTheTableAloneAndIsTheSeedLinkWhenItAnswers() throws Exception {
        silentSeed("a");
        PeerNode b = node("b", true);
        PeerNode host = node("h", false, "a", "b");
        assertThat(until(Duration.ofSeconds(10), Duration.ofMillis(10),
                () -> sees(runtimes.get("h"), b) && sees(runtimes.get("b"), host))).isNotNegative();
        time.advance(Duration.ofSeconds(10));
        assertThat(host.seedLinkPeer(groupA.group())).isEqualTo(b.peerId());
        wire.silent.add("b");
        long silenced = time.monoMillis();
        long gone = until(Duration.ofSeconds(120), Duration.ofMillis(10), () -> !sees(runtimes.get("h"), b));
        assertThat(gone).as("b is dropped").isNotNegative();
        long dropped = time.monoMillis();
        time.advance(Duration.ofMinutes(2));
        List<Long> toA = wire.instants("h", "a", "RUMOR", dropped);
        List<Long> toB = wire.instants("h", "b", "RUMOR", dropped);
        System.out.printf("[seed-interaction] lost seed member: silenced %d, dropped %d (%s)%n  a %s%n  b %s%n",
                silenced, dropped, host.seedLinkStats(), wire.instants("h", "a", "RUMOR", silenced),
                wire.instants("h", "b", "RUMOR", silenced));
        assertThat(toB).as("b is dialled in the rounds alone, last: never between them").hasSameSizeAs(toA);
        for (int i = 0; i < toA.size(); i++) {
            assertThat(toB.get(i) - toA.get(i)).as("round " + i + ": b 250 ms after a")
                    .isEqualTo(SeedRedial.STAGGER_MILLIS);
        }

        wire.silent.remove("b");
        assertThat(until(Duration.ofSeconds(60), Duration.ofMillis(10), () -> sees(runtimes.get("h"), b)))
                .as("b answers a round").isNotNegative();
        time.advance(Duration.ofSeconds(1));
        assertThat(host.seedLinkPeer(groupA.group())).as("the link b answered on is the seed link")
                .isEqualTo(b.peerId());
    }

    // ---- 4. parity: the wake is a forward step past the keepalive bound, as in the clients and SPEC v0.1.37

    /**
     * SPEC v0.1.37 (§9.4, the wake row) and all four clients call a forward step past the 30 s keepalive bound a
     * wake. The core called it one only past the member TTL (45 s at the least), so a 40 s suspend left a dead seed
     * link to the ordinary probes there and nowhere else. A 40 s sleep is a wake: the seed is PINGed at once and
     * dropped 2 s later.
     */
    @Test
    void aFortySecondSleepIsAWakeAsInEveryClient() throws Exception {
        PeerNode s1 = node("s1", true);
        VirtualTime.NodeTime view = time.unwatchedNode();
        PeerNode host = bare("h", true, PeerNode.builder(PeerIdentity.generate()).clock(view).time(view)
                .randomSeed(7));
        GroupRuntime runtime = host.joinGroup(groupA, GroupMembership.Config.defaults(), List.of(mem("s1")));
        host.startTicking(TICK);
        assertThat(until(Duration.ofSeconds(30), Duration.ofMillis(100),
                () -> sees(runtime, s1) && sees(runtimes.get("s1"), host))).isNotNegative();
        time.advance(Duration.ofSeconds(10));
        assertThat(GroupMembership.KEEPALIVE_TTL).isEqualTo(Duration.ofSeconds(30));
        assertThat(runtime.membership().memberTtlMillis()).as("the member TTL is longer than the sleep")
                .isGreaterThan(40_000L);
        view.suspend();
        wire.silent.add("s1");
        time.advance(Duration.ofSeconds(40));
        view.resume();
        long woke = time.monoMillis();
        long left = until(Duration.ofSeconds(30), Duration.ofMillis(10), () -> !sees(runtime, s1));
        System.out.printf("[seed-interaction] a 40 s sleep: s1 left %d ms after the wake (%s)%n", left,
                host.seedLinkStats());
        assertThat(host.seedLinkStats().resumes()).as("a 40 s forward step is a wake").isEqualTo(1);
        assertThat(host.seedLinkStats().wakePings()).as("the wake PINGs the seed").isEqualTo(1);
        assertThat(wire.instants("h", "s1", "PING", woke)).isNotEmpty();
        assertThat(left).as("no answer in 2 s: failed over").isBetween(2_000L, 2_100L);
    }

    // ---- 3. the locator's answer during a round

    /** A locator whose answer the test moves, counting the asks. */
    private static final class Movable implements BeaconLocator {
        final AtomicReference<String> address = new AtomicReference<>();
        final AtomicInteger asked = new AtomicInteger();

        Movable(String address) {
            this.address.set(address);
        }

        @Override
        public String name() {
            return "movable";
        }

        @Override
        public BeaconHint locate(BeaconRequest request) {
            asked.incrementAndGet();
            return new BeaconHint(List.of(mem(address.get())), Optional.empty(), Optional.empty(), Map.of(),
                    Duration.ofSeconds(1), "movable " + address.get());
        }
    }

    /**
     * The seed moved. An empty view's round of the old seeds asks the locator again; its answer arrives while that
     * round still waits on its silent seed. The new seed is dialled in the round under way (after the stagger), not
     * after the table's next wait, which the round swallowed before this review.
     */
    @Test
    void aLocatorAnswerDuringARoundIsDialledInThatRound() throws Exception {
        silentSeed("x");
        PeerNode a = node("a", true);
        Movable locator = new Movable("x");
        PeerNode host = bare("h", true, builder("h").locateInterval(Duration.ZERO, Duration.ofMinutes(10)));
        GroupRuntime runtime = host.joinGroup(groupA, GroupMembership.Config.defaults(), locator);
        host.startTicking(TICK);
        assertThat(locator.asked).as("asked at the join").hasValue(1);
        time.advance(Duration.ofSeconds(2)); // the join's round dialled x and failed; the table waits 5 s
        locator.address.set("a");
        long asked = until(Duration.ofSeconds(10), Duration.ofMillis(10), () -> locator.asked.get() > 1);
        assertThat(asked).as("the next round asks the locator again").isNotNegative();
        long askedAt = time.monoMillis();
        // The re-ask runs on a virtual thread: wait (in real time, bounded) for its answer to reach the node.
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!host.groupSeedsOf(groupA.group()).contains(mem("a")) && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(host.groupSeedsOf(groupA.group())).contains(mem("a"));
        long dialled = until(Duration.ofSeconds(40), Duration.ofMillis(10),
                () -> !wire.instants("h", "a", "RUMOR", askedAt).isEmpty());
        long joined = until(Duration.ofSeconds(40), Duration.ofMillis(10), () -> sees(runtime, a));
        System.out.printf("[seed-interaction] locator answer mid-round: a dialled %d ms after the re-ask%n",
                dialled);
        assertThat(dialled).as("the new seed is dialled in the round under way, after x's stagger")
                .isBetween(0L, SeedRedial.STAGGER_MILLIS + 20);
        assertThat(joined).isNotNegative();
    }
}
