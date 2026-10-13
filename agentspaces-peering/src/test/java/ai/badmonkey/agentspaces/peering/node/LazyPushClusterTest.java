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
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lazy push across real nodes (PERF2 §3.4): once the tree has pruned itself, each rumor costs
 * one RUMOR frame per receiving member rather than fanout per member; a member that does not
 * advertise the hint still receives every item through the random fan-out and never sees a
 * HAVE, GRAFT or PRUNE; and a member cut off from the tree is grafted back by an announcement.
 */
class LazyPushClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<GroupRuntime> runtimes = new ArrayList<>();
    private final List<List<String>> received = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zTree");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zTree",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "tree", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, boolean lazyPush) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .lazyPush(lazyPush).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                nodes.size() == 1 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
        runtimes.add(runtime);
        List<String> seen = new CopyOnWriteArrayList<>();
        runtime.gossip().onStream("news", (from, id, payload) -> seen.add(new String(payload, StandardCharsets.UTF_8)));
        received.add(seen);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Ticks (one second of clock per round) and sleeps until the condition holds or five seconds pass. */
    private boolean until(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(25);
            tickAll(1);
        }
        return condition.getAsBoolean();
    }

    private boolean everyoneHas(int count) {
        return received.stream().allMatch(list -> list.size() >= count);
    }

    private long receivedRumors() {
        return nodes.stream().mapToLong(n -> n.traffic().receivedByKind().getOrDefault(Envelope.Kind.RUMOR, 0L)).sum();
    }

    private long sent(Envelope.Kind kind) {
        return nodes.stream().mapToLong(n -> n.traffic().sentByKind().getOrDefault(kind, 0L)).sum();
    }

    /** Publishes from one member; the bus does not deliver to its own handler, so the publisher is counted here. */
    private void publish(int from, String text) {
        received.get(from).add(text);
        runtimes.get(from).gossip().publish("news", text, text.getBytes(StandardCharsets.UTF_8));
    }

    private void cluster(int size, boolean lastIsLegacy) throws IOException {
        for (int i = 0; i < size; i++) {
            node("n" + i, 100 + i, !(lastIsLegacy && i == size - 1));
        }
        tickAll(6);
        for (GroupRuntime runtime : runtimes) {
            assertThat(runtime.membership().allMembers()).hasSize(size - 1);
        }
    }

    @Test
    @Timeout(60)
    void oncePrunedEachRumorReachesEachMemberOnce() throws Exception {
        cluster(6, false);
        int n = nodes.size();
        // The tree forms on the first rumors: every eager edge carries them and duplicates prune.
        for (int i = 0; i < 12; i++) {
            publish(i % n, "warm-" + i);
        }
        assertThat(until(() -> everyoneHas(12))).as("warm-up items reach everyone").isTrue();
        tickAll(2);
        long rumorsBefore = receivedRumors();
        long graftsBefore = sent(Envelope.Kind.GRAFT);
        long prunesBefore = sent(Envelope.Kind.PRUNE);
        for (int i = 0; i < 30; i++) {
            publish(i % n, "item-" + i);
        }
        assertThat(until(() -> everyoneHas(42))).as("every member received every item").isTrue();
        for (List<String> seen : received) {
            assertThat(seen).hasSize(42).doesNotHaveDuplicates();
        }
        long rumors = receivedRumors() - rumorsBefore;
        long ideal = 30L * (n - 1);
        assertThat(rumors).as("the tree carries each item to each member about once (random fan-out 3 cost ~%d)",
                        30L * (n - 1) * 3).isBetween(ideal, ideal + ideal / 3);
        assertThat(prunesBefore).as("the tree pruned itself during the warm-up").isGreaterThan(0);
        assertThat(sent(Envelope.Kind.HAVE)).as("announcements went out on the period").isGreaterThan(0);
        assertThat(sent(Envelope.Kind.GRAFT) - graftsBefore).as("a settled tree needs no repair, or almost none")
                .isLessThanOrEqualTo(3);
    }

    @Test
    @Timeout(60)
    void aMemberWithoutTheHintGetsEveryItemByFanoutAndNeverSeesTheNewFrames() throws Exception {
        cluster(5, true);
        PeerNode legacy = nodes.get(4);
        for (int i = 0; i < 20; i++) {
            publish(i % 4, "item-" + i);
        }
        assertThat(until(() -> everyoneHas(20))).as("the legacy member received every item").isTrue();
        assertThat(received.get(4)).hasSize(20).doesNotHaveDuplicates();
        var kinds = legacy.traffic().receivedByKind();
        assertThat(kinds.getOrDefault(Envelope.Kind.HAVE, 0L)).isZero();
        assertThat(kinds.getOrDefault(Envelope.Kind.GRAFT, 0L)).isZero();
        assertThat(kinds.getOrDefault(Envelope.Kind.PRUNE, 0L)).isZero();
        assertThat(legacy.traffic().sentByKind().keySet())
                .doesNotContain(Envelope.Kind.HAVE, Envelope.Kind.GRAFT, Envelope.Kind.PRUNE);
        // Its own publish still reaches the others, and a duplicate it sends is never answered with a PRUNE.
        publish(4, "from-legacy");
        assertThat(until(() -> everyoneHas(21))).isTrue();
        assertThat(kinds.getOrDefault(Envelope.Kind.PRUNE, 0L)).isZero();
    }

    @Test
    @Timeout(60)
    void aMemberCutOffFromTheTreeIsGraftedBackByAnAnnouncement() throws Exception {
        cluster(5, false);
        for (int i = 0; i < 10; i++) {
            publish(i % 5, "warm-" + i);
        }
        assertThat(until(() -> everyoneHas(10))).isTrue();
        // Node 4 tells every other member to stop pushing to it: an orphan with only announcements left.
        PeerNode orphan = nodes.get(4);
        byte[] hash = Digests.sha256("warm-0".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 4; i++) {
            orphan.send(groupId, nodes.get(i).peerId(), Envelope.Kind.PRUNE, new Bodies.Prune("news", hash));
        }
        assertThat(until(() -> runtimes.stream().limit(4).allMatch(r -> r.gossip().lazyPeers().contains(orphan.peerId()))))
                .as("every other member now holds the orphan lazy").isTrue();
        long graftsBefore = orphan.traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L);
        publish(0, "after-cut");
        assertThat(until(() -> received.get(4).contains("after-cut")))
                .as("the orphan hears a HAVE, grafts, and gets the item").isTrue();
        assertThat(orphan.traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L)).isGreaterThan(graftsBefore);
        assertThat(received.get(4)).doesNotHaveDuplicates();
        // Repaired: the next item arrives eagerly, without another graft.
        long graftsAfterRepair = orphan.traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L);
        publish(1, "after-repair");
        assertThat(until(() -> received.get(4).contains("after-repair"))).isTrue();
        assertThat(orphan.traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L)).isEqualTo(graftsAfterRepair);
    }

    /**
     * Five members (the tree is in use above the fan-out), one cut off from every other by its
     * own prunes: once the cut has settled, no standalone announcement goes out any more, and
     * the member hears of a later item only from a probe, grafts, and gets it.
     */
    @Test
    @Timeout(60)
    void anAgedTreeRepairsACutOffMemberThroughTheProbesAlone() throws Exception {
        cluster(5, false);
        for (int i = 0; i < 10; i++) {
            publish(i % 5, "warm-" + i);
        }
        assertThat(until(() -> everyoneHas(10))).isTrue();
        PeerNode cut = nodes.get(4);
        byte[] hash = Digests.sha256("warm-0".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 4; i++) {
            cut.send(groupId, nodes.get(i).peerId(), Envelope.Kind.PRUNE, new Bodies.Prune("news", hash));
        }
        assertThat(until(() -> runtimes.stream().limit(4).allMatch(r -> r.gossip().lazyPeers().contains(cut.peerId())))).isTrue();
        tickAll(GossipBus_SETTLING_TICKS + 4); // the prunes are old news: no standalone announcements any more
        long havesHeard = cut.traffic().receivedByKind().getOrDefault(Envelope.Kind.HAVE, 0L);
        long graftsBefore = cut.traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L);
        publish(0, "late");
        assertThat(until(() -> received.get(4).contains("late")))
                .as("the cut-off member learns of the item from a probe, grafts, and gets it").isTrue();
        assertThat(cut.traffic().receivedByKind().getOrDefault(Envelope.Kind.HAVE, 0L) - havesHeard)
                .as("it heard of it from a probe, not from a HAVE frame").isZero();
        assertThat(cut.traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L)).isGreaterThan(graftsBefore);
        assertThat(received.get(4)).doesNotHaveDuplicates();
    }

    private static final int GossipBus_SETTLING_TICKS = 32;
}
