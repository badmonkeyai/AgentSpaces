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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No mode thrashes (srad/replication-by-entry.md RE-Q-REL-09, decision 19), with the status figures that show it
 * (RE-Q-MNT-02): the anti-entropy form a member takes toward a partner changes only when the partner's card does,
 * through a card that flaps; a member that joins and leaves again and again is given a form on its card and dropped
 * from every count when it leaves, and each newcomer converges; a session that trips its exchange cap waits one full
 * round and no more; and a pace raised at run time spaces the rounds at its raised value for one period before a
 * lowering takes effect.
 */
class ReplicationHysteresisTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));
    private static final String STREAM = "space:tasks";

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zHysteresis");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zHysteresis", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "hysteresis",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
        PeerId id() {
            return identity.peerId();
        }

        GossipBus gossip() {
            return runtime.gossip();
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer peer(String address, long seed, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed)
                .peerAdInterval(Duration.ofSeconds(2)).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        nodes.add(node);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker").clock(clock)
                .settleWindow(Duration.ZERO).build();
        return new Peer(identity, node, runtime, space);
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Ticks until the condition holds, at most the given rounds; returns whether it held. */
    private boolean tickUntil(int rounds, BooleanSupplier condition) {
        for (int i = 0; i < rounds && !condition.getAsBoolean(); i++) {
            tick(1);
        }
        return condition.getAsBoolean();
    }

    /** Runs work on a thread with a deep stack: the fabric nests every exchange of an audit in the call that opens it. */
    private static void deep(Runnable work) throws Exception {
        AtomicReference<Throwable> failed = new AtomicReference<>();
        Thread thread = new Thread(null, () -> {
            try {
                work.run();
            } catch (Throwable t) {
                failed.set(t);
            }
        }, "hysteresis-deep", 512L << 20);
        thread.start();
        thread.join();
        if (failed.get() instanceof Exception e) {
            throw e;
        }
        if (failed.get() instanceof Error e) {
            throw e;
        }
    }

    private static Set<String> topics(ReplicatedSpace space) {
        Set<String> out = new HashSet<>();
        for (TaskEntry entry : space.readAll(Template.of(TaskEntry.class), 100_000)) {
            out.add(entry.topic());
        }
        return out;
    }

    /**
     * A card that flaps: b's card alternates between {@code wire.ae=0} and {@code wire.ae=2} six times. Within each
     * card a's every round toward b takes one form, the card's; a's form changes exactly once per card change, never
     * in between; nothing either side sends is refused; and what b writes under each card reaches a.
     */
    @Test
    void theFormTowardAPartnerChangesOnlyWhenItsCardDoes() throws Exception {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        assertThat(a.gossip().formToward(b.id())).isEqualTo(GossipBus.Form.TREE);
        deep(() -> a.gossip().antiEntropyWith(b.id()));
        long before = a.gossip().formChanges();
        int cardChanges = 0;
        for (int phase = 0; phase < 6; phase++) {
            boolean audits = phase % 2 == 1;
            GossipBus.Form expected = audits ? GossipBus.Form.TREE : GossipBus.Form.HASHED;
            b.node().groupHint(groupId, PeerNode.AE_HINT, audits ? PeerNode.AE_LEVEL : "0");
            cardChanges++;
            assertThat(tickUntil(12, () -> a.gossip().formToward(b.id()) == expected))
                    .as("phase %d: a reads b's new card", phase).isTrue();
            network.partition("a", "b");
            for (int i = 0; i < 5; i++) {
                b.space().write(new TaskEntry("phase-" + phase + "-" + i, i), HOUR);
            }
            network.heal();
            long changesAtStart = a.gossip().formChanges();
            for (int round = 0; round < 4; round++) {
                deep(() -> a.gossip().antiEntropyWith(b.id()));
                assertThat(a.gossip().lastFormToward(b.id())).as("phase %d round %d", phase, round)
                        .isEqualTo(expected);
                tick(1);
            }
            assertThat(a.gossip().formChanges() - changesAtStart).as("phase %d: no change while the card holds",
                    phase).isLessThanOrEqualTo(1);
        }
        assertThat(a.gossip().formChanges() - before).as("one form change per card change, and no more")
                .isEqualTo(cardChanges);
        assertThat(topics(a.space())).as("every phase's writes reached a").isEqualTo(topics(b.space())).hasSize(30);
        assertThat(a.gossip().auditSignals().refusals()).doesNotContainKeys("undecodable", "lacks-hint");
        assertThat(b.gossip().auditSignals().refusals()).doesNotContainKeys("undecodable", "lacks-hint");
    }

    /**
     * A member that joins and leaves: three newcomers in turn join, converge, and leave by a partition that outlasts
     * the member TTL. While each is in view a's status counts it on wire.ae=2 and its space converges by the audit;
     * once it is gone a's status counts only b, and the churn never moved the form toward b.
     */
    @Test
    void aMemberThatJoinsAndLeavesIsCountedOnItsCardAndDroppedWhenItLeaves() throws Exception {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        for (int i = 0; i < 80; i++) {
            a.space().write(new TaskEntry("base-" + i, i), HOUR);
        }
        tick(4);
        deep(() -> a.gossip().antiEntropyWith(b.id()));
        long formChanges = a.gossip().formChanges();
        for (int cycle = 0; cycle < 3; cycle++) {
            String address = "c" + cycle;
            Peer c = peer(address, 10 + cycle, "a");
            assertThat(tickUntil(20, () -> topics(c.space()).size() == 80 + 0
                    && a.gossip().partnersByForm().get(GossipBus.Form.TREE) == 2))
                    .as("cycle %d: the newcomer converged and is counted on its card", cycle).isTrue();
            GossipBus.ReplicationStatus status = a.runtime().replicationStatus();
            assertThat(status.partnersByForm()).containsEntry(GossipBus.Form.TREE, 2)
                    .containsEntry(GossipBus.Form.HASHED, 0).containsEntry(GossipBus.Form.TEXT, 0);
            assertThat(c.space().streamHash()).isEqualTo(a.space().streamHash());

            network.partition("a", address);
            network.partition("b", address);
            assertThat(tickUntil(90, () -> a.runtime().membership().member(c.id()).isEmpty()
                    && a.gossip().partnersByForm().get(GossipBus.Form.TREE) == 1))
                    .as("cycle %d: the member left the view", cycle).isTrue();
            assertThat(a.runtime().replicationStatus().partnersByForm().values().stream().mapToInt(Integer::intValue)
                    .sum()).as("the status counts the view alone").isEqualTo(a.runtime().membership().allMembers().size());
            c.node().close();
            nodes.remove(c.node());
            network.heal();
        }
        assertThat(a.gossip().formToward(b.id())).isEqualTo(GossipBus.Form.TREE);
        deep(() -> a.gossip().antiEntropyWith(b.id()));
        assertThat(a.gossip().formChanges()).as("the churn never changed the form toward b").isEqualTo(formChanges);
    }

    /**
     * A cap that trips: under a small budget a large descent closes its session at 64 exchanges. Each closure makes
     * the stream skip exactly the next opening, never two, and the space still converges.
     */
    @Test
    void aSessionThatTripsItsCapWaitsOneRoundAndNoMore() throws Exception {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        a.gossip().auditBudget(4096);
        network.partition("a", "b");
        for (int i = 0; i < 5_000; i++) {
            b.space().write(new TaskEntry("bulk-" + i, i), HOUR);
        }
        network.heal();
        List<Long> cappedAfterRound = new ArrayList<>();
        List<Long> coolingAfterRound = new ArrayList<>();
        deep(() -> {
            for (int round = 0; round < 16 && a.space().knownEntries() < 5_000; round++) {
                a.gossip().antiEntropyWith(b.id());
                Map<String, Long> refusals = b.gossip().auditSignals().refusals();
                cappedAfterRound.add(refusals.getOrDefault("session-capped", 0L));
                coolingAfterRound.add(refusals.getOrDefault("session-cooling", 0L));
                clock.advance(Duration.ofSeconds(1));
            }
        });
        long capped = cappedAfterRound.get(cappedAfterRound.size() - 1);
        long cooling = coolingAfterRound.get(coolingAfterRound.size() - 1);
        assertThat(capped).as("the cap tripped").isPositive();
        assertThat(cooling).as("each closure skipped one opening; the last may still be waiting")
                .isBetween(capped - 1, capped);
        for (int r = 1; r < cappedAfterRound.size(); r++) {
            assertThat(coolingAfterRound.get(r) - coolingAfterRound.get(r - 1)).as("round %d skips at most once", r)
                    .isLessThanOrEqualTo(1);
        }
        assertThat(a.space().knownEntries()).isEqualTo(5_000);
        assertThat(a.space().streamHash()).isEqualTo(b.space().streamHash());
        GossipBus.StreamStatus tasks = a.gossip().replicationStatus().streams().stream()
                .filter(s -> s.stream().equals(STREAM)).findFirst().orElseThrow();
        assertThat(tasks.auditRounds()).as("the status counts the stream's audit rounds").isPositive();
        assertThat(tasks.auditBytes()).isPositive();
        ReconcilableState.Figures figures = tasks.figures();
        assertThat(figures.live()).isEqualTo(5_000);
        assertThat(figures.tombstones()).isZero();
        assertThat(figures.trieDepth()).as("5,000 slots run two or three nibbles deep").isBetween(2, 4);
    }

    /**
     * A pace raised at run time: with the stretch off, a's periodic rounds follow the base period it is set to; a
     * raise to four seconds spaces them four seconds apart at once, and a lowering asked for straight after waits
     * until the raised period has served one period of its own, then rounds come every second again.
     */
    @Test
    void aRaisedPeriodSpacesTheRoundsForOnePeriodBeforeALoweringTakesEffect() throws Exception {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        a.gossip().antiEntropyStretch(1);
        long start = a.gossip().antiEntropyRounds();
        tick(8);
        long atBase = a.gossip().antiEntropyRounds() - start;
        assertThat(atBase).as("a round a second at the founding's period").isBetween(7L, 9L);

        a.gossip().antiEntropyPeriod(Duration.ofSeconds(4));
        a.gossip().antiEntropyPeriod(Duration.ofSeconds(1));
        assertThat(a.gossip().antiEntropyPeriod()).hasSeconds(4);
        start = a.gossip().antiEntropyRounds();
        tick(3);
        assertThat(a.gossip().antiEntropyPeriod()).as("still held").hasSeconds(4);
        long held = a.gossip().antiEntropyRounds() - start;
        assertThat(held).as("at most one round in three seconds at four").isLessThanOrEqualTo(1L);
        tick(2);
        assertThat(a.gossip().antiEntropyPeriod()).as("the hold served").hasSeconds(1);
        start = a.gossip().antiEntropyRounds();
        tick(8);
        assertThat(a.gossip().antiEntropyRounds() - start).as("a round a second again").isBetween(6L, 9L);
        assertThat(a.runtime().replicationStatus().paces()).anySatisfy(p -> {
            assertThat(p.name()).isEqualTo("anti-entropy-period");
            assertThat(p.raises()).isEqualTo(1);
            assertThat(p.deferred()).isEqualTo(1);
        });
        assertThat(b.gossip().antiEntropyPeriod()).as("a pace is one member's own").hasSeconds(1);
    }
}
