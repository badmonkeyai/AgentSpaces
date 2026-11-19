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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Probe P38 of reviews/RBE-REGRESSION-MATRIX.md, the content swarm's part (srad/replication-by-entry.md RE-H-04,
 * RE-Q-REL-04): a holding the swarm cancels (a rewrite, a release) is collected ten minutes past its own lease
 * under the per-entry horizon (the drift floor of SPEC §7.3), and a lagging replica that still holds the holding live
 * re-offers it after that; the door refuses it and acknowledges it, and the provider index never shows the withdrawn
 * holder again.
 *
 * <p>The lagging replica runs its clock four minutes behind while the group forms, inside the five-minute frame age
 * every receiver allows, and drifts to eight minutes behind while a partition cuts it off, and it reconciles by the
 * text digest ({@code agentspaces.gossip.audit=false}), which still lists a present entry whose lease lapsed. When the
 * partition heals the holding has lapsed by its clock but is not yet collected there, so it offers the holding to the
 * replica that withdrew it and collected it: the re-offer the door must refuse. A bridge four minutes behind stays
 * with it through the partition, so its frames carry stamps the actor accepts (the bridge's clock below). (Between two
 * members on {@code wire.ae=2} a lapsed entry is out of the audited set and is never offered.)
 */
class HorizonLaggingCopyTest {

    private static final long LAG_MILLIS = Duration.ofMinutes(4).toMillis();

    /**
     * The lag the drifted clock reaches while it is cut off: past the five-minute lapse grace and within MAX_DRIFT (ten
     * minutes, SPEC §11). Since the drift floor of SPEC §7.3 the actor collects a withdrawal ten minutes past its
     * lease, and a replica still four minutes behind has dropped its own lapsed copy a minute before that, so nothing
     * is left to re-offer. One that has drifted to eight minutes behind still holds the copy within its grace, and
     * offers it once the healed link has carried the actor's stamps to its clock.
     */
    private static final long DRIFTED_LAG_MILLIS = Duration.ofMinutes(8).toMillis();

    private final TestClock clock = TestClock.create();
    /** The lagging clock's lag now, and the lag it drifts to; it grows by half of each step, so it never runs back. */
    private volatile long lag = LAG_MILLIS;
    private volatile long lagTarget = LAG_MILLIS;
    private final InstantSource lagging = () -> clock.instant().minusMillis(lag);
    /**
     * The bridge's clock, four minutes behind the actor's throughout. A replica whose clock trails the actor's by more
     * than the five-minute frame age is heard by the actor only while its frames carry newer stamps than its own clock
     * reads: stamps its hybrid logical clock took from members it hears. Cut off together with the bridge, the lagging
     * replica's stamps follow the bridge's, within five minutes of the actor's, so when the partition heals the actor
     * hears it again and it offers the copy it still holds. Alone, its stamps would fall eight minutes behind and the
     * actor would drop its frames as too old, so the copy would never reach the door.
     */
    private final InstantSource bridging = () -> clock.instant().minusMillis(LAG_MILLIS);
    private final SimNetwork network = new SimNetwork();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zHorizonContent",
            PeerIdentity.generate().peerId(), GroupId.of("zHorizonContent"), Instant.EPOCH, Duration.ofDays(1),
            "content", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Member member(String address, InstantSource memberClock, boolean audit, String... seeds) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(memberClock).randomSeed(address.hashCode()).audit(audit)
                .build();
        nodes.add(node);
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        // A long member TTL keeps the partitioned member in view, as a slow or cut-off member stays in a real one.
        GroupRuntime runtime = node.joinGroup(groupAd,
                new GroupMembership.Config(Duration.ofHours(1), Duration.ofSeconds(2), 2), endpoints);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(memberClock).settleWindow(Duration.ZERO).build();
        return new Member(identity, node, runtime, space);
    }

    private boolean runUntil(int ticks, Duration step, BooleanSupplier until) {
        for (int i = 0; i < ticks && !until.getAsBoolean(); i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(step);
            lag = Math.min(lagTarget, lag + step.toMillis() / 2);
        }
        return until.getAsBoolean();
    }

    @Test
    void aCancelledHoldingReofferedByALaggingReplicaIsRefusedAtTheDoor() throws Exception {
        Member a = member("a", clock, true);
        Member b = member("b", bridging, true, "a");
        Member c = member("c", lagging, false, "b");
        assertThat(runUntil(4_000, Duration.ofSeconds(1),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && c.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && b.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && a.runtime().membership().allMembers().size() == 2
                        && c.runtime().membership().allMembers().size() == 2))
                .as(() -> "every replica under the per-entry horizon: b " + b.space().horizonMode() + "; a "
                        + a.space().horizonMode() + " with "
                        + a.runtime().membership().allMembers().size() + " member(s), hinted "
                        + a.runtime().advertisesHorizon(c.identity().peerId()) + "; c " + c.space().horizonMode()
                        + " with " + c.runtime().membership().allMembers().size() + " member(s), hinted "
                        + c.runtime().advertisesHorizon(a.identity().peerId()))
                .isTrue();

        // The swarm's own write path, then the cancel its rewrite and its release perform.
        ContentSpaces swarmA = new ContentSpaces(a.space());
        // A long-leased holding elsewhere puts the space's maximum lease, and the legacy horizon, an hour out.
        swarmA.writeHolding(Holding.of("bafkreilonglease", a.identity().peerId(), fullBitSet(1), 1, null),
                Lease.of(Duration.ofHours(1)));
        String root = "bafkreihorizonprobe";
        EntryHandle holding = swarmA.writeHolding(Holding.of(root, a.identity().peerId(), fullBitSet(4), 4, null),
                Lease.of(Duration.ofMinutes(2)));
        EntryId id = holding.entryId();
        assertThat(runUntil(200, Duration.ofMillis(250), () -> c.space().recordOf(id).isPresent())).isTrue();
        long lease = a.space().recordOf(id).orElseThrow().lease().expiresAtMillis();

        // The actor alone on one side; the bridge and the lagging replica together on the other, so neither hears
        // the withdrawal and the lagging replica's hybrid logical clock keeps following the bridge's.
        network.partition("a", "c");
        network.partition("a", "b");
        lagTarget = DRIFTED_LAG_MILLIS; // the cut-off clock slows, as a drifting clock does, never stepping back
        holding.cancel();
        assertThat(runUntil(1_000, Duration.ofSeconds(1), () -> a.space().signedState(id).isEmpty()))
                .as("collected under the per-entry horizon").isTrue();
        assertThat(clock.millis()).isGreaterThanOrEqualTo(lease + SpaceStateCrdt.WITHDRAWN_DRIFT_FLOOR_MILLIS)
                .isLessThan(lease + 2 * a.space().maxLeaseMillis());
        assertThat(c.space().signedState(id)).as("still held, present, by the lagging replica").isPresent();

        long refusedBefore = a.space().collectedReoffersAcknowledged();
        long healedAt = clock.millis();
        network.heal();
        // 200 s: a lost member is re-dialed at most a minute apart (a quarter either way) in its first hour lost
        // (PeerNode.LOST_RETRY_EARLY_CAP_MILLIS), so after this partition the fleet rejoins within about 75 s
        assertThat(runUntil(800, Duration.ofMillis(250), () -> {
            assertThat(a.space().recordOf(id, true)).as("never back on the replica that withdrew it").isEmpty();
            return a.space().collectedReoffersAcknowledged() > refusedBefore;
        })).as(() -> "the lagging copy reached the door and was refused there; a: stale frames "
                + a.node().framesStale() + ", members " + a.runtime().membership().allMembers().size()
                + "; c: stale frames " + c.node().framesStale() + ", far-future " + c.node().framesFarFuture() + "/"
                + c.node().framesFarFutureSkewed() + ", members " + c.runtime().membership().allMembers().size()
                + ", holds the copy " + c.space().signedState(id).isPresent() + " (present "
                + c.space().recordOf(id).isPresent() + "), rounds " + c.runtime().gossip().antiEntropyRounds()
                + "; a rounds " + a.runtime().gossip().antiEntropyRounds() + ", form toward c "
                + a.runtime().gossip().lastFormToward(c.identity().peerId()) + ", stale at b " + b.node().framesStale()
                + ", b holds " + b.space().signedState(id).isPresent() + ", lag " + lag + " ms")
                .isTrue();
        System.out.println("P38 content: the lagging copy was refused at the door " + (clock.millis() - healedAt)
                + " ms after the heal, on the fake clock");
        assertThat(swarmA.holdings(root)).as("the provider index never shows the withdrawn holder again").isEmpty();
    }

    private static BitSet fullBitSet(int pieces) {
        BitSet have = new BitSet(pieces);
        have.set(0, pieces);
        return have;
    }
}
