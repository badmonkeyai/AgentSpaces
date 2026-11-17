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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.content.tree.Cids;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Probe P38 of reviews/RBE-REGRESSION-MATRIX.md, the content store's part (srad/replication-by-entry.md RE-H-04,
 * RE-Q-REL-04): a receipt the store withdraws when it releases a pin ({@code ContentStore.withdrawWritten}) is
 * collected ten minutes past its own lease (the drift floor of SPEC §7.3) under the per-entry horizon. A lagging
 * replica that still holds the receipt re-offers it after that; the door refuses it, and no reader takes the store for
 * a holder again.
 *
 * <p>The receipt is written as the store writes it, a {@link StoreReceipt} under its own tags, and withdrawn by the
 * same cancel; the pin and release flow is covered by {@code StorePeerTest}. Note for the review: that test's restart
 * case withdraws from a second node holding the store's identity, the break condition of the horizon (P40), and is
 * safe only because the first node is gone by then.
 */
class HorizonLaggingCopyTest {

    /** Inside the five-minute frame age every receiver allows (PeerNode.MAX_FRAME_AGE_MILLIS). */
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
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zHorizonStore",
            PeerIdentity.generate().peerId(), GroupId.of("zHorizonStore"), Instant.EPOCH, Duration.ofDays(1),
            "horizon", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /**
     * A member of the probe's group. The lagging replica reconciles by the text digest, which still lists a present
     * entry whose lease lapsed; between two members on {@code wire.ae=2} such an entry is out of the audited set and
     * is never offered. A clock lagging far enough to hold it live is heard only through a bridge (the bridge's clock
     * below).
     */
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
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "worker")
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

    /** The actor and the lagging replica, both under the per-entry horizon. */
    private Member[] fleet() throws Exception {
        Member a = member("a", clock, true);
        Member b = member("b", bridging, true, "a");
        Member c = member("c", lagging, false, "b");
        assertThat(runUntil(4_000, Duration.ofSeconds(1),
                () -> a.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && c.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && b.space().horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY
                        && a.runtime().membership().allMembers().size() == 2
                        && c.runtime().membership().allMembers().size() == 2))
                .as("both replicas under the per-entry horizon").isTrue();
        return new Member[]{a, c};
    }

    /**
     * The probe itself: the entry reaches the lagging replica, a partition cuts it off, the actor withdraws the entry
     * as the module does, and collects the tombstone ten minutes past its own lease (the drift floor of SPEC §7.3),
     * long before twice the maximum lease. When the partition heals the lagging replica offers its present copy; the
     * door refuses and acknowledges it, and the actor never holds the entry again.
     */
    private void probe(Member a, Member c, EntryHandle handle, Runnable withdraw) {
        EntryId id = handle.entryId();
        assertThat(runUntil(200, Duration.ofMillis(250), () -> c.space().recordOf(id).isPresent())).isTrue();
        long lease = a.space().recordOf(id).orElseThrow().lease().expiresAtMillis();
        // The actor alone on one side; the bridge and the lagging replica together on the other, so neither hears
        // the withdrawal and the lagging replica's hybrid logical clock keeps following the bridge's.
        network.partition("a", "c");
        network.partition("a", "b");
        lagTarget = DRIFTED_LAG_MILLIS; // the cut-off clock slows, as a drifting clock does, never stepping back
        withdraw.run();
        assertThat(a.space().recordOf(id)).isEmpty();
        assertThat(runUntil(1_000, Duration.ofSeconds(1), () -> a.space().signedState(id).isEmpty()))
                .as("collected under the per-entry horizon").isTrue();
        assertThat(clock.millis()).isGreaterThanOrEqualTo(lease + SpaceStateCrdt.WITHDRAWN_DRIFT_FLOOR_MILLIS)
                .isLessThan(lease + 2 * a.space().maxLeaseMillis());
        assertThat(c.space().signedState(id)).as("still held, present, by the lagging replica").isPresent();
        long refusedBefore = a.space().collectedReoffersAcknowledged();
        long healedAt = clock.millis();
        network.heal();
        assertThat(runUntil(400, Duration.ofMillis(250), () -> {
            assertThat(a.space().recordOf(id, true)).as("never back on the replica that withdrew it").isEmpty();
            return a.space().collectedReoffersAcknowledged() > refusedBefore;
        })).as("the lagging copy reached the door and was refused there").isTrue();
        System.out.println("P38 store: the lagging copy was refused at the door " + (clock.millis() - healedAt)
                + " ms after the heal, on the fake clock");
    }

    /** An entry with a long lease, so the space's maximum lease, and with it the legacy horizon, is far out. */
    public record Marker(String name) {
    }

    private static void longLease(Member a) {
        a.space().write(new Marker("long"), Lease.of(Duration.ofHours(1)));
    }

    @Test
    void aWithdrawnReceiptReofferedByALaggingReplicaIsRefusedAtTheDoor() throws Exception {
        Member[] fleet = fleet();
        Member a = fleet[0];
        Member c = fleet[1];
        longLease(a);
        String root = Cids.of("pinned".getBytes(StandardCharsets.UTF_8), Cids.RAW);
        StoreReceipt receipt = new StoreReceipt(StoreReceipt.VERSION, root, a.identity().peerId(), 1_024,
                clock.millis() + Duration.ofMinutes(2).toMillis(), false);
        EntryHandle handle = a.space().write(receipt, Lease.of(Duration.ofMinutes(2)), receipt.tags());
        probe(a, c, handle, handle::cancel);
        assertThat(a.space().readAll(Template.of(StoreReceipt.class), 10)).as("no receipt names the store again")
                .isEmpty();
    }
}
