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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Entry-handle and subscription lifecycle on the replicated space (spec §7.2, P2):
 * renewals and cancellations replicate as issuer-signed state deltas, lapsed
 * handles refuse renewal, and subscriptions see each transition exactly once even
 * though the same state arrives by rumor and again by anti-entropy.
 */
class ReplicatedSpaceLifecycleTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zLifecycle");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zLifecycle", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "lifecycle",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .build();
        nodes.add(node);
        return new Peer(node, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    /** Spec §7.2/P2: a renewed write lease replicates, so the entry outlives its original TTL fleet-wide. */
    @Test
    void aRenewedWriteLeaseReplicates() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        EntryHandle handle = a.space().write(new TaskEntry("keep-alive", 1),
                Lease.of(Duration.ofMinutes(1)));
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();

        clock.advance(Duration.ofSeconds(50));
        handle.renew(Duration.ofMinutes(10));
        tickAll(4);
        clock.advance(Duration.ofMinutes(2)); // past the original 1-minute lease

        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .as("the renewed lease replaced the original on the remote replica")
                .isPresent();
    }

    /** Spec §7.2: cancelling a handle withdraws the entry on every replica, leaving a tombstone. */
    @Test
    void cancelWithdrawsTheEntryEverywhere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        EntryHandle handle = a.space().write(new TaskEntry("withdraw-me", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();

        handle.cancel();
        tickAll(4);

        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(b.space().knownEntries()).as("withdrawn, not forgotten").isEqualTo(1);
        assertThat(b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isEmpty();
    }

    /** Spec §7.2/P2: a handle whose write lease has lapsed cannot be renewed. */
    @Test
    void aLapsedHandleRefusesRenewal() throws Exception {
        Peer a = newPeer("a", 1);
        tickAll(2);

        EntryHandle handle = a.space().write(new TaskEntry("ephemeral", 1),
                Lease.of(Duration.ofMinutes(1)));
        clock.advance(Duration.ofMinutes(2));

        assertThatThrownBy(() -> handle.renew(Duration.ofMinutes(5)))
                .isInstanceOf(LeaseExpiredException.class);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /** Spec §7.2/P2: a take handle whose TAKE lease has lapsed cannot be renewed or completed. */
    @Test
    void aLapsedTakeRefusesRenewalAndCompletion() throws Exception {
        Peer a = newPeer("a", 1);
        tickAll(2);
        a.space().write(new TaskEntry("short-hold", 1), MINUTES_30);
        TakenEntry<TaskEntry> taken = a.space().take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();

        clock.advance(Duration.ofMinutes(2));

        assertThatThrownBy(() -> taken.renew(Duration.ofMinutes(5)))
                .isInstanceOf(LeaseExpiredException.class);
        assertThatThrownBy(() -> a.space().complete(taken))
                .isInstanceOf(LeaseExpiredException.class);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("the entry reappeared for other takers").isPresent();
    }

    /** Spec §7.2 notify: each transition is delivered once per replica despite rumor plus anti-entropy re-delivery. */
    @Test
    void subscriptionsSeeEachTransitionOnceAcrossRumorAndAntiEntropy() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        List<SpaceEvent<TaskEntry>> onB = new CopyOnWriteArrayList<>();
        b.space().notify(Template.of(TaskEntry.class), onB::add, MINUTES_30);

        a.space().write(new TaskEntry("observed", 1), MINUTES_30);
        tickAll(8); // several anti-entropy rounds re-offer nothing new
        assertThat(onB).extracting(SpaceEvent::kind).containsExactly(SpaceEvent.Kind.WRITTEN);

        TakenEntry<TaskEntry> taken = a.space().take(Template.of(TaskEntry.class),
                MINUTES_10, Duration.ZERO).orElseThrow();
        a.space().complete(taken);
        tickAll(8);

        assertThat(onB).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.COMPLETED);
        assertThat(onB).extracting(SpaceEvent::entry)
                .containsOnly(new TaskEntry("observed", 1));
    }

    /** Spec §7.2 notify: a replicated subscriber sees REAPPEARED when a take lease lapses uncompleted and EXPIRED when the write lease lapses, each once. */
    @Test
    void subscriptionsSeeExpiryAndReappearance() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        List<SpaceEvent<TaskEntry>> onB = new CopyOnWriteArrayList<>();
        b.space().notify(Template.of(TaskEntry.class), onB::add, MINUTES_30);

        a.space().write(new TaskEntry("watched", 1), Lease.of(Duration.ofMinutes(5)));
        TakenEntry<TaskEntry> taken = a.space().take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        tickAll(2);
        assertThat(onB).extracting(SpaceEvent::kind).containsExactly(SpaceEvent.Kind.WRITTEN);

        // The taker never completes; its claim lapses and the entry reappears.
        clock.advance(Duration.ofMinutes(2));
        tickAll(4); // the anti-entropy tick runs the lease sweep on every replica
        assertThat(onB).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.REAPPEARED);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThatThrownBy(() -> a.space().complete(taken))
                .isInstanceOf(LeaseExpiredException.class);

        // The write lease lapses; the entry vanishes and EXPIRED fires once.
        clock.advance(Duration.ofMinutes(5));
        tickAll(8);
        assertThat(onB).extracting(SpaceEvent::kind).containsExactly(
                SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.REAPPEARED, SpaceEvent.Kind.EXPIRED);
        assertThat(onB).extracting(SpaceEvent::entry).containsOnly(new TaskEntry("watched", 1));
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();

        // An explicit sweep without traffic adds nothing further.
        b.space().sweepNow();
        assertThat(onB).hasSize(3);
    }

    /** Spec §7.2 notify: a closed or lapsed subscription receives nothing further. */
    @Test
    void closedAndLapsedSubscriptionsStopReceiving() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        List<SpaceEvent<TaskEntry>> closedSink = new CopyOnWriteArrayList<>();
        List<SpaceEvent<TaskEntry>> lapsedSink = new CopyOnWriteArrayList<>();
        var closed = b.space().notify(Template.of(TaskEntry.class), closedSink::add, MINUTES_30);
        var lapsed = b.space().notify(Template.of(TaskEntry.class), lapsedSink::add,
                Lease.of(Duration.ofMinutes(1)));

        closed.close();
        clock.advance(Duration.ofMinutes(2));
        assertThatThrownBy(() -> lapsed.renew(Duration.ofMinutes(5)))
                .isInstanceOf(LeaseExpiredException.class);
        a.space().write(new TaskEntry("unseen", 1), MINUTES_30);
        tickAll(4);

        assertThat(closedSink).isEmpty();
        assertThat(lapsedSink).isEmpty();
    }
}
