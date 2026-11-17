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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The space side of reviews/IDLE-WORK-2026-11-13.md: a writer-local space costs the group nothing, a waiting take
 * sleeps on a change signal instead of polling every 20 ms, a delta that changes nothing is reported as such and
 * keeps the digest cache, and a withdrawn entry keeps no decoded value.
 */
class IdleWorkSpaceTest {

    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zIdle");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zIdle", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "idle",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerIdentity identity, PeerNode node, GroupRuntime runtime) {
        ReplicatedSpace.Builder space(String name, TestClock clock) {
            return ReplicatedSpace.builder(runtime, name, identity, "worker").clock(clock).settleWindow(Duration.ZERO);
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer peer(String address, long seed, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        nodes.add(node);
        return new Peer(identity, node, runtime);
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /**
     * A writer-local space (IDLE-WORK item 15, 5a(f)) registers nothing on the bus and publishes nothing, so a member
     * that opened the same name replicated never sees its entries; its leases still lapse, from the group tick alone.
     */
    @Test
    void aWriterLocalSpaceCostsTheGroupNothingAndStillKeepsItsLeases() throws IOException {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        ReplicatedSpace scratch = a.space("positions", clock).local().build();
        ReplicatedSpace replicated = b.space("positions", clock).build();
        assertThat(a.runtime().gossip().streams()).doesNotContain("space:positions");
        assertThat(b.runtime().gossip().streams()).contains("space:positions");

        List<SpaceEvent<TaskEntry>> events = new CopyOnWriteArrayList<>();
        scratch.notify(Template.of(TaskEntry.class), events::add, Lease.of(Duration.ofHours(1)));
        scratch.write(new TaskEntry("truck-1", 1), Lease.of(Duration.ofSeconds(2)));
        scratch.write(new TaskEntry("truck-2", 2), MINUTES_10);
        assertThat(scratch.read(Template.of(TaskEntry.class))).isPresent();
        assertThat(a.runtime().gossip().rumorsSentByStream()).doesNotContainKey("space:positions");
        tick(4); // anti-entropy runs and the lease of truck-1 lapses
        assertThat(replicated.knownEntries()).as("the replicated namesake never hears of them").isZero();
        assertThat(events).extracting(SpaceEvent::kind)
                .as("the lapse fired from the tick, with no digest asked for")
                .contains(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.EXPIRED);
        assertThat(scratch.readAll(Template.of(TaskEntry.class), 10)).extracting(TaskEntry::topic)
                .containsExactly("truck-2");
        Optional<TakenEntry<TaskEntry>> taken = scratch.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        scratch.complete(taken.get());
        assertThat(a.runtime().gossip().rumorsSentByStream()).doesNotContainKey("space:positions");
        scratch.close();
    }

    /**
     * A writer-local space advertises nothing, even with a publisher set: every space advertisement says the space
     * replicates, so advertising it would send members to a replica that never answers. A {@code FRESH} read on it
     * starts no anti-entropy round, since no member holds anything for it.
     */
    @Test
    void aWriterLocalSpaceAdvertisesNothingAndAFreshReadStartsNoRound() throws IOException {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        List<Object> published = new CopyOnWriteArrayList<>();
        ReplicatedSpace scratch = a.space("positions", clock).local().advertise(published::add).build();
        assertThat(published).as("no advertisement on build").isEmpty();
        assertThat(scratch.refreshAdvertisement()).isEmpty();
        assertThat(scratch.advertisement()).isEmpty();
        assertThat(published).as("none on refresh either").isEmpty();

        // A replicated space on the same node, so the bus holds a stream to reconcile and a round would start.
        ReplicatedSpace shared = a.space("shared", clock).advertise(published::add).build();
        assertThat(published).as("the replicated space does advertise").hasSize(1);

        scratch.write(new TaskEntry("truck-1", 1), MINUTES_10);
        long rounds = a.runtime().gossip().antiEntropyRounds();
        assertThat(scratch.read(Template.of(TaskEntry.class), ai.badmonkey.agentspaces.space.ConsistencyHint.FRESH,
                Duration.ZERO)).isPresent();
        assertThat(a.runtime().gossip().antiEntropyRounds()).as("a FRESH read on a local space pulls nothing")
                .isEqualTo(rounds);
        shared.read(Template.of(TaskEntry.class), ai.badmonkey.agentspaces.space.ConsistencyHint.FRESH, Duration.ZERO);
        assertThat(a.runtime().gossip().antiEntropyRounds()).as("the same read on the replicated space does")
                .isGreaterThan(rounds);
        shared.close();
        scratch.close();
    }

    /**
     * A take with a timeout sleeps on the space's change signal (IDLE-WORK 5a(a)): an idle wait of a second and a
     * half tries a handful of times, not the seventy-five a 20 ms poll took, and a write wakes it at once.
     */
    @Test
    void aWaitingTakeSleepsUntilAChangeInsteadOfPolling() throws Exception {
        Peer a = peer("a", 1);
        ReplicatedSpace space = a.space("tasks", clock).build();

        long polls = space.waitingPolls();
        long started = System.nanoTime();
        Optional<TakenEntry<TaskEntry>> none = space.take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ofMillis(1_500));
        long waited = (System.nanoTime() - started) / 1_000_000;
        assertThat(none).isEmpty();
        assertThat(waited).as("it waited out its timeout").isGreaterThanOrEqualTo(1_450);
        assertThat(space.waitingPolls() - polls).as("an idle wait: first try, a slice, the deadline").isLessThanOrEqualTo(4);

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> waiting = CompletableFuture.supplyAsync(() ->
                space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(20)));
        Thread.sleep(300);
        long before = space.waitingPolls();
        long wrote = System.nanoTime();
        space.write(new TaskEntry("job", 1), MINUTES_10);
        Optional<TakenEntry<TaskEntry>> taken = waiting.get(5, TimeUnit.SECONDS);
        long woke = (System.nanoTime() - wrote) / 1_000_000;
        assertThat(taken).isPresent();
        assertThat(woke).as("the write woke it, well before a slice ran out").isLessThan(900);
        assertThat(space.waitingPolls() - before).isLessThanOrEqualTo(2);
    }

    /**
     * A delta whose every state the replica already holds changes nothing (IDLE-WORK items 6 and 8): it reports no
     * change, so the bus counts no activity, and the digest cache survives it.
     */
    @Test
    void aDeltaOfStatesAlreadyHeldChangesNothingAndKeepsTheDigestCache() throws IOException {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2);
        ReplicatedSpace writer = a.space("tasks", clock).build();
        ReplicatedSpace replica = b.space("tasks", clock).build();
        for (int i = 0; i < 3; i++) {
            writer.write(new TaskEntry("task-" + i, i), MINUTES_10);
        }
        byte[] delta = writer.deltaFor(new byte[0]);
        assertThat(replica.applySyncDelta(delta)).isTrue();
        assertThat(replica.knownEntries()).isEqualTo(3);
        byte[] digest = replica.digest();
        long rebuilds = replica.digestComputations();

        assertThat(replica.applySyncDelta(delta)).as("the same states again").isFalse();
        assertThat(replica.digest()).isSameAs(digest);
        assertThat(replica.digestComputations()).isEqualTo(rebuilds);
        assertThat(replica.applySyncDelta(new byte[] {1, 2, 3})).as("an undecodable delta").isFalse();
        assertThat(writer.digest()).isEqualTo(digest);
    }

    /**
     * IDLE-WORK 5a(e): an entry withdrawn by a merged cancel drops its decoded value at once, also when its EXPIRED
     * already fired at an earlier lapse and a later renewal brought it back; before, that value stayed until the
     * tombstone was collected, two maximum leases on.
     */
    @Test
    void aWithdrawnEntryKeepsNoDecodedValue() throws IOException {
        Peer a = peer("a", 1);
        Peer b = peer("b", 2, "a");
        tick(6);
        ReplicatedSpace writer = a.space("tasks", clock).build();
        ReplicatedSpace replica = b.space("tasks", clock).build();
        EntryHandle handle = writer.write(new TaskEntry("view", 1), Lease.of(Duration.ofSeconds(5)));
        tick(2);
        assertThat(replica.read(Template.of(TaskEntry.class))).isPresent();

        network.partition("a", "b");
        handle.renew(Duration.ofMinutes(10)); // the replica does not hear of it yet
        clock.advance(Duration.ofSeconds(5));
        replica.sweepNow();
        assertThat(replica.lastSweepActions().expired()).as("the lapse, as the replica sees it").hasSize(1);
        network.heal();
        tick(8); // the renewal arrives by anti-entropy
        assertThat(replica.read(Template.of(TaskEntry.class))).as("back, and decoded again").isPresent();
        assertThat(replica.cachedValues()).isEqualTo(1);

        handle.cancel();
        tick(2);
        assertThat(replica.read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(replica.cachedValues()).as("the withdrawn entry's value went with the cancel").isZero();
        assertThat(replica.knownEntries()).as("its tombstone stays").isEqualTo(1);
    }
}
