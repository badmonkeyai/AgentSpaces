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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console as one more read-only peer on a SimNetwork fleet: everything it
 * reports arrives by replication, and watching changes nothing about how the
 * fleet coordinates.
 */
class ReplicatedConsoleTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zConsole");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zConsole", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "console-fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity,
                address).clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Peer(node, runtime, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    @Test
    void consoleObservesRemoteWorkWithoutParticipating() throws Exception {
        Peer coordinator = newPeer("coordinator", 1);
        Peer worker = newPeer("worker", 2, "coordinator");
        Peer console = newPeer("console", 3, "coordinator");
        tickAll(4);

        try (ConsoleView view = ConsoleView.builder()
                .space("tasks", console.space(), TaskEntry.class)
                .results("tasks", FindingEntry.class, entry -> {
                    FindingEntry finding = (FindingEntry) entry;
                    return finding.summary().startsWith("by ")
                            ? finding.summary().substring(3) : "";
                })
                .members(() -> console.runtime().membership().allMembers())
                .clock(clock)
                .build()) {

            coordinator.space().write(new TaskEntry("replicated job", 5), MINUTES_30);
            tickAll(4);
            assertThat(view.spaces().get(0).written()).isEqualTo(1);
            assertThat(view.spaces().get(0).queued()).isEqualTo(1);

            Optional<TakenEntry<TaskEntry>> taken = worker.space().take(
                    Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)),
                    Duration.ZERO);
            assertThat(taken).isPresent();
            worker.space().complete(taken.get(),
                    new FindingEntry("replicated job", "by worker"), MINUTES_30);
            tickAll(6);

            ConsoleView.SpaceStats stats = view.spaces().get(0);
            assertThat(stats.completed()).isEqualTo(1);
            assertThat(stats.queued()).isZero();
            assertThat(stats.inProgress()).isZero();
            assertThat(view.perWorker()).containsEntry("worker", 1);

            assertThat(view.members()).hasSizeGreaterThanOrEqualTo(2);
            assertThat(view.eventsAfter(0))
                    .extracting(ConsoleEvent::kind)
                    .contains("written", "completed");

            // The console never took anything: the fleet's ledger is untouched.
            assertThat(console.space().readAll(Template.of(FindingEntry.class), 100))
                    .hasSize(1);
        }
    }

    /**
     * RBE regression matrix probe P14 (srad/replication-by-entry.md phase 1, RE-M-08): a console that joins after the
     * fleet worked learns the space in one {@code PULL_RESP} and counts from that batch's events. Work completed before
     * it joined arrives completed, so the console hears COMPLETED for it and never WRITTEN; it must still count that
     * work as written, or {@code written - completed - queued} comes out short and the work in progress reads zero.
     * After the batch, live events count on as before.
     */
    @Test
    void aLateConsoleCountsTheWorkABatchBrings() throws Exception {
        Peer coordinator = newPeer("coordinator", 1);
        Peer worker = newPeer("worker", 2, "coordinator");
        tickAll(4);
        for (int i = 1; i <= 5; i++) {
            coordinator.space().write(new TaskEntry("job-" + i, i), MINUTES_30);
        }
        tickAll(4);
        Lease takeLease = Lease.of(Duration.ofMinutes(5));
        for (int i = 0; i < 2; i++) {
            TakenEntry<TaskEntry> taken = worker.space().take(Template.of(TaskEntry.class), takeLease, Duration.ZERO)
                    .orElseThrow();
            worker.space().complete(taken, new FindingEntry(taken.entry().topic(), "by worker"), MINUTES_30);
        }
        TakenEntry<TaskEntry> held = worker.space().take(Template.of(TaskEntry.class), takeLease, Duration.ZERO)
                .orElseThrow();
        tickAll(4);
        int total = coordinator.space().knownEntries();
        assertThat(worker.space().knownEntries()).isEqualTo(total);

        // Joins after the work: no lazy-push announcement covers it, so one anti-entropy PULL_RESP brings it all.
        Peer console = newPeer("console", 3, "coordinator");
        List<Integer> knownAtEvent = new java.util.concurrent.CopyOnWriteArrayList<>();
        console.space().notify(Template.of(TaskEntry.class),
                event -> knownAtEvent.add(console.space().knownEntries()), MINUTES_30);
        try (ConsoleView view = ConsoleView.builder()
                .space("tasks", console.space(), TaskEntry.class)
                .results("tasks", FindingEntry.class, entry -> {
                    FindingEntry finding = (FindingEntry) entry;
                    return finding.summary().startsWith("by ") ? finding.summary().substring(3) : "";
                })
                .clock(clock)
                .build()) {
            assertThat(console.space().knownEntries()).as("watching before anything arrived").isZero();
            for (int i = 0; i < 20 && console.space().knownEntries() < total; i++) {
                tickAll(1);
            }
            assertThat(console.space().knownEntries()).isEqualTo(total);
            assertThat(knownAtEvent).as("one batch: every event fired after the whole space was in")
                    .isNotEmpty().allSatisfy(known -> assertThat(known).isEqualTo(total));

            ConsoleView.SpaceStats stats = view.spaces().get(0);
            assertThat(stats.written()).as("every task the fleet wrote, the completed ones included").isEqualTo(5);
            assertThat(stats.completed()).isEqualTo(2);
            assertThat(stats.queued()).isEqualTo(2);
            assertThat(stats.inProgress()).as("the one task a take lease holds").isEqualTo(1);
            assertThat(view.perWorker()).containsEntry("worker", 2);

            // After the batch the live events count on.
            worker.space().complete(held, new FindingEntry(held.entry().topic(), "by worker"), MINUTES_30);
            coordinator.space().write(new TaskEntry("job-6", 6), MINUTES_30);
            tickAll(6);
            stats = view.spaces().get(0);
            assertThat(stats.written()).isEqualTo(6);
            assertThat(stats.completed()).isEqualTo(3);
            assertThat(stats.queued()).isEqualTo(3);
            assertThat(stats.inProgress()).isZero();
            assertThat(view.perWorker()).containsEntry("worker", 3);
        }
    }
}
