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
package ai.badmonkey.agentspaces.agent.remote;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The withdrawal of a timed-out input (reviews/embabel-agentspaces-review-2026-11-12.md §7.5, §10.2 deviation 1),
 * pinned: an input nobody took is withdrawn, an input a worker holds is left to the worker, and the one race the
 * caller-side check cannot close, a claim still in flight to the caller's replica, robs the worker exactly as the
 * core's issuer-cancel rule says (the worker's complete fails; the work is not done twice).
 */
class RemoteActionWithdrawTest {

    private static final Lease TEN_MINUTES = Lease.of(Duration.ofMinutes(10));

    private final List<AutoCloseable> closing = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : closing) {
            c.close();
        }
    }

    private static RemoteAction action(Space tasks) {
        PeerIdentity foreign = PeerIdentity.generate();
        GroupId group = GroupId.of("zWithdraw");
        AgentCard card = new AgentCard("aspace://" + group.value() + "/agent/worker", foreign.peerId(), group,
                Instant.now(), Duration.ofMinutes(15), foreign.agent("worker"), "works", List.of(),
                List.of(TaskEntry.class.getName() + "#v1"), List.of(FindingEntry.class.getName() + "#v1"), Map.of());
        return new RemoteAction(card, TaskEntry.class, FindingEntry.class, tasks, tasks,
                Correlation.sharedFields(), TEN_MINUTES);
    }

    // ------------------------------------------------------------------ LocalSpace

    @Test
    @Timeout(30)
    void anInputNobodyTookIsWithdrawnUnderWithdrawAndLeftUnderLeave() throws Exception {
        LocalSpace tasks = LocalSpace.builder("tasks", new AgentId(PeerIdentity.generate().peerId(), "caller")).build();
        closing.add(tasks);
        RemoteAction action = action(tasks);

        assertThat(action.invoke(new TaskEntry("withdrawn", 1), Duration.ofMillis(300),
                RemoteAction.OnTimeout.WITHDRAW)).isEmpty();
        assertThat(action.invoke(new TaskEntry("left", 1), Duration.ofMillis(300),
                RemoteAction.OnTimeout.LEAVE)).isEmpty();

        assertThat(tasks.readAll(Template.of(TaskEntry.class), 10)).extracting(TaskEntry::topic)
                .containsExactly("left");
    }

    @Test
    @Timeout(30)
    void anInputAWorkerHoldsIsNotWithdrawnAndTheWorkerCompletes() throws Exception {
        LocalSpace tasks = LocalSpace.builder("tasks", new AgentId(PeerIdentity.generate().peerId(), "caller")).build();
        closing.add(tasks);
        RemoteAction action = action(tasks);

        // The worker takes the input as soon as it is written and holds it past the caller's timeout.
        CompletableFuture<Optional<TakenEntry<TaskEntry>>> worker = CompletableFuture.supplyAsync(() ->
                tasks.take(Template.of(TaskEntry.class), TEN_MINUTES, Duration.ofSeconds(5)));
        assertThat(action.invoke(new TaskEntry("held", 1), Duration.ofMillis(500),
                RemoteAction.OnTimeout.WITHDRAW)).isEmpty();
        TakenEntry<TaskEntry> taken = worker.get(5, TimeUnit.SECONDS).orElseThrow();

        tasks.complete(taken, new FindingEntry("held", "late but done"), TEN_MINUTES);
        assertThat(tasks.readAll(Template.of(FindingEntry.class), 10)).extracting(FindingEntry::summary)
                .containsExactly("late but done");
    }

    // ------------------------------------------------------------------ ReplicatedSpace, two peers

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zWithdrawCluster");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zWithdrawCluster",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "withdraw",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private ReplicatedSpace peer(String address, long seed, String agent, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, agent)
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        closing.add(space);
        closing.add(node);
        return space;
    }

    private synchronized void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private void hold(boolean held) {
        for (String from : List.of("a", "b")) {
            for (String to : List.of("a", "b")) {
                if (!from.equals(to)) {
                    if (held) {
                        network.hold(from, to);
                    } else {
                        network.release(from, to, SimNetwork.Order.FIFO, 7);
                    }
                }
            }
        }
    }

    /** Starts the caller's invocation on A and waits until B holds a replica of the input. */
    private CompletableFuture<Optional<Object>> invokeAndReplicate(ReplicatedSpace a, ReplicatedSpace b, String topic,
                                                                   Duration timeout) throws Exception {
        RemoteAction action = action(a);
        CompletableFuture<Optional<Object>> calling = CompletableFuture.supplyAsync(() ->
                action.invoke(new TaskEntry(topic, 1), timeout, RemoteAction.OnTimeout.WITHDRAW));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (b.readAll(Template.of(TaskEntry.class), 10).isEmpty() && System.nanoTime() < deadline) {
            tickAll(1);
            Thread.sleep(20);
        }
        assertThat(b.readAll(Template.of(TaskEntry.class), 10)).as("the input reached B").hasSize(1);
        return calling;
    }

    @Test
    @Timeout(60)
    void aClaimTheCallerAlreadyKnowsIsNotWithdrawnAndTheWorkerCompletes() throws Exception {
        ReplicatedSpace a = peer("a", 1, "caller");
        ReplicatedSpace b = peer("b", 2, "worker", "a");
        tickAll(6);

        CompletableFuture<Optional<Object>> calling = invokeAndReplicate(a, b, "known", Duration.ofSeconds(3));
        TakenEntry<TaskEntry> taken = b.take(Template.of(TaskEntry.class), TEN_MINUTES, Duration.ZERO).orElseThrow();
        tickAll(3); // B's claim reaches A before the caller gives up
        assertThat(a.readAll(Template.of(TaskEntry.class), 10)).as("A no longer offers the claimed input").isEmpty();

        assertThat(calling.get(10, TimeUnit.SECONDS)).isEmpty();
        tickAll(3);
        b.complete(taken, new FindingEntry("known", "done after the caller left"), TEN_MINUTES);
        tickAll(3);
        assertThat(a.readAll(Template.of(FindingEntry.class), 10)).extracting(FindingEntry::topic)
                .containsExactly("known");
    }

    @Test
    @Timeout(60)
    void aClaimStillInFlightToTheCallerIsRobbedAsTheCoreCancelRuleSays() throws Exception {
        ReplicatedSpace a = peer("a", 1, "caller");
        ReplicatedSpace b = peer("b", 2, "worker", "a");
        tickAll(6);

        CompletableFuture<Optional<Object>> calling = invokeAndReplicate(a, b, "raced", Duration.ofSeconds(2));
        hold(true); // B's claim does not reach A before the caller's timeout
        TakenEntry<TaskEntry> taken = b.take(Template.of(TaskEntry.class), TEN_MINUTES, Duration.ZERO).orElseThrow();
        assertThat(a.readAll(Template.of(TaskEntry.class), 10)).as("A still offers the input").hasSize(1);

        assertThat(calling.get(10, TimeUnit.SECONDS)).isEmpty(); // A withdraws what it believes is unclaimed
        hold(false);
        tickAll(3);

        assertThatThrownBy(() -> b.complete(taken, new FindingEntry("raced", "too late"), TEN_MINUTES))
                .isInstanceOf(LeaseExpiredException.class);
        tickAll(3);
        assertThat(a.readAll(Template.of(FindingEntry.class), 10)).as("no result is written").isEmpty();
        assertThat(b.readAll(Template.of(TaskEntry.class), 10)).as("the input is gone everywhere").isEmpty();
    }
}
