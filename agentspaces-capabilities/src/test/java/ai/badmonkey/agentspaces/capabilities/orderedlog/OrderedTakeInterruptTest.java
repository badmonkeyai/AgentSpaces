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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An ordered take and an interrupt, which is how a closing reduce or join stops its take in flight. The take used to
 * keep the interrupt status but not stop: every later sleep threw at once, so it spun through its timeout, claiming
 * whatever it found, and could adopt an entry written after the binding closed. Now it makes no new claim after the
 * interrupt, waits only for the claim it already has out (so a claim the log grants is not left with nobody to adopt
 * it), and returns with the status kept.
 */
class OrderedTakeInterruptTest {

    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private PeerNode node;
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zOrderedInterrupt", PeerIdentity.generate().peerId(), GroupId.of("zOrderedInterrupt"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "ordered-interrupt",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    /** One member's ordered takes over a log of {@code voters}, this member first. */
    private OrderedTakes ordered(List<PeerIdentity> voters) throws IOException {
        PeerIdentity identity = voters.get(0);
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "commits", identity, "taker").clock(clock)
                .settleWindow(Duration.ZERO).build();
        List<PeerId> members = voters.stream().map(PeerIdentity::peerId).toList();
        return OrderedTakes.over(new CapabilityPipes(runtime, codec), codec, identity, "taker", members, clock, 7,
                space);
    }

    /** Two voters and only one running: no leader, so a claim is never decided and a take waits on the log. */
    private OrderedTakes leaderless() throws IOException {
        return ordered(List.of(PeerIdentity.generate(), PeerIdentity.generate()));
    }

    @Test
    @Timeout(120)
    void anInterruptEndsATakeAfterTheClaimItHasOutAndLeavesTheEntryFree() throws Exception {
        // The take's claim sits in the log's attempt window (3 s), the state a closing reducer's take is in while
        // the log is busy. It waits that window out, as a timeout does, and stops instead of claiming again.
        OrderedTakes takes = leaderless();
        takes.space().write(new TaskEntry("job", 1), MINUTES_10);
        Duration timeout = Duration.ofSeconds(60);
        AtomicReference<Optional<TakenEntry<TaskEntry>>> taken = new AtomicReference<>();
        AtomicBoolean kept = new AtomicBoolean();
        Thread taker = Thread.ofPlatform().start(() -> {
            taken.set(takes.take(Template.of(TaskEntry.class), MINUTES_10, timeout));
            kept.set(Thread.currentThread().isInterrupted());
        });
        Thread.sleep(300);
        long start = System.nanoTime();
        taker.interrupt();
        assertThat(taker.join(Duration.ofSeconds(90))).as("the take returned").isTrue();
        long millis = (System.nanoTime() - start) / 1_000_000;
        System.out.println("ORDERED-INTERRUPT-MEASURE take returned " + millis + " ms after the interrupt");

        assertThat(taken.get()).as("an undecided claim gives nothing").isEmpty();
        assertThat(kept.get()).as("the interrupt is kept for the caller").isTrue();
        assertThat(millis).as("ended after the claim it had out, not at its 60 s timeout")
                .isLessThan(timeout.toMillis() / 2);
        assertThat(takes.space().readAll(Template.of(TaskEntry.class), 10)).as("the entry is still free here")
                .containsExactly(new TaskEntry("job", 1));
    }

    @Test
    @Timeout(120)
    void anInterruptedThreadMakesNoClaimAndReturnsAtOnce() throws Exception {
        OrderedTakes takes = leaderless();
        takes.space().write(new TaskEntry("job", 1), MINUTES_10);
        AtomicReference<Optional<TakenEntry<TaskEntry>>> taken = new AtomicReference<>();
        AtomicBoolean kept = new AtomicBoolean();
        java.util.concurrent.atomic.AtomicLong millis = new java.util.concurrent.atomic.AtomicLong();
        Thread taker = Thread.ofPlatform().start(() -> {
            Thread.currentThread().interrupt();
            long start = System.nanoTime();
            taken.set(takes.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(60)));
            millis.set((System.nanoTime() - start) / 1_000_000);
            kept.set(Thread.currentThread().isInterrupted());
        });
        assertThat(taker.join(Duration.ofSeconds(90))).as("the take returned").isTrue();
        System.out.println("ORDERED-INTERRUPT-MEASURE interrupted take returned in " + millis.get() + " ms");

        assertThat(taken.get()).isEmpty();
        assertThat(kept.get()).as("the interrupt is kept for the caller").isTrue();
        // A claim would have meant waiting on the leaderless log for its 3 s attempt window.
        assertThat(millis.get()).as("returned without making a claim to wait on").isLessThan(3_000);
    }
}
