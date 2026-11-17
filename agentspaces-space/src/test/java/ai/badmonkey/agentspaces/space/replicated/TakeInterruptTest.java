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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
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
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A take and an interrupt, which is how a closing binding stops a take in flight. A waiting take ends at the
 * interrupt instead of waiting out its timeout, as on a local space, so it cannot claim an entry written after the
 * binding closed; and a take that has already published its claim still waits out the whole settle window before it
 * judges the race, because judging early is how a take wins next to a rival (SPEC §7.4). Either way the interrupt is
 * kept for the caller.
 */
class TakeInterruptTest {

    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private PeerNode node;
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zInterrupt", PeerIdentity.generate().peerId(), GroupId.of("zInterrupt"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "interrupt",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    private ReplicatedSpace space(Duration settleWindow) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        return ReplicatedSpace.builder(runtime, "jobs", identity, "worker").clock(clock)
                .settleWindow(settleWindow).build();
    }

    @Test
    @Timeout(120)
    void aWaitingTakeEndsAtTheInterruptAndKeepsIt() throws Exception {
        ReplicatedSpace space = space(Duration.ZERO);
        Duration timeout = Duration.ofSeconds(60);
        AtomicReference<Optional<TakenEntry<TaskEntry>>> taken = new AtomicReference<>();
        AtomicBoolean kept = new AtomicBoolean();
        Thread taker = Thread.ofPlatform().start(() -> {
            taken.set(space.take(Template.of(TaskEntry.class), MINUTES_10, timeout));
            kept.set(Thread.currentThread().isInterrupted());
        });
        Thread.sleep(200); // the take is waiting on the space's change signal by now
        long start = System.nanoTime();
        taker.interrupt();
        assertThat(taker.join(Duration.ofSeconds(90))).as("the take returned").isTrue();
        long millis = (System.nanoTime() - start) / 1_000_000;
        System.out.println("TAKE-INTERRUPT-MEASURE waiting take returned " + millis + " ms after the interrupt");

        assertThat(taken.get()).as("nothing taken").isEmpty();
        assertThat(kept.get()).as("the interrupt is kept for the caller").isTrue();
        assertThat(millis).as("ended at the interrupt, not at its 60 s timeout").isLessThan(timeout.toMillis() / 2);
        space.write(new TaskEntry("after", 1), MINUTES_10);
        assertThat(space.readAll(Template.of(TaskEntry.class), 10)).as("an entry written afterwards stays free")
                .containsExactly(new TaskEntry("after", 1));
    }

    @Test
    @Timeout(120)
    void anInterruptedTakeStillWaitsOutTheSettleWindowBeforeItJudges() throws Exception {
        Duration settle = Duration.ofMillis(300);
        ReplicatedSpace space = space(settle);
        space.write(new TaskEntry("job", 1), MINUTES_10);
        AtomicReference<Optional<TakenEntry<TaskEntry>>> taken = new AtomicReference<>();
        AtomicBoolean kept = new AtomicBoolean();
        AtomicLong millis = new AtomicLong();
        Thread taker = Thread.ofPlatform().start(() -> {
            Thread.currentThread().interrupt();
            long start = System.nanoTime();
            taken.set(space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(30)));
            millis.set((System.nanoTime() - start) / 1_000_000);
            kept.set(Thread.currentThread().isInterrupted());
        });
        assertThat(taker.join(Duration.ofSeconds(90))).as("the take returned").isTrue();
        System.out.println("TAKE-INTERRUPT-MEASURE interrupted take judged after " + millis.get() + " ms (settle "
                + settle.toMillis() + " ms)");

        assertThat(taken.get()).as("the only claimant wins once the window has passed").isPresent();
        assertThat(millis.get()).as("the whole settle window passed before the race was judged")
                .isGreaterThanOrEqualTo(settle.toMillis());
        assertThat(kept.get()).as("the interrupt is kept for the caller").isTrue();
    }
}
