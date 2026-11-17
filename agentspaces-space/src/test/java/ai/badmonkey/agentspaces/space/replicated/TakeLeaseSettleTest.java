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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A take whose lease cannot outlast its settle window (SPEC §7.4, found 2026-11-15 by the TypeScript fix). The race
 * is judged right after the window and only a live claim wins, so such a take used to publish a claim that had lapsed
 * when it was judged, try again, and come back empty at its timeout as if it had lost every race. It is now refused at
 * once with an IllegalArgumentException that names both values, as the TypeScript take refuses with a RangeError.
 *
 * <p>The node runs on the system clock, as a deployment does: the dead end needs the clock to move during the window.
 */
class TakeLeaseSettleTest {

    private static final Duration SETTLE = Duration.ofMillis(200);

    private final SimNetwork network = new SimNetwork();
    private PeerNode node;
    private ReplicatedSpace space;

    @BeforeEach
    void setUp() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        GroupAdvertisement groupAd = new GroupAdvertisement(
                "aspace://zSettle", PeerIdentity.generate().peerId(), GroupId.of("zSettle"),
                java.time.Instant.EPOCH, Duration.ofDays(1), "settle",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        node = PeerNode.builder(identity).randomSeed(1).build();
        node.listen(network.register("solo"), "solo");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker").settleWindow(SETTLE).build();
    }

    @AfterEach
    void tearDown() {
        node.close();
    }

    @Test
    void aTakeLeaseShorterThanTheSettleWindowIsRefusedAtOnceNamingBothValues() {
        space.write(new TaskEntry("job", 1), Lease.of(Duration.ofMinutes(10)));
        long started = System.nanoTime();
        assertThatThrownBy(() -> space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMillis(150)),
                Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("150 ms")
                .hasMessageContaining("200 ms");
        long millis = (System.nanoTime() - started) / 1_000_000;
        System.out.println("TakeLeaseSettle refusal took " + millis + " ms");
        // Refused before any claim: the entry is free for a take with a lease that outlasts the window.
        assertThat(space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofSeconds(30)), Duration.ZERO))
                .as("the refused take published no claim").isPresent();
    }

    @Test
    void aTakeLeaseEqualToTheSettleWindowIsRefusedToo() {
        space.write(new TaskEntry("job", 1), Lease.of(Duration.ofMinutes(10)));
        assertThatThrownBy(() -> space.take(Template.of(TaskEntry.class), Lease.of(SETTLE), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("200 ms");
    }

    /**
     * A lease well above the window takes as before, in about one window. The ceiling is a liveness bound, 30 s, a
     * hundred and fifty times the 200 ms window a quiet solo take typically needs; the time is printed.
     */
    @Test
    void aTakeLeaseThatOutlastsTheWindowTakesInAboutOneWindow() {
        space.write(new TaskEntry("job", 1), Lease.of(Duration.ofMinutes(10)));
        long started = System.nanoTime();
        Optional<TakenEntry<TaskEntry>> taken = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofSeconds(2)), Duration.ofSeconds(30));
        long millis = (System.nanoTime() - started) / 1_000_000;
        System.out.println("TakeLeaseSettle take with a 2 s lease and a 200 ms window took " + millis + " ms");
        assertThat(taken).isPresent();
        space.complete(taken.get());
    }
}
