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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GroupRuntime#onDueWall} as the one converter from a registrant's wall clock to the loop's monotonic base
 * (srad/agentspaces-event-loop.md EL-T-09, §3.6), {@link GroupRuntime#directRun()}, and the hint-change wakes of
 * EL-T-07: an AppCard or horizon hint that moves asks the group's registrants once, with no backstop finding.
 */
@Timeout(120)
class GroupRuntimeDueWallTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zDueWall");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zDueWall",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "due-wall",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private GroupRuntime runtime(String name, String seed, boolean start, int unused) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        if (start) {
            node.startTicking(TICK);
        }
        return runtime;
    }

    @Test
    void aWallDeadlineOnASkewedClockRunsAtItsOwnClocksInstant() throws Exception {
        GroupRuntime runtime = runtime("a", null, true, 0);
        java.time.InstantSource skewed = time.offset(Duration.ofHours(3)); // the registrant's clock, 3 h ahead
        AtomicLong due = new AtomicLong(skewed.millis() + 5_000);
        List<Long> ranAt = new CopyOnWriteArrayList<>();
        runtime.onDueWall("probe", skewed, now -> {
            if (now >= due.get()) {
                ranAt.add(time.monoNanos());
                due.set(DueWork.NEVER);
            }
            return due.get();
        });
        long start = time.monoNanos();
        time.advance(Duration.ofSeconds(7));
        assertThat(ranAt).as("ran once, at 5 s of its own clock").hasSize(1);
        long at = (ranAt.get(0) - start) / 1_000_000L;
        assertThat(at).as("converted against both clocks read together, not against the loop's wall").isEqualTo(5_000L);
    }

    @Test
    void aFarDeadlineIsCappedAndNeverIsNever() throws Exception {
        GroupRuntime runtime = runtime("a", null, false, 0);
        long now = time.millis();
        long nowNanos = time.monoNanos();
        assertThat(runtime.dueAt(time, DueWork.NEVER)).isEqualTo(DueWork.NEVER);
        assertThat(runtime.dueAt(time, now + Duration.ofDays(30).toMillis()) - nowNanos)
                .as("capped at an hour ahead").isEqualTo(Duration.ofHours(1).toNanos());
        assertThat(runtime.dueAt(time, now - 10_000)).as("a past deadline is due now").isEqualTo(nowNanos);
    }

    @Test
    void directRunIsTrueOnlyUnderTick() throws Exception {
        GroupRuntime runtime = runtime("a", null, false, 0);
        List<Boolean> seen = new CopyOnWriteArrayList<>();
        runtime.onDue("direct-probe", now -> {
            seen.add(runtime.directRun());
            return DueWork.NEVER;
        });
        assertThat(runtime.directRun()).as("outside any run").isFalse();
        assertThat(runtime.loopRunning()).isFalse();
        nodes.get(0).tick();
        assertThat(seen).contains(true);
        seen.clear();
        nodes.get(0).startTicking(TICK);
        assertThat(runtime.loopRunning()).isTrue();
        time.advance(Duration.ofSeconds(3)); // the registration run and the backstop sweeps
        assertThat(seen).isNotEmpty().containsOnly(false);
    }

    @Test
    void anAppCardHintThatMovesAsksTheGroupsRegistrants() throws Exception {
        GroupRuntime a = runtime("a", null, true, 0);
        GroupRuntime b = runtime("b", "a", true, 0);
        long waited = 0;
        while (a.membership().allMembers().isEmpty() || b.membership().allMembers().isEmpty()) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).isLessThan(60_000);
        }
        time.advance(Duration.ofMinutes(2));
        AtomicLong seenVersion = new AtomicLong(a.appCardVersion());
        AtomicInteger changes = new AtomicInteger();
        AtomicBoolean asked = new AtomicBoolean();
        a.onDue("appcard-watch", now -> {
            long v = a.appCardVersion();
            if (v != seenVersion.getAndSet(v)) {
                changes.incrementAndGet();
                asked.set(true);
            }
            return DueWork.NEVER;
        });
        time.advance(Duration.ofSeconds(2));
        long before = a.appCardVersion();
        // b's card drops to AppCard level 1: a hears the card by gossip, and the version moves with no view change
        long view = a.membership().viewVersion();
        nodes.get(1).credentialHint(PeerNode.APPCARD_HINT, "1");
        waited = 0;
        while (!asked.get()) {
            time.advance(Duration.ofMillis(50));
            waited += 50;
            assertThat(waited).as("the moved hint reaches a's registrants").isLessThan(30_000);
        }
        assertThat(a.appCardVersion()).isNotEqualTo(before);
        assertThat(a.membership().viewVersion()).as("no view change carried it").isEqualTo(view);
        assertThat(changes.get()).isEqualTo(1);
        assertThat(nodes.get(0).loopStats().backstopFoundWork()).isZero();
    }
}
