/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An idle group's scheduled tick runs one time in {@link PeerNode#IDLE_TICK_DIVIDER} (idle-loop O8), an idle node's
 * ticker sleeps the whole span (idle-loop item 1), and the first sign of work brings it back to every tick. On the wall clock, with a 10 ms ticker so the test is short.
 */
class IdleTickTest {

    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zIdleTick");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zIdleTick",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "idle-tick", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private static boolean within(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    @Test
    void anIdleGroupSkipsMostScheduledTicksAndWorkBringsItBack() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        List<String> heard = new java.util.concurrent.CopyOnWriteArrayList<>();
        rb.gossip().onStream("s", (from, id, payload) -> heard.add(id));
        ra.gossip().onStream("s", (from, id, payload) -> { });
        a.startTicking(Duration.ofMillis(10));
        b.startTicking(Duration.ofMillis(10));
        assertThat(within(Duration.ofSeconds(5), () -> ra.membership().allMembers().size() == 1
                && rb.membership().allMembers().size() == 1)).as("the pair sees each other").isTrue();

        assertThat(within(Duration.ofSeconds(5), () -> a.idleSleeps() > 10))
                .as("an idle node's ticker sleeps whole spans").isTrue();

        // work: a rumor published at A, held by both
        ra.gossip().publish("s", "item-1", "x".getBytes(StandardCharsets.UTF_8));
        long sleepsAtWork = a.idleSleeps();
        Thread.sleep(150); // 15 ticks, inside the quiet run before the group counts as idle again
        assertThat(a.idleSleeps() - sleepsAtWork).as("no idle sleep right after work").isLessThanOrEqualTo(1);
        assertThat(within(Duration.ofSeconds(5), () -> heard.contains("item-1"))).isTrue();

        // a working frame or a wake cuts a long sleep short: the next tick runs at once, not a span later
        assertThat(within(Duration.ofSeconds(5), () -> a.idleSleeps() > sleepsAtWork + 10)).isTrue();
        long skippedBefore = a.idleTicksSkipped();
        a.wake(groupId);
        Thread.sleep(150);
        assertThat(a.idleTicksSkipped() - skippedBefore).as("after a wake the ticks are full ones")
                .isLessThanOrEqualTo(PeerNode.IDLE_TICK_DIVIDER);

        // idle again, and the view holds through it
        long sleepsAfterWake = a.idleSleeps();
        assertThat(within(Duration.ofSeconds(5), () -> a.idleSleeps() > sleepsAfterWake + 10)).isTrue();
        assertThat(ra.membership().allMembers()).hasSize(1);
        assertThat(rb.membership().allMembers()).hasSize(1);
    }
}
