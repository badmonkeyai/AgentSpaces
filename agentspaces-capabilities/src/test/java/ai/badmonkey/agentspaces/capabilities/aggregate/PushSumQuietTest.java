/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.capabilities.aggregate;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * A settled epoch goes silent and is eventually forgotten (the superfriends CPU audit of
 * 2026-10-11: usage sinks open an epoch per time window, and every epoch cost every participant
 * one signed frame per tick forever, so an hour-old fleet spent most of a core on them).
 */
class PushSumQuietTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zQuiet");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zQuiet", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "quiet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Member member(String address, long seed, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        nodes.add(node);
        return new Member(identity, node, runtime);
    }

    private PushSumAggregate aggregate(Member m) {
        return new PushSumAggregate(new CapabilityPipes(m.runtime(), codec), m.runtime().sampler(),
                m.identity().peerId(), codec, clock).quiet(10, 1e-3, 40);
    }

    private void tick(List<PushSumAggregate> aggregates, int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            aggregates.forEach(PushSumAggregate::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    @Test
    void aSettledEpochGoesSilentWakesOnChangeAndIsForgottenAfterTheRetention() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        for (int i = 0; i < 6; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
        List<PushSumAggregate> aggregates = List.of(aggregate(a), aggregate(b), aggregate(c));
        aggregates.get(0).start("load", 1.0);
        aggregates.get(1).start("load", 2.0);
        aggregates.get(2).start("load", 3.0);

        // Converges, then settles: every participant stops gossiping the epoch.
        tick(aggregates, 30);
        for (PushSumAggregate aggregate : aggregates) {
            assertThat(aggregate.estimate("load")).isPresent();
            assertThat(aggregate.estimate("load").getAsDouble()).isCloseTo(2.0, within(0.05));
            assertThat(aggregate.quiet("load")).as("settled epoch is silent").isTrue();
        }

        // A new contribution moves the estimate: the epoch wakes, converges, settles again.
        aggregates.get(0).start("load", 7.0);
        tick(aggregates, 3);
        assertThat(aggregates.get(0).quiet("load")).as("a changed estimate wakes the epoch").isFalse();
        tick(aggregates, 30);
        for (PushSumAggregate aggregate : aggregates) {
            assertThat(aggregate.estimate("load").getAsDouble()).isGreaterThan(2.5);
            assertThat(aggregate.quiet("load")).isTrue();
        }

        // Past the retention the state is forgotten: no estimate, nothing to gossip.
        tick(aggregates, 45);
        for (PushSumAggregate aggregate : aggregates) {
            assertThat(aggregate.estimate("load")).as("evicted after the retention").isEmpty();
            assertThat(aggregate.quiet("load")).isFalse();
        }
    }
}
