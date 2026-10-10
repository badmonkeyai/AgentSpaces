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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * {@link PushSumAggregate.Settle} and {@link PushSumAggregate#onEstimate}
 * on their own (ISSUE-OnEstimate; the only coverage was one positive
 * {@code @OnEstimate} cluster test): the rule's arithmetic, its validation,
 * {@code FIRST} firing on the first estimate present, a watch firing once
 * after the ticks it asks for, a watch registered after a settle firing
 * again, and a removed watch staying silent.
 */
class PushSumSettleTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zSettle");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zSettle", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "settle",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PushSumAggregate peer(String address, long seed, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        PushSumAggregate aggregate = new PushSumAggregate(new CapabilityPipes(runtime, codec),
                runtime.sampler(), identity.peerId(), codec, clock);
        nodes.add(node);
        return aggregate;
    }

    private void tick(List<PushSumAggregate> aggregates, int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            aggregates.forEach(PushSumAggregate::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void theRuleIsArithmeticOverTheLastValueAndRefusesNegatives() {
        PushSumAggregate.Settle rule = PushSumAggregate.Settle.after(3, 0.01);
        assertThat(rule.unchanged(100.0, 100.5)).as("within one percent of the last value").isTrue();
        assertThat(rule.unchanged(100.0, 102.0)).isFalse();
        assertThat(rule.unchanged(0.005, 0.0)).as("tolerance is relative to at least one").isTrue();
        assertThat(rule.unchanged(1.0, Double.NaN)).as("no last value is not unchanged").isFalse();
        assertThat(PushSumAggregate.Settle.FIRST.ticks()).isZero();
        assertThatThrownBy(() -> new PushSumAggregate.Settle(-1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PushSumAggregate.Settle.after(1, -0.5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void firstFiresOnTheFirstEstimateAndAfterFiresOnceWhenStable() throws Exception {
        PushSumAggregate a = peer("a", 1);
        PushSumAggregate b = peer("b", 2, "a");
        List<PushSumAggregate> all = List.of(a, b);
        tick(all, 3);
        List<PushSumAggregate.Estimate> first = new CopyOnWriteArrayList<>();
        List<PushSumAggregate.Estimate> settled = new CopyOnWriteArrayList<>();
        a.onEstimate("load"::equals, PushSumAggregate.Settle.FIRST, first::add);
        a.onEstimate("load"::equals, PushSumAggregate.Settle.after(4, 0.001), settled::add);
        a.start("load", 10.0);
        b.start("load", 30.0);
        tick(all, 2);
        Thread.sleep(100);
        assertThat(first).as("FIRST fires on the first estimate present").hasSize(1);
        assertThat(settled).as("four stable ticks have not passed").isEmpty();
        tick(all, 30);
        Thread.sleep(100);
        assertThat(settled).hasSize(1);
        assertThat(settled.get(0).value()).isCloseTo(20.0, within(0.5));
        assertThat(settled.get(0).mode()).isEqualTo(PushSumAggregate.Mode.AVG);
        assertThat(settled.get(0).ticks()).isGreaterThanOrEqualTo(4);
        assertThat(first).as("once per epoch").hasSize(1);

        // A watch registered after the settle counts afresh and fires again; a removed one stays silent.
        List<PushSumAggregate.Estimate> late = new CopyOnWriteArrayList<>();
        AutoCloseable handle = a.onEstimate("load"::equals, PushSumAggregate.Settle.after(2, 0.001), late::add);
        List<PushSumAggregate.Estimate> removed = new CopyOnWriteArrayList<>();
        a.onEstimate("load"::equals, PushSumAggregate.Settle.after(2, 0.001), removed::add).close();
        tick(all, 6);
        Thread.sleep(100);
        assertThat(late).hasSize(1);
        assertThat(removed).isEmpty();
        handle.close();

        // Another epoch under the same predicate does not fire a watch scoped to "load".
        a.start("other", 1.0);
        b.start("other", 3.0);
        tick(all, 10);
        Thread.sleep(100);
        assertThat(late).hasSize(1);
    }
}
