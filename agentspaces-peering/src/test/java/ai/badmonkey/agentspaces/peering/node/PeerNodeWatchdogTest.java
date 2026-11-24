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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Measure;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A node's escalation (srad/agentspaces-event-loop.md decision 4, WD-5, EL-H-08, EL-H-14): three re-initialisations of
 * its loop within 10 minutes turn {@link PeerNode#loopHealth()} DOWN and tell every listener; under
 * {@link PeerNode.Escalation#EXIT} the node then closes and the process exits with
 * {@link PeerNode#WATCHDOG_EXIT_STATUS}. The flush timer is watched too (EL-H-02). Faults are scripted lost nudges on
 * virtual time.
 */
@Timeout(120)
class PeerNodeWatchdogTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-21T00:00:00Z"));
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zWatchdogNode",
            PeerIdentity.generate().peerId(), GroupId.of("zWatchdogNode"), Instant.EPOCH, Duration.ofDays(1),
            "watchdog-node", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(PeerNode.Builder builder, String address) throws Exception {
        PeerNode node = builder.clock(time).time(time).coalesce(Duration.ofMillis(20)).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void loseThreeNudges(PeerNode node, DueWork.Registration reg) {
        for (int i = 0; i < 3; i++) {
            node.loop().dropNextDispatch();
            reg.wake();
            time.advance(Duration.ofSeconds(3));
        }
    }

    @Test
    void threeReinitialisationsInTenMinutesReportDownToEveryListener() throws Exception {
        time.watchdog().markScripted();
        PeerNode node = node(PeerNode.builder(PeerIdentity.generate()), "a");
        GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        List<NodeLoop.Health> healths = new CopyOnWriteArrayList<>();
        node.onLoopHealth(healths::add);
        AtomicInteger runs = new AtomicInteger();
        DueWork.Registration reg = rt.onDue("asked", now -> {
            runs.incrementAndGet();
            return DueWork.NEVER;
        });
        node.startTicking(Duration.ofMillis(250));
        time.advance(Duration.ofSeconds(1));
        assertThat(time.watchdog().nextTickNanos()).as("the loop and its flush timer are watched")
                .isNotEqualTo(Long.MAX_VALUE);
        assertThat(node.loopHealth().state()).isEqualTo(NodeLoop.HealthState.OK);
        loseThreeNudges(node, reg);
        assertThat(node.loopStats().generation()).isEqualTo(3);
        assertThat(node.loopHealth().state()).isEqualTo(NodeLoop.HealthState.DOWN);
        assertThat(node.loopHealth().reasons()).singleElement().asString().contains("3 re-initialisations");
        assertThat(healths).extracting(NodeLoop.Health::state).containsExactly(NodeLoop.HealthState.DOWN);
        int before = runs.get();
        reg.wake();
        time.advance(Duration.ofSeconds(1));
        assertThat(runs.get()).as("report does not stop the loop").isGreaterThan(before);
    }

    @Test
    void exitEscalationClosesTheNodeAndExitsWithTheWatchdogStatus() throws Exception {
        time.watchdog().markScripted();
        List<Integer> exits = new CopyOnWriteArrayList<>();
        PeerNode node = node(PeerNode.builder(PeerIdentity.generate())
                .watchdogEscalation(PeerNode.Escalation.EXIT).exiter(exits::add), "b");
        GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        DueWork.Registration reg = rt.onDue("asked", now -> DueWork.NEVER);
        node.startTicking(Duration.ofMillis(250));
        time.advance(Duration.ofSeconds(1));
        loseThreeNudges(node, reg);
        Measure.await("core.spoiler.watchdog-exit", Duration.ofSeconds(10), () -> !exits.isEmpty());
        assertThat(exits).containsExactly(PeerNode.WATCHDOG_EXIT_STATUS);
        assertThat(node.loop().closed()).as("the node closed before the exit").isTrue();
    }

    @Test
    void escalationParsesReportAndExitOnly() {
        assertThat(PeerNode.Escalation.parse("report")).isEqualTo(PeerNode.Escalation.REPORT);
        assertThat(PeerNode.Escalation.parse(" EXIT ")).isEqualTo(PeerNode.Escalation.EXIT);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PeerNode.Escalation.parse("log"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
