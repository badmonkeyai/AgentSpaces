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

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The targeted relay (srad/agentspaces-event-loop.md §3.12, TR-1 to TR-5, EL-P-13, EL-P-14) at four members on virtual
 * time: a dial-only peer linked to one member is probed by the others through that member alone, never directly, and
 * stays in every view for 30 minutes; when its only link is cut, every view drops it (REPAIRED) and no copy of its card
 * brings it back.
 */
@Timeout(300)
class TargetedRelayTest {

    private static final Duration STEP = Duration.ofMillis(250);

    private static void step(RobustnessFleet fleet) {
        for (PeerNode node : List.copyOf(fleet.nodes.values())) {
            node.tick();
        }
        fleet.clock.advance(STEP);
    }

    private static boolean everyViewHolds(RobustnessFleet fleet, PeerId peer) {
        return fleet.runtimes.values().stream().allMatch(r -> r.membership().member(peer).isPresent());
    }

    private static boolean noViewHolds(RobustnessFleet fleet, PeerId peer) {
        return fleet.runtimes.values().stream().noneMatch(r -> r.membership().member(peer).isPresent());
    }

    @Test
    void aDialOnlyPeerIsProbedThroughItsHolderAndDroppedEverywhereWhenItsLinkIsCut() throws Exception {
        RobustnessFleet fleet = new RobustnessFleet("TargetedRelay4");
        try (ThinSimPeer thin = new ThinSimPeer(fleet.network, "thin", fleet.groupId, fleet.clock)) {
            for (int i = 0; i < 3; i++) {
                fleet.add("m" + i, i == 0 ? null : "m0", PeerNode.ChannelAuth.SIGNED,
                        GroupMembership.Config.defaults(), true);
            }
            PeerId seed = fleet.id("m0");
            thin.dial("m0", seed, List.of(seed));
            int steps = 0;
            while (!everyViewHolds(fleet, thin.peerId()) || fleet.runtimes.values().stream()
                    .anyMatch(r -> r.membership().size() != 3)) {
                step(fleet);
                assertThat(++steps).as("the group and the thin peer form within 60 s").isLessThan(240);
            }
            int departures = 0;
            for (int i = 0; i < 30 * 60 * 4; i++) {
                step(fleet);
                departures += everyViewHolds(fleet, thin.peerId()) ? 0 : 1;
            }
            assertThat(departures).as("PREVENTED: steps in 30 minutes with the thin peer out of a view").isZero();
            assertThat(thin.pingsFrom().keySet()).as("every PING it answered came over its one link")
                    .containsExactly(seed);
            for (String member : List.of("m1", "m2")) {
                GroupMembership.ProbeStats stats = fleet.runtimes.get(member).membership().probeStats();
                assertThat(stats.holderRounds()).as("%s probed it through its holder", member).isGreaterThan(30);
                assertThat(stats.holderPingReqsByRelay()).as("%s asked the seed alone", member).containsOnlyKeys(seed);
                assertThat(stats.randomPingReqs()).as("%s never escalated: no direct PING went nowhere", member)
                        .isZero();
                assertThat(stats.escalations()).isZero();
            }
            assertThat(fleet.runtimes.get("m0").membership().probeStats().holderRounds())
                    .as("the seed holds the link and probes directly").isZero();

            // REPAIRED: the link is cut; every view drops the thin peer, and no copy of its card brings it back
            fleet.network.partition("m0", "thin");
            long cutAt = fleet.clock.millis();
            long bound = fleet.runtimes.values().stream().mapToLong(r -> r.membership().memberTtlMillis()
                    + r.membership().quietProbeCycleMillis()).max().orElseThrow();
            while (!noViewHolds(fleet, thin.peerId())) {
                step(fleet);
                assertThat(fleet.clock.millis() - cutAt).as("dropped within the member TTL and one cycle")
                        .isLessThanOrEqualTo(bound);
            }
            long dropped = fleet.clock.millis() - cutAt;
            int returns = 0;
            for (int i = 0; i < 4 * 60 * 4; i++) {
                step(fleet);
                returns += noViewHolds(fleet, thin.peerId()) ? 0 : 1;
            }
            assertThat(returns).as("not re-admitted from a copy of its card").isZero();
            System.out.printf("[targeted-relay] 4 members: kept 30 min; dropped everywhere %d ms after the cut%n",
                    dropped);
        } finally {
            fleet.close();
        }
    }
}
