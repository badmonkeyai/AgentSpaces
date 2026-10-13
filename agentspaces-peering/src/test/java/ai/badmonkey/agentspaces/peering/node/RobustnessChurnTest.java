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

import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §3.10: a fleet of a dozen members under seeded random churn (joins, crashes,
 * partitions, one-way losses, held and reordered links) with publishing throughout, for several
 * seeds. At every quiet point the invariants hold: views equal the live set, inventories
 * converged, nobody quarantined, connections bounded.
 */
class RobustnessChurnTest {

    private RobustnessFleet fleet;

    @AfterEach
    void tearDown() {
        if (fleet != null) {
            fleet.close();
        }
    }

    @Test
    @Timeout(300)
    void aFleetUnderSeededChurnAlwaysSettlesToTheInvariants() throws Exception {
        for (long seed = 1; seed <= 5; seed++) {
            int initial = seed == 5 ? 20 : 8; // the last seed starts large
            if (fleet != null) {
                fleet.close();
            }
            fleet = new RobustnessFleet("Churn" + seed);
            Random random = new Random(seed);
            GroupMembership.Config config = new GroupMembership.Config(Duration.ofSeconds(20), Duration.ofSeconds(2), 2);
            fleet.add("n0", null, PeerNode.ChannelAuth.SESSION, config);
            for (int i = 1; i < initial; i++) {
                fleet.add("n" + i, "n0", PeerNode.ChannelAuth.SESSION, config);
            }
            assertThat(fleet.settle(40)).as("seed %d: initial fleet settles", seed).isTrue();
            int next = initial;
            int published = 0;
            for (int round = 0; round < 12; round++) {
                List<String> live = new ArrayList<>(fleet.nodes.keySet());
                String x = live.get(random.nextInt(live.size()));
                String y = live.get(random.nextInt(live.size()));
                switch (random.nextInt(6)) {
                    case 0 -> {
                        if (live.size() < 24) {
                            fleet.add("n" + next++, x, PeerNode.ChannelAuth.SESSION, config);
                        }
                    }
                    case 1 -> {
                        if (live.size() > 4 && !x.equals("n0")) {
                            fleet.crash(x);
                        }
                    }
                    case 2 -> {
                        if (!x.equals(y)) {
                            fleet.network.partition(x, y);
                        }
                    }
                    case 3 -> {
                        if (!x.equals(y)) {
                            fleet.network.oneWay(x, y);
                        }
                    }
                    case 4 -> {
                        if (!x.equals(y)) {
                            fleet.network.hold(x, y);
                        }
                    }
                    default -> { }
                }
                for (int i = 0; i < 3; i++) {
                    String from = new ArrayList<>(fleet.nodes.keySet()).get(random.nextInt(fleet.nodes.size()));
                    fleet.publish(from, "s" + seed + "-r" + round + "-" + published++);
                }
                fleet.tickAll(2 + random.nextInt(4));
                // Every disturbance ends: links heal, held traffic lands in a random order, directions return.
                fleet.network.heal();
                for (String p : List.copyOf(fleet.nodes.keySet())) {
                    for (String q : List.copyOf(fleet.nodes.keySet())) {
                        if (!p.equals(q)) {
                            fleet.network.twoWay(p, q);
                            fleet.network.release(p, q, ai.badmonkey.agentspaces.test.SimNetwork.Order.values()[random.nextInt(4)], seed);
                        }
                    }
                }
                assertThat(fleet.settle(60)).as("seed %d round %d settles", seed, round).isTrue();
            }
            for (String address : fleet.nodes.keySet()) {
                assertThat(fleet.nodes.get(address).framesDropped()).as("%s dropped nothing: no frame was malformed", address).isZero();
            }
        }
    }
}
