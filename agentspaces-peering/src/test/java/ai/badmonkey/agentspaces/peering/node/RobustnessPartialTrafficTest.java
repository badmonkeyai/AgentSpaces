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

import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §3.6, §3.7 and §3.3: a direction of a link lost, a slow direction, and a member
 * whose backlog is older than the staleness window when it finally lands.
 */
class RobustnessPartialTrafficTest {

    private final RobustnessFleet fleet = new RobustnessFleet("Partial");

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    @Timeout(60)
    void aOneWayPartitionIsSurvivedAndHealsWithoutAStrike() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        fleet.add("c", "a");
        assertThat(fleet.settle(10)).isTrue();

        // B hears nothing from A; A hears everything from B. C hears both.
        fleet.network.oneWay("a", "b");
        for (int i = 0; i < 6; i++) {
            fleet.publish("a", "during-" + i);
            fleet.tickAll(5);
        }
        assertThat(fleet.network.oneWayDropped("a", "b")).as("A kept sending into the void").isGreaterThan(0);
        // What A published still reached B through C (the tree) or by anti-entropy with C.
        assertThat(fleet.news.get("b")).as("B got A's items by way of C").contains("during-0", "during-5");
        // B suspects A for its silence; A keeps B. Neither quarantines the other: silence is not misbehaviour.
        assertThat(fleet.nodes.get("b").quarantined(fleet.id("a"))).isFalse();
        assertThat(fleet.nodes.get("a").quarantined(fleet.id("b"))).isFalse();
        long bDroppedDuring = fleet.nodes.get("b").framesDropped();

        fleet.network.twoWay("a", "b");
        assertThat(fleet.settle(40)).as("the views and inventories heal once the direction returns").isTrue();
        assertThat(fleet.nodes.get("b").framesDropped()).as("nothing was dropped as malformed").isEqualTo(bDroppedDuring);
        fleet.publish("a", "after");
        fleet.tickAll(4);
        assertThat(fleet.news.get("b")).contains("after");
        fleet.assertInvariants();
    }

    @Test
    @Timeout(60)
    void aSlowDirectionNeitherGraftsNorPrunesASoundEdge() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        fleet.add("c", "a");
        assertThat(fleet.settle(10)).isTrue();
        long graftsBefore = fleet.nodes.get("c").traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L);
        long prunesBefore = fleet.nodes.get("c").traffic().sentByKind().getOrDefault(Envelope.Kind.PRUNE, 0L);

        // Everything A sends C arrives one tick late, for twenty ticks of steady publishing.
        for (int i = 0; i < 20; i++) {
            fleet.network.hold("a", "c");
            fleet.publish("a", "slow-" + i);
            fleet.tickAll(1);
            fleet.network.release("a", "c", SimNetwork.Order.FIFO, 0);
        }
        fleet.tickAll(3);
        assertThat(fleet.news.get("c")).as("every item arrived once").hasSize(20).doesNotHaveDuplicates();
        assertThat(fleet.nodes.get("c").traffic().sentByKind().getOrDefault(Envelope.Kind.GRAFT, 0L) - graftsBefore)
                .as("a copy a tick late is not a missing copy").isZero();
        assertThat(fleet.nodes.get("c").traffic().sentByKind().getOrDefault(Envelope.Kind.PRUNE, 0L) - prunesBefore)
                .as("a late copy is a duplicate at most once per edge, never a storm").isLessThanOrEqualTo(2);
        fleet.assertInvariants();
    }

    @Test
    @Timeout(60)
    void aLateSendersStaleBacklogIsDroppedQuietlyAndItsStateStillConverges() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();

        // Everything A sends B queues for longer than the staleness window.
        fleet.network.hold("a", "b");
        for (int i = 0; i < 5; i++) {
            fleet.publish("a", "old-" + i);
            fleet.tickAll(2);
        }
        fleet.clock.advance(Duration.ofMinutes(6)); // past MAX_FRAME_AGE
        int held = fleet.network.heldCount("a", "b");
        assertThat(held).isGreaterThan(5);
        long droppedBefore = fleet.nodes.get("b").framesDropped();
        fleet.network.release("a", "b", SimNetwork.Order.FIFO, 0);
        assertThat(fleet.news.get("b")).as("stale rumors are dropped, not delivered").doesNotContain("old-0", "old-4");
        assertThat(fleet.nodes.get("b").quarantined(fleet.id("a"))).as("stale frames never strike the signer").isFalse();
        assertThat(fleet.nodes.get("b").framesDropped()).as("stale is not malformed").isEqualTo(droppedBefore);

        // Both dropped each other for the silence; the lost-member retry brings them back together
        // and anti-entropy brings the state the stale rumors carried.
        assertThat(fleet.settle(40)).as("views and inventories converge").isTrue();
        assertThat(fleet.inventories.get("b").members).contains("old-0", "old-4");
        fleet.publish("a", "fresh");
        fleet.tickAll(3);
        assertThat(fleet.news.get("b")).as("fresh traffic flows again").contains("fresh");
        fleet.assertInvariants();
    }
}
