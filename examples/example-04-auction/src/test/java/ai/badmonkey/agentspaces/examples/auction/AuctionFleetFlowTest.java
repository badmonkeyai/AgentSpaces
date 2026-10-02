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
package ai.badmonkey.agentspaces.examples.auction;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.examples.auction.AuctionFleet.MiniModelWorker;
import ai.badmonkey.agentspaces.examples.auction.AuctionFleet.ModelResult;
import ai.badmonkey.agentspaces.examples.auction.AuctionFleet.ModelTask;
import ai.badmonkey.agentspaces.examples.auction.AuctionFleet.Peer;
import ai.badmonkey.agentspaces.examples.auction.AuctionFleet.PremiumModelWorker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The auction fleet over real TCP: every task completes exactly once under
 * concurrent bidding. Deterministic per-task allocation is pinned separately by
 * {@code AuctionClusterTest} on the SimNetwork; this test proves the live fleet.
 */
class AuctionFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * QA4 A4-8: a space consults one cost function, so two bidders on one peer
     * are refused — by name — rather than the second silently replacing the
     * first. The auction in the next test seats them on two peers, which is the
     * shape that works.
     */
    @Test
    @Timeout(60)
    void twoBiddersOnOnePeerAreRefusedByName() throws Exception {
        Peer alone = AuctionFleet.startPeer("alone", freePort(), 0);
        try {
            alone.binder().bind(new MiniModelWorker());
            assertThatThrownBy(() -> alone.binder().bind(new PremiumModelWorker()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("mini-model")
                    .hasMessageContaining("premium-model")
                    .hasMessageContaining("model-tasks")
                    .hasMessageContaining("peer");
        } finally {
            alone.close();
        }
    }

    @Test
    @Timeout(90)
    void auctionFleetCompletesEveryTaskExactlyOnce() throws Exception {
        int seedPort = freePort();
        Peer coordinator = AuctionFleet.startPeer("coordinator", seedPort, 0);
        Peer mini = AuctionFleet.startPeer("mini", freePort(), seedPort);
        Peer premium = AuctionFleet.startPeer("premium", freePort(), seedPort);
        try {
            mini.binder().bind(new MiniModelWorker());
            premium.binder().bind(new PremiumModelWorker());
            Thread.sleep(1500);

            int tasks = 6;
            for (int difficulty = 1; difficulty <= tasks; difficulty++) {
                coordinator.tasks().write(new ModelTask("task-" + difficulty, difficulty),
                        Lease.of(Duration.ofMinutes(10)));
            }

            List<ModelResult> results = List.of();
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (results.size() < tasks && System.nanoTime() < deadline) {
                results = coordinator.tasks().readAll(Template.of(ModelResult.class), 20);
                Thread.sleep(250);
            }

            assertThat(results).hasSize(tasks);
            assertThat(results).extracting(ModelResult::prompt)
                    .containsExactlyInAnyOrder("task-1", "task-2", "task-3",
                            "task-4", "task-5", "task-6");
            assertThat(coordinator.tasks().read(Template.of(ModelTask.class))).isEmpty();
        } finally {
            premium.close();
            mini.close();
            coordinator.close();
        }
    }
}
