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
package ai.badmonkey.agentspaces.examples.fleet;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.examples.fleet.ResearchFleet.Finding;
import ai.badmonkey.agentspaces.examples.fleet.ResearchFleet.Peer;
import ai.badmonkey.agentspaces.examples.fleet.ResearchFleet.ResearchTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The M1 success criterion as an integration test: peers joined over real TCP on
 * localhost, a worker killed mid-task, and every task completed exactly once.
 */
class ResearchFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * QA4 A4-9: one replica per space name per node. A second handle for an open
     * name is refused instead of silently stealing the first handle's
     * replication; closing the space deregisters it, and the same name can then
     * be opened again and converges. "Close, then open again" is the one
     * legitimate reason a program builds the same space twice.
     */
    @Test
    @Timeout(60)
    void aClosedSpaceReopensByNameAndConverges() throws Exception {
        int coordinatorPort = freePort();
        Peer coordinator = ResearchFleet.startPeer("coordinator", coordinatorPort, 0);
        Peer worker = ResearchFleet.startPeer("worker", freePort(), coordinatorPort);
        ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace reopened = null;
        try {
            assertThatThrownBy(() -> ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace
                    .builder(coordinator.runtime(), "tasks", coordinator.node().identity(), "again")
                    .build())
                    .as("a second handle for an open name is refused")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("space:tasks");

            coordinator.space().close();
            reopened = ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace
                    .builder(coordinator.runtime(), "tasks", coordinator.node().identity(), "coordinator")
                    .build();
            worker.space().write(new ResearchTask("after reopen", 1),
                    ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(10)));
            var seen = reopened.read(ai.badmonkey.agentspaces.api.space.Template.of(ResearchTask.class),
                    Duration.ofSeconds(20));
            assertThat(seen).as("the reopened handle converges").isPresent();
        } finally {
            if (reopened != null) {
                reopened.close();
            }
            worker.close();
            coordinator.node().close();
        }
    }

    @Test
    @Timeout(60)
    void fleetDrainsTasksWithKillToleranceOverTcp() throws Exception {
        int coordinatorPort = freePort();
        int workerPort = freePort();
        Peer coordinator = ResearchFleet.startPeer("coordinator", coordinatorPort, 0);
        Peer worker = ResearchFleet.startPeer("worker", workerPort, coordinatorPort);
        Peer doomed = ResearchFleet.startPeer("doomed", freePort(), coordinatorPort);
        try {
            Thread.sleep(1500); // membership gossip settles

            int tasks = 4;
            for (int i = 1; i <= tasks; i++) {
                coordinator.space().write(new ResearchTask("t-" + i, i),
                        Lease.of(Duration.ofMinutes(10)));
            }

            // The doomed worker takes one task and dies without completing.
            Optional<TakenEntry<ResearchTask>> dying = doomed.space().take(
                    Template.of(ResearchTask.class),
                    Lease.of(Duration.ofSeconds(2)), Duration.ofSeconds(10));
            assertThat(dying).isPresent();
            doomed.close();

            // The surviving worker drains everything, including the reappearing task.
            Set<String> completedTopics = new HashSet<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (completedTopics.size() < tasks && System.nanoTime() < deadline) {
                Optional<TakenEntry<ResearchTask>> taken = worker.space().take(
                        Template.of(ResearchTask.class),
                        Lease.of(Duration.ofSeconds(5)), Duration.ofSeconds(2));
                if (taken.isEmpty()) {
                    continue;
                }
                worker.space().complete(taken.get(),
                        new Finding(taken.get().entry().topic(), "done", "worker"),
                        Lease.of(Duration.ofMinutes(10)));
                completedTopics.add(taken.get().entry().topic());
            }

            assertThat(completedTopics)
                    .containsExactlyInAnyOrder("t-1", "t-2", "t-3", "t-4");

            // The coordinator converges on all findings, each completed exactly once.
            List<Finding> findings = List.of();
            deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (findings.size() < tasks && System.nanoTime() < deadline) {
                findings = coordinator.space().readAll(Template.of(Finding.class), 10);
                Thread.sleep(200);
            }
            assertThat(findings).hasSize(tasks);
            assertThat(coordinator.space().read(Template.of(ResearchTask.class))).isEmpty();
        } finally {
            worker.close();
            coordinator.close();
        }
    }
}
