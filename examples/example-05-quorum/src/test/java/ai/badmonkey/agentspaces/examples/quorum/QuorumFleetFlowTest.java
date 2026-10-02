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
package ai.badmonkey.agentspaces.examples.quorum;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.examples.quorum.QuorumFleet.Peer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** The aggregate-then-vote choreography over real TCP. */
class QuorumFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(90)
    void fleetSensesBacklogAndDecidesByQuorum() throws Exception {
        int seedPort = freePort();
        Peer supervisor = QuorumFleet.startPeer("supervisor", seedPort, 0);
        Peer w1 = QuorumFleet.startPeer("worker-1", freePort(), seedPort);
        Peer w2 = QuorumFleet.startPeer("worker-2", freePort(), seedPort);
        Peer w3 = QuorumFleet.startPeer("worker-3", freePort(), seedPort);
        List<Peer> fleet = List.of(supervisor, w1, w2, w3);
        try {
            Thread.sleep(1500);

            double[] backlogs = {4, 30, 41, 31};
            for (int i = 0; i < fleet.size(); i++) {
                fleet.get(i).aggregate().start("backlog", backlogs[i]);
            }
            for (int round = 0; round < 60; round++) {
                fleet.forEach(p -> p.aggregate().tick());
                Thread.sleep(10);
            }
            assertThat(supervisor.aggregate().estimate("backlog").orElseThrow())
                    .isCloseTo(26.5, within(0.5));

            supervisor.vote().propose("scale", "Scale up?", List.of("approve", "reject"),
                    4, Lease.of(Duration.ofMinutes(10)));
            Thread.sleep(1000);
            for (int i = 0; i < fleet.size(); i++) {
                String ballot = backlogs[i] > QuorumFleet.BACKLOG_THRESHOLD / 2
                        ? "approve" : "reject";
                fleet.get(i).vote().castBallot("scale", ballot,
                        Lease.of(Duration.ofMinutes(10)));
            }

            Optional<VoteCapability.Decision> decision = Optional.empty();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (decision.isEmpty() && System.nanoTime() < deadline) {
                decision = w3.vote().decision("scale");
                Thread.sleep(200);
            }
            assertThat(decision).isPresent();
            assertThat(decision.get().winner()).isEqualTo("approve");
            assertThat(decision.get().tally())
                    .containsEntry("approve", 3)
                    .containsEntry("reject", 1);
        } finally {
            fleet.forEach(Peer::close);
        }
    }

    /**
     * Act two (QA4 A4-7 phases 2 and 3): three seats on one peer, each an agent
     * with its own certified key voting through its own view; the authorizer
     * grants two. The two count, the intern does not, a quorum of two closes at
     * AGENT granularity — and a worker counting per peer sees the whole council
     * as one member with one ballot.
     */
    @Test
    @Timeout(90)
    void aGrantedCouncilOnOnePeerCountsPerAgentWhileTheFleetCountsPerPeer() throws Exception {
        int seedPort = freePort();
        Peer supervisor = QuorumFleet.startPeer("supervisor", seedPort, 0);
        Peer worker = QuorumFleet.startPeer("worker-1", freePort(), seedPort);
        try {
            Thread.sleep(1500);
            List<QuorumFleet.Seat> council = QuorumFleet.seatCouncil(supervisor,
                    List.of("finance", "ops", "intern"), java.util.Set.of("finance", "ops"));
            QuorumFleet.Seat finance = council.get(0);
            QuorumFleet.Seat ops = council.get(1);
            QuorumFleet.Seat intern = council.get(2);
            assertThat(finance.identity().isSubordinate()).isTrue();
            assertThat(finance.votes().writer()).contains(supervisor.node().identity().agent("finance"));
            assertThat(finance.vote().granularity())
                    .isEqualTo(ai.badmonkey.agentspaces.api.spi.Authorizer.Granularity.AGENT);

            finance.vote().propose("budget", "Approve?", List.of("approve", "reject"), 2,
                    Lease.of(Duration.ofMinutes(10)));
            Thread.sleep(500);
            finance.vote().castBallot("budget", "approve", Lease.of(Duration.ofMinutes(10)));
            assertThatThrownBy(() -> intern.vote().castBallot("budget", "reject",
                    Lease.of(Duration.ofMinutes(10))))
                    .as("an ungranted seat is refused before it writes")
                    .isInstanceOf(IllegalStateException.class);
            assertThat(finance.vote().tally("budget")).containsEntry("approve", 1);
            assertThat(finance.vote().decision("budget")).as("one of two").isEmpty();

            ops.vote().castBallot("budget", "approve", Lease.of(Duration.ofMinutes(10)));
            assertThat(finance.vote().tally("budget")).containsEntry("approve", 2);
            assertThat(finance.vote().decision("budget")).hasValueSatisfying(d -> {
                assertThat(d.winner()).isEqualTo("approve");
                assertThat(d.granularity())
                        .isEqualTo(ai.badmonkey.agentspaces.api.spi.Authorizer.Granularity.AGENT);
            });

            // The fleet's ordinary voters count per peer: the council is one member.
            Optional<VoteCapability.Decision> seen = Optional.empty();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (worker.vote().tally("budget").getOrDefault("approve", 0) < 1
                    && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(worker.vote().tally("budget")).containsEntry("approve", 1);
            assertThat(worker.vote().decision("budget")).as("two seats, one peer, one vote").isEmpty();
            assertThat(seen).isEmpty();
        } finally {
            supervisor.close();
            worker.close();
        }
    }

    /**
     * The same fleet through the annotations (LAYER4-ANNOTATIONS.md §2.1, §2.2):
     * members are {@code @Ballot} agents, the supervisor an {@code @OnDecision}
     * agent that writes the decision on the record. Nobody polls, nobody waits
     * for a proposal to become visible, nobody keeps a voted set.
     */
    @Test
    @Timeout(90)
    void annotatedMembersVoteAndTheSupervisorRecordsTheDecision() throws Exception {
        int seedPort = freePort();
        Peer supervisor = QuorumFleet.startPeer("supervisor", seedPort, 0);
        Peer w1 = QuorumFleet.startPeer("worker-1", freePort(), seedPort);
        Peer w2 = QuorumFleet.startPeer("worker-2", freePort(), seedPort);
        Peer w3 = QuorumFleet.startPeer("worker-3", freePort(), seedPort);
        List<Peer> fleet = List.of(supervisor, w1, w2, w3);
        List<AutoCloseable> agents = new java.util.ArrayList<>();
        try {
            Thread.sleep(1500);
            double[] backlogs = {4, 30, 41, 31};
            for (int i = 0; i < fleet.size(); i++) {
                agents.add(QuorumFleet.runMember(fleet.get(i), backlogs[i]));
            }
            agents.add(QuorumFleet.runSupervisor(supervisor));

            supervisor.vote().propose("scale-2", "Scale up?", List.of("approve", "reject"), 4,
                    Lease.of(Duration.ofMinutes(10)));

            Optional<QuorumFleet.ScalingDecision> recorded = Optional.empty();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (recorded.isEmpty() && System.nanoTime() < deadline) {
                recorded = supervisor.votes().read(ai.badmonkey.agentspaces.api.space.Template.of(
                        QuorumFleet.ScalingDecision.class));
                Thread.sleep(200);
            }
            assertThat(recorded).isPresent();
            assertThat(recorded.get().winner()).isEqualTo("approve");
            assertThat(recorded.get().countedPer()).isEqualTo("PEER");
            assertThat(w3.vote().decision("scale-2")).hasValueSatisfying(d ->
                    assertThat(d.tally()).containsEntry("approve", 3).containsEntry("reject", 1));
        } finally {
            for (AutoCloseable agent : agents) {
                agent.close();
            }
            fleet.forEach(Peer::close);
        }
    }

    /** Act two through the annotations: two granted council seats on one peer count per agent; the third is refused. */
    @Test
    @Timeout(90)
    void annotatedCouncilSeatsOnOnePeerCountPerGrantedAgent() throws Exception {
        int seedPort = freePort();
        Peer supervisor = QuorumFleet.startPeer("supervisor", seedPort, 0);
        List<AutoCloseable> seats = List.of();
        try {
            Thread.sleep(1000);
            seats = QuorumFleet.bindCouncil(supervisor, List.of("finance", "ops", "intern"),
                    java.util.Set.of("finance", "ops"));
            // A counting view at AGENT granularity, over the same grants the seats were bound with.
            List<QuorumFleet.Seat> council = QuorumFleet.seatCouncil(supervisor, List.of("auditor"),
                    java.util.Set.of("finance", "ops"));
            QuorumFleet.Seat counting = council.get(0);
            counting.vote().propose("budget-2", "Approve?", List.of("approve", "reject"), 2,
                    Lease.of(Duration.ofMinutes(10)));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (counting.vote().decision("budget-2").isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(counting.vote().tally("budget-2")).containsEntry("approve", 2);
            assertThat(counting.vote().decision("budget-2")).hasValueSatisfying(d ->
                    assertThat(d.granularity()).isEqualTo(ai.badmonkey.agentspaces.api.spi.Authorizer.Granularity.AGENT));
            // Per peer, the whole council is one member with one ballot.
            assertThat(supervisor.vote().tally("budget-2")).containsEntry("approve", 1);
        } finally {
            for (AutoCloseable seat : seats) {
                seat.close();
            }
            supervisor.close();
        }
    }
}
