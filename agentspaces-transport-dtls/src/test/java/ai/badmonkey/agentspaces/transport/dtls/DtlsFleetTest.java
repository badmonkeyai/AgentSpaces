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
package ai.badmonkey.agentspaces.transport.dtls;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole fabric over DTLS: two peers join a group through
 * {@code Endpoint("dtls", …)} seeds, membership and gossip converge, and a
 * replicated space carries a leased take round-trip — nothing above the
 * transport changes.
 */
class DtlsFleetTest {


    /** A task the fleet works. */
    public record Job(String jobId, String payload) {
    }

    /** The worked result. */
    public record Done(String jobId, String by) {
    }

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                        ReplicatedSpace space, DtlsTransport transport) {
    }

    private final List<Peer> peers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.space().close();
            peer.node().close();
            peer.transport().close();
        }
    }

    @Test
    @Timeout(120)
    void membershipGossipAndLeasedTakesRunUnchangedOverDtls() throws Exception {
        int seedPort = freeUdpPort();
        Peer first = startPeer(seedPort, 0);
        Peer second = startPeer(freeUdpPort(), seedPort);

        // The writer's entry replicates to the taker over DTLS.
        first.space().write(new Job("job-1", "compress the archives"),
                Lease.of(Duration.ofMinutes(10)));
        Optional<TakenEntry<Job>> taken = second.space().take(
                Template.of(Job.class), Lease.of(Duration.ofMinutes(5)),
                Duration.ofSeconds(30));
        assertThat(taken).isPresent();
        assertThat(taken.get().entry().jobId()).isEqualTo("job-1");

        // The completion and its result flow back the other way.
        second.space().complete(taken.get(), new Done("job-1", "second"),
                Lease.of(Duration.ofMinutes(10)));
        Optional<Done> done = awaitDone(first, Duration.ofSeconds(30));
        assertThat(done).isPresent();
        assertThat(done.get().by()).isEqualTo("second");

        // And each membership view holds the other peer, learned over DTLS.
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && (memberIds(first).isEmpty()
                || memberIds(second).isEmpty())) {
            Thread.sleep(250);
        }
        assertThat(memberIds(first)).contains(second.identity().peerId());
        assertThat(memberIds(second)).contains(first.identity().peerId());
    }

    /**
     * P30 of reviews/RBE-REGRESSION-MATRIX.md (srad/replication-by-entry.md RE-I-04, RE-I-05; SPEC annex I.6): a member
     * joins late over DTLS a space another member already holds, with every anti-entropy answer cut to 4 KiB. The
     * joiner's audit descends the holder's trie past the root to the leaves and fetches every entry by id in answers
     * cut to the budget, continuing each that carries a remainder; each answer is a frame the recovery sublayer
     * fragments into datagrams. The space converges to equal stream hashes. The test prints what the join took; the
     * 120 s wait only means "broken" (a few seconds here).
     */
    @Test
    @Timeout(180)
    void aLateJoinersTrieAuditDescendsAndFetchesInBudgetedAnswersOverDtls() throws Exception {
        long budget = 4_096;
        int n = 600;
        int seedPort = freeUdpPort();
        Peer holder = startPeer(seedPort, 0, budget, "tasks");
        for (int i = 0; i < n; i++) {
            holder.space().write(new Job("job-" + i, "row " + i + " of the late join"), Lease.of(Duration.ofHours(1)));
        }
        long started = System.nanoTime();
        Peer joiner = startPeer(freeUdpPort(), seedPort, budget, "tasks");
        long deadline = started + Duration.ofSeconds(120).toNanos();
        while (System.nanoTime() < deadline && jobIds(joiner).size() < n) {
            Thread.sleep(50);
        }
        long millis = Duration.ofNanos(System.nanoTime() - started).toMillis();
        GossipBus.AuditSignals asker = joiner.runtime().gossip().auditSignals();
        GossipBus.AuditSignals partner = holder.runtime().gossip().auditSignals();
        GossipBus.PacingSignals pacing = joiner.runtime().gossip().pacingSignals();
        System.out.printf("dtls budgeted join (P30): %d entries under a %,d-byte budget in %d ms; the holder answered %d"
                        + " roots and %d descent requests (%d nodes) and cut %d answers to the budget; the joiner asked"
                        + " %d ids, continued %d at once, re-queued %d, received %d PULL_RESP of %,d bytes with %d states"
                        + " left for later; dtls retransmitted %d of %d datagrams%n", n, budget, millis,
                partner.rootsAnswered(), partner.requestsAnswered(), partner.nodesAnswered(), partner.budgetedAnswers(),
                asker.idsFetched(), asker.fetchContinued(), asker.fetchRequeued(), pacing.pullRespReceived(),
                pacing.pullRespBytesReceived(), asker.remainderReceived(), holder.transport().counters().retransmitted(),
                holder.transport().counters().pdusSent());

        assertThat(jobIds(joiner)).as("the joiner holds every entry").hasSize(n).isEqualTo(jobIds(holder));
        assertThat(joiner.space().knownEntries()).isEqualTo(holder.space().knownEntries());
        assertThat(partner.rootsAnswered()).as("the joiner's audit opened on the holder's root").isPositive();
        assertThat(partner.requestsAnswered()).as("and descended past it").isPositive();
        assertThat(asker.idsFetched()).as("every entry fetched by id").isGreaterThanOrEqualTo(n);
        assertThat(partner.budgetedAnswers()).as("a sequence of bounded answers").isGreaterThan(1);
        assertThat(asker.remainderReceived()).isPositive();
        assertThat(asker.refusals()).doesNotContainKeys("fingerprint-mismatch", "unsolicited-answer");
    }

    private static java.util.Set<String> jobIds(Peer peer) {
        java.util.Set<String> ids = new java.util.HashSet<>();
        for (Job job : peer.space().readAll(Template.of(Job.class), 100_000)) {
            ids.add(job.jobId());
        }
        return ids;
    }

    private static List<ai.badmonkey.agentspaces.common.id.PeerId> memberIds(Peer peer) {
        return peer.runtime().membership().allMembers().stream()
                .map(GroupMembership.Member::id).toList();
    }

    private static Optional<Done> awaitDone(Peer peer, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Done> done = peer.space().read(Template.of(Done.class));
            if (done.isPresent()) {
                return done;
            }
            Thread.sleep(200);
        }
        return Optional.empty();
    }

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "dtls-fleet-test-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "dtls-fleet-test",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private Peer startPeer(int port, int seedPort) throws Exception {
        return startPeer(port, seedPort, 0, "jobs");
    }

    private Peer startPeer(int port, int seedPort, long auditBudget, String spaceName) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode.Builder builder = PeerNode.builder(identity);
        if (auditBudget > 0) {
            builder.auditBudget(auditBudget);
        }
        PeerNode node = builder.build();
        DtlsTransport transport = new DtlsTransport(identity);
        node.listen(transport, "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("dtls", "127.0.0.1:" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, spaceName, identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, space, transport);
        peers.add(peer);
        return peer;
    }

    private static int freeUdpPort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
