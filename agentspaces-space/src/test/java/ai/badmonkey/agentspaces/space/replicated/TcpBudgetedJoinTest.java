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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A budgeted join over a real transport (srad/replication-by-entry.md RE-I-05, RE-Q-PRF-03; the matrix's probe P31):
 * two nodes on TCP over the loopback, the holder with n signed entries and the joiner with none. The joiner audits the
 * holder, fetches every entry by id in answers cut to its budget, continues each answer that carries a remainder at
 * once, and checks each answer's signatures on the verification pool before its single-lock merge.
 *
 * <p>By default it joins 2,000 entries under a 64 KiB budget, which takes many answers. The time of RE-Q-PRF-03 is
 * measured with {@code -Dagentspaces.bench.tcp-join=100000}, whose budget is the default 1 MiB unless
 * {@code agentspaces.bench.tcp-join-budget} says otherwise. The test prints what it measured.
 */
class TcpBudgetedJoinTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    @Test
    void aBudgetedJoinOverTcpConvergesInManyBoundedAnswers() throws Exception {
        int n = Integer.getInteger("agentspaces.bench.tcp-join", 2_000);
        long budget = Long.getLong("agentspaces.bench.tcp-join-budget", n >= 50_000 ? 1L << 20 : 64L << 10);
        GroupId groupId = GroupId.fromFounding("tcp-budgeted-join".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "tcp-join",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        PeerIdentity identityA = PeerIdentity.generate();
        PeerIdentity identityB = PeerIdentity.generate();
        int portA = freePort();
        int portB = freePort();
        PeerNode a = PeerNode.builder(identityA).auditBudget(budget).build();
        PeerNode b = PeerNode.builder(identityB).auditBudget(budget).build();
        try {
            a.listen(new TcpTransport(), "127.0.0.1:" + portA);
            GroupRuntime runtimeA = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
            ReplicatedSpace sa = ReplicatedSpace.builder(runtimeA, "tasks", identityA, "worker")
                    .settleWindow(Duration.ZERO).build();

            // The holder's entries: signed in parallel as their writer would, and merged in batches.
            long seeding = System.nanoTime();
            long base = System.currentTimeMillis() - n; // one millisecond apart, all in the past
            long expires = System.currentTimeMillis() + Duration.ofHours(2).toMillis();
            List<SpaceWire.EntryStateDto> signed = IntStream.range(0, n).parallel()
                    .mapToObj(i -> ForgeSupport.authoredWrite(identityA, "worker", sa.id(),
                            new TaskEntry("join-" + i, i),
                            new HlcTimestamp(base + i, 0, identityA.peerId().value()), expires))
                    .toList();
            for (int at = 0; at < n; at += 5_000) {
                List<SpaceWire.EntryStateDto> chunk = new ArrayList<>(signed.subList(at, Math.min(n, at + 5_000)));
                sa.applySyncDelta(CODEC.toBytes(new SpaceWire.SyncDelta(chunk, Map.of())));
            }
            double seedSeconds = (System.nanoTime() - seeding) / 1e9;
            assertThat(sa.knownEntries()).isEqualTo(n);

            b.listen(new TcpTransport(), "127.0.0.1:" + portB);
            GroupRuntime runtimeB = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + portA, 0)));
            ReplicatedSpace sb = ReplicatedSpace.builder(runtimeB, "tasks", identityB, "worker")
                    .settleWindow(Duration.ZERO).build();
            long started = System.nanoTime();
            a.startTicking(Duration.ofMillis(200));
            b.startTicking(Duration.ofMillis(200));
            long deadline = started + Duration.ofSeconds(Math.max(60, n / 500)).toNanos();
            while (sb.knownEntries() < n && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            double joinSeconds = (System.nanoTime() - started) / 1e9;

            GossipBus.AuditSignals asker = runtimeB.gossip().auditSignals();
            GossipBus.AuditSignals holder = runtimeA.gossip().auditSignals();
            GossipBus.PacingSignals pacing = runtimeB.gossip().pacingSignals();
            System.out.printf("tcp budgeted join: %,d entries under a %,d-byte budget joined in %.2f s "
                            + "(%.1f us per entry; seeding the holder took %.1f s, not counted); %,d answers cut to the "
                            + "budget, %,d delta bytes received, %,d ids asked, %,d continued at once, %,d re-queued, "
                            + "%,d timed out%n", n, budget, joinSeconds, joinSeconds * 1e6 / n, seedSeconds,
                    holder.budgetedAnswers(), pacing.pullRespBytesReceived(), asker.idsFetched(),
                    asker.fetchContinued(), asker.fetchRequeued(), asker.fetchTimedOut());

            assertThat(sb.knownEntries()).as("the joiner holds every entry").isEqualTo(n);
            assertThat(sb.streamHash()).isEqualTo(sa.streamHash());
            assertThat(holder.budgetedAnswers()).as("a sequence of bounded answers").isGreaterThan(1);
            assertThat(asker.remainderReceived()).isPositive();
            assertThat(pacing.pullRespBytesReceived() / Math.max(1, pacing.pullRespReceived()))
                    .as("answers average within the budget").isLessThanOrEqualTo(budget);
        } finally {
            a.close();
            b.close();
        }
    }

    private static int freePort() throws Exception {
        // below the ephemeral range, so no other process takes it before the test binds it (parallel forks)
        return ai.badmonkey.agentspaces.test.Ports.free();
    }
}
