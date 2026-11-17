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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 0 of srad/replication-by-entry.md on the replicated space: the renewal that asks for less than its lease has
 * left is counted and logged once per space (RE-H-01), and since phase 2 keeps the longer lease (RE-H-02); and the
 * space reports what it holds and what its anti-entropy cost in one snapshot (RE-Q-MNT-02, RE-F-03).
 */
class SpaceStatsTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zStats");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zStats", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "stats",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace space(String address, long seed, String name) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        nodes.add(node);
        return ReplicatedSpace.builder(runtime, name, identity, "worker").clock(clock).settleWindow(Duration.ZERO)
                .build();
    }

    /**
     * RE-H-01 and RE-H-02: a write or take renewal for less than the lease has left is counted every time and logged
     * once for the space, naming it and the entry; renewing for more counts nothing; and the call keeps the longer
     * lease, since a renewal never shortens.
     */
    @Test
    void aShorteningRenewalIsCountedLoggedOnceAndKeepsTheLongerLease() throws IOException {
        ReplicatedSpace space = space("a", 1, "jobs");
        List<LogRecord> logged = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(ReplicatedSpace.class.getName());
        Handler capture = new Handler() {
            @Override public void publish(LogRecord record) { logged.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(capture);
        try {
            EntryHandle longer = space.write(new TaskEntry("longer", 1), Lease.of(Duration.ofMinutes(10)));
            longer.renew(Duration.ofMinutes(20));
            assertThat(space.renewShortenings()).as("renewing for more than is left").isZero();

            EntryHandle shorter = space.write(new TaskEntry("shorter", 2), Lease.of(Duration.ofMinutes(10)));
            shorter.renew(Duration.ofMinutes(1));
            shorter.renew(Duration.ofSeconds(30));
            assertThat(space.renewShortenings()).isEqualTo(2);

            TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class).where("topic",
                    ai.badmonkey.agentspaces.api.space.Matchers.eq("longer")), Lease.of(Duration.ofMinutes(10)),
                    Duration.ZERO).orElseThrow();
            taken.renew(Duration.ofMinutes(2));
            assertThat(space.renewShortenings()).as("a take renewal counts the same way").isEqualTo(3);
            assertThat(space.stats().renewShortenings()).isEqualTo(3);

            assertThat(logged).as("logged once for the space").hasSize(1);
            LogRecord record = logged.get(0);
            assertThat(record.getLevel()).isEqualTo(java.util.logging.Level.INFO);
            assertThat(record.getMessage()).contains("space jobs", shorter.entryId().value(), "write lease",
                    "kept the longer lease");

            clock.advance(Duration.ofMinutes(1));
            assertThat(space.read(Template.of(TaskEntry.class).where("topic",
                    ai.badmonkey.agentspaces.api.space.Matchers.eq("shorter")))).as("the thirty-second renewal kept "
                    + "the ten-minute lease").isPresent();
        } finally {
            logger.removeHandler(capture);
        }
    }

    /**
     * The snapshot sorts what the space knows into present, lapsed, withdrawn and completed, files the tombstones by
     * the time since their lease expired (a withdrawn or completed entry whose lease still runs apart, not as a fresh
     * one), and counts the digest cache and the states anti-entropy moved.
     */
    @Test
    void theSnapshotCountsEntriesTombstonesPastExpiryTheDigestCacheAndSyncStates() throws IOException {
        ReplicatedSpace space = space("a", 1, "jobs");
        ReplicatedSpace replica = space("b", 2, "jobs");
        ReplicatedSpace.Stats empty = space.stats();
        assertThat(empty.known()).isZero();
        assertThat(empty.tombstonesPastExpiry().total()).isZero();

        space.write(new TaskEntry("present", 1), Lease.of(Duration.ofHours(1)));
        space.write(new TaskEntry("lapses", 2), Lease.of(Duration.ofSeconds(2)));
        space.write(new TaskEntry("withdrawn", 3), Lease.of(Duration.ofMinutes(10))).cancel();
        space.write(new TaskEntry("done", 4), Lease.of(Duration.ofMinutes(10)));
        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class).where("topic",
                ai.badmonkey.agentspaces.api.space.Matchers.eq("done")), Lease.of(Duration.ofMinutes(5)),
                Duration.ZERO).orElseThrow();
        assertThat(space.stats().claims()).as("the take's claim").isEqualTo(1);
        space.complete(taken);
        clock.advance(Duration.ofSeconds(90));

        ReplicatedSpace.Stats stats = space.stats();
        assertThat(stats.present()).isEqualTo(1);
        assertThat(stats.lapsedInGrace()).isEqualTo(1);
        assertThat(stats.withdrawn()).isEqualTo(1);
        assertThat(stats.completed()).isEqualTo(1);
        assertThat(stats.known()).isEqualTo(4).isEqualTo(space.knownEntries());
        assertThat(stats.tombstones()).isEqualTo(3);
        // The lapsed entry expired 88 s ago; the withdrawn and completed ones keep leases that have not run out yet, so
        // they are counted as lease-running, not as tombstones under a minute past expiry.
        assertThat(stats.tombstonesPastExpiry()).isEqualTo(new ReplicatedSpace.TombstonesPastExpiry(2, 0, 1, 0, 0));
        assertThat(stats.tombstonesPastExpiry().total()).isEqualTo(stats.tombstones());
        assertThat(stats.maxLeaseObservedMillis()).isEqualTo(Duration.ofHours(1).toMillis());
        assertThat(stats.acknowledgementLines()).isZero();
        assertThat(stats.shardDroppedLines()).isZero();

        // The withdrawn and completed leases ran out a minute and a half ago; the lapsed one over eleven minutes ago.
        clock.advance(Duration.ofMinutes(10));
        assertThat(space.stats().tombstonesPastExpiry())
                .isEqualTo(new ReplicatedSpace.TombstonesPastExpiry(0, 0, 2, 1, 0));
        clock.advance(Duration.ofMinutes(20));
        assertThat(space.stats().tombstonesPastExpiry()).as("the snapshot runs no sweep, so a due tombstone still counts")
                .isEqualTo(new ReplicatedSpace.TombstonesPastExpiry(0, 0, 0, 3, 0));
        assertThat(space.stats().present()).as("the hour-long lease has not run out").isEqualTo(1);

        // A hash ask counts in the hash pair only, so one stale cache shows as one hash miss and one rebuild, not as a
        // cache miss as well; a full-digest ask then counts in the cache pair only.
        space.invalidateDigestForTest();
        ReplicatedSpace.Stats before = space.stats();
        space.digestHash();
        space.digestHash();
        ReplicatedSpace.Stats hashed = space.stats();
        assertThat(hashed.digestHashMisses() - before.digestHashMisses()).isEqualTo(1);
        assertThat(hashed.digestHashHits() - before.digestHashHits()).isEqualTo(1);
        assertThat(hashed.digestComputations() - before.digestComputations()).isEqualTo(1);
        assertThat(hashed.digestCacheMisses()).as("hash asks are not digest-cache asks")
                .isEqualTo(before.digestCacheMisses());
        assertThat(hashed.digestCacheHits()).isEqualTo(before.digestCacheHits());
        space.digest();
        space.invalidateDigestForTest();
        space.digest();
        ReplicatedSpace.Stats after = space.stats();
        assertThat(after.digestCacheHits() - hashed.digestCacheHits()).isEqualTo(1);
        assertThat(after.digestCacheMisses() - hashed.digestCacheMisses()).isEqualTo(1);
        assertThat(after.digestComputations() - hashed.digestComputations()).isEqualTo(1);
        assertThat(after.digestHashHits() + after.digestHashMisses()).as("full asks are not hash asks")
                .isEqualTo(hashed.digestHashHits() + hashed.digestHashMisses());

        byte[] delta = space.deltaFor(new byte[0]);
        long sent = space.stats().syncStatesSent();
        assertThat(sent).as("every state the space holds signed, tombstones included").isPositive();
        assertThat(replica.applySyncDelta(delta)).isTrue();
        assertThat(replica.stats().syncStatesReceived()).isEqualTo(sent);
        assertThat(replica.stats().syncClaimsReceived()).isEqualTo(space.stats().syncClaimsSent());
    }
}
