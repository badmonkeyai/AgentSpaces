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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The expiry index against the full pass it replaced (reviews/IDLE-WORK-2026-11-13.md item 4). Two replicas run a
 * random history of writes, cancels, renewals, takes, completions, take renewals, partitions, heals, ticks, clock
 * jumps past lease lapses and collection horizons, and one replica's clock stepping back; at random moments each
 * replica is asked what a sweep over every entry would do now ({@link ReplicatedSpace#fullScanPlan}), then sweeps
 * through its index, and the two must agree exactly: the same EXPIRED entries, the same REAPPEARED claim epochs, the
 * same orphaned claims dropped, the same tombstones collected. The tick, the digest and the delta sweep in between, as
 * in production, and each of those sweeps is audited too ({@link ReplicatedSpace#auditSweeps}): one the index skipped
 * must be one the full pass would have found nothing for, and one it ran must do what the full pass would have done
 * at that instant. A sweep skipped or run late between two check points is caught where it happens, not left for a
 * later lapse to hide.
 */
class ExpiryIndexPropertyTest {

    private static final String[] TOPICS = {"alpha", "beta", "gamma"};

    private final TestClock clock = TestClock.create();
    /** Replica A reads the shared clock less this skew, so its clock can step back. */
    private final AtomicLong skewMillis = new AtomicLong();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zExpiryIndex");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zExpiryIndex", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "expiry-index",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace replica(String address, long seed, InstantSource spaceClock, Duration maxLease,
                                    String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        nodes.add(node);
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(spaceClock)
                .settleWindow(Duration.ZERO);
        if (maxLease != null) {
            builder.maxLease(maxLease);
        }
        return builder.build();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
    void theIndexedSweepDoesExactlyWhatTheFullPassDoes(long seed) throws IOException {
        Random random = new Random(seed);
        Duration maxLease = seed % 3 == 0 ? Duration.ofSeconds(20) : null; // a fixed horizon, or the observed one
        InstantSource skewed = () -> clock.instant().minusMillis(skewMillis.get());
        ReplicatedSpace a = replica("a", seed, skewed, maxLease);
        ReplicatedSpace b = replica("b", seed + 100, clock, maxLease, "a");
        List<ReplicatedSpace> replicas = List.of(a, b);
        List<String> mismatches = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicLong audited = new AtomicLong();
        AtomicLong skipped = new AtomicLong();
        for (ReplicatedSpace replica : replicas) {
            String label = replica == a ? "a" : "b";
            // Records only: the sweep runs under the replica's lock, often on the tick, where a throw would be lost.
            replica.auditSweeps((now, planned, done) -> {
                audited.incrementAndGet();
                if (done.equals(ReplicatedSpace.SweepActions.NONE)) {
                    skipped.incrementAndGet();
                }
                if (!planned.equals(done)) {
                    mismatches.add("replica " + label + " at " + now + ": full pass " + planned + ", indexed " + done);
                }
            });
        }
        tick(6);

        List<EntryHandle> handles = new ArrayList<>();
        List<TakenEntry<TaskEntry>> taken = new ArrayList<>();
        Template<TaskEntry> any = Template.of(TaskEntry.class);
        boolean partitioned = false;
        int checks = 0;
        int acted = 0;
        for (int step = 0; step < 400; step++) {
            ReplicatedSpace space = replicas.get(random.nextInt(2));
            int op = random.nextInt(100);
            try {
                if (op < 25) {
                    handles.add(space.write(new TaskEntry(TOPICS[random.nextInt(TOPICS.length)], step),
                            Lease.of(Duration.ofSeconds(1 + random.nextInt(60)))));
                } else if (op < 33 && !handles.isEmpty()) {
                    handles.get(random.nextInt(handles.size())).cancel();
                } else if (op < 40 && !handles.isEmpty()) {
                    handles.get(random.nextInt(handles.size())).renew(Duration.ofSeconds(1 + random.nextInt(60)));
                } else if (op < 52) {
                    space.take(any, Lease.of(Duration.ofSeconds(1 + random.nextInt(40))), Duration.ZERO)
                            .ifPresent(taken::add);
                } else if (op < 57 && !taken.isEmpty()) {
                    TakenEntry<TaskEntry> t = taken.remove(random.nextInt(taken.size()));
                    space.complete(t);
                } else if (op < 62 && !taken.isEmpty()) {
                    taken.get(random.nextInt(taken.size())).renew(Duration.ofSeconds(1 + random.nextInt(40)));
                } else if (op < 66) {
                    if (partitioned) {
                        network.heal();
                    } else {
                        network.partition("a", "b");
                    }
                    partitioned = !partitioned;
                } else if (op < 78) {
                    tick(1 + random.nextInt(3));
                } else if (op < 90) {
                    // Short steps across lease lapses and claim lapses, now and then a jump past the horizons.
                    clock.advance(random.nextInt(10) == 0
                            ? Duration.ofMinutes(1 + random.nextInt(8))
                            : Duration.ofMillis(random.nextInt(45_000)));
                } else if (op < 93) {
                    // Replica A's clock steps back (or returns): the index judged what is due from a later time.
                    skewMillis.set(skewMillis.get() == 0 ? 1 + random.nextInt(40_000) : 0);
                } else {
                    for (ReplicatedSpace replica : replicas) {
                        long now = replica == a ? skewed.instant().toEpochMilli() : clock.instant().toEpochMilli();
                        ReplicatedSpace.SweepActions planned = replica.fullScanPlan(now);
                        replica.sweepNow();
                        ReplicatedSpace.SweepActions done = replica.lastSweepActions();
                        assertThat(done).as("seed %d step %d: the indexed sweep at %d", seed, step, now)
                                .isEqualTo(planned);
                        checks++;
                        if (!planned.equals(ReplicatedSpace.SweepActions.NONE)) {
                            acted++;
                        }
                    }
                }
            } catch (LeaseExpiredException | IllegalStateException expected) {
                // A renewal or completion of something that lapsed or went: part of a random history.
            }
        }
        // Drain: let everything lapse and collect, checking on the way.
        network.heal();
        skewMillis.set(0);
        for (int i = 0; i < 12; i++) {
            clock.advance(Duration.ofMinutes(10));
            for (ReplicatedSpace replica : replicas) {
                ReplicatedSpace.SweepActions planned = replica.fullScanPlan(clock.instant().toEpochMilli());
                replica.sweepNow();
                assertThat(replica.lastSweepActions()).as("seed %d drain %d", seed, i).isEqualTo(planned);
                checks++;
            }
            tick(1);
        }
        assertThat(checks).isGreaterThan(20);
        assertThat(acted).as("the history exercised sweeps that did something").isPositive();
        assertThat(mismatches).as("seed %d: every tick, digest and delta sweep agreed with the full pass", seed)
                .isEmpty();
        assertThat(audited.get()).as("the tick and the digest swept between the checks").isGreaterThan(100);
        assertThat(skipped.get()).as("and the index skipped some of those sweeps, each one checked").isPositive();
        for (ReplicatedSpace replica : replicas) {
            assertThat(replica.knownEntries()).as("seed %d: every lease lapsed and every tombstone went", seed).isZero();
            assertThat(replica.claimsHeld()).as("seed %d: and every claim with it", seed).isZero();
        }
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }
}
