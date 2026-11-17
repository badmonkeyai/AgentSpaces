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
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adversarial checks of phase 2 of srad/replication-by-entry.md: a renewal that races a cancel or a completion on
 * another replica, deltas delivered out of order, and the bound on acknowledgements of collected tombstones.
 */
class AdversaryRenewAckTest {

    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAdvRenew", PeerIdentity.generate().peerId(), GroupId.of("zAdvRenew"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "adv-renew",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace.Builder space(String address, long seed) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        nodes.add(node);
        return ReplicatedSpace.builder(runtime, "jobs", identity, "worker").clock(clock)
                .settleWindow(Duration.ZERO);
    }

    private static Template<TaskEntry> topic(String topic) {
        return Template.of(TaskEntry.class).where("topic", Matchers.eq(topic));
    }

    /** Exchanges anti-entropy both ways until neither side has anything to offer, at most ten rounds. */
    private static void sync(ReplicatedSpace a, ReplicatedSpace b) {
        for (int i = 0; i < 10; i++) {
            byte[] toB = a.deltaFor(b.digest());
            if (toB.length > 0) {
                b.applySyncDelta(toB);
            }
            byte[] toA = b.deltaFor(a.digest());
            if (toA.length > 0) {
                a.applySyncDelta(toA);
            }
            if (toA.length == 0 && toB.length == 0) {
                return;
            }
        }
    }

    private static long ackLines(ReplicatedSpace space) {
        return Arrays.stream(new String(space.digest(), StandardCharsets.UTF_8).split("\n"))
                .filter(line -> line.startsWith("a:")).count();
    }

    /**
     * A renewal and a cancel of the same entry reach a replica in the reverse order (the renewal's delta was in
     * flight when the cancel went out). The replica must end withdrawn, in step with the issuer, and a third replica
     * that saw them in the right order must hold the same state.
     */
    @Test
    void aRenewalDeliveredAfterTheCancelDoesNotRevivePresence() throws IOException {
        ReplicatedSpace issuer = space("a", 1).build();
        ReplicatedSpace reversed = space("b", 2).build();
        ReplicatedSpace ordered = space("c", 3).build();
        EntryHandle handle = issuer.write(new TaskEntry("race", 1), MINUTES_10);
        byte[] written = issuer.deltaFor(new byte[0]);
        reversed.applySyncDelta(written);
        ordered.applySyncDelta(written);
        byte[] before = reversed.digest();

        clock.advance(Duration.ofMinutes(1));
        handle.renew(Duration.ofMinutes(30));
        byte[] renewal = issuer.deltaFor(before);
        handle.cancel();
        byte[] cancel = issuer.deltaFor(before);

        reversed.applySyncDelta(cancel);
        reversed.applySyncDelta(renewal);
        ordered.applySyncDelta(renewal);
        ordered.applySyncDelta(cancel);

        assertThat(reversed.read(topic("race"))).as("withdrawn on the replica that saw the renewal last").isEmpty();
        assertThat(ordered.read(topic("race"))).isEmpty();
        assertThat(reversed.digest()).isEqualTo(issuer.digest());
        assertThat(ordered.digest()).isEqualTo(issuer.digest());
        clock.advance(Duration.ofMinutes(5));
        assertThat(reversed.read(topic("race"))).isEmpty();
    }

    /**
     * The issuer renews while a member elsewhere takes and completes the entry, neither having seen the other. After
     * anti-entropy both hold it completed, their digests agree, and the completing replica raises no second WRITTEN
     * when the issuer's renewal reaches it.
     */
    @Test
    void aRenewalRacingACompletionElsewhereConvergesCompleted() throws IOException {
        ReplicatedSpace issuer = space("a", 1).build();
        ReplicatedSpace taker = space("b", 2).build();
        List<SpaceEvent<TaskEntry>> heard = new CopyOnWriteArrayList<>();
        taker.notify(topic("job"), heard::add, Lease.of(Duration.ofHours(1)));
        EntryHandle handle = issuer.write(new TaskEntry("job", 1), MINUTES_10);
        taker.applySyncDelta(issuer.deltaFor(new byte[0]));

        clock.advance(Duration.ofMinutes(1));
        TakenEntry<TaskEntry> taken = taker.take(topic("job"), MINUTES_10, Duration.ZERO).orElseThrow();
        taker.complete(taken);
        handle.renew(Duration.ofMinutes(30)); // the issuer has not heard of the take

        sync(issuer, taker);
        assertThat(issuer.read(topic("job"))).as("completed at the issuer").isEmpty();
        assertThat(taker.read(topic("job"))).as("completed at the taker").isEmpty();
        assertThat(issuer.digest()).isEqualTo(taker.digest());
        assertThat(heard.stream().filter(e -> e.kind() == SpaceEvent.Kind.WRITTEN).count())
                .as("one WRITTEN at the taker, before the take").isEqualTo(1);
        assertThat(heard.stream().filter(e -> e.kind() == SpaceEvent.Kind.COMPLETED).count()).isEqualTo(1);
    }

    /**
     * RE-H-03's bound: a member that keeps many more collected tombstones than the cap re-offers them all. The
     * acknowledgements stay within {@link ReplicatedSpace#REFUSED_ACK_CAP}'s 8192 lines, the hold table with them, and
     * every line is forgotten once its hold passes.
     */
    @Test
    void acknowledgementsOfCollectedTombstonesStayWithinTheCap() throws Exception {
        ReplicatedSpace old = space("a", 1).maxLease(Duration.ofHours(1)).build();
        ReplicatedSpace fresh = space("b", 2).build();
        int count = 8192 + 40;
        for (int i = 0; i < count; i++) {
            old.write(new TaskEntry("t" + i, i), Lease.of(Duration.ofMinutes(1))).cancel();
        }
        // The old member keeps them two hours; the fresh one, whose longest lease is a minute, ten minutes past the
        // lease, the drift floor of SPEC §7.3 (2 x maxLease alone would be two).
        clock.advance(Duration.ofMinutes(12));

        assertThat(fresh.applySyncDelta(old.deltaFor(fresh.digest()))).as("all refused at the door").isFalse();
        assertThat(fresh.knownEntries()).isZero();
        assertThat(fresh.collectedReoffersAcknowledged()).isEqualTo(count);
        assertThat(ackLines(fresh)).as("acknowledged lines stay within the cap").isEqualTo(8192);
        java.lang.reflect.Field holds = ReplicatedSpace.class.getDeclaredField("collectedAcks");
        holds.setAccessible(true);
        assertThat(((Map<?, ?>) holds.get(fresh)).size()).as("the hold table is bounded by the same cap")
                .isLessThanOrEqualTo(8192);
        assertThat(old.deltaFor(fresh.digest())).as("only the evicted forty are offered again").isNotEmpty();

        clock.advance(Duration.ofMillis(ReplicatedSpace.COLLECTED_ACK_HOLD_MILLIS + 1_000));
        fresh.sweepIfDue();
        assertThat(ackLines(fresh)).as("every hold passed").isZero();
        assertThat(((Map<?, ?>) holds.get(fresh)).size()).isZero();
    }

    /**
     * The acknowledgement is only of a state the receiver would refuse anyway. A replica that acknowledged a
     * withdrawal and then learns a longer maximum lease (so the same withdrawal is no longer past its horizon) must
     * still converge with the old member once the hold passes, and never hold the entry present.
     */
    @Test
    void anAcknowledgedWithdrawalConvergesAfterTheReceiverLearnsALongerLease() throws IOException {
        ReplicatedSpace old = space("a", 1).maxLease(Duration.ofHours(1)).build();
        ReplicatedSpace fresh = space("b", 2).build();
        EntryHandle gone = old.write(new TaskEntry("gone", 1), Lease.of(Duration.ofMinutes(1)));
        gone.cancel();
        clock.advance(Duration.ofMinutes(12)); // past the fresh replica's horizon, the lease plus the drift floor
        fresh.applySyncDelta(old.deltaFor(fresh.digest()));
        assertThat(fresh.collectedReoffersAcknowledged()).isEqualTo(1);

        // A long lease arrives: the fresh replica's maximum lease is now an hour, so a withdrawal eleven minutes past
        // its lease is inside its horizon again.
        old.write(new TaskEntry("long", 2), Lease.of(Duration.ofHours(1)));
        fresh.applySyncDelta(old.deltaFor(fresh.digest()));
        assertThat(fresh.read(topic("long"))).isPresent();

        clock.advance(Duration.ofMillis(ReplicatedSpace.COLLECTED_ACK_HOLD_MILLIS + 1_000));
        fresh.sweepIfDue();
        sync(old, fresh);
        assertThat(fresh.read(topic("gone"))).isEmpty();
        assertThat(fresh.digest()).as("the withdrawal, re-offered after the hold, is now kept: in step")
                .isEqualTo(old.digest());
    }
}
