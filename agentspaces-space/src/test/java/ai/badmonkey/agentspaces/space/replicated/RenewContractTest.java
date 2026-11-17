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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 2 of srad/replication-by-entry.md on the replicated space: a renewal never shortens a lease and needs the entry
 * present, unexpired and renewed by its issuer (RE-H-02); a renewal converges on every replica to the longer lease; the
 * lapse horizon is {@code min(2 x maxLease, 5 min)}; and a re-offered state of a collected tombstone is refused and
 * acknowledged, so the member offering it stops (RE-H-03).
 */
class RenewContractTest {

    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRenew", PeerIdentity.generate().peerId(), GroupId.of("zRenew"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "renew",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerIdentity identity, GroupRuntime runtime) {
        ReplicatedSpace.Builder space(String name, TestClock clock) {
            return ReplicatedSpace.builder(runtime, name, identity, "worker").clock(clock).settleWindow(Duration.ZERO);
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** A member on its own: the tests carry deltas between replicas by hand, so nothing gossips under them. */
    private Peer peer(String address, long seed, boolean renewShorteningLog) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed)
                .renewShorteningLog(renewShorteningLog).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        nodes.add(node);
        return new Peer(identity, runtime);
    }

    private Peer peer(String address, long seed) throws IOException {
        return peer(address, seed, true);
    }

    private static Template<TaskEntry> topic(String topic) {
        return Template.of(TaskEntry.class).where("topic", Matchers.eq(topic));
    }

    private static long expiry(ReplicatedSpace space, String topic) {
        return space.readAllEntries(topic(topic), 1).get(0).lease().expiresAtMillis();
    }

    private long now() {
        return clock.instant().toEpochMilli();
    }

    /**
     * RE-H-02: renewing for less than the lease has left keeps the longer lease and sends nothing, and renewing for
     * more extends it to now plus the extension. The take lease follows the same rule.
     */
    @Test
    void aShorterExtensionKeepsTheLongerLeaseAndALongerOneExtends() throws IOException {
        ReplicatedSpace space = peer("a", 1).space("jobs", clock).build();
        long written = now();
        EntryHandle handle = space.write(new TaskEntry("clamp", 1), MINUTES_10);
        clock.advance(Duration.ofMinutes(1));
        byte[] digest = space.digest();

        handle.renew(Duration.ofMinutes(2));
        assertThat(expiry(space, "clamp")).as("nine minutes were left; two would have shortened it")
                .isEqualTo(written + Duration.ofMinutes(10).toMillis());
        assertThat(space.digest()).as("nothing changed, so nothing new to offer").isEqualTo(digest);
        assertThat(space.renewShortenings()).isEqualTo(1);

        handle.renew(Duration.ofMinutes(30));
        assertThat(expiry(space, "clamp")).isEqualTo(now() + Duration.ofMinutes(30).toMillis());
        assertThat(space.renewShortenings()).isEqualTo(1);
        clock.advance(Duration.ofMinutes(20));
        assertThat(space.read(topic("clamp"))).as("alive at minute twenty-one").isPresent();

        space.write(new TaskEntry("held", 2), Lease.of(Duration.ofHours(1)));
        TakenEntry<TaskEntry> taken = space.take(topic("held"), MINUTES_10, Duration.ZERO).orElseThrow();
        taken.renew(Duration.ofSeconds(30));
        assertThat(space.renewShortenings()).as("a take renewal counts the same way").isEqualTo(2);
        clock.advance(Duration.ofMinutes(9));
        assertThat(space.take(topic("held"), MINUTES_10, Duration.ZERO)).as("the take kept its ten minutes")
                .isEmpty();
        taken.renew(Duration.ofMinutes(10));
        clock.advance(Duration.ofMinutes(5));
        space.complete(taken); // still held at minute fourteen of the take

        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        Subscription subscription = space.notify(topic("heard"), received::add, MINUTES_10);
        subscription.renew(Duration.ofSeconds(1));
        clock.advance(Duration.ofMinutes(9));
        space.write(new TaskEntry("heard", 3), MINUTES_10);
        assertThat(received).as("the subscription kept its ten minutes").hasSize(1);
        subscription.close();
    }

    /**
     * RE-H-02's preconditions: renewal is refused for an entry that is gone (cancelled or completed), for one whose
     * lease lapsed, and for an actor that is not the entry's issuer, each with the exception the API documents.
     */
    @Test
    void aRenewalNeedsTheEntryPresentUnexpiredAndItsIssuer() throws IOException {
        Peer peer = peer("a", 1);
        ReplicatedSpace space = peer.space("jobs", clock).build();

        EntryHandle cancelled = space.write(new TaskEntry("cancelled", 1), MINUTES_10);
        cancelled.cancel();
        assertThatThrownBy(() -> cancelled.renew(Duration.ofMinutes(5))).isInstanceOf(LeaseExpiredException.class);

        EntryHandle completed = space.write(new TaskEntry("completed", 2), MINUTES_10);
        space.complete(space.take(topic("completed"), MINUTES_10, Duration.ZERO).orElseThrow());
        assertThatThrownBy(() -> completed.renew(Duration.ofMinutes(5))).isInstanceOf(LeaseExpiredException.class);

        EntryHandle lapsed = space.write(new TaskEntry("lapsed", 3), Lease.of(Duration.ofMinutes(1)));
        clock.advance(Duration.ofMinutes(2));
        assertThatThrownBy(() -> lapsed.renew(Duration.ofMinutes(5))).isInstanceOf(LeaseExpiredException.class);

        AgentIdentity planner = peer.identity().renewingSubordinate("planner", Duration.ofDays(1), clock);
        AgentIdentity sibling = peer.identity().renewingSubordinate("sibling", Duration.ofDays(1), clock);
        EntryHandle owned = space.as(planner).write(new TaskEntry("owned", 4), MINUTES_10);
        EntryHandle other = space.handleAs(owned.entryId(), sibling);
        long before = expiry(space, "owned");
        assertThatThrownBy(() -> other.renew(Duration.ofMinutes(20))).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot act on");
        assertThat(expiry(space, "owned")).isEqualTo(before);
        owned.renew(Duration.ofMinutes(20));
        assertThat(expiry(space, "owned")).as("the issuer's own handle still renews")
                .isEqualTo(now() + Duration.ofMinutes(20).toMillis());
    }

    /**
     * A renewal converges on two replicas to the longer lease: the longer renewal reaches the replica, a shorter one
     * afterwards changes neither side, and the write's own state, delivered late, never takes the replica back to
     * the shorter lease.
     */
    @Test
    void aReplicatedRenewalConvergesOnTwoReplicasToTheLongerLease() throws IOException {
        ReplicatedSpace issuer = peer("a", 1).space("jobs", clock).build();
        ReplicatedSpace replica = peer("b", 2).space("jobs", clock).build();
        EntryHandle handle = issuer.write(new TaskEntry("lease", 1), MINUTES_10);
        byte[] asWritten = issuer.deltaFor(new byte[0]);
        assertThat(replica.applySyncDelta(asWritten)).isTrue();
        clock.advance(Duration.ofMinutes(1));

        handle.renew(Duration.ofMinutes(30));
        long longer = now() + Duration.ofMinutes(30).toMillis();
        assertThat(replica.applySyncDelta(issuer.deltaFor(replica.digest()))).isTrue();
        assertThat(expiry(issuer, "lease")).isEqualTo(longer);
        assertThat(expiry(replica, "lease")).isEqualTo(longer);

        clock.advance(Duration.ofMinutes(1));
        handle.renew(Duration.ofMinutes(5));
        assertThat(expiry(issuer, "lease")).as("the shorter renewal keeps the longer lease").isEqualTo(longer);
        assertThat(issuer.deltaFor(replica.digest())).as("the two replicas are already in step").isEmpty();
        assertThat(replica.deltaFor(issuer.digest())).isEmpty();

        assertThat(replica.applySyncDelta(asWritten)).as("the ten-minute state, delivered late, changes nothing")
                .isFalse();
        assertThat(expiry(replica, "lease")).isEqualTo(longer);
        clock.advance(Duration.ofMinutes(20));
        assertThat(issuer.read(topic("lease"))).isPresent();
        assertThat(replica.read(topic("lease"))).as("alive on both at minute twenty-two").isPresent();
    }

    /**
     * The node setting {@code agentspaces.space.renew-shortening-log=false} silences the log, and the space still
     * counts; the default logs once per space, saying the call kept the longer lease.
     */
    @Test
    void theShorteningLogFollowsTheNodeSettingAndTheCountRunsEither() throws IOException {
        List<LogRecord> logged = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(ReplicatedSpace.class.getName());
        Handler capture = new Handler() {
            @Override public void publish(LogRecord record) { logged.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(capture);
        try {
            ReplicatedSpace quiet = peer("q", 1, false).space("quiet", clock).build();
            quiet.write(new TaskEntry("x", 1), MINUTES_10).renew(Duration.ofMinutes(1));
            assertThat(quiet.renewShortenings()).isEqualTo(1);
            assertThat(logged).isEmpty();

            ReplicatedSpace loud = peer("l", 2).space("loud", clock).build();
            loud.write(new TaskEntry("y", 1), MINUTES_10).renew(Duration.ofMinutes(1));
            loud.write(new TaskEntry("z", 2), MINUTES_10).renew(Duration.ofMinutes(1));
            assertThat(loud.renewShortenings()).isEqualTo(2);
            assertThat(logged).hasSize(1);
            assertThat(logged.get(0).getMessage()).contains("space loud", "kept the longer lease");
        } finally {
            logger.removeHandler(capture);
        }
    }

    /**
     * RE-H-03: the lapse horizon is {@code min(2 x maxLease, 5 min)} past the lease, the withdrawal horizon
     * {@code 2 x maxLease}; and the renewal rule saturates rather than wrapping.
     */
    @Test
    void theLapseHorizonIsTheLesserOfTwoMaximumLeasesAndFiveMinutes() {
        assertThat(SpaceStateCrdt.LAPSED_GRACE_MILLIS).isEqualTo(Duration.ofMinutes(5).toMillis());
        assertThat(SpaceStateCrdt.lapsedKeepMillis(Duration.ofSeconds(30).toMillis()))
                .isEqualTo(Duration.ofMinutes(1).toMillis());
        assertThat(SpaceStateCrdt.lapsedKeepMillis(Duration.ofMinutes(2).toMillis()))
                .isEqualTo(Duration.ofMinutes(4).toMillis());
        assertThat(SpaceStateCrdt.lapsedKeepMillis(Duration.ofMinutes(10).toMillis()))
                .isEqualTo(Duration.ofMinutes(5).toMillis());
        assertThat(SpaceStateCrdt.lapsedKeepMillis(0)).isZero();
        assertThat(SpaceStateCrdt.lapsedKeepMillis(-1)).isZero();

        assertThat(SpaceStateCrdt.renewedExpiry(1_000, 500, 100)).as("shorter: kept").isEqualTo(1_000);
        assertThat(SpaceStateCrdt.renewedExpiry(1_000, 500, 600)).as("longer: extended").isEqualTo(1_100);
        assertThat(SpaceStateCrdt.renewedExpiry(1_000, 500, 500)).as("equal").isEqualTo(1_000);
        assertThat(SpaceStateCrdt.renewedExpiry(1_000, 500, -900)).as("negative: kept").isEqualTo(1_000);
        assertThat(SpaceStateCrdt.renewedExpiry(1_000, Long.MAX_VALUE - 10, 100)).isEqualTo(Long.MAX_VALUE);
    }

    /**
     * RE-H-03 on two replicas: one keeps tombstones longer (it saw a longer lease) and re-offers a withdrawal the
     * other already collected. The door refuses it and acknowledges the line, so the next round offers nothing; the
     * acknowledgement goes once its hold passes, and a member still holding the tombstone offers it once more.
     */
    @Test
    void aReofferedCollectedTombstoneIsRefusedAndAcknowledgedSoTheReoffersEnd() throws IOException {
        ReplicatedSpace old = peer("a", 1).space("jobs", clock).maxLease(Duration.ofHours(1)).build();
        ReplicatedSpace fresh = peer("b", 2).space("jobs", clock).build();
        EntryHandle handle = old.write(new TaskEntry("gone", 1), Lease.of(Duration.ofMinutes(1)));
        handle.cancel();
        assertThat(fresh.applySyncDelta(old.deltaFor(new byte[0]))).isTrue();
        assertThat(fresh.knownEntries()).as("the withdrawal, held as a tombstone").isEqualTo(1);

        // Past the lease plus the drift floor of SPEC §7.3, ten minutes, which a withdrawal keeps when two maximum
        // leases of one minute are shorter.
        clock.advance(Duration.ofMinutes(12));
        fresh.sweepIfDue();
        assertThat(fresh.knownEntries()).as("collected here").isZero();
        assertThat(old.knownEntries()).as("kept two hours there").isEqualTo(1);

        byte[] reoffer = old.deltaFor(fresh.digest());
        assertThat(reoffer).as("the old member offers it again").isNotEmpty();
        assertThat(fresh.applySyncDelta(reoffer)).as("refused at the door: no change, no activity").isFalse();
        assertThat(fresh.knownEntries()).isZero();
        assertThat(fresh.collectedReoffersAcknowledged()).isEqualTo(1);
        String acknowledged = "a:e:" + handle.entryId().value() + "=";
        assertThat(new String(fresh.digest(), StandardCharsets.UTF_8)).contains(acknowledged);
        assertThat(old.deltaFor(fresh.digest())).as("the re-offers end").isEmpty();

        assertThat(fresh.applySyncDelta(reoffer)).as("a re-offer already in flight").isFalse();
        assertThat(fresh.collectedReoffersAcknowledged()).as("acknowledged once").isEqualTo(1);

        clock.advance(Duration.ofMillis(ReplicatedSpace.COLLECTED_ACK_HOLD_MILLIS + 1_000));
        fresh.sweepIfDue();
        assertThat(new String(fresh.digest(), StandardCharsets.UTF_8)).as("the hold passed: the line is forgotten")
                .doesNotContain(acknowledged);
        assertThat(old.deltaFor(fresh.digest())).as("a member still holding it offers it once more").isNotEmpty();

        clock.advance(Duration.ofHours(2));
        old.sweepIfDue();
        assertThat(old.knownEntries()).as("the old member collects it at its own horizon").isZero();
        assertThat(old.deltaFor(fresh.digest())).isEmpty();
        assertThat(fresh.digest()).as("and the two replicas agree again").isEqualTo(old.digest());
    }
}
