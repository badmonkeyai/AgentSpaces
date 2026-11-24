/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.capabilities.aggregate;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Push-sum on time on a started loop (srad/agentspaces-event-loop.md EL-T-03, EL-T-16; owner answers 1 and 4,
 * 2026-11-22): an active epoch gossips once a {@code ROUND_PERIOD}, goes silent 30 s after its estimate last moved,
 * is forgotten after an hour, and a watch whose settle span is longer than the quiet stretch fires at the estimate's
 * last move plus the span with no further frame.
 */
@Timeout(180)
class PushSumDueTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPushSumDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zPushSumDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "push-sum-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PushSumAggregate member(String name, String seed) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        PushSumAggregate aggregate = new PushSumAggregate(new CapabilityPipes(runtime, codec), runtime.sampler(),
                identity.peerId(), codec, time);
        capabilities.register(aggregate);
        node.startTicking(Duration.ofMillis(250));
        return aggregate;
    }

    @Test
    void anActiveEpochGossipsOnceASecondGoesSilentAndIsForgottenAndALongSettleFiresOnTheQuietEpoch()
            throws Exception {
        PushSumAggregate a = member("a", null);
        PushSumAggregate b = member("b", "a");
        time.advance(Duration.ofSeconds(10)); // the view forms and both cards spread
        List<PushSumAggregate.Estimate> fired = new CopyOnWriteArrayList<>();
        long[] firedAt = {0};
        a.onEstimate(id -> id.equals("v"), PushSumAggregate.Settle.after(Duration.ofSeconds(40), 0.001), e -> {
            firedAt[0] = time.monoNanos();
            fired.add(e);
        });
        a.start("v", 10.0);
        b.start("v", 20.0);
        long started = time.monoNanos();
        long stepsBefore = a.timedSteps();
        time.advance(Duration.ofSeconds(20));
        long active = a.timedSteps() - stepsBefore;
        System.out.printf("[push-sum-due] steps in 20 s while active: %d%n", active);
        assertThat(active).as("one round a second").isBetween(19L, 21L);
        assertThat(a.estimate("v").orElseThrow()).isCloseTo(15.0, org.assertj.core.data.Offset.offset(1e-3));

        time.advance(Duration.ofSeconds(60));
        assertThat(fired).as("EL-T-16: the 40 s settle fired on the quiet epoch, no frame needed").hasSize(1);
        long settleMillis = (firedAt[0] - started) / 1_000_000;
        System.out.printf("[push-sum-due] the 40 s watch fired %d ms after the epoch started%n", settleMillis);
        assertThat(a.quiet("v")).as("silent 30 s after its last move").isTrue();
        long quietSteps = a.timedSteps();
        time.advance(Duration.ofMinutes(10));
        assertThat(a.timedSteps() - quietSteps).as("no round while everything is quiet").isZero();
        time.advance(Duration.ofMinutes(55));
        assertThat(a.estimate("v")).as("forgotten an hour after it settled").isEmpty();
        System.out.printf("[push-sum-due] steps over the quiet hour: %d%n", a.timedSteps() - quietSteps);
    }
    /**
     * No watch: an epoch started between two steps has no quiet record yet, and the participant used to sleep to NEVER
     * with it, so nothing ever gossiped (the starter's {@code theStarterDrivesAggregateToConvergenceWithNoApplicationClock}
     * found it; the test above registers a watch, which kept the rounds going and hid it).
     */
    @Test
    void anEpochStartedBetweenStepsWithNoWatchGossipsAndConverges() throws Exception {
        PushSumAggregate a = member("a", null);
        PushSumAggregate b = member("b", "a");
        time.advance(Duration.ofSeconds(10)); // both participants have run with no epoch: they sleep to NEVER
        a.start("w", 10.0);
        b.start("w", 30.0);
        time.advance(Duration.ofSeconds(20));
        assertThat(a.estimate("w").orElseThrow()).isCloseTo(20.0, org.assertj.core.data.Offset.offset(1e-3));
        assertThat(b.estimate("w").orElseThrow()).isCloseTo(20.0, org.assertj.core.data.Offset.offset(1e-3));
        for (PeerNode node : nodes) {
            assertThat(node.loopStats().backstopFoundBy()).as("no missed wake").isEmpty();
        }
    }

    /**
     * A lone member (the Micronaut passthrough host, srad/agentspaces-event-loop.md §10.16): its epoch has nobody to
     * gossip with, and a two-round settle must still fire on it.
     */
    @Test
    void aLoneMembersEpochSettlesAndItsWatchFires() throws Exception {
        PushSumAggregate a = member("a", null);
        time.advance(Duration.ofSeconds(5));
        List<PushSumAggregate.Estimate> fired = new CopyOnWriteArrayList<>();
        a.onEstimate(id -> id.startsWith("load:"), PushSumAggregate.Settle.after(Duration.ofSeconds(2), 0.01),
                fired::add);
        a.start("load:cpu", 7.0);
        time.advance(Duration.ofSeconds(30));
        System.out.printf("[push-sum-due] lone member: %d timed steps, fired %s%n", a.timedSteps(), fired);
        assertThat(fired).as("the settle fired on the lone member's epoch").hasSize(1);
        assertThat(fired.get(0).value()).isCloseTo(7.0, org.assertj.core.data.Offset.offset(1e-6));
    }

    /**
     * The wake comes after the write (srad/agentspaces-event-loop.md §10.16). A driver whose wake runs the participant
     * at once stands for a loop thread that runs between a wake and the write that follows it. Before the fix the
     * accessors woke before they created the state, so that run saw no epoch, slept to NEVER with its wake spent, and
     * the epoch never stepped (the Micronaut host's lone-member settle, two failures in three runs).
     */
    @Test
    void aWakeRunsTheParticipantOnlyAfterTheStateItAnnouncesIsWritten() throws Exception {
        VirtualTime clock = time;
        PushSumAggregate aggregate = member("a", null);
        time.advance(Duration.ofSeconds(5)); // its participant ran with no epoch and sleeps to NEVER
        List<Long> nextDue = new CopyOnWriteArrayList<>();
        aggregate.attach(new ai.badmonkey.agentspaces.api.spi.DueWork.Driver() {
            @Override
            public void wake() {
                nextDue.add(aggregate.run(clock.monoNanos())); // the loop runs at once, as another thread may
            }

            @Override
            public void describeChanged() {
                wake();
            }

            @Override
            public long monoNanos() {
                return clock.monoNanos();
            }

            @Override
            public long dueAt(long wallMillis) {
                return clock.monoNanos() + (wallMillis - clock.millis()) * 1_000_000L;
            }

            @Override
            public boolean timed() {
                return true;
            }

            @Override
            public ai.badmonkey.agentspaces.common.time.TimeSource time() {
                return clock;
            }
        });
        aggregate.start("lone", 7.0);
        assertThat(nextDue).as("one wake per start").isNotEmpty();
        assertThat(nextDue.get(nextDue.size() - 1)).as("the run the wake started saw the epoch and keeps stepping")
                .isNotEqualTo(ai.badmonkey.agentspaces.api.spi.DueWork.NEVER);
    }
}
