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
package ai.badmonkey.agentspaces.capabilities;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor;
import ai.badmonkey.agentspaces.capabilities.learn.GossipLearning;
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
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Layer 4 clock (QA3 A3-1 through A3-4). A capability is driven because it
 * is registered, exactly as a replicated space converges because it is
 * constructed: the capability runtime joins the group's peer tick, and the peer
 * tick advances every registered provider's protocol alongside membership and
 * anti-entropy. These tests pin that wiring, the honesty of a capability that
 * has not been driven yet, and the truthfulness of a cadence-derived
 * advertisement.
 */
class CapabilityClockTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zClock");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zClock", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "clock",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, GroupRuntime runtime, PeerIdentity identity,
                        DiscoveryService discovery, CapabilityRuntime capabilities,
                        PushSumAggregate aggregate) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        PushSumAggregate aggregate = new PushSumAggregate(
                new CapabilityPipes(runtime, codec), runtime.sampler(),
                identity.peerId(), codec, clock);
        nodes.add(node);
        return new Peer(node, runtime, identity, discovery, capabilities, aggregate);
    }

    /** Drives only the peer clock. No capability is ticked by hand anywhere here. */
    private void tickPeersOnly(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    // ------------------------------------------------- A3-1: one clock, auto-wired

    /** QA3 A3-1: a registered capability converges on the peer tick alone, with no application scheduler. */
    @Test
    void registeredCapabilitiesConvergeOnThePeerTickWithNoApplicationDriver() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        Peer d = newPeer("d", 4, "a");
        List<Peer> fleet = List.of(a, b, c, d);
        tickPeersOnly(5);

        fleet.forEach(p -> p.capabilities().register(p.aggregate()));
        a.aggregate().start("backlog", 2.0);
        b.aggregate().start("backlog", 4.0);
        c.aggregate().start("backlog", 6.0);
        d.aggregate().start("backlog", 8.0);   // fleet average is 5

        // The only thing driven is the node. Before this change, nothing would
        // ever have exchanged and every peer would have reported its own value.
        tickPeersOnly(60);

        for (Peer peer : fleet) {
            assertThat(peer.aggregate().estimate("backlog"))
                    .as("every member converges, driven by the peer tick alone").isPresent();
            assertThat(peer.aggregate().estimate("backlog").orElseThrow())
                    .isCloseTo(5.0, within(1e-3));
        }
    }

    /** QA3 A3-1: the capability runtime joins the group's tick registry on construction and leaves it on close. */
    @Test
    void theCapabilityRuntimeJoinsAndLeavesThePeerTick() throws Exception {
        Peer a = newPeer("a", 1);

        assertThat(a.capabilities().isDrivenByPeerTick()).isTrue();
        assertThat(a.runtime().tickWorkCount()).isEqualTo(1);

        a.capabilities().close();

        assertThat(a.capabilities().isDrivenByPeerTick()).isFalse();
        assertThat(a.runtime().tickWorkCount()).isZero();
    }

    /** QA3 A3-1: a detached runtime is driven only by its host, which is what a deterministic test wants. */
    @Test
    void detachingHandsTheClockBackToTheHost() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickPeersOnly(5);

        a.capabilities().detachFromPeerTick().register(a.aggregate());
        b.capabilities().detachFromPeerTick().register(b.aggregate());
        assertThat(a.capabilities().isDrivenByPeerTick()).isFalse();
        assertThat(a.runtime().tickWorkCount()).isZero();

        a.aggregate().start("v", 10.0);
        b.aggregate().start("v", 20.0);
        tickPeersOnly(40);

        assertThat(a.aggregate().estimate("v"))
                .as("detached: the peer tick moves membership but not the capability")
                .isEmpty();

        for (int i = 0; i < 40; i++) {
            a.capabilities().protocolTick();
            b.capabilities().protocolTick();
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }

        assertThat(a.aggregate().estimate("v").orElseThrow()).isCloseTo(15.0, within(1e-3));
    }

    /** QA3 A3-1: one provider that throws in tick() neither stops its siblings nor kills the peer tick. */
    @Test
    void aThrowingProviderDoesNotStopTheOthersOrThePeerTick() throws Exception {
        Peer a = newPeer("a", 1);
        tickPeersOnly(2);

        AtomicInteger goodTicks = new AtomicInteger();
        a.capabilities().register(new CountingProvider(a.identity(), "aspace:cap/good",
                goodTicks, false));
        a.capabilities().register(new CountingProvider(a.identity(), "aspace:cap/bad",
                new AtomicInteger(), true));

        long membershipBefore = a.runtime().membership().allMembers().size();
        tickPeersOnly(5);

        assertThat(goodTicks.get()).as("the well-behaved sibling kept ticking").isGreaterThanOrEqualTo(5);
        assertThat(a.runtime().tickWorkCount()).as("the registration survived").isEqualTo(1);
        assertThat(a.runtime().membership().allMembers()).hasSizeGreaterThanOrEqualTo((int) membershipBefore);
    }

    /** Periodic work registered on a group runtime is driven in order and deregisters cleanly. */
    @Test
    void groupRuntimeTickWorkRunsAndDeregisters() throws Exception {
        Peer a = newPeer("a", 1);
        AtomicInteger runs = new AtomicInteger();

        AutoCloseable registration = a.runtime().onTick(runs::incrementAndGet);
        assertThat(a.runtime().tickWorkCount()).isEqualTo(2); // ours plus the capability runtime
        tickPeersOnly(3);
        assertThat(runs.get()).isEqualTo(3);

        registration.close();
        tickPeersOnly(3);
        assertThat(runs.get()).as("deregistered work stops running").isEqualTo(3);
    }

    // ------------------------------------------------- A3-2: untickled honesty

    /** QA3 A3-2: an aggregate that has never exchanged refuses to answer instead of reporting its own seed as the fleet figure. */
    @Test
    void anUndrivenAggregateReturnsEmptyRatherThanItsOwnLocalValue() throws Exception {
        Peer a = newPeer("a", 1);
        a.capabilities().detachFromPeerTick().register(a.aggregate());   // nothing drives it
        a.aggregate().start("backlog", 42.0);

        assertThat(a.aggregate().estimate("backlog"))
                .as("the local seed is not a fleet average")
                .isEmpty();

        a.aggregate().start("headcount", PushSumAggregate.Mode.COUNT, 1.0);
        assertThat(a.aggregate().estimate("headcount"))
                .as("COUNT must not answer 'one member' before it has counted")
                .isEmpty();

        // One round of its own is participation enough: a fleet of one is
        // legitimately its own average.
        a.capabilities().protocolTick();
        assertThat(a.aggregate().estimate("backlog").orElseThrow()).isCloseTo(42.0, within(1e-9));
    }

    /** QA3 A3-2: receiving a peer's contribution is participation too, even on a node that never ticks itself. */
    @Test
    void receivingAContributionCountsAsParticipation() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickPeersOnly(5);
        a.capabilities().register(a.aggregate());
        b.capabilities().detachFromPeerTick().register(b.aggregate());

        a.aggregate().start("v", 4.0);
        b.aggregate().start("v", 8.0);
        assertThat(b.aggregate().estimate("v")).isEmpty();

        tickPeersOnly(30);   // only a is driven; its shares reach b

        assertThat(b.aggregate().estimate("v"))
                .as("b holds more than its own seed once a contribution arrives")
                .isPresent();
    }

    // ------------------------------------------------- requiresTick declarations

    /** Continuous capabilities declare that they need a clock; request/response ones declare that they do not. */
    @Test
    void capabilitiesDeclareWhetherTheyNeedAClock() throws Exception {
        Peer a = newPeer("a", 1);
        CapabilityPipes pipes = new CapabilityPipes(a.runtime(), codec);

        assertThat(a.aggregate().requiresTick()).isTrue();
        assertThat(new GossipLearning(pipes, a.runtime().sampler(), a.identity().peerId(),
                codec, clock).requiresTick()).isTrue();
        assertThat(new GroupKeyDistributor(pipes, a.identity().peerId(), codec, clock)
                .requiresTick()).isFalse();
    }

    // ------------------------------------------------- A3-4: cadence truthfulness

    /** QA3 A3-4: a hand-driven node reports no cadence, so a provider keeps the period its host declared. */
    @Test
    void aHandDrivenNodeDoesNotInventACadence() throws Exception {
        Peer a = newPeer("a", 1);

        assertThat(a.node().tickPeriod()).as("nobody knows the rate of a hand-driven tick").isEmpty();
        assertThat(a.capabilities().cadence()).isEmpty();
    }

    /** QA3 A3-4: once the node drives its own clock, the real cadence reaches every provider. */
    @Test
    void startTickingPropagatesTheRealCadenceToProviders() throws Exception {
        Peer a = newPeer("a", 1);
        RecordingProvider provider = new RecordingProvider(a.identity());
        a.capabilities().register(provider);
        assertThat(provider.cadence).as("nothing to report while hand-driven").isNull();

        a.node().startTicking(Duration.ofMillis(120));
        assertThat(a.node().tickPeriod()).hasValue(Duration.ofMillis(120));

        // The node's own scheduler will tick; give it room, then assert.
        for (int i = 0; i < 100 && provider.cadence == null; i++) {
            Thread.sleep(10);
        }
        assertThat(provider.cadence).isEqualTo(Duration.ofMillis(120));
        assertThat(a.capabilities().cadence()).hasValue(Duration.ofMillis(120));
    }

    /** QA3 A3-4: a host that drives the protocol itself declares its own cadence, and providers are told. */
    @Test
    void aDetachedHostCanDeclareItsOwnCadence() throws Exception {
        Peer a = newPeer("a", 1);
        RecordingProvider provider = new RecordingProvider(a.identity());
        a.capabilities().detachFromPeerTick().register(provider);

        a.capabilities().cadence(Duration.ofMillis(300));

        assertThat(provider.cadence).isEqualTo(Duration.ofMillis(300));
        assertThat(a.capabilities().cadence()).hasValue(Duration.ofMillis(300));
        assertThatThrownBy(() -> a.capabilities().cadence(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------- A4-10: republish on change

    /**
     * QA4 A4-10: an advertisement that describes protocol state reaches discovery
     * within one protocol tick of changing, with no advertisement refresh. Found
     * by example-12: a Raft leader that only announced itself on the five-minute
     * card refresh is a leader nobody can find.
     */
    @Test
    void aChangedDescriptionIsRepublishedOnTheNextProtocolTickWithoutARefresh() throws Exception {
        Peer a = newPeer("a", 1);
        tickPeersOnly(2);
        MutableProvider provider = new MutableProvider(a.identity(), clock);
        a.capabilities().detachFromPeerTick().register(provider);
        assertThat(a.capabilities().providersOf(provider.capabilityType()).get(0).parameters())
                .containsEntry("role", "follower");

        provider.role = "leader";                      // protocol state moved...
        clock.advance(Duration.ofSeconds(1));          // (a newer issue instant is what wins in the cache)
        assertThat(a.capabilities().providersOf(provider.capabilityType()).get(0).parameters())
                .as("nothing republished yet: no tick, no refresh")
                .containsEntry("role", "follower");

        a.capabilities().protocolTick();               // ...and one tick tells the fleet
        assertThat(a.capabilities().providersOf(provider.capabilityType()).get(0).parameters())
                .as("republished on change, on the protocol clock")
                .containsEntry("role", "leader");

        int publishesBefore = provider.describes;
        a.capabilities().protocolTick();
        a.capabilities().protocolTick();
        assertThat(a.capabilities().providersOf(provider.capabilityType())).hasSize(1);
        // Unchanged content is described (to compare) but not re-signed or re-sent
        // every tick; that stays the advertisement clock's job.
        assertThat(provider.describes).isGreaterThan(publishesBefore);
    }

    /** A provider whose described state can be changed from the test. */
    private static final class MutableProvider implements CapabilityProvider {
        private final PeerIdentity identity;
        private final java.time.InstantSource clock;
        volatile String role = "follower";
        volatile int describes;

        MutableProvider(PeerIdentity identity, java.time.InstantSource clock) {
            this.identity = identity;
            this.clock = clock;   // issued on the fixture's clock, or the TTL is already spent
        }

        @Override
        public String capabilityType() {
            return "aspace:cap/mutable";
        }

        @Override
        public boolean requiresTick() {
            return true;
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            describes++;
            return new CapabilityAdvertisement("aspace://" + group.value() + "/cap/mutable",
                    identity.peerId(), group, clock.instant(), Duration.ofMinutes(15),
                    capabilityType(), "0.1", "pipe", java.util.Map.of("role", role),
                    java.util.Map.of());
        }
    }

    /** A provider that counts its ticks, and optionally throws in every one. */
    private static final class CountingProvider implements CapabilityProvider {
        private final PeerIdentity identity;
        private final String type;
        private final AtomicInteger ticks;
        private final boolean explode;

        CountingProvider(PeerIdentity identity, String type, AtomicInteger ticks, boolean explode) {
            this.identity = identity;
            this.type = type;
            this.ticks = ticks;
            this.explode = explode;
        }

        @Override
        public String capabilityType() {
            return type;
        }

        @Override
        public boolean requiresTick() {
            return true;
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return new CapabilityAdvertisement("aspace://" + group.value() + "/cap/x/" + type,
                    identity.peerId(), group, java.time.Instant.EPOCH, Duration.ofMinutes(15),
                    type, "0.1", "pipe", java.util.Map.of(), java.util.Map.of());
        }

        @Override
        public void tick() {
            if (explode) {
                throw new IllegalStateException("this capability is broken on purpose");
            }
            ticks.incrementAndGet();
        }
    }

    /** A provider that records the cadence it was told about. */
    private static final class RecordingProvider implements CapabilityProvider {
        private final PeerIdentity identity;
        private volatile Duration cadence;

        RecordingProvider(PeerIdentity identity) {
            this.identity = identity;
        }

        @Override
        public String capabilityType() {
            return "aspace:cap/recording";
        }

        @Override
        public boolean requiresTick() {
            return true;
        }

        @Override
        public void driverCadence(Duration period) {
            this.cadence = period;
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return new CapabilityAdvertisement("aspace://" + group.value() + "/cap/recording",
                    identity.peerId(), group, java.time.Instant.EPOCH, Duration.ofMinutes(15),
                    capabilityType(), "0.1", "pipe", java.util.Map.of(), java.util.Map.of());
        }
    }
}
