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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Joining through a {@link BeaconLocator} (SRAD DB-N-01 to DB-N-05): the
 * locator is asked at join, re-asked on an empty view once its hint has
 * aged, never faster than the minimum interval; a founding carried in the
 * hint joins by GroupId without a fetch; the advertised address is what
 * members learn, not the bind address.
 */
class LocatorJoinTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zLocator");
    private final GroupAdvertisement group = new GroupAdvertisement("aspace://zLocator",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "locator-fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private static PeerAdvertisement.Endpoint mem(String address) {
        return new PeerAdvertisement.Endpoint("mem", address, 0);
    }

    /** A locator whose answer the test changes, counting how often it is asked. */
    private static final class Movable implements BeaconLocator {
        final AtomicReference<String> address = new AtomicReference<>();
        final AtomicInteger asked = new AtomicInteger();
        final AtomicReference<BeaconRequest> lastRequest = new AtomicReference<>();
        final Duration ttl;

        Movable(String address, Duration ttl) {
            this.address.set(address);
            this.ttl = ttl;
        }

        @Override
        public String name() {
            return "movable";
        }

        @Override
        public BeaconHint locate(BeaconRequest request) {
            asked.incrementAndGet();
            lastRequest.set(request);
            return new BeaconHint(List.of(mem(address.get())), Optional.empty(), Optional.empty(),
                    Map.of(), ttl, "movable " + address.get());
        }
    }

    private void tickAll(int rounds, Duration step) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(step);
        }
    }

    private boolean settled(GroupRuntime runtime, PeerNode member) {
        return runtime.membership().member(member.peerId()).isPresent();
    }

    private void awaitAdmission(GroupRuntime runtime, PeerNode member, Duration step) throws InterruptedException {
        for (int i = 0; i < 200 && !settled(runtime, member); i++) {
            tickAll(1, step);
            Thread.sleep(5); // the re-ask runs on a virtual thread
        }
    }

    @Test
    void theLocatorIsAskedAtJoinWithWhatTheNodeKnows() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        GroupRuntime ra = a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        Movable locator = new Movable("a", Duration.ofMinutes(5));
        GroupRuntime rb = b.joinGroup(group, GroupMembership.Config.defaults(), locator);
        tickAll(6, Duration.ofSeconds(1));

        assertThat(settled(ra, b)).isTrue();
        assertThat(settled(rb, a)).isTrue();
        assertThat(locator.asked).hasValue(1);
        assertThat(locator.lastRequest.get().groupName()).isEqualTo("locator-fleet");
        assertThat(locator.lastRequest.get().groupId()).contains(groupId);
        assertThat(locator.lastRequest.get().schemes()).containsExactly("mem");
        assertThat(b.lastBeaconHint(groupId)).hasValueSatisfying(h -> assertThat(h.evidence()).isEqualTo("movable a"));
    }

    /** The seed moved: once the hint has aged, an empty view re-asks the locator and dials the new answer. */
    @Test
    void anEmptyViewReAsksTheLocatorOnceTheHintHasAged() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        GroupRuntime ra = a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        Movable locator = new Movable("nowhere", Duration.ofMinutes(1));
        GroupRuntime rb = b.joinGroup(group, GroupMembership.Config.defaults(), locator);
        tickAll(5, Duration.ofSeconds(1));
        assertThat(settled(rb, a)).as("the stale answer reaches nobody").isFalse();
        assertThat(locator.asked).as("not re-asked before the hint ages").hasValue(1);

        locator.address.set("a"); // the record moved
        awaitAdmission(rb, a, Duration.ofSeconds(20));

        assertThat(settled(rb, a)).isTrue();
        assertThat(settled(ra, b)).isTrue();
        assertThat(locator.asked.get()).as("re-asked once the hint aged, not per tick").isBetween(2, 4);
    }

    /** A stable view never re-asks, whatever the hint's TTL. */
    @Test
    void aStableViewNeverReAsks() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        Movable locator = new Movable("a", Duration.ofSeconds(1));
        GroupRuntime rb = b.joinGroup(group, GroupMembership.Config.defaults(), locator);
        tickAll(60, Duration.ofSeconds(10)); // ten minutes
        assertThat(settled(rb, a)).isTrue();
        assertThat(locator.asked).hasValue(1);
    }

    /** A hint that carries the verified founding joins by GroupId without a GROUP_AD_WANT round trip. */
    @Test
    void aFoundingInTheHintJoinsByGroupIdWithoutFetching() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement founding = GroupFounding.found(founderId, "friends",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        PeerNode founder = PeerNode.builder(founderId).clock(clock).randomSeed(1).build();
        founder.listen(network.register("f"), "f");
        nodes.add(founder);
        GroupRuntime rf = founder.joinGroup(founding, GroupMembership.Config.defaults(), List.of());
        PeerNode guest = node("g", 2);
        BeaconLocator document = new BeaconLocator() {
            @Override
            public String name() {
                return "url";
            }

            @Override
            public BeaconHint locate(BeaconRequest request) {
                return new BeaconHint(List.of(mem("f")), Optional.of(founding.advertisement().group()),
                        Optional.of(codec.toBytes(founding)), Map.of(), Duration.ofMinutes(5), "GET doc");
            }
        };
        network.hold("g", "f"); // nothing may reach the founder before the join returns
        GroupRuntime rg = guest.joinGroup(founding.advertisement().group(), GroupMembership.Config.defaults(),
                document, Duration.ofMillis(50));
        assertThat(rg.founding()).as("joined with the carried founding, no fetch needed").isPresent();
        network.release("g", "f", SimNetwork.Order.FIFO, 0);
        tickAll(6, Duration.ofSeconds(1));
        assertThat(settled(rf, guest)).isTrue();
    }

    /** A founding in the hint that is not this group's is ignored, and the seeds are asked instead. */
    @Test
    void aWrongFoundingInTheHintFallsBackToTheSeeds() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement founding = GroupFounding.found(founderId, "friends",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        SignedGroupAdvertisement other = GroupFounding.found(PeerIdentity.generate(), "friends",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        PeerNode founder = PeerNode.builder(founderId).clock(clock).randomSeed(1).build();
        founder.listen(network.register("f"), "f");
        nodes.add(founder);
        founder.joinGroup(founding, GroupMembership.Config.defaults(), List.of());
        PeerNode guest = node("g", 2);
        BeaconLocator lying = new BeaconLocator() {
            @Override
            public String name() {
                return "url";
            }

            @Override
            public BeaconHint locate(BeaconRequest request) {
                return new BeaconHint(List.of(mem("f")), Optional.empty(), Optional.of(codec.toBytes(other)),
                        Map.of(), Duration.ofMinutes(5), "GET doc");
            }
        };
        GroupRuntime rg = guest.joinGroup(founding.advertisement().group(), GroupMembership.Config.defaults(),
                lying, Duration.ofSeconds(5));
        assertThat(rg.founding()).hasValueSatisfying(f ->
                assertThat(f.advertisement().group()).isEqualTo(founding.advertisement().group()));
    }

    /** The advertised address, not the bind address, is what members learn (DB-N-05). */
    @Test
    void theAdvertisedAddressIsWhatMembersLearn() throws Exception {
        PeerNode a = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1).build();
        a.listen(network.register("a"), "a", "a.public.example:7500");
        nodes.add(a);
        PeerNode b = node("b", 2);
        a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(group, GroupMembership.Config.defaults(), List.of(mem("a")));
        tickAll(6, Duration.ofSeconds(1));

        assertThat(rb.membership().member(a.peerId())).hasValueSatisfying(m ->
                assertThat(m.endpoints()).extracting(PeerAdvertisement.Endpoint::address)
                        .containsExactly("a.public.example:7500"));
    }

    /** A chain's policy paces the re-ask (FR-3): a 10 s floor re-asks sooner than the node's 30 s. */
    @Test
    void aLocatorsPolicyPacesTheReAsk() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        GroupRuntime ra = a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        Movable inner = new Movable("nowhere", Duration.ofSeconds(1));
        ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain chain =
                ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain.builder().add(inner).expandNames(null)
                        .locatePolicy(ai.badmonkey.agentspaces.api.spi.LocatePolicy.of(Duration.ofSeconds(10), Duration.ofSeconds(20)))
                        .build();
        GroupRuntime rb = b.joinGroup(group, GroupMembership.Config.defaults(), chain);
        assertThat(b.lastBootstrapTrace(groupId)).as("the chain's rows are kept").extracting(r -> r.locator())
                .containsExactly("movable");
        inner.address.set("a");
        tickAll(12, Duration.ofSeconds(1)); // twelve seconds: past the 10 s floor, well under the node's 30 s
        for (int i = 0; i < 100 && !settled(rb, a); i++) {
            tickAll(1, Duration.ofSeconds(1));
            Thread.sleep(5);
        }
        assertThat(settled(rb, a)).isTrue();
        assertThat(inner.asked.get()).isBetween(2, 4);
    }

    /** A required chain that answers nothing refuses the join and names the group (FR-4). */
    @Test
    void aRequiredLocatorThatAnswersNothingRefusesTheJoin() throws Exception {
        PeerNode b = node("b", 2);
        ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain chain =
                ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain.builder()
                        .add(ai.badmonkey.agentspaces.peering.bootstrap.SeedsLocator.of(List.of()))
                        .expandNames(null)
                        .locatePolicy(ai.badmonkey.agentspaces.api.spi.LocatePolicy.DEFAULT.withRequire(true))
                        .build();
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                b.joinGroup(group, GroupMembership.Config.defaults(), chain, Map.of(), null, "configured-name"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("configured-name").hasMessageContaining("bootstrap.require");
        assertThat(b.group(groupId)).as("nothing was joined").isEmpty();
    }

    /** The configured name is what the locator is asked by (FR-9), not the advertisement's. */
    @Test
    void theConfiguredNameIsWhatTheLocatorIsAskedBy() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        Movable locator = new Movable("a", Duration.ofMinutes(5));
        b.joinGroup(group, GroupMembership.Config.defaults(), locator, Map.of(), null, "weekend");
        assertThat(locator.lastRequest.get().groupName()).isEqualTo("weekend");
        PeerNode c = node("c", 3);
        Movable byAd = new Movable("a", Duration.ofMinutes(5));
        c.joinGroup(group, GroupMembership.Config.defaults(), byAd);
        assertThat(byAd.lastRequest.get().groupName()).as("no configured name: the advertisement's").isEqualTo("locator-fleet");
    }

    /** A hint naming its beacon and its wire hints seeds the session expectation before the ad arrives (FR-10). */
    @Test
    void aHintsWireParametersSeedTheChannelExpectation() throws Exception {
        PeerNode a = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1)
                .channelAuth(PeerNode.ChannelAuth.SESSION).build();
        a.listen(network.register("a"), "a");
        nodes.add(a);
        PeerNode b = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(2)
                .channelAuth(PeerNode.ChannelAuth.SESSION).build();
        b.listen(network.register("b"), "b");
        nodes.add(b);
        a.joinGroup(group, GroupMembership.Config.defaults(), List.of());
        BeaconLocator document = new BeaconLocator() {
            @Override
            public String name() {
                return "url";
            }

            @Override
            public BeaconHint locate(BeaconRequest request) {
                return new BeaconHint(List.of(mem("a")), Optional.empty(), Optional.empty(),
                        Map.of("beacon", a.peerId().value(), "wire.session", "1"), Duration.ofMinutes(5), "GET doc");
            }
        };
        GroupRuntime rb = b.joinGroup(group, GroupMembership.Config.defaults(), document);
        assertThat(b.expectsSession(a.peerId())).as("expected before the ad arrived").isTrue();
        tickAll(6, Duration.ofSeconds(1));
        assertThat(settled(rb, a)).isTrue();
    }
}
