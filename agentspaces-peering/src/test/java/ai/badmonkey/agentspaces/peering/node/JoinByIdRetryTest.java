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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.api.spi.LocatePolicy;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain;
import ai.badmonkey.agentspaces.peering.bootstrap.SeedsLocator;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A join by GroupId keeps asking while it waits (ISSUE-BootstrapRobustness
 * FR-4 to FR-6, PR-1, PR-2): a locator that answers nothing at first is
 * asked again inside the timeout, an endpoint that refused is dialed again,
 * a good first answer is asked once, a slow re-ask never extends the
 * timeout, and the report names what was tried.
 */
class JoinByIdRetryTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private static PeerAdvertisement.Endpoint mem(String address) {
        return new PeerAdvertisement.Endpoint("mem", address, 0);
    }

    /** A founder that holds a signed founding, listening at {@code f}. */
    private SignedGroupAdvertisement founder() throws IOException {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement founding = GroupFounding.found(founderId, "friends",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        PeerNode founder = PeerNode.builder(founderId).clock(clock).randomSeed(1).build();
        founder.listen(network.register("f"), "f");
        nodes.add(founder);
        founder.joinGroup(founding, GroupMembership.Config.defaults(), List.of());
        return founding;
    }

    private PeerNode guest() throws IOException {
        PeerNode guest = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(2).build();
        guest.listen(network.register("g"), "g");
        nodes.add(guest);
        return guest;
    }

    /** A locator whose answer depends on how many times it has been asked. */
    private static final class Scripted implements BeaconLocator {
        final AtomicInteger asked = new AtomicInteger();
        final IntFunction<BeaconHint> script;

        Scripted(IntFunction<BeaconHint> script) {
            this.script = script;
        }

        @Override
        public String name() {
            return "scripted";
        }

        @Override
        public BeaconHint locate(BeaconRequest request) {
            return script.apply(asked.incrementAndGet());
        }
    }

    /** FR-4: the first answer is empty, the second names the founder; the join completes inside the timeout. */
    @Test
    void anEmptyFirstAnswerIsAskedAgainInsideTheTimeout() throws Exception {
        SignedGroupAdvertisement founding = founder();
        PeerNode guest = guest();
        Scripted locator = new Scripted(n -> n == 1 ? BeaconHint.none() : BeaconHint.of(List.of(mem("f")), "late"));
        long started = System.nanoTime();
        GroupRuntime joined = guest.joinGroup(founding.advertisement().group(), GroupMembership.Config.defaults(),
                locator, Duration.ofSeconds(10));
        long millis = (System.nanoTime() - started) / 1_000_000;
        assertThat(joined.founding()).isPresent();
        assertThat(locator.asked.get()).as("asked once at join and once at the first slice").isEqualTo(2);
        assertThat(millis).as("joined at the first re-ask, well before the timeout").isLessThan(5_000);
    }

    /** PR-2: a good first answer is asked once and the join behaves as before. */
    @Test
    void aGoodFirstAnswerIsAskedOnce() throws Exception {
        SignedGroupAdvertisement founding = founder();
        PeerNode guest = guest();
        Scripted locator = new Scripted(n -> BeaconHint.of(List.of(mem("f")), "direct"));
        GroupRuntime joined = guest.joinGroup(founding.advertisement().group(), GroupMembership.Config.defaults(),
                locator, Duration.ofSeconds(10));
        assertThat(joined.founding()).isPresent();
        assertThat(locator.asked.get()).isEqualTo(1);
    }

    /** FR-4: an endpoint that refused the dial is dialed again, so a seed that starts late is reached. */
    @Test
    void anEndpointThatRefusedIsDialedAgainWhenItStartsLate() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        SignedGroupAdvertisement founding = GroupFounding.found(founderId, "friends",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults(), clock.instant(), Duration.ofDays(1));
        PeerNode guest = guest();
        Scripted locator = new Scripted(n -> BeaconHint.of(List.of(mem("f")), "seeds[1]"));
        Thread lateSeed = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(1_200); // after the first slice, before the second ends
                PeerNode founder = PeerNode.builder(founderId).clock(clock).randomSeed(1).build();
                founder.listen(network.register("f"), "f");
                synchronized (nodes) {
                    nodes.add(founder);
                }
                founder.joinGroup(founding, GroupMembership.Config.defaults(), List.of());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        GroupRuntime joined = guest.joinGroup(founding.advertisement().group(), GroupMembership.Config.defaults(),
                locator, Duration.ofSeconds(10));
        lateSeed.join();
        assertThat(joined.founding()).isPresent();
        assertThat(locator.asked.get()).as("the locator was asked again while nothing was connected")
                .isGreaterThanOrEqualTo(2);
    }

    /** FR-6: the report names the locates and the endpoints; PR-1: the locates follow the slice schedule. */
    @Test
    void theReportNamesLocatesAndEndpoints() throws Exception {
        PeerNode guest = guest();
        GroupId wanted = GroupId.of("zNobodyHasThis");
        Scripted locator = new Scripted(n -> BeaconHint.none());
        assertThatThrownBy(() -> guest.joinGroup(wanted, GroupMembership.Config.defaults(), locator,
                Duration.ofMillis(1_500)))
                .isInstanceOf(FoundingTimeoutException.class)
                .hasMessageContaining("2 locate(s)")
                .hasMessageContaining("0 of 0 endpoint(s) connected")
                .hasMessageContaining("1500 ms")
                .satisfies(e -> {
                    FoundingTimeoutException report = (FoundingTimeoutException) e;
                    assertThat(report.locates()).isEqualTo(2);
                    assertThat(report.dialed()).isZero();
                    assertThat(report.connected()).isZero();
                });
        assertThat(locator.asked.get()).as("asked at 0 s and at 1 s, then the deadline").isEqualTo(2);
        assertThat(guest.group(wanted)).isEmpty();
    }

    /** FR-6: a reached seed that holds no signed founding is reported as connected and silent. */
    @Test
    void aConnectedSeedThatSendsNothingIsReportedAsSuch() throws Exception {
        PeerNode textFounded = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1).build();
        textFounded.listen(network.register("f"), "f");
        nodes.add(textFounded);
        GroupId wanted = GroupId.of("zTextFounded");
        GroupAdvertisement ad = new GroupAdvertisement("aspace://zTextFounded", textFounded.peerId(), wanted,
                clock.instant(), Duration.ofDays(1), "text", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        textFounded.joinGroup(ad, GroupMembership.Config.defaults(), List.of());
        PeerNode guest = guest();
        Scripted locator = new Scripted(n -> BeaconHint.of(List.of(mem("f")), "seeds[1]"));
        assertThatThrownBy(() -> guest.joinGroup(wanted, GroupMembership.Config.defaults(), locator,
                Duration.ofMillis(1_200)))
                .isInstanceOf(FoundingTimeoutException.class)
                .hasMessageContaining("1 of 1 endpoint(s) connected")
                .hasMessageContaining("sent no founding")
                .satisfies(e -> assertThat(((FoundingTimeoutException) e).connected()).isEqualTo(1));
        assertThat(locator.asked.get()).as("a connected seed means no re-ask").isEqualTo(1);
    }

    /** A re-ask that takes longer than the time left never extends the timeout. */
    @Test
    void aSlowReAskDoesNotExtendTheTimeout() throws Exception {
        PeerNode guest = guest();
        GroupId wanted = GroupId.of("zSlowLocator");
        Scripted locator = new Scripted(n -> {
            if (n > 1) {
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return BeaconHint.none();
        });
        long started = System.nanoTime();
        assertThatThrownBy(() -> guest.joinGroup(wanted, GroupMembership.Config.defaults(), locator,
                Duration.ofMillis(2_000)))
                .isInstanceOf(FoundingTimeoutException.class)
                .hasMessageContaining("2 locate(s)");
        long millis = (System.nanoTime() - started) / 1_000_000;
        assertThat(millis).as("the timeout is the bound, whatever the locator does").isLessThan(3_500);
    }

    /** FR-5: an empty first answer under require refuses at once, with no slices and no re-ask. */
    @Test
    void anEmptyFirstAnswerUnderRequireRefusesWithoutRetrying() throws Exception {
        PeerNode guest = guest();
        GroupId wanted = GroupId.of("zRequired");
        Scripted empty = new Scripted(n -> BeaconHint.none());
        BootstrapChain chain = BootstrapChain.builder().add(empty).expandNames(null)
                .locatePolicy(LocatePolicy.DEFAULT.withRequire(true)).build();
        long started = System.nanoTime();
        assertThatThrownBy(() -> guest.joinGroup(wanted, GroupMembership.Config.defaults(), chain,
                Duration.ofSeconds(10), Map.of(), null, "required-fleet"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bootstrap.require");
        assertThat((System.nanoTime() - started) / 1_000_000).isLessThan(900);
        assertThat(empty.asked.get()).isEqualTo(1);
    }

    /** {@link SeedsLocator#isEmpty()}: no strings, blank strings, or an empty fixed list. */
    @Test
    void aSeedsLocatorKnowsWhenItIsEmpty() {
        assertThat(SeedsLocator.of(List.of()).isEmpty()).isTrue();
        assertThat(SeedsLocator.of(List.of("", "  ")).isEmpty()).isTrue();
        assertThat(SeedsLocator.of(List.of("127.0.0.1:1")).isEmpty()).isFalse();
        assertThat(SeedsLocator.fixed(List.of()).isEmpty()).isTrue();
        assertThat(SeedsLocator.fixed(List.of(mem("f"))).isEmpty()).isFalse();
    }
}
