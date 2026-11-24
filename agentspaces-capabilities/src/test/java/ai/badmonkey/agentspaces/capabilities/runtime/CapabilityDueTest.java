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
package ai.badmonkey.agentspaces.capabilities.runtime;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.DueCapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.DueWork;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Capability providers as due work on a started loop (srad/agentspaces-event-loop.md EL-T-05, owner answer 3,
 * 2026-11-22): a provider that overrides neither {@code tick()} nor {@code requiresTick()} never runs and its card is
 * not republished between refreshes; a {@link DueCapabilityProvider} runs at its own deadline and its card is
 * republished in the run after it says {@code describeChanged()}; a legacy ticking provider keeps its 250 ms step.
 */
@Timeout(120)
class CapabilityDueTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zCapDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zCapDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "cap-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final CborCodec codec = CborCodec.defaultCodec();
    private PeerNode node;
    private PeerIdentity identity;
    private CapabilityRuntime capabilities;

    @AfterEach
    void tearDown() {
        if (capabilities != null) {
            capabilities.close();
        }
        if (node != null) {
            node.close();
        }
    }

    private void start() throws Exception {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(time).time(time).randomSeed(7).build();
        node.listen(network.register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, identity.peerId());
        capabilities = new CapabilityRuntime(runtime, discovery, identity);
        node.startTicking(Duration.ofMillis(250));
        time.advance(Duration.ofSeconds(1));
    }

    private CapabilityAdvertisement card(String type, String role) {
        return new CapabilityAdvertisement("aspace://" + groupId.value() + "/cap/" + type, identity.peerId(), groupId,
                time.instant(), Duration.ofMinutes(15), "aspace:cap/" + type, "0.1", "pipe", Map.of("role", role),
                Map.of());
    }

    /** Like {@code VoteCapability}: overrides neither tick() nor requiresTick(), and describes a static card. */
    private final class StaticProvider implements CapabilityProvider {
        final AtomicInteger describes = new AtomicInteger();

        @Override
        public String capabilityType() {
            return "aspace:cap/static";
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            describes.incrementAndGet();
            return card("static", "voter");
        }
    }

    /** A third-party provider on the legacy step: it overrides tick(). */
    private final class LegacyProvider implements CapabilityProvider {
        final AtomicInteger ticks = new AtomicInteger();

        @Override
        public String capabilityType() {
            return "aspace:cap/legacy";
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return card("legacy", "x");
        }

        @Override
        public void tick() {
            ticks.incrementAndGet();
        }
    }

    /** A provider with one deadline and a role the test moves. */
    private final class TimedProvider implements DueCapabilityProvider {
        volatile String role = "follower";
        volatile long dueNanos = DueWork.NEVER;
        final AtomicInteger runs = new AtomicInteger();
        final AtomicInteger describes = new AtomicInteger();
        DueWork.Driver driver;

        @Override
        public void attach(DueWork.Driver driver) {
            this.driver = driver;
        }

        @Override
        public long run(long nowNanos) {
            runs.incrementAndGet();
            if (nowNanos >= dueNanos) {
                dueNanos = DueWork.NEVER;
            }
            return dueNanos;
        }

        @Override
        public String capabilityType() {
            return "aspace:cap/timed";
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            describes.incrementAndGet();
            return card("timed", role);
        }
    }

    private String publishedRole(String type) {
        return capabilities.providersOf("aspace:cap/" + type).get(0).parameters().get("role");
    }

    @Test
    void aStaticProviderNeverRunsAndIsNotRepublishedBetweenRefreshes() throws Exception {
        start();
        StaticProvider vote = new StaticProvider();
        assertThat(CapabilityRuntime.driveOf(vote)).isEqualTo(CapabilityRuntime.Drive.STATIC);
        capabilities.register(vote);
        time.advance(Duration.ofSeconds(1));
        int described = vote.describes.get();
        time.advance(Duration.ofMinutes(1));
        assertThat(vote.describes.get()).as("not described, so not fingerprinted or republished, in a minute")
                .isEqualTo(described);
        capabilities.refreshTick();
        assertThat(vote.describes.get()).as("the refresh republishes it").isEqualTo(described + 1);
        assertThat(node.loopStats().backstopFoundWork()).isZero();
    }

    @Test
    void aDueProviderRunsAtItsDeadlineAndIsRepublishedInTheRunAfterItSaysItChanged() throws Exception {
        start();
        TimedProvider provider = new TimedProvider();
        capabilities.register(provider);
        time.advance(Duration.ofSeconds(1));
        assertThat(provider.driver.timed()).isTrue();
        int runs = provider.runs.get();
        time.advance(Duration.ofMinutes(1));
        int backstopRuns = provider.runs.get() - runs;
        int describes = provider.describes.get();

        provider.dueNanos = provider.driver.monoNanos() + Duration.ofSeconds(3).toNanos();
        provider.driver.wake();
        time.advance(Duration.ofMillis(10));
        long armed = time.monoNanos();
        int before = provider.runs.get();
        time.advance(Duration.ofSeconds(4));
        assertThat(provider.dueNanos).as("ran at its deadline").isEqualTo(DueWork.NEVER);
        assertThat(provider.describes.get()).as("no change said, no describe").isEqualTo(describes);

        time.advance(Duration.ofSeconds(1));
        provider.role = "leader";
        provider.driver.describeChanged();
        time.advance(Duration.ofMillis(1));
        assertThat(publishedRole("timed")).as("republished in the run after describeChanged").isEqualTo("leader");
        assertThat(node.loopStats().backstopFoundWork()).isZero();
        System.out.printf("[cap-due] a due provider: %d runs a minute idle (backstop sweeps), %d after its wake%n",
                backstopRuns, provider.runs.get() - before);
        assertThat(armed).isPositive();
    }

    /**
     * Wire adversary of phase 2 (srad/agentspaces-event-loop.md §10.14): two changes of a card in one millisecond (a
     * Raft candidate's card, then its winner's; a key ring's epoch, then its fetched neighbour) both reach discovery.
     * Before the fix the second card carried the first one's issue instant: the cache keeps the version it holds on an
     * equal instant, the gossip item id (the card's id and issue millisecond) was already seen everywhere, and the
     * runtime had recorded the new fingerprint as published, so the change was lost until the next refresh.
     */
    @Test
    void twoChangesOfACardInOneMillisecondBothReachDiscovery() throws Exception {
        start();
        TimedProvider provider = new TimedProvider();
        capabilities.register(provider);
        time.advance(Duration.ofSeconds(1));
        provider.role = "candidate";
        provider.driver.describeChanged();
        time.runDue();
        assertThat(publishedRole("timed")).isEqualTo("candidate");
        provider.role = "leader";
        provider.driver.describeChanged();
        time.runDue(); // the same virtual instant
        assertThat(publishedRole("timed")).as("the second change in the same millisecond").isEqualTo("leader");
        time.advance(Duration.ofMillis(1));
        provider.role = "follower";
        provider.driver.describeChanged();
        time.runDue();
        assertThat(publishedRole("timed")).as("and the next, a millisecond on").isEqualTo("follower");
    }

    @Test
    void aLegacyTickingProviderKeepsItsQuarterSecondStep() throws Exception {
        start();
        LegacyProvider legacy = new LegacyProvider();
        assertThat(CapabilityRuntime.driveOf(legacy)).isEqualTo(CapabilityRuntime.Drive.LEGACY);
        capabilities.register(legacy);
        time.advance(Duration.ofSeconds(1));
        int ticks = legacy.ticks.get();
        time.advance(Duration.ofSeconds(10));
        assertThat(legacy.ticks.get() - ticks).as("every 250 ms").isBetween(39, 41);
    }
}
