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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The replicated space as one due participant on a started loop (srad/agentspaces-event-loop.md EL-T-09, EL-L-03):
 * every node runs its own loop on one virtual time and nobody calls {@code tick()}. The space looks under its lock only
 * at its deadlines, and every deadline that moves earlier wakes it, so the loop's backstop never finds work
 * (EL-Q-REL-03).
 */
@Timeout(120)
class SpaceDueWorkTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zSpaceDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSpaceDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "space-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Member member(String name, String seed) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker").clock(time)
                .settleWindow(Duration.ZERO).build();
        node.startTicking(TICK);
        nodes.add(node);
        return new Member(node, runtime, space);
    }

    private void form(Member... members) {
        long waited = 0;
        while (!java.util.Arrays.stream(members).allMatch(m -> m.runtime().membership().allMembers().size()
                == members.length - 1)) { // the view holds the others
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the group forms within 60 s").isLessThan(60_000);
        }
    }

    private static long found(Member m) {
        return m.node().loopStats().backstopFoundWork();
    }

    @Test
    void anIdleSpaceWithLongLeasesLooksOnlyWhenTheViewMoves() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        form(a, b);
        for (int i = 0; i < 5; i++) {
            a.space().write(new TaskEntry("t" + i, i), Lease.of(Duration.ofHours(2)));
        }
        time.advance(Duration.ofMinutes(2)); // the entries spread and the group goes quiet
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(5);
        long looksA = a.space().dueLooks();
        long looksB = b.space().dueLooks();
        time.advance(Duration.ofMinutes(10));
        System.out.printf("[space-due] idle 10 min: looks a=%d b=%d, backstop found a=%d b=%d%n",
                a.space().dueLooks() - looksA, b.space().dueLooks() - looksB, found(a), found(b));
        assertThat(a.space().dueLooks() - looksA).as("no deadline falls due in 10 minutes of 2 h leases").isZero();
        assertThat(b.space().dueLooks() - looksB).isZero();
        assertThat(found(a)).isZero();
        assertThat(found(b)).isZero();
    }

    @Test
    void aShortLeaseMergedByGossipLapsesOnTimeWithNoBackstopFinding() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        form(a, b);
        time.advance(Duration.ofMinutes(1));
        List<Object> lapsed = new java.util.concurrent.CopyOnWriteArrayList<>();
        b.space().notify(Template.of(TaskEntry.class), e -> {
            if (e.kind() == ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.EXPIRED) {
                lapsed.add(e);
            }
        }, Lease.of(Duration.ofHours(1)));
        long wrote = time.millis();
        a.space().write(new TaskEntry("short", 1), Lease.of(Duration.ofSeconds(5)));
        long waited = 0;
        while (b.space().readAll(Template.of(TaskEntry.class), 10).isEmpty()) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
            assertThat(waited).as("the RUMOR lands").isLessThan(2_000);
        }
        while (lapsed.isEmpty()) {
            time.advance(Duration.ofMillis(10));
            assertThat(time.millis() - wrote).as("the lapse fires at its deadline").isLessThan(7_000);
        }
        long at = time.millis() - wrote;
        System.out.printf("[space-due] a 5 s lease merged by RUMOR lapsed on b after %d ms%n", at);
        assertThat(at).isBetween(5_000L, 5_000L + Duration.ofSeconds(1).toMillis());
        assertThat(found(b)).as("the merge's own wake filed the lapse, not the backstop").isZero();
        assertThat(b.node().loopStats().backstopFoundBy()).isEmpty();
    }

    /**
     * The seam the content swarm's manifest repair stands on (srad/agentspaces-event-loop.md §10.14, the phase 2 wire
     * adversary; owner answer 5): a lapse moves the space's change signal, so a participant that listens to it hears
     * of a lapsed manifest at the lapse. Before the fix the sweep signalled a reappearance, an orphan or a collection
     * but not an expiry, so a lapsed manifest was repaired only at the 5-minute scan or the collection after the
     * 5-minute grace.
     */
    @Test
    void aLapseMovesTheChangeSignalAtTheLapse() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        form(a, b);
        time.advance(Duration.ofMinutes(1));
        long wrote = time.millis();
        a.space().write(new TaskEntry("short", 1), Lease.of(Duration.ofSeconds(5)));
        long waited = 0;
        while (b.space().readAll(Template.of(TaskEntry.class), 10).isEmpty()) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
            assertThat(waited).as("the RUMOR lands").isLessThan(2_000);
        }
        time.advance(Duration.ofSeconds(1)); // the write's own signals are over
        java.util.concurrent.atomic.AtomicLong signalledAt = new java.util.concurrent.atomic.AtomicLong(-1);
        try (AutoCloseable listening = b.space().onChangeSignal(() -> signalledAt.compareAndSet(-1, time.millis()))) {
            while (!b.space().readAll(Template.of(TaskEntry.class), 10).isEmpty()) {
                time.advance(Duration.ofMillis(10));
                assertThat(time.millis() - wrote).as("the entry lapses").isLessThan(7_000);
            }
            time.advance(Duration.ofMillis(10));
            assertThat(signalledAt.get()).as("the lapse moved the change signal").isNotNegative();
            assertThat(signalledAt.get() - wrote).as("at the lapse, not at the collection")
                    .isBetween(5_000L, 6_000L);
        }
    }

    @Test
    void theHorizonEngagesOnItsOwnRoundsWithNoDigestTraffic() throws Exception {
        Member a = member("a", null);
        Member b = member("b", "a");
        form(a, b);
        long waited = 0;
        while (a.space().horizonMode() != ReplicatedSpace.HorizonMode.SETTLING
                && a.space().horizonMode() != ReplicatedSpace.HorizonMode.PER_ENTRY) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the horizon engages within its rounds").isLessThan(30_000);
        }
        System.out.printf("[space-due] horizon engaged %d ms after the view formed%n", waited);
        assertThat(found(a)).isZero();
    }
}
