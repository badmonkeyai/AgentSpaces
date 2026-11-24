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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario 5 of srad/agentspaces-event-loop.md §2.5, a fleet test on virtual time (EL-Q-TST-03, §10.17): four nodes,
 * two spaces and a binder on one {@link VirtualTime}, node c on a view offset 200 ms. The test advances 30 s in 250 ms
 * steps; each advance runs the due tasks across every node in deadline order, releases the binder's renewer and the
 * space's settle, and returns. It then silences a member and asserts the others drop it within the band, and reads
 * {@code loopStats()} for the wake count. No {@code Thread.sleep} appears.
 */
@Timeout(180)
class VirtualFleetTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration SETTLE = Duration.ofMillis(200);
    /** The subscription lease of the binder's notify: its renewer runs every 2 s of virtual time. */
    private static final String NOTIFY_LEASE = "4s";
    /** The band's limit for a silent member's drop (core.detect.silent-member, the TCP limit at ten members). */
    private static final Duration DROP_LIMIT = Duration.ofSeconds(55);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zVirtualFleet");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zVirtualFleet",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "virtual-fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<String, Member> members = new LinkedHashMap<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    private record Member(String name, PeerIdentity identity, TimeSource time, PeerNode node, GroupRuntime runtime,
                          ReplicatedSpace work, ReplicatedSpace notes) {
    }

    @AgentSpec(name = "scribe", description = "Counts the notes it hears", goals = {"notes"})
    public static class Scribe {
        final AtomicInteger heard = new AtomicInteger();

        @SpaceNotify(space = "notes", lease = NOTIFY_LEASE)
        public void onNote(FindingEntry note) {
            heard.incrementAndGet();
        }
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        members.values().forEach(m -> m.node().close());
    }

    private Member member(String name, String seed, TimeSource source) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).time(source).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "worker").settleWindow(SETTLE).build();
        ReplicatedSpace notes = ReplicatedSpace.builder(runtime, "notes", identity, "worker").build();
        node.startTicking(TICK);
        Member m = new Member(name, identity, source, node, runtime, work, notes);
        members.put(name, m);
        return m;
    }

    private boolean formed() {
        return members.values().stream().allMatch(m -> m.runtime().membership().allMembers().size() == members.size() - 1);
    }

    private long wakes() {
        return members.values().stream().mapToLong(m -> m.node().loopStats().timerWakes()
                + m.node().loopStats().eventWakes()).sum();
    }

    @Test
    void aFleetOnOneVirtualTimeRunsItsRenewerAndSettleAndDropsASilentMemberWithinTheBand() throws Exception {
        Member a = member("a", null, time);
        Member b = member("b", "a", time);
        Member c = member("c", "a", time.offset(Duration.ofMillis(200)));
        Member d = member("d", "a", time);
        AgentSpaces agents = new AgentSpaces(a.identity(), time);
        closeables.add(agents);
        DiscoveryService discovery = new DiscoveryService(a.runtime(), new AdCache(codec, time), codec,
                a.identity().peerId());
        AgentSpaces.GroupContext group = agents.register(groupAd.name(), groupId, a.runtime(), discovery);
        group.space("notes", a.notes());
        Scribe scribe = new Scribe();
        closeables.add(group.bind(scribe));

        long wakesBefore = wakes();
        for (int step = 0; step < Duration.ofSeconds(30).dividedBy(TICK); step++) {
            time.advance(TICK);
        }
        assertThat(formed()).as("the four formed inside 30 s").isTrue();
        long wakes30s = wakes() - wakesBefore;
        System.out.printf("SCENARIO-5 wakes over the first 30 s, four nodes: %d (%.1f a minute a node)%n", wakes30s,
                wakes30s * 2.0 / members.size());
        members.values().forEach(m -> assertThat(m.node().loopStats().backstopFoundWork())
                .as(m.name() + ": every deadline that moved earlier woke its loop").isZero());

        // The binder's renewer: the 4 s subscription was renewed every 2 s of virtual time for 30 s, so a note
        // written now still reaches the scribe.
        b.notes().write(new FindingEntry("n1", "after 30 s"), Lease.of(Duration.ofHours(1)));
        time.advance(TICK);
        Measure.await("core.virtual-fleet.notify", Duration.ofSeconds(10), () -> scribe.heard.get() == 1, Wait.with());

        // The space's settle, on node c's view (offset 200 ms): exactly the window, in virtual time.
        b.work().write(new TaskEntry("job", 1), Lease.of(Duration.ofHours(1)));
        for (int step = 0; step < 40 && c.work().read(Template.of(TaskEntry.class)).isEmpty(); step++) {
            time.advance(TICK);
        }
        VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> taken = time.run(Duration.ofMillis(10),
                Duration.ofSeconds(10), () -> c.work().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)),
                        Duration.ZERO));
        assertThat(taken.value()).as("c took the job b wrote").isPresent();
        assertThat(taken.took()).as("the settle window, exactly, on c's offset view").isEqualTo(SETTLE);

        // A silent member: d's links are cut both ways; the others drop it within the band.
        for (String other : List.of("a", "b", "c")) {
            network.partition("d", other);
        }
        long silentAt = time.monoNanos();
        while (members.values().stream().filter(m -> m != d)
                .anyMatch(m -> m.runtime().membership().member(d.node().peerId()).isPresent())) {
            time.advance(TICK);
            assertThat(Duration.ofNanos(time.monoNanos() - silentAt)).as("dropped within the band's limit")
                    .isLessThanOrEqualTo(DROP_LIMIT);
        }
        Duration dropped = Duration.ofNanos(time.monoNanos() - silentAt);
        System.out.printf("SCENARIO-5 silent member dropped by all three after %d ms of virtual time; settle took %d ms%n",
                dropped.toMillis(), taken.took().toMillis());
        members.values().forEach(m -> assertThat(m.node().loopStats().backstopFoundWork()).isZero());
    }
}
