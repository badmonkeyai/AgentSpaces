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
package ai.badmonkey.agentspaces.agent;

import static org.assertj.core.api.Assertions.assertThat;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code @SpaceJoin} in {@code ORDERED} mode over a real three-member log
 * (SPEC §10.3, issue #16 §14.9): one joiner per member, parts of each key
 * written at different members, exactly one firing per key fleet-wide, and
 * every ticket drained at every replica, including one that holds no claim.
 */
class AgentBinderJoinClusterTest {

    public record Left(String caseId, int value) {
    }

    public record Right(String caseId, String note) {
    }

    public record Result(String caseId, int value, String note, String by) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zJoinLog");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zJoinLog", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "joinlog",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                          ReplicatedSpace space, CapabilityRuntime capabilities,
                          AgentSpaces.GroupContext group) {
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
        nodes.forEach(PeerNode::close);
    }

    private Member member(String address, long seed, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "intake", identity, "joiner")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("joinlog", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("intake", space);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, runtime, space, capabilities, group);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    private boolean leads(Member m) {
        return m.capabilities().providersOf(RaftLog.TYPE).stream()
                .anyMatch(ad -> "leader".equals(ad.parameters().get("role"))
                        && ad.issuer().equals(m.identity().peerId()));
    }

    @AgentSpec(name = "joiner", description = "Joins left and right once fleet-wide", goals = {"join"})
    public static class Joiner {
        final List<String> fired = new CopyOnWriteArrayList<>();
        final String label;

        Joiner(String label) {
            this.label = label;
        }

        @SpaceJoin(space = "intake", key = "caseId", mode = SpaceJoin.Mode.ORDERED,
                takeLease = "30s", pollTimeout = "200ms",
                parts = {@Part(Left.class), @Part(Right.class)})
        public Result join(Joined j) {
            fired.add(j.key());
            return new Result(j.key(), j.get(Left.class).value(), j.get(Right.class).note(), label);
        }
    }

    @Test
    void anOrderedJoinFiresOncePerKeyFleetWideAndDrainsItsTicketsEverywhere() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        Member desk = member("d", 4, "a");          // no log membership, no joiner: a bystander
        List<Member> members = List.of(a, b, c);
        List<PeerId> memberIds = members.stream().map(m -> m.identity().peerId()).toList();
        tickAll(4);
        List<Joiner> joiners = new ArrayList<>();
        int seed = 100;
        for (Member m : members) {
            OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec,
                    m.identity(), "joiner", memberIds, clock, seed++, m.space());
            m.group().ordered("intake", ordered);
            Joiner joiner = new Joiner(m.identity().peerId().display());
            closeables.add(m.group().bind(joiner));
            joiners.add(joiner);
        }
        for (int i = 0; i < 60 && members.stream().noneMatch(this::leads); i++) {
            tickAll(1);
        }
        assertThat(members.stream().filter(this::leads).count()).as("exactly one leader").isEqualTo(1);

        // Six keys, each half written at a different member, so completeness is
        // first seen at different replicas and more than one joiner may write a ticket.
        Member[] writers = {a, b, c};
        for (int i = 1; i <= 6; i++) {
            writers[i % 3].space().write(new Left("k" + i, i), Lease.of(Duration.ofMinutes(10)));
            writers[(i + 1) % 3].space().write(new Right("k" + i, "n" + i), Lease.of(Duration.ofMinutes(10)));
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline
                && (a.space().readAll(Template.of(Result.class), 10).size() < 6
                    || !members.stream().allMatch(m -> m.space().readAll(Template.of(JoinTicket.class), 20).isEmpty())
                    || !desk.space().readAll(Template.of(JoinTicket.class), 20).isEmpty())) {
            tickAll(1);
            Thread.sleep(20);
        }
        List<Result> results = a.space().readAll(Template.of(Result.class), 10);
        assertThat(results).extracting(Result::caseId)
                .containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5", "k6");
        assertThat(joiners.stream().mapToInt(j -> j.fired.size()).sum())
                .as("every key fired exactly once across the fleet").isEqualTo(6);
        for (Member m : members) {
            assertThat(m.space().readAll(Template.of(JoinTicket.class), 20))
                    .as("tickets drained at " + m.identity().peerId().display()).isEmpty();
        }
        assertThat(desk.space().readAll(Template.of(JoinTicket.class), 20))
                .as("tickets drained at the bystander, which never held a claim").isEmpty();
        assertThat(desk.space().readAll(Template.of(Result.class), 10)).hasSize(6);
    }
}
