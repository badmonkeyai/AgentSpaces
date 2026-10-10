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
import ai.badmonkey.agentspaces.api.space.Space;
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
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The ordered join under contention: more keys, parts written at different
 * members in both orders, three joiners racing for every ticket, and the join
 * package's debug log captured so a key that never fires can be read back as a
 * timeline. Opt-in: {@code -Djoin.stress=true}; {@code -Djoin.stress.keys=N}.
 */
@EnabledIfSystemProperty(named = "join.stress", matches = "true")
class AgentBinderJoinClusterStressTest {

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
    private final GroupId groupId = GroupId.of("zJoinStress");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zJoinStress", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "joinstress",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<String> captured = new CopyOnWriteArrayList<>();
    private final Logger joinLogger = Logger.getLogger("ai.badmonkey.agentspaces.agent.join");
    private final Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            captured.add(String.format("%tT.%<tL %s %s", new java.util.Date(record.getMillis()),
                    record.getLevel(), record.getMessage()));
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    private record Member(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                          ReplicatedSpace space, CapabilityRuntime capabilities,
                          AgentSpaces.GroupContext group) {
    }

    @AfterEach
    void tearDown() {
        joinLogger.removeHandler(handler);
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
        AgentSpaces.GroupContext group = spaces.register("joinstress", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("intake", space);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(address, identity, node, runtime, space, capabilities, group);
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
    void manyKeysUnderContentionFireOnceEachFleetWide() throws Exception {
        int keys = Integer.getInteger("join.stress.keys", 40);
        joinLogger.setLevel(Level.FINE);
        joinLogger.addHandler(handler);
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        List<Member> members = List.of(a, b, c);
        List<PeerId> memberIds = members.stream().map(m -> m.identity().peerId()).toList();
        tickAll(4);
        List<Joiner> joiners = new ArrayList<>();
        int seed = 100;
        for (Member m : members) {
            OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec,
                    m.identity(), "joiner", memberIds, clock, seed++, m.space());
            m.group().ordered("intake", ordered);
            Joiner joiner = new Joiner(m.name());
            closeables.add(m.group().bind(joiner));
            joiners.add(joiner);
        }
        for (int i = 0; i < 60 && members.stream().noneMatch(this::leads); i++) {
            tickAll(1);
        }
        assertThat(members.stream().filter(this::leads).count()).as("exactly one leader").isEqualTo(1);

        // Every key's two parts land at two different members, in both orders, with
        // a tick between bursts so completeness is first seen at different replicas.
        Member[] writers = {a, b, c};
        for (int i = 1; i <= keys; i++) {
            Member leftAt = writers[i % 3];
            Member rightAt = writers[(i + 1) % 3];
            if (i % 2 == 0) {
                leftAt.space().write(new Left("k" + i, i), Lease.of(Duration.ofMinutes(10)));
                rightAt.space().write(new Right("k" + i, "n" + i), Lease.of(Duration.ofMinutes(10)));
            } else {
                rightAt.space().write(new Right("k" + i, "n" + i), Lease.of(Duration.ofMinutes(10)));
                leftAt.space().write(new Left("k" + i, i), Lease.of(Duration.ofMinutes(10)));
            }
            if (i % 5 == 0) {
                tickAll(1);
            }
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (System.nanoTime() < deadline
                && (a.space().readAll(Template.of(Result.class), keys + 10).size() < keys
                    || !members.stream().allMatch(m -> m.space().readAll(Template.of(JoinTicket.class), 200).isEmpty()))) {
            tickAll(1);
            Thread.sleep(20);
        }
        List<Result> results = a.space().readAll(Template.of(Result.class), keys + 10);
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= keys; i++) {
            expected.add("k" + i);
        }
        List<String> firedKeys = results.stream().map(Result::caseId).toList();
        List<String> missing = expected.stream().filter(k -> !firedKeys.contains(k)).toList();
        int totalFired = joiners.stream().mapToInt(j -> j.fired.size()).sum();
        if (!missing.isEmpty() || totalFired != keys) {
            System.err.println("=== STRESS FAILURE: missing " + missing + ", fired " + totalFired + " of " + keys);
            for (String key : missing) {
                System.err.println("--- timeline of " + key);
                captured.stream().filter(line -> line.contains("'" + key + "'")).forEach(System.err::println);
                for (Member m : members) {
                    System.err.println("    at " + m.name() + ": tickets="
                            + m.space().readAllEntries(Template.of(JoinTicket.class).whereTag(JoinTicket.KEY_TAG, ai.badmonkey.agentspaces.api.space.Matchers.eq(key)), 10)
                                    .stream().map(e -> e.entryId() + "@" + e.issued()).toList()
                            + " left=" + m.space().readAll(Template.of(Left.class).where("caseId", ai.badmonkey.agentspaces.api.space.Matchers.eq(key)), 5).size()
                            + " right=" + m.space().readAll(Template.of(Right.class).where("caseId", ai.badmonkey.agentspaces.api.space.Matchers.eq(key)), 5).size()
                            + " results=" + m.space().readAll(Template.of(Result.class), keys + 10).stream().filter(r -> r.caseId().equals(key)).toList());
                }
            }
            Map<String, Long> firedBy = new java.util.TreeMap<>();
            for (Joiner j : joiners) {
                firedBy.put(j.label, (long) j.fired.size());
            }
            System.err.println("fired by joiner: " + firedBy);
            System.err.println("duplicates: " + joiners.stream().flatMap(j -> j.fired.stream())
                    .collect(java.util.stream.Collectors.groupingBy(k -> k, java.util.stream.Collectors.counting()))
                    .entrySet().stream().filter(e -> e.getValue() > 1).toList());
        }
        assertThat(missing).as("keys that never fired").isEmpty();
        assertThat(totalFired).as("every key fired exactly once across the fleet").isEqualTo(keys);
    }
}
