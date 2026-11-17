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
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
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
import ai.badmonkey.agentspaces.peering.node.PeerNodeProbe;
import ai.badmonkey.agentspaces.test.FleetProbe;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The fleet of the space scenarios (srad/spoiler-peers.md §3.8 A, B and H): members on one {@link SimNetwork} under
 * one {@link TestClock}, each with a replicated space, discovery, a capability runtime and an agent context, as an
 * application assembles them. Ticks run on the test thread and move the clock one period; takes, settles and the
 * binders' loops run on their own threads, as in a host (§3.4.4: their order is scripted, their durations measured).
 */
final class SpaceFleet implements AutoCloseable {

    /** One tick of fleet time. */
    static final Duration PERIOD = Duration.ofMillis(300);

    final TestClock clock = TestClock.create();
    final SimNetwork network = new SimNetwork();
    final CborCodec codec = CborCodec.defaultCodec();
    final GroupId groupId;
    final GroupAdvertisement groupAd;
    final String spaceName;
    /** The agent name each member's space writes as; an ordered coordinator takes under the same one. */
    String agentName = "worker";
    final Map<String, Member> members = new LinkedHashMap<>();
    /** Run after each tick on the test thread, so a script's time and counter triggers are judged there. */
    final List<Runnable> afterTick = new CopyOnWriteArrayList<>();
    /** Errors the fleet's threads raised, for the oracle. */
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    /** One member. */
    record Member(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space,
                  DiscoveryService discovery, CapabilityRuntime capabilities, AgentSpaces.GroupContext group) {
        PeerId id() {
            return identity.peerId();
        }
    }

    SpaceFleet(String name, ConflictStrategyType strategy, String spaceName) {
        this.groupId = GroupId.of("z" + name);
        this.groupAd = new GroupAdvertisement("aspace://z" + name, PeerIdentity.generate().peerId(), groupId,
                java.time.Instant.EPOCH, Duration.ofDays(1), name, GroupAdvertisement.MembershipPolicy.OPEN, strategy,
                GroupAdvertisement.GossipParameters.defaults());
        this.spaceName = spaceName;
    }

    Member member(String name, long seed, Function<ReplicatedSpace.Builder, ReplicatedSpace.Builder> space,
                  String... seeds) throws IOException {
        return member(name, seed, PeerIdentity.generate(), space, seeds);
    }

    Member member(String name, long seed, PeerIdentity identity,
                  Function<ReplicatedSpace.Builder, ReplicatedSpace.Builder> space, String... seeds) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(name), name);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace replica = space.apply(ReplicatedSpace.builder(runtime, spaceName, identity, agentName)
                .clock(clock)).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register(groupAd.name(), groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space(spaceName, replica);
        closeables.add(spaces);
        Member m = new Member(name, identity, node, runtime, replica, discovery, capabilities, group);
        members.put(name, m);
        return m;
    }

    Member get(String name) {
        return members.get(name);
    }

    List<PeerId> ids() {
        return members.values().stream().map(Member::id).toList();
    }

    void tickAll() {
        for (Member m : List.copyOf(members.values())) {
            try {
                m.node().tick();
            } catch (RuntimeException e) {
                errors.add(e);
                throw e;
            }
        }
        clock.advance(PERIOD);
        afterTick.forEach(Runnable::run);
    }

    void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            tickAll();
        }
    }

    /** Ticks with a short wall pause between ticks, so the members' own threads (takes, binders) run between them. */
    void tickSlowly() {
        tickAll();
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while ticking", e);
        }
    }

    void own(AutoCloseable closeable) {
        closeables.add(closeable);
    }

    /** The fleet as the oracle reads it: a member's space as one comparable digest of its live and completed entries. */
    FleetProbe probe(Set<String> spoilers, Template<?>... templates) {
        return new FleetProbe() {
            @Override
            public Set<String> members() {
                return members.keySet().stream().filter(n -> !spoilers.contains(n)).collect(Collectors.toCollection(TreeSet::new));
            }

            @Override
            public Map<String, String> roots(String member) {
                Space space = get(member).space();
                List<String> held = new ArrayList<>();
                for (Template<?> t : templates) {
                    space.readAll(t, 1000).forEach(e -> held.add(String.valueOf(e)));
                }
                held.sort(null);
                return Map.of("space:" + spaceName, String.join("|", held));
            }

            @Override
            public Map<String, Long> drops(String member) {
                PeerNode node = get(member).node();
                return Map.of("stale", node.framesStale(), "replay", node.framesReplayed(), "undecodable",
                        node.framesDropped(), "logical", node.framesLogicalRefused(), "farFuture", node.framesFarFuture());
            }

            @Override
            public Set<String> punished(String member) {
                PeerNode node = get(member).node();
                Set<String> out = new TreeSet<>();
                for (Member other : SpaceFleet.this.members.values()) {
                    if (!other.name().equals(member)
                            && (PeerNodeProbe.strikes(node, other.id()) > 0 || PeerNodeProbe.quarantined(node, other.id()))) {
                        out.add(other.name());
                    }
                }
                return out;
            }

            @Override
            public List<Throwable> errors(String member) {
                return List.copyOf(errors);
            }
        };
    }

    @Override
    public void close() {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort: a closed space has stopped its threads
            }
        }
        members.values().forEach(m -> m.node().close());
    }
}
