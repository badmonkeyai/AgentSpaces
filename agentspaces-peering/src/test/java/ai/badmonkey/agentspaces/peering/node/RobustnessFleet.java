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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.FleetInvariants;
import ai.badmonkey.agentspaces.test.FleetProbe;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shared harness of the robustness scenarios (ROBUSTNESS.md §2): a fleet of real nodes on a
 * {@link SimNetwork}, a set-union {@link Inventory} registered with every member so anti-entropy
 * has something to converge, a per-node news log, and the invariants every scenario checks at
 * its quiet points: every live member's view is exactly the live set, every inventory is the
 * same set, no honest member is quarantined anywhere, no node holds more connections than it
 * has members, and the bounded caches stayed bounded.
 */
final class RobustnessFleet {

    /** A set-union state: converges under pulls in any order and any number. */
    static final class Inventory implements ReconcilableState {
        final ConcurrentSkipListSet<String> members = new ConcurrentSkipListSet<>();

        void add(String member) {
            members.add(member);
        }

        String digestString() {
            return String.join(",", members);
        }

        @Override
        public byte[] digest() {
            return digestString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] deltaFor(byte[] theirDigest) {
            return java.util.Arrays.equals(theirDigest, digest()) ? new byte[0] : digest();
        }

        @Override
        public void applyDelta(byte[] delta) {
            for (String member : new String(delta, StandardCharsets.UTF_8).split(",")) {
                if (!member.isEmpty()) {
                    members.add(member);
                }
            }
        }
    }

    final TestClock clock = TestClock.create();
    final SimNetwork network = new SimNetwork();
    final GroupId groupId;
    final GroupAdvertisement groupAd;
    final Map<String, PeerNode> nodes = new LinkedHashMap<>();
    final Map<String, PeerIdentity> identities = new LinkedHashMap<>();
    final Map<String, GroupRuntime> runtimes = new LinkedHashMap<>();
    final Map<String, Inventory> inventories = new LinkedHashMap<>();
    final Map<String, List<String>> news = new LinkedHashMap<>();
    private long seeds = 100;

    RobustnessFleet(String name) {
        this.groupId = GroupId.of("z" + name);
        this.groupAd = new GroupAdvertisement("aspace://z" + name, PeerIdentity.generate().peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(1), name, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    PeerNode add(String address, String seedAddress, PeerNode.ChannelAuth mode, GroupMembership.Config config)
            throws IOException {
        return add(address, seedAddress, mode, config, true);
    }

    /** With {@code lazyPush} false the member advertises no tree hint: an older node, or a client. */
    PeerNode add(String address, String seedAddress, PeerNode.ChannelAuth mode, GroupMembership.Config config,
                 boolean lazyPush) throws IOException {
        PeerIdentity identity = identities.computeIfAbsent(address, a -> PeerIdentity.generate());
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seeds++).channelAuth(mode)
                .lazyPush(lazyPush).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, config, seedAddress == null ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0)));
        Inventory inventory = new Inventory();
        runtime.gossip().reconcile("inventory", inventory);
        List<String> log = new CopyOnWriteArrayList<>();
        runtime.gossip().onStream("news", (from, id, payload) -> log.add(new String(payload, StandardCharsets.UTF_8)));
        nodes.put(address, node);
        runtimes.put(address, runtime);
        inventories.put(address, inventory);
        news.put(address, log);
        return node;
    }

    PeerNode add(String address, String seedAddress) throws IOException {
        return add(address, seedAddress, PeerNode.ChannelAuth.SESSION, GroupMembership.Config.defaults());
    }

    /** Removes a node from the fleet as a crash would: closed, never heard from again. */
    void crash(String address) {
        PeerNode node = nodes.remove(address);
        runtimes.remove(address);
        inventories.remove(address);
        news.remove(address);
        if (node != null) {
            node.close();
        }
    }

    void publish(String from, String text) {
        news.get(from).add(text);
        inventories.get(from).add(text);
        runtimes.get(from).gossip().publish("news", text, text.getBytes(StandardCharsets.UTF_8));
    }

    void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            for (PeerNode node : List.copyOf(nodes.values())) {
                node.tick();
            }
            clock.advance(Duration.ofSeconds(1));
        }
    }

    void close() {
        nodes.values().forEach(PeerNode::close);
    }

    PeerId id(String address) {
        return nodes.get(address).peerId();
    }

    /** The invariants, checked at a quiet point. */
    void assertInvariants() {
        for (Map.Entry<String, PeerNode> entry : nodes.entrySet()) {
            String address = entry.getKey();
            PeerNode node = entry.getValue();
            GroupRuntime runtime = runtimes.get(address);
            List<PeerId> expected = new ArrayList<>();
            for (Map.Entry<String, PeerNode> other : nodes.entrySet()) {
                if (!other.getKey().equals(address)) {
                    expected.add(other.getValue().peerId());
                }
            }
            assertThat(runtime.membership().allMembers()).extracting(GroupMembership.Member::id)
                    .as("%s's view is the live set", address).containsExactlyInAnyOrderElementsOf(expected);
            for (PeerId peer : expected) {
                assertThat(node.quarantined(peer)).as("%s never quarantined an honest member", address).isFalse();
            }
            assertThat(node.channelModes().size()).as("%s holds no more connections than members", address)
                    .isLessThanOrEqualTo(expected.size());
            assertThat(runtime.gossip().lazyPeers()).as("%s prunes members only", address)
                    .allSatisfy(peer -> assertThat(expected).contains(peer));
        }
        // the shared minimal oracle (srad/spoiler-peers.md SP-O-01): every inventory converged, no honest member
        // quarantined; the views, connections and lazy peers above are this fleet's fuller set
        assertThat(FleetInvariants.state(probe(), true)).as("the fleet invariants").isEmpty();
    }

    /**
     * This fleet as the shared oracle reads it: each member's inventory as its root, and, as the punished,
     * the members it quarantines (strikes are left out, as this fleet's invariants always left them out:
     * several scenarios strike honest members for frames the scenario itself made stale).
     */
    FleetProbe probe() {
        return new FleetProbe() {
            @Override
            public Set<String> members() {
                return Set.copyOf(nodes.keySet());
            }

            @Override
            public Map<String, String> roots(String member) {
                return Map.of("inventory", inventories.get(member).digestString());
            }

            @Override
            public Map<String, Long> drops(String member) {
                PeerNode node = nodes.get(member);
                return Map.of("stale", node.framesStale(), "replay", node.framesReplayed(),
                        "undecodable", node.framesDropped(), "logical", node.framesLogicalRefused(),
                        "farFuture", node.framesFarFuture());
            }

            @Override
            public Set<String> punished(String member) {
                PeerNode node = nodes.get(member);
                Set<String> out = new java.util.TreeSet<>();
                nodes.forEach((other, peer) -> {
                    if (!other.equals(member) && node.quarantined(peer.peerId())) {
                        out.add(other);
                    }
                });
                return out;
            }

            @Override
            public List<Throwable> errors(String member) {
                return List.of();
            }
        };
    }

    /** Ticks until the invariants hold or the budget runs out; returns whether they held. */
    boolean settle(int maxRounds) {
        for (int i = 0; i < maxRounds; i++) {
            tickAll(1);
            try {
                assertInvariants();
                return true;
            } catch (AssertionError pending) {
                // not yet
            }
        }
        return false;
    }
}
