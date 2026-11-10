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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asker-only repair (ROBUSTNESS.md §10.8 decision 2, §10.9): five real buses on an inline fabric with a hand-shaped
 * tree. The answer to a GRAFT goes to the asker alone with a hop budget that ends there, and the asker forwards the
 * item along its own eager edges as usual. A member that already holds the item then takes the asker's copy as a
 * point-to-point copy, so the late repair prunes no sound edge of the tree.
 */
class AskerOnlyRepairTest {

    private record Frame(PeerId from, PeerId to, String kind, Object body) { }

    private final long[] clock = {0};
    private final Map<String, PeerId> ids = new LinkedHashMap<>();
    private final Map<PeerId, GossipBus> buses = new LinkedHashMap<>();
    private final Map<PeerId, List<String>> delivered = new LinkedHashMap<>();
    private final Deque<Runnable> queue = new ArrayDeque<>();
    private final List<Runnable> held = new ArrayList<>();
    private final List<Frame> frames = new ArrayList<>();
    /** Holds the eager copies from {@code b} to {@code a}: the tree copy that is merely late. */
    private boolean holdBToA = true;

    private PeerId id(String name) {
        return ids.get(name);
    }

    private GossipBus bus(String name) {
        return buses.get(id(name));
    }

    private void build() {
        for (String name : List.of("o", "x", "b", "c", "a")) {
            ids.put(name, PeerIdentity.generate().peerId());
        }
        long seed = 11;
        for (PeerId self : ids.values()) {
            List<PeerId> others = new ArrayList<>(ids.values());
            others.remove(self);
            PeerSampler sampler = n -> n >= others.size() ? List.copyOf(others) : others.subList(0, n);
            GossipBus bus = new GossipBus(sampler, sender(self), CborCodec.defaultCodec(), 3, GossipBus.DEFAULT_HOPS,
                    () -> clock[0], new java.util.Random(seed++));
            bus.lazyPush(peer -> true);
            List<String> got = new ArrayList<>();
            delivered.put(self, got);
            bus.onStream("news", (from, itemId, payload) -> got.add(itemId));
            buses.put(self, bus);
        }
    }

    private GossipBus.FrameSender sender(PeerId self) {
        return new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
                frames.add(new Frame(self, to, "RUMOR", rumor));
                Runnable deliver = () -> buses.get(to).onRumor(self, rumor);
                if (holdBToA && self.equals(id("b")) && to.equals(id("a"))) {
                    held.add(deliver);
                } else {
                    queue.add(deliver);
                }
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digest) {
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp resp) {
            }

            @Override
            public void sendHave(PeerId to, Bodies.Have have) {
                frames.add(new Frame(self, to, "HAVE", have));
                queue.add(() -> buses.get(to).onHave(self, have));
            }

            @Override
            public void sendGraft(PeerId to, Bodies.Graft graft) {
                frames.add(new Frame(self, to, "GRAFT", graft));
                queue.add(() -> buses.get(to).onGraft(self, graft));
            }

            @Override
            public void sendPrune(PeerId to, Bodies.Prune prune) {
                frames.add(new Frame(self, to, "PRUNE", prune));
                queue.add(() -> buses.get(to).onPrune(self, prune));
            }
        };
    }

    private void pump() {
        while (!queue.isEmpty()) {
            queue.poll().run();
        }
    }

    /** Makes the edge between two members lazy at both ends, as a prune does. */
    private void lazyEdge(String u, String v) {
        Bodies.Prune prune = new Bodies.Prune("news", Digests.sha256("shape".getBytes(StandardCharsets.UTF_8)));
        bus(u).onPrune(id(v), prune);
        bus(v).onPrune(id(u), prune);
    }

    private List<Frame> sent(String from, String to, String kind) {
        return frames.stream().filter(f -> f.from().equals(id(from)) && (to == null || f.to().equals(id(to)))
                && f.kind().equals(kind)).toList();
    }

    /**
     * The tree is o-x, x-b, x-c and b-a; every other edge is lazy. The copy from b to a runs late, so a hears the item
     * announced, asks for it, and gets the answer before the tree copy. On the old bus the answer kept its rumor's hop
     * budget, a forwarded it to b, which already held the item, and b pruned its sound edge to a. Now the answer ends at
     * a, a's copy to b carries one hop, and b prunes nothing; once the late tree copy lands, a drops the edge the graft
     * made and keeps b.
     */
    @Test
    void aRepairReplyEndsAtTheAskerAndTheAskersForwardPrunesNoSoundEdge() {
        build();
        for (String[] edge : new String[][] {{"o", "a"}, {"o", "b"}, {"o", "c"}, {"x", "a"}, {"a", "c"}, {"b", "c"}}) {
            lazyEdge(edge[0], edge[1]);
        }
        bus("o").publish("news", "item", "the item".getBytes(StandardCharsets.UTF_8));
        pump();
        assertThat(delivered.get(id("a"))).as("a's tree copy is late").isEmpty();
        for (int tick = 0; tick < 16; tick++) {
            clock[0] += 250;
            for (GossipBus each : buses.values()) {
                each.tick(clock[0]);
                pump();
            }
        }
        assertThat(delivered.get(id("a"))).as("a asked and got the item").containsExactly("item");
        assertThat(sent("b", null, "PRUNE")).as("b already held the item and prunes no sound edge").isEmpty();
        List<Frame> answers = frames.stream().filter(f -> f.to().equals(id("a")) && f.kind().equals("RUMOR")
                && !f.from().equals(id("b"))).toList();
        assertThat(answers).as("one answer to a's ask").singleElement()
                .satisfies(f -> assertThat(((Bodies.Rumor) f.body()).hopsRemaining()).as("the answer ends at the asker")
                        .isEqualTo(1));
        assertThat(sent("a", "b", "RUMOR")).as("a forwards the item along its own eager edge to b").singleElement()
                .satisfies(f -> assertThat(((Bodies.Rumor) f.body()).hopsRemaining()).isEqualTo(1));
        holdBToA = false;
        held.forEach(Runnable::run);
        pump();
        assertThat(bus("b").lazyPeers()).as("b keeps its tree edge to a").doesNotContain(id("a"));
        assertThat(bus("a").lazyPeers()).as("a keeps its tree edge to b").doesNotContain(id("b"));
        for (String member : List.of("x", "b", "c", "a")) {
            assertThat(delivered.get(id(member))).as("%s delivered the item once", member).containsExactly("item");
        }
    }

    /** A GRAFT for a cached item is answered to the asker alone, with a hop budget of one. */
    @Test
    void aGraftIsAnsweredWithAHopBudgetOfOne() {
        build();
        bus("o").publish("news", "k", "k".getBytes(StandardCharsets.UTF_8));
        pump();
        frames.clear();
        bus("o").onGraft(id("a"), new Bodies.Graft("news", Digests.sha256("k".getBytes(StandardCharsets.UTF_8))));
        assertThat(sent("o", null, "RUMOR")).singleElement().satisfies(f -> {
            assertThat(f.to()).isEqualTo(id("a"));
            assertThat(((Bodies.Rumor) f.body()).hopsRemaining()).isEqualTo(1);
        });
    }
}
