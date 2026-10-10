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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Gossip abuse resistance (ASF-012): deduplication keys on the payload's own
 * bytes, never on a sender-chosen itemId; inbound hop budgets are clamped; and
 * a stream's forward filter keeps this node from amplifying content it can
 * already tell is unverifiable.
 */
class GossipBusTest {

    private final PeerId neighbor = PeerIdentity.generate().peerId();
    private final PeerId attacker = PeerIdentity.generate().peerId();
    private final List<Bodies.Rumor> forwarded = new ArrayList<>();
    private final List<PeerId> forwardedTo = new ArrayList<>();
    private final List<PeerId> digestsSentTo = new ArrayList<>();
    private final List<Bodies.PullResp> pullRespsSent = new ArrayList<>();
    private final PeerSampler sampler = n -> List.of(neighbor);
    private final GossipBus.FrameSender sender = new GossipBus.FrameSender() {
        @Override
        public void sendRumor(PeerId to, Bodies.Rumor rumor) {
            forwarded.add(rumor);
            forwardedTo.add(to);
        }

        @Override
        public void sendDigest(PeerId to, Bodies.Digest digests) {
            digestsSentTo.add(to);
        }

        @Override
        public void sendPullResp(PeerId to, Bodies.PullResp deltas) {
            pullRespsSent.add(deltas);
        }
    };
    private final GossipBus bus =
            new GossipBus(sampler, sender, CborCodec.defaultCodec(), 3, 6);

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * QA4 A4-9, written before the fix: registrations are handles, a duplicate
     * stream id is refused, and closing a handle removes exactly that entry.
     */
    @Test
    void streamRegistrationsAreHandlesAndDuplicatesAreRefused() throws Exception {
        AutoCloseable handler = bus.onStream("space:tasks", (from, itemId, payload) -> { });
        AutoCloseable state = bus.reconcile("space:tasks", new ReconcilableState() {
            @Override public byte[] digest() { return new byte[0]; }
            @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
            @Override public void applyDelta(byte[] delta) { }
        });
        assertThat(bus.streams()).contains("space:tasks");

        assertThatThrownBy(() -> bus.onStream("space:tasks", (from, itemId, payload) -> { }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("space:tasks");
        assertThatThrownBy(() -> bus.reconcile("space:tasks", new ReconcilableState() {
            @Override public byte[] digest() { return new byte[0]; }
            @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
            @Override public void applyDelta(byte[] delta) { }
        })).isInstanceOf(IllegalStateException.class);

        handler.close();
        state.close();
        assertThat(bus.streams()).doesNotContain("space:tasks");
        bus.onStream("space:tasks", (from, itemId, payload) -> { }).close();   // accepted again
    }

    @Test
    void aPreSeededItemIdCannotSuppressTheGenuineItem() {
        List<byte[]> delivered = new ArrayList<>();
        bus.onStream("peers", (from, itemId, payload) -> delivered.add(payload));

        // The attacker predicts the victim's future itemId and seeds the cache
        // with garbage under it. The victim's genuine item carries different
        // payload bytes, so it still deduplicates independently and delivers.
        String futureId = "peer:victim:1234567890";
        bus.onRumor(attacker, new Bodies.Rumor("peers", futureId, 3, bytes("garbage")));
        bus.onRumor(neighbor, new Bodies.Rumor("peers", futureId, 3, bytes("genuine ad")));

        assertThat(delivered).hasSize(2);
        assertThat(new String(delivered.get(1), StandardCharsets.UTF_8))
                .isEqualTo("genuine ad");

        // While a true replay of the same payload still deduplicates.
        bus.onRumor(attacker, new Bodies.Rumor("peers", futureId, 3, bytes("genuine ad")));
        assertThat(delivered).hasSize(2);
    }

    @Test
    void inboundHopBudgetsAreClampedToTheBusDefault() {
        bus.onStream("s", (from, itemId, payload) -> {
        });
        bus.onRumor(attacker, new Bodies.Rumor("s", "amplify", 1_000_000, bytes("x")));
        assertThat(forwarded).isNotEmpty();
        assertThat(forwarded).extracting(Bodies.Rumor::hopsRemaining)
                .as("forwarded budget is exactly the default minus this hop")
                .containsOnly(5);
    }

    @Test
    void theForwardFilterStopsAmplificationButNotLocalDelivery() {
        List<byte[]> delivered = new ArrayList<>();
        bus.onStream("filtered", (from, itemId, payload) -> delivered.add(payload));
        bus.forwardFilter("filtered", payload -> false);

        bus.onRumor(attacker, new Bodies.Rumor("filtered", "i1", 6, bytes("unverifiable")));

        assertThat(delivered).hasSize(1); // the handler still judges for itself
        assertThat(forwarded).as("but this node never re-signs and forwards it").isEmpty();
    }

    /** A trivially mergeable set-of-strings state for anti-entropy assertions. */
    private static final class SetState implements ReconcilableState {
        final Set<String> items = new TreeSet<>();

        @Override
        public byte[] digest() {
            return String.join("\n", items).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] deltaFor(byte[] remoteDigest) {
            Set<String> remote = new HashSet<>(List.of(
                    new String(remoteDigest, StandardCharsets.UTF_8).split("\n")));
            Set<String> missing = new TreeSet<>(items);
            missing.removeAll(remote);
            return String.join("\n", missing).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void applyDelta(byte[] delta) {
            for (String item : new String(delta, StandardCharsets.UTF_8).split("\n")) {
                if (!item.isEmpty()) {
                    items.add(item);
                }
            }
        }
    }

    /** Spec §5.3: publish pushes to exactly {@code fanout} sampled members; forwarding excludes the previous hop. */
    @Test
    void publishContactsExactlyFanoutPeersAndForwardingExcludesTheSender() {
        List<PeerId> view = new ArrayList<>(List.of(neighbor));
        for (int i = 0; i < 4; i++) {
            view.add(PeerIdentity.generate().peerId());
        }
        PeerSampler wide = n -> view.subList(0, Math.min(n, view.size()));
        GossipBus fanoutTwo = new GossipBus(wide, sender, CborCodec.defaultCodec(), 2, 6);
        fanoutTwo.onStream("s", (from, itemId, payload) -> {
        });

        fanoutTwo.publish("s", "i1", bytes("fresh"));
        assertThat(forwardedTo).hasSize(2).doesNotHaveDuplicates();
        assertThat(forwarded).extracting(Bodies.Rumor::hopsRemaining).containsOnly(6);

        forwarded.clear();
        forwardedTo.clear();
        fanoutTwo.onRumor(neighbor, new Bodies.Rumor("s", "i2", 6, bytes("relayed")));
        assertThat(forwardedTo).hasSize(2)
                .as("never back to the hop it arrived from").doesNotContain(neighbor);
        assertThat(forwarded).extracting(Bodies.Rumor::hopsRemaining)
                .as("one hop of budget spent").containsOnly(5);
    }

    /** Spec §5.3 hop decay: an item with one hop left is delivered locally but travels no further. */
    @Test
    void aRumorWithOneHopLeftIsDeliveredButNotForwarded() {
        List<byte[]> delivered = new ArrayList<>();
        bus.onStream("s", (from, itemId, payload) -> delivered.add(payload));

        bus.onRumor(attacker, new Bodies.Rumor("s", "last-hop", 1, bytes("x")));

        assertThat(delivered).hasSize(1);
        assertThat(forwarded).isEmpty();
    }

    /** Spec §5.3 anti-entropy (ASF-003): pulls are strictly request-response; only the digest partner's delta applies. */
    @Test
    void anUnsolicitedPullRespIsDroppedAndTheDigestPartnersIsApplied() {
        SetState state = new SetState();
        bus.reconcile("set", state);
        byte[] delta = "fact-1".getBytes(StandardCharsets.UTF_8);

        bus.onPullResp(attacker, new Bodies.PullResp(Map.of("set", delta)));
        assertThat(state.items).as("no digest was ever sent: nothing may be injected").isEmpty();

        bus.antiEntropyTick();
        assertThat(digestsSentTo).containsExactly(neighbor);
        bus.onPullResp(attacker, new Bodies.PullResp(Map.of("set", delta)));
        assertThat(state.items).as("a delta from anyone but the partner is dropped").isEmpty();
        bus.onPullResp(neighbor, new Bodies.PullResp(Map.of("set", delta,
                "unregistered", delta)));
        assertThat(state.items).containsExactly("fact-1");

        // Answering a digest: only streams with something missing are in the reply.
        bus.onDigest(neighbor, new Bodies.Digest(Map.of("set", state.digest(),
                "unregistered", new byte[0])));
        assertThat(pullRespsSent).as("converged partner gets no reply").isEmpty();
        state.items.add("fact-2");
        bus.onDigest(neighbor, new Bodies.Digest(Map.of("set", "fact-1".getBytes(StandardCharsets.UTF_8))));
        assertThat(pullRespsSent).hasSize(1);
        assertThat(pullRespsSent.get(0).deltas()).containsOnlyKeys("set");
        assertThat(new String(pullRespsSent.get(0).deltas().get("set"), StandardCharsets.UTF_8))
                .isEqualTo("fact-2");
    }

    /**
     * ISSUE-CanonicalMaps: a PULL_RESP's deltas apply in the order their
     * streams were registered, not the order the wire (now sorted, shorter
     * stream ids first) carries them, so a revocation registry registered
     * before a space judges first.
     */
    @Test
    void pullRespDeltasApplyInRegistrationOrderNotWireOrder() {
        List<String> applied = new ArrayList<>();
        ReconcilableState first = recording("credential-revocations", applied);
        ReconcilableState second = recording("space:tasks", applied);
        bus.reconcile("credential-revocations", first);
        bus.reconcile("space:tasks", second);
        bus.antiEntropyTick();
        Map<String, byte[]> wireOrder = new java.util.LinkedHashMap<>();
        wireOrder.put("space:tasks", "x".getBytes(StandardCharsets.UTF_8));          // shorter id: first on the wire
        wireOrder.put("credential-revocations", "y".getBytes(StandardCharsets.UTF_8));
        bus.onPullResp(neighbor, new Bodies.PullResp(wireOrder));
        assertThat(applied).containsExactly("credential-revocations", "space:tasks");
    }

    private static ReconcilableState recording(String name, List<String> applied) {
        return new ReconcilableState() {
            @Override public byte[] digest() { return new byte[0]; }
            @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
            @Override public void applyDelta(byte[] delta) { applied.add(name); }
        };
    }

    /** Spec §5.3 dedup: the seen-cache is bounded (insertion-order eviction) yet recent items still dedup. */
    @Test
    void theSeenCacheIsBoundedButRecentItemsStillDedup() {
        List<String> delivered = new ArrayList<>();
        bus.onStream("s", (from, itemId, payload) ->
                delivered.add(new String(payload, StandardCharsets.UTF_8)));

        int overflow = 4_097;
        for (int i = 0; i < overflow; i++) {
            bus.onRumor(neighbor, new Bodies.Rumor("s", "i" + i, 1, bytes("p" + i)));
        }
        assertThat(delivered).hasSize(overflow);

        bus.onRumor(neighbor, new Bodies.Rumor("s", "i0", 1, bytes("p0")));
        assertThat(delivered).as("the oldest entry was evicted, so it delivers again")
                .hasSize(overflow + 1);
        bus.onRumor(neighbor, new Bodies.Rumor("s", "iLast", 1, bytes("p" + (overflow - 1))));
        assertThat(delivered).as("a recent item still deduplicates").hasSize(overflow + 1);
    }
}
