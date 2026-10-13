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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §3.5 and §3.3, one frame pair at a time: an ACK before its PING, an answering
 * hello before an opening one, a PRUNE in a group too small to prune, a BATCH holding a stale
 * inner frame, and a member whose clock runs ahead within and beyond the drift bound. Each is
 * a member's frame under a good signature, so what matters is that nothing throws, the right
 * thing is ignored or dropped, the rest is delivered, and a single oddity is never a sentence.
 */
class RobustnessReorderTest {

    private final RobustnessFleet fleet = new RobustnessFleet("Reorder");
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private Envelope envelope(String from, String to, Envelope.Kind kind, Object body, long physical, int logical) {
        return new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, kind, fleet.id(from), fleet.id(to),
                new HlcTimestamp(physical, logical, from), body instanceof byte[] raw ? raw : codec.toBytes(body));
    }

    private byte[] signed(String from, String to, Envelope.Kind kind, Object body, long physical, int logical) {
        return wire.encode(envelope(from, to, kind, body, physical, logical), fleet.identities.get(from));
    }

    @Test
    @Timeout(60)
    void anAckBeforeItsPingAndAnAnswerBeforeAnOpeningHelloAreIgnoredWithoutASentence() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(12)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        long now = fleet.clock.millis();
        // An ACK A never asked for, and an answering hello to a session A never opened.
        assertThat(fleet.network.inject("b", "a", signed("b", "a", Envelope.Kind.ACK, new Bodies.Ack(424242L, null), now, 1))).isTrue();
        assertThat(fleet.network.inject("b", "a", signed("b", "a", Envelope.Kind.SESSION_HELLO,
                new Bodies.SessionHello(new byte[32], new byte[32], fleet.id("a"), new byte[32]), now, 2))).isTrue();
        fleet.tickAll(2);
        assertThat(a.quarantined(fleet.id("b"))).isFalse();
        assertThat(a.channelModes()).as("the real session stands, or forms, untouched").containsEntry(fleet.id("b"), "session");
        fleet.publish("b", "after-oddities");
        fleet.tickAll(2);
        assertThat(fleet.news.get("a")).contains("after-oddities");
        fleet.assertInvariants();
    }

    @Test
    @Timeout(60)
    void aPruneInAGroupTooSmallToPruneDoesNotCutTheEdge() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        fleet.add("c", "a");
        assertThat(fleet.settle(12)).isTrue();
        byte[] hash = Digests.sha256("whatever".getBytes(StandardCharsets.UTF_8));
        assertThat(fleet.network.inject("b", "a", signed("b", "a", Envelope.Kind.PRUNE, new Bodies.Prune("news", hash),
                fleet.clock.millis(), 1))).isTrue();
        long rumorsToB = fleet.nodes.get("b").traffic().receivedByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
        fleet.publish("a", "still-direct");
        assertThat(fleet.news.get("b")).as("delivered in one hop regardless of the prune").contains("still-direct");
        assertThat(fleet.nodes.get("b").traffic().receivedByKind().getOrDefault(Envelope.Kind.RUMOR, 0L)).isGreaterThan(rumorsToB);
        assertThat(fleet.settle(20)).as("inventories converge by anti-entropy").isTrue();
    }

    @Test
    @Timeout(60)
    void aBatchHoldingAStaleInnerFrameDeliversTheFreshOnesAndDropsTheStale() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(12)).isTrue();
        long now = fleet.clock.millis();
        Bodies.Rumor fresh = new Bodies.Rumor("news", "fresh", 6, "fresh".getBytes(StandardCharsets.UTF_8));
        Bodies.Rumor old = new Bodies.Rumor("news", "old", 6, "old".getBytes(StandardCharsets.UTF_8));
        byte[] freshInner = codec.toBytes(envelope("b", "a", Envelope.Kind.RUMOR, fresh, now, 3));
        byte[] staleInner = codec.toBytes(envelope("b", "a", Envelope.Kind.RUMOR, old, now - Duration.ofMinutes(6).toMillis(), 4));
        long droppedBefore = fleet.nodes.get("a").framesDropped();
        assertThat(fleet.network.inject("b", "a", signed("b", "a", Envelope.Kind.BATCH,
                new Bodies.Batch(List.of(staleInner, freshInner)), now, 5))).isTrue();
        assertThat(fleet.news.get("a")).as("the fresh inner frame is delivered").contains("fresh").doesNotContain("old");
        assertThat(fleet.nodes.get("a").quarantined(fleet.id("b"))).as("a stale inner frame is not misbehaviour").isFalse();
        assertThat(fleet.nodes.get("a").framesDropped()).isEqualTo(droppedBefore);
        fleet.tickAll(2);
        fleet.assertInvariants();
    }

    @Test
    @Timeout(60)
    void aClockAheadWithinTheDriftBoundIsAcceptedAndBeyondItIsNotASentenceOnItsOwn() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(12)).isTrue();
        long now = fleet.clock.millis();
        long within = now + HybridLogicalClock.MAX_DRIFT_MILLIS / 2;
        long beyond = now + HybridLogicalClock.MAX_DRIFT_MILLIS + Duration.ofMinutes(1).toMillis();
        assertThat(fleet.network.inject("b", "a", signed("b", "a", Envelope.Kind.RUMOR,
                new Bodies.Rumor("news", "ahead", 6, "a little ahead".getBytes(StandardCharsets.UTF_8)), within, 1))).isTrue();
        assertThat(fleet.news.get("a")).as("a clock a little ahead is the normal case").contains("a little ahead");
        assertThat(fleet.network.inject("b", "a", signed("b", "a", Envelope.Kind.RUMOR,
                new Bodies.Rumor("news", "far", 6, "far ahead".getBytes(StandardCharsets.UTF_8)), beyond, 2))).isTrue();
        assertThat(fleet.news.get("a")).as("a stamp far in the future is refused").doesNotContain("far ahead");
        assertThat(fleet.nodes.get("a").quarantined(fleet.id("b"))).as("one such frame is noted, not sentenced").isFalse();
        fleet.publish("b", "back-to-normal");
        assertThat(fleet.news.get("a")).contains("back-to-normal");
        fleet.tickAll(2);
        fleet.assertInvariants();
    }
}
