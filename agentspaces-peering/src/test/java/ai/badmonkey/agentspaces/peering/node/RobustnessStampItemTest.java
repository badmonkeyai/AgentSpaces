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
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.HostileStamps;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HLC round four, 2026-11-08 (decisions D3 and D5): a frame whose stamp is any CBOR item other
 * than a definite-length, untagged, shortest-form text string is undecodable. The receiver counts
 * it in {@link PeerNode#framesDropped()}, never verifies it as signed, never dispatches it, and
 * strikes nobody, since no verified signature names a sender. An inner frame of a signed BATCH
 * is covered by the outer signature, so the same item there is counted and struck.
 */
class RobustnessStampItemTest {

    private final RobustnessFleet fleet = new RobustnessFleet("StampItem");
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private Envelope rumor(String from, String to, String node, int logical) {
        PeerIdentity identity = fleet.identities.get(from);
        return new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, Envelope.Kind.RUMOR, identity.peerId(),
                fleet.id(to), new HlcTimestamp(fleet.clock.millis(), logical, node),
                codec.toBytes(new Bodies.Rumor("news", "item-" + logical, 1,
                        ("rumor-" + logical).getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    @Timeout(60)
    void aFrameWhoseStampIsNoCanonicalTextIsCountedUndecodableAndStrikesNobody() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        PeerIdentity b = fleet.identities.get("b");
        int logical = 1;
        for (Map.Entry<String, byte[]> variant : HostileStamps.items(fleet.clock.millis() + ":" + logical + ":b", "A")
                .entrySet()) {
            Envelope envelope = rumor("b", "a", "bA", logical);
            byte[] honest = wire.encode(envelope, b);
            byte[] item = HostileStamps.item(envelope.stamp().encoded());
            long dropped = a.framesDropped();
            long rumors = a.traffic().receivedByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
            assertThat(fleet.network.inject("b", "a", HostileStamps.splice(honest, item, variant.getValue()))).isTrue();
            assertThat(a.framesDropped()).as(variant.getKey() + ": counted undecodable").isEqualTo(dropped + 1);
            assertThat(a.traffic().receivedByKind().getOrDefault(Envelope.Kind.RUMOR, 0L))
                    .as(variant.getKey() + ": never verified or dispatched").isEqualTo(rumors);
            assertThat(fleet.news.get("a")).doesNotContain("rumor-" + logical);
        }
        assertThat(a.strikes(fleet.id("b"))).as("no verified signature names a sender").isZero();
        assertThat(a.framesLogicalRefused()).isZero();

        long dropped = a.framesDropped();
        Envelope honest = rumor("b", "a", "bA", logical + 1);
        assertThat(fleet.network.inject("b", "a", wire.encode(honest, b))).isTrue();
        assertThat(a.framesDropped()).as("the same frame, canonical, is not dropped").isEqualTo(dropped);
        assertThat(fleet.news.get("a")).contains("rumor-" + (logical + 1));
    }

    @Test
    @Timeout(60)
    void aBatchedFrameWhoseStampIsNoCanonicalTextIsCountedAndItsSignerStruck() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        PeerIdentity b = fleet.identities.get("b");
        Envelope inner = rumor("b", "a", "bA", 3);
        byte[] item = HostileStamps.item(inner.stamp().encoded());
        byte[] tagged = HostileStamps.splice(codec.toBytes(inner), item, HostileStamps.cat(new byte[]{(byte) 0xC0}, item));
        Envelope batch = new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, Envelope.Kind.BATCH, b.peerId(),
                fleet.id("a"), new HlcTimestamp(fleet.clock.millis(), 4, b.peerId().value()),
                codec.toBytes(new Bodies.Batch(List.of(tagged))));
        long dropped = a.framesDropped();
        int strikes = a.strikes(b.peerId());

        assertThat(fleet.network.inject("b", "a", wire.encode(batch, b))).isTrue();

        assertThat(a.framesDropped()).as("the inner frame is undecodable and counted").isEqualTo(dropped + 1);
        assertThat(a.strikes(b.peerId())).as("the outer signature names its signer").isEqualTo(strikes + 1);
        assertThat(fleet.news.get("a")).doesNotContain("rumor-3");
    }
}
