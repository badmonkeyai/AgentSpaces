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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HLC round five, 2026-11-08 (decisions E8 and E15): outside its stamp, a frame is read as every
 * implementation reads CBOR. A tag Jackson never writes, or invalid or overlong UTF-8 in any text
 * string, makes the frame undecodable: counted in {@link PeerNode#framesDropped()}, never
 * verified, never dispatched, and nobody struck. Jackson once dropped the tag or decoded the
 * overlong letter, re-encoded the envelope, and verified the signer's signature over it, so a
 * relay could alter signed bytes that the other implementations refused. Inside a signed BATCH
 * the outer signature covers the inner bytes, so the same inner frame strikes the outer signer,
 * the one exception to striking nobody. Since HLC round six, 2026-11-08 (owner ruling F3), a short
 * text in an indefinite-length item is undecodable as well: the wire encoding chunks only a text
 * past 3996 UTF-16 units.
 */
class RobustnessCborItemTest {

    private final RobustnessFleet fleet = new RobustnessFleet("CborItem");
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);
    private final byte[] kind = text("RUMOR");

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private Envelope rumor(String from, String to, int logical) {
        PeerIdentity identity = fleet.identities.get(from);
        return new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, Envelope.Kind.RUMOR, identity.peerId(),
                fleet.id(to), new HlcTimestamp(fleet.clock.millis(), logical, from),
                codec.toBytes(new Bodies.Rumor("news", "item-" + logical, 1,
                        ("rumor-" + logical).getBytes(StandardCharsets.UTF_8))));
    }

    /** The relabelled forms of the envelope's {@code kind} text that no encoder writes. */
    private Map<String, byte[]> hostileKinds() {
        Map<String, byte[]> kinds = new LinkedHashMap<>();
        kinds.put("overlong UTF-8 C1 92 for the final 'R'", HostileStamps.cat(new byte[]{0x66},
                "RUMO".getBytes(StandardCharsets.US_ASCII), new byte[]{(byte) 0xC1, (byte) 0x92}));
        kinds.put("tag 0", HostileStamps.cat(new byte[]{(byte) 0xC0}, kind));
        kinds.put("tag 42", HostileStamps.cat(new byte[]{(byte) 0xD8, 0x2A}, kind));
        return kinds;
    }

    @Test
    @Timeout(60)
    void aFrameWithATagOrOverlongUtf8OutsideItsStampIsUndecodableAndStrikesNobody() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        PeerIdentity b = fleet.identities.get("b");
        int logical = 1;
        for (Map.Entry<String, byte[]> variant : hostileKinds().entrySet()) {
            byte[] honest = wire.encode(rumor("b", "a", logical), b);
            long dropped = a.framesDropped();
            assertThat(fleet.network.inject("b", "a", HostileStamps.splice(honest, kind, variant.getValue())))
                    .isTrue();
            assertThat(a.framesDropped()).as(variant.getKey() + ": counted undecodable").isEqualTo(dropped + 1);
            assertThat(fleet.news.get("a")).as(variant.getKey()).doesNotContain("rumor-" + logical);
            logical++;
        }
        assertThat(a.strikes(fleet.id("b"))).as("no verified signature names a sender").isZero();

        // HLC round six, 2026-11-08 (S6): Jackson chunks only a text past 3996 UTF-16 units, so a
        // short text in an indefinite-length item is no longer the frame's encoding
        long dropped = a.framesDropped();
        byte[] indefinite = HostileStamps.cat(new byte[]{0x7F}, kind, new byte[]{(byte) 0xFF});
        assertThat(fleet.network.inject("b", "a",
                HostileStamps.splice(wire.encode(rumor("b", "a", logical), b), kind, indefinite))).isTrue();
        assertThat(a.framesDropped()).as("a short indefinite-length text is undecodable").isEqualTo(dropped + 1);
        assertThat(fleet.news.get("a")).doesNotContain("rumor-" + logical);
        assertThat(a.strikes(fleet.id("b"))).isZero();
    }

    @Test
    @Timeout(60)
    void aBatchedFrameWithAnOverlongLetterIsCountedAndItsOuterSignerStruck() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        PeerIdentity b = fleet.identities.get("b");
        byte[] inner = HostileStamps.splice(codec.toBytes(rumor("b", "a", 7)), kind,
                hostileKinds().get("overlong UTF-8 C1 92 for the final 'R'"));
        Envelope batch = new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, Envelope.Kind.BATCH, b.peerId(),
                fleet.id("a"), new HlcTimestamp(fleet.clock.millis(), 8, b.peerId().value()),
                codec.toBytes(new Bodies.Batch(List.of(inner))));
        long dropped = a.framesDropped();
        int strikes = a.strikes(b.peerId());

        assertThat(fleet.network.inject("b", "a", wire.encode(batch, b))).isTrue();

        assertThat(a.framesDropped()).as("the inner frame is undecodable and counted").isEqualTo(dropped + 1);
        assertThat(a.strikes(b.peerId())).as("the outer signature names its signer").isEqualTo(strikes + 1);
        assertThat(fleet.news.get("a")).doesNotContain("rumor-7");
    }

    /**
     * HLC round six, 2026-11-08 (owner rulings F1 and F3): a body decodes only from its own wire
     * encoding. A member's signed RUMOR whose body holds its hop count under a wider head, or
     * its fields out of order, once decoded and spread; now the body is undecodable and the
     * envelope's signature names the member who signed it, which Java strikes as it strikes
     * every malformed body.
     */
    @Test
    @Timeout(60)
    void aSignedFrameWhoseBodyIsNotItsWireEncodingIsRefusedAndItsSignerStruck() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        PeerIdentity b = fleet.identities.get("b");
        Map<String, byte[]> bodies = new LinkedHashMap<>();
        byte[] honest = codec.toBytes(new Bodies.Rumor("news", "item-31", 1, "rumor-31".getBytes(StandardCharsets.UTF_8)));
        byte[] hops = HostileStamps.cat(text("hopsRemaining"), new byte[]{0x01});
        bodies.put("hopsRemaining under 18 01", HostileStamps.splice(honest, hops,
                HostileStamps.cat(text("hopsRemaining"), new byte[]{0x18, 0x01})));
        ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim reordered =
                new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        reordered.put("itemId", "item-31");
        reordered.put("streamId", "news");
        reordered.put("hopsRemaining", 1);
        reordered.put("payload", "rumor-31".getBytes(StandardCharsets.UTF_8));
        bodies.put("fields out of order", codec.toBytes(reordered));
        int strikes = 0;
        int logical = 31;
        for (Map.Entry<String, byte[]> body : bodies.entrySet()) {
            Envelope envelope = new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, Envelope.Kind.RUMOR,
                    b.peerId(), fleet.id("a"), new HlcTimestamp(fleet.clock.millis(), logical++, "b"), body.getValue());
            assertThat(fleet.network.inject("b", "a", wire.encode(envelope, b))).isTrue();
            assertThat(fleet.news.get("a")).as(body.getKey()).doesNotContain("rumor-31");
            assertThat(a.strikes(b.peerId())).as(body.getKey() + ": malformed RUMOR body").isEqualTo(++strikes);
        }
    }

    private static byte[] text(String ascii) {
        byte[] bytes = ascii.getBytes(StandardCharsets.US_ASCII);
        return HostileStamps.cat(new byte[]{(byte) (0x60 | bytes.length)}, bytes);
    }
}
