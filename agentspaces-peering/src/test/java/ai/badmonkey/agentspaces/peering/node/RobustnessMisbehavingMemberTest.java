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
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §3.9: a member whose frames are malformed under its own good signature. One
 * such frame is noted, not punished; the threshold quarantines it locally for the sentence and
 * nowhere else; the rest of the fleet is untouched; the sentence lapses and the member is
 * served again. Nothing throws at any step.
 */
class RobustnessMisbehavingMemberTest {

    private final RobustnessFleet fleet = new RobustnessFleet("Misbehaving");
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private byte[] malformed(String from, String to, Envelope.Kind kind, int logical) {
        PeerIdentity identity = fleet.identities.get(from);
        Envelope envelope = new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, kind, identity.peerId(),
                fleet.id(to), new HlcTimestamp(fleet.clock.millis(), logical, from), new byte[] {(byte) 0xff, 0x01, 0x02});
        return wire.encode(envelope, identity);
    }

    @Test
    @Timeout(60)
    void malformedBodiesUnderAGoodSignatureStrikeUpToAQuarantineThatLapses() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        fleet.add("c", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");

        // One bad frame of a handful of kinds from B: noted, nothing more.
        int logical = 0;
        for (Envelope.Kind kind : new Envelope.Kind[] {Envelope.Kind.RUMOR, Envelope.Kind.DIGEST, Envelope.Kind.PULL_RESP,
                Envelope.Kind.HAVE, Envelope.Kind.GRAFT, Envelope.Kind.PRUNE, Envelope.Kind.PING, Envelope.Kind.ACK}) {
            assertThat(fleet.network.inject("b", "a", malformed("b", "a", kind, logical++))).isTrue();
        }
        assertThat(a.quarantined(fleet.id("b"))).as("a handful of bad frames is noise, not a sentence").isFalse();
        fleet.publish("b", "still-served");
        fleet.tickAll(2);
        assertThat(fleet.news.get("a")).contains("still-served");

        // The threshold: B is quarantined at A alone, C still serves it, and A still serves C.
        for (int i = 0; i < PeerNode.STRIKE_THRESHOLD; i++) {
            fleet.network.inject("b", "a", malformed("b", "a", Envelope.Kind.RUMOR, logical++));
        }
        assertThat(a.quarantined(fleet.id("b"))).isTrue();
        assertThat(fleet.nodes.get("c").quarantined(fleet.id("b"))).as("a local verdict, never gossiped").isFalse();
        fleet.publish("b", "while-quarantined");
        fleet.tickAll(2);
        assertThat(fleet.news.get("c")).contains("while-quarantined");
        fleet.publish("c", "from-c");
        fleet.tickAll(2);
        assertThat(fleet.news.get("a")).contains("from-c");

        // The sentence lapses: served again, and the fleet converges, including what A missed.
        fleet.clock.advance(PeerNode.QUARANTINE.plusSeconds(1));
        fleet.tickAll(2);
        assertThat(a.quarantined(fleet.id("b"))).isFalse();
        fleet.publish("b", "after-sentence");
        assertThat(fleet.settle(40)).isTrue();
        assertThat(fleet.news.get("a")).contains("after-sentence");
        assertThat(fleet.inventories.get("a").members).contains("while-quarantined");
    }

    /** Every kind, malformed under C's good signature, one after another: nothing throws, the strikes add up to the one local sentence, and it lapses. */
    @Test
    @Timeout(60)
    void everyKindMalformedUnderAGoodSignatureIsSurvived() throws Exception {
        fleet.add("a", null);
        fleet.add("b", "a");
        fleet.add("c", "a");
        assertThat(fleet.settle(10)).isTrue();
        PeerNode a = fleet.nodes.get("a");
        long droppedBefore = a.framesDropped();
        int logical = 0;
        for (Envelope.Kind kind : Envelope.Kind.values()) {
            fleet.network.inject("c", "a", malformed("c", "a", kind, logical++));
        }
        assertThat(a.framesDropped()).as("a good signature is not dropped at the door").isEqualTo(droppedBefore);
        // Only the kinds whose bodies the node decodes itself strike; one bad frame of each is
        // below the threshold. A second sweep is not.
        assertThat(a.quarantined(fleet.id("c"))).as("one bad frame per kind is noise").isFalse();
        for (Envelope.Kind kind : Envelope.Kind.values()) {
            fleet.network.inject("c", "a", malformed("c", "a", kind, logical++));
        }
        assertThat(a.quarantined(fleet.id("c"))).as("a second sweep passes the threshold: sentenced here").isTrue();
        assertThat(fleet.nodes.get("b").quarantined(fleet.id("c"))).as("and nowhere else").isFalse();
        fleet.publish("b", "others-unaffected");
        fleet.tickAll(2);
        assertThat(fleet.news.get("a")).contains("others-unaffected");
        fleet.clock.advance(PeerNode.QUARANTINE.plusSeconds(1));
        fleet.tickAll(2);
        assertThat(a.quarantined(fleet.id("c"))).isFalse();
        fleet.publish("c", "served-again");
        assertThat(fleet.settle(40)).isTrue();
        assertThat(fleet.news.get("a")).contains("served-again");
    }
}
