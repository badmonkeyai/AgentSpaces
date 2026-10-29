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

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** SPEC v0.1.23 §5.4 and annex G: the path frame, the signal, and the reach hint pin their bytes. */
class RelayPathVectorsTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static Map<String, Object> golden;
    private final CborCodec codec = CborCodec.defaultCodec();

    private record RelayPipeFrame(String capability, byte[] payload) {
    }

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void loadVectors() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required but none of " + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
        assumeTrue(golden.containsKey("relay_frame_with_path_cbor"), "golden.json predates the path vectors");
    }

    private byte[] hex(String name) {
        return HexFormat.of().parseHex((String) golden.get(name));
    }

    @Test
    void aPathFrameCarriesItsHopsAndTheOldFormStaysTheOldBytes() {
        byte[] bytes = hex("relay_frame_with_path_cbor");
        Bodies.RelayFrame frame = codec.fromBytes(bytes, Bodies.RelayFrame.class);
        assertThat(frame.target()).isEqualTo(PeerId.of("zB"));
        assertThat(frame.path()).containsExactly(PeerId.of("zR1"), PeerId.of("zR2"));
        assertThat(frame.hop()).isEqualTo(0);
        assertThat(codec.toBytes(frame)).isEqualTo(bytes);
        assertThat(frame.nextHop().hop()).isEqualTo(1);
        // the 0.4.0 body, unchanged: no path, no hop, the same bytes as before
        Bodies.RelayFrame legacy = codec.fromBytes(hex("relay_frame_body_cbor"), Bodies.RelayFrame.class);
        assertThat(legacy.path()).isNull();
        assertThat(legacy.hop()).isNull();
        assertThat(codec.toBytes(new Bodies.RelayFrame(legacy.target(), legacy.frame())))
                .isEqualTo(hex("relay_frame_body_cbor"));
        assertThat(RelayPaths.wellFormed(frame.path(), 0, PeerId.of("zR1"), frame.target(), 4)).isTrue();
    }

    @Test
    void theUnreachableSignalIsAPipeBodyNamingTargetAndHop() {
        byte[] bytes = hex("relay_signal_unreachable_cbor");
        RelayPipeFrame body = codec.fromBytes(bytes, RelayPipeFrame.class);
        assertThat(body.capability()).isEqualTo(PeerNode.RELAY_SIGNAL_CAPABILITY);
        PeerNode.RelaySignal signal = codec.fromBytes(body.payload(), PeerNode.RelaySignal.class);
        assertThat(signal.target()).isEqualTo(PeerId.of("zB"));
        assertThat(signal.failedHop()).isEqualTo(PeerId.of("zR2"));
        assertThat(signal.reason()).isEqualTo("unreachable");
        assertThat(codec.toBytes(new RelayPipeFrame(body.capability(), codec.toBytes(signal)))).isEqualTo(bytes);
    }

    @Test
    void theReachHintRidesTheAdvertisementsResourceHints() {
        byte[] bytes = hex("peer_ad_with_via_hint_cbor");
        PeerAdvertisement ad = codec.fromBytes(bytes, PeerAdvertisement.class);
        assertThat(ad.endpoints()).as("dial-only").isEmpty();
        assertThat(RelayPaths.decode(ad.resourceHints().get(RelayPaths.VIA_HINT)))
                .containsExactly(PeerId.of("zR1"), PeerId.of("zR2"));
        assertThat(codec.toBytes(ad)).isEqualTo(bytes);
    }
}
