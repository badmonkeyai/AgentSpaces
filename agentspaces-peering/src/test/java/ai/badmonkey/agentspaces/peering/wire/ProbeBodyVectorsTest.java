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
package ai.badmonkey.agentspaces.peering.wire;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The PING and ACK vectors of golden.json with {@code hashes} and {@code have} present and absent (SPEC §5.2;
 * srad/replication-by-entry.md RE-L-05, phase 2): each body decodes to what it names and re-encodes to the same
 * bytes, the twenty-stream PING carries twenty hashes (more than the sixteen of the old probe bound, RE-T-07), and the
 * signed frame decodes to a PING carrying the first body.
 */
class ProbeBodyVectorsTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final CborCodec codec = CborCodec.defaultCodec();
    private static Map<String, Object> golden;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required but none of " + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
        if (GOLDEN_REQUIRED) {
            assertThat(golden).containsKey("ping_hashes_have_body_cbor");
        }
        assumeTrue(golden.containsKey("ping_hashes_have_body_cbor"), "golden.json predates the probe vectors");
    }

    private static byte[] hex(String name) {
        return HexFormat.of().parseHex((String) golden.get(name));
    }

    private static Bodies.Ping ping(String name) {
        Bodies.Ping ping = codec.fromWire(hex(name), Bodies.Ping.class);
        assertThat(codec.toBytes(ping)).as(name + " re-encodes to its own bytes").isEqualTo(hex(name));
        assertThat(ping.nonce()).isEqualTo(42);
        assertThat(ping.root()).as("no vector carries the root").isNull();
        return ping;
    }

    private static Bodies.Ack ack(String name) {
        Bodies.Ack ack = codec.fromWire(hex(name), Bodies.Ack.class);
        assertThat(codec.toBytes(ack)).as(name + " re-encodes to its own bytes").isEqualTo(hex(name));
        assertThat(ack.nonce()).isEqualTo(42);
        assertThat(ack.onBehalf()).isNull();
        assertThat(ack.root()).isNull();
        return ack;
    }

    @Test
    void everyPingAndAckBodyDecodesToWhatItNamesAndBackToItsBytes() {
        Bodies.Ping both = ping("ping_hashes_have_body_cbor");
        assertThat(both.hashes()).containsOnlyKeys("peers", "space:jobs");
        assertThat(both.have()).singleElement().satisfies(item -> assertThat(item.streamId()).isEqualTo("space:jobs"));
        Bodies.Ping hashesOnly = ping("ping_hashes_body_cbor");
        assertThat(hashesOnly.hashes()).containsOnlyKeys("peers", "space:jobs");
        assertThat(hashesOnly.have()).isNull();
        Bodies.Ping haveOnly = ping("ping_have_body_cbor");
        assertThat(haveOnly.hashes()).as("a busy member's probe: no hashes").isNull();
        assertThat(haveOnly.have()).hasSize(1);
        Bodies.Ping twenty = ping("ping_hashes_twenty_streams_body_cbor");
        assertThat(twenty.hashes()).hasSize(20).allSatisfy((stream, hash) -> assertThat(hash).hasSize(32));

        Bodies.Ack bare = ack("ack_body_cbor");
        assertThat(bare.hashes()).isNull();
        assertThat(bare.have()).isNull();
        assertThat(ack("ack_hashes_have_body_cbor").hashes()).containsOnlyKeys("peers", "space:jobs");
        assertThat(ack("ack_hashes_body_cbor").have()).isNull();
        assertThat(ack("ack_have_body_cbor").hashes()).isNull();
    }

    @Test
    void theSignedFrameCarriesThePingWithHashesAndHave() {
        Optional<Envelope> envelope = new WireCodec(codec).decode(hex("ping_hashes_have_frame_cbor"));
        assertThat(envelope).as("the frame verifies").isPresent();
        assertThat(envelope.get().kind()).isEqualTo(Envelope.Kind.PING);
        assertThat(envelope.get().body()).isEqualTo(hex("ping_hashes_have_body_cbor"));
    }
}
