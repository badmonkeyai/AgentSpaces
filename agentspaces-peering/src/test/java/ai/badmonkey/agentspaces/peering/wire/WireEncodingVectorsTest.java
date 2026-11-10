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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HLC round six, 2026-11-08 (owner rulings F1 and F3): the frame vectors of {@code tools/wire-encoding/vectors.json}. Java signed each base carrier with the
 * golden fixture key, and each relabel keeps that signature over an altered form, so a receiver
 * that verified over a repaired re-encoding accepted it. A frame decodes only from its own wire
 * encoding, and its body only from the body's.
 */
public class WireEncodingVectorsTest {

    private static final Path VECTORS = Path.of("..", "tools", "wire-encoding", "vectors.json");
    private static final HexFormat HEX = HexFormat.of();

    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    public static List<JsonNode> vectors(String carrier) throws Exception {
        assertThat(VECTORS).as("the wire-encoding vectors ship with the repository").exists();
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode v : new ObjectMapper().readTree(Files.readAllBytes(VECTORS)).get("vectors")) {
            if (v.get("carrier").asText().equals(carrier)) {
                out.add(v);
            }
        }
        return out;
    }

    /** Whether a frame decodes and verifies, and its body decodes as its kind's record, as PeerNode reads it. */
    private boolean frameAccepted(byte[] frame) {
        Optional<Envelope> envelope = wire.decode(frame);
        if (envelope.isEmpty()) {
            return false;
        }
        Class<?> body = switch (envelope.get().kind()) {
            case PING -> Bodies.Ping.class;
            case DIGEST -> Bodies.Digest.class;
            default -> throw new IllegalArgumentException("no vector carries " + envelope.get().kind());
        };
        try {
            codec.fromWire(envelope.get().body(), body);
            return true;
        } catch (RuntimeException undecodable) {
            return false;
        }
    }

    @TestFactory
    Stream<DynamicTest> everyFrameVectorIsJudgedAsItsVerdictSays() throws Exception {
        return vectors("frame").stream().map(v -> DynamicTest.dynamicTest(
                v.get("id").asText() + " " + v.get("verdict").asText() + ": " + v.get("why").asText(),
                () -> assertThat(frameAccepted(HEX.parseHex(v.get("hex").asText())))
                        .isEqualTo(v.get("verdict").asText().equals("accept"))));
    }
}
