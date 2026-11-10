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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HLC round six, 2026-11-08 (owner rulings F1 and F3): the space delta vectors of
 * {@code tools/wire-encoding/vectors.json}, state and claim deltas Java signed with the golden
 * fixture key, read as {@link ReplicatedSpace} reads every delta from a rumor. A relabelled delta
 * (text chunked another way, tags out of order, an integer where text or a boolean belongs, a
 * number as text, a field the schema does not name, a null for an omitted field, a claim in
 * another field order, a bid that is not a finite double) is undecodable, and every delta Java
 * writes decodes, a bid of -0.0 and dots on the wire in either order included.
 */
class SpaceWireVectorsTest {

    private static final Path VECTORS = Path.of("..", "tools", "wire-encoding", "vectors.json");

    @TestFactory
    Stream<DynamicTest> everySpaceDeltaVectorDecodesAsItsVerdictSays() throws Exception {
        assertThat(VECTORS).as("the wire-encoding vectors ship with the repository").exists();
        CborCodec codec = CborCodec.defaultCodec();
        JsonNode vectors = new ObjectMapper().readTree(Files.readAllBytes(VECTORS)).get("vectors");
        return StreamSupport.stream(vectors.spliterator(), false)
                .filter(v -> v.get("carrier").asText().equals("space-delta"))
                .map(v -> DynamicTest.dynamicTest(
                        v.get("id").asText() + " " + v.get("verdict").asText() + ": " + v.get("why").asText(), () -> {
                            boolean decoded;
                            try {
                                decoded = codec.fromWire(HexFormat.of().parseHex(v.get("hex").asText()), SpaceWire.Delta.class) != null;
                            } catch (RuntimeException undecodable) {
                                decoded = false;
                            }
                            assertThat(decoded).isEqualTo(v.get("verdict").asText().equals("accept"));
                        }));
    }
}
