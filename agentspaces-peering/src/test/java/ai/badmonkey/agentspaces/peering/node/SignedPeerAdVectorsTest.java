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
import ai.badmonkey.agentspaces.peering.wire.WireEncodingVectorsTest;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.HexFormat;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HLC round six, 2026-11-08 (owner rulings F1 and F3): the signed peer advertisement vectors of
 * {@code tools/wire-encoding/vectors.json}, judged by the read every peers stream, beacon and
 * bootstrap hint goes through. Java checks the signature over the raw {@code adBytes}, so a
 * relabelled advertisement verified as long as Jackson could read it; now the advertisement and
 * its wrapper each decode only from their own wire encoding, the roles in element order (JC-4)
 * and the instant and duration as Java prints them.
 */
class SignedPeerAdVectorsTest {

    @TestFactory
    Stream<DynamicTest> everySignedPeerAdVectorIsJudgedAsItsVerdictSays() throws Exception {
        CborCodec codec = CborCodec.defaultCodec();
        return WireEncodingVectorsTest.vectors("signed-peer-ad").stream().map(v -> DynamicTest.dynamicTest(
                v.get("id").asText() + " " + v.get("verdict").asText() + ": " + v.get("why").asText(),
                () -> assertThat(PeerNode.decodeSignedPeerAd(codec, HexFormat.of().parseHex(v.get("hex").asText()))
                        .isPresent()).isEqualTo(v.get("verdict").asText().equals("accept"))));
    }
}
