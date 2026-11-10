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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The level-2 forms of the content swarm (PDT 2 phase 3, P2-S-07 and P2-F-05; srad/agentspaces-pdt2.md
 * §6.10.3 rules 12 to 15 and §6.10.4): tag 12 {@code CHALLENGE} with its picks, the appended
 * {@code NODES.deltas} and {@code TREE_END.sketch}, and the sketch {@code TREE_WANT}. Each reads through
 * the strict decoder, and a message without an appended field keeps its level-1 bytes.
 */
class Level2MessagesTest {

    private final CborCodec codec = CborCodec.defaultCodec();
    private final String node = Cids.of(new byte[]{(byte) 0xa0}, Cids.DAG_CBOR);
    private final String chunk = Cids.of(new byte[0], Cids.RAW);
    /** A BLAKE3 identity: a CIDv1 of codec raw, multihash 0x1e, 32 bytes. */
    static final String BLOB = blake3Identity((byte) 7);

    static String blake3Identity(byte fill) {
        byte[] binary = new byte[36];
        binary[0] = 0x01;
        binary[1] = 0x55;
        binary[2] = 0x1e;
        binary[3] = 0x20;
        java.util.Arrays.fill(binary, 4, 36, fill);
        return ai.badmonkey.agentspaces.common.codec.Multibase.base32(binary);
    }

    @Test
    void aChallengeCarriesTagTwelveFirstAndReadsStrictly() {
        Messages.Challenge challenge = new Messages.Challenge(42, node,
                List.of(Messages.Pick.node(node), Messages.Pick.chunkGroup(BLOB, 1024L, 1024)));
        byte[] encoded = codec.toBytes(challenge);
        assertThat(Messages.peekTag(codec, encoded)).contains(Messages.CHALLENGE);
        assertThat(encoded[1]).as("the first key is 't'").isEqualTo((byte) 0x61);
        Messages.Challenge back = Messages.decode(codec, encoded, Messages.Challenge.class).orElseThrow();
        assertThat(codec.toBytes(back)).isEqualTo(encoded);
        assertThat(back.violation()).isEmpty();
        assertThat(back.picks().get(0).names()).as("a pick without offset and length names a node").isEqualTo(Messages.Pick.NODE);
        assertThat(back.picks().get(1).names()).isEqualTo(Messages.Pick.CHUNK_GROUP);
        String nodePick = HexFormat.of().formatHex(codec.toBytes(Messages.Pick.node(node)));
        assertThat(nodePick).as("a node pick omits offset and length").doesNotContain(HexFormat.of().formatHex("offset".getBytes()));
    }

    @Test
    void aChallengeOutsideItsBoundsIsRefusedByName() {
        List<Messages.Pick> seventeen = new ArrayList<>();
        for (int i = 0; i < Messages.MAX_PICKS + 1; i++) {
            seventeen.add(Messages.Pick.node(node));
        }
        assertThat(new Messages.Challenge(1, node, seventeen).violation()).get().asString().contains("at most 16");
        assertThat(new Messages.Challenge(1, node, List.of()).violation()).get().asString().contains("at least one");
        assertThat(new Messages.Challenge(1, "nope", List.of(Messages.Pick.node(node))).violation()).get().asString().contains("root");
        assertThat(new Messages.Challenge(1, node, List.of(Messages.Pick.node("nope"))).violation()).get().asString().contains("not a CID");
        assertThat(new Messages.Challenge(1, node, List.of(new Messages.Pick(BLOB, 0L, null))).violation()).get().asString()
                .contains("both or neither");
        assertThat(new Messages.Challenge(1, node, List.of(Messages.Pick.chunkGroup(BLOB, -1L, 10))).violation()).get().asString()
                .contains("offset");
        assertThat(new Messages.Challenge(1, node, List.of(Messages.Pick.chunkGroup(BLOB, 0L, 0))).violation()).get().asString()
                .contains("length");
        assertThat(new Messages.Challenge(9, 1, node, List.of(Messages.Pick.node(node))).violation()).get().asString()
                .contains("not CHALLENGE");
        assertThat(new Messages.Challenge(1, node, List.of(Messages.Pick.chunkGroup(chunk, 0L, 10))).violation()).as(
                "a chunk group names a BLAKE3 identity").get().asString().contains("BLAKE3 identity");
        assertThat(new Messages.Challenge(1, node, List.of(Messages.Pick.node(BLOB))).violation()).as(
                "a node pick names a sha2-256 CID").get().asString().contains("sha2-256");
    }

    @Test
    void nodesWithoutDeltasKeepTheirLevelOneBytesAndDeltasDecode() {
        Messages.Nodes plain = Messages.Nodes.plain(7, List.of(new byte[]{(byte) 0xa0}), null);
        Messages.Nodes sixFields = new Messages.Nodes(Messages.NODES, 7, List.of(new byte[]{(byte) 0xa0}), null, null, null);
        assertThat(codec.toBytes(plain)).isEqualTo(codec.toBytes(sixFields));
        assertThat(HexFormat.of().formatHex(codec.toBytes(plain))).doesNotContain(HexFormat.of().formatHex("deltas".getBytes()));

        Messages.Nodes withDeltas = new Messages.Nodes(Messages.NODES, 7, List.of(), null, null, null,
                List.of(new Messages.NodeDelta(chunk, chunk, new byte[]{0x28, (byte) 0xb5})));
        byte[] encoded = codec.toBytes(withDeltas);
        Messages.Nodes back = Messages.decode(codec, encoded, Messages.Nodes.class).orElseThrow();
        assertThat(codec.toBytes(back)).isEqualTo(encoded);
        assertThat(back.deltas()).hasSize(1);
        assertThat(back.violation()).isEmpty();
        Messages.Nodes badDelta = new Messages.Nodes(Messages.NODES, 7, List.of(), null, null, null,
                List.of(new Messages.NodeDelta("nope", chunk, new byte[1])));
        assertThat(badDelta.violation()).get().asString().contains("deltas");
    }

    @Test
    void aSealedNodesThatCarriesDeltasIsRefused() {
        // PDT 2 phase 4 measures (owner default of 2026-11-09, srad/agentspaces-pdt2.md §10.14): frames travel in
        // plaintext answers only, so every port refuses a sealed answer that carries them
        List<Messages.NodeDelta> frames = List.of(new Messages.NodeDelta(chunk, chunk, new byte[]{0x28, (byte) 0xb5}));
        Messages.Nodes sealed = new Messages.Nodes(Messages.NODES, 7, null, new byte[]{1, 2, 3}, 0L, null, frames);
        assertThat(sealed.violation()).get().asString().contains("sealed").contains("deltas");
        Messages.Nodes sealedEmpty = new Messages.Nodes(Messages.NODES, 7, null, new byte[]{1, 2, 3}, 0L, null, List.of());
        assertThat(sealedEmpty.violation()).as("the field present at all").isPresent();
        Messages.Nodes decoded = Messages.decode(codec, codec.toBytes(sealed), Messages.Nodes.class).orElseThrow();
        assertThat(decoded.violation()).as("the wire form reads, and the bounds refuse it").isPresent();
        assertThat(Messages.Nodes.sealed(7, new byte[]{1, 2, 3}, 0L, null).violation()).isEmpty();
    }

    @Test
    void aSketchTreeEndCarriesReasonFiveAndAnEmptyFrontier() {
        Messages.TreeEnd level1 = new Messages.TreeEnd(9, List.of(node), Messages.END_BUDGET);
        assertThat(codec.toBytes(level1)).isEqualTo(codec.toBytes(new Messages.TreeEnd(Messages.TREE_END, 9, List.of(node),
                Messages.END_BUDGET)));
        assertThat(HexFormat.of().formatHex(codec.toBytes(level1))).doesNotContain(HexFormat.of().formatHex("sketch".getBytes()));

        Messages.TreeEnd round = new Messages.TreeEnd(Messages.TREE_END, 9, List.of(), Messages.END_SKETCH, new byte[]{1, 2});
        byte[] encoded = codec.toBytes(round);
        assertThat(codec.toBytes(Messages.decode(codec, encoded, Messages.TreeEnd.class).orElseThrow())).isEqualTo(encoded);
        assertThat(round.violation()).isEmpty();
        assertThat(new Messages.TreeEnd(9, List.of(), Messages.END_SKETCH).violation()).get().asString().contains("sketch");
        assertThat(new Messages.TreeEnd(Messages.TREE_END, 9, List.of(node), Messages.END_SKETCH, new byte[]{1}).violation())
                .get().asString().contains("frontier");
    }

    @Test
    void aSketchTreeWantNamesNoRootAndNoHaves() {
        Messages.TreeWant sketch = new Messages.TreeWant(Messages.TREE_WANT, 3, "", List.of(), Messages.MAX_TREE_BUDGET, new byte[]{1});
        assertThat(sketch.violation()).isEmpty();
        assertThat(sketch.reconciles()).isTrue();
        assertThat(new Messages.TreeWant(3, node, List.of(), Messages.MAX_TREE_BUDGET).reconciles()).isFalse();
        assertThat(new Messages.TreeWant(Messages.TREE_WANT, 3, "", List.of(), Messages.MAX_TREE_BUDGET, null).violation())
                .get().asString().contains("root");
        assertThat(new Messages.TreeWant(Messages.TREE_WANT, 3, "", List.of(chunk), Messages.MAX_TREE_BUDGET, new byte[]{1}).violation())
                .get().asString().contains("haves");
    }
}
