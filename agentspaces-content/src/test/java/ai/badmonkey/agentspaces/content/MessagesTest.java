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

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class MessagesTest {

    private final CborCodec codec = CborCodec.defaultCodec();

    @Test
    void everyMessageCarriesItsTagFirstAndRoundTrips() {
        byte[] bytes = new byte[300];
        new Random(1).nextBytes(bytes);
        Object[] messages = {
                new Messages.PieceWant("broot", 3, 65_536, 65_536, 42),
                new Messages.Piece("broot", 3, 65_536, bytes, 42),
                new Messages.Have("broot", new byte[]{1, 2}, false),
                new Messages.Cancel("broot", 3, 42),
                new Messages.Reject("broot", 3, Messages.REASON_OVER_BUDGET),
                new Messages.ManifestWant("broot"),
                new Messages.ManifestReply("broot", bytes),
        };
        int[] tags = {1, 2, 3, 4, 5, 6, 7};
        for (int i = 0; i < messages.length; i++) {
            byte[] encoded = codec.toBytes(messages[i]);
            assertThat(Messages.tagOf(codec, encoded)).contains(tags[i]);
            assertThat(encoded[1]).as("the first key is 't'").isEqualTo((byte) 0x61);
            assertThat(encoded[2]).isEqualTo((byte) 't');
            Object back = Messages.decode(codec, encoded, messages[i].getClass()).orElseThrow();
            assertThat(codec.toBytes(back)).isEqualTo(encoded);
        }
        assertThat(Messages.tagOf(codec, new byte[]{(byte) 0xf6})).isEmpty();
        assertThat(Messages.tagOf(codec, new byte[]{0x01, 0x02})).isEmpty();
    }

    @Test
    void treeMessagesCarryTheirTagsFirstAndRoundTrip() {
        byte[] node = new byte[]{(byte) 0xa0};
        String cid = Cids.of(node, Cids.DAG_CBOR);
        String chunk = Cids.of(new byte[0], Cids.RAW);
        Object[] messages = {
                new Messages.NodeWant(7, List.of(cid, chunk)),
                Messages.Nodes.plain(7, List.of(node, new byte[0]), List.of(chunk)),
                Messages.Nodes.sealed(8, new byte[]{1, 2, 3}, 2L, null),
                new Messages.TreeWant(9, cid, List.of(chunk), Messages.MAX_TREE_BUDGET),
                new Messages.TreeEnd(9, List.of(cid), Messages.END_BUDGET),
        };
        int[] tags = {8, 9, 9, 10, 11};
        for (int i = 0; i < messages.length; i++) {
            byte[] encoded = codec.toBytes(messages[i]);
            assertThat(Messages.tagOf(codec, encoded)).contains(tags[i]);
            assertThat(encoded[1]).as("the first key is 't'").isEqualTo((byte) 0x61);
            assertThat(encoded[2]).isEqualTo((byte) 't');
            Object back = Messages.decode(codec, encoded, messages[i].getClass()).orElseThrow();
            assertThat(codec.toBytes(back)).isEqualTo(encoded);
        }
        Messages.Nodes plain = Messages.decode(codec, codec.toBytes(messages[1]), Messages.Nodes.class).orElseThrow();
        assertThat(plain.plaintext()).isTrue();
        assertThat(plain.nodes()).hasSize(2);
        assertThat(plain.missing()).containsExactly(chunk);
        assertThat(plain.sealed()).isNull();
        assertThat(plain.epoch()).isNull();
        Messages.Nodes sealed = Messages.decode(codec, codec.toBytes(messages[2]), Messages.Nodes.class).orElseThrow();
        assertThat(sealed.plaintext()).isFalse();
        assertThat(sealed.nodes()).isNull();
        assertThat(sealed.epoch()).isEqualTo(2L);
        Messages.TreeWant want = Messages.decode(codec, codec.toBytes(messages[3]), Messages.TreeWant.class).orElseThrow();
        assertThat(want.sketch()).isNull();
        assertThat(want.budget()).isEqualTo(64L * 1024 * 1024);
        for (Object m : messages) {
            assertThat(violationOf(m)).as(m.toString()).isEmpty();
        }
        assertThat(Messages.TREE_INDEX).isEqualTo(-1);
        assertThat(Messages.SEALING_AAD_PREFIX).isEqualTo("aspace:pdt|");
    }

    @Test
    void treeMessagesRefuseTheirBoundsByName() {
        byte[] node = new byte[]{(byte) 0xa0};
        String cid = Cids.of(node, Cids.DAG_CBOR);
        List<String> tooMany = java.util.stream.IntStream.range(0, Messages.MAX_WANT_CIDS + 1)
                .mapToObj(i -> Cids.of(new byte[]{(byte) i, (byte) (i >> 8)}, Cids.RAW)).toList();
        assertThat(new Messages.NodeWant(1, List.of()).violation().orElse("")).contains("a NODE_WANT names at least one cid");
        assertThat(new Messages.NodeWant(1, null).violation()).isPresent();
        assertThat(new Messages.NodeWant(1, tooMany).violation().orElse("")).contains("at most 256 cids, got 257");
        assertThat(new Messages.NodeWant(1, List.of("not a cid")).violation().orElse("")).contains("not a CID: not a cid");
        assertThat(new Messages.NodeWant(Messages.PIECE_WANT, 1, List.of(cid)).violation().orElse("")).contains("not NODE_WANT");

        assertThat(new Messages.Nodes(1, null, null, null, null).violation().orElse("")).contains("exactly one of nodes and sealed");
        assertThat(new Messages.Nodes(1, List.of(node), new byte[1], 1L, null).violation().orElse("")).contains("exactly one of nodes and sealed");
        assertThat(new Messages.Nodes(1, null, new byte[1], null, null).violation().orElse("")).contains("names its epoch");
        assertThat(new Messages.Nodes(1, List.of(node), null, 1L, null).violation().orElse("")).contains("no epoch");
        assertThat(Messages.Nodes.sealed(1, new byte[1], -1L, null).violation().orElse("")).contains("negative epoch");
        List<byte[]> nulls = new java.util.ArrayList<>();
        nulls.add(null);
        assertThat(Messages.Nodes.plain(1, nulls, null).violation().orElse("")).contains("a null node");
        assertThat(Messages.Nodes.plain(1, List.of(new byte[Messages.MAX_NODES_BYTES], new byte[1]), null).violation().orElse(""))
                .contains("at most 4194304 bytes");
        assertThat(Messages.Nodes.plain(1, List.of(new byte[Messages.MAX_NODES_BYTES]), null).violation()).isEmpty();
        assertThat(Messages.Nodes.sealed(1, new byte[Messages.MAX_NODES_BYTES + 1], 1L, null).violation().orElse(""))
                .contains("at most 4194304 bytes");
        assertThat(Messages.Nodes.plain(1, List.of(), tooMany).violation().orElse("")).contains("at most 256 missing cids");
        assertThat(Messages.Nodes.plain(1, List.of(), List.of("x")).violation().orElse("")).contains("missing holds a value that is not a CID");
        assertThat(Messages.Nodes.plain(1, List.of(), List.of()).violation()).isEmpty();

        assertThat(new Messages.TreeWant(1, "broot", List.of(), 1).violation().orElse("")).contains("root is not a CID: broot");
        assertThat(new Messages.TreeWant(1, cid, tooMany.subList(0, Messages.MAX_HAVES + 1), 1).violation().orElse(""))
                .contains("at most 64 haves, got 65");
        assertThat(new Messages.TreeWant(1, cid, List.of("nope"), 1).violation().orElse("")).contains("haves holds a value that is not a CID");
        assertThat(new Messages.TreeWant(1, cid, List.of(), 0).violation().orElse("")).contains("budget 0");
        assertThat(new Messages.TreeWant(1, cid, List.of(), Messages.MAX_TREE_BUDGET + 1).violation().orElse(""))
                .contains("budget 67108865");
        assertThat(new Messages.TreeWant(1, cid, null, 1).violation()).as("haves may be absent").isEmpty();
        assertThat(new Messages.TreeWant(Messages.TREE_WANT, 1, cid, List.of(), 1, new byte[]{9}).violation())
                .as("a sketch is reserved and ignored (PD-F-03)").isEmpty();

        assertThat(new Messages.TreeEnd(1, null, Messages.END_DONE).violation().orElse("")).contains("carries a frontier list");
        List<String> bigFrontier = java.util.stream.IntStream.range(0, Messages.MAX_FRONTIER + 1)
                .mapToObj(i -> Cids.of(new byte[]{(byte) i, (byte) (i >> 8)}, Cids.DAG_CBOR)).toList();
        assertThat(new Messages.TreeEnd(1, bigFrontier, Messages.END_DONE).violation().orElse("")).contains("at most 1024 frontier cids, got 1025");
        assertThat(new Messages.TreeEnd(1, List.of(), 0).violation().orElse("")).contains("unknown TREE_END reason 0");
        assertThat(new Messages.TreeEnd(1, List.of(), 5).violation().orElse("")).contains("unknown TREE_END reason 5");
        assertThat(new Messages.TreeEnd(1, List.of("?"), Messages.END_DONE).violation().orElse("")).contains("frontier holds a value that is not a CID");
        for (int reason : new int[]{Messages.END_DONE, Messages.END_BUDGET, Messages.END_HAVES_NOT_HELD, Messages.END_WALK_CAP}) {
            assertThat(new Messages.TreeEnd(1, List.of(cid), reason).violation()).isEmpty();
        }
    }

    private static java.util.Optional<String> violationOf(Object message) {
        if (message instanceof Messages.NodeWant m) {
            return m.violation();
        }
        if (message instanceof Messages.Nodes m) {
            return m.violation();
        }
        if (message instanceof Messages.TreeWant m) {
            return m.violation();
        }
        return ((Messages.TreeEnd) message).violation();
    }

    @Test
    void rootsAreCidV1AndPiecesVerifyAgainstTheTable() {
        byte[] artifact = new byte[3 * 65_536 + 10];
        new Random(2).nextBytes(artifact);
        ContentRoots.Hashed hashed = ContentRoots.hash(artifact, 65_536);
        assertThat(hashed.size()).isEqualTo(artifact.length);
        assertThat(hashed.pieces(65_536)).isEqualTo(4);
        assertThat(hashed.table()).hasSize(4 * 32);
        assertThat(hashed.root()).startsWith("bafkrei").hasSize(59);
        assertThat(ContentRoots.isRoot(hashed.root())).isTrue();
        assertThat(ContentRoots.isRoot("zLegacyBlock")).isFalse();
        byte[] last = java.util.Arrays.copyOfRange(artifact, 3 * 65_536, artifact.length);
        assertThat(ContentRoots.verifies(hashed.table(), 3, last)).isTrue();
        last[0] ^= 1;
        assertThat(ContentRoots.verifies(hashed.table(), 3, last)).isFalse();
        assertThat(ContentRoots.verifies(hashed.table(), 4, last)).isFalse();
        assertThat(ContentRoots.rootOf(hashed.table())).isEqualTo(hashed.root());
    }
}
