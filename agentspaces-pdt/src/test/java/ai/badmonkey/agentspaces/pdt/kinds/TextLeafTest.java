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
package ai.badmonkey.agentspaces.pdt.kinds;

import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.TextRun;
import ai.badmonkey.agentspaces.pdt.TextRun.CharId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * PDT 2 phase 0 (P2-T-01): a text leaf holds Fugue runs in one canonical form, reads its text in
 * Fugue order, and unions two replicas' leaves into one leaf with no interleaving.
 */
class TextLeafTest {

    private static TextRun run(String replica, long counter, CharId parent, String side, long length, boolean deleted,
                               String text) {
        return new TextRun(replica, counter, parent, side, length, deleted, text);
    }

    @Test
    void theTextReadsInFugueOrder() {
        TextLeaf leaf = new TextLeaf(List.of(
                run("alice", 0, null, TextRun.RIGHT, 4, false, "Ab😀c"),
                run("alice", 4, new CharId("alice", 1), TextRun.LEFT, 2, true, ""),
                run("bob", 0, new CharId("alice", 3), TextRun.RIGHT, 2, false, "é!")));
        assertThat(leaf.text()).isEqualTo("Ab😀cé!");
        assertThat(TextLeaf.decode(null, leaf.encode())).isEqualTo(leaf);
    }

    @Test
    void twoConcurrentInsertsMergeWithoutInterleavingAndADeleteSticks() {
        TextLeaf base = new TextLeaf(List.of(run("alice", 0, null, TextRun.RIGHT, 2, false, "ac")));
        TextLeaf alice = new TextLeaf(List.of(
                run("alice", 0, null, TextRun.RIGHT, 2, false, "ac"),
                run("alice", 2, new CharId("alice", 1), TextRun.LEFT, 1, false, "b")));
        TextLeaf bob = new TextLeaf(List.of(
                run("alice", 0, null, TextRun.RIGHT, 1, true, ""),
                run("alice", 1, new CharId("alice", 0), TextRun.RIGHT, 1, false, "c"),
                run("bob", 0, new CharId("alice", 1), TextRun.LEFT, 2, false, "XY")));
        assertThat(base.text()).isEqualTo("ac");
        assertThat(alice.text()).isEqualTo("abc");
        assertThat(bob.text()).isEqualTo("XYc");
        TextLeaf merged = alice.union(bob);
        assertThat(merged.text()).isEqualTo("bXYc");
        assertThat(bob.union(alice)).as("the union commutes byte for byte").isEqualTo(merged);
        assertThat(merged.union(merged)).isEqualTo(merged);
        assertThat(merged.runs()).containsExactly(
                run("alice", 0, null, TextRun.RIGHT, 1, true, ""),
                run("alice", 1, new CharId("alice", 0), TextRun.RIGHT, 1, false, "c"),
                run("alice", 2, new CharId("alice", 1), TextRun.LEFT, 1, false, "b"),
                run("bob", 0, new CharId("alice", 1), TextRun.LEFT, 2, false, "XY"));
    }

    @Test
    void ofCompressesCharactersIntoMaximalRuns() {
        TextLeaf split = TextLeaf.of(List.of(
                run("alice", 0, null, TextRun.RIGHT, 1, false, "a"),
                run("alice", 1, new CharId("alice", 0), TextRun.RIGHT, 1, false, "b")));
        assertThat(split.runs()).containsExactly(run("alice", 0, null, TextRun.RIGHT, 2, false, "ab"));
    }

    @Test
    void theLeafRefusesEveryFormButTheCanonicalOne() {
        assertThatThrownBy(() -> new TextLeaf(List.of(
                run("alice", 0, null, TextRun.RIGHT, 1, false, "a"),
                run("alice", 1, new CharId("alice", 0), TextRun.RIGHT, 1, false, "b"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical");
        assertThatThrownBy(() -> new TextLeaf(List.of(
                run("bob", 0, null, TextRun.RIGHT, 1, false, "b"),
                run("alice", 0, null, TextRun.RIGHT, 1, false, "a"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical");
        assertThatThrownBy(() -> new TextLeaf(List.of(
                run("alice", 0, null, TextRun.RIGHT, 2, false, "ab"),
                run("alice", 1, null, TextRun.RIGHT, 1, false, "x"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("twice");
        assertThatThrownBy(() -> new TextLeaf(List.of(run("alice", 0, new CharId("carol", 9), TextRun.RIGHT, 1, false, "a"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("parent carol:9");
        assertThatThrownBy(() -> new TextLeaf(List.of(
                run("alice", 0, new CharId("bob", 0), TextRun.RIGHT, 1, false, "a"),
                run("bob", 0, new CharId("alice", 0), TextRun.RIGHT, 1, false, "b"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cycle");
        Map<String, Object> v2 = new LinkedHashMap<>();
        v2.put("k", Kind.T);
        v2.put("r", List.of(run("alice", 0, null, TextRun.RIGHT, 1, false, "a")));
        v2.put("v", 2L);
        assertThatThrownBy(() -> TextLeaf.decode(null, NodeCodec.encode(v2))).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("text leaf version 2");
        assertThatThrownBy(() -> alice().union(new TextLeaf(List.of(run("alice", 0, null, TextRun.RIGHT, 1, false, "z")))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("alice:0");
    }

    @Test
    void aChildOfTheDocumentRootHangsOnItsRight() {
        // PDT 2 phase 0 (P2-T-01): Fugue's document root is every insert's leftmost neighbour, so it
        // has right children only; a root child on the left would read in another order in a port
        // that walks left children first, and would give one text two encodings.
        assertThatThrownBy(() -> run("alice", 0, null, TextRun.LEFT, 1, false, "a"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("document root");
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("k", Kind.T);
        node.put("r", List.of(run("alice", 0, null, TextRun.RIGHT, 1, false, "a")));
        node.put("v", 1L);
        byte[] bytes = NodeCodec.encode(node);
        int side = indexOf(bytes, new byte[]{0x61, 0x52}) + 1;
        bytes[side] = 0x4c;
        byte[] leftOfRoot = bytes;
        assertThatThrownBy(() -> TextLeaf.decode(null, leftOfRoot)).isInstanceOf(NodeFormatException.class)
                .hasMessageContaining("document root");
    }

    @Test
    void aLeafHoldsAtMostTheNodeByteBoundOfCharacters() {
        // PDT 2 phase 0 (P2-T-01, gate finding G1): a deleted run costs no text bytes, so without a
        // bound a 29-byte node naming one deleted run of 2^40 characters would expand forever in every
        // port; a leaf holds at most MAX_CHARS characters, live and deleted, checked before expansion.
        assertThat(TextLeaf.MAX_CHARS).isEqualTo(NodeCodec.MAX_NODE_BYTES);
        TextLeaf full = new TextLeaf(List.of(run("alice", 0, null, TextRun.RIGHT, TextLeaf.MAX_CHARS, true, "")));
        assertThat(full.text()).isEmpty();
        assertThatThrownBy(() -> new TextLeaf(List.of(run("alice", 0, null, TextRun.RIGHT, TextLeaf.MAX_CHARS + 1, true, ""))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at most 1048576 characters");
        assertThatThrownBy(() -> TextLeaf.of(List.of(
                run("alice", 0, null, TextRun.RIGHT, TextLeaf.MAX_CHARS, true, ""),
                run("bob", 0, null, TextRun.RIGHT, 1, false, "b"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at most 1048576 characters");
        assertThatThrownBy(() -> full.union(new TextLeaf(List.of(run("bob", 0, null, TextRun.RIGHT, 1, false, "b")))))
                .as("a union past the bound").isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 1048576 characters");
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("k", Kind.T);
        node.put("r", List.of(run("a", 0, null, TextRun.RIGHT, 1L << 40, true, "")));
        node.put("v", 1L);
        byte[] hostile = NodeCodec.encode(node);
        assertThat(hostile).hasSize(29);
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThatThrownBy(() -> TextLeaf.decode(null, hostile))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("at most 1048576 characters"));
    }

    @Test
    void replicasSortByTheirUtf8Bytes() {
        // PDT 2 phase 0 (P2-T-01): U+FFFD sorts before U+1F600 in UTF-8 (EF.. before F0..) and after it
        // in UTF-16 (FFFD after D83D); every port compares the UTF-8 bytes.
        String replacement = "\uFFFD";
        String emoji = "\uD83D\uDE00";
        TextLeaf leaf = TextLeaf.of(List.of(
                run(emoji, 0, null, TextRun.RIGHT, 1, false, "y"),
                run(replacement, 0, null, TextRun.RIGHT, 1, false, "x")));
        assertThat(leaf.runs().get(0).replica()).isEqualTo(replacement);
        assertThat(leaf.text()).isEqualTo("xy");
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            if (java.util.Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
                return i;
            }
        }
        throw new AssertionError("needle not found");
    }

    private static TextLeaf alice() {
        return new TextLeaf(List.of(run("alice", 0, null, TextRun.RIGHT, 1, false, "a")));
    }
}
