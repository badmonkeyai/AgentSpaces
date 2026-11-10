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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.TextRun;
import ai.badmonkey.agentspaces.pdt.TextRun.CharId;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import ai.badmonkey.agentspaces.pdt.kinds.TextLeaf;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PDT 2 phase 6, group T (srad/agentspaces-pdt2.md §3.9): the {@link PText} value. */
class PTextTest {

    @Test
    void insertDeleteToStringAndLengthCountCodePointsAndNeverChangeTheReceiver() {
        PText empty = PText.empty("alice");
        PText text = empty.insert(0, "Hello").insert(5, " world").insert(5, ",").insert(0, "😀 ");
        assertThat(text.toString()).isEqualTo("😀 Hello, world");
        assertThat(text.length()).isEqualTo(14);
        PText cut = text.delete(0, 2).delete(5, 1);
        assertThat(cut.toString()).isEqualTo("Hello world");
        assertThat(cut.length()).isEqualTo(11);
        assertThat(text.toString()).isEqualTo("😀 Hello, world");
        assertThat(empty.length()).isZero();
        assertThat(cut.delete(0, 0)).isSameAs(cut);
        assertThat(cut.insert(3, "")).isSameAs(cut);
        assertThatThrownBy(() -> cut.insert(12, "x")).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> cut.delete(10, 2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> PText.empty("")).isInstanceOf(IllegalArgumentException.class);
    }

    /** P2-T-01: the edits of {@code pdt_text_merge} write its two leaves and their union byte for byte. */
    @Test
    void theEditsOfTheGoldenMergeVectorWriteItsLeavesByteForByte() {
        PText base = PText.empty("alice").insert(0, "ac");
        PText alice = base.insert(1, "b");
        PText bob = base.withReplica("bob").insert(1, "XY").delete(0, 1);
        assertThat(alice.leaf().encode()).isEqualTo(PdtVectors.textAlice().encode());
        assertThat(bob.leaf().encode()).isEqualTo(PdtVectors.textBob().encode());
        PText merged = alice.merge(bob);
        assertThat(merged.toString()).isEqualTo("bXYc");
        assertThat(merged.leaf()).isEqualTo(PdtVectors.textAlice().union(PdtVectors.textBob()));
        assertThat(bob.merge(alice).leaf()).isEqualTo(merged.leaf());
        assertThat(merged.insert(4, "!").leaf().runs()).contains(
                new TextRun("alice", 3, new CharId("alice", 1), TextRun.RIGHT, 1, false, "!"));
    }

    @Test
    void typingAtTheCursorExtendsOneRunAndALeafReadsBackAsTheSameText() {
        PText text = PText.empty("alice");
        for (int i = 0; i < 500; i++) {
            text = text.insert(text.length(), "x");
        }
        text = text.insert(250, "abc").insert(253, "def").delete(100, 50);
        assertThat(text.leaf().runs()).hasSize(4);
        PText read = PText.of(TextLeaf.decode(null, text.leaf().encode()), "bob");
        assertThat(read.toString()).isEqualTo(text.toString());
        assertThat(read.leaf()).isEqualTo(text.leaf());
        assertThat(read.insert(0, "y").leaf().runs().get(read.leaf().runs().size()).replica()).isEqualTo("bob");
        assertThat(PText.codec("carol").decode(PText.codec("alice").encode(text)).toString()).isEqualTo(text.toString());
    }

    /** P2-T-03: two replicas and 1,000 random concurrent edits converge, and no insert interleaves with the other's. */
    @Test
    void twoReplicasConvergeAfterAThousandConcurrentEditsWithoutInterleaving() {
        Random random = new Random(47);
        for (int trial = 0; trial < 20; trial++) {
            PText base = PText.empty("base");
            base = base.insert(0, "the quick brown fox jumps over the lazy dog");
            PText a = base.withReplica("alice");
            PText b = base.withReplica("bob");
            Map<String, List<long[]>> inserts = new HashMap<>();
            for (int e = 0; e < 1000; e++) {
                boolean onA = random.nextBoolean();
                PText t = onA ? a : b;
                t = edit(t, random, inserts);
                if (onA) {
                    a = t;
                } else {
                    b = t;
                }
            }
            long started = System.nanoTime();
            PText ab = a.merge(b);
            long mergeNanos = System.nanoTime() - started;
            PText ba = b.merge(a);
            if (trial == 19) {
                // P2-Q-PRF-09: the merge time and the bytes each edit adds to the leaf, for the record of §10.10
                System.out.printf("PDT 2 phase 6 (P2-Q-PRF-09): 1,000 concurrent edits by two replicas: merge %.1f ms, "
                        + "leaf growth %.1f bytes per edit%n", mergeNanos / 1e6,
                        (ab.leaf().encode().length - base.leaf().encode().length) / 1000.0);
            }
            assertThat(ab.leaf()).isEqualTo(ba.leaf());
            assertThat(ab.toString()).isEqualTo(ba.toString());
            List<CharId> order = ab.order();
            Map<CharId, Integer> at = new HashMap<>();
            for (int i = 0; i < order.size(); i++) {
                at.put(order.get(i), i);
            }
            for (Map.Entry<String, List<long[]>> mine : inserts.entrySet()) {
                for (long[] op : mine.getValue()) {
                    int from = at.get(new CharId(mine.getKey(), op[0]));
                    int to = at.get(new CharId(mine.getKey(), op[0] + op[1] - 1));
                    for (int i = from; i <= to; i++) {
                        assertThat(order.get(i).replica()).as("trial %d: an insert of %s interleaves", trial, mine.getKey())
                                .isNotEqualTo(mine.getKey().equals("alice") ? "bob" : "alice");
                    }
                }
            }
        }
    }

    /** P2-Q-TST-02: the t merge over 10,000 small cases converges in either order and keeps each side's live text. */
    @Test
    void tenThousandSmallConcurrentCasesConvergeInEitherOrder() {
        Random random = new Random(50);
        for (int c = 0; c < 10_000; c++) {
            PText base = PText.empty("base");
            for (int e = random.nextInt(4); e > 0; e--) {
                base = edit(base, random, new HashMap<>());
            }
            PText a = base.withReplica("alice");
            PText b = base.withReplica("bob");
            Map<String, List<long[]>> inserts = new HashMap<>();
            for (int e = random.nextInt(6); e > 0; e--) {
                a = edit(a, random, inserts);
            }
            for (int e = random.nextInt(6); e > 0; e--) {
                b = edit(b, random, inserts);
            }
            PText ab = a.merge(b);
            assertThat(ab.leaf()).as("case %d", c).isEqualTo(b.merge(a).leaf());
            assertThat(ab.merge(a).leaf()).isEqualTo(ab.leaf());
            assertThat(ab.length()).isLessThanOrEqualTo(a.length() + b.length());
        }
    }

    /**
     * P2-T-03 across three replicas (PDT 2 phase 6 review): the union is associative as well as
     * commutative, so replicas that merge one another's texts in any order and grouping hold the
     * same leaf.
     */
    @Test
    void threeReplicasConvergeWhateverOrderAndGroupingTheyMergeIn() {
        Random random = new Random(51);
        for (int c = 0; c < 2_000; c++) {
            PText base = PText.empty("base");
            for (int e = random.nextInt(4); e > 0; e--) {
                base = edit(base, random, new HashMap<>());
            }
            PText[] sides = {base.withReplica("alice"), base.withReplica("bob"), base.withReplica("carol")};
            for (int s = 0; s < sides.length; s++) {
                for (int e = random.nextInt(5); e > 0; e--) {
                    sides[s] = edit(sides[s], random, new HashMap<>());
                }
            }
            TextLeaf left = sides[0].merge(sides[1]).merge(sides[2]).leaf();
            assertThat(sides[0].merge(sides[1].merge(sides[2])).leaf()).as("case %d", c).isEqualTo(left);
            assertThat(sides[2].merge(sides[0]).merge(sides[1]).leaf()).as("case %d", c).isEqualTo(left);
            assertThat(sides[1].merge(sides[2]).merge(sides[0].merge(sides[1])).leaf()).as("case %d", c).isEqualTo(left);
        }
    }

    /**
     * P2-T-02 and PD-N-04 (PDT 2 phase 6 review): a text whose {@code t} node would pass the 1 MiB
     * node bound is refused when it enters a map, as an {@code x} value of that size is, and two
     * texts whose union would pass it go to the resolver.
     */
    @Test
    void aTextOverTheNodeBoundIsRefusedOnTheWayInAndAUnionOverItGoesToTheResolver() {
        NodeStore store = NodeStore.inMemory();
        Codec<PText> texts = PText.codec("alice");
        PMap<String, PText> empty = PMap.empty(store, Codec.of(String.class), texts);
        PText huge = leftChain("abcdefgh", 36_000);
        assertThat(huge.leaf().runs()).hasSize(36_000);
        assertThatThrownBy(() -> empty.assoc("doc", huge))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 MiB node bound");

        PMap<String, PText> mine = empty.assoc("doc", leftChain("alicealx", 20_000));
        PMap<String, PText> theirs = empty.assoc("doc", leftChain("bobbobbo", 20_000));
        List<String> resolved = new ArrayList<>();
        PMap<String, PText> merged = mine.merge3(empty, theirs, (key, b, m, t) -> {
            resolved.add(key);
            return t;
        });
        assertThat(resolved).as("the union of 40,000 runs passes one t node").containsExactly("doc");
        assertThat(merged.get("doc").orElseThrow()).isEqualTo(theirs.get("doc").orElseThrow());
        assertThat(merged.root()).isEqualTo(theirs.root());
    }

    /** A text of {@code n} characters, each the left child of the one before it, so each is a run of its own. */
    private static PText leftChain(String replica, int n) {
        List<TextRun> runs = new ArrayList<>(n);
        runs.add(new TextRun(replica, 0, null, TextRun.RIGHT, 1, false, "a"));
        for (int i = 1; i < n; i++) {
            runs.add(new TextRun(replica, i, new CharId(replica, i - 1), TextRun.LEFT, 1, false, "a"));
        }
        return PText.of(TextLeaf.of(runs), replica);
    }

    /** One random edit: an insert of 1 to 8 characters (recorded by its first counter and length) or a delete. */
    private static PText edit(PText t, Random random, Map<String, List<long[]>> inserts) {
        if (t.length() > 0 && random.nextInt(4) == 0) {
            long at = random.nextInt((int) t.length());
            return t.delete(at, Math.min(t.length() - at, 1 + random.nextInt(4)));
        }
        int k = 1 + random.nextInt(8);
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < k; i++) {
            s.append((char) ('a' + random.nextInt(26)));
        }
        long first = 1 + t.order().stream().filter(id -> id.replica().equals(t.replica()))
                .mapToLong(CharId::counter).max().orElse(-1);
        inserts.computeIfAbsent(t.replica(), r -> new ArrayList<>()).add(new long[] {first, k});
        return t.insert(random.nextInt((int) t.length() + 1), s.toString());
    }

    /**
     * P2-T-04 as the build restated it (§10.10): a run-compressed leaf at rest, measured on a 100 KB
     * document built by 10,000 edits, in two workloads: edit sessions (the cursor moves on three
     * edits in ten) and scattered edits (every edit at a random position).
     */
    @Test
    void aHundredKilobyteDocumentOfTenThousandEditsAtRest() {
        String replica = PText.replicaOf(AgentId.parse(PdtVectors.GOLDEN_WRITER));
        double sessions = bytesPerLiveCharacter(replica, 0.3, new Random(48));
        double scattered = bytesPerLiveCharacter(replica, 1.0, new Random(49));
        System.out.printf("PDT 2 phase 6 (P2-T-04): bytes per live character at rest: edit sessions %.3f, "
                + "scattered edits %.3f%n", sessions, scattered);
        assertThat(sessions).isLessThan(3.0);
        assertThat(scattered).isLessThan(5.5);
    }

    private static double bytesPerLiveCharacter(String replica, double moves, Random random) {
        PText t = PText.empty(replica);
        long cursor = 0;
        for (int e = 0; e < 10_000; e++) {
            if (e == 0 || random.nextDouble() < moves) {
                cursor = t.length() == 0 ? 0 : random.nextInt((int) t.length() + 1);
            }
            if (cursor > 0 && random.nextInt(100) < 15) {
                long k = Math.min(cursor, 1 + random.nextInt(9));
                t = t.delete(cursor - k, k);
                cursor -= k;
            } else {
                int k = 1 + random.nextInt(23);
                StringBuilder s = new StringBuilder();
                for (int i = 0; i < k; i++) {
                    s.append((char) ('a' + random.nextInt(26)));
                }
                t = t.insert(cursor, s.toString());
                cursor += k;
            }
        }
        assertThat(t.length()).isBetween(90_000L, 120_000L);
        TextLeaf leaf = t.leaf();
        long bytes = leaf.encode().length;
        assertThat((double) (bytes - t.length()) / leaf.runs().size()).as("bytes a run costs beyond its text").isLessThan(34);
        return (double) bytes / t.length();
    }

    /** P2-T-02: a map value of text stays inline up to 1 KiB and spills to a {@code t} node above it. */
    @Test
    void aTextMapValueSpillsToATNodeAboveOneKibibyteAndAnEscalatedPayloadKeepsLevelOneKinds() {
        NodeStore store = NodeStore.inMemory();
        Codec<PText> texts = PText.codec("alice");
        PText small = PText.empty("alice").insert(0, "a short note");
        PText large = PText.empty("alice").insert(0, "x".repeat(2000));
        PMap<String, PText> map = PMap.empty(store, Codec.of(String.class), texts).assoc("small", small).assoc("large", large);
        Map<String, byte[]> written = map.editor().done().nodes();
        String root = map.root();
        assertThat(kinds(store, root)).containsExactlyInAnyOrder(Kind.M, Kind.T);
        PMap<String, PText> read = PMap.load(store, root, Codec.of(String.class), PText.codec("bob"));
        assertThat(read.get("small").orElseThrow().toString()).isEqualTo("a short note");
        assertThat(read.get("large").orElseThrow().toString()).isEqualTo("x".repeat(2000));
        assertThat(read.get("large").orElseThrow().replica()).isEqualTo("bob");
        assertThat(written).isNotNull();

        byte[] payload = texts.encode(large);
        PayloadTree.Written tree = PayloadTree.write(payload, store);
        assertThat(kinds(store, tree.root())).as("rule 7: an escalated payload holds level-1 kinds").containsExactly(Kind.X);
        assertThat(PayloadTree.read(tree.root(), store, NodeLoader.of(store))).isEqualTo(payload);
    }

    private static List<String> kinds(NodeStore store, String root) {
        List<String> out = new ArrayList<>();
        for (String cid : store.reachable(root)) {
            out.add(NodeCodec.kindOf(store.get(cid).orElseThrow()));
        }
        return out;
    }

    /** P2-T-03: a key changed on both sides to two texts merges by union, and the resolver never runs. */
    @Test
    void aTextChangedOnBothSidesMergesByUnionWithoutTheResolver() {
        NodeStore store = NodeStore.inMemory();
        Codec<PText> texts = PText.codec("alice");
        PText ac = PText.empty("alice").insert(0, "ac");
        PMap<String, PText> base = PMap.empty(store, Codec.of(String.class), texts).assoc("doc", ac).assoc("n", ac);
        PMap<String, PText> mine = base.assoc("doc", ac.insert(1, "b"));
        PMap<String, PText> theirs = base.assoc("doc", ac.withReplica("bob").insert(1, "XY").delete(0, 1));
        PMap<String, PText> merged = mine.merge3(base, theirs, (key, b, m, t) -> {
            throw new AssertionError("the resolver ran for " + key);
        });
        assertThat(merged.get("doc").orElseThrow().toString()).isEqualTo("bXYc");
        assertThat(merged.get("n").orElseThrow().toString()).isEqualTo("ac");
        PMap<String, PText> removed = base.dissoc("doc");
        List<String> resolved = new ArrayList<>();
        PMap<String, PText> kept = mine.merge3(base, removed, (key, b, m, t) -> {
            resolved.add(key);
            return m;
        });
        assertThat(resolved).as("a removal against an edit still goes to the resolver").containsExactly("doc");
        assertThat(kept.get("doc").orElseThrow().toString()).isEqualTo("abc");
    }

    @Test
    void theReplicaOfAnAgentIsEightBase32CharactersOfItsHash() {
        String replica = PText.replicaOf(AgentId.parse(PdtVectors.GOLDEN_WRITER));
        assertThat(replica).hasSize(8).matches("[a-z2-7]{8}");
        assertThat(PText.replicaOf(AgentId.parse(PdtVectors.GOLDEN_WRITER))).isEqualTo(replica);
        assertThat(PText.replicaOf(AgentId.parse(PdtVectors.GOLDEN_WRITER + "2"))).isNotEqualTo(replica);
    }
}
