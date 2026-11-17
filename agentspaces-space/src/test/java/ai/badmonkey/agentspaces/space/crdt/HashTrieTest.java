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
package ai.badmonkey.agentspaces.space.crdt;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.HEX;
import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.ground;
import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.path;
import static ai.badmonkey.agentspaces.space.crdt.TrieSupport.stateSlot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fingerprinted trie of srad/replication-by-entry.md §3.1 against SPEC annex I.6: the annex's three
 * illustrations byte for byte, the empty space, the split and the collapse at the threshold at the root and deeper,
 * the audit's view of a node, the text digest a slot renders, and the laziness that keeps a batch from hashing.
 */
class HashTrieTest {

    @Test
    void theEmptySpaceHashesZeroBytes() {
        HashTrie<String> empty = HashTrie.empty();
        assertThat(HEX.formatHex(empty.streamHash()))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertThat(empty.size()).isZero();
        assertThat(empty.node(new byte[0]).kind()).isEqualTo(AuditNode.Kind.EMPTY);
        assertThat(empty.node(new byte[0]).fingerprint()).isEqualTo(new byte[16]);
        assertThat(empty.legacyDigest()).isEmpty();
    }

    /** Annex I.6: the slot for {@code a} holding {@code e:a=1} and {@code c:a=2} is a leaf of 11 bytes. */
    @Test
    void theOneSlotIllustration() {
        Slot<String> a = stateSlot("a", "1").withClaim("claim", "2");
        assertThat(HEX.formatHex(a.key())).startsWith("ca978112");
        assertThat(a.nibble(0)).isEqualTo(0xc);
        assertThat(a.nibble(1)).isEqualTo(0xa);
        assertThat(a.nibble(2)).isEqualTo(9);
        assertThat(a.nibble(3)).isEqualTo(7);

        HashTrie<String> trie = HashTrie.<String>empty().put(a);
        AuditNode root = trie.node(new byte[0]);
        assertThat(root.kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(root.count()).isEqualTo(1);
        assertThat(root.lines()).isEqualTo("c:a=2\ne:a=1");
        assertThat(root.preimage()).hasSize(11);
        assertThat(HEX.formatHex(root.fingerprint())).isEqualTo("b5af992c6ac029d376fa9dec750a5c4b");
        assertThat(HEX.formatHex(trie.streamHash())).startsWith("b5af992c6ac029d376fa9dec750a5c4b");
    }

    /** Annex I.6: e00 to e32 make an inner root with empty children at 2 and 7; e00 to e31 make a leaf root. */
    @Test
    void theThirtyThreeAndThirtyTwoSlotIllustrations() {
        HashTrie<String> trie = HashTrie.empty();
        for (int i = 0; i < 32; i++) {
            String id = String.format("e%02d", i);
            trie = trie.put(stateSlot(id, "0"));
        }
        assertThat(trie.node(new byte[0]).kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(HEX.formatHex(trie.streamHash()))
                .isEqualTo("fb7d5b769f66fc5abcc7636fe0bb14131574cac97d0feba9525ced60a95ac77e");

        HashTrie<String> grown = trie.put(stateSlot("e32", "0"));
        grown.checkShape();
        AuditNode root = grown.node(new byte[0]);
        assertThat(root.kind()).isEqualTo(AuditNode.Kind.INNER);
        assertThat(root.count()).isEqualTo(33);
        assertThat(root.preimage()).hasSize(384);
        assertThat(grown.node(new byte[] {2}).kind()).isEqualTo(AuditNode.Kind.EMPTY);
        assertThat(grown.node(new byte[] {7}).kind()).isEqualTo(AuditNode.Kind.EMPTY);
        assertThat(HEX.formatHex(grown.streamHash()))
                .isEqualTo("988d034b3abe2292c17478e2f646d845710c3ce40c30dfd9732598be8c2560cb");

        // The collapse: taking e32 out again gives back the very bytes of the 32-slot leaf.
        HashTrie<String> shrunk = grown.remove(EntryId.of("e32"));
        shrunk.checkShape();
        assertThat(shrunk.node(new byte[0]).kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(shrunk.streamHash()).isEqualTo(trie.streamHash());
    }

    /** An inner node's preimage lists each child's fingerprint and count, and its own fingerprint hashes that. */
    @Test
    void anInnerPreimageListsItsChildren() {
        HashTrie<String> trie = HashTrie.empty();
        for (int i = 0; i <= 32; i++) {
            trie = trie.put(stateSlot(String.format("e%02d", i), "0"));
        }
        byte[] nodes = trie.node(new byte[0]).preimage();
        long sum = 0;
        for (int c = 0; c < 16; c++) {
            AuditNode child = trie.node(new byte[] {(byte) c});
            assertThat(Arrays.copyOfRange(nodes, c * 24, c * 24 + 16)).isEqualTo(child.fingerprint());
            long count = 0;
            for (int i = 0; i < 8; i++) {
                count = (count << 8) | (nodes[c * 24 + 16 + i] & 0xFF);
            }
            assertThat(count).isEqualTo(child.count());
            sum += count;
        }
        assertThat(sum).isEqualTo(33);
        assertThat(trie.streamHash()).isEqualTo(TrieSupport.sha256(nodes));
    }

    /**
     * The split and the collapse below the root: 33 slots under the prefix 5e make inner nodes at the root, at 5 and
     * at 5e, and removing one collapses all three into one root leaf; 33 under abc do the same one level deeper.
     */
    @Test
    void splitsAndCollapsesAtDeeperLevels() {
        for (String prefix : List.of("5e", "abc")) {
            List<String> ids = ground(prefix, 33, "b");
            HashTrie<String> trie = HashTrie.empty();
            for (int i = 0; i < 32; i++) {
                trie = trie.put(stateSlot(ids.get(i), "h" + i));
                trie.checkShape();
            }
            assertThat(trie.node(new byte[0]).kind()).isEqualTo(AuditNode.Kind.LEAF);
            HashTrie<String> grown = trie.put(stateSlot(ids.get(32), "h32"));
            grown.checkShape();
            for (int depth = 0; depth <= prefix.length(); depth++) {
                AuditNode node = grown.node(path(prefix.substring(0, depth)));
                // Every slot shares the whole prefix, so each node down to it holds all 33 and is inner.
                assertThat(node.kind()).as("depth %d of %s", depth, prefix).isEqualTo(AuditNode.Kind.INNER);
                assertThat(node.count()).isEqualTo(33);
            }
            // Under the full prefix the 33 slots spread over the next nibble into leaves.
            long under = 0;
            for (int c = 0; c < 16; c++) {
                AuditNode child = grown.node(path(prefix + Character.forDigit(c, 16)));
                assertThat(child.kind()).isNotEqualTo(AuditNode.Kind.INNER);
                under += child.count();
            }
            assertThat(under).isEqualTo(33);
            assertThat(grown.streamHash()).isEqualTo(TrieSupport.referenceStreamHash(slots(grown)));

            HashTrie<String> shrunk = grown.remove(EntryId.of(ids.get(5)));
            shrunk.checkShape();
            assertThat(shrunk.node(new byte[0]).kind()).isEqualTo(AuditNode.Kind.LEAF);
            assertThat(shrunk.streamHash()).isEqualTo(TrieSupport.referenceStreamHash(slots(shrunk)));
            assertThat(shrunk.remove(EntryId.of(ids.get(32))).put(stateSlot(ids.get(5), "h5")).streamHash())
                    .isEqualTo(trie.streamHash());
        }
    }

    /** A node at a path below a stored leaf is answered from that leaf's slots under the path. */
    @Test
    void aPathBelowALeafAnswersTheSlotsUnderIt() {
        List<String> ids = ground("7", 3, "p");
        HashTrie<String> trie = HashTrie.empty();
        for (String id : ids) {
            trie = trie.put(stateSlot(id, "x"));
        }
        String elsewhere = ground("1", 1, "q").get(0);
        trie = trie.put(stateSlot(elsewhere, "y"));
        AuditNode seven = trie.node(new byte[] {7});
        assertThat(seven.kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(seven.count()).isEqualTo(3);
        Set<String> expected = new TreeSet<>();
        ids.forEach(id -> expected.add("e:" + id + "=x"));
        assertThat(seven.lines().split("\n")).containsExactlyInAnyOrderElementsOf(expected);
        Object[] reference = TrieSupport.referenceFingerprint(slots(trie), "7");
        assertThat(seven.fingerprint()).isEqualTo((byte[]) reference[0]);
        // The full key of one slot is a leaf of that slot alone; one nibble off is empty.
        String key = TrieSupport.key(ids.get(0));
        assertThat(trie.node(path(key)).count()).isEqualTo(1);
        String other = key.substring(0, 63) + (key.charAt(63) == '0' ? '1' : '0');
        assertThat(trie.node(path(other)).kind()).isEqualTo(AuditNode.Kind.EMPTY);
    }

    @Test
    void aPathMustBeNibblesAndAtMostSixtyFour() {
        HashTrie<String> trie = HashTrie.<String>empty().put(stateSlot("a", "1"));
        assertThatThrownBy(() -> trie.node(new byte[] {16})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> trie.node(new byte[] {-1})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> trie.node(new byte[65])).isInstanceOf(IllegalArgumentException.class);
        assertThat(trie.node(new byte[64]).kind()).isEqualTo(AuditNode.Kind.EMPTY);
    }

    /**
     * Lines sort by unsigned UTF-8 bytes across slots, so every claim line precedes every state line, and U+E000
     * precedes U+1F600 although Java's string order puts the surrogate pair first.
     */
    @Test
    void linesSortByUnsignedUtf8Bytes() {
        String bmp = "x";
        String astral = "x😀";
        assertThat(astral.compareTo(bmp)).isNegative();
        HashTrie<String> trie = HashTrie.<String>empty()
                .put(stateSlot(bmp, "1"))
                .put(stateSlot(astral, "1"))
                .put(Slot.<String>empty(EntryId.of("z")).withClaim("claim", "9"));
        assertThat(trie.node(new byte[0]).lines())
                .isEqualTo("c:z=9\ne:" + bmp + "=1\ne:" + astral + "=1");
        assertThat(trie.streamHash()).isEqualTo(TrieSupport.referenceStreamHash(slots(trie)));
    }

    /**
     * An acknowledged line stands as the line itself: a replica that refused a state and one that holds it reach
     * the same set, and an acknowledgement equal to a held line is one line.
     */
    @Test
    void anAcknowledgedLineStandsAsTheLineItself() {
        HashTrie<String> holder = HashTrie.<String>empty().put(stateSlot("a", "1"));
        HashTrie<String> refuser = HashTrie.<String>empty().put(Slot.<String>empty(EntryId.of("a")).withAck("e:a=1"));
        assertThat(refuser.streamHash()).isEqualTo(holder.streamHash());
        HashTrie<String> both = HashTrie.<String>empty().put(stateSlot("a", "1").withAck("e:a=1"));
        assertThat(both.node(new byte[0]).lines()).isEqualTo("e:a=1");
        assertThat(both.streamHash()).isEqualTo(holder.streamHash());
        // Two slots can name one line when an id holds '=': the line is still one line of the set, and both slots count.
        HashTrie<String> shared = HashTrie.<String>empty()
                .put(Slot.<String>empty(EntryId.of("a")).withAck("e:a=b=1"))
                .put(stateSlot("a=b", "1"));
        assertThat(shared.node(new byte[0]).lines()).isEqualTo("e:a=b=1");
        assertThat(shared.node(new byte[0]).count()).isEqualTo(2);
        assertThat(shared.streamHash()).isEqualTo(TrieSupport.referenceStreamHash(slots(shared)));
        assertThatThrownBy(() -> Slot.<String>empty(EntryId.of("a")).withAck("e:b=1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Slot.<String>empty(EntryId.of("a")).withAck("a:e:a=1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * RE-T-06 is not precluded: a slot marked lapsed keeps its state for the map but leaves its state and claim lines
     * out of the audited set, keeps its acknowledged lines, and leaves the count when nothing audited remains. A
     * shard-dropped line has no lease to lapse. An id holding a control character never enters the audited set.
     */
    @Test
    void lapsedAndControlCharacterSlotsLeaveTheAuditedSet() {
        HashTrie<String> base = HashTrie.<String>empty().put(stateSlot("b", "2"));
        Slot<String> lapsed = stateSlot("a", "1").withClaim("claim", "3").withLapsed(true);
        HashTrie<String> withLapsed = base.put(lapsed);
        assertThat(withLapsed.size()).isEqualTo(2);
        assertThat(withLapsed.auditedCount()).isEqualTo(1);
        assertThat(withLapsed.get(EntryId.of("a")).state()).isNotNull();
        assertThat(withLapsed.streamHash()).isEqualTo(base.streamHash());
        assertThat(new String(withLapsed.legacyDigest(), StandardCharsets.UTF_8))
                .isEqualTo("c:a=3\ne:a=1\ne:b=2\n");

        HashTrie<String> acked = base.put(lapsed.withAck("e:a=0"));
        assertThat(acked.auditedCount()).isEqualTo(2);
        assertThat(acked.node(new byte[0]).lines()).isEqualTo("e:a=0\ne:b=2");

        Slot<String> dropped = Slot.<String>empty(EntryId.of("a")).withDropped("7").withLapsed(true);
        assertThat(dropped.auditedLines()).containsExactly("e:a=7");

        HashTrie<String> control = base.put(stateSlot("a\u0001", "1")).put(stateSlot("c\u007F", "1"));
        assertThat(control.size()).isEqualTo(3);
        assertThat(control.auditedCount()).isEqualTo(1);
        assertThat(control.streamHash()).isEqualTo(base.streamHash());
        assertThat(new String(control.legacyDigest(), StandardCharsets.UTF_8)).contains("e:a\u0001=1\n");

        // Toggling the flag is one put of a changed slot: one path copied, the audited count moved by one.
        HashTrie<String> toggled = withLapsed.update(EntryId.of("a"), s -> s.withLapsed(false));
        assertThat(toggled.auditedCount()).isEqualTo(2);
        assertThat(toggled.streamHash()).isEqualTo(base.put(lapsed.withLapsed(false)).streamHash());
    }

    /** A stored inner node whose audited count is at most the threshold answers as one leaf, as the annex has it. */
    @Test
    void anInnerNodeWithFewAuditedSlotsAnswersAsALeaf() {
        List<String> ids = ground("5e", 33, "b");
        HashTrie<String> trie = HashTrie.empty();
        for (int i = 0; i < 33; i++) {
            trie = trie.put(stateSlot(ids.get(i), "h" + i).withLapsed(i == 0));
        }
        trie.checkShape();
        assertThat(trie.size()).isEqualTo(33);
        assertThat(trie.auditedCount()).isEqualTo(32);
        assertThat(trie.node(new byte[0]).kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(trie.node(path("5e")).kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(trie.streamHash()).isEqualTo(TrieSupport.referenceStreamHash(slots(trie)));
        HashTrie<String> without = HashTrie.empty();
        for (int i = 1; i < 33; i++) {
            without = without.put(stateSlot(ids.get(i), "h" + i));
        }
        assertThat(trie.streamHash()).isEqualTo(without.streamHash());
    }

    @Test
    void getPutReplaceRemoveAndIterateInKeyOrder() {
        HashTrie<String> trie = HashTrie.empty();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            ids.add("id-" + i);
            trie = trie.put(stateSlot("id-" + i, "v" + i));
        }
        trie.checkShape();
        assertThat(trie.size()).isEqualTo(300);
        assertThat(trie.get(EntryId.of("id-17")).stateHash()).isEqualTo("v17");
        assertThat(trie.get(EntryId.of("absent"))).isNull();
        HashTrie<String> replaced = trie.put(stateSlot("id-17", "w"));
        assertThat(replaced.size()).isEqualTo(300);
        assertThat(replaced.get(EntryId.of("id-17")).stateHash()).isEqualTo("w");
        assertThat(trie.get(EntryId.of("id-17")).stateHash()).as("the old trie is untouched").isEqualTo("v17");
        assertThat(trie.put(trie.get(EntryId.of("id-3")))).isSameAs(trie);
        assertThat(trie.remove(EntryId.of("absent"))).isSameAs(trie);
        assertThat(trie.put(Slot.empty(EntryId.of("id-4"))).containsKey(EntryId.of("id-4"))).isFalse();

        String previous = null;
        int seen = 0;
        for (Slot<String> slot : trie) {
            String key = HEX.formatHex(slot.key());
            if (previous != null) {
                assertThat(key).isGreaterThan(previous);
            }
            previous = key;
            seen++;
        }
        assertThat(seen).isEqualTo(300);
    }

    /**
     * The text digest a trie renders is byte for byte what {@code ReplicatedSpace.computeDigest} builds from the same
     * states, claims, shard-dropped lines and acknowledgements: string order of the key, a held state's line over a
     * remembered one, and the {@code a:} block last.
     */
    @Test
    void theLegacyDigestMatchesTodaysConstruction() {
        Random random = new Random(7);
        for (int round = 0; round < 50; round++) {
            Map<String, String> states = new TreeMap<>();
            Map<String, String> claims = new TreeMap<>();
            Map<String, String> dropped = new TreeMap<>();
            Set<String> acks = new TreeSet<>();
            HashTrie.Builder<String> builder = HashTrie.builder();
            int n = random.nextInt(120);
            for (int i = 0; i < n; i++) {
                // Ids that are prefixes of one another and hold '=' are where key order and line order part.
                String id = "id" + "=-a".charAt(random.nextInt(3)) + random.nextInt(40);
                Slot<String> slot = builder.get(EntryId.of(id));
                if (slot == null) {
                    slot = Slot.empty(EntryId.of(id));
                }
                switch (random.nextInt(4)) {
                    case 0 -> {
                        String h = Integer.toHexString(random.nextInt());
                        slot = slot.withState(TrieSupport.state(id), h);
                        states.put(id, h);
                    }
                    case 1 -> {
                        String h = random.nextInt(9) + ":" + random.nextInt();
                        slot = slot.withClaim("claim", h);
                        claims.put(id, h);
                    }
                    case 2 -> {
                        String h = Integer.toHexString(random.nextInt());
                        slot = slot.withDropped(h);
                        dropped.put(id, h);
                    }
                    default -> {
                        String line = (random.nextBoolean() ? "e:" : "c:") + id + "=" + random.nextInt(5);
                        slot = slot.withAck(line);
                        acks.add(line);
                    }
                }
                builder.put(slot);
            }
            HashTrie<String> trie = builder.build();
            trie.checkShape();
            assertThat(new String(trie.legacyDigest(), StandardCharsets.UTF_8))
                    .isEqualTo(todaysDigest(states, claims, dropped, acks));
        }
    }

    /**
     * Laziness: a run of changes hashes no intermediate trie, and asking for the stream hash of the last hashes only
     * that trie; asking again reads the memo.
     */
    @Test
    void aBatchOfChangesHashesNoIntermediateTrie() {
        HashTrie<String> trie = HashTrie.empty();
        for (int i = 0; i < 2_000; i++) {
            trie = trie.put(stateSlot("id-" + i, "v"));
        }
        byte[] before = trie.streamHash();
        assertThat(trie.rootHashed()).isTrue();
        List<HashTrie<String>> steps = new ArrayList<>();
        HashTrie<String> changed = trie;
        for (int i = 0; i < 100; i++) {
            changed = changed.put(stateSlot("id-" + (i * 7), "w"));
            steps.add(changed);
        }
        byte[] after = changed.streamHash();
        assertThat(after).isNotEqualTo(before);
        for (int i = 0; i < steps.size() - 1; i++) {
            assertThat(steps.get(i).rootHashed()).as("step %d", i).isFalse();
        }
        assertThat(changed.streamHash()).isEqualTo(after);
        assertThat(after).isEqualTo(TrieSupport.referenceStreamHash(slots(changed)));
    }

    /** The builder makes the same trie from nothing, from a trie with a small batch, and from one with a large one. */
    @Test
    void theBuilderMakesTheCanonicalTrie() {
        HashTrie<String> stepwise = HashTrie.empty();
        HashTrie.Builder<String> fresh = HashTrie.builder();
        for (int i = 0; i < 1_000; i++) {
            Slot<String> slot = stateSlot("id-" + i, "v" + i);
            stepwise = stepwise.put(slot);
            fresh.put(slot);
        }
        HashTrie<String> built = fresh.build();
        built.checkShape();
        assertThat(built).isEqualTo(stepwise);
        assertThat(built.streamHash()).isEqualTo(stepwise.streamHash());

        for (int batch : new int[] {10, 600}) {
            HashTrie.Builder<String> b = built.toBuilder();
            HashTrie<String> expected = built;
            for (int i = 0; i < batch; i++) {
                if (i % 3 == 0) {
                    b.remove(EntryId.of("id-" + i));
                    expected = expected.remove(EntryId.of("id-" + i));
                } else {
                    b.update(EntryId.of("id-" + i), s -> s.withClaim("claim", "c"));
                    expected = expected.update(EntryId.of("id-" + i), s -> s.withClaim("claim", "c"));
                }
            }
            HashTrie<String> applied = b.build();
            applied.checkShape();
            assertThat(applied).isEqualTo(expected);
            assertThat(applied.streamHash()).isEqualTo(expected.streamHash());
        }
        assertThat(built.toBuilder().build()).isSameAs(built);
    }

    private static List<Slot<String>> slots(HashTrie<String> trie) {
        List<Slot<String>> out = new ArrayList<>();
        trie.forEach(out::add);
        return out;
    }

    /** ReplicatedSpace.computeDigest as it reads on 2026-11-14, over plain maps. */
    private static String todaysDigest(Map<String, String> states, Map<String, String> claims,
                                       Map<String, String> dropped, Set<String> refusedAck) {
        TreeMap<String, String> summary = new TreeMap<>();
        states.forEach((id, h) -> summary.put("e:" + id, h));
        claims.forEach((id, h) -> summary.put("c:" + id, h));
        dropped.forEach((id, h) -> summary.putIfAbsent("e:" + id, h));
        StringBuilder sb = new StringBuilder();
        summary.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        new TreeSet<>(refusedAck).forEach(line -> sb.append("a:").append(line).append('\n'));
        return sb.toString();
    }

    /**
     * The bounded line build a partner uses before it answers lines (annex I.6, step 4): the very bytes of the whole
     * build when they fit, exactly at the bound included, and null one byte below it, at the root and deeper, with an
     * acknowledgement equal to a held line counted once, as the preimage counts it.
     */
    @Test
    void boundedLinesMatchTheWholeBuildAndStopPastTheBound() {
        HashTrie<String> trie = HashTrie.empty();
        for (int i = 0; i < 300; i++) {
            Slot<String> slot = stateSlot("entry-" + i, Integer.toHexString(i * 7919));
            if (i % 5 == 0) {
                slot = slot.withAck("e:entry-" + i + "=" + Integer.toHexString(i * 7919)); // one line with the held
            }
            if (i % 7 == 0) {
                slot = slot.withClaim("claim", "c" + i);
            }
            trie = trie.put(slot);
        }
        List<byte[]> paths = new ArrayList<>();
        paths.add(new byte[0]);
        for (int a = 0; a < 16; a++) {
            paths.add(new byte[] {(byte) a});
            paths.add(new byte[] {(byte) a, (byte) (15 - a)});
        }
        for (byte[] p : paths) {
            byte[] whole = trie.linesUnder(p);
            assertThat(trie.linesUnder(p, Long.MAX_VALUE)).isEqualTo(whole);
            assertThat(trie.linesUnder(p, whole.length)).as("exactly at the bound").isEqualTo(whole);
            if (whole.length > 0) {
                assertThat(trie.linesUnder(p, whole.length - 1L)).as("one byte below it").isNull();
            }
        }
    }
}
