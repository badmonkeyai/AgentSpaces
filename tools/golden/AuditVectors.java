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

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.space.crdt.AuditNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.HashTrie;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.Slot;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Writes {@code audit-vectors.json}, the conformance vectors of SPEC annex I.6 (srad/replication-by-entry.md RE-L-05,
 * second half; decision 14): the fingerprints of known sets, the preimage of every node an audit of them reads, their
 * stream hashes, and descent transcripts between two sets, each exchange with the {@code DIGEST} body that carries it.
 * Every value comes from the core's own {@code HashTrie}, so the file says what the Java implementation computes; the
 * conformance harness ({@code tools/conformance/audit}) checks it with Java and with an independent Python fold, and
 * the TypeScript, Rust, Python and Ruby clients join that check as they build {@code wire.ae=2}.
 *
 * <p>The sets are deterministic (a seeded random source and fixed ids), so a regeneration with no change to the trie
 * writes the same bytes. The program never touches {@code golden.json}. From the {@code agentspaces} directory, with
 * the core modules compiled:
 * <pre>
 * mvn -o -q -f agentspaces-space/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/audit-cp.txt
 * CP="agentspaces-common/target/classes:agentspaces-api/target/classes:agentspaces-peering/target/classes:\
 * agentspaces-space/target/classes:$(cat /tmp/audit-cp.txt)"
 * lockf -k -t 7200 /tmp/claude-watch/golden.lock \
 *     java -cp "$CP" tools/golden/AuditVectors.java ../agentspaces-spec/audit-vectors.json tools/golden/audit-vectors.json
 * </pre>
 * The module directories go first, so the classes of this tree win over the jars of {@code ~/.m2}. Every path named
 * gets the same bytes: the specification kit's copy, the authority, and the core's vendored one, which the core's
 * {@code AuditVectorsConformanceTest} reads when the kit is not beside the repository.
 */
public final class AuditVectors {

    private static final HexFormat HEX = HexFormat.of();
    private static final int T = 32;
    private static final String STREAM = "space:tasks";
    private static final long BUDGET = 1L << 20;
    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final AgentId AGENT = AgentId.parse("zP/writer");
    private static final SpaceId SPACE = SpaceId.local("audit");

    private AuditVectors() {
    }

    /** One slot of a set, as the vectors give it: what a replica holds for one entry id. */
    record SlotSpec(String id, String state, String claim, String dropped, boolean lapsed, List<String> acks) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", id);
            out.put("state", state);
            out.put("claim", claim);
            out.put("dropped", dropped);
            out.put("lapsed", lapsed);
            out.put("acks", acks);
            return out;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: AuditVectors <audit-vectors.json>...");
            System.exit(2);
        }
        Map<String, List<SlotSpec>> sets = sets();
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("description", "SPEC annex I.6 (v0.1.35) conformance vectors for wire.ae=2: the audited lines, stream "
                + "hash and node fingerprints and preimages of known sets, and descent transcripts between two sets with "
                + "the DIGEST bodies that carry them. Generated by agentspaces/tools/golden/AuditVectors.java from the "
                + "Java HashTrie; do not edit by hand.");
        doc.put("spec", "SPEC v0.1.35 annex I.6");
        doc.put("parameters", parameters());
        doc.put("reading", reading());
        List<Object> setDocs = new ArrayList<>();
        for (Map.Entry<String, List<SlotSpec>> set : sets.entrySet()) {
            setDocs.add(setDoc(set.getKey(), set.getValue()));
        }
        doc.put("sets", setDocs);
        // annex I.6's illustrations, whose values the annex prints, hold here or the trie has moved
        require(HEX.formatHex(trie(sets.get("illustration-e00-e32")).streamHash())
                .equals("988d034b3abe2292c17478e2f646d845710c3ce40c30dfd9732598be8c2560cb"), "the annex's 33 slots");
        require(HEX.formatHex(trie(sets.get("illustration-e00-e31")).streamHash())
                .equals("fb7d5b769f66fc5abcc7636fe0bb14131574cac97d0feba9525ced60a95ac77e"), "the annex's 32 slots");
        require(HEX.formatHex(trie(sets.get("illustration-a")).node(new byte[0]).fingerprint())
                .equals("b5af992c6ac029d376fa9dec750a5c4b"), "the annex's slot a");
        List<Object> descents = new ArrayList<>();
        for (String[] pair : DESCENTS) {
            descents.add(descent(pair[0], pair[1], pair[2], sets.get(pair[1]), sets.get(pair[2])));
        }
        doc.put("descents", descents);

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter()
                .withObjectIndenter(new DefaultIndenter(" ", "\n"))
                .withArrayIndenter(new DefaultIndenter(" ", "\n"));
        String text = mapper.writer(printer).writeValueAsString(doc) + "\n";
        // Every path gets the same bytes: the specification kit's copy and the core's vendored one never differ.
        for (String arg : args) {
            Path out = Path.of(arg);
            Files.createDirectories(out.toAbsolutePath().getParent());
            Files.writeString(out, text, StandardCharsets.UTF_8);
            System.out.println("wrote " + out + ": " + setDocs.size() + " sets, " + descents.size() + " descents, "
                    + text.length() + " bytes");
        }
    }

    private static Map<String, Object> parameters() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("fanout", 16);
        p.put("leaf_max", T);
        p.put("key", "SHA-256 of the entry id's UTF-8 bytes, read as 64 nibbles, most significant first");
        p.put("fingerprint_bytes", 16);
        p.put("child_bytes", 24);
        p.put("inner_preimage_bytes", 384);
        p.put("path_max", 64);
        p.put("tree_nodes_max", Bodies.Digest.TREE_NODES_MAX);
        p.put("ids_max", Bodies.Digest.IDS_MAX);
        p.put("budget_default", BUDGET);
        p.put("empty_stream_hash", HEX.formatHex(sha256(new byte[0])));
        return p;
    }

    private static Map<String, Object> reading() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("slot", "id is the entry id; state, claim and dropped are the hashes of the slot's state line "
                + "e:<id>=<state>, claim line c:<id>=<claim> and remembered shard-dropped line e:<id>=<dropped>, null "
                + "when absent; a held state's line wins over a dropped one; lapsed hides the state and claim lines of "
                + "a slot that holds a state; acks are acknowledged lines, each standing as itself");
        r.put("lines", "the set's audited lines, sorted by unsigned UTF-8 bytes, each once");
        r.put("path", "nibbles as lower-case hex digits, one per nibble; the empty string is the root");
        r.put("node", "kind is empty, leaf or inner; count is the number of audited slots under the path; "
                + "fingerprint is null for an empty node; preimage is the leaf's joined lines or the inner node's 384 "
                + "bytes, as hex");
        r.put("descent", "asker and partner name sets; the asker opens with its stream hash; exchange 0 is the "
                + "partner's root answer; each later exchange is the asker's request nodes, in the order the previous "
                + "exchange's answers name them (answers in order, children in nibble order: every child whose count "
                + "at the partner is above 0 and whose fingerprint or count differs from the asker's), and the "
                + "partner's answers in the order asked: lines when the partner's count or the request's count is at "
                + "most 32, else nodes; all under the default budget, which no answer here reaches. ids are the entry "
                + "ids of every partner line in a lines answer that the asker's own lines under that path lack, "
                + "sorted by UTF-8 bytes. request_body_cbor and answer_body_cbor are the DIGEST bodies for the stream "
                + STREAM + "; each request body carries the budget " + BUDGET + ", and exchange 0's request is the "
                + "hashed opening, which carries no tree");
        return r;
    }

    // ---- the sets

    /** Asker and partner set of each descent. */
    private static final String[][] DESCENTS = {
            {"equal", "random-200", "random-200"},
            {"one-differs-in-301", "base-301-changed", "base-301"},
            {"asker-lacks-many", "base-301-head", "base-301"},
            {"both-ways", "both-a", "both-b"},
            {"partner-root-leaf", "base-301-head-40", "base-301-head-20"},
            {"asker-empty", "empty", "random-64"},
            {"deep", "deep-abc-less", "deep-abc"},
            {"refused-stands-as-held", "acked", "held"},
            {"lapsed-left-out", "lapsed-one", "base-301"},
    };

    private static Map<String, List<SlotSpec>> sets() {
        Map<String, List<SlotSpec>> sets = new LinkedHashMap<>();
        sets.put("empty", List.of());
        // annex I.6's illustrations
        sets.put("illustration-a", List.of(new SlotSpec("a", "1", "2", null, false, List.of())));
        List<SlotSpec> thirtyThree = new ArrayList<>();
        for (int i = 0; i <= 32; i++) {
            thirtyThree.add(new SlotSpec(String.format("e%02d", i), "0", null, null, false, List.of()));
        }
        sets.put("illustration-e00-e32", thirtyThree);
        sets.put("illustration-e00-e31", thirtyThree.subList(0, 32));
        // lines that sort apart by UTF-16 units and by UTF-8 bytes: U+FFEF before U+1F600 in UTF-8, after in UTF-16
        sets.put("utf8-order", List.of(
                new SlotSpec("Z", "1", null, null, false, List.of()),
                new SlotSpec("a", "1", "c0", null, false, List.of()),
                new SlotSpec("é", "2", null, null, false, List.of()),
                new SlotSpec("￯", "3", null, null, false, List.of()),
                new SlotSpec("😀", "4", null, null, false, List.of()),
                new SlotSpec("𐍈", "5", null, null, false, List.of()),
                new SlotSpec("a=b", "6", null, null, false, List.of())));
        // the slot rules one at a time: an ack equal to the held line counts once, a dropped line, a held line over a
        // dropped one, a lapsed slot keeps its acks, a lapsed slot with nothing else leaves the set
        sets.put("slot-rules", List.of(
                new SlotSpec("held-and-acked", "aa", null, null, false, List.of("e:held-and-acked=aa")),
                new SlotSpec("dropped-only", null, null, "bb", false, List.of()),
                new SlotSpec("held-over-dropped", "cc", null, "dd", false, List.of()),
                new SlotSpec("lapsed-with-ack", "ee", "ff", null, true, List.of("c:lapsed-with-ack=99")),
                new SlotSpec("lapsed-alone", "11", "22", null, true, List.of()),
                new SlotSpec("claim-only", null, "33", null, false, List.of()),
                new SlotSpec("two-acks", "44", null, null, false, List.of("e:two-acks=45", "c:two-acks=46"))));
        Random random = new Random(20261115L);
        for (int n : new int[]{1, 2, 7, 31, 32, 33, 34, 50, 64, 200, 700}) {
            Set<String> ids = new TreeSet<>();
            while (ids.size() < n) {
                ids.add(randomId(random));
            }
            List<SlotSpec> slots = new ArrayList<>();
            for (String id : ids) {
                slots.add(randomSlot(random, id));
            }
            sets.put("random-" + n, slots);
        }
        List<SlotSpec> deep = new ArrayList<>();
        for (String id : ground("abc", 40, "d3")) {
            deep.add(new SlotSpec(id, hash(random), null, null, false, List.of()));
        }
        for (String id : ground("ab", 6, "d2")) {
            deep.add(new SlotSpec(id, hash(random), null, null, false, List.of()));
        }
        for (String id : ground("a", 4, "d1")) {
            deep.add(new SlotSpec(id, hash(random), null, null, false, List.of()));
        }
        sets.put("deep-abc", deep);
        List<SlotSpec> deepLess = new ArrayList<>(deep);
        deepLess.remove(7);
        deepLess.set(3, new SlotSpec(deepLess.get(3).id(), "f00d", null, null, false, List.of()));
        sets.put("deep-abc-less", deepLess);
        sets.put("boundary-5e-33", plain(ground("5e", 33, "b"), random));
        sets.put("boundary-5e-32", plain(ground("5e", 32, "b"), random));
        List<SlotSpec> lapsed5e = new ArrayList<>();
        List<String> five = ground("5e", 33, "b");
        for (int k = 0; k < five.size(); k++) {
            lapsed5e.add(new SlotSpec(five.get(k), "h" + k, null, null, k == 0, List.of()));
        }
        sets.put("lapsed-5e", lapsed5e);

        // descent pairs
        List<SlotSpec> base = new ArrayList<>();
        for (int i = 0; i < 301; i++) {
            base.add(new SlotSpec("entry-" + i, hash(random), i % 7 == 0 ? hash(random) : null, null, false,
                    List.of()));
        }
        sets.put("base-301", base);
        List<SlotSpec> changed = new ArrayList<>(base);
        changed.set(150, new SlotSpec("entry-150", "c0ffee", null, null, false, List.of()));
        sets.put("base-301-changed", changed);
        sets.put("base-301-head", base.subList(0, 100));
        sets.put("base-301-head-40", base.subList(0, 40));
        sets.put("base-301-head-20", base.subList(0, 20));
        List<SlotSpec> a = new ArrayList<>(base.subList(0, 250));
        List<SlotSpec> b = new ArrayList<>(base.subList(50, 301));
        a.set(120, new SlotSpec("entry-120", "aaaa", null, null, false, List.of()));
        sets.put("both-a", a);
        sets.put("both-b", b);
        List<SlotSpec> held = new ArrayList<>(base.subList(0, 60));
        sets.put("held", held);
        List<SlotSpec> acked = new ArrayList<>(base.subList(0, 60));
        SlotSpec refused = acked.get(17);
        acked.set(17, new SlotSpec(refused.id(), null, null, null, false,
                List.of("e:" + refused.id() + "=" + refused.state())));
        sets.put("acked", acked);
        List<SlotSpec> lapsedOne = new ArrayList<>(base);
        SlotSpec one = lapsedOne.get(42);
        lapsedOne.set(42, new SlotSpec(one.id(), one.state(), one.claim(), null, true, List.of()));
        sets.put("lapsed-one", lapsedOne);
        return sets;
    }

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789-=:.";
    private static final String[] EXOTIC = {"é", "中", "", "￯", "😀", "𐍈"};

    private static String randomId(Random random) {
        int n = 1 + random.nextInt(12);
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < n; i++) {
            id.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        if (random.nextDouble() < 0.15) {
            id.append(EXOTIC[random.nextInt(EXOTIC.length)]);
        }
        return id.toString();
    }

    private static String hash(Random random) {
        int n = 1 + random.nextInt(8);
        StringBuilder h = new StringBuilder();
        for (int i = 0; i < n; i++) {
            h.append("0123456789abcdef".charAt(random.nextInt(16)));
        }
        return h.toString();
    }

    private static SlotSpec randomSlot(Random random, String id) {
        String state = random.nextDouble() < 0.8 ? hash(random) : null;
        String claim = random.nextDouble() < 0.3 ? hash(random) : null;
        String dropped = random.nextDouble() < 0.1 ? hash(random) : null;
        boolean lapsed = random.nextDouble() < 0.1;
        Set<String> acks = new TreeSet<>();
        int count = new int[]{0, 0, 0, 1, 2}[random.nextInt(5)];
        for (int i = 0; i < count; i++) {
            boolean stateKind = random.nextBoolean();
            String heldHash = stateKind ? state : claim;
            String h = heldHash != null && random.nextDouble() < 0.4 ? heldHash : hash(random);
            acks.add((stateKind ? "e:" : "c:") + id + "=" + h);
        }
        if (state == null && claim == null && dropped == null && acks.isEmpty()) {
            state = hash(random);
        }
        return new SlotSpec(id, state, claim, dropped, lapsed, List.copyOf(acks));
    }

    private static List<SlotSpec> plain(List<String> ids, Random random) {
        List<SlotSpec> out = new ArrayList<>();
        for (String id : ids) {
            out.add(new SlotSpec(id, hash(random), null, null, false, List.of()));
        }
        return out;
    }

    /** The first n ids of the form tag-k whose key starts with the hex prefix. */
    private static List<String> ground(String prefix, int n, String tag) {
        List<String> out = new ArrayList<>(n);
        for (int k = 0; out.size() < n; k++) {
            String id = tag + "-" + k;
            if (HEX.formatHex(Slot.keyOf(EntryId.of(id))).startsWith(prefix)) {
                out.add(id);
            }
        }
        return out;
    }

    // ---- the trie of a set

    /** A state for an id; the trie reads only the hash handed in beside it, so one shape of state serves. */
    private static EntryState state(String id) {
        EntryRecord record = new EntryRecord(EntryId.of(id), SPACE, "T#v1", new byte[]{1}, null, AGENT,
                new HlcTimestamp(1L, 0, "seed"), new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE), Map.of(), null);
        return EntryState.added(record, new Dot("r1", 1),
                new LwwRegister<>(new HlcTimestamp(1L, 0, "r1"), new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE)));
    }

    private static HashTrie<String> trie(List<SlotSpec> specs) {
        HashTrie<String> trie = HashTrie.empty();
        for (SlotSpec spec : specs) {
            Slot<String> slot = Slot.empty(EntryId.of(spec.id()));
            if (spec.state() != null) {
                slot = slot.withState(state(spec.id()), spec.state());
            }
            if (spec.claim() != null) {
                slot = slot.withClaim("claim", spec.claim());
            }
            if (spec.dropped() != null) {
                slot = slot.withDropped(spec.dropped());
            }
            slot = slot.withLapsed(spec.lapsed());
            if (!spec.acks().isEmpty()) {
                slot = slot.withAcks(spec.acks());
            }
            trie = trie.put(slot);
        }
        // The builder must answer the same tree as the put path; a difference is a fault in the trie, not a vector.
        HashTrie.Builder<String> builder = HashTrie.builder();
        trie.forEach(builder::put);
        require(Arrays.equals(builder.build().streamHash(), trie.streamHash()), "the built and the put tries agree");
        return trie;
    }

    private static Map<String, Object> setDoc(String name, List<SlotSpec> specs) {
        HashTrie<String> trie = trie(specs);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", name);
        List<Object> slots = new ArrayList<>();
        specs.forEach(s -> slots.add(s.json()));
        doc.put("slots", slots);
        String joined = new String(trie.linesUnder(new byte[0]), StandardCharsets.UTF_8);
        doc.put("lines", joined.isEmpty() ? List.of() : List.of(joined.split("\n")));
        AuditNode root = trie.node(new byte[0]);
        doc.put("count", root.count());
        doc.put("stream_hash", HEX.formatHex(trie.streamHash()));
        require(Arrays.equals(trie.streamHash(), sha256(root.preimage())), name + ": the stream hash is the root's");
        List<Object> nodes = new ArrayList<>();
        walk(trie, new byte[0], nodes);
        doc.put("nodes", nodes);
        return doc;
    }

    /**
     * Every inner node, every child of one (the first empty child of each inner node among them), and for each leaf
     * the node one nibble below its first slot, so a reader is checked on paths that run below a stored leaf too.
     */
    private static void walk(HashTrie<String> trie, byte[] path, List<Object> out) {
        AuditNode node = trie.node(path);
        out.add(nodeDoc(node));
        if (node.kind() == AuditNode.Kind.INNER) {
            boolean emptyDone = false;
            byte[] pre = node.preimage();
            for (int c = 0; c < 16; c++) {
                byte[] child = child(path, c);
                if (countAt(pre, c) > 0) {
                    walk(trie, child, out);
                } else if (!emptyDone) {
                    out.add(nodeDoc(trie.node(child)));
                    emptyDone = true;
                }
            }
        } else if (node.kind() == AuditNode.Kind.LEAF && path.length < 64) {
            String first = new String(node.preimage(), StandardCharsets.UTF_8).split("\n")[0];
            String id = entryIdOf(first);
            byte[] key = Slot.keyOf(EntryId.of(id));
            byte[] below = child(path, nibble(key, path.length));
            out.add(nodeDoc(trie.node(below)));
        }
    }

    private static Map<String, Object> nodeDoc(AuditNode node) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("path", pathText(node.path()));
        doc.put("kind", node.kind().name().toLowerCase(java.util.Locale.ROOT));
        doc.put("count", node.count());
        doc.put("fingerprint", node.kind() == AuditNode.Kind.EMPTY ? null : HEX.formatHex(node.fingerprint()));
        doc.put("preimage", HEX.formatHex(node.preimage()));
        if (node.kind() != AuditNode.Kind.EMPTY) {
            require(Arrays.equals(node.fingerprint(), Arrays.copyOf(sha256(node.preimage()), 16)),
                    "a fingerprint is its preimage's");
        }
        return doc;
    }

    // ---- descents

    private static Map<String, Object> descent(String name, String askerName, String partnerName,
                                               List<SlotSpec> askerSpecs, List<SlotSpec> partnerSpecs) {
        HashTrie<String> asker = trie(askerSpecs);
        HashTrie<String> partner = trie(partnerSpecs);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", name);
        doc.put("asker", askerName);
        doc.put("partner", partnerName);
        doc.put("asker_stream_hash", HEX.formatHex(asker.streamHash()));
        doc.put("partner_stream_hash", HEX.formatHex(partner.streamHash()));
        List<Object> exchanges = new ArrayList<>();
        Set<String> ids = new TreeSet<>(AuditVectors::compareUtf8);
        if (!Arrays.equals(asker.streamHash(), partner.streamHash())) {
            // exchange 0: the opening and the root answer
            AuditNode root = partner.node(new byte[0]);
            Bodies.TreeNode rootAnswer = answer(root, Long.MAX_VALUE, partner);
            Map<String, byte[]> hashes = new java.util.TreeMap<>(Map.of(STREAM, asker.streamHash()));
            exchanges.add(exchange(List.of(), List.of(rootAnswer),
                    CODEC.toBytes(new Bodies.Digest(null, hashes, null)),
                    CODEC.toBytes(tree(List.of(rootAnswer), null))));
            List<Bodies.TreeNode> answers = List.of(rootAnswer);
            while (true) {
                List<Bodies.TreeNode> requests = new ArrayList<>();
                for (Bodies.TreeNode answer : answers) {
                    if (answer.nodes() != null) {
                        for (int c = 0; c < 16; c++) {
                            long theirs = countAt(answer.nodes(), c);
                            if (theirs == 0) {
                                continue;
                            }
                            byte[] childPath = child(answer.path(), c);
                            AuditNode own = asker.node(childPath);
                            byte[] theirFingerprint = Arrays.copyOfRange(answer.nodes(), c * 24, c * 24 + 16);
                            if (own.count() != theirs || !Arrays.equals(own.fingerprint(), theirFingerprint)) {
                                requests.add(Bodies.TreeNode.request(childPath, own.count()));
                            }
                        }
                    } else if (!answer.lines().isEmpty()) {
                        Set<String> mine = new java.util.HashSet<>();
                        String own = new String(asker.linesUnder(answer.path()), StandardCharsets.UTF_8);
                        if (!own.isEmpty()) {
                            mine.addAll(Arrays.asList(own.split("\n")));
                        }
                        for (String line : answer.lines().split("\n")) {
                            if (!mine.contains(line)) {
                                ids.add(entryIdOf(line));
                            }
                        }
                    }
                }
                if (requests.isEmpty()) {
                    break;
                }
                require(requests.size() <= Bodies.Digest.TREE_NODES_MAX, name + ": one request body per exchange");
                List<Bodies.TreeNode> next = new ArrayList<>();
                for (Bodies.TreeNode request : requests) {
                    next.add(answer(partner.node(request.path()), request.count(), partner));
                }
                exchanges.add(exchange(requests, next, CODEC.toBytes(tree(requests, BUDGET)),
                        CODEC.toBytes(tree(next, null))));
                answers = next;
            }
        }
        doc.put("exchanges", exchanges);
        doc.put("ids", new ArrayList<>(ids));
        return doc;
    }

    /** The partner's answer at a node: lines when its count or the asker's is at most 32, else nodes. */
    private static Bodies.TreeNode answer(AuditNode node, long askerCount, HashTrie<String> partner) {
        if (node.count() == 0) {
            return Bodies.TreeNode.lines(node.path(), "");
        }
        if (node.kind() == AuditNode.Kind.LEAF) {
            return Bodies.TreeNode.lines(node.path(), node.lines());
        }
        if (askerCount <= T) {
            return Bodies.TreeNode.lines(node.path(),
                    new String(partner.linesUnder(node.path()), StandardCharsets.UTF_8));
        }
        return Bodies.TreeNode.inner(node.path(), node.preimage());
    }

    private static Bodies.Digest tree(List<Bodies.TreeNode> nodes, Long budget) {
        return new Bodies.Digest(null, null, null, Map.of(STREAM, nodes), null, budget, null);
    }

    private static Map<String, Object> exchange(List<Bodies.TreeNode> requests, List<Bodies.TreeNode> answers,
                                                byte[] requestBody, byte[] answerBody) {
        Map<String, Object> doc = new LinkedHashMap<>();
        List<Object> asked = new ArrayList<>();
        for (Bodies.TreeNode r : requests) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("path", pathText(r.path()));
            n.put("count", r.count());
            asked.add(n);
        }
        doc.put("requests", asked);
        List<Object> answered = new ArrayList<>();
        for (Bodies.TreeNode a : answers) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("path", pathText(a.path()));
            if (a.nodes() != null) {
                n.put("nodes", HEX.formatHex(a.nodes()));
            } else {
                n.put("lines", a.lines());
            }
            answered.add(n);
        }
        doc.put("answers", answered);
        doc.put("request_body_cbor", HEX.formatHex(requestBody));
        doc.put("answer_body_cbor", HEX.formatHex(answerBody));
        // every body decodes under the strict reader back to its own bytes
        require(Arrays.equals(CODEC.toBytes(CODEC.fromWire(requestBody, Bodies.Digest.class)), requestBody),
                "a request body round-trips");
        require(Arrays.equals(CODEC.toBytes(CODEC.fromWire(answerBody, Bodies.Digest.class)), answerBody),
                "an answer body round-trips");
        return doc;
    }

    // ---- helpers

    private static byte[] child(byte[] path, int nibble) {
        byte[] out = Arrays.copyOf(path, path.length + 1);
        out[path.length] = (byte) nibble;
        return out;
    }

    private static int nibble(byte[] key, int position) {
        int b = key[position / 2] & 0xFF;
        return position % 2 == 0 ? b >>> 4 : b & 0x0F;
    }

    private static long countAt(byte[] nodes, int child) {
        long count = 0;
        for (int i = 0; i < 8; i++) {
            count = (count << 8) | (nodes[child * 24 + 16 + i] & 0xFF);
        }
        return count;
    }

    private static String pathText(byte[] path) {
        StringBuilder out = new StringBuilder(path.length);
        for (byte b : path) {
            out.append(Character.forDigit(b, 16));
        }
        return out.toString();
    }

    private static String entryIdOf(String line) {
        return line.substring(2, line.lastIndexOf('='));
    }

    private static int compareUtf8(String a, String b) {
        return Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void require(boolean condition, String what) {
        if (!condition) {
            throw new IllegalStateException("audit vectors: " + what);
        }
    }
}
