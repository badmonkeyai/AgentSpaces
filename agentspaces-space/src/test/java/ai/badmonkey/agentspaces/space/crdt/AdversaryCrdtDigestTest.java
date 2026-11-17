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
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Convergence adversary of phase 1 (srad/replication-by-entry.md): SpaceStateCrdt on the trie, driven by random
 * histories through every public and internal mutator, against a plain model of maps. After every step the legacy
 * text digest must be the bytes the pre-trie computeDigest wrote for the same states, claims, dropped lines and
 * acknowledgements, and the stream hash must be an independent annex I.6 fold over the model (written here from the
 * annex text, not from Slot). Ids include astral, private-use, prefix, '=' and control-character ids, and a ground
 * prefix so inner nodes split and collapse below the root.
 */
class AdversaryCrdtDigestTest {

    private static final AgentId AGENT = AgentId.parse("zP/writer");
    private static final SpaceId SPACE = SpaceId.local("adv");

    /** The model: what the maps of ReplicatedSpace held before the trie. */
    private static final class Model {
        final Map<EntryId, EntryState> states = new HashMap<>();
        final Map<EntryId, String> claimHashes = new HashMap<>();
        final Map<EntryId, Object> claims = new HashMap<>();
        final Map<EntryId, String> dropped = new HashMap<>();
        final Map<EntryId, Set<String>> acks = new HashMap<>();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void everyStepMatchesTheOldDigestAndAnIndependentFold(long seed) {
        Random random = new Random(seed);
        List<EntryId> pool = pool();
        Model model = new Model();
        SpaceStateCrdt crdt = SpaceStateCrdt.empty();
        int dots = 0;
        int maxSize = 0;
        for (int step = 0; step < 3_000; step++) {
            EntryId id = pool.get(random.nextInt(pool.size()));
            int op = random.nextInt(16);
            // Grow phases and shrink phases, so the root and the ground prefix split and collapse many times.
            boolean shrinking = (step / 400) % 2 == 1;
            if (shrinking && op < 3) {
                op = 10;
            }
            switch (op) {
                case 0, 1, 2 -> {
                    EntryRecord record = record(id);
                    Dot dot = new Dot("r" + random.nextInt(3), ++dots);
                    LwwRegister<LeaseInfo> lease = lease(random.nextInt(1000), random.nextInt(100_000));
                    crdt = crdt.add(record, dot, lease);
                    EntryState added = EntryState.added(record, dot, lease);
                    model.states.merge(id, added, EntryState::merge);
                }
                case 3 -> {
                    crdt = crdt.remove(id);
                    model.states.computeIfPresent(id, (k, s) -> s.withObservedRemoved());
                }
                case 4 -> {
                    crdt = crdt.complete(id);
                    model.states.computeIfPresent(id, (k, s) -> s.withCompleted());
                }
                case 5 -> {
                    LwwRegister<LeaseInfo> lease = lease(random.nextInt(2000), random.nextInt(100_000));
                    crdt = crdt.setLease(id, lease);
                    model.states.computeIfPresent(id, (k, s) -> s.withLease(lease));
                }
                case 6 -> {
                    EntryState other = EntryState.added(record(id), new Dot("x", ++dots),
                            lease(random.nextInt(2000), random.nextInt(100_000)));
                    if (random.nextBoolean()) {
                        other = other.withObservedRemoved();
                    }
                    crdt = random.nextBoolean() ? crdt.merge(id, other) : crdt.merge(SpaceStateCrdt.of(id, other));
                    model.states.merge(id, other, EntryState::merge);
                }
                case 7 -> {
                    String hash = Integer.toHexString(random.nextInt(64));
                    Object claim = "claim-" + hash;
                    crdt = crdt.withClaim(id, claim, hash);
                    model.claims.put(id, claim);
                    model.claimHashes.put(id, hash);
                }
                case 8 -> {
                    crdt = crdt.withoutClaim(id);
                    model.claims.remove(id);
                    model.claimHashes.remove(id);
                }
                case 9 -> {
                    String hash = random.nextInt(4) == 0 ? null : Integer.toHexString(random.nextInt(64));
                    crdt = crdt.withDropped(id, hash);
                    if (hash == null) {
                        model.dropped.remove(id);
                    } else {
                        model.dropped.put(id, hash);
                    }
                }
                case 10 -> {
                    Set<EntryId> gone = new HashSet<>();
                    int k = random.nextInt(4) == 0 ? 1 + random.nextInt(pool.size()) : 1 + random.nextInt(4);
                    for (int i = 0; i < k; i++) {
                        gone.add(pool.get(random.nextInt(pool.size())));
                    }
                    crdt = crdt.without(gone);
                    gone.forEach(model.states::remove);
                }
                case 11, 12 -> {
                    String line = ackLine(random, id, model);
                    crdt = crdt.withAck(id, line);
                    model.acks.computeIfAbsent(id, k -> new TreeSet<>()).add(line);
                }
                case 13 -> {
                    Set<String> held = model.acks.get(id);
                    String line = held != null && !held.isEmpty() && random.nextBoolean()
                            ? held.iterator().next() : ackLine(random, id, model);
                    crdt = crdt.withoutAck(id, line);
                    if (held != null) {
                        held.remove(line);
                        if (held.isEmpty()) {
                            model.acks.remove(id);
                        }
                    }
                }
                case 14 -> {
                    // A large merge: the batch path of SpaceStateCrdt.merge, including states already held.
                    SpaceStateCrdt bulk = SpaceStateCrdt.empty();
                    int k = 1 + random.nextInt(pool.size());
                    for (int i = 0; i < k; i++) {
                        EntryId other = pool.get(random.nextInt(pool.size()));
                        EntryState held = model.states.get(other);
                        EntryState incoming = held != null && random.nextBoolean() ? held
                                : EntryState.added(record(other), new Dot("y", ++dots),
                                        lease(random.nextInt(2000), random.nextInt(100_000)));
                        bulk = bulk.merge(SpaceStateCrdt.of(other, incoming));
                    }
                    SpaceStateCrdt reversed = bulk.merge(crdt);
                    crdt = crdt.merge(bulk);
                    for (EntryId other : crdt.entryIds()) {
                        assertThat(reversed.state(other)).as("merge commutes over the state of %s", other.value())
                                .isEqualTo(crdt.state(other));
                    }
                    assertThat(reversed).as("merge commutes over the states").isEqualTo(crdt);
                    for (EntryId other : bulk.entryIds()) {
                        model.states.merge(other, bulk.state(other).orElseThrow(), EntryState::merge);
                    }
                }
                default -> {
                    // Nothing changes: an equal re-add of a held state through merge(id, state).
                    EntryState held = model.states.get(id);
                    if (held != null) {
                        SpaceStateCrdt again = crdt.merge(id, held);
                        assertThat(again).as("a held state costs no copy").isSameAs(crdt);
                    }
                }
            }
            maxSize = Math.max(maxSize, model.states.size());
            check(crdt, model, pool, random, "seed " + seed + " step " + step + " op " + op + " id " + id.value());
        }
        assertThat(maxSize).as("the history crossed the leaf threshold").isGreaterThan(64);
    }

    private static void check(SpaceStateCrdt crdt, Model model, List<EntryId> pool, Random random, String at) {
        assertThat(new String(crdt.legacyDigest(), StandardCharsets.UTF_8)).as("legacy digest at %s", at)
                .isEqualTo(new String(oldDigest(model), StandardCharsets.UTF_8));
        Map<String, List<String>> audited = auditedLines(model);
        assertThat(hex(crdt.streamHash())).as("stream hash at %s", at).isEqualTo(hex(foldRoot(audited)));
        assertThat(crdt.knownEntries()).as("known entries at %s", at).isEqualTo(model.states.size());
        assertThat(crdt.claimCount()).as("claims at %s", at).isEqualTo(model.claims.size());
        assertThat(crdt.entryIds()).as("ids at %s", at).isEqualTo(model.states.keySet());
        for (EntryId id : pool) {
            assertThat(crdt.state(id).orElse(null)).as("state of %s at %s", id.value(), at)
                    .isEqualTo(model.states.get(id));
            assertThat(crdt.claim(id)).as("claim of %s at %s", id.value(), at).isEqualTo(model.claims.get(id));
        }
        // The CRDT's equality is over the states alone, whatever lines the slots hold besides.
        SpaceStateCrdt rebuilt = SpaceStateCrdt.empty();
        for (Map.Entry<EntryId, EntryState> e : model.states.entrySet()) {
            rebuilt = rebuilt.merge(SpaceStateCrdt.of(e.getKey(), e.getValue()));
        }
        assertThat(crdt).as("equals at %s", at).isEqualTo(rebuilt);
        assertThat(crdt.hashCode()).as("hashCode at %s", at).isEqualTo(model.states.hashCode());
        Set<EntryId> live = new HashSet<>();
        model.states.forEach((id, s) -> {
            if (s.present()) {
                live.add(id);
            }
        });
        Set<EntryId> liveIds = new HashSet<>();
        crdt.liveRecords().forEach(r -> liveIds.add(r.entryId()));
        assertThat(liveIds).as("live at %s", at).isEqualTo(live);
        // A few random audit paths, against the fold.
        for (int i = 0; i < 3; i++) {
            int depth = random.nextInt(4);
            StringBuilder hexPath = new StringBuilder();
            byte[] path = new byte[depth];
            for (int d = 0; d < depth; d++) {
                path[d] = (byte) random.nextInt(16);
                hexPath.append(Character.forDigit(path[d], 16));
            }
            if (depth > 0 && random.nextBoolean()) {
                // Bias toward the ground prefix 3a7, where the deep nodes are.
                String p = "3a7".substring(0, Math.min(3, depth));
                for (int d = 0; d < p.length(); d++) {
                    path[d] = (byte) Character.digit(p.charAt(d), 16);
                }
                hexPath.replace(0, p.length(), p);
            }
            AuditNode node = crdt.node(path);
            Map<String, List<String>> under = new HashMap<>();
            audited.forEach((id, lines) -> {
                if (key(id).startsWith(hexPath.toString())) {
                    under.put(id, lines);
                }
            });
            assertThat(node.count()).as("count at %s path %s", at, hexPath).isEqualTo(under.size());
            if (!under.isEmpty()) {
                byte[] preimage = preimage(under, hexPath.toString());
                assertThat(hex(node.preimage())).as("preimage at %s path %s", at, hexPath).isEqualTo(hex(preimage));
            }
        }
    }

    // ---- the pre-trie digest, copied in its algorithm from ReplicatedSpace.computeDigest before phase 1

    private static byte[] oldDigest(Model model) {
        TreeMap<String, String> summary = new TreeMap<>();
        for (Map.Entry<EntryId, EntryState> e : model.states.entrySet()) {
            EntryState state = e.getValue();
            summary.put("e:" + e.getKey().value(), Integer.toHexString(Objects.hash(state.adds(), state.removes(),
                    state.lease().stamp(), state.completed())));
        }
        model.claimHashes.forEach((id, hash) -> summary.put("c:" + id.value(), hash));
        model.dropped.forEach((id, hash) -> summary.putIfAbsent("e:" + id.value(), hash));
        StringBuilder sb = new StringBuilder();
        summary.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        TreeSet<String> acks = new TreeSet<>();
        model.acks.values().forEach(acks::addAll);
        acks.forEach(line -> sb.append("a:").append(line).append('\n'));
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ---- an independent fold of annex I.6, from the annex text

    private static Map<String, List<String>> auditedLines(Model model) {
        Set<EntryId> ids = new HashSet<>();
        ids.addAll(model.states.keySet());
        ids.addAll(model.claims.keySet());
        ids.addAll(model.dropped.keySet());
        ids.addAll(model.acks.keySet());
        Map<String, List<String>> out = new HashMap<>();
        for (EntryId id : ids) {
            if (control(id.value())) {
                continue; // the audited set never holds such an id
            }
            Set<String> lines = new HashSet<>();
            EntryState state = model.states.get(id);
            if (state != null) {
                lines.add("e:" + id.value() + "=" + Integer.toHexString(Objects.hash(state.adds(), state.removes(),
                        state.lease().stamp(), state.completed())));
            } else if (model.dropped.containsKey(id)) {
                lines.add("e:" + id.value() + "=" + model.dropped.get(id));
            }
            if (model.claimHashes.containsKey(id)) {
                lines.add("c:" + id.value() + "=" + model.claimHashes.get(id));
            }
            lines.addAll(model.acks.getOrDefault(id, Set.of()));
            if (!lines.isEmpty()) {
                out.put(id.value(), new ArrayList<>(lines));
            }
        }
        return out;
    }

    private static boolean control(String s) {
        return s.chars().anyMatch(c -> c < 0x20 || c == 0x7F);
    }

    private static byte[] foldRoot(Map<String, List<String>> audited) {
        if (audited.isEmpty()) {
            return sha256(new byte[0]);
        }
        return sha256(preimage(audited, ""));
    }

    private static byte[] preimage(Map<String, List<String>> under, String path) {
        if (under.size() <= 32 || path.length() == 64) {
            List<byte[]> lines = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            under.values().forEach(ls -> ls.forEach(l -> {
                if (seen.add(l)) {
                    lines.add(l.getBytes(StandardCharsets.UTF_8));
                }
            }));
            lines.sort(Arrays::compareUnsigned);
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            for (int i = 0; i < lines.size(); i++) {
                if (i > 0) {
                    out.write('\n');
                }
                out.writeBytes(lines.get(i));
            }
            return out.toByteArray();
        }
        byte[] out = new byte[384];
        for (int c = 0; c < 16; c++) {
            String sub = path + Character.forDigit(c, 16);
            Map<String, List<String>> child = new HashMap<>();
            under.forEach((id, lines) -> {
                if (key(id).startsWith(sub)) {
                    child.put(id, lines);
                }
            });
            if (!child.isEmpty()) {
                System.arraycopy(sha256(preimage(child, sub)), 0, out, c * 24, 16);
                long count = child.size();
                for (int i = 7; i >= 0; i--) {
                    out[c * 24 + 16 + i] = (byte) count;
                    count >>>= 8;
                }
            }
        }
        return out;
    }

    // ---- inputs

    private static List<EntryId> pool() {
        List<String> ids = new ArrayList<>(List.of(
                "a", "a=b", "a=", "ab", "e:a", "c", "", "😀", "😀x", "￿", "é",
                "日本", "x\u0001y", "\u007f", "tab\there", "=", "a b", "Z", "zz"));
        for (int i = 0; i < 70; i++) {
            ids.add("e" + i);
        }
        // Ground ids under the key prefix 3a7, so a node below the root holds more than the threshold and splits.
        int found = 0;
        for (int k = 0; found < 45; k++) {
            String id = "g-" + k;
            if (key(id).startsWith("3a7")) {
                ids.add(id);
                found++;
            }
        }
        List<EntryId> out = new ArrayList<>();
        ids.forEach(s -> out.add(EntryId.of(s)));
        return out;
    }

    private static String ackLine(Random random, EntryId id, Model model) {
        EntryState held = model.states.get(id);
        if (held != null && random.nextBoolean()) {
            // An acknowledgement equal to the held line: one line in the audited set, two in the text.
            return "e:" + id.value() + "=" + Integer.toHexString(Objects.hash(held.adds(), held.removes(),
                    held.lease().stamp(), held.completed()));
        }
        String claim = model.claimHashes.get(id);
        if (claim != null && random.nextBoolean()) {
            return "c:" + id.value() + "=" + claim;
        }
        return (random.nextBoolean() ? "e:" : "c:") + id.value() + "=" + Integer.toHexString(random.nextInt(64));
    }

    /** One record per id, as every replica holds the one signed record of an entry (its payload compares by reference). */
    private static final Map<EntryId, EntryRecord> RECORDS = new java.util.concurrent.ConcurrentHashMap<>();

    private static EntryRecord record(EntryId id) {
        return RECORDS.computeIfAbsent(id, AdversaryCrdtDigestTest::newRecord);
    }

    private static EntryRecord newRecord(EntryId id) {
        return new EntryRecord(id, SPACE, "T#v1", new byte[] {1}, null, AGENT, new HlcTimestamp(1L, 0, "seed"),
                new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE), Map.of(), null);
    }

    /** Each lease write gets its own stamp, as an HLC gives: two writes never share one with different values. */
    private static int logical;

    private static LwwRegister<LeaseInfo> lease(long physical, long expiry) {
        return new LwwRegister<>(new HlcTimestamp(physical, ++logical, "n" + (physical % 3)),
                new LeaseInfo(AGENT, expiry, LeaseKind.WRITE));
    }

    private static final Map<String, String> KEYS = new LinkedHashMap<>();

    private static String key(String id) {
        return KEYS.computeIfAbsent(id, i -> hex(sha256(i.getBytes(StandardCharsets.UTF_8))));
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }
}
