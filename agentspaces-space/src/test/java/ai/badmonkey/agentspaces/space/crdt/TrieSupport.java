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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * What the trie tests share: a state to hang lines on, ids ground to a key prefix, and a reference fold of annex I.6
 * written straight from its text, independent of the trie's own code, so each test can compare the trie against it.
 */
final class TrieSupport {

    static final HexFormat HEX = HexFormat.of();
    private static final AgentId AGENT = AgentId.parse("zP/writer");
    private static final SpaceId SPACE = SpaceId.local("trie");

    private TrieSupport() {
    }

    /** A state for an id; the trie reads only the hash handed in beside it, so one shape of state serves. */
    static EntryState state(String id) {
        EntryRecord record = new EntryRecord(EntryId.of(id), SPACE, "T#v1", new byte[]{1}, null, AGENT,
                new HlcTimestamp(1L, 0, "seed"), new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE), Map.of(), null);
        return EntryState.added(record, new Dot("r1", 1),
                new LwwRegister<>(new HlcTimestamp(1L, 0, "r1"), new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE)));
    }

    /** A slot holding one state line {@code e:<id>=<hash>}. */
    static Slot<String> stateSlot(String id, String hash) {
        return Slot.<String>empty(EntryId.of(id)).withState(state(id), hash);
    }

    /** The first n ids of the form tag-k whose key starts with the hex prefix. */
    static List<String> ground(String prefix, int n, String tag) {
        List<String> out = new ArrayList<>(n);
        for (int k = 0; out.size() < n; k++) {
            String id = tag + "-" + k;
            if (HEX.formatHex(Slot.keyOf(EntryId.of(id))).startsWith(prefix)) {
                out.add(id);
            }
        }
        return out;
    }

    /** The audited lines of every slot, as a set: what two replicas compare. */
    static Set<String> auditedSet(Collection<? extends Slot<?>> slots) {
        Set<String> lines = new TreeSet<>();
        for (Slot<?> slot : slots) {
            lines.addAll(slot.auditedLines());
        }
        return lines;
    }

    /** The reference stream hash of annex I.6 over audited slots, recomputed from nothing. */
    static byte[] referenceStreamHash(Collection<? extends Slot<?>> slots) {
        Map<String, List<String>> byId = byId(slots);
        if (byId.isEmpty()) {
            return sha256(new byte[0]);
        }
        return referenceNode(byId, "")[1];
    }

    /** The reference fingerprint and count at a hex path, or null when no audited slot lies under it. */
    static Object[] referenceFingerprint(Collection<? extends Slot<?>> slots, String path) {
        Map<String, List<String>> under = new HashMap<>();
        byId(slots).forEach((id, lines) -> {
            if (key(id).startsWith(path)) {
                under.put(id, lines);
            }
        });
        if (under.isEmpty()) {
            return null;
        }
        byte[][] node = referenceNode(under, path);
        return new Object[] {Arrays.copyOf(node[1], 16), under.size()};
    }

    private static Map<String, List<String>> byId(Collection<? extends Slot<?>> slots) {
        Map<String, List<String>> byId = new HashMap<>();
        for (Slot<?> slot : slots) {
            List<String> lines = slot.auditedLines();
            if (!lines.isEmpty()) {
                byId.put(slot.id().value(), lines);
            }
        }
        return byId;
    }

    /** Returns {preimage, sha256} of the node at a hex path over the ids under it. */
    private static byte[][] referenceNode(Map<String, List<String>> under, String path) {
        byte[] preimage;
        if (under.size() <= 32 || path.length() == 64) {
            TreeSet<byte[]> lines = new TreeSet<>(Arrays::compareUnsigned);
            under.values().forEach(ls -> ls.forEach(l -> lines.add(l.getBytes(StandardCharsets.UTF_8))));
            List<byte[]> joined = new ArrayList<>(lines);
            int size = Math.max(0, joined.size() - 1);
            for (byte[] l : joined) {
                size += l.length;
            }
            preimage = new byte[size];
            int at = 0;
            for (byte[] l : joined) {
                if (at > 0) {
                    preimage[at++] = '\n';
                }
                System.arraycopy(l, 0, preimage, at, l.length);
                at += l.length;
            }
        } else {
            preimage = new byte[384];
            for (int c = 0; c < 16; c++) {
                String sub = path + Character.forDigit(c, 16);
                Map<String, List<String>> child = new HashMap<>();
                under.forEach((id, lines) -> {
                    if (key(id).startsWith(sub)) {
                        child.put(id, lines);
                    }
                });
                if (!child.isEmpty()) {
                    byte[] digest = referenceNode(child, sub)[1];
                    System.arraycopy(digest, 0, preimage, c * 24, 16);
                    long count = child.size();
                    for (int i = 7; i >= 0; i--) {
                        preimage[c * 24 + 16 + i] = (byte) count;
                        count >>>= 8;
                    }
                }
            }
        }
        return new byte[][] {preimage, sha256(preimage)};
    }

    /** The hex key of an id; remembered, since the reference fold asks for the same keys at every level. */
    static String key(String id) {
        return KEYS.computeIfAbsent(id, i -> HEX.formatHex(sha256(i.getBytes(StandardCharsets.UTF_8))));
    }

    private static final Map<String, String> KEYS = new java.util.concurrent.ConcurrentHashMap<>();

    static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A hex path such as "5e" as one nibble per byte. */
    static byte[] path(String hex) {
        byte[] out = new byte[hex.length()];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Character.digit(hex.charAt(i), 16);
        }
        return out;
    }
}
