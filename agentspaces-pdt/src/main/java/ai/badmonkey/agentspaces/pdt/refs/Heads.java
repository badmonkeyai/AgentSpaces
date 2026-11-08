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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * The heads of one ref as a reader sees them (srad/agentspaces-pdt.md §3.6):
 * the live, loadable {@code PRef} entries of the name that no other such entry
 * lists in its base, with equal roots folded into one head (PD-R-08). The order
 * is the merge order of §3.6.1: issued HLC ascending, ties by the issuer's
 * PeerId bytes, then by root, so every member folds the same heads in the same
 * order.
 *
 * @param name  the ref name
 * @param heads the heads in merge order; empty when the ref has no version
 */
public record Heads(String name, List<Head> heads) {

    /**
     * One version of a ref: the fields of its head entry a reader keeps, with
     * the record's issue stamp and issuer for the merge order.
     *
     * @param root            the root CID
     * @param base            the roots it was computed from
     * @param seq             its sequence number
     * @param size            its entry count or byte count
     * @param kind            the kind the head names
     * @param mode            the mode the head names
     * @param issuer          the authenticated writer
     * @param issued          the record's issue stamp
     * @param entryId         the entry id of the head record this version was last seen under
     * @param expiresAtMillis when its write lease lapses, as last seen
     */
    public record Head(String root, List<String> base, long seq, long size, String kind, String mode,
                       AgentId issuer, HlcTimestamp issued, EntryId entryId, long expiresAtMillis) {

        public Head {
            Objects.requireNonNull(root, "root");
            base = base == null ? List.of() : List.copyOf(base);
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(issuer, "issuer");
            Objects.requireNonNull(issued, "issued");
        }

        /** This version seen again under another entry: the later lease and entry id, the same content. */
        public Head seenAs(EntryId entryId, long expiresAtMillis) {
            return new Head(root, base, seq, size, kind, mode, issuer, issued, entryId, expiresAtMillis);
        }

        /**
         * This version with another base list: the union of the bases every
         * head of this root named, so the version graph keeps the links of a
         * version a later swap recreated (PD-R-06).
         */
        public Head withBase(List<String> base) {
            return new Head(root, base, seq, size, kind, mode, issuer, issued, entryId, expiresAtMillis);
        }
    }

    /** The merge order of §3.6.1: issued HLC, then the issuer's PeerId bytes, then the root. */
    public static final Comparator<Head> MERGE_ORDER = (a, b) -> {
        int byPhysical = Long.compare(a.issued().physical(), b.issued().physical());
        if (byPhysical != 0) {
            return byPhysical;
        }
        int byLogical = Integer.compare(a.issued().logical(), b.issued().logical());
        if (byLogical != 0) {
            return byLogical;
        }
        int byIssuer = Arrays.compareUnsigned(a.issuer().peer().value().getBytes(StandardCharsets.UTF_8),
                b.issuer().peer().value().getBytes(StandardCharsets.UTF_8));
        if (byIssuer != 0) {
            return byIssuer;
        }
        return a.root().compareTo(b.root());
    };

    public Heads {
        Objects.requireNonNull(name, "name");
        List<Head> ordered = new ArrayList<>(heads == null ? List.of() : heads);
        ordered.sort(MERGE_ORDER);
        heads = List.copyOf(ordered);
    }

    /** Whether the ref has no version. */
    public boolean isEmpty() {
        return heads.isEmpty();
    }

    /** The roots of the heads in merge order. */
    public List<String> roots() {
        List<String> roots = new ArrayList<>(heads.size());
        for (Head head : heads) {
            roots.add(head.root());
        }
        return roots;
    }

    /** The largest sequence number among the heads, 0 when there is none. */
    public long maxSeq() {
        long max = 0;
        for (Head head : heads) {
            max = Math.max(max, head.seq());
        }
        return max;
    }

    /** The head with the latest issued HLC, or null when there is none. */
    public Head latest() {
        return heads.isEmpty() ? null : heads.get(heads.size() - 1);
    }
}
