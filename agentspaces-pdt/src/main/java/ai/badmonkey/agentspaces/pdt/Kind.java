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
package ai.badmonkey.agentspaces.pdt;

import java.util.Set;

/**
 * The node kinds of srad/agentspaces-pdt.md §3.1 (PD-N-01) and of PDT 2 (srad/agentspaces-pdt2.md
 * §3), carried in every structured node's {@code k} field. A blob chunk is raw bytes with no map
 * and no kind.
 *
 * <p>The seven level-1 kinds ({@code m}, {@code mc}, {@code v}, {@code vr}, {@code b}, {@code x},
 * {@code sn}) are the annex H set every 0.5.0 peer reads. PDT 2 phase 0 adds seven level-2 kinds
 * that live only where no level-1 reader looks (srad/agentspaces-pdt2.md §6.10.3, rules 1 to 5):
 * {@code db}, {@code dn}, {@code rr}, {@code rv}, and {@code t} under a {@code PRef.v} 2 head, and
 * {@code vg} and {@code bo} behind the reserved attributes {@code pdt.ancestry} and
 * {@code pdt.bao}. Kinds {@link #S}, {@link #TM}, and {@link #EC} are reserved (PD-F-01,
 * P2-F-04): a decoder refuses them by name until a later release gives them a schema.
 */
public final class Kind {

    /** CHAMP map interior or leaf node (§3.2). */
    public static final String M = "m";

    /** CHAMP map collision node past level 51 (§3.2). */
    public static final String MC = "mc";

    /** Vector trie interior or leaf node (§3.2). */
    public static final String V = "v";

    /** Vector root with count, shift, and tail (§3.2). */
    public static final String VR = "vr";

    /** Blob interior node of the prolly tree (§3.3). */
    public static final String B = "b";

    /** Value spill: a large CBOR value held out of line (PD-N-05). */
    public static final String X = "x";

    /** Sealed node envelope: a node sealed under a per-epoch key with its links in the clear (§3.8, PD-P-05). */
    public static final String SN = "sn";

    /** PDT 2 phase 0 (P2-D-01): the root of a DataScript database (srad/agentspaces-pdt2.md §3.1.1). */
    public static final String DB = "db";

    /** PDT 2 phase 0 (P2-D-01): a DataScript sorted-set node, an index branch or leaf or a tail (§3.1.1). */
    public static final String DN = "dn";

    /** PDT 2 phase 0 (P2-R-01): the root of an RRB vector that still carries a relaxed node (§3.7). */
    public static final String RR = "rr";

    /** PDT 2 phase 0 (P2-R-01): an RRB interior node, with cumulative sizes when relaxed (§3.7). */
    public static final String RV = "rv";

    /** PDT 2 phase 0 (P2-T-01): a text leaf of Fugue runs (§3.9). */
    public static final String T = "t";

    /** PDT 2 phase 0 (P2-V-01): a version graph node, named by the attribute {@code pdt.ancestry} (§3.10). */
    public static final String VG = "vg";

    /** PDT 2 phase 0 (P2-B-02): a node of a blob's Bao outboard, named by the attribute {@code pdt.bao} (§3.5). */
    public static final String BO = "bo";

    /** Reserved: sorted map (PD-V-10, PD-F-01, P2-D-11). */
    public static final String S = "s";

    /** Reserved: text marks (P2-F-04, P2-F-11). */
    public static final String TM = "tm";

    /** Reserved: erasure-coded stripe (P2-F-04, P2-F-10). */
    public static final String EC = "ec";

    private static final Set<String> KNOWN = Set.of(M, MC, V, VR, B, X, SN, DB, DN, RR, RV, T, VG, BO);
    private static final Set<String> RESERVED = Set.of(S, TM, EC);

    private Kind() {
    }

    /** Whether a kind has a schema in this release: the seven level-1 kinds and the seven of PDT 2. */
    public static boolean known(String kind) {
        return kind != null && KNOWN.contains(kind);
    }

    /** Whether a kind is reserved for a later release: {@code s}, {@code tm}, {@code ec}. */
    public static boolean reserved(String kind) {
        return kind != null && RESERVED.contains(kind);
    }

    /** The kinds with a schema. */
    public static Set<String> all() {
        return KNOWN;
    }
}
