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
 * The node kinds of srad/agentspaces-pdt.md §3.1 (PD-N-01), carried in every
 * structured node's {@code k} field. A blob chunk is raw bytes with no map and no
 * kind. Kinds {@link #S} and {@link #RR} are reserved by PD-F-01: a decoder refuses
 * them by name until a later release gives them a schema.
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

    /** Reserved: sorted map (PD-V-10, PD-F-01). */
    public static final String S = "s";

    /** Reserved: RRB vector (PD-F-11, PD-F-01). */
    public static final String RR = "rr";

    private static final Set<String> KNOWN = Set.of(M, MC, V, VR, B, X, SN);
    private static final Set<String> RESERVED = Set.of(S, RR);

    private Kind() {
    }

    /** Whether a kind has a schema in this release: {@code m}, {@code mc}, {@code v}, {@code vr}, {@code b}, {@code x}, {@code sn}. */
    public static boolean known(String kind) {
        return kind != null && KNOWN.contains(kind);
    }

    /** Whether a kind is reserved for a later release: {@code s}, {@code rr}. */
    public static boolean reserved(String kind) {
        return kind != null && RESERVED.contains(kind);
    }

    /** The kinds with a schema, in the order of §3.1. */
    public static Set<String> all() {
        return KNOWN;
    }
}
