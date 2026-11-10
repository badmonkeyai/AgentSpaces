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
package ai.badmonkey.agentspaces.agent.content;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.WireRecord;
import ai.badmonkey.agentspaces.common.id.PeerId;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A member's request that a content store hold a root (PDT 2 phase 0, P2-S-02;
 * srad/agentspaces-pdt2.md §3.2.2): the terms a requester signs. Since the PDT 2 follow-ups of
 * 2026-11-09 (F1) the entry a store takes is the {@link SignedPin} that carries these terms under the
 * requester's signature, tagged {@link #ROOT_TAG} and {@link #REQUESTER_TAG}, the way a connector
 * takes a {@code DataQuery}. Named {@code ai.badmonkey.agentspaces.agent.content.StorePin#v1}.
 *
 * <p>A protocol record with the annex I schema of §6.10.4, read from its own wire encoding alone
 * ({@link WireRecord}), so a payload with a field this schema does not name is undecodable. The
 * shape never gains a field; a change of shape takes a new type name (P2-F-08).
 *
 * @param v         schema version, {@link #VERSION}
 * @param root      the root to hold: a tree root, or an artifact's manifest root
 * @param kind      {@link #KIND_TREE} or {@link #KIND_ARTIFACT}
 * @param lease     the seconds the requester asks the store to hold the root, renewed by a rewrite
 * @param copies    the stores the requester wants holding it, at least 1 and at most a store's {@code copiesMax}
 * @param requester the requesting peer
 * @param haves     bases the store may already hold, at most {@link #MAX_HAVES}, which a tree fetch sends as its haves
 * @param attrs     user attributes, at most {@link #MAX_ATTRS} entries and {@link #MAX_ATTRS_BYTES} bytes, or null
 */
@WireRecord
public record StorePin(int v, String root, String kind, long lease, int copies, PeerId requester, List<String> haves,
                       @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, String> attrs) {

    /** Schema version this record carries. */
    public static final int VERSION = 1;

    /** Hold a persistent tree, fetched by the tree exchange. */
    public static final String KIND_TREE = "tree";

    /** Hold a flat artifact, fetched by the piece swarm. */
    public static final String KIND_ARTIFACT = "artifact";

    /** Tag every pin carries: the root. */
    public static final String ROOT_TAG = "root";

    /** Tag every pin carries: the requester. */
    public static final String REQUESTER_TAG = "requester";

    /** Bound on {@link #haves}. */
    public static final int MAX_HAVES = 8;

    /** Bounds on {@link #attrs}. */
    public static final int MAX_ATTRS = 32;
    public static final int MAX_ATTRS_BYTES = 4 * 1024;

    /** Copies the lists and refuses a null element, which no list on the wire holds. */
    public StorePin {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(requester, "requester");
        haves = haves == null ? List.of() : List.copyOf(haves);
        attrs = attrs == null ? null : Map.copyOf(attrs);
    }

    /** The tags a pin entry is written with, its {@link SignedPin}'s included. */
    public Map<String, String> tags() {
        return Map.of(ROOT_TAG, root, REQUESTER_TAG, requester.value());
    }

    /**
     * Checks the bounds of §3.2.2 that a store judges before it takes the pin.
     *
     * @return the first violated rule, or empty when the pin is well formed
     */
    public Optional<String> violation() {
        if (v != VERSION) {
            return Optional.of("unsupported StorePin version " + v);
        }
        if (!PRef.isCid(root)) {
            return Optional.of("root is not a CID");
        }
        if (!KIND_TREE.equals(kind) && !KIND_ARTIFACT.equals(kind)) {
            return Optional.of("unknown kind " + kind);
        }
        if (lease < 1) {
            return Optional.of("lease of " + lease + " seconds");
        }
        if (copies < 1) {
            return Optional.of("copies below 1");
        }
        if (haves.size() > MAX_HAVES) {
            return Optional.of("more than " + MAX_HAVES + " haves");
        }
        if (!haves.stream().allMatch(PRef::isCid)) {
            return Optional.of("have is not a CID");
        }
        if (attrs != null) {
            if (attrs.size() > MAX_ATTRS) {
                return Optional.of("more than " + MAX_ATTRS + " attrs");
            }
            int bytes = 0;
            for (Map.Entry<String, String> e : attrs.entrySet()) {
                bytes += e.getKey().getBytes(StandardCharsets.UTF_8).length + e.getValue().getBytes(StandardCharsets.UTF_8).length;
            }
            if (bytes > MAX_ATTRS_BYTES) {
                return Optional.of("attrs larger than " + MAX_ATTRS_BYTES + " bytes");
            }
        }
        return Optional.empty();
    }
}
