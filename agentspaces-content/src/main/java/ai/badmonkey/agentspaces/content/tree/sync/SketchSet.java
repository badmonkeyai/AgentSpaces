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
package ai.badmonkey.agentspaces.content.tree.sync;

import ai.badmonkey.agentspaces.content.tree.Cids;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A set of CIDs as sketch items (PDT 2 phase 5, P2-K-02, P2-K-03): the id of a CID is its 32-byte sha2-256 digest
 * (§3.6), and every item stands at timestamp 0. A holding's {@code issued} differs from holder to holder, so the same
 * root would stand at two places in two members' sets; with one timestamp the items sort by digest alone and one root
 * is one item everywhere. A text that is no canonical sha2-256 CID is left out, and a raw and a dag-cbor CID of one
 * digest are one item.
 */
public final class SketchSet {

    private final Map<String, String> cidsByDigest;
    private final List<byte[]> ids;
    private final Negentropy.Storage storage;

    private SketchSet(Map<String, String> cidsByDigest, List<byte[]> ids) {
        this.cidsByDigest = cidsByDigest;
        this.ids = ids;
        List<Negentropy.Item> items = new ArrayList<>(ids.size());
        for (byte[] id : ids) {
            items.add(new Negentropy.Item(0, id));
        }
        this.storage = Negentropy.Storage.of(items);
    }

    /**
     * The set of a collection of CIDs.
     *
     * @param cids the CIDs
     * @return the set
     */
    public static SketchSet of(Collection<String> cids) {
        Map<String, String> byDigest = new LinkedHashMap<>();
        List<byte[]> ids = new ArrayList<>();
        for (String cid : cids) {
            if (!Cids.isCid(cid)) {
                continue;
            }
            byte[] digest = Cids.parse(cid).digest();
            if (byDigest.putIfAbsent(HexFormat.of().formatHex(digest), cid) == null) {
                ids.add(digest);
            }
        }
        return new SketchSet(byDigest, ids);
    }

    /** The items. */
    public int size() {
        return ids.size();
    }

    /** The ids, in the order the CIDs came. */
    List<byte[]> ids() {
        return ids;
    }

    /** The sealed negentropy storage of the items. */
    Negentropy.Storage storage() {
        return storage;
    }

    /**
     * The CID this set holds for an id.
     *
     * @param id a 32-byte digest
     * @return the CID, or empty when the set holds no CID of that digest
     */
    public Optional<String> cid(byte[] id) {
        return Optional.ofNullable(cidsByDigest.get(HexFormat.of().formatHex(id)));
    }
}
