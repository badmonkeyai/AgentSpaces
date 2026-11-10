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
package ai.badmonkey.agentspaces.pdt.blob;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.content.tree.Cids;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What a head or a manifest claims about a blob (PDT 2 phase 4, P2-B-01, P2-B-06; srad/agentspaces-pdt2.md §3.5,
 * rules 10 and 11 of §6.10.3): its BLAKE3 identity under {@code pdt.blake3} and the root of its {@code bo} outboard
 * under {@code pdt.bao}. A reader verifies a window against the pair ({@link PBlob#openVerified}); the attributes are
 * advisory, so a pair that does not read as two canonical CIDs of the expected codec and hash reads as absent, and
 * nothing is refused or struck for it.
 *
 * @param identity the identity: a CIDv1 of codec {@code raw} over multihash 0x1e
 * @param bao      the root of the outboard: a CIDv1 of codec {@code dag-cbor} over sha2-256
 */
public record BlobIdentity(String identity, String bao) {

    /** Checks both names. */
    public BlobIdentity {
        if (!Cids.isIdentity(identity)) {
            throw new IllegalArgumentException("pdt.blake3 names a BLAKE3 identity, found " + identity);
        }
        if (!Cids.isCid(bao) || Cids.parse(bao).codec() != Cids.DAG_CBOR) {
            throw new IllegalArgumentException("pdt.bao names a dag-cbor node, found " + bao);
        }
    }

    /** The claim of a built outboard. */
    public static BlobIdentity of(BaoOutboard.Built outboard) {
        return new BlobIdentity(outboard.identity(), outboard.root());
    }

    /**
     * The claim in a record's attributes, when both read.
     *
     * @param attrs a head's or a manifest's attributes, or null
     * @return the claim, or empty when either attribute is absent or does not read
     */
    public static Optional<BlobIdentity> read(Map<String, ?> attrs) {
        if (attrs == null || !(attrs.get(Manifest.BLAKE3_ATTR) instanceof String identity)
                || !(attrs.get(Manifest.BAO_ATTR) instanceof String bao)) {
            return Optional.empty();
        }
        try {
            return Optional.of(new BlobIdentity(identity, bao));
        } catch (IllegalArgumentException advisory) {
            return Optional.empty();
        }
    }

    /** The two attributes, {@code pdt.blake3} first. */
    public Map<String, String> attrs() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put(Manifest.BLAKE3_ATTR, identity);
        attrs.put(Manifest.BAO_ATTR, bao);
        return attrs;
    }

    /** The BLAKE3 hash the identity names. */
    byte[] digest() {
        return Cids.identityDigest(identity);
    }
}
