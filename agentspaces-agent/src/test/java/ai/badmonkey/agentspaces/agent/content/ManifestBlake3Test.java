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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 0 (P2-H-05, P2-B-06; srad/agentspaces-pdt2.md §6.10.3, rule 11): a manifest carries a
 * blob's BLAKE3 identity and its Bao outboard root in its open text map, under {@code pdt.blake3}
 * and {@code pdt.bao}, so the record keeps its schema and every level-1 reader passes them through.
 * Both are advisory: a reader takes a value only when its text is the CID the attribute promises.
 */
class ManifestBlake3Test {

    private static final String SHA_ROOT = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";

    private static String cid(int codec, int multihash) {
        byte[] binary = new byte[36];
        binary[0] = 0x01;
        binary[1] = (byte) codec;
        binary[2] = (byte) multihash;
        binary[3] = 0x20;
        for (int i = 4; i < 36; i++) {
            binary[i] = (byte) i;
        }
        return Multibase.base32(binary);
    }

    private static Manifest manifest(Map<String, String> attrs) {
        return new Manifest(1, SHA_ROOT, 0, 64 * 1024, 0, new byte[0], null, null, null, null, Manifest.SEALING_NONE,
                null, Manifest.HASHING_PLAIN, PeerId.of("zPeer"), 0, attrs);
    }

    @Test
    void aManifestReadsAWellFormedIdentityAndOutboardRoot() {
        String identity = cid(0x55, 0x1e);
        String bao = cid(0x71, 0x12);
        Manifest manifest = manifest(Map.of(Manifest.BLAKE3_ATTR, identity, Manifest.BAO_ATTR, bao, "origin", "test"));
        assertThat(manifest.blake3()).contains(identity);
        assertThat(manifest.bao()).contains(bao);
        assertThat(manifest.violation()).isEmpty();
        assertThat(manifest.tags()).as("attrs ride as tags, so a template finds a file by its identity (LB-M-09)")
                .containsEntry(Manifest.BLAKE3_ATTR, identity);
        byte[] bytes = CborCodec.defaultCodec().toBytes(manifest);
        assertThat(CborCodec.defaultCodec().fromDocument(bytes, Manifest.class).blake3()).contains(identity);
    }

    @Test
    void aMalformedValueReadsAsAbsentAndBreaksNothing() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(Manifest.BLAKE3_ATTR, cid(0x55, 0x12));
        attrs.put(Manifest.BAO_ATTR, cid(0x55, 0x12));
        Manifest wrongHash = manifest(attrs);
        assertThat(wrongHash.blake3()).as("a sha2-256 CID is no BLAKE3 identity").isEmpty();
        assertThat(wrongHash.bao()).as("an outboard root is a dag-cbor node").isEmpty();
        assertThat(wrongHash.violation()).as("advisory: the manifest stays well formed").isEmpty();
        assertThat(manifest(Map.of(Manifest.BLAKE3_ATTR, "bafkr4i-not-base32")).blake3()).isEmpty();
        assertThat(manifest(Map.of(Manifest.BLAKE3_ATTR, cid(0x71, 0x1e))).blake3()).as("an identity names raw bytes").isEmpty();
        assertThat(manifest(Map.of(Manifest.BLAKE3_ATTR, cid(0x55, 0x1e).toUpperCase())).blake3()).isEmpty();
        String canonical = cid(0x55, 0x1e);
        String alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        char last = canonical.charAt(canonical.length() - 1);
        String trailingBits = canonical.substring(0, canonical.length() - 1) + alphabet.charAt(alphabet.indexOf(last) ^ 1);
        assertThat(Multibase.decode(trailingBits)).as("the decoder drops the unused trailing bits")
                .isEqualTo(Multibase.decode(canonical));
        assertThat(manifest(Map.of(Manifest.BLAKE3_ATTR, trailingBits)).blake3())
                .as("an identity is the one base32 text of its CID (PDT 2 phase 0, rule 11)").isEmpty();
        assertThat(manifest(null).blake3()).isEmpty();
        assertThat(manifest(null).bao()).isEmpty();
    }
}
