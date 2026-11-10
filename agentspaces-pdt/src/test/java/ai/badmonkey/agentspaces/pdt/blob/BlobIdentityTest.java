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
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A blob's identity and outboard (PDT 2 phase 4, P2-B-01, P2-B-02; srad/agentspaces-pdt2.md §3.5): the identity is the
 * BLAKE3 of the whole content as a {@code raw} CID over multihash 0x1e, whatever the chunking, and the outboard is the
 * {@code bo} tree a reader reaches only through {@code pdt.bao}.
 */
class BlobIdentityTest {

    private final NodeStore store = NodeStore.inMemory();

    private static byte[] seeded(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    @Test
    void theIdentityIsTheBlake3OfTheContentWhateverTheChunking() throws Exception {
        byte[] bytes = seeded(300 * 1024 + 7, 1);
        String expected = Cids.identity(Blake3.digest(bytes));
        PBlob whole = PBlob.of(bytes, store);
        PBlob built = PBlob.of(Arrays.copyOf(bytes, 100_000), store).append(Arrays.copyOfRange(bytes, 100_000, bytes.length));
        assertThat(whole.identity()).isEqualTo(expected).startsWith("bafkr4i");
        assertThat(built.identity()).isEqualTo(expected);
        assertThat(BaoOutboard.of(new ByteArrayInputStream(bytes)).identity()).isEqualTo(expected);
        assertThat(Blake3.identity(Blake3.digest(bytes))).as("one encoding of the identity").isEqualTo(expected);
    }

    @Test
    void theOutboardIsStoredOnceAndNamesTheSameIdentity() throws Exception {
        byte[] bytes = seeded(700_000, 2);
        PBlob blob = PBlob.of(bytes, store);
        BaoOutboard.Built outboard = blob.outboard();
        assertThat(blob.outboard()).isSameAs(outboard);
        assertThat(outboard.identity()).isEqualTo(blob.identity());
        assertThat(outboard.root()).isEqualTo(BaoOutboard.of(new ByteArrayInputStream(bytes)).root());
        for (Map.Entry<String, byte[]> node : outboard.nodes().entrySet()) {
            assertThat(store.get(node.getKey())).get().isEqualTo(node.getValue());
        }
        assertThat(store.reachable(blob.root())).as("rule 5: the blob tree links no bo node").doesNotContain(outboard.root());
    }

    @Test
    void anEditHasTheIdentityOfItsNewBytes() {
        byte[] bytes = seeded(200_000, 3);
        PBlob v1 = PBlob.of(bytes, store);
        PBlob v2 = v1.splice(150_000, 10, new byte[]{1, 2, 3});
        byte[] after = new byte[bytes.length - 7];
        System.arraycopy(bytes, 0, after, 0, 150_000);
        after[150_000] = 1;
        after[150_001] = 2;
        after[150_002] = 3;
        System.arraycopy(bytes, 150_010, after, 150_003, bytes.length - 150_010);
        assertThat(v2.identity()).isEqualTo(Cids.identity(Blake3.digest(after))).isNotEqualTo(v1.identity());
    }

    @Test
    void aClaimReadsBothAttributesOrNone() {
        PBlob blob = PBlob.of(seeded(50_000, 4), store);
        BlobIdentity identity = BlobIdentity.of(blob.outboard());
        Map<String, String> attrs = identity.attrs();
        assertThat(attrs).containsOnlyKeys(Manifest.BLAKE3_ATTR, Manifest.BAO_ATTR)
                .containsEntry("pdt.blake3", blob.identity()).containsEntry("pdt.bao", blob.outboard().root());
        assertThat(BlobIdentity.read(attrs)).contains(identity);
        assertThat(BlobIdentity.read(Map.of(Manifest.BLAKE3_ATTR, blob.identity()))).as("the identity alone verifies no window").isEmpty();
        assertThat(BlobIdentity.read(Map.of(Manifest.BLAKE3_ATTR, blob.root(), Manifest.BAO_ATTR, blob.outboard().root())))
                .as("advisory: a sha2-256 CID under pdt.blake3 reads as absent").isEmpty();
        assertThat(BlobIdentity.read(Map.of(Manifest.BLAKE3_ATTR, blob.identity(), Manifest.BAO_ATTR, blob.root())))
                .as("pdt.bao names a dag-cbor node").isEmpty();
        assertThat(BlobIdentity.read(null)).isEmpty();
        assertThatThrownBy(() -> new BlobIdentity(blob.root(), blob.outboard().root())).isInstanceOf(IllegalArgumentException.class);
    }
}
