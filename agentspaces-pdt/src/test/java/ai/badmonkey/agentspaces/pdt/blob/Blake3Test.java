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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;
import org.bouncycastle.crypto.digests.Blake3Digest;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 0 (P2-B-02, decision 10): the BLAKE3 identity of a blob and the chaining values of
 * its 16 KiB chunk groups. The identity is BouncyCastle's {@code Blake3Digest}; the group and
 * parent chaining values come from this module's own compression, because the digest exposes no
 * subtree, and every root the tree computes equals the digest.
 */
class Blake3Test {

    private static byte[] bc(byte[] data) {
        Blake3Digest digest = new Blake3Digest(256);
        digest.update(data, 0, data.length);
        byte[] out = new byte[32];
        digest.doFinal(out, 0);
        return out;
    }

    private static byte[] seeded(int size) {
        byte[] data = new byte[size];
        new Random(42).nextBytes(data);
        return data;
    }

    @Test
    void theDigestMatchesThePublishedVectors() {
        assertThat(HexFormat.of().formatHex(Blake3.digest(new byte[0])))
                .isEqualTo("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262");
        assertThat(HexFormat.of().formatHex(Blake3.digest("abc".getBytes(StandardCharsets.US_ASCII))))
                .isEqualTo("6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85");
    }

    @Test
    void theTreeRootEqualsTheDigestAtEveryBoundary() {
        int[] sizes = {0, 1, 63, 64, 65, 1023, 1024, 1025, 2048, 3073, 16383, 16384, 16385, 32768, 32769, 49152,
                100 * 1024, 1024 * 1024 + 5};
        for (int size : sizes) {
            byte[] data = seeded(size);
            assertThat(Blake3.treeRoot(data)).as("%d bytes", size).isEqualTo(bc(data));
        }
    }

    @Test
    void theIdentityIsARawCidOverTheBlake3Multihash() {
        byte[] digest = Blake3.digest(seeded(1000));
        String identity = Blake3.identity(digest);
        assertThat(identity).startsWith("bafkr4i");
        assertThat(identity).isNotEqualTo(Cids.ofDigest(digest, Cids.RAW)).as("0x1e is no sha2-256 name");
        Manifest manifest = new Manifest(1, Cids.of(new byte[1], Cids.RAW), 0, 64 * 1024, 0, new byte[0], null, null,
                null, null, Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, PeerId.of("zPeer"), 0,
                Map.of(Manifest.BLAKE3_ATTR, identity));
        assertThat(manifest.blake3()).as("the manifest reads the identity this module writes").contains(identity);
    }

    @Test
    void anOutboardOfOneGroupHasNoParents() throws Exception {
        BaoOutboard.Built built = BaoOutboard.of(new ByteArrayInputStream(seeded(5000)));
        assertThat(built.nodes()).hasSize(1);
        BoNode root = BoNode.decode(built.root(), built.nodes().get(built.root()));
        assertThat(root.size()).isEqualTo(5000);
        assertThat(root.parents()).isEmpty();
        assertThat(built.identity()).isEqualTo(Blake3.identity(bc(seeded(5000))));
    }

    @Test
    void anOutboardHoldsThePreOrderParentsOfTheGroupTree() throws Exception {
        byte[] data = seeded(100 * 1024);
        BaoOutboard.Built built = BaoOutboard.of(new ByteArrayInputStream(data));
        assertThat(built.identity()).isEqualTo(Blake3.identity(bc(data)));
        BoNode root = BoNode.decode(built.root(), built.nodes().get(built.root()));
        byte[] parents = root.parents();
        assertThat(parents).hasSize(6 * BoNode.PARENT_BYTES);
        // seven groups: the root splits 4 | 3, the left 2 | 2, the right 2 | 1
        byte[][] groups = new byte[7][];
        for (int g = 0; g < 7; g++) {
            int start = g * BoNode.GROUP_BYTES;
            groups[g] = Blake3.groupCv(data, start, Math.min(BoNode.GROUP_BYTES, data.length - start), (long) g << BoNode.GROUP_LOG);
        }
        byte[] g01 = Blake3.parent(groups[0], groups[1], false);
        byte[] g23 = Blake3.parent(groups[2], groups[3], false);
        byte[] g45 = Blake3.parent(groups[4], groups[5], false);
        byte[] left = Blake3.parent(g01, g23, false);
        byte[] right = Blake3.parent(g45, groups[6], false);
        List<byte[]> preOrder = List.of(concat(left, right), concat(g01, g23), concat(groups[0], groups[1]),
                concat(groups[2], groups[3]), concat(g45, groups[6]), concat(groups[4], groups[5]));
        for (int i = 0; i < preOrder.size(); i++) {
            assertThat(Arrays.copyOfRange(parents, i * 64, i * 64 + 64)).as("parent %d", i).isEqualTo(preOrder.get(i));
        }
        assertThat(Blake3.parent(left, right, true)).as("the root parent hashes to the identity").isEqualTo(bc(data));
    }

    @Test
    void theRootPairOfEveryOutboardHashesToTheIdentity() throws Exception {
        // PDT 2 phase 0 (P2-B-02): the pre-order parents follow BLAKE3's split (the largest power of two
        // strictly below the group count on the left) at every count, the uneven ones included
        for (int groups = 2; groups <= 20; groups++) {
            for (int size : new int[]{(groups - 1) * BoNode.GROUP_BYTES + 1, groups * BoNode.GROUP_BYTES}) {
                byte[] data = seeded(size);
                BaoOutboard.Built built = BaoOutboard.of(new ByteArrayInputStream(data));
                byte[] parents = BoNode.decode(built.root(), built.nodes().get(built.root())).parents();
                assertThat(parents).as("%d groups", groups).hasSize((groups - 1) * BoNode.PARENT_BYTES);
                assertThat(Blake3.parent(Arrays.copyOfRange(parents, 0, 32), Arrays.copyOfRange(parents, 32, 64), true))
                        .as("%d bytes in %d groups", size, groups).isEqualTo(bc(data));
            }
        }
    }

    @Test
    void theOutboardOfAnEmptyBlobIsOneEmptyLeaf() throws Exception {
        BaoOutboard.Built built = BaoOutboard.of(new ByteArrayInputStream(new byte[0]));
        assertThat(BoNode.decode(built.root(), built.nodes().get(built.root()))).isEqualTo(new BoNode(0, new byte[0], List.of()));
        assertThat(built.identity()).isEqualTo(Blake3.identity(bc(new byte[0])));
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
