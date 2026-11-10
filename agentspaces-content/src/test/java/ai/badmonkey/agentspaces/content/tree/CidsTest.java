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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.content.ContentRoots;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link Cids}: the CIDv1 form of PD-N-02 against known vectors, round trips, and refusals by name (PD-N-04). */
class CidsTest {

    /** The IPFS CID of the empty raw block: sha2-256 of zero bytes, codec raw. */
    private static final String EMPTY_RAW = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

    /** The IPFS CID of the empty DAG-CBOR map ({@code 0xa0}). */
    private static final String EMPTY_DAG_CBOR_MAP = "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua";

    /**
     * PDT 2 phase 1, P2-X-09 (phase 0 gate finding G2, srad/agentspaces-pdt2.md §10.5): a CID's
     * text is its one canonical base32 form. 36 bytes take 58 base32 characters with two padding
     * bits in the last; text whose padding bits are set decodes to the same bytes, and every port
     * refuses it, so the content swarm's strict reads refuse it too.
     */
    @Test
    void aCidWithPaddingBitsSetIsNoCid() {
        String canonical = Cids.of(new byte[]{1, 2, 3}, Cids.RAW);
        String alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        char last = canonical.charAt(canonical.length() - 1);
        String padded = canonical.substring(0, canonical.length() - 1) + alphabet.charAt(alphabet.indexOf(last) | 1);
        assertThat(padded).isNotEqualTo(canonical);
        assertThat(Cids.parse(padded).digest()).as("the same bytes").isEqualTo(Cids.parse(canonical).digest());
        assertThat(Cids.isCid(canonical)).isTrue();
        assertThat(Cids.isCid(padded)).isFalse();
        assertThat(Cids.isCid(canonical.toUpperCase(java.util.Locale.ROOT))).isFalse();
    }

    @Test
    void emptyRawChunkMatchesTheKnownVector() {
        assertThat(Cids.of(new byte[0], Cids.RAW)).isEqualTo(EMPTY_RAW);
    }

    @Test
    void emptyDagCborMapMatchesTheKnownVector() {
        assertThat(Cids.of(new byte[] {(byte) 0xa0}, Cids.DAG_CBOR)).isEqualTo(EMPTY_DAG_CBOR_MAP);
    }

    @Test
    void rawCodecAgreesWithContentRoots() {
        byte[] table = new byte[64];
        new Random(42).nextBytes(table);
        assertThat(Cids.of(table, Cids.RAW)).isEqualTo(ContentRoots.rootOf(table));
    }

    @Test
    void ofDigestEqualsOfBytes() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        assertThat(Cids.ofDigest(Digests.sha256(bytes), Cids.DAG_CBOR)).isEqualTo(Cids.of(bytes, Cids.DAG_CBOR));
    }

    @Test
    void parseRoundTripsCodecAndDigest() {
        byte[] bytes = "node".getBytes(StandardCharsets.UTF_8);
        String cid = Cids.of(bytes, Cids.DAG_CBOR);
        Cids.Parsed parsed = Cids.parse(cid);
        assertThat(parsed.codec()).isEqualTo(Cids.DAG_CBOR);
        assertThat(parsed.structured()).isTrue();
        assertThat(parsed.digest()).isEqualTo(Digests.sha256(bytes));
        assertThat(parsed.cid()).isEqualTo(cid);
        assertThat(Cids.parse(EMPTY_RAW).codec()).isEqualTo(Cids.RAW);
        assertThat(Cids.parse(EMPTY_RAW).structured()).isFalse();
    }

    @Test
    void binaryRoundTripsAndCarriesThePrefix() {
        byte[] binary = Cids.binary(EMPTY_RAW);
        assertThat(binary).hasSize(Cids.BINARY_LENGTH);
        assertThat(binary[0]).isEqualTo((byte) 0x01);
        assertThat(binary[1]).isEqualTo((byte) 0x55);
        assertThat(binary[2]).isEqualTo((byte) 0x12);
        assertThat(binary[3]).isEqualTo((byte) 0x20);
        assertThat(Cids.fromBinary(binary)).isEqualTo(EMPTY_RAW);
        byte[] structured = Cids.binary(EMPTY_DAG_CBOR_MAP);
        assertThat(structured[1]).isEqualTo((byte) 0x71);
        assertThat(Cids.fromBinary(structured)).isEqualTo(EMPTY_DAG_CBOR_MAP);
    }

    @Test
    void isCidAcceptsBothCodecsAndRefusesTheRest() {
        assertThat(Cids.isCid(EMPTY_RAW)).isTrue();
        assertThat(Cids.isCid(EMPTY_DAG_CBOR_MAP)).isTrue();
        assertThat(Cids.isCid(null)).isFalse();
        assertThat(Cids.isCid("")).isFalse();
        assertThat(Cids.isCid("b")).isFalse();
        assertThat(Cids.isCid("zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA")).isFalse();
        assertThat(Cids.isCid("not a cid")).isFalse();
    }

    @Test
    void refusesAnUnknownCodecByName() {
        String cid = withBytes(0x01, 0x70, 0x12, 0x20);
        assertThatThrownBy(() -> Cids.parse(cid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown codec 0x70");
        assertThatThrownBy(() -> Cids.of(new byte[0], 0x70))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown codec 0x70");
    }

    @Test
    void refusesAnUnknownMultihashByName() {
        String cid = withBytes(0x01, 0x71, 0x13, 0x20);
        assertThatThrownBy(() -> Cids.parse(cid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown multihash 0x13");
    }

    @Test
    void refusesAnotherCidVersionAndAWrongDigestLength() {
        assertThatThrownBy(() -> Cids.parse(withBytes(0x00, 0x71, 0x12, 0x20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown CID version 0");
        assertThatThrownBy(() -> Cids.parse(withBytes(0x01, 0x71, 0x12, 0x10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
        assertThatThrownBy(() -> Cids.ofDigest(new byte[16], Cids.RAW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("digest of 16 bytes");
    }

    @Test
    void refusesAStringWithoutTheBase32Prefix() {
        assertThatThrownBy(() -> Cids.parse("z" + EMPTY_RAW.substring(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no 'b' prefix");
        assertThatThrownBy(() -> Cids.parse("b!!!"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a base32 CIDv1");
    }

    @Test
    void codecNamesTheTwoCodecs() {
        assertThat(Cids.codecName(Cids.DAG_CBOR)).isEqualTo("dag-cbor");
        assertThat(Cids.codecName(Cids.RAW)).isEqualTo("raw");
        assertThatThrownBy(() -> Cids.codecName(0x00)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aBlobIdentityIsARawCidOverBlake3ThatParseLeavesToItsOwnReader() {
        // PDT 2 phase 4, P2-B-01: Cids names a blob's identity by multihash 0x1e beside the sha2-256 node CIDs
        byte[] digest = new byte[32];
        new Random(3).nextBytes(digest);
        String identity = Cids.identity(digest);
        assertThat(identity).startsWith("bafkr4i");
        assertThat(Cids.isIdentity(identity)).isTrue();
        assertThat(Cids.identityDigest(identity)).isEqualTo(digest);
        assertThat(Cids.isCid(identity)).as("a node CID names sha2-256 (decision 9)").isFalse();
        assertThatThrownBy(() -> Cids.parse(identity)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLAKE3 identity").hasMessageContaining("identityDigest");
        assertThat(Cids.isIdentity(EMPTY_RAW)).as("a sha2-256 CID is no identity").isFalse();
        assertThat(Cids.isIdentity(withBytes(1, Cids.DAG_CBOR, Cids.BLAKE3, 32))).as("an identity names raw bytes").isFalse();
        assertThat(Cids.isIdentity(null)).isFalse();
        assertThat(Cids.isIdentity("bnope")).isFalse();
        String alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        int last = alphabet.indexOf(identity.charAt(identity.length() - 1));
        String padded = identity.substring(0, identity.length() - 1) + alphabet.charAt(last ^ 1);
        assertThat(Cids.isIdentity(padded)).as("a padding bit set: only the canonical text").isFalse();
        assertThatThrownBy(() -> Cids.identityDigest(EMPTY_RAW)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLAKE3 identity");
        assertThatThrownBy(() -> Cids.identity(new byte[31])).isInstanceOf(IllegalArgumentException.class);
    }

    private static String withBytes(int version, int codec, int multihash, int length) {
        byte[] binary = new byte[4 + 32];
        binary[0] = (byte) version;
        binary[1] = (byte) codec;
        binary[2] = (byte) multihash;
        binary[3] = (byte) length;
        return Multibase.base32(binary);
    }
}
