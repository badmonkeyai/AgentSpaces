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
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class HoldingTest {

    private final PeerId holder = PeerId.of("zHolder");

    @Test
    void bitfieldRoundTripsThroughRunLengths() {
        Random random = new Random(5);
        for (int pieces : new int[]{1, 7, 64, 1_000, 10_240}) {
            BitSet have = new BitSet(pieces);
            for (int i = 0; i < pieces; i++) {
                if (random.nextInt(4) != 0) {
                    have.set(i);
                }
            }
            have.clear(pieces - 1); // keep it partial whatever the dice said
            Holding holding = Holding.of("broot", holder, have, pieces, null);
            assertThat(holding.count()).isEqualTo(have.cardinality());
            assertThat(holding.complete()).isFalse();
            assertThat(holding.decodeHave(pieces)).isEqualTo(have);
        }
    }

    @Test
    void longRunsStaySmallAndACompleteHoldingCarriesNoBitfield() {
        BitSet have = new BitSet();
        have.set(0, 5_000);
        have.set(7_000, 10_000);
        Holding partial = Holding.of("broot", holder, have, 10_240, null);
        assertThat(partial.have()).hasSizeLessThan(16);
        assertThat(partial.decodeHave(10_240)).isEqualTo(have);

        BitSet all = new BitSet();
        all.set(0, 10_240);
        Holding complete = Holding.of("broot", holder, all, 10_240, List.of(PeerId.of("zRelay")));
        assertThat(complete.complete()).isTrue();
        assertThat(complete.have()).isNull();
        assertThat(complete.decodeHave(10_240).cardinality()).isEqualTo(10_240);
        assertThat(complete.tags()).containsEntry(Holding.ROOT_TAG, "broot").containsEntry(Holding.HOLDER_TAG, "zHolder");
    }

    @Test
    void recordsRoundTripThroughTheCodec() {
        CborCodec codec = CborCodec.defaultCodec();
        BitSet have = new BitSet();
        have.set(1);
        have.set(3);
        Holding holding = Holding.of("broot", holder, have, 4, List.of(PeerId.of("zRelay")));
        Holding back = codec.fromBytes(codec.toBytes(holding), Holding.class);
        assertThat(back.root()).isEqualTo("broot");
        assertThat(back.holder()).isEqualTo(holder);
        assertThat(back.decodeHave(4)).isEqualTo(have);
        assertThat(back.via()).containsExactly(PeerId.of("zRelay"));

        Manifest manifest = new Manifest(1, "broot", 3 * 65_536 + 1, 65_536, 4, new byte[4 * 32], null, null,
                "m.bin", "application/octet-stream", Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN,
                holder, 1_000L, Map.of("run", "42"));
        assertThat(manifest.violation()).isEmpty();
        assertThat(manifest.pieceLength(3)).isEqualTo(1);
        assertThat(manifest.tags()).containsEntry(Manifest.NAME_TAG, "m.bin");
        Manifest backM = codec.fromBytes(codec.toBytes(manifest), Manifest.class);
        assertThat(backM).isEqualTo(manifest.withTableCopy());
        assertThat(new Manifest(1, "broot", 100, 65_536, 1, new byte[64], null, null, null, null,
                Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, holder, 0, null).violation())
                .contains("table of 64 bytes for 1 pieces");
        assertThat(new Manifest(1, "broot", 100, 65_536, 1, null, null, null, null, null,
                Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, holder, 0, null).violation())
                .contains("exactly one of table and tableRef must be present");
    }
}
