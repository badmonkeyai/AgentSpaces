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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class MessagesTest {

    private final CborCodec codec = CborCodec.defaultCodec();

    @Test
    void everyMessageCarriesItsTagFirstAndRoundTrips() {
        byte[] bytes = new byte[300];
        new Random(1).nextBytes(bytes);
        Object[] messages = {
                new Messages.PieceWant("broot", 3, 65_536, 65_536, 42),
                new Messages.Piece("broot", 3, 65_536, bytes, 42),
                new Messages.Have("broot", new byte[]{1, 2}, false),
                new Messages.Cancel("broot", 3, 42),
                new Messages.Reject("broot", 3, Messages.REASON_OVER_BUDGET),
                new Messages.ManifestWant("broot"),
                new Messages.ManifestReply("broot", bytes),
        };
        int[] tags = {1, 2, 3, 4, 5, 6, 7};
        for (int i = 0; i < messages.length; i++) {
            byte[] encoded = codec.toBytes(messages[i]);
            assertThat(Messages.tagOf(codec, encoded)).contains(tags[i]);
            assertThat(encoded[1]).as("the first key is 't'").isEqualTo((byte) 0x61);
            assertThat(encoded[2]).isEqualTo((byte) 't');
            Object back = Messages.decode(codec, encoded, messages[i].getClass()).orElseThrow();
            assertThat(codec.toBytes(back)).isEqualTo(encoded);
        }
        assertThat(Messages.tagOf(codec, new byte[]{(byte) 0xf6})).isEmpty();
        assertThat(Messages.tagOf(codec, new byte[]{0x01, 0x02})).isEmpty();
    }

    @Test
    void rootsAreCidV1AndPiecesVerifyAgainstTheTable() {
        byte[] artifact = new byte[3 * 65_536 + 10];
        new Random(2).nextBytes(artifact);
        ContentRoots.Hashed hashed = ContentRoots.hash(artifact, 65_536);
        assertThat(hashed.size()).isEqualTo(artifact.length);
        assertThat(hashed.pieces(65_536)).isEqualTo(4);
        assertThat(hashed.table()).hasSize(4 * 32);
        assertThat(hashed.root()).startsWith("bafkrei").hasSize(59);
        assertThat(ContentRoots.isRoot(hashed.root())).isTrue();
        assertThat(ContentRoots.isRoot("zLegacyBlock")).isFalse();
        byte[] last = java.util.Arrays.copyOfRange(artifact, 3 * 65_536, artifact.length);
        assertThat(ContentRoots.verifies(hashed.table(), 3, last)).isTrue();
        last[0] ^= 1;
        assertThat(ContentRoots.verifies(hashed.table(), 3, last)).isFalse();
        assertThat(ContentRoots.verifies(hashed.table(), 4, last)).isFalse();
        assertThat(ContentRoots.rootOf(hashed.table())).isEqualTo(hashed.root());
    }
}
