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

import ai.badmonkey.agentspaces.common.crypto.Digests;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 5, P2-K-05: rateless IBLT symbols (sum, SipHash-2-4 checksum keyed per session, count; α = 0.5) decode a
 * set difference in one pass as the symbols arrive, and 1,000 differences decode under 2,000 symbols.
 */
class RibltTest {

    private static final byte[] KEY = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f");

    private static byte[] id(String universe, int i) {
        return Digests.sha256((universe + ":" + i).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sipHashMatchesTheReferenceVectorOfItsAuthors() {
        // Aumasson and Bernstein, "SipHash: a fast short-input PRF", appendix A: key 00..0f, message 00..0e
        byte[] message = new byte[15];
        for (int i = 0; i < message.length; i++) {
            message[i] = (byte) i;
        }
        assertThat(Riblt.sipHash24(KEY, message)).isEqualTo(0xa129ca6149be45e5L);
    }

    @Test
    void aThousandScatteredDifferencesAmongAHundredThousandDecodeUnderTwoThousandSymbols() {
        Random random = new Random(42);
        int n = 100_000;
        Set<Integer> clientOnly = new HashSet<>();
        Set<Integer> serverOnly = new HashSet<>();
        while (clientOnly.size() < 500) {
            clientOnly.add(random.nextInt(n));
        }
        while (serverOnly.size() < 500) {
            int i = random.nextInt(n);
            if (!clientOnly.contains(i)) {
                serverOnly.add(i);
            }
        }
        List<byte[]> client = new ArrayList<>();
        List<byte[]> server = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (!serverOnly.contains(i)) {
                client.add(id("k", i));
            }
            if (!clientOnly.contains(i)) {
                server.add(id("k", i));
            }
        }
        Riblt.Encoder encoder = new Riblt.Encoder(KEY, client);
        Riblt.Decoder decoder = new Riblt.Decoder(KEY, server);
        int sent = 0;
        while (!decoder.decoded() && sent < 4_000) {
            for (Riblt.Symbol symbol : encoder.next(100)) {
                decoder.add(symbol);
            }
            sent += 100;
        }
        assertThat(decoder.decoded()).isTrue();
        assertThat(decoder.symbols()).as("P2-K-05: under 2,000 symbols").isLessThan(2_000);
        assertThat(hex(decoder.remoteOnly())).containsExactlyInAnyOrderElementsOf(hex(clientOnly.stream().map(i -> id("k", i)).toList()));
        assertThat(hex(decoder.localOnly())).containsExactlyInAnyOrderElementsOf(hex(serverOnly.stream().map(i -> id("k", i)).toList()));
    }

    @Test
    void equalSetsDecodeAtTheFirstSymbolAndAnEmptyDifferenceListsNothing() {
        List<byte[]> both = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            both.add(id("same", i));
        }
        Riblt.Decoder decoder = new Riblt.Decoder(KEY, both);
        decoder.add(new Riblt.Encoder(KEY, both).next(1).get(0));
        assertThat(decoder.decoded()).isTrue();
        assertThat(decoder.remoteOnly()).isEmpty();
        assertThat(decoder.localOnly()).isEmpty();
    }

    @Test
    void anotherSessionKeyGivesOtherChecksumsAndTheSameDifference() {
        List<byte[]> a = new ArrayList<>();
        List<byte[]> b = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            a.add(id("x", i));
            if (i % 50 != 0) {
                b.add(id("x", i));
            }
        }
        byte[] other = HexFormat.of().parseHex("f0e0d0c0b0a090807060504030201000");
        assertThat(new Riblt.Encoder(other, a).next(1).get(0).checksum())
                .isNotEqualTo(new Riblt.Encoder(KEY, a).next(1).get(0).checksum());
        Riblt.Encoder encoder = new Riblt.Encoder(other, a);
        Riblt.Decoder decoder = new Riblt.Decoder(other, b);
        while (!decoder.decoded()) {
            decoder.add(encoder.next(1).get(0));
        }
        assertThat(decoder.remoteOnly()).hasSize(6);
        assertThat(decoder.localOnly()).isEmpty();
    }

    @Test
    void aSymbolRoundTripsItsWireFormAndAMalformedOneIsRefusedByName() {
        List<byte[]> ids = List.of(id("w", 1), id("w", 2), id("w", 3));
        List<Riblt.Symbol> symbols = new Riblt.Encoder(KEY, ids).next(4);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        symbols.forEach(s -> s.write(out));
        Negentropy.Reader in = new Negentropy.Reader(out.toByteArray());
        for (Riblt.Symbol symbol : symbols) {
            assertThat(Riblt.Symbol.read(in)).isEqualTo(symbol);
        }
        assertThat(in.remaining()).isZero();
        assertThat(symbols.get(0).count()).as("symbol 0 holds every id").isEqualTo(3);
        assertThatThrownBy(() -> Riblt.Symbol.read(new Negentropy.Reader(new byte[39]))).hasMessageContaining("prematurely");
        assertThatThrownBy(() -> new Riblt.Encoder(new byte[8], ids)).hasMessageContaining("16 bytes");
    }

    private static List<String> hex(List<byte[]> ids) {
        return ids.stream().map(HexFormat.of()::formatHex).toList();
    }
}
