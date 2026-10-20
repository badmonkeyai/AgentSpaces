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

package ai.badmonkey.agentspaces.peering.bootstrap;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

/** The sealed beacon datagram (SPEC §10.1 v0.1.19, SRAD dtls-support DT-M-01, DT-M-02, DT-Q-SEC-12). */
class BeaconSealTest {

    private static final byte[] KEY = new byte[32];
    private static final byte[] OTHER_KEY = new byte[32];

    static {
        Arrays.fill(KEY, (byte) 7);
        Arrays.fill(OTHER_KEY, (byte) 9);
    }

    @Test
    void hkdfMatchesRfc5869TestCaseThree() {
        byte[] ikm = new byte[22];
        Arrays.fill(ikm, (byte) 0x0b);
        assertThat(HexFormat.of().formatHex(BeaconSeal.hkdfSha256(ikm, new byte[0], new byte[0], 42)))
                .isEqualTo("8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8");
    }

    @Test
    void aSealOpensWithTheSameKeyInThisOrTheLastEpochOnly() {
        BeaconSeal seal = new BeaconSeal(KEY);
        byte[] ad = "a signed self-advertisement".getBytes();
        Instant now = Instant.parse("2026-10-18T12:00:00Z");
        byte[] sealed = seal.seal(ad, now);
        assertThat(BeaconSeal.isSealed(sealed)).isTrue();
        assertThat(sealed).hasSize(ad.length + BeaconSeal.OVERHEAD);
        assertThat(seal.open(sealed, now)).contains(ad);
        assertThat(new BeaconSeal(KEY).open(sealed, now.plusSeconds(BeaconSeal.EPOCH_SECONDS))).contains(ad);
        assertThat(seal.open(sealed, now.plusSeconds(2 * BeaconSeal.EPOCH_SECONDS))).isEmpty();
        assertThat(new BeaconSeal(OTHER_KEY).open(sealed, now)).isEmpty();
        byte[] tampered = sealed.clone();
        tampered[tampered.length - 1] ^= 1;
        assertThat(seal.open(tampered, now)).isEmpty();
        assertThat(BeaconSeal.isSealed(ad)).isFalse();
    }

    @Test
    void theTagRotatesAndRevealsNothingOfTheAdvertisement() {
        BeaconSeal seal = new BeaconSeal(KEY);
        Instant now = Instant.parse("2026-10-18T12:00:00Z");
        byte[] ad = "aspace://zGroupIdThatMustNotLeak".getBytes();
        byte[] first = seal.seal(ad, now);
        byte[] later = seal.seal(ad, now.plusSeconds(BeaconSeal.EPOCH_SECONDS));
        assertThat(Arrays.copyOfRange(first, 4, 20)).isNotEqualTo(Arrays.copyOfRange(later, 4, 20));
        assertThat(new String(first)).doesNotContain("zGroupIdThatMustNotLeak");
        assertThat(seal.seal(ad, now)).as("a fresh nonce each time").isNotEqualTo(first);
    }

    /** SRAD dtls-support DT-M-04: the golden sealed datagram, made by an independent implementation, opens. */
    @Test
    void theGoldenSealedDatagramOpens() throws Exception {
        String json = java.nio.file.Files.readString(java.nio.file.Path.of("..", "tools", "golden", "golden.json"));
        HexFormat hex = HexFormat.of();
        BeaconSeal seal = new BeaconSeal(hex.parseHex(field(json, "beacon_seal_content_key")));
        Instant at = Instant.ofEpochSecond(Long.parseLong(field(json, "beacon_seal_epoch_second")));
        assertThat(hex.formatHex(seal.tag(BeaconSeal.epoch(at)))).isEqualTo(field(json, "beacon_seal_tag"));
        assertThat(seal.open(hex.parseHex(field(json, "beacon_seal_datagram")), at))
                .hasValueSatisfying(opened -> assertThat(hex.formatHex(opened))
                        .isEqualTo(field(json, "beacon_seal_plaintext")));
    }

    private static String field(String json, String name) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"" + name + "\": \"([0-9a-f]+)\"").matcher(json);
        assertThat(m.find()).as(name).isTrue();
        return m.group(1);
    }
}
