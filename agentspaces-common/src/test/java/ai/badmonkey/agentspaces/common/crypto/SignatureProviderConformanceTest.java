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
package ai.badmonkey.agentspaces.common.crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.security.PrivateKey;
import java.security.PublicKey;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins every {@link SignatureProvider} to the project's golden vectors: the
 * fixed key and the exact RFC 8032 signatures the reference implementation
 * produced and the Python and TypeScript clients verify against (spec §11a.4).
 * Ed25519 signatures are deterministic, so a conforming provider must
 * reproduce each signature byte for byte; an implementation that diverges
 * cannot pass this test, which is the safety net under provider swapping.
 */
class SignatureProviderConformanceTest {

    private record Vectors(PrivateKey privateKey, byte[] rawPublicKey, PublicKey publicKey,
                           JsonNode cases) {
    }

    private Vectors load() throws Exception {
        JsonNode root = new ObjectMapper().readTree(
                getClass().getResourceAsStream("signature-vectors.json"));
        HexFormat hex = HexFormat.of();
        byte[] pkcs8 = hex.parseHex(root.get("private_key_pkcs8").asText());
        byte[] raw = hex.parseHex(root.get("public_key_raw").asText());
        return new Vectors(Ed25519.privateKeyFromPkcs8(pkcs8), raw,
                Ed25519.publicKeyFromRaw(raw), root.get("vectors"));
    }

    private void assertConforms(SignatureProvider provider) throws Exception {
        Vectors vectors = load();
        HexFormat hex = HexFormat.of();
        assertThat(vectors.cases()).isNotEmpty();
        for (JsonNode vector : vectors.cases()) {
            byte[] message = hex.parseHex(vector.get("message").asText());
            byte[] expected = hex.parseHex(vector.get("signature").asText());
            String name = vector.get("name").asText();

            assertThat(provider.sign(vectors.privateKey(), message))
                    .as("deterministic signature for vector '%s'", name)
                    .isEqualTo(expected);
            assertThat(provider.verify(vectors.publicKey(), message, expected))
                    .as("verify for vector '%s'", name)
                    .isTrue();
            assertThat(provider.verifyRaw(vectors.rawPublicKey(), message, expected))
                    .as("verifyRaw for vector '%s'", name)
                    .isTrue();

            byte[] tampered = message.clone();
            tampered[0] ^= 0x01;
            assertThat(provider.verify(vectors.publicKey(), tampered, expected)).isFalse();
            assertThat(provider.verifyRaw(vectors.rawPublicKey(), tampered, expected)).isFalse();
        }
        // A generated keypair round-trips through sign, verify, and verifyRaw.
        var pair = provider.generate();
        byte[] sig = provider.sign(pair.getPrivate(), new byte[]{1, 2, 3});
        assertThat(provider.verify(pair.getPublic(), new byte[]{1, 2, 3}, sig)).isTrue();
        assertThat(provider.verifyRaw(Ed25519.rawPublicKey(pair.getPublic()),
                new byte[]{1, 2, 3}, sig)).isTrue();
    }

    @Test
    void theJdkProviderConforms() throws Exception {
        assertConforms(new JdkSignatureProvider());
    }

    @Test
    void theActiveProviderConforms() throws Exception {
        // Whatever selection produced (property, discovery, or the default),
        // the provider actually signing this process's traffic must conform.
        Vectors vectors = load();
        HexFormat hex = HexFormat.of();
        for (JsonNode vector : vectors.cases()) {
            byte[] message = hex.parseHex(vector.get("message").asText());
            byte[] expected = hex.parseHex(vector.get("signature").asText());
            assertThat(Ed25519.sign(vectors.privateKey(), message)).isEqualTo(expected);
            assertThat(Ed25519.verify(vectors.publicKey(), message, expected)).isTrue();
            assertThat(Ed25519.verifyRaw(vectors.rawPublicKey(), message, expected)).isTrue();
        }
        assertThat(Ed25519.providerName()).isNotBlank();
    }
}
