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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The provider seam itself: the selection rule, the programmatic override, and
 * facade delegation. The fake provider stays out of {@code META-INF/services}
 * deliberately, so discovery never leaks into other tests in this JVM; the
 * selection rule is a pure function and is tested as one.
 */
class Ed25519ProviderSelectionTest {

    /** Counts calls and otherwise behaves exactly like the JDK provider. */
    private static final class CountingProvider implements SignatureProvider {
        private final JdkSignatureProvider delegate = new JdkSignatureProvider();
        private final String name;
        final AtomicInteger signs = new AtomicInteger();
        final AtomicInteger verifies = new AtomicInteger();

        private CountingProvider(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public KeyPair generate() {
            return delegate.generate();
        }

        @Override
        public byte[] sign(PrivateKey key, byte[] bytes) {
            signs.incrementAndGet();
            return delegate.sign(key, bytes);
        }

        @Override
        public boolean verify(PublicKey key, byte[] bytes, byte[] sig) {
            verifies.incrementAndGet();
            return delegate.verify(key, bytes, sig);
        }

        @Override
        public boolean verifyRaw(byte[] rawPublicKey, byte[] bytes, byte[] sig) {
            verifies.incrementAndGet();
            return delegate.verifyRaw(rawPublicKey, bytes, sig);
        }
    }

    @AfterEach
    void restoreJdkProvider() {
        Ed25519.select("jdk");
    }

    @Test
    void theChoiceRulePrefersExplicitNameThenSoleDiscoveryThenJdk() {
        JdkSignatureProvider jdk = new JdkSignatureProvider();
        CountingProvider sodium = new CountingProvider("sodium");
        CountingProvider other = new CountingProvider("other");

        // Explicit name wins, and "jdk" always resolves.
        assertThat(Ed25519.choose("sodium", List.of(sodium, other), jdk)).isSameAs(sodium);
        assertThat(Ed25519.choose("jdk", List.of(sodium, other), jdk)).isSameAs(jdk);
        // A sole discovered provider activates itself.
        assertThat(Ed25519.choose(null, List.of(sodium), jdk)).isSameAs(sodium);
        // Nothing discovered, or an ambiguous field, stays on the JDK.
        assertThat(Ed25519.choose(null, List.of(), jdk)).isSameAs(jdk);
        assertThat(Ed25519.choose("", List.of(sodium, other), jdk)).isSameAs(jdk);
        // An unknown explicit name fails loudly rather than silently signing
        // with the wrong implementation.
        assertThatThrownBy(() -> Ed25519.choose("missing", List.of(sodium), jdk))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void useSwapsTheFacadeAndSelectJdkRestoresIt() {
        CountingProvider counting = new CountingProvider("counting");
        KeyPair pair = Ed25519.generate();

        Ed25519.use(counting);
        assertThat(Ed25519.providerName()).isEqualTo("counting");
        byte[] sig = Ed25519.sign(pair.getPrivate(), new byte[]{7});
        assertThat(Ed25519.verify(pair.getPublic(), new byte[]{7}, sig)).isTrue();
        assertThat(Ed25519.verifyRaw(Ed25519.rawPublicKey(pair.getPublic()),
                new byte[]{7}, sig)).isTrue();
        assertThat(counting.signs.get()).isEqualTo(1);
        assertThat(counting.verifies.get()).isEqualTo(2);

        Ed25519.select("jdk");
        assertThat(Ed25519.providerName()).isEqualTo("jdk");
        Ed25519.sign(pair.getPrivate(), new byte[]{8});
        assertThat(counting.signs.get()).isEqualTo(1); // untouched after restore
    }

    @Test
    void selectRejectsUnknownNames() {
        assertThatThrownBy(() -> Ed25519.select("no-such-provider"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Ed25519.providerName()).isEqualTo("jdk"); // unchanged on failure
    }
}
