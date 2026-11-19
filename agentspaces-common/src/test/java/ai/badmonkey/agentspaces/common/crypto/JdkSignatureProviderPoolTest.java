/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.common.crypto;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The provider reuses initialized signatures per key (idle-loop O1). A reused instance must answer exactly as a fresh
 * one would: after a valid signature, an invalid one, a malformed one, and under concurrent callers.
 */
class JdkSignatureProviderPoolTest {

    private final JdkSignatureProvider provider = new JdkSignatureProvider();
    private final KeyPair pair = provider.generate();
    private final byte[] raw = Ed25519.rawPublicKey(pair.getPublic());

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aReusedVerifierAnswersEveryCaseAsAFreshOneWould() {
        byte[] good = provider.sign(pair.getPrivate(), bytes("hello"));
        byte[] other = provider.sign(pair.getPrivate(), bytes("other"));
        for (int round = 0; round < 3; round++) {
            assertThat(provider.verifyRaw(raw, bytes("hello"), good)).isTrue();
            assertThat(provider.verifyRaw(raw, bytes("hellp"), good)).as("tampered bytes").isFalse();
            assertThat(provider.verifyRaw(raw, bytes("hello"), other)).as("another message's signature").isFalse();
            assertThat(provider.verifyRaw(raw, bytes("hello"), new byte[7])).as("malformed signature").isFalse();
            assertThat(provider.verify(pair.getPublic(), bytes("hello"), good)).isTrue();
            assertThat(provider.verify(pair.getPublic(), bytes("hellp"), good)).isFalse();
        }
        assertThat(provider.verifyRaw(new byte[31], bytes("hello"), good)).as("a raw key of the wrong length").isFalse();
    }

    @Test
    void aReusedSignerSignsAsAFreshOneDoes() {
        byte[] first = provider.sign(pair.getPrivate(), bytes("same"));
        byte[] again = provider.sign(pair.getPrivate(), bytes("same"));
        byte[] fresh = new JdkSignatureProvider().sign(pair.getPrivate(), bytes("same"));
        assertThat(again).isEqualTo(first).isEqualTo(fresh); // Ed25519 is deterministic
        assertThat(provider.verifyRaw(raw, bytes("same"), again)).isTrue();
    }

    @Test
    void concurrentCallersShareTheKeysWithoutCrossTalk() throws Exception {
        List<KeyPair> pairs = new ArrayList<>();
        List<byte[]> sigs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            KeyPair p = provider.generate();
            pairs.add(p);
            sigs.add(provider.sign(p.getPrivate(), bytes("m" + i)));
        }
        AtomicInteger wrong = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                int seed = t;
                done.add(pool.submit(() -> {
                    for (int n = 0; n < 500; n++) {
                        int i = (seed + n) % 4;
                        byte[] key = Ed25519.rawPublicKey(pairs.get(i).getPublic());
                        boolean valid = provider.verifyRaw(key, bytes("m" + i), sigs.get(i));
                        boolean forged = provider.verifyRaw(key, bytes("m" + ((i + 1) % 4)), sigs.get(i));
                        byte[] signed = provider.sign(pairs.get(i).getPrivate(), bytes("s" + n));
                        if (!valid || forged || !provider.verifyRaw(key, bytes("s" + n), signed)) {
                            wrong.incrementAndGet();
                        }
                    }
                }));
            }
            for (Future<?> f : done) {
                f.get();
            }
        } finally {
            pool.shutdown();
        }
        assertThat(wrong.get()).isZero();
    }

    @Test
    void thePoolHoldsABoundedNumberOfKeys() {
        byte[] sig = provider.sign(pair.getPrivate(), bytes("x"));
        for (int i = 0; i < 1500; i++) {
            provider.verifyRaw(Ed25519.rawPublicKey(provider.generate().getPublic()), bytes("x"), sig);
        }
        assertThat(provider.pooledVerifierKeys()).isLessThanOrEqualTo(1024);
        assertThat(provider.verifyRaw(raw, bytes("x"), sig)).isTrue();
    }
}
