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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The memory of verified signatures (swap amplification, ROBUSTNESS.md §10.8): a repeat of the exact key, signature
 * and bytes costs no second verification, and nothing that was never verified passes. The adversarial cases are the
 * gate's list: the same signature over other bytes, the same bytes and signature under another key, an invalid
 * signature offered twice, a flood that pushes honest verifications out, and verifiers racing on one memory.
 */
class VerifiedSignaturesTest {

    /** Counts the verifications that reach the provider. */
    private static final class CountingProvider implements SignatureProvider {
        private final JdkSignatureProvider delegate = new JdkSignatureProvider();
        final AtomicInteger verifies = new AtomicInteger();

        @Override
        public String name() {
            return "counting";
        }

        @Override
        public KeyPair generate() {
            return delegate.generate();
        }

        @Override
        public byte[] sign(PrivateKey key, byte[] bytes) {
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

    private final CountingProvider counting = new CountingProvider();
    private final KeyPair alice = Ed25519.generate();
    private final KeyPair mallory = Ed25519.generate();

    @BeforeEach
    void count() {
        Ed25519.use(counting);
    }

    @AfterEach
    void restore() {
        Ed25519.select("jdk");
    }

    private static byte[] raw(KeyPair pair) {
        return Ed25519.rawPublicKey(pair.getPublic());
    }

    @Test
    void aRepeatOfTheSameKeySignatureAndBytesIsAnsweredWithoutASecondVerification() {
        VerifiedSignatures memory = new VerifiedSignatures(16);
        byte[] bytes = "state".getBytes();
        byte[] sig = Ed25519.sign(alice.getPrivate(), bytes);

        assertThat(memory.verify(raw(alice), bytes, sig)).isTrue();
        assertThat(memory.verify(raw(alice), bytes.clone(), sig.clone())).isTrue();
        assertThat(memory.verify(raw(alice), bytes, sig)).isTrue();

        assertThat(counting.verifies.get()).isEqualTo(1);
        assertThat(memory.verified()).isEqualTo(1);
        assertThat(memory.hits()).isEqualTo(2);
    }

    @Test
    void theSameSignatureOverOtherBytesIsVerifiedAndRefused() {
        VerifiedSignatures memory = new VerifiedSignatures(16);
        byte[] bytes = "lease until 10:00".getBytes();
        byte[] sig = Ed25519.sign(alice.getPrivate(), bytes);
        assertThat(memory.verify(raw(alice), bytes, sig)).isTrue();

        byte[] forged = "lease until 23:00".getBytes();
        assertThat(memory.verify(raw(alice), forged, sig)).isFalse();
        byte[] longer = java.util.Arrays.copyOf(bytes, bytes.length + 1);
        assertThat(memory.verify(raw(alice), longer, sig)).isFalse();
        assertThat(memory.verify(raw(alice), new byte[0], sig)).isFalse();

        assertThat(counting.verifies.get()).isEqualTo(4);
        assertThat(memory.hits()).isZero();
    }

    @Test
    void theSameBytesAndSignatureUnderAnotherKeyAreVerifiedAndRefused() {
        VerifiedSignatures memory = new VerifiedSignatures(16);
        byte[] bytes = "record".getBytes();
        byte[] sig = Ed25519.sign(alice.getPrivate(), bytes);
        assertThat(memory.verify(raw(alice), bytes, sig)).isTrue();

        assertThat(memory.verify(raw(mallory), bytes, sig)).isFalse();
        byte[] flipped = raw(alice);
        flipped[0] ^= 1;
        assertThat(memory.verify(flipped, bytes, sig)).isFalse();

        assertThat(counting.verifies.get()).isEqualTo(3);
        assertThat(memory.hits()).isZero();
    }

    @Test
    void anInvalidSignatureIsNeverRememberedSoEveryOfferIsVerifiedAgain() {
        VerifiedSignatures memory = new VerifiedSignatures(16);
        byte[] bytes = "claim".getBytes();
        byte[] sig = Ed25519.sign(mallory.getPrivate(), bytes);

        for (int i = 0; i < 3; i++) {
            assertThat(memory.verify(raw(alice), bytes, sig)).isFalse();
        }
        byte[] bent = Ed25519.sign(alice.getPrivate(), bytes);
        bent[10] ^= 0x40;
        for (int i = 0; i < 3; i++) {
            assertThat(memory.verify(raw(alice), bytes, bent)).isFalse();
        }

        assertThat(counting.verifies.get()).isEqualTo(6);
        assertThat(memory.size()).isZero();
        assertThat(memory.hits()).isZero();
    }

    @Test
    void aFloodOfValidSignaturesPushesHonestOnesOutButNeverPassesAForgery() {
        VerifiedSignatures memory = new VerifiedSignatures(4);
        byte[] honest = "honest".getBytes();
        byte[] honestSig = Ed25519.sign(alice.getPrivate(), honest);
        assertThat(memory.verify(raw(alice), honest, honestSig)).isTrue();

        for (int i = 0; i < 50; i++) {
            byte[] junk = ("flood " + i).getBytes();
            assertThat(memory.verify(raw(mallory), junk, Ed25519.sign(mallory.getPrivate(), junk))).isTrue();
            assertThat(memory.size()).isLessThanOrEqualTo(4);
        }
        int before = counting.verifies.get();
        assertThat(memory.verify(raw(alice), honest, honestSig)).isTrue();
        assertThat(counting.verifies.get()).as("the honest signature left and is verified again").isEqualTo(before + 1);

        // the flooder's own remembered signatures cover only their own bytes
        byte[] last = "flood 49".getBytes();
        byte[] lastSig = Ed25519.sign(mallory.getPrivate(), last);
        assertThat(memory.verify(raw(mallory), "flood 50".getBytes(), lastSig)).isFalse();
        assertThat(memory.verify(raw(alice), last, lastSig)).isFalse();
        assertThat(memory.size()).isEqualTo(4);
    }

    @Test
    void keysAndSignaturesOfTheWrongLengthAreRefusedAndNeverRemembered() {
        VerifiedSignatures memory = new VerifiedSignatures(16);
        byte[] bytes = "short".getBytes();
        byte[] sig = Ed25519.sign(alice.getPrivate(), bytes);

        assertThat(memory.verify(java.util.Arrays.copyOf(raw(alice), 31), bytes, sig)).isFalse();
        assertThat(memory.verify(java.util.Arrays.copyOf(raw(alice), 33), bytes, sig)).isFalse();
        assertThat(memory.verify(raw(alice), bytes, java.util.Arrays.copyOf(sig, 63))).isFalse();
        assertThat(memory.verify(raw(alice), bytes, java.util.Arrays.copyOf(sig, 65))).isFalse();
        assertThat(memory.verify(raw(alice), bytes, new byte[0])).isFalse();
        assertThat(memory.size()).isZero();

        assertThatThrownBy(() -> memory.verify(null, bytes, sig)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> memory.verify(raw(alice), null, sig)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> memory.verify(raw(alice), bytes, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new VerifiedSignatures(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void verifiersRacingOnOneMemoryNeverPassAForgery() throws Exception {
        VerifiedSignatures memory = new VerifiedSignatures(8);
        List<byte[]> bytes = new ArrayList<>();
        List<byte[]> sigs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            byte[] b = ("item " + i).getBytes();
            bytes.add(b);
            sigs.add(Ed25519.sign(alice.getPrivate(), b));
        }
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger wrong = new AtomicInteger();
        List<Thread> running = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int offset = t;
            running.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    for (int round = 0; round < 200; round++) {
                        int i = (round + offset) % bytes.size();
                        int j = (i + 1) % bytes.size();
                        if (!memory.verify(raw(alice), bytes.get(i), sigs.get(i))) {
                            wrong.incrementAndGet(); // a valid signature refused
                        }
                        if (memory.verify(raw(alice), bytes.get(j), sigs.get(i))) {
                            wrong.incrementAndGet(); // another item's signature passed
                        }
                        if (memory.verify(raw(mallory), bytes.get(i), sigs.get(i))) {
                            wrong.incrementAndGet(); // another key passed
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    wrong.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Thread t : running) {
            t.join();
        }
        assertThat(wrong.get()).isZero();
        assertThat(memory.size()).isLessThanOrEqualTo(8);
        assertThat(memory.hits()).isPositive();
    }
}
