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

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The default {@link SignatureProvider}: the JDK's built-in EdDSA
 * implementation, exactly the code every AgentSpaces version has run so far.
 * Pure Java, no dependencies, correct everywhere the JDK runs; deployments
 * that need more signing headroom drop a native provider on the classpath
 * instead of changing anything here.
 */
public final class JdkSignatureProvider implements SignatureProvider {

    private static final String ALGORITHM = "Ed25519";

    @Override
    public String name() {
        return "jdk";
    }

    @Override
    public KeyPair generate() {
        try {
            return KeyPairGenerator.getInstance(ALGORITHM).generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 unavailable in this JVM", e);
        }
    }

    @Override
    public byte[] sign(PrivateKey key, byte[] bytes) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(bytes, "bytes");
        Signature signature = signers.borrow(key);
        try {
            if (signature == null) {
                signature = Signature.getInstance(ALGORITHM);
                signature.initSign(key);
            }
            signature.update(bytes);
            byte[] out = signature.sign();
            signers.giveBack(key, signature); // sign() leaves it initialized for this key again
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 signing failed", e);
        }
    }

    @Override
    public boolean verify(PublicKey key, byte[] bytes, byte[] sig) {
        Objects.requireNonNull(key, "key");
        return verifyWith(key, key, bytes, sig);
    }

    @Override
    public boolean verifyRaw(byte[] rawPublicKey, byte[] bytes, byte[] sig) {
        Objects.requireNonNull(rawPublicKey, "rawPublicKey");
        ByteBuffer raw = ByteBuffer.wrap(rawPublicKey.clone());
        Signature pooled = verifiers.borrow(raw);
        if (pooled != null) {
            return verifyOn(raw, pooled, bytes, sig);
        }
        PublicKey key;
        try {
            key = Ed25519.publicKeyFromRaw(rawPublicKey);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return verifyWith(raw, key, bytes, sig);
    }

    private boolean verifyWith(Object poolKey, PublicKey key, byte[] bytes, byte[] sig) {
        Signature signature = verifiers.borrow(poolKey);
        if (signature == null) {
            try {
                signature = Signature.getInstance(ALGORITHM);
                signature.initVerify(key);
            } catch (GeneralSecurityException e) {
                return false;
            }
        }
        return verifyOn(poolKey, signature, bytes, sig);
    }

    /** Verifies on an initialized instance; it goes back to the pool only when verify returned normally. */
    private boolean verifyOn(Object poolKey, Signature signature, byte[] bytes, byte[] sig) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(sig, "sig");
        try {
            signature.update(bytes);
            boolean valid = signature.verify(sig);
            verifiers.giveBack(poolKey, signature); // verify() leaves it initialized for this key again
            return valid;
        } catch (GeneralSecurityException e) {
            return false; // its state is unspecified after a throw, so it is dropped
        }
    }

    /**
     * Initialized {@link Signature} instances by key (idle-loop O1, reviews/IDLE-LOOP-2026-11-18.md). Initializing for
     * verification decodes the key's point, a modular square root, and initializing for signing expands the private
     * key; both used to run for every frame. A JDK signature returns to its initialized state after {@code verify} or
     * {@code sign}, so an instance serves its key again. The pool keeps a few instances per key for concurrent callers
     * and at most {@code keys} keys, the least recently used leaving first; a pool miss costs exactly what every call
     * used to cost.
     */
    static final class Pool {
        private final int keys;
        private final int perKey;
        private final LinkedHashMap<Object, ArrayDeque<Signature>> byKey;

        Pool(int keys, int perKey) {
            this.keys = keys;
            this.perKey = perKey;
            this.byKey = new LinkedHashMap<>(64, 0.75f, true);
        }

        synchronized Signature borrow(Object key) {
            ArrayDeque<Signature> free = byKey.get(key);
            return free == null ? null : free.pollFirst();
        }

        synchronized void giveBack(Object key, Signature signature) {
            ArrayDeque<Signature> free = byKey.computeIfAbsent(key, k -> new ArrayDeque<>(perKey));
            if (free.size() < perKey) {
                free.addFirst(signature);
            }
            if (byKey.size() > keys) {
                Iterator<Map.Entry<Object, ArrayDeque<Signature>>> eldest = byKey.entrySet().iterator();
                eldest.next();
                eldest.remove();
            }
        }

        synchronized int keyCount() {
            return byKey.size();
        }
    }

    /** Keys of peers this process verifies; a fleet's members and the issuers of what it holds. */
    private final Pool verifiers = new Pool(1024, 4);
    /** Keys this process signs with: its own identity and a few derived ones. */
    private final Pool signers = new Pool(64, 4);

    /** How many keys hold pooled verifiers, for tests. */
    int pooledVerifierKeys() {
        return verifiers.keyCount();
    }
}
