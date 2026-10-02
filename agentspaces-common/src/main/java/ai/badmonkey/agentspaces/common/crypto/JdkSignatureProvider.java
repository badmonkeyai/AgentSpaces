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

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
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
        try {
            Signature signature = Signature.getInstance(ALGORITHM);
            signature.initSign(key);
            signature.update(bytes);
            return signature.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 signing failed", e);
        }
    }

    @Override
    public boolean verify(PublicKey key, byte[] bytes, byte[] sig) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(sig, "sig");
        try {
            Signature signature = Signature.getInstance(ALGORITHM);
            signature.initVerify(key);
            signature.update(bytes);
            return signature.verify(sig);
        } catch (GeneralSecurityException e) {
            return false;
        }
    }

    @Override
    public boolean verifyRaw(byte[] rawPublicKey, byte[] bytes, byte[] sig) {
        Objects.requireNonNull(rawPublicKey, "rawPublicKey");
        PublicKey key;
        try {
            key = Ed25519.publicKeyFromRaw(rawPublicKey);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return verify(key, bytes, sig);
    }
}
