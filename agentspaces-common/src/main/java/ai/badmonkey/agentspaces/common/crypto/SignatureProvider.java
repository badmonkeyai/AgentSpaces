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

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

/**
 * The pluggable Ed25519 implementation seam (PERF1 phase 3). Every signature
 * AgentSpaces creates or checks goes through the {@link Ed25519} facade, which
 * delegates to one process-wide provider; swapping the provider swaps the
 * implementation under all twenty-plus call sites at once, with no API change
 * anywhere else.
 *
 * <p>The contract is RFC 8032 Ed25519, exactly: deterministic 64-byte
 * signatures over the raw message (no prehash), 32-byte raw public keys. The
 * conformance test in {@code agentspaces-common} pins every provider to the
 * project's golden vectors, the same bytes the Python and TypeScript clients
 * verify against, so an implementation that diverges from the JDK's answers
 * cannot pass the build.
 *
 * <p>{@link #verifyRaw} exists because half the verification sites hold a raw
 * 32-byte key rather than a JDK {@link PublicKey}, and native libraries take
 * raw keys directly; forcing a JDK key reconstruction into the SPI would
 * penalize exactly the implementations the SPI exists for.
 *
 * <p>Discovery: providers register through {@link java.util.ServiceLoader}
 * (a {@code META-INF/services} entry for this interface). Selection order is
 * the {@code agentspaces.crypto.signature-provider} system property (or the
 * Spring property of the same name) by {@link #name()}, then a sole discovered
 * provider, then the built-in {@code jdk} provider. {@link Ed25519#use}
 * overrides everything, for tests and programmatic wiring.
 */
public interface SignatureProvider {

    /** Returns the provider's selection name, e.g. {@code "jdk"}. */
    String name();

    /** Generates a fresh Ed25519 keypair. */
    KeyPair generate();

    /**
     * Signs bytes with an Ed25519 private key.
     *
     * @param key   the signing key
     * @param bytes the bytes to sign
     * @return the 64-byte RFC 8032 signature
     */
    byte[] sign(PrivateKey key, byte[] bytes);

    /**
     * Verifies an Ed25519 signature against a JDK public key.
     *
     * @param key   the signer's public key
     * @param bytes the signed bytes
     * @param sig   the signature
     * @return whether the signature is valid
     */
    boolean verify(PublicKey key, byte[] bytes, byte[] sig);

    /**
     * Verifies an Ed25519 signature against a raw 32-byte public key.
     *
     * @param rawPublicKey the signer's raw 32-byte key
     * @param bytes        the signed bytes
     * @param sig          the signature
     * @return whether the signature is valid
     */
    boolean verifyRaw(byte[] rawPublicKey, byte[] bytes, byte[] sig);
}
