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
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Ed25519 (RFC 8032), the facade every signature in AgentSpaces goes through.
 * Every peer key is an Ed25519 keypair; the PeerID is derived from the raw
 * 32-byte public key (§4.1 of the spec), so this class also converts between
 * the JDK's X.509 SubjectPublicKeyInfo encoding and the raw key.
 *
 * <p>Since PERF1 phase 3 the cryptography itself is pluggable: the facade
 * delegates to one process-wide {@link SignatureProvider}, default
 * {@link JdkSignatureProvider}. Selection at class load: the
 * {@code agentspaces.crypto.signature-provider} system property picks a
 * discovered provider by name, a sole {@link ServiceLoader}-discovered
 * provider wins on its own, and everything else stays on {@code jdk};
 * {@link #use} overrides programmatically. The key-encoding helpers
 * ({@link #rawPublicKey}, {@link #publicKeyFromRaw},
 * {@link #privateKeyFromPkcs8}) are provider-independent codecs and always
 * run on the JDK.
 */
public final class Ed25519 {

    private static final String ALGORITHM = "Ed25519";

    /** The system (and Spring) property naming the provider to select. */
    public static final String PROVIDER_PROPERTY = "agentspaces.crypto.signature-provider";

    private static volatile SignatureProvider provider = loadProvider();

    /**
     * The fixed 12-byte X.509 SubjectPublicKeyInfo prefix for an Ed25519 public key:
     * SEQUENCE(SEQUENCE(OID 1.3.101.112), BIT STRING of the 32 raw bytes).
     */
    private static final byte[] SPKI_PREFIX = {
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00
    };

    /** Length of a raw Ed25519 public key in bytes. */
    public static final int RAW_PUBLIC_KEY_LENGTH = 32;

    private Ed25519() {
    }

    /** Generates a fresh Ed25519 keypair through the active provider. */
    public static KeyPair generate() {
        return provider.generate();
    }

    /**
     * Signs the given bytes with an Ed25519 private key.
     *
     * @param privateKey the signing key
     * @param bytes      the bytes to sign
     * @return the 64-byte signature
     */
    /**
     * Builds the key pair whose private key is exactly {@code seed} (RFC 8032
     * §5.1.5: the 32-byte private key from which the signing scalar and the
     * public key derive). Deterministic: the same seed always yields the same
     * key pair, which is what lets an invite secret derive a signing key on
     * every holder's device without ever transmitting the key itself.
     *
     * <p>The JDK exposes no public API that derives a public key from a
     * private one, so the pair is generated through the platform generator
     * with a source of randomness that hands it the seed; the generator draws
     * exactly the 32 private-key bytes from it. The result is checked: the
     * private key's bytes must equal the seed, or the platform drew
     * differently and the method fails rather than returning a pair the seed
     * does not name.
     *
     * @param seed the 32-byte private key
     * @return the key pair
     * @throws IllegalArgumentException when the seed is not 32 bytes
     * @throws IllegalStateException    when the platform generator does not derive from the seed
     */
    public static KeyPair fromSeed(byte[] seed) {
        if (seed == null || seed.length != RAW_PUBLIC_KEY_LENGTH) {
            throw new IllegalArgumentException("an Ed25519 seed is 32 bytes");
        }
        byte[] copy = seed.clone();
        java.security.SecureRandom seeded = new java.security.SecureRandom() {
            private boolean drawn;

            @Override
            public void nextBytes(byte[] out) {
                if (drawn || out.length != copy.length) {
                    throw new IllegalStateException("the Ed25519 generator drew more than the seed");
                }
                drawn = true;
                System.arraycopy(copy, 0, out, 0, copy.length);
            }
        };
        try {
            java.security.KeyPairGenerator generator = java.security.KeyPairGenerator.getInstance("Ed25519");
            generator.initialize(java.security.spec.NamedParameterSpec.ED25519, seeded);
            KeyPair pair = generator.generateKeyPair();
            byte[] drawn = ((java.security.interfaces.EdECPrivateKey) pair.getPrivate()).getBytes()
                    .orElseThrow(() -> new IllegalStateException("the platform withholds Ed25519 private bytes"));
            if (!java.util.Arrays.equals(drawn, copy)) {
                throw new IllegalStateException("the platform generator did not derive the key from the seed");
            }
            return pair;
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 is unavailable", e);
        } finally {
            java.util.Arrays.fill(copy, (byte) 0);
        }
    }

    public static byte[] sign(PrivateKey privateKey, byte[] bytes) {
        return provider.sign(privateKey, bytes);
    }

    /**
     * Verifies an Ed25519 signature.
     *
     * @param publicKey the signer's public key
     * @param bytes     the signed bytes
     * @param sig       the signature to verify
     * @return {@code true} when the signature is valid for the bytes and key
     */
    public static boolean verify(PublicKey publicKey, byte[] bytes, byte[] sig) {
        return provider.verify(publicKey, bytes, sig);
    }

    /**
     * Verifies an Ed25519 signature against a raw 32-byte public key, the form
     * half the fabric's verification sites hold. Native providers verify raw
     * keys directly with no JDK key object in the path.
     *
     * @param rawPublicKey the signer's raw 32-byte key
     * @param bytes        the signed bytes
     * @param sig          the signature to verify
     * @return {@code true} when the signature is valid for the bytes and key
     */
    public static boolean verifyRaw(byte[] rawPublicKey, byte[] bytes, byte[] sig) {
        return provider.verifyRaw(rawPublicKey, bytes, sig);
    }

    /** Returns the active provider's name, for diagnostics and benchmarks. */
    public static String providerName() {
        return provider.name();
    }

    /**
     * Replaces the process-wide provider. Wiring-time only: call it before the
     * fabric starts, never mid-flight, since in-progress operations may still
     * run on the previous provider.
     *
     * @param replacement the provider to use
     */
    public static void use(SignatureProvider replacement) {
        provider = Objects.requireNonNull(replacement, "replacement");
    }

    /**
     * Selects the provider by name: {@code "jdk"}, or the {@link ServiceLoader}
     * name of a provider on the classpath. This is what the Spring property
     * {@code agentspaces.crypto.signature-provider} calls.
     *
     * @param name the provider name
     * @throws IllegalArgumentException when no provider carries the name
     */
    public static void select(String name) {
        Objects.requireNonNull(name, "name");
        use(choose(name, discovered(), new JdkSignatureProvider()));
    }

    private static SignatureProvider loadProvider() {
        String wanted = System.getProperty(PROVIDER_PROPERTY);
        return choose(wanted, discovered(), new JdkSignatureProvider());
    }

    private static List<SignatureProvider> discovered() {
        List<SignatureProvider> providers = new ArrayList<>();
        for (SignatureProvider candidate : ServiceLoader.load(SignatureProvider.class)) {
            providers.add(candidate);
        }
        return providers;
    }

    /**
     * The selection rule, pure for testability: an explicit name must match a
     * discovered provider or {@code jdk} (anything else throws, because
     * silently signing with the wrong implementation is worse than failing);
     * with no name, a sole discovered provider wins, and zero or several
     * discovered providers fall back to {@code jdk}.
     */
    static SignatureProvider choose(String wanted, List<SignatureProvider> discovered,
                                    SignatureProvider jdk) {
        if (wanted != null && !wanted.isBlank()) {
            String name = wanted.trim();
            if (jdk.name().equals(name)) {
                return jdk;
            }
            for (SignatureProvider candidate : discovered) {
                if (name.equals(candidate.name())) {
                    return candidate;
                }
            }
            throw new IllegalArgumentException("no SignatureProvider named '" + name
                    + "' on the classpath; discovered: "
                    + discovered.stream().map(SignatureProvider::name).toList());
        }
        return discovered.size() == 1 ? discovered.get(0) : jdk;
    }

    /**
     * Extracts the raw 32-byte public key from a JDK Ed25519 {@link PublicKey}.
     *
     * @param publicKey an Ed25519 public key
     * @return the raw 32-byte key
     * @throws IllegalArgumentException if the key is not an Ed25519 SPKI encoding
     */
    public static byte[] rawPublicKey(PublicKey publicKey) {
        Objects.requireNonNull(publicKey, "publicKey");
        byte[] encoded = publicKey.getEncoded();
        if (encoded == null
                || encoded.length != SPKI_PREFIX.length + RAW_PUBLIC_KEY_LENGTH
                || !Arrays.equals(encoded, 0, SPKI_PREFIX.length, SPKI_PREFIX, 0, SPKI_PREFIX.length)) {
            throw new IllegalArgumentException("not an Ed25519 X.509-encoded public key");
        }
        return Arrays.copyOfRange(encoded, SPKI_PREFIX.length, encoded.length);
    }

    /**
     * Reconstructs a JDK {@link PublicKey} from a raw 32-byte Ed25519 public key.
     *
     * @param raw the raw 32-byte key
     * @return the JDK public key
     * @throws IllegalArgumentException if the input is not 32 bytes or is invalid
     */
    public static PublicKey publicKeyFromRaw(byte[] raw) {
        Objects.requireNonNull(raw, "raw");
        if (raw.length != RAW_PUBLIC_KEY_LENGTH) {
            throw new IllegalArgumentException("raw Ed25519 public key must be 32 bytes, got " + raw.length);
        }
        byte[] spki = new byte[SPKI_PREFIX.length + raw.length];
        System.arraycopy(SPKI_PREFIX, 0, spki, 0, SPKI_PREFIX.length);
        System.arraycopy(raw, 0, spki, SPKI_PREFIX.length, raw.length);
        try {
            return KeyFactory.getInstance(ALGORITHM).generatePublic(new X509EncodedKeySpec(spki));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("invalid raw Ed25519 public key", e);
        }
    }

    /**
     * Decodes a PKCS#8-encoded Ed25519 private key.
     *
     * @param pkcs8 the PKCS#8 encoding, as produced by {@link PrivateKey#getEncoded()}
     * @return the JDK private key
     * @throws IllegalArgumentException if the encoding is invalid
     */
    public static PrivateKey privateKeyFromPkcs8(byte[] pkcs8) {
        Objects.requireNonNull(pkcs8, "pkcs8");
        try {
            return KeyFactory.getInstance(ALGORITHM).generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("invalid PKCS#8 Ed25519 private key", e);
        }
    }
}
