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
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.api.error.AgentSpacesException;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.X25519;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Key-file mechanics shared by the peer and agent keystores (TODO-9-10-11 B5):
 * private keys as PKCS#8 DER, public keys raw; every file written to a
 * same-directory temp file, flushed, and atomically renamed into place, private
 * files created owner-only (0600) so no window exists in which another local
 * user can read them; and every loaded pair proven by a sign/verify (or key
 * agreement) probe, so a mismatched or half-written pair is refused with a
 * message naming the files rather than used.
 */
public final class KeyFiles {

    /** Agent and file names: no separators, no traversal, bounded. */
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]{1,64}");
    private static final SecureRandom RANDOM = new SecureRandom();

    private KeyFiles() {
    }

    /** Refuses a name that could escape its directory or is not a plain token. */
    static String safeName(String name, String what) {
        if (name == null || !NAME.matcher(name).matches() || name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException(what + " '" + name + "' is not a safe file name:"
                    + " use 1 to 64 letters, digits, '.', '_' or '-', and not '.' or '..'");
        }
        return name;
    }

    /**
     * Writes a file atomically: a same-directory temp file, flushed, renamed
     * into place; a secret file is created owner-only (0600).
     *
     * @param file   the destination
     * @param bytes  the content
     * @param secret whether the content is key material
     * @throws IOException on I/O failure
     */
    public static void write(Path file, byte[] bytes, boolean secret) throws IOException {
        Path directory = file.toAbsolutePath().getParent();
        Files.createDirectories(directory);
        Path temp;
        if (secret) {
            try {
                Set<PosixFilePermission> ownerOnly = PosixFilePermissions.fromString("rw-------");
                temp = Files.createTempFile(directory, ".key-", ".tmp",
                        PosixFilePermissions.asFileAttribute(ownerOnly));
            } catch (UnsupportedOperationException e) {
                System.getLogger(KeyFiles.class.getName()).log(System.Logger.Level.WARNING,
                        "filesystem has no POSIX permissions; relying on OS ACLs for " + file);
                temp = Files.createTempFile(directory, ".key-", ".tmp");
            }
        } else {
            temp = Files.createTempFile(directory, ".pub-", ".tmp");
        }
        try {
            try (var channel = java.nio.channels.FileChannel.open(temp,
                    java.nio.file.StandardOpenOption.WRITE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
                channel.write(java.nio.ByteBuffer.wrap(bytes));
                channel.force(true);
            }
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /** Loads or creates an Ed25519 pair; refuses a mismatched or half-written pair. */
    static KeyPair ed25519(Path privateFile, Path publicFile) throws IOException {
        boolean hasPrivate = Files.exists(privateFile);
        boolean hasPublic = Files.exists(publicFile);
        if (hasPrivate != hasPublic) {
            throw half(privateFile, publicFile, hasPrivate);
        }
        if (!hasPrivate) {
            KeyPair fresh = Ed25519.generate();
            write(privateFile, fresh.getPrivate().getEncoded(), true);
            write(publicFile, Ed25519.rawPublicKey(fresh.getPublic()), false);
            return fresh;
        }
        PrivateKey privateKey;
        PublicKey publicKey;
        try {
            privateKey = Ed25519.privateKeyFromPkcs8(Files.readAllBytes(privateFile));
            byte[] raw = Files.readAllBytes(publicFile);
            if (raw.length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
                throw new AgentSpacesException("corrupt keystore: " + publicFile
                        + " is not a raw Ed25519 public key");
            }
            publicKey = Ed25519.publicKeyFromRaw(raw);
        } catch (RuntimeException e) {
            if (e instanceof AgentSpacesException ase) {
                throw ase;
            }
            throw new AgentSpacesException("corrupt keystore: cannot read " + privateFile
                    + " and " + publicFile + ": " + e.getMessage(), e);
        }
        byte[] probe = new byte[32];
        RANDOM.nextBytes(probe);
        if (!Ed25519.verify(publicKey, probe, Ed25519.sign(privateKey, probe))) {
            throw new AgentSpacesException("corrupt keystore: " + privateFile
                    + " does not belong to the public key in " + publicFile);
        }
        return new KeyPair(publicKey, privateKey);
    }

    /** Loads or creates an X25519 pair; refuses a mismatched or half-written pair. */
    static KeyPair x25519(Path privateFile, Path publicFile) throws IOException {
        boolean hasPrivate = Files.exists(privateFile);
        boolean hasPublic = Files.exists(publicFile);
        if (hasPrivate != hasPublic) {
            throw half(privateFile, publicFile, hasPrivate);
        }
        if (!hasPrivate) {
            KeyPair fresh = X25519.generate();
            write(privateFile, fresh.getPrivate().getEncoded(), true);
            write(publicFile, X25519.rawPublicKey(fresh.getPublic()), false);
            return fresh;
        }
        PrivateKey privateKey;
        byte[] raw;
        try {
            privateKey = KeyFactory.getInstance("XDH").generatePrivate(
                    new PKCS8EncodedKeySpec(Files.readAllBytes(privateFile)));
            raw = Files.readAllBytes(publicFile);
        } catch (GeneralSecurityException | RuntimeException e) {
            throw new AgentSpacesException("corrupt keystore: cannot read " + privateFile
                    + ": " + e.getMessage(), e);
        }
        if (raw.length != X25519.RAW_PUBLIC_KEY_LENGTH) {
            throw new AgentSpacesException("corrupt keystore: " + publicFile
                    + " is not a raw X25519 public key");
        }
        // The pair agrees with itself: an ephemeral peer derives the same secret
        // against the stored public key as the stored private key does against it.
        KeyPair ephemeral = X25519.generate();
        byte[] ours = X25519.agree(privateKey, X25519.rawPublicKey(ephemeral.getPublic()));
        byte[] theirs = X25519.agree(ephemeral.getPrivate(), raw);
        if (!Arrays.equals(ours, theirs)) {
            throw new AgentSpacesException("corrupt keystore: " + privateFile
                    + " does not belong to the public key in " + publicFile);
        }
        return new KeyPair(X25519.publicKeyFromRaw(raw), privateKey);
    }

    private static AgentSpacesException half(Path privateFile, Path publicFile, boolean hasPrivate) {
        return new AgentSpacesException("incomplete keystore: " + (hasPrivate ? privateFile : publicFile)
                + " exists but " + (hasPrivate ? publicFile : privateFile) + " does not; restore the"
                + " missing file from backup, or remove both to mint a new identity (which"
                + " changes the PeerID or agent key)");
    }
}
