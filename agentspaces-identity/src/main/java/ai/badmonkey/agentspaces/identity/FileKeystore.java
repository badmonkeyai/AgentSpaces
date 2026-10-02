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


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Loads or creates a peer identity persisted on disk (spec §4.1: the ID is stable
 * across restarts because the keypair is). The keystore is a directory holding
 * {@code peer.key} (PKCS#8 private key) and {@code peer.pub} (raw 32-byte public
 * key); on POSIX filesystems the private key is written with owner-only
 * permissions.
 */
public final class FileKeystore {

    private static final String PRIVATE_FILE = "peer.key";
    private static final String PUBLIC_FILE = "peer.pub";

    private FileKeystore() {
    }

    /**
     * Loads the identity stored in the directory, or generates and persists a
     * fresh one when the directory holds none.
     *
     * @param directory the keystore directory; created when absent
     * @return the peer identity
     */
    public static PeerIdentity loadOrCreate(Path directory) {
        Objects.requireNonNull(directory, "directory");
        try {
            Files.createDirectories(directory);
            // TODO-9-10-11 B5: atomic, owner-only writes (ASF-035 kept), a load
            // that proves the pair matches, and a half-written pair refused with
            // a message naming both files instead of an opaque I/O failure.
            return PeerIdentity.of(KeyFiles.ed25519(directory.resolve(PRIVATE_FILE),
                    directory.resolve(PUBLIC_FILE)));
        } catch (IOException e) {
            throw new UncheckedIOException("keystore I/O failure at " + directory, e);
        }
    }

    /**
     * The peer's X25519 key-agreement pair (SPEC §11a.2), loaded or minted and
     * persisted beside its Ed25519 identity as {@code peer-x25519.key} (0600)
     * and {@code peer-x25519.pub}, so content keys sealed to this peer stay
     * openable across restarts (TODO-9-10-11 D6).
     *
     * @param directory the keystore directory
     * @return the key pair
     */
    public static java.security.KeyPair encryptionKeys(Path directory) {
        Objects.requireNonNull(directory, "directory");
        try {
            Files.createDirectories(directory);
            return KeyFiles.x25519(directory.resolve("peer-x25519.key"),
                    directory.resolve("peer-x25519.pub"));
        } catch (IOException e) {
            throw new UncheckedIOException("keystore I/O failure at " + directory, e);
        }
    }
}
