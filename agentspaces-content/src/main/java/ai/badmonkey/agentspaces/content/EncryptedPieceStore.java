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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.common.crypto.Digests;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Encryption at rest for a piece store (srad/large-networks-bulk-data.md LB-T-11): every piece
 * is written through AES-256-CTR under a local key, with a counter that starts from a value
 * derived from the root and the index, so a range anywhere in a piece decrypts on its own and
 * the file on disk holds no plaintext. The key never leaves the peer and protects a stolen disk,
 * not the wire: pieces still travel as the manifest says (sealed under the group key or not), and
 * integrity stays the piece table's job, checked by the fetcher on every piece.
 *
 * <p>The counter for {@code (root, index)} is fixed, which is safe here because a piece's bytes
 * never change: a root names its bytes, and a store never rewrites a held piece.
 */
public final class EncryptedPieceStore implements PieceStore {

    /** The key file a store directory keeps; created on first use with owner-only permissions. */
    public static final String KEY_FILE = ".at-rest.key";

    private static final int BLOCK = 16;

    private final PieceStore inner;
    private final SecretKeySpec key;

    /**
     * Wraps a store.
     *
     * @param inner the store that holds the encrypted pieces
     * @param key   the 32-byte local key
     */
    public EncryptedPieceStore(PieceStore inner, byte[] key) {
        this.inner = Objects.requireNonNull(inner, "inner");
        if (inner instanceof HashPieceStore) {
            // a hash store names each piece by its plaintext hash and verifies it on write (P2-H-01)
            throw new IllegalArgumentException("a hash store keeps pieces under their plaintext hashes; encrypt at rest over a file store");
        }
        if (key == null || key.length != 32) {
            throw new IllegalArgumentException("an at-rest key is 32 bytes");
        }
        this.key = new SecretKeySpec(key.clone(), "AES");
    }

    /**
     * The local key kept in a directory: read when present, generated and written owner-only
     * otherwise.
     *
     * @param dir the store directory
     * @return the key
     * @throws IOException when the key cannot be read or written
     */
    public static byte[] keyIn(Path dir) throws IOException {
        Files.createDirectories(dir);
        Path file = dir.resolve(KEY_FILE);
        if (Files.isRegularFile(file)) {
            byte[] key = Files.readAllBytes(file);
            if (key.length != 32) {
                throw new IOException(file + " is not a 32-byte key");
            }
            return key;
        }
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        Path tmp = dir.resolve(KEY_FILE + ".tmp");
        Files.write(tmp, key, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        try {
            Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException e) {
            // a filesystem without POSIX permissions keeps its defaults
        }
        Files.move(tmp, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        return key;
    }

    private byte[] crypt(String root, int index, int offset, byte[] data) {
        byte[] iv = Arrays.copyOf(Digests.sha256((root + "#" + index).getBytes(StandardCharsets.UTF_8)), BLOCK);
        BigInteger counter = new BigInteger(1, iv).add(BigInteger.valueOf(offset / BLOCK));
        byte[] start = counter.toByteArray();
        byte[] block = new byte[BLOCK];
        int len = Math.min(start.length, BLOCK);
        System.arraycopy(start, start.length - len, block, BLOCK - len, len);
        try {
            Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(block));
            int skip = offset % BLOCK;
            if (skip > 0) {
                cipher.update(new byte[skip]);
            }
            return cipher.update(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-CTR is unavailable", e);
        }
    }

    @Override
    public String kind() {
        return inner.kind();
    }

    @Override
    public boolean writable() {
        return inner.writable();
    }

    @Override
    public Set<String> roots() {
        return inner.roots();
    }

    @Override
    public Optional<Manifest> manifest(String root) {
        return inner.manifest(root);
    }

    @Override
    public void open(Manifest manifest) {
        inner.open(manifest);
    }

    @Override
    public BitSet have(String root) {
        return inner.have(root);
    }

    @Override
    public Optional<byte[]> read(String root, int index, int offset, int length) {
        return inner.read(root, index, offset, length).map(bytes -> crypt(root, index, offset, bytes));
    }

    @Override
    public void write(String root, int index, byte[] piece) {
        inner.write(root, index, crypt(root, index, 0, piece));
    }

    @Override
    public SeekableByteChannel openArtifact(String root) throws IOException {
        Manifest m = inner.manifest(root).orElseThrow(() -> new IOException("root " + root + " is not held here"));
        if (inner.have(root).cardinality() < m.pieces()) {
            throw new IOException("root " + root + " is incomplete here");
        }
        Path plain = Files.createTempFile("agentspaces-at-rest-", ".bin");
        try (FileChannel out = FileChannel.open(plain, StandardOpenOption.WRITE)) {
            for (int i = 0; i < m.pieces(); i++) {
                byte[] piece = read(root, i, 0, m.pieceLength(i)).orElseThrow(() ->
                        new IOException("root " + root + " is incomplete here"));
                ByteBuffer buffer = ByteBuffer.wrap(piece);
                while (buffer.hasRemaining()) {
                    out.write(buffer);
                }
            }
        } catch (IOException | UncheckedIOException e) {
            Files.deleteIfExists(plain);
            throw e;
        }
        return FileChannel.open(plain, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
    }

    @Override
    public void pin(String root) {
        inner.pin(root);
    }

    @Override
    public void unpin(String root) {
        inner.unpin(root);
    }

    @Override
    public boolean pinned(String root) {
        return inner.pinned(root);
    }

    @Override
    public long bytesUsed() {
        return inner.bytesUsed();
    }

    @Override
    public long quota() {
        return inner.quota();
    }

    @Override
    public void evict() {
        inner.evict();
    }

    @Override
    public void forget(String root) {
        inner.forget(root);
    }

    @Override
    public void flush() {
        inner.flush();
    }
}
