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
package ai.badmonkey.agentspaces.capabilities.keywrap;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap;
import ai.badmonkey.agentspaces.common.crypto.X25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.KeyFiles;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Persists a group's content-key ring (SPEC §11a.3, v0.1.13, review B-2), so
 * epochs minted by rotation survive restarts: every epoch key is sealed to this
 * peer's own persisted X25519 key under a binding naming the group, epoch, and
 * rotator, and the file is written atomically, owner-only, at
 * {@code <keystore>/content-keys/<group-id>.ring}. A ring that has never been
 * rotated still saves its epoch 0, which the configuration also supplies.
 */
public final class RingStore {

    private record StoredKey(long epoch, String rotator, String cutover,
                             @com.fasterxml.jackson.annotation.JsonInclude(
                                     com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                             byte[] proof,
                             byte[] ephemeralPublicKey, byte[] sealed) {
    }

    private record StoredRing(String group, List<StoredKey> keys) {
    }

    private record SelfBinding(String purpose, String group, long epoch, String rotator) {
    }

    private final Path file;
    private final KeyPair encryptionKeys;
    private final GroupId group;
    private final CborCodec codec;

    /**
     * A store for one group's ring.
     *
     * @param keystore       the peer keystore directory
     * @param group          the group
     * @param encryptionKeys the peer's persisted X25519 pair
     * @param codec          the CBOR codec
     */
    public RingStore(Path keystore, GroupId group, KeyPair encryptionKeys, CborCodec codec) {
        Objects.requireNonNull(keystore, "keystore");
        this.group = Objects.requireNonNull(group, "group");
        if (!group.value().matches("[A-Za-z0-9._-]{1,128}") || group.value().contains("..")) {
            throw new IllegalArgumentException("group id '" + group.value()
                    + "' is not a safe file name for its content-key ring");
        }
        this.file = keystore.resolve("content-keys").resolve(group.value() + ".ring");
        this.encryptionKeys = Objects.requireNonNull(encryptionKeys, "encryptionKeys");
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    /** The ring file. */
    public Path file() {
        return file;
    }

    /**
     * Installs every key the file holds into a ring.
     *
     * @param ring the ring to fill
     * @return how many keys were restored
     */
    public int restoreInto(GroupKeyRing ring) {
        if (!Files.exists(file)) {
            return 0;
        }
        StoredRing stored;
        try {
            stored = codec.fromBytes(Files.readAllBytes(file), StoredRing.class);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read content-key ring " + file, e);
        }
        if (stored == null || !group.value().equals(stored.group()) || stored.keys() == null) {
            throw new IllegalStateException("content-key ring " + file + " is not this group's");
        }
        int restored = 0;
        for (StoredKey key : stored.keys()) {
            Optional<GroupKey> opened = GroupKeyWrap.unwrapBound(
                    new GroupKeyWrap.WrappedKey(key.ephemeralPublicKey(), key.sealed()),
                    encryptionKeys.getPrivate(), X25519.rawPublicKey(encryptionKeys.getPublic()),
                    binding(key.epoch(), key.rotator()));
            if (opened.isEmpty()) {
                throw new IllegalStateException("content-key ring " + file + " holds epoch "
                        + key.epoch() + " sealed to another key; was the peer keystore replaced?");
            }
            ring.install(key.epoch(), key.rotator(), opened.get(), Instant.parse(key.cutover()),
                    key.proof());
            restored++;
        }
        return restored;
    }

    /**
     * Writes the ring's keys, sealed to this peer.
     *
     * @param ring the ring to save
     */
    public synchronized void save(GroupKeyRing ring) {
        List<StoredKey> keys = new ArrayList<>();
        byte[] self = X25519.rawPublicKey(encryptionKeys.getPublic());
        for (GroupKeyRing.Held held : ring.held()) {
            GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrapBound(held.key(), self,
                    binding(held.epoch(), held.rotator()));
            keys.add(new StoredKey(held.epoch(), held.rotator(), held.cutover().toString(),
                    held.proof(), wrapped.ephemeralPublicKey(), wrapped.sealed()));
        }
        try {
            KeyFiles.write(file, codec.toBytes(new StoredRing(group.value(), keys)), true);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write content-key ring " + file, e);
        }
    }

    /**
     * Restores the ring from the file, then saves it on every newly installed key.
     *
     * @param ring the group's ring
     * @return the ring
     */
    public GroupKeyRing attach(GroupKeyRing ring) {
        restoreInto(ring);
        save(ring);
        ring.onInstalled(held -> save(ring));
        ring.markPersistent();
        return ring;
    }

    private byte[] binding(long epoch, String rotator) {
        return codec.toBytes(new SelfBinding("aspace-content-key-ring-v1", group.value(), epoch,
                rotator));
    }
}
