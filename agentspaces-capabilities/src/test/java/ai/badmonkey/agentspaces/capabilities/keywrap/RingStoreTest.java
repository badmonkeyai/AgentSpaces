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
import ai.badmonkey.agentspaces.common.crypto.X25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.FileKeystore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Review B-2 of TODO-9-10-11: rotated epochs survive a restart, sealed to the peer's own key. */
class RingStoreTest {

    @TempDir
    Path keystore;

    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId group = GroupId.of("zRingStore");
    private final Instant cutover = Instant.parse("2026-10-01T12:00:00Z");

    @Test
    void aRestartedPeerRestoresEveryEpochItHeld() {
        var keys = FileKeystore.encryptionKeys(keystore);
        GroupKey zero = GroupKey.generate();
        GroupKey one = GroupKey.generate();
        GroupKeyRing ring = new RingStore(keystore, group, keys, codec).attach(GroupKeyRing.of(zero));
        ring.install(1, "zRotator", one, cutover, new byte[] {1, 2, 3});
        assertThat(ring.persistent()).as("an attached ring is persistent; rotation stays quiet").isTrue();

        GroupKeyRing restored = GroupKeyRing.empty();
        assertThat(restored.persistent()).as("a bare ring is not; rotating into it warns").isFalse();
        int count = new RingStore(keystore, group, FileKeystore.encryptionKeys(keystore), codec)
                .restoreInto(restored);
        assertThat(count).isEqualTo(2);
        assertThat(restored.epochs()).containsExactly(0L, 1L);
        assertThat(restored.sealingKey(1).orElseThrow().rawBytes()).isEqualTo(one.rawBytes());
        assertThat(restored.held()).filteredOn(h -> h.epoch() == 1).singleElement()
                .satisfies(h -> {
                    assertThat(h.proof()).containsExactly(1, 2, 3);
                    assertThat(h.cutover()).isEqualTo(cutover);
                });
    }

    @Test
    void aRingSealedToAnotherKeyOrGroupIsRefused() {
        new RingStore(keystore, group, FileKeystore.encryptionKeys(keystore), codec)
                .attach(GroupKeyRing.of(GroupKey.generate()));
        assertThatThrownBy(() -> new RingStore(keystore, group, X25519.generate(), codec)
                .restoreInto(GroupKeyRing.empty()))
                .hasMessageContaining("sealed to another key");
        Path other = keystore.resolve("content-keys").resolve("zOther.ring");
        assertThatThrownBy(() -> {
            Files.copy(keystore.resolve("content-keys").resolve("zRingStore.ring"), other);
            new RingStore(keystore, GroupId.of("zOther"), FileKeystore.encryptionKeys(keystore), codec)
                    .restoreInto(GroupKeyRing.empty());
        }).hasMessageContaining("not this group's");
        assertThatThrownBy(() -> new RingStore(keystore, GroupId.of("../x"),
                FileKeystore.encryptionKeys(keystore), codec)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRingFileIsOwnerOnly() throws Exception {
        assumeTrue(keystore.getFileSystem().supportedFileAttributeViews().contains("posix"));
        RingStore store = new RingStore(keystore, group, FileKeystore.encryptionKeys(keystore), codec);
        store.attach(GroupKeyRing.of(GroupKey.generate()));
        assertThat(Files.getPosixFilePermissions(store.file()))
                .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }
}
