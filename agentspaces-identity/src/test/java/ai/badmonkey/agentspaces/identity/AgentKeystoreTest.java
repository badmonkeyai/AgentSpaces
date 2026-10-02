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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * TODO-9-10-11 B5 (R9): agent keys persist per peer and agent, survive a
 * reload, refuse traversal names and mismatched or half-written pairs, and are
 * written owner-only; the peer keystore gains the same load checks.
 */
class AgentKeystoreTest {

    @TempDir
    Path dir;

    private final PeerIdentity peer = PeerIdentity.generate();

    @Test
    void anAgentKeepsItsKeysAcrossReloads() {
        KeyPair first = AgentKeystore.at(dir).signingKeys(peer.peerId(), "auditor");
        KeyPair again = AgentKeystore.at(dir).signingKeys(peer.peerId(), "auditor");
        assertThat(Ed25519.rawPublicKey(again.getPublic()))
                .isEqualTo(Ed25519.rawPublicKey(first.getPublic()));
        KeyPair x1 = AgentKeystore.at(dir).encryptionKeys(peer.peerId(), "auditor");
        KeyPair x2 = AgentKeystore.at(dir).encryptionKeys(peer.peerId(), "auditor");
        assertThat(X25519.rawPublicKey(x2.getPublic())).isEqualTo(X25519.rawPublicKey(x1.getPublic()));
        assertThat(Files.exists(dir.resolve(peer.peerId().value()).resolve("auditor").resolve("agent.key"))).isTrue();
    }

    @Test
    void twoNamesAndTwoPeersSharingADirectoryGetDistinctKeys() {
        AgentKeystore keystore = AgentKeystore.at(dir);
        byte[] auditor = Ed25519.rawPublicKey(keystore.signingKeys(peer.peerId(), "auditor").getPublic());
        byte[] clerk = Ed25519.rawPublicKey(keystore.signingKeys(peer.peerId(), "clerk").getPublic());
        byte[] otherPeers = Ed25519.rawPublicKey(keystore.signingKeys(
                PeerIdentity.generate().peerId(), "auditor").getPublic());
        assertThat(auditor).isNotEqualTo(clerk).isNotEqualTo(otherPeers);
    }

    @Test
    void traversalNamesAreRefusedNotSanitized() {
        AgentKeystore keystore = AgentKeystore.at(dir);
        for (String hostile : new String[] {"../x", "a/b", "..", ".", "", "a\\b", "x".repeat(65)}) {
            assertThatThrownBy(() -> keystore.signingKeys(peer.peerId(), hostile))
                    .as(hostile).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not a safe file name");
        }
    }

    @Test
    void aMismatchedPairIsRefused() throws Exception {
        AgentKeystore keystore = AgentKeystore.at(dir);
        keystore.signingKeys(peer.peerId(), "auditor");
        Path agentDir = dir.resolve(peer.peerId().value()).resolve("auditor");
        Files.write(agentDir.resolve("agent.pub"), Ed25519.rawPublicKey(Ed25519.generate().getPublic()));
        assertThatThrownBy(() -> keystore.signingKeys(peer.peerId(), "auditor"))
                .isInstanceOf(AgentSpacesException.class).hasMessageContaining("does not belong");
    }

    @Test
    void aHalfWrittenPairIsRefusedNamingBothFiles() throws Exception {
        AgentKeystore keystore = AgentKeystore.at(dir);
        keystore.signingKeys(peer.peerId(), "auditor");
        Path agentDir = dir.resolve(peer.peerId().value()).resolve("auditor");
        Files.delete(agentDir.resolve("agent.pub"));
        assertThatThrownBy(() -> keystore.signingKeys(peer.peerId(), "auditor"))
                .isInstanceOf(AgentSpacesException.class)
                .hasMessageContaining("agent.key").hasMessageContaining("agent.pub");
    }

    @Test
    void privateFilesAreOwnerOnly() throws Exception {
        assumeTrue(dir.getFileSystem().supportedFileAttributeViews().contains("posix"));
        AgentKeystore.at(dir).signingKeys(peer.peerId(), "auditor");
        AgentKeystore.at(dir).encryptionKeys(peer.peerId(), "auditor");
        Path agentDir = dir.resolve(peer.peerId().value()).resolve("auditor");
        for (String file : new String[] {"agent.key", "agent-x25519.key"}) {
            assertThat(Files.getPosixFilePermissions(agentDir.resolve(file))).as(file)
                    .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        }
        try (var leftovers = Files.list(agentDir)) {
            assertThat(leftovers.map(p -> p.getFileName().toString()))
                    .as("no temp files left behind").noneMatch(name -> name.endsWith(".tmp"));
        }
    }

    @Test
    void thePeerKeystoreRefusesAMismatchedOrHalfWrittenPair() throws Exception {
        Path peerDir = dir.resolve("peer");
        PeerIdentity stored = FileKeystore.loadOrCreate(peerDir);
        assertThat(FileKeystore.loadOrCreate(peerDir).peerId()).isEqualTo(stored.peerId());
        Files.write(peerDir.resolve("peer.pub"), Ed25519.rawPublicKey(Ed25519.generate().getPublic()));
        assertThatThrownBy(() -> FileKeystore.loadOrCreate(peerDir))
                .hasMessageContaining("does not belong");
        Files.delete(peerDir.resolve("peer.pub"));
        assertThatThrownBy(() -> FileKeystore.loadOrCreate(peerDir))
                .hasMessageContaining("peer.key").hasMessageContaining("peer.pub");
    }

    /** A subordinate agent over persisted keys keeps its key — and so its attested identity — across restarts. */
    @Test
    void aRenewingAgentOverPersistedKeysKeepsItsKey() {
        ai.badmonkey.agentspaces.test.TestClock clock = ai.badmonkey.agentspaces.test.TestClock.create();
        KeyPair keys = AgentKeystore.at(dir).signingKeys(peer.peerId(), "auditor");
        var before = peer.renewingSubordinate("auditor", keys, java.time.Duration.ofHours(24), clock);
        var after = peer.renewingSubordinate("auditor",
                AgentKeystore.at(dir).signingKeys(peer.peerId(), "auditor"),
                java.time.Duration.ofHours(24), clock);
        assertThat(after.publicKey()).isEqualTo(before.publicKey());
    }
}
