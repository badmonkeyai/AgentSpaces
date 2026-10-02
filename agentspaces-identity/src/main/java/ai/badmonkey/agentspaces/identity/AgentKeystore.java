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

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.Objects;

/**
 * Persistent agent keys (SPEC §4.2, v0.1.13, TODO-9-10-11 B5), so a subordinate
 * agent keeps its key, and so its attested identity, across restarts. Layout
 * under the configured directory, one directory per peer and agent:
 *
 * <pre>
 * &lt;dir&gt;/&lt;peer-id&gt;/&lt;local-name&gt;/agent.key          Ed25519 PKCS#8, 0600
 * &lt;dir&gt;/&lt;peer-id&gt;/&lt;local-name&gt;/agent.pub          raw 32 bytes
 * &lt;dir&gt;/&lt;peer-id&gt;/&lt;local-name&gt;/agent-x25519.key   X25519 PKCS#8, 0600
 * &lt;dir&gt;/&lt;peer-id&gt;/&lt;local-name&gt;/agent-x25519.pub   raw 32 bytes
 * </pre>
 *
 * The peer level keeps two peers sharing a directory from loading each
 * other's agent keys. Certificates are not stored: the peer re-issues them at
 * start, since the key is what must persist. Agent names are refused, never
 * sanitized, when they could escape the directory.
 */
public final class AgentKeystore {

    private final Path directory;

    private AgentKeystore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
    }

    /**
     * A keystore rooted at a directory, created on first use.
     *
     * @param directory the keystore root
     * @return the keystore
     */
    public static AgentKeystore at(Path directory) {
        return new AgentKeystore(directory);
    }

    /**
     * The agent's Ed25519 signing pair, loaded or minted and persisted.
     *
     * @param peer      the owning peer
     * @param localName the agent's local name
     * @return the key pair
     */
    public KeyPair signingKeys(PeerId peer, String localName) {
        Path agentDir = agentDirectory(peer, localName);
        try {
            return KeyFiles.ed25519(agentDir.resolve("agent.key"), agentDir.resolve("agent.pub"));
        } catch (IOException e) {
            throw new UncheckedIOException("agent keystore I/O failure at " + agentDir, e);
        }
    }

    /**
     * The agent's X25519 encryption pair (per-agent key wrap, SPEC §11a.2),
     * loaded or minted and persisted.
     *
     * @param peer      the owning peer
     * @param localName the agent's local name
     * @return the key pair
     */
    public KeyPair encryptionKeys(PeerId peer, String localName) {
        Path agentDir = agentDirectory(peer, localName);
        try {
            return KeyFiles.x25519(agentDir.resolve("agent-x25519.key"),
                    agentDir.resolve("agent-x25519.pub"));
        } catch (IOException e) {
            throw new UncheckedIOException("agent keystore I/O failure at " + agentDir, e);
        }
    }

    /** The keystore root. */
    public Path directory() {
        return directory;
    }

    private Path agentDirectory(PeerId peer, String localName) {
        Objects.requireNonNull(peer, "peer");
        return directory.resolve(KeyFiles.safeName(peer.value(), "peer id"))
                .resolve(KeyFiles.safeName(localName, "agent name"));
    }
}
