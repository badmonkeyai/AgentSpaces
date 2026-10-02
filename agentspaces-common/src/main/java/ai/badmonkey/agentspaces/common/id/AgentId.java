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
package ai.badmonkey.agentspaces.common.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

/**
 * The identity of a logical agent hosted by a peer (§4.2 of the spec):
 * {@code AgentID = (PeerID, local-name)}. One peer commonly hosts several agents,
 * as an Embabel application does.
 *
 * <p>The canonical string form is {@code <peerId>/<localName>}.
 *
 * @param peer      the hosting peer's ID
 * @param localName the agent's name, unique within its peer
 */
public record AgentId(PeerId peer, String localName) {

    public AgentId {
        Objects.requireNonNull(peer, "peer");
        Objects.requireNonNull(localName, "localName");
        if (localName.isEmpty() || localName.indexOf('/') >= 0) {
            throw new IllegalArgumentException(
                    "localName must be non-empty and contain no '/': " + localName);
        }
    }

    /**
     * Parses the canonical {@code <peerId>/<localName>} form.
     *
     * @param encoded the canonical string form
     * @return the parsed AgentID
     * @throws IllegalArgumentException if the input does not match the canonical form
     */
    @JsonCreator
    public static AgentId parse(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        int slash = encoded.indexOf('/');
        if (slash <= 0 || slash == encoded.length() - 1) {
            throw new IllegalArgumentException("expected <peerId>/<localName>, got: " + encoded);
        }
        return new AgentId(PeerId.of(encoded.substring(0, slash)), encoded.substring(slash + 1));
    }

    /** Returns the canonical {@code <peerId>/<localName>} form; this is the serialized form. */
    @JsonValue
    public String encoded() {
        return peer.value() + "/" + localName;
    }

    @Override
    public String toString() {
        return encoded();
    }
}
