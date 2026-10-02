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
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;

import java.util.Objects;

/**
 * The stable cryptographic identity of a peer (§4.1 of the spec):
 * {@code PeerID = multibase(sha-256(raw-public-key))}. The ID is independent of
 * network location and stays constant across restarts, addresses, and transports.
 *
 * @param value the multibase (base58btc) encoding of the SHA-256 of the raw public key
 */
public record PeerId(String value) {

    public PeerId {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("PeerId value must be non-empty");
        }
    }

    /**
     * Derives the PeerID for a raw 32-byte Ed25519 public key.
     *
     * @param rawPublicKey the raw public key bytes
     * @return the derived PeerID
     */
    public static PeerId fromPublicKey(byte[] rawPublicKey) {
        Objects.requireNonNull(rawPublicKey, "rawPublicKey");
        return new PeerId(Multibase.base58btc(Digests.sha256(rawPublicKey)));
    }

    /**
     * Wraps an existing encoded PeerID value.
     *
     * @param value the multibase-encoded ID
     * @return the PeerID
     */
    @JsonCreator
    public static PeerId of(String value) {
        return new PeerId(value);
    }

    /** Returns the encoded value; this is the serialized form. */
    @JsonValue
    public String value() {
        return value;
    }

    /** Returns a short display form for logs, e.g. {@code peer:z3vQx…}. */
    public String display() {
        return "peer:" + (value.length() <= 10 ? value : value.substring(0, 10) + "…");
    }

    @Override
    public String toString() {
        return value;
    }
}
