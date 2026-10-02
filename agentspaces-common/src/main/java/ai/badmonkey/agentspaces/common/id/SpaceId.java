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
 * The self-certifying identifier of a space (§4.4 of the spec): the multibase-encoded
 * SHA-256 of the space's founding advertisement bytes. A purely local space derives
 * its ID from its name instead, via {@link #local(String)}.
 *
 * @param value the multibase (base58btc) encoded hash
 */
public record SpaceId(String value) {

    public SpaceId {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("SpaceId value must be non-empty");
        }
    }

    /**
     * Derives a SpaceID from the canonical bytes of the founding advertisement.
     *
     * @param foundingAdvertisementBytes canonical CBOR of the founding SpaceAdvertisement
     * @return the derived, self-certifying SpaceID
     */
    public static SpaceId fromFounding(byte[] foundingAdvertisementBytes) {
        Objects.requireNonNull(foundingAdvertisementBytes, "foundingAdvertisementBytes");
        return new SpaceId(Multibase.base58btc(Digests.sha256(foundingAdvertisementBytes)));
    }

    /**
     * Derives a deterministic SpaceID for a purely local (unadvertised) space.
     *
     * @param name the local space name
     * @return a SpaceID derived from the name
     */
    public static SpaceId local(String name) {
        Objects.requireNonNull(name, "name");
        return new SpaceId(Multibase.base58btc(
                Digests.sha256(("local:" + name).getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    /**
     * Wraps an existing encoded SpaceID value.
     *
     * @param value the multibase-encoded ID
     * @return the SpaceID
     */
    @JsonCreator
    public static SpaceId of(String value) {
        return new SpaceId(value);
    }

    /** Returns the encoded value; this is the serialized form. */
    @JsonValue
    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
