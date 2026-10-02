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
 * The self-certifying identifier of a peer group (§4.4, §5.1 of the spec): the
 * multibase-encoded SHA-256 of the group's founding advertisement bytes. Possession
 * of the founding document proves the name.
 *
 * @param value the multibase (base58btc) encoded hash
 */
public record GroupId(String value) {

    public GroupId {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("GroupId value must be non-empty");
        }
    }

    /**
     * Derives a GroupID from the canonical bytes of the founding advertisement.
     *
     * @param foundingAdvertisementBytes canonical CBOR of the founding GroupAdvertisement
     * @return the derived, self-certifying GroupID
     */
    public static GroupId fromFounding(byte[] foundingAdvertisementBytes) {
        Objects.requireNonNull(foundingAdvertisementBytes, "foundingAdvertisementBytes");
        return new GroupId(Multibase.base58btc(Digests.sha256(foundingAdvertisementBytes)));
    }

    /**
     * Wraps an existing encoded GroupID value.
     *
     * @param value the multibase-encoded ID
     * @return the GroupID
     */
    @JsonCreator
    public static GroupId of(String value) {
        return new GroupId(value);
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
