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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.util.Objects;

/**
 * Encodes a key or value to the canonical CBOR item a node embeds and decodes it
 * back (srad/agentspaces-pdt.md §3.1, §3.2). The default of {@link #of} is
 * {@code CborCodec.toBytes} and {@code fromBytes} with the class, so a key hashes
 * and compares by the same bytes every language produces.
 *
 * @param <T> the application type
 */
public interface Codec<T> {

    /** The canonical CBOR bytes of a value, exactly one data item. */
    byte[] encode(T value);

    /** The value of one CBOR data item. */
    T decode(byte[] bytes);

    /** The canonical codec for a class. */
    static <T> Codec<T> of(Class<T> type) {
        Objects.requireNonNull(type, "type");
        CborCodec cbor = CborCodec.defaultCodec();
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                return cbor.toBytes(value);
            }

            @Override
            public T decode(byte[] bytes) {
                return cbor.fromBytes(bytes, type);
            }
        };
    }
}
