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
package ai.badmonkey.agentspaces.pdt;

import java.util.Arrays;
import java.util.Objects;

/**
 * An application value embedded in a node as one raw CBOR item (srad/agentspaces-pdt.md
 * §3.1, PD-N-01): a map key or value, a vector element, or a spilled value. The
 * bytes are what {@code CborCodec.toBytes} produced; {@link NodeCodec} splices them
 * into the node unchanged and recovers them by skipping one CBOR item, so a node
 * stays valid DAG-CBOR without the codec learning the application's types.
 *
 * @param bytes exactly one CBOR data item
 */
public record Embedded(byte[] bytes) {

    /** Validates and copies the bytes. */
    public Embedded {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0) {
            throw new IllegalArgumentException("an embedded item has at least one byte");
        }
        bytes = bytes.clone();
    }

    /** The CBOR bytes, copied. */
    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    /** The number of bytes. */
    public int length() {
        return bytes.length;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Embedded other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "Embedded[" + bytes.length + " bytes]";
    }
}
