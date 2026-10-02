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
package ai.badmonkey.agentspaces.common.codec;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * The canonical AgentSpaces codec: CBOR (RFC 8949) for all wire payloads and entry
 * serialization, with a JSON view for logs and debugging. Records serialize in
 * declared component order, which is the v0.1 canonical form that advertisement
 * signatures are computed over.
 *
 * <p>Unknown fields are ignored on read, so a v0.1 peer tolerates cards and entries
 * from newer peers that add fields.
 */
public final class CborCodec {

    private static final CborCodec DEFAULT = new CborCodec();

    private final ObjectMapper cbor;
    private final ObjectMapper json;

    /**
     * Deepest container nesting the parser accepts (ASF-018): matches the
     * 64-level cap the non-JVM clients enforce, so all three implementations
     * refuse hostile deep nesting identically, and far below any honest wire
     * structure. Document length is bounded upstream by the 8 MiB frame cap.
     */
    private static final int MAX_NESTING_DEPTH = 64;

    private CborCodec() {
        this.cbor = configure(new ObjectMapper(constrained(new CBORFactory())));
        this.json = configure(new ObjectMapper());
    }

    private static CBORFactory constrained(CBORFactory factory) {
        factory.setStreamReadConstraints(StreamReadConstraints.builder()
                .maxNestingDepth(MAX_NESTING_DEPTH)
                .build());
        return factory;
    }

    private static ObjectMapper configure(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // ISO-8601 strings for Instant and Duration (e.g. "PT15M"), matching the
        // spec's advertisement examples and keeping the canonical form readable.
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
        return mapper;
    }

    /** Returns the shared default codec. */
    public static CborCodec defaultCodec() {
        return DEFAULT;
    }

    /**
     * Serializes a value to canonical CBOR bytes.
     *
     * @param value the value to serialize
     * @return the CBOR encoding
     */
    public byte[] toBytes(Object value) {
        Objects.requireNonNull(value, "value");
        try {
            return cbor.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize " + value.getClass().getName(), e);
        }
    }

    /**
     * Deserializes CBOR bytes into the given type.
     *
     * @param bytes the CBOR encoding
     * @param type  the target type
     * @param <T>   the target type
     * @return the deserialized value
     */
    public <T> T fromBytes(byte[] bytes, Class<T> type) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(type, "type");
        try {
            return cbor.readValue(bytes, type);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to deserialize " + type.getName(), e);
        }
    }

    /**
     * Renders a value as JSON, for logs and debugging only. The wire format is CBOR.
     *
     * @param value the value to render
     * @return a JSON string
     */
    public String toJson(Object value) {
        Objects.requireNonNull(value, "value");
        try {
            return json.writeValueAsString(value);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to render " + value.getClass().getName(), e);
        }
    }
}
