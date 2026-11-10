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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The canonical AgentSpaces codec: CBOR (RFC 8949) for all wire payloads and entry
 * serialization, with a JSON view for logs and debugging. Records serialize in
 * declared component order, which is the v0.1 canonical form that advertisement
 * signatures are computed over.
 *
 * <p>The encoder writes the wire encoding (HLC round six, 2026-11-08): one form per value, so
 * every implementation that decodes a document and encodes it again gets the bytes it received.
 * Three reads serve three kinds of input:
 *
 * <ul>
 *   <li>{@link #fromWire} reads a protocol record from the network: the item rules, a mapping
 *       with every coercion off (owner ruling F1), and a re-encoding that must equal the bytes
 *       received (owner ruling F3);</li>
 *   <li>{@link #fromDocument} reads an application document, an entry payload: the item rules,
 *       and the lenient mapping that ignores unknown fields;</li>
 *   <li>{@link #fromBytes} reads what this node persisted, written before the wire encoding,
 *       with the lenient checks of HLC round five.</li>
 * </ul>
 */
public final class CborCodec {

    private static final CborCodec DEFAULT = new CborCodec();

    private final ObjectMapper cbor;
    private final ObjectMapper wire;
    private final ObjectMapper json;

    /**
     * Deepest container nesting the parser accepts (ASF-018): matches the
     * 64-level cap the non-JVM clients enforce, so all three implementations
     * refuse hostile deep nesting identically, and far below any honest wire
     * structure. Document length is bounded upstream by the 8 MiB frame cap.
     */
    private static final int MAX_NESTING_DEPTH = 64;

    /** The first and last instants an {@link Instant} field holds on the wire (P2): years 0000 to 9999. */
    private static final Instant FIRST_INSTANT = Instant.parse("0000-01-01T00:00:00Z");
    private static final Instant LAST_INSTANT = Instant.parse("9999-12-31T23:59:59.999999999Z");

    private CborCodec() {
        this.cbor = configure(new ObjectMapper(constrained(new CBORFactory())));
        // ISSUE-CanonicalMaps (wire v3): map entries in RFC 8949 order in every
        // CBOR structure. The JSON rendering is for logs and keeps its order.
        // HLC round six, 2026-11-08 (JC-4): set elements in the order of their encodings.
        this.cbor.registerModule(CanonicalMaps.module(this::toBytes));
        // HLC round six, 2026-11-08 (JC-1 to JC-3): one encoding per number.
        this.cbor.registerModule(CanonicalNumbers.module());
        // HLC round four, 2026-11-08 (decision D3): a stamp decodes only from its canonical item.
        this.cbor.registerModule(CanonicalStamps.module());
        this.wire = strict();
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

    /**
     * The reader of protocol records (HLC round six, 2026-11-08, owner ruling F1): an item of the
     * wrong kind never becomes a field. A float, a decimal, a bignum or text is no integer, an
     * integer is no double, a boolean or text, a byte string is no text, base64 text is no byte
     * string, a number is no enum, an {@link Instant} or a {@link Duration} is text alone, a null
     * is no primitive, and a field the record does not name is refused.
     */
    private static ObjectMapper strict() {
        ObjectMapper mapper = CBORMapper.builder(constrained(new CBORFactory()))
                .addModule(new JavaTimeModule())
                .addModule(CanonicalStamps.module())
                .addModule(strictScalars())
                .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
                .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .withCoercionConfigDefaults(config -> config
                        .setAcceptBlankAsEmpty(false)
                        .setCoercion(CoercionInputShape.EmptyString, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.EmptyArray, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.EmptyObject, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.String, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail)
                        .setCoercion(CoercionInputShape.Binary, CoercionAction.Fail))
                .withCoercionConfig(LogicalType.Textual, config -> config
                        .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert))
                .withCoercionConfig(LogicalType.Integer, config -> config
                        .setCoercion(CoercionInputShape.Integer, CoercionAction.TryConvert))
                .withCoercionConfig(LogicalType.Float, config -> config
                        .setCoercion(CoercionInputShape.Float, CoercionAction.TryConvert))
                .withCoercionConfig(LogicalType.Boolean, config -> config
                        .setCoercion(CoercionInputShape.Boolean, CoercionAction.TryConvert))
                // HLC round six, 2026-11-08 (P2, ruling F1): a map<String, X> field never holds a
                // null value; PRef.attrs reads its reserved keys with its own deserializer
                .withConfigOverride(Map.class, override -> override
                        .setSetterInfo(JsonSetter.Value.forContentNulls(Nulls.FAIL)))
                // HLC round seven, 2026-11-08 (P2, ruling F1): a list<X> or set<X> field holds
                // items of kind X, so a null element is no element
                .withConfigOverride(List.class, override -> override
                        .setSetterInfo(JsonSetter.Value.forContentNulls(Nulls.FAIL)))
                .withConfigOverride(Set.class, override -> override
                        .setSetterInfo(JsonSetter.Value.forContentNulls(Nulls.FAIL)))
                .build();
        return mapper;
    }

    /**
     * Text, byte strings, instants and durations from their own items alone. Jackson turns a
     * byte string into base64 text, base64 text into bytes and a number into an instant, and
     * reads a negative duration and an instant outside the years 0000 to 9999, which no protocol
     * field holds.
     */
    private static SimpleModule strictScalars() {
        SimpleModule module = new SimpleModule("agentspaces-wire-scalars");
        module.addDeserializer(String.class, new Scalar<>(String.class, JsonToken.VALUE_STRING) {
            @Override
            String read(JsonParser p) throws IOException {
                return p.getText();
            }
        });
        module.addDeserializer(byte[].class, new Scalar<>(byte[].class, JsonToken.VALUE_EMBEDDED_OBJECT) {
            @Override
            byte[] read(JsonParser p) throws IOException {
                return p.getEmbeddedObject() instanceof byte[] bytes ? bytes : null;
            }
        });
        module.addDeserializer(Instant.class, new Scalar<>(Instant.class, JsonToken.VALUE_STRING) {
            @Override
            Instant read(JsonParser p) throws IOException {
                try {
                    Instant instant = Instant.parse(p.getText());
                    // HLC round six, 2026-11-08 (P2): the years 0000 to 9999 alone, which every
                    // implementation's date type holds; Java prints any other year with a sign
                    return instant.isBefore(FIRST_INSTANT) || instant.isAfter(LAST_INSTANT) ? null : instant;
                } catch (DateTimeParseException e) {
                    return null;
                }
            }
        });
        module.addDeserializer(Duration.class, new Scalar<>(Duration.class, JsonToken.VALUE_STRING) {
            @Override
            Duration read(JsonParser p) throws IOException {
                try {
                    Duration duration = Duration.parse(p.getText());
                    return duration.isNegative() ? null : duration;
                } catch (DateTimeParseException e) {
                    return null;
                }
            }
        });
        return module;
    }

    /** A scalar read from one token kind; {@link #read} returns null for an item it refuses. */
    private abstract static class Scalar<T> extends StdDeserializer<T> {
        private static final long serialVersionUID = 1L;

        private final JsonToken token;

        Scalar(Class<T> type, JsonToken token) {
            super(type);
            this.token = token;
        }

        abstract T read(JsonParser p) throws IOException;

        @Override
        public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            T value = p.currentToken() == token ? read(p) : null;
            if (value == null) {
                @SuppressWarnings("unchecked")
                T refused = (T) ctxt.reportInputMismatch(this, "a %s field holds %s",
                        handledType().getSimpleName(), p.currentToken());
                return refused;
            }
            return value;
        }
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
     * Deserializes CBOR bytes this node persisted into the given type, leniently: stores written
     * before the wire encoding still open. Bytes from the network go through {@link #fromWire} or
     * {@link #fromDocument}.
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
            // HLC round five, 2026-11-08 (decision E8): no tag but the big-number tags Jackson
            // writes, and no invalid or overlong UTF-8 in any text string, before Jackson reads.
            WellFormedCbor.check(bytes, MAX_NESTING_DEPTH);
            return cbor.readerFor(type).withAttribute(CanonicalStamps.RAW_INPUT, bytes).readValue(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to deserialize " + type.getName(), e);
        }
    }

    /**
     * Reads a protocol record from the network (HLC round six, 2026-11-08, owner rulings F1 and
     * F3): the bytes hold one item in the wire encoding, the record maps from them with every
     * coercion off, and its encoding is exactly the bytes received. Every relabelling of a signed
     * record (a wider head, a field out of order or unknown, a null for an absent field, an
     * integer sent as a decimal or as text) is therefore undecodable, and a signature checked over
     * the record's encoding covers the bytes that arrived.
     *
     * @param bytes the CBOR encoding as received
     * @param type  the protocol record type
     * @param <T>   the protocol record type
     * @return the record
     * @throws UncheckedIOException when the bytes are not the wire encoding of a {@code type}
     */
    public <T> T fromWire(byte[] bytes, Class<T> type) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(type, "type");
        try {
            WellFormedCbor.checkCanonical(bytes, MAX_NESTING_DEPTH);
            T value = wire.readerFor(type).withAttribute(CanonicalStamps.RAW_INPUT, bytes).readValue(bytes);
            if (value == null) {
                throw new IOException("a protocol record is not null");
            }
            if (!Arrays.equals(cbor.writeValueAsBytes(value), bytes)) {
                throw new IOException("the bytes are not the wire encoding of the record they decode to (F3)");
            }
            return value;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode " + type.getName() + " from the wire", e);
        }
    }

    /**
     * Reads an application document, such as an entry payload, from the network (HLC round six,
     * 2026-11-08): the bytes hold one item in the wire encoding (rules S1 to S12, tags 2, 3 and 4
     * included under owner ruling F2), and the value maps leniently, unknown fields ignored. A
     * type annotated {@link WireRecord} is a protocol record that travels as a document and reads
     * as {@link #fromWire} reads.
     *
     * @param bytes the CBOR encoding as received
     * @param type  the target type
     * @param <T>   the target type
     * @return the value
     * @throws UncheckedIOException when the bytes are not in the wire encoding
     */
    public <T> T fromDocument(byte[] bytes, Class<T> type) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(type, "type");
        if (type.isAnnotationPresent(WireRecord.class)) {
            return fromWire(bytes, type);
        }
        try {
            WellFormedCbor.checkCanonical(bytes, MAX_NESTING_DEPTH);
            return cbor.readerFor(type).withAttribute(CanonicalStamps.RAW_INPUT, bytes).readValue(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode " + type.getName() + " from a document", e);
        }
    }

    /**
     * Checks that bytes hold one item in the wire encoding (rules S1 to S12), as an embedded item
     * of a persistent-tree node must (HLC round six, 2026-11-08, change JD-4).
     *
     * @param bytes the encoded item
     * @throws UncheckedIOException naming the first rule the bytes break
     */
    public static void checkDocument(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            WellFormedCbor.checkCanonical(bytes, MAX_NESTING_DEPTH);
        } catch (IOException e) {
            throw new UncheckedIOException("not a document in the wire encoding", e);
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
