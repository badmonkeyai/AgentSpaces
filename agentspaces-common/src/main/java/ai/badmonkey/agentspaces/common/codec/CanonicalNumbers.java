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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * The number forms of the wire encoding (HLC round six, 2026-11-08, changes JC-1 to JC-3), which
 * make the bytes of a number a function of its value, so Python, TypeScript and Ruby re-encode
 * what Java writes to the identical bytes:
 *
 * <ul>
 *   <li>a {@link BigInteger} inside the {@code long} range is a plain integer, and only a value
 *       outside it is a tag 2 or 3 bignum (Jackson writes {@code BigInteger.ONE} as tag 2);</li>
 *   <li>a {@link BigDecimal} is tag 4 over its value stripped of trailing zeros (zero as
 *       {@code [0, 0]}), the exponent inside -(2^31-1) to 2^31-1 and the mantissa under its
 *       shortest head (Jackson keeps the scale and writes a mantissa of 32 to 63 bits under an
 *       eight-byte head);</li>
 *   <li>every NaN is the one NaN of its width, {@code Double.NaN} or {@code Float.NaN} (Jackson
 *       writes the raw bits that arithmetic left), in a {@code double[]} and a {@code float[]}
 *       as well.</li>
 * </ul>
 */
final class CanonicalNumbers {

    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private CanonicalNumbers() {
    }

    /** The module that installs the number forms on the CBOR writer. */
    static SimpleModule module() {
        SimpleModule module = new SimpleModule("agentspaces-canonical-numbers");
        module.addSerializer(BigInteger.class, new Integers());
        module.addSerializer(BigDecimal.class, new Decimals());
        Doubles doubles = new Doubles();
        module.addSerializer(Double.class, doubles);
        module.addSerializer(double.class, doubles);
        Floats floats = new Floats();
        module.addSerializer(Float.class, floats);
        module.addSerializer(float.class, floats);
        // HLC round six, 2026-11-08 (JC-3): Jackson writes a primitive array through its own
        // serializer, which keeps a NaN's raw bits as the boxed writer once did
        module.addSerializer(double[].class, new DoubleArrays());
        module.addSerializer(float[].class, new FloatArrays());
        return module;
    }

    /**
     * Writes an integer: a plain integer inside the {@code long} range, a Jackson bignum outside it.
     * HLC round seven, 2026-11-08 (S9): a bignum's magnitude holds at most
     * {@link WellFormedCbor#BIGNUM_BYTES} bytes, the most Jackson reads back, so the writer refuses a
     * longer one rather than send what no reader decodes.
     */
    static void writeInteger(BigInteger value, JsonGenerator gen) throws IOException {
        if (value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0) {
            gen.writeNumber(value.longValue());
        } else if (value.abs().toByteArray().length > WellFormedCbor.BIGNUM_BYTES) {
            throw new JsonGenerationException("a bignum holds at most " + WellFormedCbor.BIGNUM_BYTES + " bytes", gen);
        } else {
            gen.writeNumber(value);
        }
    }

    private static final class Integers extends StdSerializer<BigInteger> {
        private static final long serialVersionUID = 1L;

        Integers() {
            super(BigInteger.class);
        }

        @Override
        public void serialize(BigInteger value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            writeInteger(value, gen);
        }
    }

    private static final class Decimals extends StdSerializer<BigDecimal> {
        private static final long serialVersionUID = 1L;

        Decimals() {
            super(BigDecimal.class);
        }

        @Override
        public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            BigDecimal stripped;
            try {
                stripped = value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
            } catch (ArithmeticException e) {
                throw provider.mappingException("a decimal whose exponent leaves an int: %s", value);
            }
            if (stripped.scale() == Integer.MIN_VALUE) {
                throw provider.mappingException("a decimal exponent lies inside -(2^31-1) to 2^31-1: %s", value);
            }
            if (!(gen instanceof CBORGenerator cbor)) {
                gen.writeNumber(stripped);
                return;
            }
            cbor.writeTag(4);
            cbor.writeStartArray(null, 2);
            cbor.writeNumber(-stripped.scale());
            writeInteger(stripped.unscaledValue(), cbor);
            cbor.writeEndArray();
        }
    }

    private static final class Doubles extends StdSerializer<Double> {
        private static final long serialVersionUID = 1L;

        Doubles() {
            super(Double.class);
        }

        @Override
        public void serialize(Double value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeNumber(value.isNaN() ? Double.NaN : value);
        }
    }

    private static final class DoubleArrays extends StdSerializer<double[]> {
        private static final long serialVersionUID = 1L;

        DoubleArrays() {
            super(double[].class);
        }

        @Override
        public void serialize(double[] value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartArray(value, value.length);
            for (double element : value) {
                gen.writeNumber(Double.isNaN(element) ? Double.NaN : element);
            }
            gen.writeEndArray();
        }
    }

    private static final class FloatArrays extends StdSerializer<float[]> {
        private static final long serialVersionUID = 1L;

        FloatArrays() {
            super(float[].class);
        }

        @Override
        public void serialize(float[] value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartArray(value, value.length);
            for (float element : value) {
                gen.writeNumber(Float.isNaN(element) ? Float.NaN : element);
            }
            gen.writeEndArray();
        }
    }

    private static final class Floats extends StdSerializer<Float> {
        private static final long serialVersionUID = 1L;

        Floats() {
            super(Float.class);
        }

        @Override
        public void serialize(Float value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeNumber(value.isNaN() ? Float.NaN : value);
        }
    }
}
