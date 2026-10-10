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

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.util.StdConverter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The canonical order of map entries (ISSUE-CanonicalMaps, wire version 3,
 * TECH-SPEC §1): every map the codec serializes is written with its entries
 * sorted by the RFC 8949 §4.2.1 rule for text keys, shorter UTF-8 encoding
 * first and bytewise between equal lengths, so the signed bytes of a structure
 * are a function of its content and not of the order its author built a map
 * in. Record and bean properties are not maps and keep their declared order.
 * Decoding is unchanged: a decoded map keeps the wire order, which is sorted.
 */
public final class CanonicalMaps {

    /**
     * The key order: the UTF-8 encodings compared by length, then bytewise
     * unsigned. For text keys this is exactly the bytewise order of the encoded
     * CBOR keys RFC 8949 §4.2.1 specifies, and every language implements it the
     * same way, which {@code String.compareTo} (UTF-16 units) does not.
     */
    public static final Comparator<String> KEY_ORDER = (a, b) -> {
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        if (x.length != y.length) {
            return Integer.compare(x.length, y.length);
        }
        return java.util.Arrays.compareUnsigned(x, y);
    };

    private CanonicalMaps() {
    }

    /**
     * Returns a copy of the map with its entries in canonical order. Keys that
     * are not strings are ordered by their string form, which is the form the
     * codec writes them in.
     *
     * @param map the map
     * @param <K> the key type
     * @param <V> the value type
     * @return an insertion-ordered copy, sorted
     */
    public static <K, V> Map<K, V> sorted(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new ArrayList<>(map.entrySet());
        entries.sort((e1, e2) -> KEY_ORDER.compare(String.valueOf(e1.getKey()), String.valueOf(e2.getKey())));
        Map<K, V> out = new LinkedHashMap<>();
        for (Map.Entry<K, V> e : entries) {
            out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    /** The Jackson module that installs the order on every map serializer. */
    static SimpleModule module() {
        SimpleModule module = new SimpleModule("agentspaces-canonical-maps");
        module.setSerializerModifier(new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifyMapSerializer(SerializationConfig config, MapType valueType,
                                                         BeanDescription beanDesc, JsonSerializer<?> serializer) {
                if (Verbatim.class.isAssignableFrom(valueType.getRawClass())) {
                    return serializer; // insertion order: the sorted copy, or a record mirror
                }
                return new StdDelegatingSerializer(new ToSorted());
            }
        });
        return module;
    }

    /**
     * A map the codec writes in insertion order. The sorted copy is one; the
     * other use is a tool or a test mirroring a record's declared component
     * order with a map (the golden generator's wire mirrors). A map a signature
     * covers is never built as one: the records own those, and the codec sorts
     * them.
     */
    public static final class Verbatim extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        /** An empty verbatim map. */
        public Verbatim() {
        }

        /** A verbatim copy of {@code source}, in its iteration order. */
        public Verbatim(Map<String, ?> source) {
            super(source);
        }
    }

    private static final class ToSorted extends StdConverter<Map<?, ?>, Verbatim> {
        @Override
        public Verbatim convert(Map<?, ?> value) {
            Verbatim out = new Verbatim();
            for (Map.Entry<?, ?> e : sorted(value).entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
            return out;
        }
    }
}
