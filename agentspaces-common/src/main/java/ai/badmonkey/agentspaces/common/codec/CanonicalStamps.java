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

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Decodes a stamp from CBOR only when its raw item is canonical (HLC round four, 2026-11-08,
 * decision D3): a definite-length, untagged text string under the shortest length header,
 * whose bytes are shortest-form UTF-8 and whose text {@link HlcTimestamp#parse(String)}
 * accepts. Jackson's CBOR parser decodes overlong UTF-8, drops tags, and joins
 * indefinite-length chunks without a trace, and a frame's signature verifies over the
 * re-encoded envelope, so a relabelled stamp once passed as signed. The deserializer re-encodes
 * the decoded text and compares it with the bytes the item occupies in the input, which
 * every read of {@link CborCodec} hands it as a context attribute.
 */
final class CanonicalStamps {

    /** The context attribute under which {@link CborCodec} passes the bytes being decoded. */
    static final Object RAW_INPUT = new Object();

    private CanonicalStamps() {
    }

    /** The module that decodes every CBOR stamp through the canonical-item check. */
    static SimpleModule module() {
        SimpleModule module = new SimpleModule("agentspaces-canonical-stamps");
        module.addDeserializer(HlcTimestamp.class, new Deserializer());
        return module;
    }

    /** The CBOR item a canonical encoder writes for a text: major type 3, shortest header, UTF-8 bytes. */
    static byte[] textItem(String text) {
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
        int n = utf8.length;
        byte[] header;
        if (n < 24) {
            header = new byte[]{(byte) (0x60 | n)};
        } else if (n < 0x100) {
            header = new byte[]{0x78, (byte) n};
        } else if (n < 0x10000) {
            header = new byte[]{0x79, (byte) (n >>> 8), (byte) n};
        } else {
            header = new byte[]{0x7A, (byte) (n >>> 24), (byte) (n >>> 16), (byte) (n >>> 8), (byte) n};
        }
        byte[] item = Arrays.copyOf(header, header.length + n);
        System.arraycopy(utf8, 0, item, header.length, n);
        return item;
    }

    private static final class Deserializer extends StdDeserializer<HlcTimestamp> {

        private static final long serialVersionUID = 1L;

        Deserializer() {
            super(HlcTimestamp.class);
        }

        @Override
        public HlcTimestamp deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.currentToken() != JsonToken.VALUE_STRING) {
                return ctxt.reportInputMismatch(this, "a stamp is a CBOR text string, got %s", p.currentToken());
            }
            if (!(p instanceof CBORParser cbor) || !(ctxt.getAttribute(RAW_INPUT) instanceof byte[] raw)) {
                // Buffered tokens no longer show the item's bytes, so nothing vouches for them.
                return ctxt.reportInputMismatch(this, "a stamp decodes only from the raw CBOR input");
            }
            if (cbor.getCurrentTag() != -1) {
                return ctxt.reportInputMismatch(this, "a stamp is an untagged text string");
            }
            long offset = p.currentTokenLocation().getByteOffset();
            String text = p.getText();
            byte[] item = textItem(text);
            if (offset < 0 || offset > raw.length - item.length
                    || !Arrays.equals(raw, (int) offset, (int) offset + item.length, item, 0, item.length)) {
                return ctxt.reportInputMismatch(this,
                        "a stamp is a definite-length shortest-form UTF-8 text string");
            }
            try {
                return HlcTimestamp.parse(text);
            } catch (IllegalArgumentException e) {
                return ctxt.reportInputMismatch(this, "%s", e.getMessage());
            }
        }
    }
}
