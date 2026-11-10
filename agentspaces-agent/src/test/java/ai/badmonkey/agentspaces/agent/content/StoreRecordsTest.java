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
package ai.badmonkey.agentspaces.agent.content;

import ai.badmonkey.agentspaces.common.codec.CanonicalMaps;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 0 (P2-S-02, P2-F-08; srad/agentspaces-pdt2.md §3.2.2 and §6.10.4): the pin request
 * and the receipt are protocol records that travel as entry payloads, read from their own wire
 * encoding alone under the annex I schemas of §6.10.4, so a field a reader does not know makes the
 * payload undecodable and a change of shape takes a new type name.
 */
class StoreRecordsTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final String ROOT = "bafyreigq3u2qzvyq5r6dpq2nqq4cujdxa6wmljnrrjz4ns2nk2o3clj5vy";
    private static final String BASE = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    private static final PeerId REQUESTER = PeerId.of("zRequester");
    private static final PeerId HOLDER = PeerId.of("zHolder");

    private static StorePin pin(Map<String, String> attrs) {
        return new StorePin(StorePin.VERSION, ROOT, StorePin.KIND_TREE, 86400, 2, REQUESTER, List.of(BASE), attrs);
    }

    /** The fields of a pin in the order of §6.10.4, each written as given. */
    private static CanonicalMaps.Verbatim pinFields() {
        CanonicalMaps.Verbatim fields = new CanonicalMaps.Verbatim();
        fields.put("v", 1);
        fields.put("root", ROOT);
        fields.put("kind", "tree");
        fields.put("lease", 86400);
        fields.put("copies", 2);
        fields.put("requester", "zRequester");
        fields.put("haves", List.of(BASE));
        return fields;
    }

    @Test
    void aPinEncodesItsFieldsInSchemaOrderAndOmitsNullAttrs() {
        assertThat(CODEC.toBytes(pin(null))).isEqualTo(CODEC.toBytes(pinFields()));
        CanonicalMaps.Verbatim withAttrs = pinFields();
        withAttrs.put("attrs", new CanonicalMaps.Verbatim(Map.of("purpose", "golden")));
        StorePin pin = pin(Map.of("purpose", "golden"));
        byte[] bytes = CODEC.toBytes(pin);
        assertThat(bytes).isEqualTo(CODEC.toBytes(withAttrs));
        assertThat(CODEC.fromDocument(bytes, StorePin.class)).isEqualTo(pin);
        assertThat(pin.violation()).isEmpty();
        assertThat(pin.tags()).containsExactlyInAnyOrderEntriesOf(Map.of("root", ROOT, "requester", "zRequester"));
    }

    @Test
    void aPinWithAFieldItsSchemaDoesNotNameOrAWrongKindIsUndecodable() {
        CanonicalMaps.Verbatim extra = pinFields();
        extra.put("blind", true);
        CanonicalMaps.Verbatim floatLease = pinFields();
        floatLease.put("lease", 86400.0);
        CanonicalMaps.Verbatim nullAttrs = pinFields();
        nullAttrs.put("attrs", null);
        CanonicalMaps.Verbatim numberAttr = pinFields();
        numberAttr.put("attrs", new CanonicalMaps.Verbatim(Map.of("copies", 3)));
        CanonicalMaps.Verbatim nullHave = pinFields();
        List<String> haves = new ArrayList<>();
        haves.add(null);
        nullHave.put("haves", haves);
        CanonicalMaps.Verbatim textCopies = pinFields();
        textCopies.put("copies", "2");
        for (CanonicalMaps.Verbatim fields : List.of(extra, floatLease, nullAttrs, numberAttr, nullHave, textCopies)) {
            byte[] bytes = CODEC.toBytes(fields);
            assertThatThrownBy(() -> CODEC.fromDocument(bytes, StorePin.class)).as("%s", fields)
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void aPinNamesEveryBoundItBreaks() {
        assertThat(new StorePin(2, ROOT, "tree", 1, 1, REQUESTER, List.of(), null).violation()).hasValue("unsupported StorePin version 2");
        assertThat(new StorePin(1, "root", "tree", 1, 1, REQUESTER, List.of(), null).violation()).hasValue("root is not a CID");
        assertThat(new StorePin(1, ROOT, "file", 1, 1, REQUESTER, List.of(), null).violation()).hasValue("unknown kind file");
        assertThat(new StorePin(1, ROOT, "artifact", 0, 1, REQUESTER, List.of(), null).violation()).hasValue("lease of 0 seconds");
        assertThat(new StorePin(1, ROOT, "artifact", 1, 0, REQUESTER, List.of(), null).violation()).hasValue("copies below 1");
        assertThat(new StorePin(1, ROOT, "tree", 1, 1, REQUESTER, List.of("x"), null).violation()).hasValue("have is not a CID");
        List<String> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(BASE);
        }
        assertThat(new StorePin(1, ROOT, "tree", 1, 1, REQUESTER, nine, null).violation()).hasValue("more than 8 haves");
        Map<String, String> many = new HashMap<>();
        for (int i = 0; i < 33; i++) {
            many.put("k" + i, "v");
        }
        assertThat(pin(many).violation()).hasValue("more than 32 attrs");
        assertThat(pin(Map.of("a", "x".repeat(StorePin.MAX_ATTRS_BYTES))).violation()).hasValue("attrs larger than 4096 bytes");
    }

    @Test
    void aReceiptEncodesInSchemaOrderAndNamesItsBounds() {
        StoreReceipt receipt = new StoreReceipt(StoreReceipt.VERSION, ROOT, HOLDER, 12345, 1792406400000L, true);
        CanonicalMaps.Verbatim fields = new CanonicalMaps.Verbatim();
        fields.put("v", 1);
        fields.put("root", ROOT);
        fields.put("holder", "zHolder");
        fields.put("bytes", 12345);
        fields.put("until", 1792406400000L);
        fields.put("blind", true);
        byte[] bytes = CODEC.toBytes(receipt);
        assertThat(bytes).isEqualTo(CODEC.toBytes(fields));
        assertThat(CODEC.fromDocument(bytes, StoreReceipt.class)).isEqualTo(receipt);
        assertThat(receipt.violation()).isEmpty();
        assertThat(receipt.tags()).containsExactlyInAnyOrderEntriesOf(Map.of("root", ROOT, "holder", "zHolder"));
        fields.put("cost", "free");
        assertThatThrownBy(() -> CODEC.fromDocument(CODEC.toBytes(fields), StoreReceipt.class))
                .isInstanceOf(UncheckedIOException.class);

        assertThat(new StoreReceipt(3, ROOT, HOLDER, 1, 1, false).violation()).hasValue("unsupported StoreReceipt version 3");
        assertThat(new StoreReceipt(1, "x", HOLDER, 1, 1, false).violation()).hasValue("root is not a CID");
        assertThat(new StoreReceipt(1, ROOT, HOLDER, -1, 1, false).violation()).hasValue("negative bytes");
        assertThat(new StoreReceipt(1, ROOT, HOLDER, 1, -1, false).violation()).hasValue("negative until");
    }
}
