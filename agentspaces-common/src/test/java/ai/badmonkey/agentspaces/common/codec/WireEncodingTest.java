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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * HLC round six, 2026-11-08 (owner rulings F1, F2, F3): the wire encoding. Every document a
 * receiver reads from the network obeys the item rules S1 to S12, a protocol record also decodes
 * without coercion and re-encodes to exactly the bytes it arrived as, and the encoder writes one
 * form per value (JC-1 to JC-4). The vectors are {@code tools/wire-encoding/vectors.json}.
 */
class WireEncodingTest {

    private static final Path VECTORS = Path.of("..", "tools", "wire-encoding", "vectors.json");
    private static final HexFormat HEX = HexFormat.of();

    private final CborCodec codec = CborCodec.defaultCodec();

    static List<JsonNode> vectors(String carrier) throws Exception {
        assertThat(VECTORS).as("the wire-encoding vectors ship with the repository").exists();
        JsonNode root = new ObjectMapper().readTree(Files.readAllBytes(VECTORS));
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode v : root.get("vectors")) {
            if (v.get("carrier").asText().equals(carrier)) {
                out.add(v);
            }
        }
        return out;
    }

    @TestFactory
    Stream<DynamicTest> everyDocumentVectorDecodesAsItsVerdictSays() throws Exception {
        return vectors("document").stream().map(v -> DynamicTest.dynamicTest(
                v.get("id").asText() + " " + v.get("verdict").asText() + ": " + v.get("why").asText(), () -> {
                    byte[] bytes = HEX.parseHex(v.get("hex").asText());
                    if (v.get("verdict").asText().equals("accept")) {
                        assertThat(codec.fromDocument(bytes, Object.class)).isNotNull();
                    } else {
                        assertThatThrownBy(() -> codec.fromDocument(bytes, Object.class))
                                .isInstanceOf(UncheckedIOException.class);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> everyAcceptedDocumentReencodesToItsOwnBytes() throws Exception {
        // F2: a bignum, a decimal, a float32, a NaN, chunked text and a sorted map each come back as
        // the identical bytes; a record read as a map is the one value whose order a map forgets
        return vectors("document").stream()
                .filter(v -> v.get("verdict").asText().equals("accept"))
                .filter(v -> !v.get("id").asText().equals("doc-java-record"))
                .map(v -> DynamicTest.dynamicTest(v.get("id").asText(), () -> {
                    byte[] bytes = HEX.parseHex(v.get("hex").asText());
                    assertThat(HEX.formatHex(codec.toBytes(codec.fromDocument(bytes, Object.class))))
                            .isEqualTo(v.get("hex").asText());
                }));
    }

    @Test
    void aBigIntegerInsideTheLongRangeIsAPlainInteger() {
        // JC-1
        assertThat(hex(BigInteger.ONE)).isEqualTo("01");
        assertThat(hex(BigInteger.valueOf(Long.MIN_VALUE))).isEqualTo("3b7fffffffffffffff");
        assertThat(hex(BigInteger.ONE.shiftLeft(63))).isEqualTo("c249008000000000000000");
        assertThat(hex(BigInteger.ONE.shiftLeft(63).negate().subtract(BigInteger.ONE)))
                .isEqualTo("c349008000000000000001");
        assertThat(hex(BigInteger.ONE.shiftLeft(64))).isEqualTo("c249010000000000000000");
    }

    @Test
    void aBigDecimalIsWrittenNormalizedUnderShortestHeads() {
        // JC-2
        assertThat(hex(new BigDecimal("1.50"))).isEqualTo("c482200f");
        assertThat(hex(new BigDecimal("0.00"))).isEqualTo("c4820000");
        assertThat(hex(new BigDecimal("1E+3"))).isEqualTo("c4820301");
        assertThat(hex(new BigDecimal("1000"))).isEqualTo("c4820301");
        assertThat(hex(new BigDecimal("-12.5"))).isEqualTo("c48220387c");
        assertThat(hex(new BigDecimal(BigInteger.ONE.shiftLeft(31), 3))).isEqualTo("c482221a80000000");
        assertThat(hex(new BigDecimal(BigInteger.ONE.shiftLeft(70), 5)))
                .isEqualTo("c48224c249400000000000000000");
        assertThatThrownBy(() -> codec.toBytes(new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE)))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void aNanIsWrittenAsTheOneNanOfItsWidth() {
        // JC-3: arithmetic leaves other bits in a NaN, and the encoding holds one per width
        double negativeQuiet = Double.longBitsToDouble(0xfff8000000000000L);
        float payloadNan = Float.intBitsToFloat(0x7fc00001);
        assertThat(hex(negativeQuiet)).isEqualTo("fb7ff8000000000000");
        assertThat(hex(payloadNan)).isEqualTo("fa7fc00000");
        assertThat(hex(List.of(negativeQuiet, payloadNan))).isEqualTo("82fb7ff8000000000000fa7fc00000");
        assertThat(hex(new Bid(negativeQuiet))).isEqualTo("bf63626964fb7ff8000000000000ff");
    }

    record Bid(double bid) {
    }

    record Buckets(double[] doubles, float[] floats) {
    }

    @Test
    void aNanInAPrimitiveArrayIsTheOneNanOfItsWidth() {
        // HLC round six, 2026-11-08 (JC-3): Jackson writes double[] and float[] through its own
        // array serializers, which keep the raw bits, so a record holding a NaN its arithmetic left
        // (a push-sum histogram's buckets) once failed the strict read of every Java receiver
        double negativeQuiet = Double.longBitsToDouble(0xfff8000000000000L);
        float payloadNan = Float.intBitsToFloat(0x7fc00001);
        assertThat(hex(new double[]{negativeQuiet, 1.5})).isEqualTo("82fb7ff8000000000000fb3ff8000000000000");
        assertThat(hex(new float[]{payloadNan, 1.5f})).isEqualTo("82fa7fc00000fa3fc00000");
        Buckets buckets = new Buckets(new double[]{negativeQuiet}, new float[]{payloadNan});
        Buckets read = codec.fromWire(codec.toBytes(buckets), Buckets.class);
        assertThat(read.doubles()).hasSize(1);
        assertThat(Double.isNaN(read.doubles()[0])).isTrue();
        assertThat(Float.isNaN(read.floats()[0])).isTrue();
    }

    enum Role { RENDEZVOUS, RELAY }

    record Roles(Set<Role> roles) {
    }

    @Test
    void aSetIsWrittenInTheOrderOfItsElementsEncodings() {
        // JC-4: Set.copyOf iterates in a per-JVM order, so the encoder orders the elements itself
        Set<Role> rendezvousFirst = new LinkedHashSet<>(List.of(Role.RENDEZVOUS, Role.RELAY));
        assertThat(hex(new Roles(rendezvousFirst)))
                .isEqualTo(hex(new Roles(new LinkedHashSet<>(List.of(Role.RELAY, Role.RENDEZVOUS)))))
                .contains("82" + "6552454c4159" + "6a52454e44455a564f5553");
        assertThat(hex(new LinkedHashSet<>(List.of(300, 2, "b", "a")))).isEqualTo("8402616161621901" + "2c");
    }

    record Envelope(int ver, String group, String to, HlcTimestamp stamp, byte[] body) {
    }

    record Probe(long nonce, @com.fasterxml.jackson.annotation.JsonInclude(
            com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) byte[] root) {
    }

    record Card(String id, Instant issued, Duration ttl) {
    }

    record Tagged(Map<String, String> tags) {
    }

    record Listed(List<Card> cards, List<String> names) {
    }

    private static final HlcTimestamp STAMP = new HlcTimestamp(1_800_000_000_000L, 0, "n1");

    @Test
    void aProtocolRecordReadsFromItsOwnBytes() {
        Envelope envelope = new Envelope(3, "g", null, STAMP, new byte[]{1, 2});
        assertThat(codec.fromWire(codec.toBytes(envelope), Envelope.class).ver()).isEqualTo(3);
        Card card = new Card("c", Instant.parse("2027-01-15T08:00:00.123Z"), Duration.ofMinutes(10));
        assertThat(codec.fromWire(codec.toBytes(card), Card.class)).isEqualTo(card);
        assertThat(codec.fromWire(codec.toBytes(new Tagged(Map.of("b", "1", "10", "2", "aa", "3"))),
                Tagged.class).tags()).containsEntry("10", "2");
    }

    @Test
    void aProtocolRecordInAnyOtherFormIsUndecodable() {
        // F3: the strict read plus the byte-for-byte compare refuse every relabel of a record
        String canonical = hex(new Envelope(3, "g", null, STAMP, new byte[]{1, 2}));
        String ver = "63766572" + "03";
        String group = "6567726f7570" + "6167";
        assertThat(codec.fromWire(HEX.parseHex(canonical), Envelope.class).ver()).isEqualTo(3);
        Map<String, String> relabels = new java.util.LinkedHashMap<>();
        relabels.put("a wide head (S3)", canonical.replace(ver, "637665721803"));
        relabels.put("ver as the text 3 (F1)", canonical.replace(ver, "637665726133"));
        relabels.put("ver as a double (F1)", canonical.replace(ver, "63766572fb4008000000000000"));
        relabels.put("ver as tag 4 (F1)", canonical.replace(ver, "63766572c4820003"));
        relabels.put("ver as a half float (S10)", canonical.replace(ver, "63766572f94200"));
        relabels.put("ver as a float32 (F1)", canonical.replace(ver, "63766572fa40400000"));
        relabels.put("a trailing byte (S1)", canonical + "00");
        relabels.put("an unknown field (P1)", canonical.replace("bf" + ver, "bf6178f6" + ver));
        relabels.put("fields out of order (P1)", canonical.replace(ver + group, group + ver));
        relabels.put("a duplicate key (S8)", canonical.replace(ver, ver + ver));
        relabels.put("to absent (P1)", canonical.replace("62746ff6", ""));
        relabels.put("to undefined (S11)", canonical.replace("62746ff6", "62746ff7"));
        relabels.put("a definite map (S8)", "a5" + canonical.substring(2, canonical.length() - 2));
        relabels.put("an indefinite byte string (S5)", canonical.replace("420102", "5f41014102ff"));
        relabels.put("an indefinite short text (S6)", canonical.replace("6167", "7f6167ff"));
        relabels.put("an integer key (S8)", canonical.replace("bf" + ver, "bf01f6" + ver));
        for (Map.Entry<String, String> relabel : relabels.entrySet()) {
            assertThat(relabel.getValue()).as(relabel.getKey()).isNotEqualTo(canonical);
            assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(relabel.getValue()), Envelope.class))
                    .as(relabel.getKey()).isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void typedFieldsRefuseEveryCoercion() {
        // F1: an integer where text belongs, base64 text where bytes belong, epoch seconds for an Instant,
        // a field omitted when null written as null, an always-present field left out, a negative duration
        String tagged = hex(new Tagged(Map.of("b", "x")));
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(tagged.replace("61626178", "61620a")), Tagged.class))
                .isInstanceOf(UncheckedIOException.class);
        String optional = hex(new Probe(7, new byte[]{1}));
        assertThat(codec.fromWire(HEX.parseHex(optional), Probe.class).root()).containsExactly(1);
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(optional.replace("4101ff", "6441513d3dff")),
                Probe.class)).as("base64 text for bytes").isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(optional.replace("4101ff", "f6ff")),
                Probe.class)).as("null for a field omitted when null").isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(optional.replace("656e6f6e636507", "656e6f6e6365f6")),
                Probe.class)).as("null for a long").isInstanceOf(UncheckedIOException.class);
        String issued = text("2026-01-01T00:00:00Z");
        String card = hex(new Card("c", Instant.parse("2026-01-01T00:00:00Z"), Duration.ofMinutes(10)));
        assertThat(codec.fromWire(HEX.parseHex(card), Card.class).ttl()).isEqualTo(Duration.ofMinutes(10));
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(card.replace(issued, "1a6955b900")), Card.class))
                .as("epoch seconds for an Instant").isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(card.replace(issued,
                text("2026-01-01T00:00:00.000Z"))), Card.class))
                .as("an Instant Java does not print").isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(card.replace(text("PT10M"), text("PT600S"))), Card.class))
                .as("a Duration Java does not print").isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex(card.replace("6374746c" + text("PT10M"), "")), Card.class))
                .as("an always-present field left out").isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(codec.toBytes(new Card("c", Instant.EPOCH, Duration.ofMinutes(-10))),
                Card.class)).as("a negative duration").isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void aNullInsideAMapFieldIsUndecodable() {
        // HLC round six, 2026-11-08 (P2, owner ruling F1): a map<String, X> protocol field holds
        // values of kind X and never F6, though a null re-encodes to the bytes it arrived as
        Map<String, String> nullTag = new java.util.LinkedHashMap<>();
        nullTag.put("k", null);
        byte[] bytes = codec.toBytes(new Tagged(nullTag));
        assertThat(HEX.formatHex(bytes)).isEqualTo("bf6474616773bf616bf6ffff");
        assertThatThrownBy(() -> codec.fromWire(bytes, Tagged.class)).isInstanceOf(UncheckedIOException.class);
        assertThat(codec.fromWire(codec.toBytes(new Tagged(Map.of("k", "v"))), Tagged.class).tags())
                .containsEntry("k", "v");
    }

    @Test
    void aNullInsideAListOrASetFieldIsUndecodable() {
        // HLC round seven, 2026-11-08 (P2, owner ruling F1): a list<X> or set<X> protocol field
        // holds items of kind X, so a null element (SemResponse.hits, AppendEntries.entries,
        // RtcSignal.fp) makes the carrier undecodable, as the clients already refuse it
        Card card = new Card("c", Instant.EPOCH, Duration.ofMinutes(10));
        List<Card> nullCard = new ArrayList<>(List.of(card));
        nullCard.add(null);
        List<String> nullName = new ArrayList<>(List.of("a"));
        nullName.add(null);
        for (Listed listed : List.of(new Listed(nullCard, List.of()), new Listed(List.of(), nullName))) {
            byte[] bytes = codec.toBytes(listed);
            assertThatThrownBy(() -> codec.fromWire(bytes, Listed.class)).isInstanceOf(UncheckedIOException.class);
        }
        Listed honest = new Listed(List.of(card), List.of("a"));
        assertThat(codec.fromWire(codec.toBytes(honest), Listed.class)).isEqualTo(honest);
        assertThat(codec.fromWire(codec.toBytes(new Listed(null, null)), Listed.class)).isEqualTo(new Listed(null, null));
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex("bf65726f6c657381f6ff"), Roles.class))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void anInstantOutsideTheYearsZeroToNineThousandNineHundredNinetyNineIsUndecodable() {
        // HLC round six, 2026-11-08 (P2): an Instant field holds a year from 0000 to 9999, the
        // years every implementation's date type holds; Java prints a later year with a sign
        for (String accepted : List.of("0000-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z")) {
            Card card = new Card("c", Instant.parse(accepted), Duration.ofMinutes(10));
            assertThat(codec.fromWire(codec.toBytes(card), Card.class)).as(accepted).isEqualTo(card);
        }
        for (String refused : List.of("+10000-01-01T00:00:00Z", "-0001-12-31T23:59:59Z")) {
            byte[] bytes = codec.toBytes(new Card("c", Instant.parse(refused), Duration.ofMinutes(10)));
            assertThatThrownBy(() -> codec.fromWire(bytes, Card.class)).as(refused)
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void aNullDocumentIsNoProtocolRecord() {
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex("f6"), Card.class))
                .isInstanceOf(UncheckedIOException.class);
    }

    @TestFactory
    Stream<DynamicTest> everyVectorMeetsTheItemRulesAsItsStructuralVerdictSays() throws Exception {
        // HLC round six, 2026-11-08 (JD-4): checkDocument is the item check alone, with no Jackson
        // behind it, as a persistent-tree node's embedded items rely on it
        List<JsonNode> all = new ArrayList<>();
        for (String carrier : List.of("document", "frame", "space-delta", "pref", "signed-peer-ad")) {
            all.addAll(vectors(carrier));
        }
        assertThat(all).hasSize(165);
        return all.stream().map(v -> DynamicTest.dynamicTest(
                v.get("id").asText() + " " + v.get("structural").asText(), () -> {
                    byte[] bytes = HEX.parseHex(v.get("hex").asText());
                    if (v.get("structural").asText().equals("accept")) {
                        CborCodec.checkDocument(bytes);
                    } else {
                        assertThatThrownBy(() -> CborCodec.checkDocument(bytes))
                                .isInstanceOf(UncheckedIOException.class);
                    }
                }));
    }

    @Test
    void aTagNeverStandsInAMapKeyAndAppValuesKeepTheBigNumberTags() {
        // F2: tags 2, 3 and 4 decode as application values, never as a key, never as a protocol field
        byte[] taggedKey = HEX.parseHex("bfc2410101ff");
        assertThatThrownBy(() -> codec.fromBytes(taggedKey, Object.class)).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromDocument(taggedKey, Object.class)).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromWire(taggedKey, Object.class)).isInstanceOf(UncheckedIOException.class);
        Map<?, ?> amounts = codec.fromDocument(HEX.parseHex("bf616dc482200f616ec249010000000000000000ff"), Map.class);
        assertThat(amounts.get("m")).isEqualTo(new BigDecimal("1.5"));
        assertThat(amounts.get("n")).isEqualTo(BigInteger.ONE.shiftLeft(64));
        assertThatThrownBy(() -> codec.fromWire(HEX.parseHex("bf656e6f6e6365c24101ff"), Probe.class))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void aBignumHoldsAtMost1000BytesInEveryReadAndTheEncoderWritesNoLonger() {
        // HLC round seven, 2026-11-08 (S9): Jackson's StreamReadConstraints read a number of at most
        // 1000 bytes, the byte string of a tag 2 or 3 as written, a leading 00 included
        BigInteger widest = BigInteger.ONE.shiftLeft(7991).add(BigInteger.ONE);
        assertThat(widest.toByteArray()).hasSize(1000);
        assertThat(codec.fromDocument(codec.toBytes(widest), Object.class)).isEqualTo(widest);
        BigInteger wider = BigInteger.ONE.shiftLeft(7999);
        assertThat(wider.toByteArray()).hasSize(1001);
        assertThatThrownBy(() -> codec.toBytes(wider)).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.toBytes(wider.negate())).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.toBytes(new BigDecimal(wider.add(BigInteger.ONE), 1)))
                .isInstanceOf(UncheckedIOException.class);
        byte[] bignum = HEX.parseHex("c25903e9" + "01" + "11".repeat(1000));
        assertThatThrownBy(() -> CborCodec.checkDocument(bignum)).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> codec.fromBytes(bignum, Object.class)).isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void persistedStateKeepsTheLenientRead() {
        // stores a 0.5.0 node wrote before JC-1 and JC-2 still open: BigInteger.ONE under tag 2, 1.50 with its scale
        assertThat(codec.fromBytes(HEX.parseHex("bf6176c24101ff"), Map.class).get("v")).isEqualTo(BigInteger.ONE);
        assertThat(codec.fromBytes(HEX.parseHex("bf6176c482211896ff"), Map.class).get("v"))
                .isEqualTo(new BigDecimal("1.50"));
    }

    private String hex(Object value) {
        return HEX.formatHex(codec.toBytes(value));
    }

    private static String text(String s) {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return (b.length < 24 ? HEX.formatHex(new byte[]{(byte) (0x60 | b.length)})
                : HEX.formatHex(new byte[]{0x78, (byte) b.length})) + HEX.formatHex(b);
    }
}
