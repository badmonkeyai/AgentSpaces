/*
 * Copyright 2026 Bad Monkey, Inc. All rights reserved.
 */

package ai.badmonkey.agentspaces.constraints;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An event keeps a record's null component (found 2026-11-07: a gateway's response with no error killed the gate on
 * every write), and a mapped record is typed by the logical type it carries (found 2026-11-07: a flow step's
 * Assessment was an event of type MappedRecord, so no rule book could name it).
 */
class EventTest {

    public record Response(String id, String text, String error) {
    }

    /** The platform's shape of a record written for a type the writer has no class of; the core only knows the shape. */
    public record MappedRecord(int v, String type, String by, String key, Map<String, Object> fields) {
    }

    static final String MAPPED_WIRE = "ai.badmonkey.bma.agent.support.MappedRecord#v1";

    @Test
    void aNullFieldIsKeptNotRefused() {
        Event e = Event.of(new Response("r-1", "answer", null), Response.class.getName(), Map.of("requestId", "r-1"), Instant.EPOCH, Event.Kind.WRITTEN);
        assertThat(e.fields()).containsEntry("id", "r-1").containsEntry("text", "answer").containsKey("error");
        assertThat(e.fields().get("error")).isNull();
        assertThat(e.type()).isEqualTo("Response");
        Map<String, Object> raw = new HashMap<>();
        raw.put("x", null);
        Event fromMap = new Event("T", "a.T", "e-1", Instant.EPOCH, raw, null, Event.Kind.CLAIMED);
        assertThat(fromMap.fields()).containsKey("x");
        assertThat(fromMap.tags()).isEmpty();
        assertThatThrownBy(() -> fromMap.fields().put("y", 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aMappedRecordIsTypedByItsLogicalType() {
        MappedRecord mapped = new MappedRecord(1, "acme.Assessment", "assess-step", "c-7", Map.of("claimId", "c-7", "verdict", "approve"));
        Event e = Event.of(mapped, MAPPED_WIRE, Map.of("type", "acme.Assessment"), Instant.EPOCH, Event.Kind.WRITTEN);
        assertThat(e.type()).isEqualTo("Assessment");
        assertThat(e.wireType()).as("the wire type stays").isEqualTo(MAPPED_WIRE);
        assertThat(e.fields()).containsEntry("claimId", "c-7").containsEntry("verdict", "approve").containsEntry("key", "c-7")
                .doesNotContainKeys("v", "by", "fields", "type");
        assertThat(e.keyValue("field:claimId")).isEqualTo("c-7");
        Event fromMap = Event.of(Map.of("v", 1, "type", "Assessment", "by", "s", "key", "c-8", "fields", Map.of("key", "given", "verdict", "deny")),
                "a.b.MappedRecord", Map.of(), Instant.EPOCH, Event.Kind.CLAIMED);
        assertThat(fromMap.type()).as("a reader without the class sees the map form").isEqualTo("Assessment");
        assertThat(fromMap.fields()).as("a mapped field named key is kept over the record's key").containsEntry("key", "given").containsEntry("verdict", "deny");
        assertThat(fromMap.kind()).isEqualTo(Event.Kind.CLAIMED);
    }

    @Test
    void onlyTheMappedShapeUnderTheMappedWireTypeIsRetyped() {
        Event noFields = Event.of(Map.of("type", "Assessment", "key", "c-1"), "a.b.MappedRecord", Map.of(), Instant.EPOCH, Event.Kind.WRITTEN);
        assertThat(noFields.type()).as("no fields map: the wire type stands").isEqualTo("MappedRecord");
        assertThat(noFields.fields()).containsEntry("type", "Assessment");
        Event blankType = Event.of(new MappedRecord(1, " ", "s", "c-1", Map.of("a", 1)), MAPPED_WIRE, Map.of(), Instant.EPOCH, Event.Kind.WRITTEN);
        assertThat(blankType.type()).as("a blank logical type: the wire type stands").isEqualTo("MappedRecord");
        Event otherWire = Event.of(new MappedRecord(1, "Assessment", "s", "c-1", Map.of("a", 1)), "a.b.Envelope", Map.of(), Instant.EPOCH, Event.Kind.WRITTEN);
        assertThat(otherWire.type()).as("the shape under another wire type is that type").isEqualTo("Envelope");
        assertThat(otherWire.fields()).containsEntry("type", "Assessment").containsKey("fields");
    }
}
