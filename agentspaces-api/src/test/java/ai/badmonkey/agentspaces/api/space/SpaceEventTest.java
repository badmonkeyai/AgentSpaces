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
package ai.badmonkey.agentspaces.api.space;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpaceEventTest {

    private static final AgentId ISSUER = AgentId.parse("zP/writer");
    private static final AgentId ACTOR = AgentId.parse("zP/taker");

    @Test
    void theActorDefaultsToTheIssuer() {
        EntryId id = EntryId.newId();

        assertThat(new SpaceEvent<>(SpaceEvent.Kind.WRITTEN, id, "entry", ISSUER).actor())
                .isEqualTo(ISSUER);
        assertThat(new SpaceEvent<>(SpaceEvent.Kind.WRITTEN, id, "entry", ISSUER, null).actor())
                .isEqualTo(ISSUER);
        assertThat(new SpaceEvent<>(SpaceEvent.Kind.TAKEN, id, "entry", ISSUER, ACTOR).actor())
                .isEqualTo(ACTOR);
    }

    @Test
    void kindEntryIdEntryAndIssuerAreRequired() {
        EntryId id = EntryId.newId();

        assertThatThrownBy(() -> new SpaceEvent<>(null, id, "entry", ISSUER))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SpaceEvent<>(SpaceEvent.Kind.EXPIRED, null, "entry", ISSUER))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SpaceEvent<>(SpaceEvent.Kind.COMPLETED, id, null, ISSUER))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SpaceEvent<>(SpaceEvent.Kind.REAPPEARED, id, "entry", null))
                .isInstanceOf(NullPointerException.class);
    }

    /** Issue #16 §9.2: the appended view is nullable; the old constructors supply none. */
    @Test
    void detailsAreOptionalAndTagsComeFromThem() {
        EntryId id = EntryId.newId();
        Space.Entry<String> details = entryView(id, Map.of("region", "eu"));

        SpaceEvent<String> bare = new SpaceEvent<>(SpaceEvent.Kind.WRITTEN, id, "entry", ISSUER);
        assertThat(bare.details()).isNull();
        assertThat(bare.tags()).isEmpty();
        assertThat(new SpaceEvent<>(SpaceEvent.Kind.TAKEN, id, "entry", ISSUER, ACTOR).details())
                .isNull();

        SpaceEvent<String> full = new SpaceEvent<>(SpaceEvent.Kind.WRITTEN, id, "entry", ISSUER,
                null, details);
        assertThat(full.actor()).isEqualTo(ISSUER);
        assertThat(full.details()).isSameAs(details);
        assertThat(full.tags()).containsExactly(Map.entry("region", "eu"));
        assertThat(new SpaceEvent<>(SpaceEvent.Kind.WRITTEN, id, "entry", ISSUER, ACTOR, null)
                .tags()).isEmpty();
        assertThatThrownBy(() -> new SpaceEvent<>(null, id, "entry", ISSUER, ACTOR, details))
                .isInstanceOf(NullPointerException.class);
    }

    private static Space.Entry<String> entryView(EntryId id, Map<String, String> tags) {
        return new Space.Entry<>() {
            @Override public String value() { return "entry"; }
            @Override public EntryId entryId() { return id; }
            @Override public AgentId issuer() { return ISSUER; }
            @Override public Space.Attestation attestation() {
                return Space.Attestation.PEER_ASSERTED;
            }
            @Override public Map<String, String> tags() { return tags; }
            @Override public LeaseInfo lease() {
                return new LeaseInfo(ISSUER, 1_000L, LeaseKind.WRITE);
            }
            @Override public HlcTimestamp issued() { return null; }
        };
    }

    @Test
    void theKindsCoverAnEntrysLifecycle() {
        assertThat(SpaceEvent.Kind.values()).containsExactly(SpaceEvent.Kind.WRITTEN,
                SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED, SpaceEvent.Kind.EXPIRED,
                SpaceEvent.Kind.REAPPEARED);
    }
}
