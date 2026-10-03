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
import ai.badmonkey.agentspaces.common.id.AgentId;
import org.junit.jupiter.api.Test;

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

    @Test
    void theKindsCoverAnEntrysLifecycle() {
        assertThat(SpaceEvent.Kind.values()).containsExactly(SpaceEvent.Kind.WRITTEN,
                SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED, SpaceEvent.Kind.EXPIRED,
                SpaceEvent.Kind.REAPPEARED);
    }
}
