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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The live index's open-addressed table (RE-M-04) against a hash map model: random puts, replacements and removals,
 * through growth and shrinking, with ids drawn from a small pool so probes collide and removals shift runs back.
 */
class LiveIndexTest {

    private static final AgentId ISSUER = AgentId.parse("zP/probe");

    @Test
    void theTableAgreesWithAMapThroughGrowthShrinkingAndRemovals() {
        for (long seed = 1; seed <= 3; seed++) {
            Random random = new Random(seed);
            LiveIndex index = new LiveIndex();
            Map<EntryId, EntryRecord> model = new HashMap<>();
            int maxSize = 0;
            for (int step = 0; step < 40_000; step++) {
                // grow for a while, then drain, so the table both doubles and halves
                boolean growing = (step / 5_000) % 2 == 0;
                EntryId entryId = EntryId.of("id-" + random.nextInt(3_000));
                if (random.nextInt(10) < (growing ? 7 : 2)) {
                    EntryRecord record = record(entryId, step);
                    index.put(record);
                    model.put(entryId, record);
                } else {
                    index.remove(entryId);
                    model.remove(entryId);
                }
                maxSize = Math.max(maxSize, model.size());
                if (step % 97 == 0 || index.size() != model.size()) {
                    assertThat(index.size()).isEqualTo(model.size());
                    for (EntryId probe : List.of(entryId, EntryId.of("id-" + random.nextInt(3_000)))) {
                        assertThat(index.get(probe)).isSameAs(model.get(probe));
                    }
                    Map<EntryId, EntryRecord> held = new HashMap<>();
                    for (EntryRecord record : index.values()) {
                        assertThat(held.put(record.entryId(), record)).as("one record per id").isNull();
                    }
                    assertThat(held).isEqualTo(model);
                }
            }
            assertThat(maxSize).as("the model grew well past the first table").isGreaterThan(1_000);
        }
    }

    private static EntryRecord record(EntryId entryId, int step) {
        return new EntryRecord(entryId, SpaceId.local("probe"), "T#1", new byte[0], null, ISSUER,
                new HlcTimestamp(step, 0, "p"), new LeaseInfo(ISSUER, step + 1, LeaseKind.WRITE), Map.of(), null);
    }
}
