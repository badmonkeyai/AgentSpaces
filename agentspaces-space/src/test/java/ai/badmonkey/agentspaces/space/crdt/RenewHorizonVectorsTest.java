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
package ai.badmonkey.agentspaces.space.crdt;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The renewal clamp and lapse horizon vectors of golden.json (srad/replication-by-entry.md RE-L-05, phase 2) against
 * this implementation: each renewal case's expiry is {@code max(current, now + extension)}, and each horizon case's
 * state becomes collectable exactly at its horizon. {@code -Dgolden.required=true} fails, rather than skips, when the
 * file or the vectors are absent.
 */
class RenewHorizonVectorsTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static final AgentId AGENT = AgentId.parse("zP/writer");
    private static Map<String, Object> golden;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required but none of " + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
        if (GOLDEN_REQUIRED) {
            assertThat(golden).containsKeys("renew_clamp_cases", "lapse_horizon_cases");
        }
        assumeTrue(golden.containsKey("renew_clamp_cases"), "golden.json predates the phase 2 vectors");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> cases(String name) {
        return (List<Map<String, Object>>) golden.get(name);
    }

    private static long number(Map<String, Object> row, String key) {
        return ((Number) row.get(key)).longValue();
    }

    @Test
    void everyRenewalCaseSetsTheLaterOfTheCurrentExpiryAndNowPlusTheExtension() {
        assertThat(cases("renew_clamp_cases")).isNotEmpty().allSatisfy(row -> assertThat(
                SpaceStateCrdt.renewedExpiry(number(row, "current"), number(row, "now"), number(row, "extension")))
                .as("%s", row).isEqualTo(number(row, "expiry")));
    }

    @Test
    void everyHorizonCaseIsCollectableFromItsHorizonAndNeverBefore() {
        EntryRecord record = new EntryRecord(EntryId.of("entry-horizon"), SpaceId.local("tasks"), "T#v1",
                new byte[]{1, 2}, null, AGENT, new HlcTimestamp(1L, 0, "seed"),
                new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE), Map.of(), null);
        Dot dot = new Dot("zP", 1);
        assertThat(cases("lapse_horizon_cases")).hasSize(12).allSatisfy(row -> {
            LwwRegister<LeaseInfo> lease = new LwwRegister<>(new HlcTimestamp(1L, 0, "seed"),
                    new LeaseInfo(AGENT, number(row, "expiry"), LeaseKind.WRITE));
            EntryState state = switch ((String) row.get("state")) {
                case "lapsed" -> new EntryState(record, Set.of(dot), Set.of(), lease, false);
                case "withdrawn" -> new EntryState(record, Set.of(dot), Set.of(dot), lease, false);
                case "completed" -> new EntryState(record, Set.of(dot), Set.of(), lease, true);
                default -> throw new AssertionError("unknown state " + row);
            };
            long maxLease = number(row, "max_lease");
            long horizon = number(row, "horizon");
            assertThat(SpaceStateCrdt.collectionHorizon(state, maxLease)).as("%s", row).isEqualTo(horizon);
            assertThat(SpaceStateCrdt.collectable(state, horizon - 1, maxLease)).as("%s", row).isFalse();
            assertThat(SpaceStateCrdt.collectable(state, horizon, maxLease)).as("%s", row).isTrue();
        });
    }

    /**
     * {@code horizon_mode_cases} (srad/replication-by-entry.md RE-H-04, SPEC §7.3 under {@code wire.horizon=1}): every
     * case's horizon and its two instants, under the rule the case names, from the second static form.
     */
    @Test
    void everyHorizonModeCaseMatchesTheRuleItNames() {
        if (GOLDEN_REQUIRED) {
            assertThat(golden).containsKey("horizon_mode_cases");
        }
        assumeTrue(golden.containsKey("horizon_mode_cases"), "golden.json predates the phase 5 vector");
        EntryRecord record = new EntryRecord(EntryId.of("entry-horizon"), SpaceId.local("tasks"), "T#v1",
                new byte[]{1, 2}, null, AGENT, new HlcTimestamp(1L, 0, "seed"),
                new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE), Map.of(), null);
        Dot dot = new Dot("zP", 1);
        assertThat(cases("horizon_mode_cases")).hasSize(64).allSatisfy(row -> {
            LwwRegister<LeaseInfo> lease = new LwwRegister<>(new HlcTimestamp(1L, 0, "seed"),
                    new LeaseInfo(AGENT, number(row, "expiry"), LeaseKind.WRITE));
            EntryState state = switch ((String) row.get("state")) {
                case "lapsed" -> new EntryState(record, Set.of(dot), Set.of(), lease, false);
                case "withdrawn" -> new EntryState(record, Set.of(dot), Set.of(dot), lease, false);
                case "completed" -> new EntryState(record, Set.of(dot), Set.of(), lease, true);
                case "withdrawn_completed" -> new EntryState(record, Set.of(dot), Set.of(dot), lease, true);
                default -> throw new AssertionError("unknown state " + row);
            };
            SpaceStateCrdt.Horizon mode = switch ((String) row.get("mode")) {
                case "legacy" -> SpaceStateCrdt.Horizon.LEGACY;
                case "per-entry" -> SpaceStateCrdt.Horizon.PER_ENTRY;
                default -> throw new AssertionError("unknown mode " + row);
            };
            long maxLease = number(row, "max_lease");
            assertThat(SpaceStateCrdt.collectionHorizon(state, maxLease, mode)).as("%s", row)
                    .isEqualTo(number(row, "horizon"));
            assertThat(SpaceStateCrdt.collectable(state, number(row, "now"), maxLease, mode)).as("%s", row)
                    .isEqualTo(row.get("collectable"));
        });
    }
}
