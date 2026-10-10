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
package ai.badmonkey.agentspaces.api.entry;

import static org.assertj.core.api.Assertions.assertThat;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A signed structure's maps keep the order they arrived in (issue #16). The
 * record signature covers the tags in iteration order, so a copy that iterated
 * in a per-JVM salted order ({@code Map.copyOf}) made a receiver rebuild the
 * signed view in another order than the sender's and refuse honest records
 * with two or more tags about half the time, across processes. In one JVM the
 * salt is shared, so the proof here is that two inputs in opposite orders stay
 * in opposite orders: a salted copy would give both the same order.
 */
class EntryRecordTagOrderTest {

    private static final PeerId PEER = PeerId.of("z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");

    private static EntryRecord record(Map<String, String> tags) {
        AgentId issuer = new AgentId(PEER, "writer");
        return new EntryRecord(EntryId.newId(), SpaceId.local("tasks"), "T#v1", new byte[] {1},
                null, issuer, new HlcTimestamp(1L, 0, "n"),
                new LeaseInfo(issuer, 2L, LeaseKind.WRITE), tags, null);
    }

    private static Map<String, String> ordered(String... keysAndValues) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    @Test
    void anEntryRecordKeepsItsTagsInTheOrderTheyArrivedIn() {
        List<String> ab = new ArrayList<>(record(ordered("a", "1", "b", "2", "c", "3")).tags().keySet());
        List<String> cba = new ArrayList<>(record(ordered("c", "3", "b", "2", "a", "1")).tags().keySet());
        assertThat(ab).containsExactly("a", "b", "c");
        assertThat(cba).containsExactly("c", "b", "a");
    }

    @Test
    void aCapabilityAdvertisementKeepsItsParametersInTheOrderTheyArrivedIn() {
        GroupId group = GroupId.of("zGroup");
        CapabilityAdvertisement ad = new CapabilityAdvertisement("aspace://x/cap/y", PEER, group,
                Instant.EPOCH, Duration.ofMinutes(1), "aspace:cap/y", "0.1", "pipe",
                ordered("role", "leader", "members", "3", "term", "7"), ordered("z", "1", "a", "2"));
        assertThat(new ArrayList<>(ad.parameters().keySet())).containsExactly("role", "members", "term");
        assertThat(new ArrayList<>(ad.costHints().keySet())).containsExactly("z", "a");
    }
}
