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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PText;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 6, P2-T-01 and P2-T-03 (srad/agentspaces-pdt2.md §3.9, §6.10.3 rule 3): a map ref of
 * texts writes version 2 heads, and two forked heads that edited one text meet in its union with
 * no {@link PConflict}.
 */
class TextRefTest {

    private static final String NAME = "notes";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    void aMapRefOfTextsWritesVersionTwoHeadsAndForkedEditsMeetInTheirUnion() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        RefFleet.Peer c = fleet.newPeer("c", 3, "a");
        fleet.settle(6);
        Ref<PMap<String, PText>> onA = a.pdt().mapRef(NAME, Codec.of(String.class), PText.codec("alice"));
        Ref<PMap<String, PText>> onB = b.pdt().mapRef(NAME, Codec.of(String.class), PText.codec("bob"));
        Ref<PMap<String, PText>> onC = c.pdt().mapRef(NAME, Codec.of(String.class), PText.codec("carol"));
        PMap<String, PText> v1 = onA.swap(m -> m.assoc("doc", PText.empty("alice").insert(0, "ac")));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onC.heads().isEmpty())).isTrue();
        PRef head = RefFleet.headEntries(c, NAME).stream().map(Space.Entry::value)
                .max(Comparator.comparingLong(PRef::seq)).orElseThrow();
        assertThat(head.v()).as("rule 3: a value that holds a text").isEqualTo(PRef.VERSION_2);
        fleet.settle(2);
        // the fork: a's successor through swap, b's by hand off the same base, issued later
        PMap<String, PText> v2a = onA.swap(m -> m.update("doc", t -> t.insert(1, "b")));
        fleet.settle(1);
        PMap<String, PText> v2b = onB.at(v1.root()).update("doc", t -> t.insert(1, "XY").delete(0, 1));
        RefFleet.writeHead(b, NAME, PRef.KIND_MAP, v2b.root(), v1.root(), 2, v2b.count(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onC.heads().heads().size() == 2
                && fleet.allHold(v2a.root()) && fleet.allHold(v2b.root()))).isTrue();
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        onC.onConflict(conflicts::add);
        PMap<String, PText> merged = onC.deref();
        assertThat(merged.get("doc").orElseThrow().toString()).isEqualTo("bXYc");
        assertThat(conflicts).as("the union decides, so no conflict").isEmpty();
        PMap<String, PText> v3 = onC.swap(m -> m.update("doc", t -> t.insert(4, "!")));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onA.heads().roots().equals(List.of(v3.root())))).isTrue();
        assertThat(onA.deref().get("doc").orElseThrow().toString()).isEqualTo("bXYc!");
    }
}
