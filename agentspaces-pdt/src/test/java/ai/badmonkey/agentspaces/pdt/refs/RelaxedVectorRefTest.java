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
import ai.badmonkey.agentspaces.pdt.values.PVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 6, P2-R-01 and P2-R-03 (srad/agentspaces-pdt2.md §3.7, §6.10.3 rule 2): a vector ref
 * whose value is relaxed writes version 2 heads whose relaxed path a reader fetches and indexes,
 * and the same ref writes version 1 again once its value is packed.
 */
class RelaxedVectorRefTest {

    private static final String NAME = "transcript";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static PRef latest(RefFleet.Peer peer) {
        return RefFleet.headEntries(peer, NAME).stream().map(Space.Entry::value)
                .max(Comparator.comparingLong(PRef::seq)).orElseThrow();
    }

    @Test
    void aSplicedVectorWritesVersionTwoHeadsAndAPackedOneVersionOne() throws Exception {
        RefFleet.Peer w = fleet.newPeer("w", 1);
        RefFleet.Peer r = fleet.newPeer("r", 2, "w");
        fleet.settle(12);
        Ref<PVector<String>> onW = w.pdt().vectorRef(NAME, RefFleet.STR);
        Ref<PVector<String>> onR = r.pdt().vectorRef(NAME, RefFleet.STR);
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            lines.add("line " + i);
        }
        PVector<String> v1 = onW.swap(v -> {
            PVector<String> out = v;
            for (String line : lines) {
                out = out.conj(line);
            }
            return out;
        });
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().roots().equals(List.of(v1.root())))).isTrue();
        assertThat(latest(r).v()).as("a dense vector").isEqualTo(PRef.VERSION);

        PVector<String> v2 = onW.swap(v -> v.insertAt(1500, "an inserted line"));
        assertThat(v2.relaxed()).isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().roots().equals(List.of(v2.root())))).isTrue();
        assertThat(latest(r).v()).as("rule 2: a relaxed tree").isEqualTo(PRef.VERSION_2);
        assertThat(latest(r).nodes()).as("the relaxed path rides inline").isNotEmpty();
        lines.add(1500, "an inserted line");
        PVector<String> read = onR.deref();
        assertThat(read.relaxed()).isTrue();
        assertThat(read.nth(1500)).isEqualTo("an inserted line");
        assertThat(read.toList()).isEqualTo(lines);

        PVector<String> v3 = onR.swap(PVector::packed);
        assertThat(v3.relaxed()).isFalse();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onW.heads().roots().equals(List.of(v3.root())))).isTrue();
        assertThat(latest(w).v()).as("the packed form").isEqualTo(PRef.VERSION);
        assertThat(onW.deref().toList()).isEqualTo(lines);
    }
}
