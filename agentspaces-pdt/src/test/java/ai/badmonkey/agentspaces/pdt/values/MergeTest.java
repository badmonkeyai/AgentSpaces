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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** PD-V-09 and §3.6.1: the key-wise three-way merge and when the resolver is asked. */
class MergeTest {

    private static final Codec<String> STRINGS = Codec.of(String.class);
    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    /** A resolver that records every call and prefers theirs, after the later-HLC rule a ref applies. */
    private static final class Recording implements Merge.Resolver<String, Integer> {
        final List<String> calls = new ArrayList<>();

        @Override
        public Optional<Integer> resolve(String key, Optional<Integer> base, Optional<Integer> mine, Optional<Integer> theirs) {
            calls.add(key + ":" + base.orElse(null) + "/" + mine.orElse(null) + "/" + theirs.orElse(null));
            return theirs;
        }
    }

    @Test
    void oneSideChangesTakeThatSideAndConflictsAskTheResolverOnce() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> base = PMap.empty(store, STRINGS, INTS)
                .assoc("a", 1).assoc("b", 2).assoc("c", 3).assoc("d", 4).assoc("h", 8).assoc("i", 9).assoc("j", 10);
        PMap<String, Integer> mine = base
                .assoc("a", 10)          // changed on my side only
                .dissoc("b")             // removed on my side only
                .assoc("e", 5)           // added on my side only
                .assoc("g", 1)           // added on both sides, differently: conflict
                .dissoc("h")             // removed on my side, changed on theirs: conflict
                .assoc("i", 90)          // changed on both sides to the same value: no conflict
                .dissoc("j");            // removed on both sides: no conflict
        PMap<String, Integer> theirs = base
                .assoc("c", 30)          // changed on their side only
                .dissoc("d")             // removed on their side only
                .assoc("f", 6)           // added on their side only
                .assoc("g", 2)
                .assoc("h", 80)
                .assoc("i", 90)
                .dissoc("j");
        Recording resolver = new Recording();
        PMap<String, Integer> merged = mine.merge3(base, theirs, resolver);
        assertThat(merged.toMap()).isEqualTo(Map.of(
                "a", 10, "c", 30, "e", 5, "f", 6, "g", 2, "h", 80, "i", 90));
        assertThat(resolver.calls).containsExactlyInAnyOrder("g:null/1/2", "h:8/null/80");
        assertThat(base.count()).isEqualTo(7);
        assertThat(mine.get("c")).contains(3);
    }

    @Test
    void theResolverMayRemoveOrKeepMine() {
        NodeStore store = NodeStore.inMemory();
        PMap<String, Integer> base = PMap.empty(store, STRINGS, INTS).assoc("k", 1).assoc("m", 1);
        PMap<String, Integer> mine = base.assoc("k", 2).assoc("m", 2);
        PMap<String, Integer> theirs = base.assoc("k", 3).assoc("m", 3);
        PMap<String, Integer> removed = mine.merge3(base, theirs, (key, b, m, t) -> Optional.empty());
        assertThat(removed.count()).isZero();
        PMap<String, Integer> keptMine = mine.merge3(base, theirs, (key, b, m, t) -> m);
        assertThat(keptMine).isEqualTo(mine);
        PMap<String, Integer> summed = mine.merge3(base, theirs, (key, b, m, t) -> Optional.of(m.get() + t.get()));
        assertThat(summed.toMap()).isEqualTo(Map.of("k", 5, "m", 5));
    }

    @Test
    void mergeIsDeterministicAndCheapOnALargeMap() {
        NodeStore store = NodeStore.inMemory();
        Editor<String, Integer> editor = PMap.empty(store, STRINGS, INTS).editor();
        for (int i = 0; i < 20_000; i++) {
            editor.assoc("key" + i, i);
        }
        PMap<String, Integer> base = editor.done().value();
        PMap<String, Integer> mine = base.assoc("key7", -7).assoc("new-mine", 1);
        PMap<String, Integer> theirs = base.assoc("key9", -9).dissoc("key11").assoc("new-theirs", 2);
        Recording resolver = new Recording();
        PMap<String, Integer> merged = mine.merge3(base, theirs, resolver);
        assertThat(resolver.calls).isEmpty();
        assertThat(merged.count()).isEqualTo(20_001);
        assertThat(merged.get("key7")).contains(-7);
        assertThat(merged.get("key9")).contains(-9);
        assertThat(merged.get("key11")).isEmpty();
        assertThat(merged.get("new-mine")).contains(1);
        assertThat(merged.get("new-theirs")).contains(2);
        // The same inputs in the other order give the same value when nothing conflicts.
        assertThat(theirs.merge3(base, mine, resolver)).isEqualTo(merged);
        assertThat(mine.merge3(base, mine, resolver)).isEqualTo(mine);
        assertThat(base.merge3(base, theirs, resolver)).isEqualTo(theirs);
    }
}
