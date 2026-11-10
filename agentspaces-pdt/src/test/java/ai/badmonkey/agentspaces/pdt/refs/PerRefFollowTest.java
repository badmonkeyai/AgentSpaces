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

import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A follow per ref (PDT 2 phase 7, P2-X-08; {@code agentspaces.pdt.refs.<name>.follow}): the named ref follows
 * its own mode and every other ref the facade's, so one reader can fetch a hot ref's trees as heads arrive while
 * it loads a large cold ref only on get.
 */
class PerRefFollowTest {

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static PMap<String, String> fill(Ref<PMap<String, String>> ref, int entries, long seed) {
        return ref.swap(m -> {
            Random random = new Random(seed);
            Editor<String, String> editor = m.editor();
            for (int i = 0; i < entries; i++) {
                editor.assoc(RefFleet.key(i), RefFleet.value(100, random));
            }
            return editor.done().value();
        });
    }

    /** Writes both refs on {@code a} and returns their roots once {@code b} holds both heads and the hot tree. */
    private String[] writeHotAndCold(RefFleet.Peer a, RefFleet.Peer b) throws InterruptedException {
        Ref<PMap<String, String>> hotOnB = b.pdt().mapRef("hot", RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> coldOnB = b.pdt().mapRef("cold", RefFleet.STR, RefFleet.STR);
        String hot = fill(a.pdt().mapRef("hot", RefFleet.STR, RefFleet.STR), 500, 1).root();
        String cold = fill(a.pdt().mapRef("cold", RefFleet.STR, RefFleet.STR), 500, 2).root();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, "hot").size() == 1
                && RefFleet.headEntries(b, "cold").size() == 1)).isTrue();
        assertThat(hotOnB.name()).isEqualTo("hot");
        assertThat(coldOnB.name()).isEqualTo("cold");
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> b.store().complete(hot)))
                .as("the hot ref follows fully: its tree arrives without a get").isTrue();
        fleet.settle(12);
        return new String[]{hot, cold};
    }

    @Test
    void aRefConfiguredFullIsFetchedOnItsHeadUnderALazyFacade() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2,
                RefFleet.Spec.defaults().pdt(p -> p.follow(Pdt.FOLLOW_LAZY).follow("hot", Pdt.FOLLOW_FULL)), "a");
        fleet.settle(6);

        String[] roots = writeHotAndCold(a, b);

        assertThat(b.store().has(roots[1])).as("the cold ref stays lazy: nothing fetched on its head").isFalse();
    }

    @Test
    void aRefConfiguredLazyFetchesNothingOnItsHeadUnderAFullFacade() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2,
                RefFleet.Spec.defaults().pdt(p -> p.follow(Pdt.FOLLOW_FULL).follow("cold", Pdt.FOLLOW_LAZY)), "a");
        fleet.settle(6);

        String[] roots = writeHotAndCold(a, b);

        assertThat(b.store().has(roots[1])).as("the cold ref is lazy under a full facade").isFalse();
    }

    @Test
    void anUnknownFollowIsRefusedByName() throws Exception {
        assertThatThrownBy(() -> Pdt.builder(fleet.newPeer("a", 1).refSpace()).follow("hot", "eager"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("hot").hasMessageContaining("full or lazy");
    }
}
