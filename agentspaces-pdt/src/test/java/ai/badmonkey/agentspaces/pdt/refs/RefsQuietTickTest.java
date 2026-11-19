/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An idle ref's tick does nothing (idle-loop O4, reviews/IDLE-LOOP-2026-11-18.md): with its space unchanged, nothing in
 * flight and no lease work due, the tick returns before reading the space. It still runs in full when a head arrives,
 * and when its own head's renewal falls due, so a quiet writer's head never lapses.
 */
class RefsQuietTickTest {

    private static final Duration LEASE = Duration.ofSeconds(10);
    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    void anIdleRefIsQuietYetRenewsItsHeadAndFollowsTheNextSwap() throws Exception {
        RefFleet.Spec spec = RefFleet.Spec.defaults().pdt(b -> b.headLease(LEASE));
        RefFleet.Peer w = fleet.newPeer("w", 1, spec);
        RefFleet.Peer r = fleet.newPeer("r", 2, spec, "w");
        Ref<PMap<String, String>> onW = w.pdt().mapRef("m", RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onR = r.pdt().mapRef("m", RefFleet.STR, RefFleet.STR);
        String first = onW.swap(m -> m.assoc("a", "1")).root();
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> onR.heads().roots().equals(List.of(first)))).isTrue();

        long quietW = w.pdt().refs().quietTicks();
        long quietR = r.pdt().refs().quietTicks();
        long ticks = LEASE.multipliedBy(4).toMillis() / RefFleet.PERIOD_MILLIS;
        fleet.runFor(LEASE.multipliedBy(4), () -> false); // four head leases with nothing written

        assertThat(w.pdt().refs().quietTicks() - quietW).as("the writer's quiet ticks of %d", ticks)
                .isGreaterThan(ticks * 8 / 10);
        assertThat(r.pdt().refs().quietTicks() - quietR).as("the reader's quiet ticks of %d", ticks)
                .isGreaterThan(ticks * 8 / 10);
        // the renewals at half-life still ran: the head outlived four leases on both peers
        assertThat(onW.heads().roots()).containsExactly(first);
        assertThat(onR.heads().roots()).containsExactly(first);

        String second = onW.swap(m -> m.assoc("b", "2")).root();
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> onR.heads().roots().equals(List.of(second))
                && r.store().complete(second))).as("a quiet reader follows the next swap").isTrue();
        assertThat(onR.deref().get("b")).contains("2");
    }
}
