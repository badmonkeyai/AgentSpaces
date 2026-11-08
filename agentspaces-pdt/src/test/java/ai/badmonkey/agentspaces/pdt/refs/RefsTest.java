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
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario 1 of srad/agentspaces-pdt.md (§6.4 S1, PD-Q-PRF-05, PD-X-07, PD-R-02,
 * PD-R-03, PD-R-07): a one-key swap on a 10,000-entry map reaches four peers
 * on its head alone, and a swap whose delta exceeds the inline bound travels
 * by request for its delta and little more.
 */
class RefsTest {

    private static final int ENTRIES = 10_000;
    private static final String NAME = "facts";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static PMap<String, String> build(PMap<String, String> empty, long seed) {
        Random random = new Random(seed);
        Editor<String, String> editor = empty.editor();
        for (int i = 0; i < ENTRIES; i++) {
            editor.assoc(RefFleet.key(i), i + ":" + RefFleet.value(60, random));
        }
        return editor.done().value();
    }

    private static long treeMessages(RefFleet.Peer p) {
        Content.Stats s = p.content().stats();
        return s.nodesServed() + s.nodesReceived() + s.treeRequestsRejected() + s.treeMessagesDropped();
    }

    @Test
    void aOneKeySwapOnATenThousandEntryMapReachesEveryPeerOnTheHeadAlone() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        RefFleet.Peer c = fleet.newPeer("c", 3, "a");
        RefFleet.Peer d = fleet.newPeer("d", 4, "a");
        fleet.settle(6);
        List<RefFleet.Peer> readers = List.of(b, c, d);
        List<Ref<PMap<String, String>>> refs = new ArrayList<>();
        for (RefFleet.Peer p : fleet.peers) {
            refs.add(p.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR));
        }
        Ref<PMap<String, String>> onA = refs.get(0);
        List<PMap<String, String>> watched = new java.util.concurrent.CopyOnWriteArrayList<>();
        refs.get(1).watch(watched::add);

        // version 0: the whole map, far above the inline bound, so the readers fetch it
        PMap<String, String> v0 = onA.swap(m -> build(m, 7));
        String root0 = v0.root();
        assertThat(v0.count()).isEqualTo(ENTRIES);
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> fleet.allHold(root0))).as("every reader fetched v0").isTrue();
        fleet.settle(2);
        for (RefFleet.Peer p : readers) {
            assertThat(p.pdt().stats().deltasFetched()).as(p.name()).isEqualTo(1);
            assertThat(p.pdt().stats().deltasInline()).as(p.name()).isZero();
        }
        List<Space.Entry<PRef>> heads0 = RefFleet.headEntries(a, NAME);
        assertThat(heads0).hasSize(1);
        assertThat(heads0.get(0).value().nodes()).as("v0 is above the inline bound").isNull();
        assertThat(heads0.get(0).value().seq()).isEqualTo(1);
        long[] before = new long[fleet.peers.size()];
        for (int i = 0; i < fleet.peers.size(); i++) {
            before[i] = treeMessages(fleet.peers.get(i));
        }

        // version 1: one key, so the delta rides on the head and no tree message crosses
        PMap<String, String> v1 = onA.swap(m -> m.assoc(RefFleet.key(1234), "changed"));
        String root1 = v1.root();
        long delta = RefFleet.bytesNotIn(a.store(), root1, root0);
        assertThat(delta).isLessThanOrEqualTo(PRef.INLINE_DELTA_BYTES);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> fleet.allHold(root1))).as("every reader holds v1").isTrue();
        fleet.settle(4);
        for (RefFleet.Peer p : readers) {
            PMap<String, String> seen = p.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).deref();
            assertThat(seen.root()).as(p.name()).isEqualTo(root1);
            assertThat(seen.get(RefFleet.key(1234))).contains("changed");
            assertThat(seen.count()).isEqualTo(ENTRIES);
            Pdt.Stats stats = p.pdt().stats();
            assertThat(stats.deltasInline()).as(p.name() + " took v1 inline").isEqualTo(1);
            assertThat(stats.deltasFetched()).as(p.name() + " fetched nothing for v1").isEqualTo(1);
        }
        for (int i = 0; i < fleet.peers.size(); i++) {
            assertThat(treeMessages(fleet.peers.get(i))).as("tree pipe messages at " + fleet.peers.get(i).name()).isEqualTo(before[i]);
            assertThat(fleet.peers.get(i).content().activeTreeFetches()).isEmpty();
        }
        List<Space.Entry<PRef>> heads1 = RefFleet.headEntries(b, NAME);
        PRef head1 = heads1.stream().map(Space.Entry::value).filter(h -> h.root().equals(root1)).findFirst().orElseThrow();
        assertThat(head1.nodes()).isNotNull();
        assertThat(head1.inlineBytes()).isEqualTo(delta);
        assertThat(head1.base()).containsExactly(root0);
        assertThat(head1.seq()).isEqualTo(2);
        assertThat(watched).as("the watcher on b saw each head").extracting(PMap::root).contains(root0, root1);
        System.out.printf("PD-Q-PRF-05: one assoc on %d entries shipped %d bytes in %d inline nodes; tree messages unchanged on 4 peers%n",
                ENTRIES, delta, head1.nodes().size());

        // the writer cancels its superseded head after two gossip rounds (PD-R-03)
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(a, NAME).size() == 1
                && RefFleet.headEntries(b, NAME).size() == 1)).as("v0's head cancelled after the grace").isTrue();
        assertThat(RefFleet.headEntries(b, NAME).get(0).value().root()).isEqualTo(root1);

        // version 2: a delta above the inline bound travels by request, for its delta plus at most 1 KiB
        for (RefFleet.Peer p : readers) {
            before[fleet.peers.indexOf(p)] = p.pdt().stats().bytesReceived();
        }
        PMap<String, String> v2 = onA.swap(m -> {
            Random random = new Random(11);
            Editor<String, String> editor = m.editor();
            for (int i = 0; i < 300; i++) {
                editor.assoc("bulk-" + i, RefFleet.value(200, random));
            }
            return editor.done().value();
        });
        String root2 = v2.root();
        long delta2 = RefFleet.bytesNotIn(a.store(), root2, root1);
        assertThat(delta2).isGreaterThan(PRef.INLINE_DELTA_BYTES);
        PRef head2 = RefFleet.headEntries(a, NAME).stream().map(Space.Entry::value).filter(h -> h.root().equals(root2)).findFirst().orElseThrow();
        assertThat(head2.nodes()).as("above the bound the head carries no nodes").isNull();
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> fleet.allHold(root2))).as("every reader fetched v2").isTrue();
        fleet.settle(2);
        for (RefFleet.Peer p : readers) {
            long wire = p.pdt().stats().bytesReceived() - before[fleet.peers.indexOf(p)];
            TreeFetch.Progress progress = p.pdt().refs().fetched(NAME, root2).orElseThrow();
            long unique = progress.bytesBySource().values().stream().mapToLong(Long::longValue).sum();
            assertThat(unique).as(p.name() + " received the delta once").isGreaterThanOrEqualTo(delta2).isLessThanOrEqualTo(delta2 + 1024);
            assertThat(wire).as(p.name() + " wire bytes, the endgame may duplicate one batch").isGreaterThanOrEqualTo(unique)
                    .isLessThanOrEqualTo(delta2 + 64 * 1024);
            assertThat(p.pdt().stats().deltasFetched()).isEqualTo(2);
            PMap<String, String> seen = p.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).deref();
            assertThat(seen.root()).isEqualTo(root2);
            assertThat(seen.count()).isEqualTo(ENTRIES + 300);
            System.out.printf("PD-Q-PRF-05: %s fetched the %d-byte delta of a 300-key swap: %d unique bytes, %d on the wire, %d requests, %d round trips%n",
                    p.name(), delta2, unique, wire, progress.requestsSent(), progress.roundTrips());
        }
    }
}
