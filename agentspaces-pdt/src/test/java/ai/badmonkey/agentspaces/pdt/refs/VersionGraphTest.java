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
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.kinds.VgNode;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.store.Retention;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 1, group V (srad/agentspaces-pdt2.md §3.10, P2-V-01 to P2-V-04): every head names
 * its {@code vg} node, the writer holds the node and never carries it inline, a reader that never
 * saw a version's entry walks the chain past it, and the pins and retained versions of a peer
 * over a pack store survive its restart.
 */
class VersionGraphTest {

    private static final String NAME = "ledger";

    private static PRef record(RefFleet.Peer peer, String root) {
        return RefFleet.headEntries(peer, NAME).stream().map(Space.Entry::value).filter(r -> r.root().equals(root))
                .findFirst().orElseThrow();
    }

    /** P2-V-01: the head's attribute names a vg node of its root and bases; the node is held and never inline. */
    @Test
    void aHeadNamesTheVgNodeItsWriterHoldsAndNeverCarriesInline() throws Exception {
        RefFleet fleet = new RefFleet();
        try {
            RefFleet.Peer w = fleet.newPeer("w", 1);
            RefFleet.Peer r = fleet.newPeer("r", 2, "w");
            fleet.settle(12);
            Ref<PMap<String, String>> ref = w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            String first = ref.swap(m -> m.assoc("a", "1")).root();
            String second = ref.swap(m -> m.assoc("b", "2")).root();
            assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(r, NAME).stream()
                    .anyMatch(e -> e.value().root().equals(second)))).isTrue();
            PRef head = record(r, second);
            String vg = Ancestry.cid(head).orElseThrow();
            VgNode node = VgNode.decode(vg, w.store().get(vg).orElseThrow());
            assertThat(node.root()).isEqualTo(second);
            assertThat(node.bases()).isEqualTo(head.base()).containsExactly(first);
            assertThat(node.issuer()).isEqualTo(w.refSpace().writer().orElseThrow().encoded());
            fleet.settle(4);
            assertThat(r.store().has(vg)).as("a reader fetches no chain node until a walk needs it").isFalse();
            assertThat(node.previous()).as("a link to the base's own vg node").hasSize(1);
            String firstVg = node.previous().get(0).cid();
            assertThat(VgNode.decode(firstVg, w.store().get(firstVg).orElseThrow()).root()).isEqualTo(first);
            assertThat(head.nodes()).as("the delta rides inline").isNotEmpty();
            byte[] vgBytes = w.store().get(vg).orElseThrow();
            assertThat(head.nodes()).as("the vg node never rides inline").noneMatch(n -> java.util.Arrays.equals(n, vgBytes));
            CborCodec codec = CborCodec.defaultCodec();
            assertThat(codec.fromWire(codec.toBytes(head), PRef.class).attrs().get(Ancestry.ATTR))
                    .as("the 12-field schema carries it as text").isEqualTo(vg);
            w.pdt().collector().collect();
            assertThat(w.store().has(vg)).as("the collector keeps the chain of a live version").isTrue();
            assertThat(w.store().has(firstVg)).isTrue();
        } finally {
            fleet.close();
        }
    }

    /**
     * P2-V-02 and P2-V-04, the residual of srad/agentspaces-pdt.md: writer B forks from v1 while A
     * moves on to v3 and cancels v1 and v2, so when B's head lands no replica holds an entry of v1
     * or v2. A reader that watched throughout merges v3 and B's head against v1 from memory; a
     * joiner that arrives afterwards has never seen v1 or v2 and walks the chain to the same base,
     * the same value, and the same conflicts, where the walk over entries alone found no base.
     */
    @Test
    void aLateJoinerWalksTheChainPastVersionsWhoseEntriesAreGone() throws Exception {
        RefFleet fleet = new RefFleet();
        try {
            RefFleet.Peer a = fleet.newPeer("a", 1);
            RefFleet.Peer b = fleet.newPeer("b", 2, "a");
            RefFleet.Peer r = fleet.newPeer("r", 3, "a");
            fleet.settle(12);
            Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> onR = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<PConflict> onRConflicts = new CopyOnWriteArrayList<>();
            onR.onConflict(onRConflicts::add);
            String v1 = onA.swap(m -> m.assoc("k", "v1")).root();
            assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.heads().roots().equals(List.of(v1))
                    && onR.heads().roots().equals(List.of(v1)))).isTrue();
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread fork = Thread.ofVirtual().start(() -> {
                try {
                    onB.swap(m -> {
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return m.assoc("b", "fork");
                    });
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            onA.swap(m -> m.assoc("k", "v2"));
            fleet.settle(2);
            String v3 = onA.swap(m -> m.assoc("k", "v3").assoc("c", "3")).root();
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> RefFleet.headEntries(r, NAME).stream()
                    .map(e -> e.value().root()).toList().equals(List.of(v3))))
                    .as("A cancelled v1 and v2 after the grace; no replica holds their entries").isTrue();
            release.countDown();
            fork.join();
            assertThat(failure.get()).isNull();
            String forkVg = Ancestry.cid(RefFleet.headEntries(b, NAME).stream().map(Space.Entry::value)
                    .filter(h -> h.base().equals(List.of(v1))).findFirst().orElseThrow()).orElseThrow();
            String linked = VgNode.decode(forkVg, b.store().get(forkVg).orElseThrow()).previous().get(0).cid();
            assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> b.store().has(linked)))
                    .as("the fork's writer fetches and holds the chain node it linked, v1's, written by A").isTrue();
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> onR.heads().roots().size() == 2)).isTrue();
            fleet.settle(2);
            PMap<String, String> watched = onR.deref();
            assertThat(watched.toMap()).as("merged against v1").isEqualTo(Map.of("k", "v3", "c", "3", "b", "fork"));
            assertThat(onRConflicts).extracting(PConflict::reason).doesNotContain(PConflict.NO_COMMON_BASE);

            RefFleet.Peer joiner = fleet.newPeer("j", 4, "a");
            Ref<PMap<String, String>> onJ = joiner.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<PConflict> onJConflicts = new CopyOnWriteArrayList<>();
            onJ.onConflict(onJConflicts::add);
            Set<String> heads = Set.copyOf(onR.heads().roots());
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> Set.copyOf(onJ.heads().roots()).equals(heads))).isTrue();
            assertThat(joiner.pdt().refs().version(NAME, v1)).as("the joiner never saw an entry of v1").isEmpty();
            PMap<String, String> joined = onJ.deref();
            assertThat(joined.root()).isEqualTo(watched.root());
            assertThat(onJConflicts).extracting(PConflict::reason).doesNotContain(PConflict.NO_COMMON_BASE);
            assertThat(onJConflicts).hasSameSizeAs(onRConflicts);
        } finally {
            fleet.close();
        }
    }

    /** P2-V-03: a writer over a pack store restarts with its pins and retained versions, and its collector keeps them. */
    @Test
    void aRestartedPeerKeepsItsPinsAndRetainedVersions(@TempDir Path dir) throws Exception {
        RefFleet fleet = new RefFleet();
        try {
            PackNodeStore pack = PackNodeStore.open(dir);
            RefFleet.Peer w = fleet.newPeer("w", 1, RefFleet.Spec.defaults().store(new NodeStore(pack, fleet.clock)));
            fleet.settle(4);
            Ref<PMap<String, String>> ref = w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            for (int i = 0; i < 4; i++) {
                int n = i;
                ref.swap(m -> m.assoc("k" + n, "v" + n));
                fleet.settle(1);
            }
            List<Retention.Version> retained = w.pdt().retention().versions(NAME);
            Set<String> pinned = w.store().pinned();
            assertThat(retained).hasSize(4);
            assertThat(pinned).isNotEmpty();
            fleet.kill(w);
            pack.close();

            PackNodeStore reopened = PackNodeStore.open(dir);
            NodeStore store = new NodeStore(reopened, fleet.clock);
            assertThat(store.pinned()).isEqualTo(pinned);
            RefFleet.Peer again = fleet.newPeer("w2", 2, RefFleet.Spec.defaults().store(store));
            assertThat(again.pdt().retention().versions(NAME)).isEqualTo(retained);
            again.pdt().collector().collect();
            for (Retention.Version version : retained) {
                assertThat(store.complete(version.root())).as("retained " + version.root()).isTrue();
            }
            reopened.close();
        } finally {
            fleet.close();
        }
    }

    /**
     * PDT 2 phase 2, P2-V-05: history reads the version graph, so a writer restarted over its pack
     * store lists the versions whose entries are gone, and a lazy late joiner lists them from the
     * chain nodes it fetches from the writer.
     */
    @Test
    void historyReadsTheChainAfterARestartAndOnALazyJoiner(@TempDir Path dir) throws Exception {
        RefFleet fleet = new RefFleet();
        try {
            PackNodeStore pack = PackNodeStore.open(dir);
            RefFleet.Peer w = fleet.newPeer("w", 1, RefFleet.Spec.defaults().store(new NodeStore(pack, fleet.clock)));
            RefFleet.Peer r = fleet.newPeer("r", 2, "w");
            fleet.settle(12);
            Ref<PMap<String, String>> ref = w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<String> roots = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) {
                int n = i;
                roots.add(ref.swap(m -> m.assoc("k" + n, "v" + n)).root());
                fleet.settle(1);
            }
            String last = roots.get(3);
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> RefFleet.headEntries(r, NAME).stream()
                    .map(e -> e.value().root()).toList().equals(List.of(last))))
                    .as("the writer cancelled every superseded head after the grace").isTrue();
            assertThat(ref.history(3)).extracting(Heads.Head::root).containsExactly(roots.get(2), roots.get(1), roots.get(0));

            fleet.kill(w);
            pack.close();
            PackNodeStore reopened = PackNodeStore.open(dir);
            RefFleet.Peer again = fleet.newPeer("w2", 3, RefFleet.Spec.defaults().store(new NodeStore(reopened, fleet.clock)), "r");
            Ref<PMap<String, String>> onAgain = again.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> onAgain.heads().roots().equals(List.of(last)))).isTrue();
            assertThat(again.pdt().refs().version(NAME, roots.get(1))).as("no entry of v2 reached the restarted peer").isEmpty();
            List<Heads.Head> history = onAgain.history(3);
            assertThat(history).extracting(Heads.Head::root).containsExactly(roots.get(2), roots.get(1), roots.get(0));
            assertThat(history.get(2).base()).isEmpty();
            assertThat(history.get(0).base()).containsExactly(roots.get(1));
            assertThat(history.get(0).issuer()).isEqualTo(w.refSpace().writer().orElseThrow());
            assertThat(onAgain.at(roots.get(1)).get("k1")).contains("v1");

            // the restarted writer moves on, so the head's issuer is live and holds every chain node
            String fifth = onAgain.swap(m -> m.assoc("k4", "v4")).root();
            RefFleet.Peer lazy = fleet.newPeer("l", 4, RefFleet.Spec.defaults().pdt(b -> b.follow("lazy")), "r");
            Ref<PMap<String, String>> onLazy = lazy.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> onLazy.heads().roots().equals(List.of(fifth)))).isTrue();
            assertThat(lazy.store().has(roots.get(2))).as("a lazy follower holds no earlier version").isFalse();
            AtomicReference<List<Heads.Head>> lazyHistory = new AtomicReference<>();
            Thread reader = Thread.ofVirtual().start(() -> lazyHistory.set(onLazy.history(3)));
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !reader.isAlive())).isTrue();
            assertThat(lazyHistory.get()).extracting(Heads.Head::root).containsExactly(last, roots.get(2), roots.get(1));
            reopened.close();
        } finally {
            fleet.close();
        }
    }

    /**
     * PDT 2 phase 2, P2-D-05: a linear writer restarted over its pack store after its head lapsed on
     * every member continues from its newest retained version, where the persisted retention of
     * P2-V-03 had left it unable to take or to write a first head.
     */
    @Test
    void aLinearWriterWhoseHeadLapsedContinuesFromItsRetainedVersion(@TempDir Path dir) throws Exception {
        RefFleet fleet = new RefFleet();
        try {
            RefFleet.Spec linear = RefFleet.Spec.defaults().pdt(b -> b.mode(NAME, PRef.MODE_LINEAR)
                    .headLease(Duration.ofSeconds(5)).take(Duration.ofSeconds(5), Duration.ofMillis(300), 3));
            PackNodeStore pack = PackNodeStore.open(dir);
            RefFleet.Peer w = fleet.newPeer("w", 1, linear.store(new NodeStore(pack, fleet.clock)));
            RefFleet.Peer r = fleet.newPeer("r", 2, linear, "w");
            fleet.settle(12);
            Ref<PMap<String, String>> ref = w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            String last = ref.swap(m -> m.assoc("k", "v1")).root();
            fleet.kill(w);
            pack.close();
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> RefFleet.headEntries(r, NAME).isEmpty()))
                    .as("the head lapses with its writer gone").isTrue();

            PackNodeStore reopened = PackNodeStore.open(dir);
            RefFleet.Peer again = fleet.newPeer("w2", 3, linear.store(new NodeStore(reopened, fleet.clock)), "r");
            fleet.settle(8);
            Ref<PMap<String, String>> onAgain = again.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(onAgain.deref().root()).as("a read before the write sees the retained version too").isEqualTo(last);
            AtomicReference<PMap<String, String>> written = new AtomicReference<>();
            AtomicReference<Throwable> failed = new AtomicReference<>();
            Thread writer = Thread.ofVirtual().start(() -> {
                try {
                    written.set(onAgain.swap(m -> m.assoc("k2", "v2")));
                } catch (Throwable t) {
                    failed.set(t);
                }
            });
            assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !writer.isAlive())).isTrue();
            assertThat(failed.get()).isNull();
            assertThat(written.get().get("k")).as("the retained version is the base").contains("v1");
            assertThat(RefFleet.headEntries(again, NAME)).extracting(e -> e.value().base()).containsExactly(List.of(last));
            fleet.kill(again);
            reopened.close();
        } finally {
            fleet.close();
        }
    }
}
