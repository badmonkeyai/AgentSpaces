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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.kinds.VgNode;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.test.SimNetwork;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Swarm retries (ROBUSTNESS.md §10): lateness is normal in the swarm, so a node that waits on another peer re-queues
 * what is missing, asks again with backoff, and asks the other holders before the origin, while a caller's deadline
 * ends only the caller's wait. Each test makes one wait late on the sim fleet and checks the work still lands.
 */
class SwarmRetriesTest {

    private static final String NAME = "late";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static RefFleet.Spec pushing() {
        return RefFleet.Spec.defaults().pdt(b -> b.push(PushPublisher.Config.on()));
    }

    private static PMap<String, String> grow(PMap<String, String> base, int bytes, long seed) {
        Random random = new Random(seed);
        PMap<String, String> out = base;
        for (int i = 0; i * 120 < bytes; i++) {
            out = out.assoc("k-" + seed + "-" + i, RefFleet.value(100, random));
        }
        return out;
    }

    private static String stream() {
        return PushReceiver.stream(RefFleet.REF_SPACE, NAME);
    }

    /** Holds every bundle of the ref's stream at a follower until the test hands them to its receiver. */
    private static List<Runnable> holdBundles(RefFleet.Peer follower) {
        List<Runnable> held = new ArrayList<>();
        PushReceiver receiver = follower.pdt().refs().pushReceiver();
        follower.runtime().gossip().replaceHandler(stream(),
                (from, id, payload) -> held.add(() -> receiver.onBundle(stream(), from, payload)));
        return held;
    }

    /**
     * P4: a bundle whose eager copy runs late, here 2.5 s, past the old fixed grace of 2 s, still completes its root
     * from the push: the grace is sized from the bus's repair window and the announced bundles.
     */
    @Test
    void aPushedBundleLatePastTheFloorGraceStillCompletesTheRootWithNoFetch() throws Exception {
        RefFleet.Peer writer = fleet.newPeer("w", 400, pushing());
        RefFleet.Peer follower = fleet.newPeer("f", 401, pushing(), "w");
        fleet.settle(10);
        follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<Runnable> held = holdBundles(follower);
        PMap<String, String> v = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 150_000, 1));
        assertThat(RefFleet.headEntries(follower, NAME)).as("the head arrived; its bundle is late").hasSize(1);
        assertThat(held).isNotEmpty();
        fleet.settle(10);
        held.forEach(Runnable::run);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> follower.store().complete(v.root()))).isTrue();
        fleet.settle(2);
        assertThat(follower.pdt().refs().pushStats().fallbacks()).as("no fallback within the sized grace").isZero();
        assertThat(follower.pdt().stats().deltasFetched()).as("the push alone completed the root").isZero();
    }

    /**
     * P4: past the grace the follower fetches what the push left missing, and a bundle that comes after that is still
     * stored, so a merely late bundle lands even when the fetch finds no holder.
     */
    @Test
    void aBundleThatComesAfterTheFallbackBeganIsStillStored() throws Exception {
        RefFleet.Peer writer = fleet.newPeer("w", 410, pushing());
        RefFleet.Peer follower = fleet.newPeer("f", 411, pushing(), "w");
        fleet.settle(10);
        Ref<PMap<String, String>> onF = follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<Runnable> held = holdBundles(follower);
        PMap<String, String> v = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 150_000, 2));
        assertThat(held).isNotEmpty();
        fleet.kill(writer); // the only holder: the fallback fetch finds nobody to serve it
        fleet.settle(4 * 6); // 6 s: past the sized grace of about 4 s, inside the two graces the late bundles are kept
        assertThat(follower.pdt().refs().pushStats().fallbacks()).as("the grace ran out").isEqualTo(1);
        assertThat(follower.store().complete(v.root())).isFalse();
        held.forEach(Runnable::run);
        assertThat(follower.store().complete(v.root())).as("the late bundle's nodes are stored").isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onF.deref().root().equals(v.root()))).isTrue();
    }

    /**
     * P2 and P6: a live head whose fetch failed is fetched again by the tick, with no deref, once its backoff ends; the
     * retry prefers the members that hold the version now over the head's issuer.
     */
    @Test
    void aHeadWhoseFetchFailedIsFetchedAgainByTheTickFromAnotherHolderFirst() throws Exception {
        RefFleet.Spec quick = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(Duration.ofMillis(300)));
        RefFleet.Peer a = fleet.newPeer("a", 420, quick);
        RefFleet.Peer b = fleet.newPeer("b", 421, quick, "a");
        RefFleet.Peer c = fleet.newPeer("c", 422, quick, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> b.store().complete(v1.root()) && !onB.heads().isEmpty())).isTrue();

        // a head names a version whose nodes no member holds yet, so b's fetch fails; the version is over the inline
        // bound, so each member that writes it later holds it and says so
        NodeStore detached = NodeStore.inMemory();
        PMap<String, String> v2 = grow(PMap.empty(detached, RefFleet.STR, RefFleet.STR), 150_000, 4);
        PRef head = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, v2.root(), List.of(v1.root()), 2, v2.count(), null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        a.refSpace().write(head, Lease.of(Duration.ofMinutes(10)), head.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 2)).isTrue();
        fleet.settle(12);
        assertThat(b.store().complete(v2.root())).isFalse();

        // the version appears on c and on the issuer a, each under a name of its own, with their holdings
        assertThat(c.pdt().mapRef("mirror", RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 150_000, 4)).root())
                .as("the same content, the same root").isEqualTo(v2.root());
        a.pdt().mapRef("mirror", RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 150_000, 4));
        // no deref: the tick alone fetches it again once the backoff ends, within ten seconds of fleet time
        for (int tick = 0; tick < 40 && !b.store().complete(v2.root()); tick++) {
            fleet.tickAll();
        }
        assertThat(b.store().complete(v2.root())).as("the tick fetched the head again").isTrue();
        fleet.settle(2); // the tick settles the finished fetch
        TreeFetch.Progress progress = b.pdt().refs().fetched(NAME, v2.root()).orElseThrow();
        assertThat(progress.bytesBySource().getOrDefault(c.id(), 0L)).as("the retry asked c, a holder other than the issuer")
                .isPositive();
    }

    /**
     * P1 and P2, the gate's adversarial review (ROBUSTNESS.md §10.6): a read waits the load timeout for a head's first
     * fetch alone. The tick's retries of a head that stays late run in the background, so a later read returns at
     * once with the head left out and counted late, where each retry used to cost every read the whole load timeout.
     */
    @Test
    void aReadDoesNotWaitAgainForTheTicksRetryOfAHeadThatStaysLate() throws Exception {
        Duration loadTimeout = Duration.ofSeconds(3);
        RefFleet.Spec slow = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(loadTimeout));
        RefFleet.Peer a = fleet.newPeer("a", 440, slow);
        RefFleet.Peer b = fleet.newPeer("b", 441, slow, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> b.store().complete(v1.root()) && !onB.heads().isEmpty())).isTrue();

        // a head names a version whose nodes no member holds, so every fetch of it fails
        PMap<String, String> v2 = grow(PMap.empty(NodeStore.inMemory(), RefFleet.STR, RefFleet.STR), 150_000, 5);
        PRef head = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, v2.root(), List.of(v1.root()), 2, v2.count(), null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        a.refSpace().write(head, Lease.of(Duration.ofMinutes(10)), head.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 2)).isTrue();
        // the first fetch fails on a tick once its session finds no holder; the tick starts the retry within its first
        // backoff of 1 to 1.5 s (6 ticks of 250 ms), and the retry's own session runs at least the 3 s load timeout
        for (int tick = 0; tick < 400 && b.pdt().refs().headsLate().failedFetch() == 0; tick++) {
            fleet.tickAll();
        }
        assertThat(b.pdt().refs().headsLate().failedFetch()).as("the first fetch failed").isEqualTo(1);
        fleet.settle(7);
        long started = System.nanoTime();
        PMap<String, String> read = onB.deref();
        long millis = (System.nanoTime() - started) / 1_000_000;
        assertThat(read.root()).as("the late head is left out of the read").isEqualTo(v1.root());
        assertThat(millis).as("the read returns without waiting for the retry").isLessThan(loadTimeout.toMillis() / 2);
    }

    /**
     * P3: a lazy load whose root node runs past the load timeout ends the caller's wait, and the batch goes on; the
     * node lands when it comes, and the next read finds it.
     */
    @Test
    void aLazyLoadPastItsDeadlineLeavesTheBatchRunningAndTheNextReadFindsTheNode() throws Exception {
        RefFleet.Spec lazy = RefFleet.Spec.defaults().pdt(b -> b.follow(Pdt.FOLLOW_LAZY).loadTimeout(Duration.ofMillis(200)));
        RefFleet.Peer a = fleet.newPeer("a", 430);
        RefFleet.Peer b = fleet.newPeer("b", 431, lazy, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 150_000, 3)); // not inline
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 1)).isTrue();
        fleet.network.hold("a", "b"); // a's answers to b run late
        onB.deref();
        assertThat(b.store().has(v.root())).isFalse();
        assertThat(b.pdt().swarm().fetcher().inFlight()).as("the batch runs on past the caller's deadline")
                .containsKey(v.root());
        fleet.network.release("a", "b", SimNetwork.Order.FIFO, 0);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> b.store().has(v.root()))).as("the late node landed").isTrue();
        assertThat(onB.deref().root()).isEqualTo(v.root());
    }

    /**
     * P3, the gate's adversarial review (ROBUSTNESS.md §10.6): a lazy batch that runs on past its caller's deadline and
     * is then cancelled, here by the test, settles its future, so the next read starts a new batch. A batch nobody
     * settled kept its CID in flight, and every later read joined it and waited its deadline in vain.
     */
    @Test
    void aCancelledLazyBatchLetsTheNextReadStartANewOne() throws Exception {
        RefFleet.Spec lazy = RefFleet.Spec.defaults().pdt(b -> b.follow(Pdt.FOLLOW_LAZY).loadTimeout(Duration.ofMillis(200)));
        RefFleet.Peer a = fleet.newPeer("a", 450);
        RefFleet.Peer b = fleet.newPeer("b", 451, lazy, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).swap(m -> grow(m, 150_000, 6)); // not inline
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 1)).isTrue();
        fleet.network.hold("a", "b");
        onB.deref();
        TreeFetch batch = b.pdt().swarm().fetcher().inFlight().get(v.root());
        assertThat(batch).as("the batch runs on past the caller's deadline").isNotNull();
        batch.cancel();
        assertThat(fleet.runUntil(Duration.ofSeconds(5), () -> !b.pdt().swarm().fetcher().inFlight().containsKey(v.root())))
                .as("the cancelled batch leaves the in-flight map").isTrue();
        fleet.network.release("a", "b", SimNetwork.Order.FIFO, 0);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.deref().root().equals(v.root())))
                .as("a later read starts a new batch and finds the node").isTrue();
    }

    /**
     * P2-V-02 by the rule of P2 (srad/agentspaces-pdt2.md §10.17): a merge whose base walk needs a chain node nobody
     * serves asks for it once and waits one load timeout. The node then waits out a backoff of its own, the rule of a
     * failed head fetch, so the next read leaves the link out at once, where each read used to ask again and wait the
     * whole load timeout. Once the backoff ends and a holder answers, a read walks the chain to the base.
     */
    @Test
    void aChainNodeNobodyServesCostsOneLoadTimeoutAndLaterReadsWaitOutItsBackoff() throws Exception {
        Duration loadTimeout = Duration.ofSeconds(2);
        RefFleet.Spec slow = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(loadTimeout));
        RefFleet.Peer a = fleet.newPeer("a", 460, slow);
        RefFleet.Peer b = fleet.newPeer("b", 461, slow, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        // b forks from v1 while a moves on to v3 and cancels v1 and v2, as in VersionGraphTest's late joiner
        String v1 = onA.swap(m -> m.assoc("k", "v1")).root();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.heads().roots().equals(List.of(v1)))).isTrue();
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
        String v3 = onA.swap(m -> m.assoc("k", "v3")).root();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> RefFleet.headEntries(b, NAME).stream()
                .map(e -> e.value().root()).toList().equals(List.of(v3)))).as("a cancelled v1 and v2").isTrue();
        release.countDown();
        fork.join();
        assertThat(failure.get()).isNull();

        // a joiner holds both heads by the tick and has never seen v1, so its merge needs the chain nodes
        RefFleet.Peer j = fleet.newPeer("j", 462, slow, "a");
        Ref<PMap<String, String>> onJ = j.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> onJ.heads().roots().size() == 2
                && onJ.heads().roots().stream().allMatch(j.store()::complete))).isTrue();
        assertThat(j.pdt().refs().version(NAME, v1)).isEmpty();
        fleet.network.hold("a", "j"); // nobody serves j a chain node now
        fleet.network.hold("b", "j");
        long started = System.nanoTime();
        PMap<String, String> first = onJ.deref();
        long firstMillis = (System.nanoTime() - started) / 1_000_000;
        assertThat(firstMillis).as("the merge asked for the chain and waited one load timeout")
                .isBetween(loadTimeout.toMillis() / 2, loadTimeout.toMillis() * 2);
        long missed = j.pdt().refs().chainNodesMissed();
        assertThat(missed).as("the walk counted the links it left out").isPositive();
        started = System.nanoTime();
        PMap<String, String> second = onJ.deref();
        long secondMillis = (System.nanoTime() - started) / 1_000_000;
        assertThat(secondMillis).as("the next merge waits out the nodes' backoff and asks for nothing")
                .isLessThan(loadTimeout.toMillis() / 2);
        assertThat(second.root()).isEqualTo(first.root());
        assertThat(j.pdt().refs().chainNodesMissed()).as("and counts the links it left out again").isGreaterThan(missed);

        // the holders answer again and the backoff ends: a read walks the chain to v1 and merges against it
        fleet.network.release("a", "j", SimNetwork.Order.FIFO, 0);
        fleet.network.release("b", "j", SimNetwork.Order.FIFO, 0);
        fleet.settle(8);
        assertThat(onJ.deref().toMap()).isEqualTo(Map.of("k", "v3", "b", "fork"));
    }

    /**
     * Concurrent heads (srad/agentspaces-pdt2.md §10.20): a reader meets a head before the entry of the head's base
     * arrives, and the head's writer fetches the chain nodes it linked only on a later tick, so for that while the
     * base's own writer alone holds the base's chain node. The walk asks the writers of every version the reader knows,
     * reads the chain node from that writer, and merges against the common base, as the writer does. It asked the
     * heads' writers alone, who lacked the node, so the merge found no base and the two replicas never agreed.
     */
    @Test
    void aWalkReadsAChainNodeTheHeadsWritersLackFromTheWriterOfAVersionTheReaderKnows() throws Exception {
        Duration loadTimeout = Duration.ofSeconds(2);
        RefFleet.Spec slow = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(loadTimeout));
        RefFleet.Peer a = fleet.newPeer("a", 470, slow);
        RefFleet.Peer b = fleet.newPeer("b", 471, slow, "a");
        RefFleet.Peer c = fleet.newPeer("c", 472, slow, "a");
        RefFleet.Peer r = fleet.newPeer("r", 473, slow, "a");
        fleet.settle(8);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onC = c.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onR = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<PConflict> onRConflicts = new CopyOnWriteArrayList<>();
        onR.onConflict(onRConflicts::add);
        // b writes the base, and every member reads its entry: b wrote a version r knows
        PMap<String, String> base = onB.swap(m -> m.assoc("k", "base"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(base.root())
                && List.of(onA, onC, onR).stream().allMatch(ref -> ref.heads().roots().equals(List.of(base.root()))))).isTrue();
        String baseVg = Ancestry.cid(RefFleet.headEntries(b, NAME).get(0).value()).orElseThrow();
        // b moves on to p, whose entry r has yet to read; p's chain node sits in b's store alone
        PMap<String, String> p = onB.at(base.root()).assoc("p", "b");
        String pVg = b.store().put(vg(b, p.root(), base.root(), baseVg).encode(), Cids.DAG_CBOR);
        // c writes h over p, its chain node linking p's, which c has yet to fetch; a writes k over the base
        copyTree(b.store(), c.store(), p.root());
        PMap<String, String> h = onC.at(p.root()).assoc("h", "c");
        String hVg = c.store().put(vg(c, h.root(), p.root(), pVg).encode(), Cids.DAG_CBOR);
        RefFleet.writeHead(c, NAME, PRef.KIND_MAP, h.root(), p.root(), 2, h.count(), Duration.ofMinutes(10), Map.of(Ancestry.ATTR, hVg));
        PMap<String, String> k = onA.at(base.root()).assoc("k2", "a");
        String kVg = a.store().put(vg(a, k.root(), base.root(), baseVg).encode(), Cids.DAG_CBOR);
        RefFleet.writeHead(a, NAME, PRef.KIND_MAP, k.root(), base.root(), 2, k.count(), Duration.ofMinutes(10), Map.of(Ancestry.ATTR, kVg));
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> Set.copyOf(onR.heads().roots()).equals(Set.of(h.root(), k.root()))
                && r.store().complete(h.root()) && r.store().complete(k.root()))).isTrue();
        assertThat(r.pdt().refs().version(NAME, p.root())).as("r never read an entry of p").isEmpty();
        assertThat(c.store().has(pVg)).as("the head's writer lacks the chain node it linked").isFalse();

        Map<String, String> merged = Map.of("k", "base", "p", "b", "h", "c", "k2", "a");
        assertThat(onB.deref().toMap()).as("the base's writer merges against the base").isEqualTo(merged);
        assertThat(onR.deref().toMap()).as("and so does the reader").isEqualTo(merged);
        assertThat(onRConflicts).extracting(PConflict::reason).doesNotContain(PConflict.NO_COMMON_BASE);
        assertThat(r.pdt().refs().chainNodesMissed()).isZero();
    }

    /**
     * Concurrent heads, the gate of srad/agentspaces-pdt2.md §10.20: a chain node that is late in a walk stays the
     * version's chain node. A writer whose merge could not fetch a head's chain node, or whose tick walked the store
     * alone while the node was still in flight, forgot which node describes the head, so the version it wrote next
     * linked no node for that base. The chain broke there for good, and a peer that met a later head before the
     * base's entry ended its walk at the break and merged with no base, while the writers still raced.
     */
    @Test
    void aWriterWhoseWalkMissedAHeadsChainNodeStillLinksItFromTheVersionItWrites() throws Exception {
        Duration loadTimeout = Duration.ofSeconds(2);
        RefFleet.Spec slow = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(loadTimeout));
        RefFleet.Peer a = fleet.newPeer("a", 480, slow);
        RefFleet.Peer b = fleet.newPeer("b", 481, slow, "a");
        RefFleet.Peer c = fleet.newPeer("c", 482, slow, "a");
        RefFleet.Peer w = fleet.newPeer("w", 483, slow, "a");
        fleet.settle(8);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onC = c.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onW = w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> base = onB.swap(m -> m.assoc("k", "base"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(base.root())
                && List.of(onA, onC, onW).stream().allMatch(ref -> ref.heads().roots().equals(List.of(base.root()))))).isTrue();
        String baseVg = Ancestry.cid(RefFleet.headEntries(b, NAME).get(0).value()).orElseThrow();
        // c writes h over p, whose entry w never reads, so w's walk needs h's chain node, which c alone holds
        PMap<String, String> p = onB.at(base.root()).assoc("p", "b");
        String pVg = b.store().put(vg(b, p.root(), base.root(), baseVg).encode(), Cids.DAG_CBOR);
        copyTree(b.store(), c.store(), p.root());
        PMap<String, String> h = onC.at(p.root()).assoc("h", "c");
        String hVg = c.store().put(vg(c, h.root(), p.root(), pVg).encode(), Cids.DAG_CBOR);
        RefFleet.writeHead(c, NAME, PRef.KIND_MAP, h.root(), p.root(), 2, h.count(), Duration.ofMinutes(10), Map.of(Ancestry.ATTR, hVg));
        PMap<String, String> k = onA.at(base.root()).assoc("k2", "a");
        String kVg = a.store().put(vg(a, k.root(), base.root(), baseVg).encode(), Cids.DAG_CBOR);
        RefFleet.writeHead(a, NAME, PRef.KIND_MAP, k.root(), base.root(), 2, k.count(), Duration.ofMinutes(10), Map.of(Ancestry.ATTR, kVg));
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> Set.copyOf(onW.heads().roots()).equals(Set.of(h.root(), k.root()))
                && w.store().complete(h.root()) && w.store().complete(k.root()))).isTrue();
        assertThat(w.store().has(hVg)).as("w has not read h's chain node").isFalse();

        // h's chain node is late at w: its holders answer w only after w's merge has given up on it
        fleet.network.hold("b", "w");
        fleet.network.hold("c", "w");
        PMap<String, String> mine = onW.swap(m -> m.assoc("w", "w"));
        assertThat(w.pdt().refs().chainNodesMissed()).as("the merge walked without h's chain node").isPositive();
        PRef written = RefFleet.headEntries(w, NAME).stream().map(Space.Entry::value)
                .filter(head -> head.root().equals(mine.root())).findFirst().orElseThrow();
        String writtenVg = Ancestry.cid(written).orElseThrow();
        VgNode node = Ancestry.read(writtenVg, mine.root(), w.store().get(writtenVg).orElseThrow()).orElseThrow();
        assertThat(node.previous()).extracting(Link::cid).as("w's version links the chain node of each base it merged")
                .containsExactlyInAnyOrder(hVg, kVg);
        fleet.network.release("b", "w", SimNetwork.Order.FIFO, 0);
        fleet.network.release("c", "w", SimNetwork.Order.FIFO, 0);
    }

    /** A {@code vg} node of a version with one base, written by a peer's ref writer, linking the base's node. */
    private VgNode vg(RefFleet.Peer writer, String root, String base, String baseVg) {
        String issuer = writer.refSpace().writer().orElseThrow().encoded();
        return new VgNode(root, List.of(base), issuer, new HlcTimestamp(fleet.clock.millis(), 0, issuer).encoded(),
                List.of(new Link(baseVg)));
    }

    /** Copies the nodes of a tree from one store to another, as a writer that read the tree holds it. */
    private static void copyTree(NodeStore from, NodeStore to, String root) {
        for (String cid : from.reachable(root)) {
            to.put(from.get(cid).orElseThrow(), Cids.parse(cid).codec());
        }
    }
}
