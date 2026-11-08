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
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.space.replicated.SpaceAdmission;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The inline delta's guards and the lazy follow (srad/agentspaces-pdt.md
 * PD-X-07, PD-P-04, PD-Q-SEC-03, §6.7 rows 1, 4, and 6; PD-V-06): a head from an
 * identity the space authorizer refuses never changes a deref; inline nodes
 * whose bytes do not match their CIDs, or that no link reaches, are dropped
 * and counted; a head whose root cannot load is ignored and reported; and a
 * lazy follower fetches nothing on a head and loads nodes on get.
 */
class InlineDeltaTest {

    private static final String NAME = "facts";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    void aHeadFromAnIdentityTheAuthorizerRefusesNeverChangesADeref() throws Exception {
        Set<PeerId> refused = ConcurrentHashMap.newKeySet();
        Authorizer authorizer = (peer, operation, scope) -> !refused.contains(peer);
        RefFleet.Spec guarded = RefFleet.Spec.defaults()
                .refSpace(b -> b.admission(SpaceAdmission.authorizer(authorizer, RefFleet.REF_SPACE)));
        RefFleet.Peer a = fleet.newPeer("a", 1, guarded);
        RefFleet.Peer b = fleet.newPeer("b", 2, guarded, "a");
        // the forger's own replica admits everyone, so its head enters its replica and gossips
        RefFleet.Peer f = fleet.newPeer("f", 3, "a");
        refused.add(f.id());
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onF = f.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("truth", "a"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        assertThat(onB.deref().root()).isEqualTo(v1.root());

        // the forger swaps a head that lists a's head as its base; its replica accepts it, nobody else does
        PMap<String, String> forged = onF.swap(m -> m.assoc("truth", "forged"));
        assertThat(RefFleet.headEntries(f, NAME)).extracting(e -> e.value().root()).contains(forged.root());
        fleet.settle(12);
        assertThat(RefFleet.headEntries(b, NAME)).extracting(e -> e.value().root()).containsExactly(v1.root());
        assertThat(RefFleet.headEntries(a, NAME)).extracting(e -> e.value().root()).containsExactly(v1.root());
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        assertThat(onB.deref().get("truth")).contains("a");
        assertThat(onA.deref().root()).isEqualTo(v1.root());
        assertThat(b.pdt().stats().headsIgnored()).as("the forged head never entered b's replica").isZero();
        assertThat(b.store().has(forged.root())).isFalse();
        System.out.printf("PD-Q-SEC-03: the forged head %s stayed on the forger; b and a deref %s%n", forged.root(), v1.root());
    }

    @Test
    void inlineNodesThatFailVerificationOrReachabilityAreDroppedAndCounted() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();

        // a computes v2 in its store and holds it, then publishes a head whose inline nodes are garbage, a stray,
        // and the root alone, so the rest of v2 must be fetched
        Editor<String, String> editor = v1.editor();
        for (int i = 0; i < 200; i++) {
            editor.assoc("k2-" + i, "v2");
        }
        PMap<String, String> v2 = editor.done().value().assoc("k2", "v2");
        String root2 = v2.root();
        assertThat(a.store().reachable(root2).size()).isGreaterThan(1);
        a.pdt().swarm().hold(root2);
        Random random = new Random(5);
        byte[] garbage = new byte[64];
        random.nextBytes(garbage);
        byte[] stray = a.store().get(onA.at(v1.root()).root()).orElseThrow(); // a real node no link from root2 reaches
        byte[] rootNode = a.store().get(root2).orElseThrow();
        List<byte[]> nodes = new ArrayList<>(List.of(garbage, stray.clone(), rootNode));
        PRef bad = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, root2, List.of(v1.root()), 2, 202, nodes,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        assertThat(bad.violation()).isEmpty();
        a.refSpace().write(bad, Lease.of(Duration.ofMinutes(10)), bad.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> b.pdt().stats().nodesDropped() >= 1)).as("b dropped nodes").isTrue();
        // the root node verified and was kept; the garbage matches no CID and no link from root2 reaches the stray
        assertThat(b.store().has(root2)).isTrue();
        assertThat(b.pdt().stats().nodesDropped()).isEqualTo(2);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> b.store().complete(root2))).as("b fetched the rest from a").isTrue();
        fleet.settle(2);
        assertThat(onB.deref().root()).isEqualTo(root2);
        assertThat(onB.deref().get("k2")).contains("v2");
        assertThat(b.pdt().stats().deltasFetched()).isEqualTo(1);
        assertThat(onB.deref().count()).isEqualTo(202);
        System.out.printf("PD-X-07: a head with 3 inline nodes (garbage, stray, root): %d dropped, root kept, the other %d nodes fetched%n",
                b.pdt().stats().nodesDropped(), a.store().reachable(root2).size() - 1);
    }

    @Test
    void aHeadWhoseRootCannotBeLoadedIsIgnoredAndReported() throws Exception {
        RefFleet.Spec quick = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(Duration.ofMillis(300)));
        RefFleet.Peer a = fleet.newPeer("a", 1, quick);
        RefFleet.Peer b = fleet.newPeer("b", 2, quick, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();

        byte[] nobody = new byte[48];
        new Random(9).nextBytes(nobody);
        String phantom = Cids.of(nobody, Cids.DAG_CBOR);
        PRef unloadable = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, phantom, List.of(v1.root()), 2, 1, null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        a.refSpace().write(unloadable, Lease.of(Duration.ofMinutes(10)), unloadable.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 2)).isTrue();
        long started = System.nanoTime();
        PMap<String, String> seen = onB.deref();
        long waited = (System.nanoTime() - started) / 1_000_000;
        assertThat(seen.root()).as("the unloadable head is ignored and v1 stays the head").isEqualTo(v1.root());
        assertThat(b.pdt().stats().headsIgnored()).isEqualTo(1);
        assertThat(waited).as("deref waited the load timeout once").isGreaterThanOrEqualTo(250).isLessThan(5_000);
        started = System.nanoTime();
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        assertThat((System.nanoTime() - started) / 1_000_000).as("a second deref does not wait again").isLessThan(250);
        assertThat(b.pdt().stats().headsIgnored()).isEqualTo(1);
        fleet.settle(4);
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        System.out.printf("S7: a head with an unloadable root was ignored after %d ms; headsIgnored=%d%n", waited, b.pdt().stats().headsIgnored());
    }

    @Test
    void aLazyFollowerFetchesNothingOnAHeadAndLoadsNodesOnGet() throws Exception {
        RefFleet.Spec lazy = RefFleet.Spec.defaults().pdt(b -> b.follow(Pdt.FOLLOW_LAZY));
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, lazy, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        int entries = 2_000;
        PMap<String, String> v1 = onA.swap(m -> {
            Random random = new Random(3);
            Editor<String, String> editor = m.editor();
            for (int i = 0; i < entries; i++) {
                editor.assoc(RefFleet.key(i), RefFleet.value(100, random));
            }
            return editor.done().value();
        });
        String root = v1.root();
        int total = a.store().reachable(root).size();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 1)).isTrue();
        fleet.settle(8);
        assertThat(b.content().stats().nodesReceived()).as("lazy: nothing fetched on the head").isZero();
        assertThat(b.store().size()).isZero();
        assertThat(b.content().activeTreeFetches()).isEmpty();
        assertThat(b.pdt().stats().deltasFetched()).isZero();

        CompletableFuture<Optional<String>> read = CompletableFuture.supplyAsync(() -> {
            PMap<String, String> lazyMap = onB.deref();
            return lazyMap.get(RefFleet.key(1234));
        }, r -> Thread.ofVirtual().start(r));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), read::isDone)).isTrue();
        assertThat(read.get()).isEqualTo(v1.get(RefFleet.key(1234)));
        assertThat(b.content().stats().nodesReceived()).as("the get loaded its path").isGreaterThan(0);
        assertThat(b.store().size()).isGreaterThan(0).isLessThan(total);
        assertThat(b.store().complete(root)).isFalse();
        System.out.printf("PD-V-06 lazy follow: deref plus one get loaded %d of %d nodes%n", b.store().size(), total);
    }
}
