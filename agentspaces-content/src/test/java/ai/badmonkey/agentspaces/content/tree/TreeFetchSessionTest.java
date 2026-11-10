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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.Messages;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fetcher alone over {@link SimTreeFabric} (srad/agentspaces-pdt.md PD-X-04 to
 * PD-X-08, PD-P-02, §6.7 row 4): a holder that names a foreign CID in a frontier
 * or a missing list is struck and steers nothing, a CID nobody holds makes the
 * fetch wait and fail at its timeout, an answer that cannot be used releases its
 * request without a strike, the endgame duplicates only late batches, and the
 * download budget stops the fetch until it is restored.
 */
class TreeFetchSessionTest {

    private static final int TICK = 250;

    private static NodeFixtures.Tree v1() {
        return NodeFixtures.tree(21, 3, 3, 400);
    }

    private static NodeFixtures.Tree v0() {
        return NodeFixtures.tree(21, 3, 3, 400, Set.of(0, 1, 2, 9), 77);
    }

    private static NodeFixtures.Tree foreign() {
        return NodeFixtures.tree(99, 2, 2, 300);
    }

    private static TreeFetchSession start(SimTreeFabric fabric, MemoryTreeStore mine, String root, List<String> haves,
                                          FetchOptions options) {
        TreeFetchSession session = new TreeFetchSession(root, haves, List.of(), fabric, mine, options, 16, 32);
        fabric.session = session;
        session.start();
        fabric.pump();
        return session;
    }

    private static long served(SimTreeFabric.Holder... holders) {
        long n = 0;
        for (SimTreeFabric.Holder h : holders) {
            n += h.server.nodesServed();
        }
        return n;
    }

    @Test
    void aForeignFrontierCidStrikesTheHolderAndStoresNoForeignNode() {
        NodeFixtures.Tree v1 = v1();
        NodeFixtures.Tree f = foreign();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder hostile = fabric.holder("zHostile").holds(v1).holds(f);
        SimTreeFabric.Holder honest = fabric.holder("zHonest").holds(v1);
        hostile.tamper = m -> m instanceof Messages.TreeEnd end
                ? new Messages.TreeEnd(end.req(), List.of(f.root()), end.reason()) : m;
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(), FetchOptions.defaults().prefer(hostile.id));

        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(mine.complete(v1.root())).isTrue();
        assertThat(fabric.strikes).hasSize(1);
        assertThat(fabric.strikes.get(0)).contains("zHostile").contains("frontier").contains(f.root());
        assertThat(session.foreignCidsRefused()).isEqualTo(1);
        for (String cid : f.nodes().keySet()) {
            assertThat(mine.has(cid)).as("no foreign node stored").isFalse();
        }
        assertThat(mine.size()).isEqualTo(v1.nodes().size());
        assertThat(session.progress().bytesBySource().get(honest.id)).isGreaterThan(0L);
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("struck peer:zHostile"));
    }

    @Test
    void aForeignMissingCidInATreeWantAnswerStrikesTheHolder() {
        NodeFixtures.Tree v1 = v1();
        NodeFixtures.Tree f = foreign();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder hostile = fabric.holder("zHostile").holds(v1);
        fabric.holder("zHonest").holds(v1);
        hostile.tamper = m -> {
            if (m instanceof Messages.Nodes nodes && nodes.nodes() != null) {
                List<String> missing = new ArrayList<>();
                if (nodes.missing() != null) {
                    missing.addAll(nodes.missing());
                }
                missing.add(f.root());
                return Messages.Nodes.plain(nodes.req(), nodes.nodes(), missing);
            }
            return m;
        };
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(), FetchOptions.defaults().prefer(hostile.id));

        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(fabric.strikes).isNotEmpty();
        assertThat(fabric.strikes.get(0)).contains("zHostile").contains("missing").contains(f.root());
        assertThat(mine.has(f.root())).isFalse();
        // the NODE_WANT path ignores a missing entry it never asked for instead of striking
        assertThat(fabric.sentBySession).anyMatch(m -> m instanceof Messages.NodeWant);
    }

    @Test
    void aNodeWantMissingEntryNobodyAskedForIsIgnored() {
        NodeFixtures.Tree v1 = v1();
        NodeFixtures.Tree f = foreign();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder odd = fabric.holder("zOdd").holds(v1);
        odd.tamper = m -> m instanceof Messages.Nodes nodes && nodes.nodes() != null && odd.askedByNodeWant(nodes.req())
                ? Messages.Nodes.plain(nodes.req(), nodes.nodes(), List.of(f.root())) : m;
        MemoryTreeStore mine = new MemoryTreeStore();
        // no haves and a held root: the fetch is one TREE_WANT per child, then NODE_WANT batches from the frontier
        mine.putVerified(v1.root(), v1.nodes().get(v1.root()));
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(f.root()), FetchOptions.defaults());
        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(mine.has(f.root())).isFalse();
    }

    @Test
    void aCidNobodyHoldsMakesTheFetchWaitAndFailAtTheTimeout() {
        NodeFixtures.Tree v1 = v1();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder partial = fabric.holder("zPartial").holds(v1);
        String leaf = null;
        for (String cid : v1.nodes().keySet()) {
            if (NodeFixtures.linksOf(v1.nodes().get(cid)).isEmpty()) {
                leaf = cid;
                break;
            }
        }
        partial.store.remove(leaf);
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(),
                FetchOptions.defaults().withTimeout(Duration.ofSeconds(5)));

        assertThat(fabric.runUntilDone(80, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isFalse();
        assertThat(session.failure()).isPresent();
        assertThat(session.failure().get().getMessage()).contains(v1.root());
        assertThat(session.progress().nodesVerified()).isEqualTo(v1.nodes().size() - 1);
        assertThat(fabric.strikes).isEmpty();
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("every source lacks"));
    }

    @Test
    void anAnswerThatCannotBeUsedReleasesItsRequestAndStrikesNobody() {
        NodeFixtures.Tree v1 = v1();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder flaky = fabric.holder("zFlaky").holds(v1);
        fabric.holder("zSteady").holds(v1);
        int[] tampered = {0};
        List<String> tooMany = new ArrayList<>();
        for (int i = 0; i <= Messages.MAX_WANT_CIDS; i++) {
            tooMany.add(v1.root());
        }
        // the first NODES of flaky's first TREE_WANT stream breaks a bound and is dropped on arrival
        flaky.tamper = m -> m instanceof Messages.Nodes nodes && tampered[0]++ == 0
                ? Messages.Nodes.plain(nodes.req(), nodes.nodes(), tooMany) : m;
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(), FetchOptions.defaults().prefer(flaky.id));

        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(fabric.strikes).as("the rest of the dropped stream struck nobody").isEmpty();
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("dropped; request released"));

        // the same for an answer this peer cannot open (the wrong key)
        SimTreeFabric sealed = new SimTreeFabric();
        SimTreeFabric.Holder wrongKey = sealed.holder("zWrongKey").holds(v1);
        wrongKey.sealsBadly = true;
        sealed.holder("zRightKey").holds(v1);
        MemoryTreeStore mine2 = new MemoryTreeStore();
        TreeFetchSession second = start(sealed, mine2, v1.root(), List.of(), FetchOptions.defaults().prefer(wrongKey.id));
        assertThat(sealed.runUntilDone(200, TICK)).as(second.trace().toString()).isTrue();
        assertThat(second.complete()).isTrue();
        assertThat(sealed.strikes).isEmpty();
        assertThat(second.trace()).anySatisfy(line -> assertThat(line).contains("unopenable"));
    }

    @Test
    void theEndgameDuplicatesOnlyLateBatches() {
        NodeFixtures.Tree v1 = v1();
        NodeFixtures.Tree v0 = v0();
        // two prompt holders, neither holding the have: the frontier goes out as batches and nothing is late
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder a = fabric.holder("zA").holds(v1);
        SimTreeFabric.Holder b = fabric.holder("zB").holds(v1);
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(v0.root()), FetchOptions.defaults());
        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        System.out.printf("PD-X-05 endgame: %d nodes served for %d verified from two prompt holders, %d requests%n",
                served(a, b), session.progress().nodesVerified(), session.progress().requestsSent());
        assertThat(served(a, b)).isLessThanOrEqualTo((long) (1.1 * v1.nodes().size()));
        assertThat(session.progress().bytesReceived()).isLessThanOrEqualTo((long) (1.1 * v1.bytes()));
        assertThat(session.trace()).noneSatisfy(line -> assertThat(line).contains("endgame"));

        // a holder that goes quiet after the top levels: its batches are late, and they are duplicated
        SimTreeFabric slow = new SimTreeFabric();
        SimTreeFabric.Holder quiet = slow.holder("zQuiet").holds(v1);
        SimTreeFabric.Holder prompt = slow.holder("zPrompt").holds(v1);
        int[] answered = {0};
        quiet.tamper = m -> {
            if (m instanceof Messages.Nodes nodes && nodes.missing() == null && answered[0]++ >= 3) {
                return null; // a NODE_WANT answer that never arrives
            }
            return m;
        };
        MemoryTreeStore mine2 = new MemoryTreeStore();
        TreeFetchSession late = start(slow, mine2, v1.root(), List.of(v0.root()), FetchOptions.defaults().prefer(quiet.id));
        assertThat(slow.runUntilDone(24, TICK)).as("completes within 6 s, well inside the 10 s request timeout: "
                + late.trace()).isTrue();
        assertThat(late.complete()).isTrue();
        assertThat(late.trace()).anySatisfy(line -> assertThat(line).contains("endgame: batch"));
        assertThat(late.progress().bytesBySource().get(prompt.id)).isGreaterThan(0L);
    }

    @Test
    void theDownloadBudgetStopsTheFetchUntilItIsRestored() {
        // four subtrees of 128 KiB: the budget runs out inside the first answer, so the last subtree is never asked for
        NodeFixtures.Tree v1 = NodeFixtures.tree(21, 4, 4, 2_000);
        SimTreeFabric fabric = new SimTreeFabric();
        fabric.holder("zA").holds(v1);
        fabric.downloadBudget = v1.bytes() / 8;
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(), FetchOptions.defaults());

        assertThat(fabric.runUntilDone(40, TICK)).isFalse();
        int stopped = session.progress().nodesVerified();
        int requests = session.progress().requestsSent();
        assertThat(stopped).isLessThan(v1.nodes().size());
        assertThat(session.progress().waiting()).as("a source exists; the policy holds the fetch").isFalse();
        for (int i = 0; i < 20; i++) {
            fabric.tick(TICK);
        }
        assertThat(session.progress().requestsSent()).as("nothing more is sent while the budget is spent").isEqualTo(requests);
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("download budget spent"));

        fabric.downloadBudget = Long.MAX_VALUE; // the next day
        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(mine.complete(v1.root())).isTrue();
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("available again"));
        System.out.printf("PD-P-02 download budget: stopped at %d of %d nodes after %d requests, completed once restored%n",
                stopped, v1.nodes().size(), requests);
    }

    /**
     * Swarm retries, B6 (ROBUSTNESS.md §10.3): a holder that answered {@code missing} for a node is benched for that
     * node alone, for a backoff, and asked again once the bench ends, so a fetch whose only holder comes to hold the
     * node later completes instead of failing at its timeout.
     */
    @Test
    void aHolderThatLackedANodeIsAskedAgainOnceItsBenchEndsAndTheFetchCompletes() {
        NodeFixtures.Tree v1 = v1();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder late = fabric.holder("zLate").holds(v1);
        String leaf = null;
        for (String cid : v1.nodes().keySet()) {
            if (NodeFixtures.linksOf(v1.nodes().get(cid)).isEmpty()) {
                leaf = cid;
                break;
            }
        }
        late.store.remove(leaf);
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(),
                FetchOptions.defaults().withTimeout(Duration.ofSeconds(5)));
        assertThat(fabric.runUntilDone(8, TICK)).as("two seconds: the holder still lacks the leaf").isFalse();
        late.store.putVerified(leaf, v1.nodes().get(leaf)); // the holder's own fetch ran on and brought the leaf
        assertThat(fabric.runUntilDone(80, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).as("asked again after its bench, the holder serves the leaf").isTrue();
        assertThat(mine.has(leaf)).isTrue();
        assertThat(fabric.strikes).isEmpty();
        assertThat(late.askedByNodeWantFor(leaf)).as("the leaf was asked for again by NODE_WANT").isTrue();
    }

    /**
     * Swarm retries, the gate's adversarial review (ROBUSTNESS.md §10.6): a bare {@code NODE_WANT} batch, which names
     * no root, fails at its timeout when every source lacks its CID. Its stall check asked the store whether the
     * absent root was complete, the store refused the null, and every tick threw before the timeout check, so the
     * batch never ended and a lazy load that joined it (P3) waited on it for good.
     */
    @Test
    void aBareBatchThatNoSourceCanServeFailsAtItsTimeout() {
        NodeFixtures.Tree v1 = v1();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder lacking = fabric.holder("zLacking").holds(v1);
        String leaf = null;
        for (String cid : v1.nodes().keySet()) {
            if (NodeFixtures.linksOf(v1.nodes().get(cid)).isEmpty()) {
                leaf = cid;
                break;
            }
        }
        lacking.store.remove(leaf);
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = new TreeFetchSession(null, List.of(), List.of(leaf), fabric, mine,
                FetchOptions.defaults().withTimeout(Duration.ofSeconds(2)).prefer(lacking.id), 16, 32);
        fabric.session = session;
        session.start();
        fabric.pump();
        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.failure()).as("no source holds the CID: the batch fails at its timeout").isPresent();
        assertThat(mine.has(leaf)).isFalse();
    }

    @Test
    void aPartialHolderAnsweringMissingHandsTheGapToAnotherSource() {
        NodeFixtures.Tree v1 = v1();
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder partial = fabric.holder("zPartial").holds(v1);
        SimTreeFabric.Holder full = fabric.holder("zFull").holds(v1);
        String child = NodeFixtures.linksOf(v1.nodes().get(v1.root())).get(0);
        String grandchild = NodeFixtures.linksOf(v1.nodes().get(child)).get(0);
        Set<String> gap = new MemoryTreeStore().putAll(v1).reachable(grandchild);
        for (String cid : gap) {
            partial.store.remove(cid);
        }
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = start(fabric, mine, v1.root(), List.of(), FetchOptions.defaults().prefer(partial.id));
        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        assertThat(fabric.strikes).isEmpty();
        assertThat(session.progress().bytesBySource().get(full.id)).isGreaterThan(0L);
        assertThat(session.progress().bytesBySource().get(partial.id)).isGreaterThan(0L);
        assertThat(session.trace()).anySatisfy(line -> assertThat(line).contains("missing"));
    }

    @Test
    void theRootIsAskedOnceWhileItsAnswerIsOutstanding() {
        NodeFixtures.Tree v1 = v1();
        SimTreeFabric fabric = new SimTreeFabric();
        fabric.holder("zOnly").holds(v1);
        MemoryTreeStore mine = new MemoryTreeStore();
        TreeFetchSession session = new TreeFetchSession(v1.root(), List.of(), List.of(), fabric, mine,
                FetchOptions.defaults(), 16, 32);
        fabric.session = session;
        session.start();
        // the capability ticks the session before the holder's answer lands; the window has room for a second ask
        fabric.now += TICK;
        session.tick();
        fabric.now += TICK;
        session.tick();
        long rootWants = fabric.sentBySession.stream()
                .filter(m -> m instanceof Messages.NodeWant want && want.cids().equals(List.of(v1.root()))).count();
        assertThat(rootWants).as(session.trace().toString()).isEqualTo(1);

        assertThat(fabric.runUntilDone(200, TICK)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();
        long treeBytes = 0;
        for (byte[] node : v1.nodes().values()) {
            treeBytes += node.length;
        }
        assertThat(session.progress().bytesReceived()).as(session.trace().toString()).isEqualTo(treeBytes);
        assertThat(session.progress().nodesVerified()).isEqualTo(v1.nodes().size());
    }
}
