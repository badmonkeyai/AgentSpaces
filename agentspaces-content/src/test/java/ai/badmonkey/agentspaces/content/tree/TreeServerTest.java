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

import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Messages;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-X-03 and PD-P-02 on the holder alone, with a recording host: the breadth-first
 * walk, pruning under held haves, the top two levels when no haves root is held,
 * the budget and walk-cap stops, the NODE_WANT order, and the refusals.
 */
class TreeServerTest {

    private static final PeerId ASKER = PeerId.of("zAsker");

    private static final class RecordingHost implements TreeServer.Host {
        final List<Object> sent = new ArrayList<>();
        boolean allow = true;
        /** When set, the CIDs the grant covers (PD-P-01); null permits every CID. */
        Set<String> allowed;
        boolean sealFails;
        GroupKey key;
        long uploadBudget = Long.MAX_VALUE;
        long now = 5_000;

        @Override
        public boolean mayServe(PeerId from, String cid) {
            return allow && (allowed == null || allowed.contains(cid));
        }

        @Override
        public long nowMillis() {
            return now;
        }

        @Override
        public boolean withinUploadBudget(int bytes) {
            if (uploadBudget < bytes) {
                return false;
            }
            uploadBudget -= bytes;
            return true;
        }

        @Override
        public Optional<Messages.Nodes> seal(PeerId to, int req, List<byte[]> nodes, List<String> missing) {
            if (sealFails) {
                return Optional.empty();
            }
            if (key != null) {
                byte[] sealed = key.encrypt(NodeFixtures.CODEC.toBytes(nodes.toArray(new byte[0][])),
                        (Messages.SEALING_AAD_PREFIX + to.value() + "|" + req).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                return Optional.of(Messages.Nodes.sealed(req, sealed, 1, missing));
            }
            return Optional.of(Messages.Nodes.plain(req, nodes, missing));
        }

        @Override
        public void send(PeerId to, Object message) {
            sent.add(message);
        }

        List<byte[]> nodes() {
            List<byte[]> all = new ArrayList<>();
            for (Object m : sent) {
                if (m instanceof Messages.Nodes n && n.nodes() != null) {
                    all.addAll(n.nodes());
                }
            }
            return all;
        }

        Set<String> cids() {
            Set<String> cids = new HashSet<>();
            for (byte[] node : nodes()) {
                cids.add(NodeFixtures.cid(node));
            }
            return cids;
        }

        Messages.TreeEnd end() {
            Object last = sent.get(sent.size() - 1);
            assertThat(last).isInstanceOf(Messages.TreeEnd.class);
            return (Messages.TreeEnd) last;
        }
    }

    @Test
    void aWalkWithAHeldHaveSendsOnlyTheNodesTheHaveLacksBreadthFirst() {
        NodeFixtures.Tree v0 = NodeFixtures.tree(1, 3, 3, 100);
        NodeFixtures.Tree v1 = NodeFixtures.tree(1, 3, 3, 100, Set.of(0, 1, 26), 2);
        MemoryTreeStore store = new MemoryTreeStore().putAll(v0).putAll(v1);
        RecordingHost host = new RecordingHost();
        TreeServer server = new TreeServer(store, host);
        server.serve(ASKER, new Messages.TreeWant(7, v1.root(), List.of(v0.root()), Messages.MAX_TREE_BUDGET));

        Set<String> onlyInV1 = new HashSet<>(v1.nodes().keySet());
        onlyInV1.removeAll(v0.nodes().keySet());
        assertThat(host.cids()).isEqualTo(onlyInV1);
        assertThat(host.end().reason()).isEqualTo(Messages.END_DONE);
        assertThat(host.end().frontier()).isEmpty();
        assertThat(host.end().req()).isEqualTo(7);
        // breadth first: the root comes first, and no node precedes its parent
        List<byte[]> sent = host.nodes();
        assertThat(NodeFixtures.cid(sent.get(0))).isEqualTo(v1.root());
        Set<String> seen = new HashSet<>();
        seen.add(v1.root());
        for (byte[] node : sent) {
            for (String child : NodeFixtures.linksOf(node)) {
                seen.add(child);
            }
            assertThat(seen).contains(NodeFixtures.cid(node));
        }
        assertThat(server.nodesServed()).isEqualTo(onlyInV1.size());
    }

    @Test
    void withoutAnyHeldHaveTheWalkSendsTwoLevelsAndTheFrontier() {
        NodeFixtures.Tree v1 = NodeFixtures.tree(3, 4, 3, 50);
        String unheld = NodeFixtures.tree(4, 1, 2, 10).root();
        MemoryTreeStore store = new MemoryTreeStore().putAll(v1);
        RecordingHost host = new RecordingHost();
        new TreeServer(store, host).serve(ASKER, new Messages.TreeWant(1, v1.root(), List.of(unheld), 1 << 20));
        assertThat(host.nodes()).as("the root and its three children").hasSize(4);
        assertThat(host.end().reason()).isEqualTo(Messages.END_HAVES_NOT_HELD);
        assertThat(host.end().frontier()).as("the nine grandchildren").hasSize(9);
        for (String cid : host.end().frontier()) {
            assertThat(v1.nodes()).containsKey(cid);
        }
    }

    @Test
    void theBudgetAndTheWalkCapStopTheWalkWithAFrontierAndTheUploadBudgetRefuses() {
        NodeFixtures.Tree v1 = NodeFixtures.tree(5, 2, 4, 1000);
        MemoryTreeStore store = new MemoryTreeStore().putAll(v1);
        RecordingHost host = new RecordingHost();
        new TreeServer(store, host).serve(ASKER, new Messages.TreeWant(2, v1.root(), List.of(), 3_000));
        assertThat(host.end().reason()).isEqualTo(Messages.END_BUDGET);
        long sentBytes = host.nodes().stream().mapToLong(n -> n.length).sum();
        assertThat(sentBytes).isLessThanOrEqualTo(3_000);
        assertThat(host.end().frontier()).isNotEmpty();
        assertThat(host.nodes().size() + host.end().frontier().size()).isLessThanOrEqualTo(v1.nodes().size());

        RecordingHost metered = new RecordingHost();
        metered.uploadBudget = 10;
        new TreeServer(store, metered).serve(ASKER, new Messages.TreeWant(3, v1.root(), List.of(), 1 << 20));
        assertThat(metered.sent).hasSize(1);
        assertThat(metered.sent.get(0)).isEqualTo(new Messages.Reject(v1.root(), Messages.TREE_INDEX, Messages.REASON_OVER_BUDGET));

        RecordingHost partial = new RecordingHost();
        partial.uploadBudget = 2_500;
        new TreeServer(store, partial).serve(ASKER, new Messages.TreeWant(4, v1.root(), List.of(), 1 << 20));
        assertThat(partial.end().reason()).isEqualTo(Messages.END_BUDGET);
        assertThat(partial.nodes()).isNotEmpty();
    }

    @Test
    void aNodeWantAnswersInOrderNamesTheMissingAndRefusesByPolicy() {
        NodeFixtures.Tree v1 = NodeFixtures.tree(6, 1, 4, 20);
        MemoryTreeStore store = new MemoryTreeStore().putAll(v1);
        List<String> cids = new ArrayList<>(v1.nodes().keySet());
        String absent = NodeFixtures.tree(7, 0, 1, 5).root();
        cids.add(2, absent);
        RecordingHost host = new RecordingHost();
        TreeServer server = new TreeServer(store, host);
        server.serve(ASKER, new Messages.NodeWant(9, cids));
        assertThat(host.sent).hasSize(1);
        Messages.Nodes answer = (Messages.Nodes) host.sent.get(0);
        assertThat(answer.req()).isEqualTo(9);
        assertThat(answer.missing()).containsExactly(absent);
        List<String> order = new ArrayList<>();
        for (byte[] node : answer.nodes()) {
            order.add(NodeFixtures.cid(node));
        }
        List<String> expected = new ArrayList<>(cids);
        expected.remove(absent);
        assertThat(order).isEqualTo(expected);

        host.allow = false;
        server.serve(ASKER, new Messages.NodeWant(10, cids));
        assertThat(host.sent.get(1)).isEqualTo(new Messages.Reject(cids.get(0), Messages.TREE_INDEX, Messages.REASON_POLICY));
        server.serve(ASKER, new Messages.TreeWant(11, v1.root(), List.of(), 100));
        assertThat(host.sent.get(2)).isEqualTo(new Messages.Reject(v1.root(), Messages.TREE_INDEX, Messages.REASON_POLICY));
        assertThat(server.rejected()).isEqualTo(2);

        host.allow = true;
        server.serve(ASKER, new Messages.TreeWant(12, absent, List.of(), 100));
        assertThat(host.sent.get(3)).isEqualTo(new Messages.Reject(absent, Messages.TREE_INDEX, Messages.REASON_NOT_HELD));

        TreeServer none = new TreeServer(null, host);
        none.serve(ASKER, new Messages.TreeWant(13, v1.root(), List.of(), 100));
        assertThat(host.sent.get(4)).isEqualTo(new Messages.Reject(v1.root(), Messages.TREE_INDEX, Messages.REASON_POLICY));

        List<String> tooMany = new ArrayList<>();
        for (int i = 0; i <= Messages.MAX_WANT_CIDS; i++) {
            tooMany.add(absent);
        }
        server.serve(ASKER, new Messages.NodeWant(14, tooMany));
        assertThat(server.dropped()).isEqualTo(1);
        assertThat(host.sent).hasSize(5);
    }

    @Test
    void theWalkCapStopsTheSendWalkAndBoundsThePruneWalkAcrossSixtyFourHaves() {
        // PD-P-02, §6.7 row 3: a lowered cap of 40 nodes on a 341-node tree
        NodeFixtures.Tree v1 = NodeFixtures.tree(8, 4, 4, 20);
        MemoryTreeStore store = new MemoryTreeStore().putAll(v1);
        RecordingHost host = new RecordingHost();
        TreeServer capped = new TreeServer(store, host, 40);
        capped.serve(ASKER, new Messages.TreeWant(1, v1.root(), List.of(), Messages.MAX_TREE_BUDGET));
        assertThat(host.end().reason()).isEqualTo(Messages.END_WALK_CAP);
        assertThat(host.nodes()).hasSize(40);
        assertThat(host.end().frontier()).isNotEmpty().hasSizeLessThanOrEqualTo(Messages.MAX_FRONTIER);
        for (String cid : host.end().frontier()) {
            assertThat(v1.nodes()).containsKey(cid);
        }
        assertThat(capped.nodesWalked()).isEqualTo(40);

        // 64 held haves of 85 nodes each (5,440 nodes) under a cap of 1,000: the prune walk stops at the cap,
        // the root walk still runs under its own cap, and a repeated request reads the prune set from the cache
        List<String> haves = new ArrayList<>();
        MemoryTreeStore big = new MemoryTreeStore().putAll(v1);
        for (int i = 0; i < Messages.MAX_HAVES; i++) {
            NodeFixtures.Tree have = NodeFixtures.tree(100 + i, 3, 4, 10);
            big.putAll(have);
            haves.add(have.root());
        }
        RecordingHost many = new RecordingHost();
        TreeServer server = new TreeServer(big, many, 1_000);
        server.serve(ASKER, new Messages.TreeWant(2, v1.root(), haves, Messages.MAX_TREE_BUDGET));
        long firstWalk = server.nodesWalked();
        assertThat(many.nodes()).as("nothing under the haves is shared, so the whole tree goes").hasSize(v1.nodes().size());
        assertThat(firstWalk).as("prune walk capped at 1,000 plus the 341-node send walk")
                .isLessThanOrEqualTo(1_000 + v1.nodes().size());
        for (int i = 0; i < 10; i++) {
            server.serve(ASKER, new Messages.TreeWant(3 + i, v1.root(), haves, Messages.MAX_TREE_BUDGET));
        }
        assertThat(server.nodesWalked() - firstWalk).as("ten more requests walk only the tree itself: the prune set is cached")
                .isEqualTo(10L * v1.nodes().size());
        many.now += TreeServer.PRUNE_CACHE_MILLIS + 1;
        server.serve(ASKER, new Messages.TreeWant(20, v1.root(), haves, Messages.MAX_TREE_BUDGET));
        assertThat(server.nodesWalked() - firstWalk).as("after the cache lapses the prune walk runs again, capped")
                .isLessThanOrEqualTo(11L * v1.nodes().size() + 1_000);

        // duplicate haves are walked once
        List<String> repeated = new ArrayList<>();
        for (int i = 0; i < Messages.MAX_HAVES; i++) {
            repeated.add(haves.get(0));
        }
        RecordingHost dup = new RecordingHost();
        TreeServer once = new TreeServer(big, dup, 100_000);
        once.serve(ASKER, new Messages.TreeWant(30, v1.root(), repeated, Messages.MAX_TREE_BUDGET));
        assertThat(once.nodesWalked()).isEqualTo(85 + v1.nodes().size());
        System.out.printf("PD-P-02 prune bound: 64 haves over 5,440 nodes walked %d nodes under a cap of 1,000 (served %d)%n",
                firstWalk, many.nodes().size());
    }

    @Test
    void aSealedBatchAtTheBoundStaysWithinTheMessageLimit() {
        // PD-P-03: 256 leaves of 16 KiB each are 4 MiB of node bytes exactly; every message must still pass violation()
        List<String> cids = new ArrayList<>();
        MemoryTreeStore store = new MemoryTreeStore();
        for (int i = 0; i < 256; i++) {
            byte[] payload = new byte[16_384 - 4];
            new java.util.Random(i).nextBytes(payload);
            byte[] leaf = NodeFixtures.node(List.of(), payload);
            String cid = NodeFixtures.cid(leaf);
            store.putVerified(cid, leaf);
            cids.add(cid);
        }
        long total = 0;
        for (String cid : cids) {
            total += store.get(cid).orElseThrow().length;
        }
        assertThat(total).isGreaterThanOrEqualTo(Messages.MAX_NODES_BYTES - 256 * 4);
        RecordingHost host = new RecordingHost();
        host.key = GroupKey.generate();
        TreeServer server = new TreeServer(store, host);
        server.serve(ASKER, new Messages.NodeWant(5, cids));
        assertThat(host.sent).hasSizeGreaterThanOrEqualTo(2);
        int sealedNodes = 0;
        for (Object m : host.sent) {
            assertThat(m).isInstanceOf(Messages.Nodes.class);
            Messages.Nodes n = (Messages.Nodes) m;
            assertThat(n.violation()).as("message of " + n.sealed().length + " sealed bytes").isEmpty();
            assertThat(n.sealed().length).isLessThanOrEqualTo(Messages.MAX_NODES_BYTES);
            byte[] plain = host.key.decrypt(n.sealed(),
                    (Messages.SEALING_AAD_PREFIX + ASKER.value() + "|5").getBytes(java.nio.charset.StandardCharsets.UTF_8)).orElseThrow();
            sealedNodes += NodeFixtures.CODEC.fromBytes(plain, byte[][].class).length;
        }
        assertThat(sealedNodes).isEqualTo(256);
        assertThat(server.nodesServed()).isEqualTo(256);

        // the plaintext form packs the same way
        RecordingHost plain = new RecordingHost();
        new TreeServer(store, plain).serve(ASKER, new Messages.NodeWant(6, cids));
        for (Object m : plain.sent) {
            assertThat(((Messages.Nodes) m).violation()).isEmpty();
        }
        assertThat(plain.nodes()).hasSize(256);
    }

    @Test
    void aHostThatCannotSealRejectsWithTheRequestRootBeforeSpendingAnything() {
        NodeFixtures.Tree v1 = NodeFixtures.tree(6, 2, 3, 50);
        MemoryTreeStore store = new MemoryTreeStore().putAll(v1);
        RecordingHost host = new RecordingHost();
        host.sealFails = true;
        host.uploadBudget = 1_000_000;
        TreeServer server = new TreeServer(store, host);
        server.serve(ASKER, new Messages.TreeWant(7, v1.root(), List.of(), 1 << 20));
        assertThat(host.sent).containsExactly(new Messages.Reject(v1.root(), Messages.TREE_INDEX, Messages.REASON_POLICY));
        List<String> cids = new ArrayList<>(v1.nodes().keySet());
        server.serve(ASKER, new Messages.NodeWant(8, cids));
        assertThat(host.sent).hasSize(2);
        assertThat(host.sent.get(1)).isEqualTo(new Messages.Reject(cids.get(0), Messages.TREE_INDEX, Messages.REASON_POLICY));
        assertThat(host.uploadBudget).as("no upload budget spent on a request that cannot be sealed").isEqualTo(1_000_000);
        assertThat(server.nodesServed()).isZero();
        assertThat(server.rejected()).isEqualTo(2);
    }

    @Test
    void anUploadBudgetThatRunsOutInsideANodeWantSendsWhatFitsThenRejectsOverBudget() {
        NodeFixtures.Tree v1 = NodeFixtures.tree(6, 1, 4, 100);
        MemoryTreeStore store = new MemoryTreeStore().putAll(v1);
        List<String> cids = new ArrayList<>(v1.nodes().keySet());
        RecordingHost host = new RecordingHost();
        host.uploadBudget = 250; // two leaves of about 105 bytes fit, the rest does not
        new TreeServer(store, host).serve(ASKER, new Messages.NodeWant(9, cids));
        assertThat(host.sent).hasSize(2);
        Messages.Nodes answer = (Messages.Nodes) host.sent.get(0);
        assertThat(answer.nodes()).hasSize(2);
        assertThat(answer.missing()).isNull();
        assertThat(host.sent.get(1)).isEqualTo(new Messages.Reject(cids.get(0), Messages.TREE_INDEX, Messages.REASON_OVER_BUDGET));
    }

    @Test
    void aNodeWantWithOneRefusedCidIsRefusedWholeAndTheGrantIsCheckedPerCid() {
        // PD-P-01, PD-Q-SEC-02: a grant covering one tree does not let a member list another tree's nodes behind it
        NodeFixtures.Tree allowed = NodeFixtures.tree(6, 1, 4, 20);
        NodeFixtures.Tree secret = NodeFixtures.tree(7, 1, 4, 20);
        MemoryTreeStore store = new MemoryTreeStore().putAll(allowed).putAll(secret);
        RecordingHost host = new RecordingHost();
        host.allowed = new HashSet<>(allowed.nodes().keySet());
        TreeServer server = new TreeServer(store, host);
        List<String> mixed = new ArrayList<>(allowed.nodes().keySet());
        mixed.add(secret.root());
        server.serve(ASKER, new Messages.NodeWant(10, mixed));
        assertThat(host.sent).containsExactly(new Messages.Reject(mixed.get(0), Messages.TREE_INDEX, Messages.REASON_POLICY));
        assertThat(server.nodesServed()).isZero();
        server.serve(ASKER, new Messages.TreeWant(11, secret.root(), List.of(), 1 << 20));
        assertThat(host.sent.get(1)).isEqualTo(new Messages.Reject(secret.root(), Messages.TREE_INDEX, Messages.REASON_POLICY));
        server.serve(ASKER, new Messages.NodeWant(12, new ArrayList<>(allowed.nodes().keySet())));
        assertThat(host.nodes()).hasSize(allowed.nodes().size());
        for (byte[] node : host.nodes()) {
            assertThat(secret.nodes()).doesNotContainKey(NodeFixtures.cid(node));
        }
    }
}
