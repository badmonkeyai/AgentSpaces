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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-Q-MNT-02: a tree fetch's trace names its sources, every request, the bytes
 * per source, what the holder pruned (a {@code TREE_END} done after pruning under
 * the haves, with its frontier size), and a strike.
 */
class TreeFetchTraceTest {

    @Test
    void theTraceOfAHeldBaseFetchNamesSourcesRequestsBytesPruningAndAStrike() {
        NodeFixtures.Tree v0 = NodeFixtures.tree(31, 3, 3, 400);
        NodeFixtures.Tree v1 = NodeFixtures.tree(31, 3, 3, 400, Set.of(0, 1, 2), 5);
        SimTreeFabric fabric = new SimTreeFabric();
        SimTreeFabric.Holder a = fabric.holder("zAlpha").holds(v0).holds(v1);
        MemoryTreeStore mine = new MemoryTreeStore().putAll(v0);
        TreeFetchSession session = new TreeFetchSession(v1.root(), List.of(v0.root()), List.of(), fabric, mine,
                FetchOptions.defaults(), 16, 32);
        fabric.session = session;
        session.start();
        assertThat(fabric.runUntilDone(100, 250)).as(session.trace().toString()).isTrue();
        assertThat(session.complete()).isTrue();

        List<String> trace = session.trace();
        assertThat(trace).anySatisfy(line -> assertThat(line).startsWith("source peer:zAlpha"));
        assertThat(trace).anySatisfy(line -> assertThat(line).startsWith("NODE_WANT ").contains("to peer:zAlpha").contains("1 cids"));
        assertThat(trace).anySatisfy(line -> assertThat(line).startsWith("TREE_WANT ").contains("to peer:zAlpha").contains("1 haves"));
        assertThat(trace).anySatisfy(line -> assertThat(line).contains("ended (1)").contains("after pruning under the haves"));
        assertThat(trace).anySatisfy(line -> assertThat(line).startsWith("complete:").contains("by source: peer:zAlpha")
                .contains(session.progress().bytesBySource().get(a.id) + " bytes"));
        long requestLines = trace.stream().filter(l -> l.startsWith("NODE_WANT ") || l.startsWith("TREE_WANT ")).count();
        assertThat(requestLines).isEqualTo(session.progress().requestsSent());

        // a strike appears by name
        SimTreeFabric bad = new SimTreeFabric();
        SimTreeFabric.Holder liar = bad.holder("zLiar").holds(v1);
        bad.holder("zTrue").holds(v1);
        String child = NodeFixtures.linksOf(v1.nodes().get(v1.root())).get(0);
        liar.store.corrupt(child, NodeFixtures.node(List.of(), new byte[]{(byte) 'x'}));
        MemoryTreeStore mine2 = new MemoryTreeStore();
        TreeFetchSession struck = new TreeFetchSession(v1.root(), List.of(), List.of(), bad, mine2,
                FetchOptions.defaults().prefer(liar.id), 16, 32);
        bad.session = struck;
        struck.start();
        assertThat(bad.runUntilDone(100, 250)).as(struck.trace().toString()).isTrue();
        assertThat(struck.complete()).isTrue();
        assertThat(struck.trace()).anySatisfy(line -> assertThat(line).startsWith("struck peer:zLiar"));
        assertThat(bad.strikes).hasSize(1);
        assertThat(bad.sentBySession).anyMatch(m -> m instanceof Messages.TreeWant);
    }
}
