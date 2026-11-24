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

package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SPEC §6.1 v0.1.38: the threads an AgentCard names (srad/cards.md §3.9, CA-C-05). */
class ChatThreadTest {

    static AgentCard card(List<ChatThread> threads) {
        return new AgentCard("aspace://zApps/agent/zPeerA/policy-helper", PeerId.of("zPeerA"), GroupId.of("zApps"), Instant.parse("2026-11-23T10:00:00Z"),
                Duration.ofMinutes(15), new AgentId(PeerId.of("zPeerA"), "policy-helper"), "Answers the travel policy", List.of(),
                List.of("ChatMessage"), List.of("ChatMessage", "PromptedResult"), Map.of(), Map.of("ChatMessage", "conversations"), null,
                List.of(new CardAction("answer", "Answers each ChatMessage of its threads", List.of("ChatMessage"), List.of("ChatMessage"), "conversations", CardAction.NOTIFY)),
                null, null, null, threads);
    }

    @Test
    void aThreadIsASpaceAKeyAndAScope() {
        ChatThread tree = new ChatThread("conversations", "main", ChatThread.TREE);
        assertThat(tree.takes("conversations", "main")).isTrue();
        assertThat(tree.takes("conversations", "main:3fa8")).isTrue();
        assertThat(tree.takes("conversations", "mainly")).isFalse();
        assertThat(tree.takes("results", "main")).isFalse();
        ChatThread exact = new ChatThread("conversations", "agent-policy-helper", ChatThread.EXACT);
        assertThat(exact.takes("conversations", "agent-policy-helper:x")).isFalse();
        ChatThread all = new ChatThread("conversations", "", ChatThread.ALL);
        assertThat(all.takes("conversations", "anything")).isTrue();
        assertThat(all.overlaps(exact)).isTrue();
        assertThat(tree.overlaps(new ChatThread("conversations", "main:3fa8", ChatThread.EXACT))).isTrue();
        assertThat(tree.overlaps(exact)).isFalse();
        assertThat(tree.overlaps(new ChatThread("other", "main", ChatThread.TREE))).as("another space").isFalse();
        assertThatThrownBy(() -> new ChatThread("conversations", "main", "some")).hasMessageContaining("thread.scope");
        assertThatThrownBy(() -> new ChatThread("conversations", "main", ChatThread.ALL)).hasMessageContaining("empty when the scope is all");
        assertThatThrownBy(() -> new ChatThread("conversations", "", ChatThread.EXACT)).hasMessageContaining("thread.key");
        assertThatThrownBy(() -> new ChatThread("", "main", ChatThread.EXACT)).hasMessageContaining("thread.space");
        assertThatThrownBy(() -> new ChatThread("conversations", "a b", ChatThread.EXACT)).hasMessageContaining("white space");
        assertThatThrownBy(() -> new ChatThread("conversations", "k".repeat(129), ChatThread.EXACT)).hasMessageContaining("1 to 128");
    }

    @Test
    void aCardNamesAtMostSixteenDistinctThreadsAndRoundTrips() throws Exception {
        CborCodec codec = CborCodec.defaultCodec();
        AgentCard withThreads = card(List.of(new ChatThread("conversations", "agent-policy-helper", ChatThread.TREE)));
        assertThat(codec.fromBytes(codec.toBytes(withThreads), AgentCard.class)).isEqualTo(withThreads);
        assertThat(codec.fromBytes(codec.toBytes(withThreads), Map.class)).containsKey("threads");

        // a card with no threads omits the field and keeps its v0.1.37 bytes; withThreads writes an empty list as absent,
        // and an empty list on the wire is not canonical, so a reader refuses it
        AgentCard none = card(null);
        assertThat(codec.fromBytes(codec.toBytes(none), Map.class)).doesNotContainKey("threads");
        assertThat(withThreads.withThreads(List.of()).threads()).isNull();
        assertThat(codec.toBytes(withThreads.withThreads(List.of()))).isEqualTo(codec.toBytes(none));
        assertThat(none.withThreads(withThreads.threads())).isEqualTo(withThreads);
        assertThatThrownBy(() -> card(List.of())).hasMessageContaining("empty threads list is refused");

        assertThatThrownBy(() -> card(Collections.nCopies(17, new ChatThread("conversations", "x", ChatThread.EXACT))))
                .hasMessageContaining("at most 16 threads");
        ChatThread one = new ChatThread("conversations", "x", ChatThread.EXACT);
        assertThatThrownBy(() -> card(List.of(one, one))).hasMessageContaining("distinct");
    }
}
