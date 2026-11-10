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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.annotation.UiComponent;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.UiDeclaration;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ISSUE-UiComponents FR-5: the binder derives the card's {@code ui} from {@code @UiComponent} on the class and its
 * methods, fills the entries from the annotations' types, derives the catalogs, and refuses a control that names no
 * action of the card; an agent without the annotation has no {@code ui}.
 */
class AgentBinderUiTest {

    private final TestClock clock = TestClock.create();
    private final GroupId groupId = GroupId.of("zUi");
    private PeerIdentity identity;
    private LocalSpace tasks;
    private AgentBinder binder;

    /** A standard entry; the IRI is what the fleet speaks (neutral vectors, 2026-11-09). */
    public record SharedItem(String id) {
    }

    @AgentSpec(name = "desk", description = "A desk that shows what it holds")
    @UiComponent(catalog = "example-widgets", component = "ItemPanel", entry = SharedItem.class,
            entryIri = "https://example.org/ns/demo/v1/SharedItem", controls = {"research"})
    public static class Desk {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        @UiComponent(catalog = "example-widgets", component = "MessageInput", id = "ask", entry = TaskEntry.class, role = "control", reads = false, writes = true)
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    @AgentSpec(name = "plain", description = "No ui")
    public static class Plain {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    @AgentSpec(name = "wrong", description = "A control nobody answers")
    @UiComponent(catalog = "example-widgets", component = "ItemPanel", controls = {"fly"})
    public static class Wrong {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "done");
        }
    }

    @BeforeEach
    void setUp() {
        identity = PeerIdentity.generate();
        tasks = LocalSpace.builder("tasks", identity.agent("host")).build();
        binder = new AgentBinder(identity, groupId, null, clock);
        binder.space("tasks", tasks);
    }

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
    }

    @Test
    void theDeclarationIsDerivedFromTheAnnotations() {
        AgentCard card = binder.bind(new Desk()).card();
        UiDeclaration ui = card.ui();
        assertThat(ui).isNotNull();
        assertThat(ui.protocol()).isEqualTo("a2ui");
        assertThat(ui.components()).extracting(UiDeclaration.Component::id).containsExactly("ItemPanel", "ask");
        assertThat(ui.components().get(0).controls()).containsExactly("research");
        assertThat(ui.components().get(0).entry()).isEqualTo("https://example.org/ns/demo/v1/SharedItem");
        assertThat(ui.components().get(1).role()).isEqualTo("control");
        assertThat(ui.components().get(1).entry()).isEqualTo(TaskEntry.class.getName());
        assertThat(ui.entries().reads()).containsExactly("https://example.org/ns/demo/v1/SharedItem");
        assertThat(ui.entries().writes()).containsExactly(TaskEntry.class.getName());
        assertThat(ui.catalogIds()).containsExactly("example-widgets");
        assertThat(card.declaresUi()).isTrue();
        assertThat(card.actions()).extracting(a -> a.name()).contains("research");
    }

    @Test
    void anAgentWithoutTheAnnotationHasNoUi() {
        assertThat(binder.bind(new Plain()).card().ui()).isNull();
    }

    @Test
    void aControlThatIsNotAnActionIsRefusedAtBind() {
        assertThatThrownBy(() -> binder.bind(new Wrong())).hasMessageContaining("control 'fly'");
    }

    @Test
    void refreshKeepsTheDeclaration() {
        AgentBinder.Bound bound = binder.bind(new Desk());
        clock.advance(java.time.Duration.ofMinutes(1));
        binder.refreshCards();
        assertThat(bound.card().ui()).isNotNull();
        assertThat(bound.card().ui().components()).hasSize(2);
        assertThat(List.of()).isEmpty();
    }
}
