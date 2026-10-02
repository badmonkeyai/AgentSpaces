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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The console model over a {@link LocalSpace}: counting, attribution, events. */
class ConsoleViewTest {

    /** A result entry naming its worker, the attribution pattern. */
    record WorkerFinding(String topic, String worker) {
    }

    private static final Lease LONG = Lease.of(Duration.ofHours(1));

    private final TestClock clock = TestClock.create();
    private final PeerId self = PeerIdentity.generate().peerId();
    private final LocalSpace space = LocalSpace.builder("tasks",
            new AgentId(self, "console")).clock(clock).build();
    private ConsoleView view;

    @AfterEach
    void tearDown() {
        if (view != null) {
            view.close();
        }
    }

    private ConsoleView newView() {
        view = ConsoleView.builder()
                .space("tasks", space, TaskEntry.class)
                .results("tasks", WorkerFinding.class,
                        entry -> ((WorkerFinding) entry).worker())
                .clock(clock)
                .build();
        return view;
    }

    @Test
    void countsWrittenQueuedInProgressAndCompleted() {
        ConsoleView v = newView();
        space.write(new TaskEntry("one", 1), LONG);
        space.write(new TaskEntry("two", 2), LONG);

        ConsoleView.SpaceStats stats = v.spaces().get(0);
        assertThat(stats.written()).isEqualTo(2);
        assertThat(stats.queued()).isEqualTo(2);
        assertThat(stats.inProgress()).isZero();
        assertThat(stats.completed()).isZero();

        Optional<TakenEntry<TaskEntry>> taken = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(5)), Duration.ZERO);
        assertThat(taken).isPresent();
        stats = v.spaces().get(0);
        assertThat(stats.queued()).isEqualTo(1);
        assertThat(stats.inProgress()).isEqualTo(1);

        space.complete(taken.get(), new WorkerFinding(taken.get().entry().topic(), "worker-a"),
                LONG);
        stats = v.spaces().get(0);
        assertThat(stats.completed()).isEqualTo(1);
        assertThat(stats.inProgress()).isZero();
        assertThat(stats.perWorker()).containsExactly(Map.entry("worker-a", 1));
        assertThat(v.perWorker()).containsExactly(Map.entry("worker-a", 1));
    }

    @Test
    void recordsEventsWithMonotoneSequenceAndAttribution() {
        ConsoleView v = newView();
        space.write(new TaskEntry("evented", 1), LONG);
        Optional<TakenEntry<TaskEntry>> taken = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(5)), Duration.ZERO);
        space.complete(taken.orElseThrow(), new WorkerFinding("evented", "worker-b"), LONG);

        List<ConsoleEvent> events = v.eventsAfter(0);
        assertThat(events).extracting(ConsoleEvent::kind)
                .contains("written", "completed");
        assertThat(events).extracting(ConsoleEvent::seq).isSorted();
        assertThat(events.getLast().seq()).isEqualTo(v.lastSeq());
        assertThat(events).filteredOn(event -> "worker-b".equals(event.worker()))
                .isNotEmpty();
        assertThat(v.eventsAfter(v.lastSeq())).isEmpty();
    }

    @Test
    void eventRingStaysBounded() {
        view = ConsoleView.builder()
                .space("tasks", space, TaskEntry.class)
                .eventCapacity(5)
                .clock(clock)
                .build();
        for (int i = 0; i < 20; i++) {
            space.write(new TaskEntry("burst-" + i, i), LONG);
        }
        List<ConsoleEvent> events = view.eventsAfter(0);
        assertThat(events).hasSize(5);
        assertThat(events.getLast().seq()).isEqualTo(20);
        assertThat(view.spaces().get(0).written()).isEqualTo(20);
    }

    @Test
    void mapsAdvertisementsToRows() {
        GroupId group = GroupId.of("zGroup");
        AgentCard agent = new AgentCard("aspace://a", self, group, Instant.EPOCH,
                Duration.ofDays(1), new AgentId(self, "researcher"), "does research",
                List.of(), List.of("TaskEntry"), List.of("WorkerFinding"), Map.of());
        AssetCard asset = new AssetCard("aspace://b", self, group, Instant.EPOCH,
                Duration.ofDays(1), "orders", "aspace://data/orders", "order history",
                "table", "PT5M", Map.of(), Map.of());
        CapabilityAdvertisement cap = new CapabilityAdvertisement("aspace://c", self,
                group, Instant.EPOCH, Duration.ofDays(1), "aspace:cap/vote", "1",
                "space", Map.of(), Map.of());
        CapabilityAdvertisement semantic = new CapabilityAdvertisement("aspace://d", self,
                group, Instant.EPOCH, Duration.ofDays(1), "aspace:cap/semantic-discovery", "0.1",
                "pipe", Map.of("embedder", "spring-ai:text-embedding-3-small", "dimensions", "1536"), Map.of());
        view = ConsoleView.builder()
                .space("tasks", space, TaskEntry.class)
                .ads(() -> List.of(agent, asset, cap, semantic))
                .clock(clock)
                .build();

        List<ConsoleView.AdRow> rows = view.ads();
        assertThat(rows).extracting(ConsoleView.AdRow::kind)
                .containsExactly("agent", "asset", "capability", "capability");
        assertThat(rows.get(3).detail()).as("each peer's embedder, in the discovery view")
                .isEqualTo("v0.1 via pipe, embedder spring-ai:text-embedding-3-small (1536 dimensions)");
        assertThat(rows.get(0).title()).isEqualTo("researcher");
        assertThat(rows.get(0).detail()).isEqualTo("does research");
        assertThat(rows.get(1).title()).isEqualTo("orders");
        assertThat(rows.get(1).detail()).contains("aspace://data/orders");
        assertThat(rows.get(2).title()).isEqualTo("aspace:cap/vote");
    }

    /** SPEC §6.1 v0.1.13: an agent row names the card's declared actions. */
    @Test
    void anAgentRowNamesItsDeclaredActions() {
        GroupId group = GroupId.of("zGroup");
        AgentCard agent = new AgentCard("aspace://a", self, group, Instant.EPOCH,
                Duration.ofDays(1), new AgentId(self, "desk"), "a desk",
                List.of(), List.of("TaskEntry"), List.of("WorkerFinding"), Map.of())
                .withActions(List.of(
                        new ai.badmonkey.agentspaces.api.ad.CardAction("audit", "", List.of("WorkerFinding"),
                                List.of(), "tasks", ai.badmonkey.agentspaces.api.ad.CardAction.NOTIFY),
                        new ai.badmonkey.agentspaces.api.ad.CardAction("research", "", List.of("TaskEntry"),
                                List.of("WorkerFinding"), "tasks", ai.badmonkey.agentspaces.api.ad.CardAction.TAKE)));
        view = ConsoleView.builder().space("tasks", space, TaskEntry.class)
                .ads(() -> List.of(agent)).clock(clock).build();
        assertThat(view.ads().get(0).detail()).isEqualTo("a desk (actions: audit, research)");
    }

    @Test
    void builderRejectsResultsForUnknownSpaceAndDuplicateNames() {
        assertThatThrownBy(() -> ConsoleView.builder()
                .results("nope", WorkerFinding.class, entry -> ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown space");
        LocalSpace other = LocalSpace.builder("tasks",
                new AgentId(self, "other")).clock(clock).build();
        assertThatThrownBy(() -> ConsoleView.builder()
                .space("tasks", space, TaskEntry.class)
                .space("tasks", other, TaskEntry.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already watched");
    }

    @Test
    void membersDefaultsEmptyWithoutASupplier() {
        ConsoleView v = newView();
        assertThat(v.members()).isEmpty();
        assertThat(v.ads()).isEmpty();
    }
}
