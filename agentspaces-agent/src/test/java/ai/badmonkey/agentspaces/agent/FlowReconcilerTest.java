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
import ai.badmonkey.agentspaces.agent.flow.FlowManifest;
import ai.badmonkey.agentspaces.agent.flow.FlowReconciler;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ISSUE-FlowManifest FR-3, FR-4, FR-8, FR-9: overrides, record tags, rebinding on a new version, failures by reason. */
class FlowReconcilerTest {

    public record Task(String key, String text) {
    }

    public record Done(String key, String by) {
    }

    @AgentSpec(name = "worker", description = "Takes tasks")
    public static class Worker {
        final AtomicInteger done = new AtomicInteger();

        @SpaceTake(space = "intake", lease = "1m", pollTimeout = "PT0.1S", resultLease = "1m")
        public Done work(Task task) {
            done.incrementAndGet();
            return new Done(task.key(), "worker");
        }
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace intake = LocalSpace.builder("intake", identity.agent("host")).sweepEvery(Duration.ofMillis(50)).build();
    private final LocalSpace fast = LocalSpace.builder("fast", identity.agent("host")).sweepEvery(Duration.ofMillis(50)).build();
    private final LocalSpace flowControl = LocalSpace.builder("flow-control", identity.agent("host")).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zFlow"), null, InstantSource.system())
            .space("intake", intake).space("fast", fast).space("flow-control", flowControl);
    private FlowReconciler reconciler;

    @AfterEach
    void tearDown() {
        if (reconciler != null) {
            reconciler.close();
        }
        binder.close();
        intake.close();
        fast.close();
        flowControl.close();
    }

    @Test
    void overridesAreCheckedAndApplied() throws Exception {
        Worker worker = new Worker();
        assertThatThrownBy(() -> binder.bind(worker, "w", Map.of("banana", "1"), Map.of()))
                .hasMessageContaining("override 'banana' names no attribute");
        assertThatThrownBy(() -> binder.bind(worker, "w", Map.of("nope.lease", "1m"), Map.of()))
                .hasMessageContaining("names no bound method");
        AgentBinder.Bound bound = binder.bind(worker, "w", Map.of("work.space", "fast", "pollTimeout", "PT0.05S"), Map.of("flow", "f", "flowVersion", "3"));
        fast.write(new Task("t-1", "hello"), Lease.of(Duration.ofMinutes(1)));
        for (int i = 0; i < 50 && worker.done.get() == 0; i++) {
            Thread.sleep(100);
        }
        assertThat(worker.done.get()).as("the override moved the take to the fast lane").isEqualTo(1);
        assertThat(fast.readAllEntries(Template.of(Done.class), 10)).singleElement()
                .satisfies(e -> assertThat(e.tags()).containsEntry("flow", "f").containsEntry("flowVersion", "3"));
        assertThat(bound.recordTags()).containsEntry("flow", "f");
        bound.close();
    }

    @Test
    void aManifestBindsRebindsOnANewVersionAndReportsFailures() throws Exception {
        Worker worker = new Worker();
        Map<String, Object> catalogue = Map.of("worker", worker);
        String self = identity.peerId().value();
        FlowManifest v1 = new FlowManifest(1, "claims", 1, "h1", "zFlow", List.of(new FlowManifest.Lane("intake", "LEASE_RACE", "PT0.1S")),
                List.of(new FlowManifest.Binding("worker", "pkg@1", "worker", List.of("all"), "intake", Map.of(), Map.of()),
                        new FlowManifest.Binding("ghost", "pkg@1", "ghost", List.of(self), "intake", Map.of(), Map.of()),
                        new FlowManifest.Binding("worker", "pkg@1", "elsewhere", List.of("zOtherPeer"), "intake", Map.of(), Map.of())),
                List.of(), FlowManifest.FLUSH, "ops", "");
        flowControl.write(v1, Lease.of(Duration.ofDays(1)), v1.tags());
        Map<String, String> hints = new java.util.concurrent.ConcurrentHashMap<>();
        reconciler = FlowReconciler.attach(flowControl, self, Map.of(), binder, name -> Optional.ofNullable(catalogue.get(name)),
                n -> Optional.empty(), null, "flow-control", InstantSource.system(), hints::put);
        FlowReconciler.Conformance c = reconciler.conformance();
        assertThat(c.versions()).containsEntry("claims", 1L);
        assertThat(c.bound()).containsExactly("worker");
        assertThat(c.failures()).containsEntry("ghost", "no class for agent ghost on this peer");
        assertThat(hints).containsEntry("flowVersion", "1").containsEntry("flowConformance", "1");
        intake.write(new Task("t-1", "a"), Lease.of(Duration.ofMinutes(1)));
        for (int i = 0; i < 50 && worker.done.get() == 0; i++) {
            Thread.sleep(100);
        }
        assertThat(worker.done.get()).isEqualTo(1);
        assertThat(intake.readAllEntries(Template.of(Done.class), 10)).singleElement()
                .satisfies(e -> assertThat(e.tags()).containsEntry("flow", "claims").containsEntry("flowVersion", "1"));
        // version 2 moves the worker to the fast lane with an asset the ad cache cannot serve on one binding
        FlowManifest v2 = new FlowManifest(1, "claims", 2, "h2", "zFlow", List.of(),
                List.of(new FlowManifest.Binding("worker", "pkg@1", "worker", List.of("all"), "fast", Map.of("pollTimeout", "PT0.05S"), Map.of()),
                        new FlowManifest.Binding("worker", "pkg@1", "enricher", List.of("all"), "fast", Map.of(), Map.of("policy", "asset:policy-api/getPolicy"))),
                List.of("worker"), FlowManifest.FLUSH, "ops", "ap-1");
        flowControl.write(v2, Lease.of(Duration.ofDays(1)), v2.tags());
        for (int i = 0; i < 50 && !reconciler.conformance().versions().getOrDefault("claims", 0L).equals(2L); i++) {
            Thread.sleep(100);
        }
        c = reconciler.conformance();
        assertThat(c.versions()).containsEntry("claims", 2L);
        assertThat(c.bound()).containsExactly("worker");
        assertThat(c.failures()).containsEntry("enricher", "no card serves asset policy-api/getPolicy").doesNotContainKey("ghost");
        fast.write(new Task("t-2", "b"), Lease.of(Duration.ofMinutes(1)));
        for (int i = 0; i < 50 && worker.done.get() < 2; i++) {
            Thread.sleep(100);
        }
        assertThat(worker.done.get()).isEqualTo(2);
        assertThat(fast.readAllEntries(Template.of(Done.class), 10)).singleElement()
                .satisfies(e -> assertThat(e.tags()).containsEntry("flowVersion", "2"));
        // the old binding is drained: a task in intake is no longer taken
        intake.write(new Task("t-3", "c"), Lease.of(Duration.ofMinutes(1)));
        Thread.sleep(500);
        assertThat(worker.done.get()).isEqualTo(2);
        assertThat(hints).containsEntry("flowVersion", "2");
    }
}
