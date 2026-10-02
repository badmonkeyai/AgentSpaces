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

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The C2 executor: dispatch/cancel, directives, the command SPI, and the audit log. */
class FleetCommanderTest {

    public record Job(String name, int weight) {
    }

    private final AgentId consoleId =
            new AgentId(PeerIdentity.generate().peerId(), "console");
    private final Space tasks = LocalSpace.builder("tasks", consoleId).build();
    private final Space control = LocalSpace.builder("fleet-control", consoleId).build();
    private final Space audit = LocalSpace.builder("c2-audit", consoleId).build();

    private FleetCommander commander(ConsoleView view, ConsoleCommand... commands) {
        return FleetCommander.builder(name -> switch (name) {
            case "tasks" -> tasks;
            case "fleet-control" -> control;
            case "c2-audit" -> audit;
            default -> null;
        }).auditSpace("c2-audit").view(view).commands(List.of(commands)).build();
    }

    @Test
    void broadcastWritesADirectiveAndAuditsIt() {
        ConsoleView view = ConsoleView.builder().space("control", control, Directive.class)
                .build();
        FleetCommander commander = commander(view);

        ConsoleCommand.Outcome outcome = commander.broadcast("pause", "worker-a", "alice");
        assertThat(outcome.ok()).isTrue();

        List<Directive> directives = control.readAll(Template.of(Directive.class), 10);
        assertThat(directives).hasSize(1);
        assertThat(directives.get(0).action()).isEqualTo("PAUSE");
        assertThat(directives.get(0).target()).isEqualTo("worker-a");
        assertThat(directives.get(0).operator()).isEqualTo("alice");
        assertThat(audit.readAll(Template.of(FleetCommander.C2Record.class), 10))
                .anySatisfy(r -> assertThat(r.kind()).isEqualTo("directive"));
        assertThat(view.eventsAfter(0)).anySatisfy(e ->
                assertThat(e.kind()).isEqualTo("command"));
    }

    @Test
    void aRegisteredCommandDispatchesAndTheConsoleCanCancelIt() {
        ConsoleCommand dispatchJob = new ConsoleCommand() {
            @Override
            public String id() {
                return "dispatch-job";
            }

            @Override
            public String title() {
                return "Dispatch a job";
            }

            @Override
            public List<Field> fields() {
                return List.of(Field.text("name", "Name"), Field.number("weight", "Weight"));
            }

            @Override
            public Outcome execute(Map<String, String> args, CommandContext ctx) {
                String id = ctx.dispatch("tasks",
                        new Job(args.get("name"), Integer.parseInt(args.get("weight"))),
                        Duration.ofMinutes(10));
                return Outcome.dispatched("dispatched " + args.get("name"), id);
            }
        };
        FleetCommander commander = commander(null, dispatchJob);

        ConsoleCommand.Outcome outcome = commander.execute("dispatch-job",
                Map.of("name", "indexing", "weight", "5"), "bob");
        assertThat(outcome.ok()).isTrue();
        assertThat(outcome.entryId()).isNotNull();
        assertThat(tasks.readAll(Template.of(Job.class), 10))
                .containsExactly(new Job("indexing", 5));

        // The console can cancel what it dispatched.
        ConsoleCommand.Outcome cancelled =
                commander.cancelDispatched(outcome.entryId(), "bob");
        assertThat(cancelled.ok()).isTrue();
        assertThat(tasks.readAll(Template.of(Job.class), 10)).isEmpty();

        // Cancelling something it never dispatched fails cleanly.
        assertThat(commander.cancelDispatched("00000000-0000-0000-0000-000000000000", "bob").ok())
                .isFalse();
    }

    @Test
    void missingRequiredFieldsAndUnknownCommandsFailWithoutThrowing() {
        ConsoleCommand needsName = new ConsoleCommand() {
            @Override
            public String id() {
                return "needs-name";
            }

            @Override
            public String title() {
                return "Needs a name";
            }

            @Override
            public List<Field> fields() {
                return List.of(Field.text("name", "Name"));
            }

            @Override
            public Outcome execute(Map<String, String> args, CommandContext ctx) {
                return Outcome.ok("ran");
            }
        };
        FleetCommander commander = commander(null, needsName);

        assertThat(commander.execute("needs-name", Map.of(), "op").ok()).isFalse();
        assertThat(commander.execute("no-such-command", Map.of(), "op").ok()).isFalse();
        assertThat(commander.execute("needs-name", Map.of("name", "x"), "op").ok()).isTrue();
    }

    @Test
    void directiveValidationRejectsBlankActionOrTarget() {
        FleetCommander commander = commander(null);
        assertThat(commander.broadcast("", "worker-a", "op").ok()).isFalse();
        assertThat(commander.broadcast("PAUSE", "", "op").ok()).isFalse();
    }
}
