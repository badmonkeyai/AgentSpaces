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
package ai.badmonkey.agentspaces.examples.console;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.console.CommandContext;
import ai.badmonkey.agentspaces.console.ConsoleCommand;
import ai.badmonkey.agentspaces.console.ConsolePanel;
import ai.badmonkey.agentspaces.console.ConsoleView;
import ai.badmonkey.agentspaces.console.DirectiveGate;
import ai.badmonkey.agentspaces.console.FleetCommander;
import ai.badmonkey.agentspaces.console.FleetConsoleServer;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Example 09: the fleet console from USE-CASES.md, now a served surface.
 * Everything shown is derived from state the fabric already maintains:
 * membership answers "who is alive", discovery answers "who can do what",
 * and the replicated space answers "what is the fleet working on". No
 * telemetry pipeline, no metrics database; the console is one more read-only
 * peer, and {@code agentspaces-console} serves its view three ways: a
 * single-file web UI, a HAL+JSON API walkable from {@code /api/v1}, and a
 * Server-Sent Events activity stream.
 *
 * <p>Run: {@code mvn -q -pl examples/example-09-fleet-console exec:java},
 * then open the printed URL while the demo fleet works. The demo also
 * registers one {@link ConsolePanel} to show the extension point: a panel is
 * a title plus a live JSON document, and the UI does the rest.
 *
 * <p>Under Spring Boot none of this assembly appears: the starter serves the
 * same console from two properties ({@code agentspaces.console.enabled},
 * {@code agentspaces.console.port}).
 */
public final class FleetConsole {

    /** A unit of fleet work. */
    public record TaskEntry(String topic, int priority) {
    }

    /** A completed result. */
    public record FindingEntry(String topic, String summary, String worker) {
    }

    /** One assembled peer, with the task space and the C2 control space. */
    public record Peer(String name, PeerNode node, GroupRuntime runtime,
                       DiscoveryService discovery, ReplicatedSpace tasks,
                       ReplicatedSpace control, PeerIdentity identity) {

        /** Shuts the peer down. */
        public void close() {
            control.close();
            tasks.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";

    private FleetConsole() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "fleet-console-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "console-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer.
     *
     * @param name     the agent name
     * @param port     the TCP port
     * @param seedPort an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String name, int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", HOST + ":" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(10), Duration.ofSeconds(2), 2),
                seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "tasks", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        ReplicatedSpace control = ReplicatedSpace.builder(runtime, "fleet-control",
                identity, name).settleWindow(Duration.ofMillis(150)).build();
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, node, runtime, discovery, tasks, control, identity);
    }

    /**
     * Builds the console model over the console peer: task counting, worker
     * attribution from findings, membership, and discovery, all read-only.
     *
     * @param peer the console peer
     * @return the view
     */
    public static ConsoleView view(Peer peer) {
        return ConsoleView.builder()
                .space("tasks", peer.tasks(), TaskEntry.class)
                .results("tasks", FindingEntry.class,
                        entry -> ((FindingEntry) entry).worker())
                .members(() -> peer.runtime().membership().allMembers())
                .discovery(peer.discovery())
                .build();
    }

    /**
     * The demo's extension-point panel: a live document the UI renders as a
     * table without any panel-specific frontend code.
     *
     * @param view the console view the panel reads
     * @return the panel
     */
    public static ConsolePanel storyPanel(ConsoleView view) {
        return new ConsolePanel() {
            @Override
            public String id() {
                return "story";
            }

            @Override
            public String title() {
                return "How this console works";
            }

            @Override
            public Object data() {
                return Map.of(
                        "source", "replica state and leased advertisements only",
                        "eventsObserved", view.lastSeq(),
                        "extension", "register a ConsolePanel bean; this panel is one");
            }
        };
    }

    /**
     * A worker that drains tasks, slowly, so WIP is visible; dies after a
     * number of takes when asked, and honors C2 directives through a
     * {@link DirectiveGate}: a PAUSE holds it off taking, RESUME lets it back
     * in, and DRAIN stops it for good.
     *
     * @param peer      the worker peer
     * @param console   the console peer whose directives the worker obeys
     * @param dieAfter  crash (no complete) after this many completions; -1 never
     * @return the worker thread
     */
    public static Thread runWorker(Peer peer, PeerId console, int dieAfter) {
        return Thread.ofVirtual().start(() -> {
            int completed = 0;
            try (DirectiveGate gate = DirectiveGate.attach(peer.control(), peer.name(), console)) {
                while (!Thread.currentThread().isInterrupted()) {
                    if (gate.draining()) {
                        return; // commanded to drain: finish and stop for good
                    }
                    if (gate.paused()) {
                        sleepQuietly(200); // commanded to pause: hold off taking
                        continue;
                    }
                    Optional<ai.badmonkey.agentspaces.api.space.TakenEntry<TaskEntry>> taken =
                            peer.tasks().take(Template.of(TaskEntry.class),
                                    Lease.of(Duration.ofSeconds(3)), Duration.ofSeconds(2));
                    if (taken.isEmpty()) {
                        continue;
                    }
                    if (dieAfter >= 0 && completed >= dieAfter) {
                        return; // crash mid-take: the lease lapses, the task reappears
                    }
                    sleepQuietly(700); // work happens; the console sees WIP
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    peer.tasks().complete(taken.get(), new FindingEntry(
                            taken.get().entry().topic(), "done", peer.name()),
                            Lease.of(Duration.ofHours(1)));
                    completed++;
                }
            }
        });
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Builds the C2 commander over the console peer: a {@code dispatch-task}
     * command that writes a {@link TaskEntry} into the task space, the control
     * space directives go to, and a durable audit space.
     *
     * @param console the console peer
     * @param audit   the durable C2 audit space
     * @param view    the console view, so commands appear in the activity stream
     * @return the commander
     */
    public static FleetCommander commander(Peer console, ReplicatedSpace audit,
                                           ConsoleView view) {
        return FleetCommander.builder(name -> switch (name) {
            case "tasks" -> console.tasks();
            case "fleet-control" -> console.control();
            case "c2-audit" -> audit;
            default -> null;
        }).auditSpace("c2-audit").view(view).command(dispatchTaskCommand()).build();
    }

    /** The C2 command an operator uses to dispatch a research task. */
    public static ConsoleCommand dispatchTaskCommand() {
        return new ConsoleCommand() {
            @Override
            public String id() {
                return "dispatch-task";
            }

            @Override
            public String title() {
                return "Dispatch a research task";
            }

            @Override
            public String description() {
                return "Write a task into the fleet's task space for a worker to take.";
            }

            @Override
            public List<ConsoleCommand.Field> fields() {
                return List.of(ConsoleCommand.Field.text("topic", "Topic"),
                        ConsoleCommand.Field.number("priority", "Priority"));
            }

            @Override
            public ConsoleCommand.Outcome execute(Map<String, String> args,
                                                  CommandContext ctx) {
                int priority = Integer.parseInt(args.getOrDefault("priority", "1"));
                String id = ctx.dispatch("tasks",
                        new TaskEntry(args.get("topic"), priority),
                        Duration.ofMinutes(10));
                return ConsoleCommand.Outcome.dispatched(
                        "dispatched '" + args.get("topic") + "'", id);
            }
        };
    }

    private static String line(ConsoleView view) {
        ConsoleView.SpaceStats stats = view.spaces().get(0);
        return String.format("members=%d queued=%d in-progress=%d done=%d %s",
                view.members().size(), stats.queued(), stats.inProgress(),
                stats.completed(), view.perWorker());
    }

    /**
     * Runs the demo: a worked fleet plus the served console.
     *
     * @param args optional HTTP port (default 7530)
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        int httpPort = args.length > 0 ? Integer.parseInt(args[0]) : 7530;
        System.out.println("fleet-console: observability from the coordination state\n");
        // Demo-only: generated per run so no fixed credential ships in source
        // (ASF-038); real deployments configure their own operator tokens.
        String token = "demo-" + java.util.UUID.randomUUID();
        Peer console = startPeer("console", 7521, 0);
        Peer workerA = startPeer("worker-a", 7522, 7521);
        Peer workerB = startPeer("worker-b", 7523, 7521);
        List<Peer> fleet = List.of(console, workerA, workerB);
        ReplicatedSpace audit = ReplicatedSpace.builder(console.runtime(), "c2-audit",
                console.identity(), "console").settleWindow(Duration.ofMillis(150)).build();
        try (ConsoleView view = view(console);
             FleetConsoleServer server = FleetConsoleServer.builder(view)
                     .fleetName("console-fleet")
                     .panel(storyPanel(view))
                     .commandAndControl(commander(console, audit, view), token)
                     .build()) {
            int bound = server.start(httpPort);
            System.out.println("console serving at http://" + HOST + ":" + bound + "/");
            System.out.println("HAL API at http://" + HOST + ":" + bound + "/api/v1");
            System.out.println("command & control ENABLED; operator token: " + token + "\n");

            Thread threadA = runWorker(workerA, console.identity().peerId(), 1); // dies after one completion
            Thread threadB = runWorker(workerB, console.identity().peerId(), -1);
            Thread.sleep(1500);

            for (int i = 1; i <= 5; i++) {
                console.tasks().write(new TaskEntry("task-" + i, i),
                        Lease.of(Duration.ofMinutes(10)));
            }
            System.out.println("5 tasks written; watching the fleet work:\n");
            for (int tick = 0; tick < 24 && view.spaces().get(0).completed() < 5; tick++) {
                System.out.println("  " + line(view));
                Thread.sleep(1000);
            }
            System.out.println("\nfinal: " + line(view));
            System.out.println("\nworker-a died mid-task; its lease lapsed, the task");
            System.out.println("reappeared, and worker-b finished it. The console saw");
            System.out.println("all of it from replica state alone.");

            // A scripted C2 demonstration: pause the only live worker, dispatch a
            // task (which then waits), and resume so it gets taken, all through
            // the control space under the console peer's signed identity.
            System.out.println("\n== command & control ==");
            FleetCommander commander = commander(console, audit, view);
            commander.broadcast("PAUSE", "worker-b", "operator");
            System.out.println("operator PAUSED worker-b");
            Thread.sleep(3000); // let the directive replicate over gossip and the gate react
            commander.execute("dispatch-task",
                    Map.of("topic", "operator-dispatched", "priority", "9"), "operator");
            System.out.println("operator dispatched a task via C2");
            Thread.sleep(2500);
            System.out.println("  " + line(view) + "  (task queued: worker-b is paused)");
            commander.broadcast("RESUME", "worker-b", "operator");
            System.out.println("operator RESUMED worker-b");
            for (int tick = 0; tick < 12 && view.spaces().get(0).completed() < 6; tick++) {
                Thread.sleep(500);
            }
            System.out.println("  " + line(view) + "  (worker-b took it after resume)");

            System.out.println("\nThe page is live for two more minutes; open it and drive");
            System.out.println("the fleet from the Command & Control panel (token: " + token + ").");
            Thread.sleep(Duration.ofMinutes(2).toMillis());
            threadA.interrupt();
            threadB.interrupt();
            audit.close();
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
