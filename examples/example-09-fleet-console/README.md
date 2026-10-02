# Example 09: Fleet Console

A live observability console derived entirely from state the fabric already
maintains. Membership answers "who is alive", discovery answers "who can do
what", and the replicated space answers "what is the fleet working on". The
console is one more read-only peer, and `agentspaces-console` serves its view
three ways: a single-file web UI, a HAL+JSON API walkable from `/api/v1`, and a
Server-Sent Events activity stream at `/api/v1/events`. No telemetry pipeline
or metrics database sits behind it.

The demo runs a small fleet in which one worker dies mid-task, and the
console shows the queue, the work in progress, the lapsed lease, and the
recovery. It then demonstrates command and control: an operator pauses a
worker, dispatches a task that waits in the queue, and resumes the worker,
all through a control space under the console peer's signed identity.

Narrative walk-through: [examples guide, example 09](../examples-guide.html#ex-09).
Console trust model and API reference: [docs/CONSOLE.md](../../docs/CONSOLE.md).

## What it demonstrates

- **Observability from coordination state.** `ConsoleView` counts written,
  queued, in-progress, and completed tasks from the space's own events, and
  attributes each completion to a worker through the result entry it wrote.
- **Standard interfaces.** The HAL+JSON API is walkable by any hypermedia client
  (Spring HATEOAS included) from `/api/v1`, and the activity stream is plain
  Server-Sent Events.
- **The panel SPI.** A `ConsolePanel` is an ID, a title, and a live JSON
  document. The UI renders it with no panel-specific frontend code, and the API
  serves it under `/api/v1/panels/{id}/data`.
- **Command and control.** `FleetCommander` executes operator commands and
  broadcasts `PAUSE`, `RESUME`, and `DRAIN` directives into a `fleet-control`
  space, with a durable `c2-audit` log. Each worker honors directives through a
  `DirectiveGate`. A token gates the HTTP command routes, and the audit
  attributes every command to the authenticated principal.

## The API in this example

The console model over the console peer:

```java
ConsoleView view = ConsoleView.builder()
        .space("tasks", peer.tasks(), TaskEntry.class)
        .results("tasks", FindingEntry.class, entry -> ((FindingEntry) entry).worker())
        .members(() -> peer.runtime().membership().allMembers())
        .discovery(peer.discovery())
        .build();
```

The served console, with a panel and command and control:

```java
FleetConsoleServer server = FleetConsoleServer.builder(view)
        .fleetName("console-fleet")
        .panel(storyPanel(view))
        .commandAndControl(commander(console, audit, view), token)
        .build();
int port = server.start(7530);
```

A worker that obeys directives:

```java
try (DirectiveGate gate = DirectiveGate.attach(peer.control(), peer.name(), consolePeerId)) {
    while (!Thread.currentThread().isInterrupted()) {
        if (gate.draining()) return;
        if (gate.paused()) { sleep(200); continue; }
        // take, work, complete
    }
}
```

An operator command, registered with the commander:

```java
public ConsoleCommand.Outcome execute(Map<String, String> args, CommandContext ctx) {
    String id = ctx.dispatch("tasks", new TaskEntry(args.get("topic"), priority), Duration.ofMinutes(10));
    return ConsoleCommand.Outcome.dispatched("dispatched '" + args.get("topic") + "'", id);
}
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `ConsoleView.builder()` | `agentspaces-console` | The read-only model: spaces, result attribution, members, discovery |
| `FleetConsoleServer.builder(view)` | `agentspaces-console` | The JDK `HttpServer`: web UI, HAL+JSON API, SSE stream |
| `ConsolePanel` | `agentspaces-console` | The extension point for one more live section |
| `FleetCommander`, `ConsoleCommand`, `CommandContext` | `agentspaces-console` | Operator commands and directive broadcast, with an audit space |
| `DirectiveGate.attach(space, agent, issuer)` | `agentspaces-console` | How a worker honors PAUSE, RESUME, and DRAIN |

Source: [FleetConsole.java](src/main/java/ai/badmonkey/agentspaces/examples/console/FleetConsole.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), peers on ports
7521 to 7523 and the console on HTTP port 7530 (pass another port as the one
argument to change it):

```
mvn -q -pl examples/example-09-fleet-console exec:java
```

Open `http://127.0.0.1:7530/` while the fleet works. The demo prints a
per-run operator token; after the scripted run it keeps the page live for two
more minutes, so you can drive the fleet from the Command & Control panel with
that token.

[FleetConsoleFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/console/FleetConsoleFlowTest.java)
asserts everything through the HTTP API an operator or an external dashboard
would consume:

- `theServedConsoleSeesQueueWipCompletionAndRecovery`: three tasks complete
  despite a crashed worker, the survivor gets the attribution, the panel serves
  live data, and the HAL root links the event stream.
- `theServedConsoleAcceptsAuthenticatedCommandsAndCommandsWorkers`: an
  unauthenticated command gets `401`, an authenticated dispatch completes, and
  the activity stream attributes the command to the authenticated operator.

```
mvn -q -pl examples/example-09-fleet-console test
```

## Next steps

- **Turn it on with two properties.** Under Spring Boot, none of this assembly
  appears. `agentspaces.console.enabled=true` and `agentspaces.console.port`
  serve the same console; `agentspaces.console.command.enabled=true` with a token
  (or `security.oidc.*` for JWT operators) turns on command and control. A
  `ConsoleViewCustomizer` bean supplies worker attribution, and every
  `ConsolePanel` or `ConsoleCommand` bean appears automatically. The
  [starter README](../../agentspaces-spring-boot-starter/README.md) shows each one.
- **Show what your agents know.** A panel is the cheapest way to show
  domain state in the console: an Embabel agent's current plan, the model spend a Spring AI
  advisor has counted, or the tally of an open vote, each as one JSON document.
- **Export to your existing tools.** The HAL+JSON API and the SSE stream are the
  integration points for dashboards and for an OpenTelemetry bridge, so the
  console complements the observability stack you already run.
- **Put the fleet behind operators you already trust.** Under the `MTLS_OIDC`
  profile, the command routes validate the operator's JWT against your identity
  provider and require the `aspace:console:operate` scope, and
  `agentspaces.security.grants.directive-issuer` limits which peers workers obey.
- **See a busy console.** The [Party Bus](../../../agentspaces-partybus/README.md)
  runs thirty-four agents under one console through a simulated week; its
  `fleet/Turret.java` registers domain `ConsolePanel`s over the same server.
