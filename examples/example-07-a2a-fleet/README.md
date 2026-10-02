# Example 07: A2A Fleet

An A2A client drives a peer-to-peer fleet through one gateway. Worker agents
join a replicated task space over TCP and publish AgentCards. The gateway
serves those cards on the standard A2A discovery interface
(`/.well-known/agent-card.json`) and speaks the A2A task interface as
JSON-RPC 2.0 over HTTP: `message/send` writes a leased task entry into the
space, and `tasks/get` reads the task's state out of space semantics
(`submitted` while unclaimed, `working` while an agent holds the take lease,
`completed` once a result entry exists).

The demo sends "reconcile the ledgers" to the analyst agent with the JDK's
plain `HttpClient` and polls the task to completion. The A2A client never sees
the space, and the fleet never sees HTTP; the gateway is the entire boundary.
Crash tolerance carries through the boundary as well: a worker that dies
mid-task lets its take lease lapse, and the task honestly returns to
`submitted` for another worker.

Narrative walk-through: [examples guide, example 07](../examples-guide.html#ex-07).

## What it demonstrates

- **A standards-based interface in front of the fleet.** Any A2A-speaking
  framework, in any language, can discover and drive the fleet without an
  AgentSpaces client library.
- **Card translation.** The gateway translates each AgentSpaces AgentCard in its
  discovery cache into an A2A agent card, and serves a fleet card that lists them
  as skills.
- **Task state derived from the space.** The gateway keeps no task table of its
  own. `A2aTaskBinding` answers `tasks/get` by reading the space, so the state an
  A2A client sees is the state the fleet agrees on.
- **Routing by agent.** Each A2A task entry names its target agent, and the
  worker takes with `Template.of(A2aTaskEntry.class).where("agent", eq(name))`.

## The API in this example

The gateway, bound to the gateway peer's discovery cache and task space:

```java
try (A2aGateway gateway = new A2aGateway("a2a-fleet", "AgentSpaces demo",
        () -> gatewayPeer.discovery().find(AgentCard.class, c -> true))
        .taskBinding(new A2aTaskBinding(gatewayPeer.tasks()))) {
    int port = gateway.start(0);   // 0 picks a free port
```

A worker, which publishes a card that consumes `A2aTaskEntry` and produces
`A2aTaskResult`, then drains its tasks:

```java
var taken = worker.tasks().take(
        Template.of(A2aTaskEntry.class).where("agent", eq(worker.name())),
        Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(2));
taken.ifPresent(t -> worker.tasks().complete(t,
        new A2aTaskResult(t.entry().taskId(), worker.name(), "worked '" + t.entry().text() + "'"),
        Lease.of(Duration.ofHours(1))));
```

The client side is plain JSON-RPC 2.0:

```json
{"jsonrpc":"2.0","id":1,"method":"message/send","params":
 {"message":{"role":"user","parts":[{"kind":"text","text":"reconcile the ledgers"}],"messageId":"m1"}}}

{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"<task id>"}}
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `A2aGateway(name, description, cardSupplier)`, `start(port)` | `agentspaces-a2a` | The JDK `HttpServer` gateway: discovery and JSON-RPC endpoints |
| `A2aTaskBinding(space)` | `agentspaces-a2a` | Maps A2A tasks to space entries and back |
| `A2aMessages.A2aTaskEntry`, `A2aTaskResult` | `agentspaces-a2a` | The entry types a worker takes and completes |
| `/.well-known/agent-card.json`, `/agents/{name}` | HTTP | The fleet card and one JSON-RPC endpoint per agent |

Source: [A2aFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/a2a/A2aFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), peers on ports
7501 and 7502, with the gateway on a free HTTP port:

```
mvn -q -pl examples/example-07-a2a-fleet exec:java
```

[A2aFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/a2a/A2aFleetFlowTest.java)
waits for the analyst's card to reach the gateway over TCP gossip, sends a
message, polls `tasks/get` until the state is `completed`, and asserts that the
artifact carries the analyst's answer:

```
mvn -q -pl examples/example-07-a2a-fleet test
```

## Next steps

- **Stream instead of polling.** The gateway also implements `message/stream`
  and `tasks/resubscribe` over Server-Sent Events, `tasks/cancel` (which
  withdraws the entry), and push notifications to allowlisted webhooks through
  `tasks/pushNotificationConfig/set`. The
  [module README](../../agentspaces-a2a/README.md) lists them.
- **Secure the door.** The gateway shares the console's bearer principal model:
  a token or an identity-provider JWT with the `aspace:a2a:client` scope, where a
  refused token is `401`, a missing scope is `403`, and the audit trail names the
  token's subject.
- **Connect other agent frameworks.** The gateway is the integration point for
  frameworks that already speak A2A, such as Google ADK agents or LangGraph
  services, and for Python shops that would rather not run a native peer. For
  Python or TypeScript code that should join the fleet as a full member, use the
  [Python](../../../agentspaces-python/README.md) or
  [TypeScript](../../../agentspaces-typescript/README.md) client instead.
- **Build real workers.** Replace the echo worker with an `@SpaceTake` method on
  `A2aTaskEntry` that calls a Spring AI `ChatClient` or runs an Embabel agent, and
  return an `A2aTaskResult`; the gateway needs no change.
- **Watch the fleet.** [Example 09](../example-09-fleet-console/README.md) adds a
  served console over the same kind of fleet, with its own HAL+JSON API and
  Server-Sent Events stream.
