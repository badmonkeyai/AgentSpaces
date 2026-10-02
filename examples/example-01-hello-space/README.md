# Example 01: Hello Space

One process and one `LocalSpace` show the whole coordination model in
miniature. A coordinator writes leased `ResearchTask` entries, a worker takes
the most urgent one and crashes, the take lease lapses, the task reappears,
and a healthy worker completes it, atomically writing a `Finding` result. A
notification subscription prints every lifecycle event as it happens.

This example has no network, no peers, and no annotations. It isolates the
five verbs of the space API (`write`, `read`, `take`, `complete`, `notify`) and
the lease, so that every later example reads as "the same verbs, over more
machines".

Narrative walk-through: [examples guide, example 01](../examples-guide.html#ex-01).

## What it demonstrates

- **Leased writes.** Every entry carries a lease. An entry whose lease lapses
  leaves the space on its own, so abandoned work never accumulates.
- **Template takes.** `Template.of(ResearchTask.class).where("priority", gte(5))`
  matches by type and field, and `take` claims the match under a lease of its own.
- **Crash recovery as a property of the space.** A worker that takes and never
  completes simply lets its take lease lapse; the space emits `REAPPEARED` and the
  task is available again. No heartbeat, reaper, or retry queue exists.
- **Atomic complete-with-result.** `complete(taken, result, lease)` removes the
  task and writes the `Finding` in one step, so a reader never sees a completed
  task without its result or a result without its completed task.
- **Notifications.** `notify` streams `WRITTEN`, `TAKEN`, `EXPIRED`,
  `REAPPEARED`, and `COMPLETED` events for a template.

## The API in this example

The entry types are plain Java records. The space needs no base class, schema
file, or registration:

```java
public record ResearchTask(String topic, int priority) { }
public record Finding(String topic, String summary) { }
```

A stable peer identity comes from a keystore, and the space is built for one
agent of that peer:

```java
PeerIdentity identity = FileKeystore.loadOrCreate(Path.of("peer.keys"));
try (LocalSpace space = LocalSpace.builder("tasks", identity.agent("coordinator"))
        .sweepEvery(Duration.ofMillis(100))
        .build()) {
```

The write, take, crash, and recover sequence:

```java
space.write(new ResearchTask("tuple spaces", 7), Lease.of(Duration.ofMinutes(30)));

Template<ResearchTask> urgent = Template.of(ResearchTask.class).where("priority", gte(5));
TakenEntry<ResearchTask> doomed = space.take(urgent,
        Lease.of(Duration.ofMillis(300)), Duration.ofSeconds(1)).orElseThrow();
// ...the worker never calls complete; 300 ms later the task reappears

TakenEntry<ResearchTask> retry = space.take(urgent,
        Lease.of(Duration.ofMinutes(10)), Duration.ofSeconds(5)).orElseThrow();
space.complete(retry, new Finding(retry.entry().topic(), "..."), Lease.of(Duration.ofHours(1)));
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `LocalSpace.builder(name, agentId)` | `agentspaces-space` | A single-JVM space with the same API as the replicated one |
| `Lease.of(Duration)` | `agentspaces-api` | The lifetime of every entry, take, and subscription |
| `Template.of(Class).where(field, matcher)` | `agentspaces-api` | Typed matching; `Matchers.gte`, `eq`, and friends |
| `TakenEntry<T>` | `agentspaces-api` | A claim on an entry, passed back to `complete` |
| `SpaceEvent<T>` | `agentspaces-api` | One lifecycle event delivered to a `notify` listener |
| `FileKeystore`, `PeerIdentity` | `agentspaces-identity` | The Ed25519 peer key and the `AgentId`s derived from it |

Source: [HelloSpace.java](src/main/java/ai/badmonkey/agentspaces/examples/hello/HelloSpace.java).

## Running it

From the `agentspaces/` repository root, after one
`mvn install -DskipTests` so the module
resolves its sibling artifacts:

```
mvn -q -pl examples/example-01-hello-space exec:java
```

The demo creates `peer.keys` in the working directory on first run and reuses
it after that, so the printed PeerID stays stable between runs.

The same flow runs as a deterministic JUnit test,
[HelloSpaceFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/hello/HelloSpaceFlowTest.java).
It builds the space with a `TestClock` and calls `clock.advance(...)` to lapse
the lease, so the test contains no sleeps and never flakes on a slow machine:

```
mvn -q -pl examples/example-01-hello-space test
```

The `TestClock` pattern is worth copying into your own tests. Any space,
capability, or binder that accepts an `InstantSource` can be driven the same way.

## Next steps

- **Distribute it.** [Example 02](../example-02-research-fleet/README.md) runs
  this exact flow across three peers over TCP, with `ReplicatedSpace` in place of
  `LocalSpace` and no other change to the verbs.
- **Stop writing the loop.** [Example 11](../example-11-quickstart/README.md)
  replaces the hand-written take and complete with one `@SpaceTake` method on a
  plain class, and the binder owns the loop, the lease, and the retry.
- **Put a model behind the worker.** The worker's body is where the model call
  goes. With Spring AI, inject a `ChatClient` and return
  `chat.prompt().user(task.topic()).call().entity(Finding.class)`; the space only
  sees the `Finding` record. With Embabel, the same method becomes an `@Action`
  on an `@Agent`, and the planner runs inside the worker while the space
  distributes work between workers.
- **Learn the full API.** The [developer guide](../developer-guide.html#space-api)
  covers every verb, template matcher, lease rule, and builder option.
