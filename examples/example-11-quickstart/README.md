# Example 11: Quickstart

The whole programming model in one file, and the recommended first read if
you want to see the code you will actually write. Two plain classes become a
two-peer fleet: a `Fulfiller` that takes orders and ships them, and an
`Auditor` that reacts to every shipment with a receipt. No space names appear
anywhere, because the group has one space and the binder infers it.
Durations read like Spring properties (`"10m"`, `"300ms"`), and a `@SpaceRef`
field gives the worker a handle for a mid-work progress entry.

Example 01 teaches the coordination model from the verbs up. This example
teaches it from the annotations down, and the two meet in the middle: every
annotation here compiles to the verbs example 01 calls by hand.

Narrative walk-through: [examples guide, quickstart](../examples-guide.html#ex-11), and
the [developer guide's quick start](../developer-guide.html#quick-start).

## What it demonstrates

- **The kill-tolerant worker.** `@SpaceTake` on `ship(Order)` means "take an
  `Order` under a lease, run this method, and complete the take with the returned
  `Shipment`". If the process dies mid-method, the lease lapses and another
  `Fulfiller` gets the order.
- **Service choreography in one returning method.** `@SpaceNotify` on
  `account(Shipment)` reacts to every shipment without consuming it. The binder
  dedupes redeliveries, runs the reaction on its own virtual thread, and writes
  the returned `Receipt`.
- **Injection for mid-method writes.** `@SpaceRef` injects the group's space into
  a field at bind time, so the worker can write a `Progress` marker before it
  returns.
- **Conventions over configuration.** With one space registered, `space` is
  optional on every annotation; lease and timeout strings accept `10m`, `300ms`,
  and ISO-8601 durations alike.

## The API in this example

The entire agent code:

```java
@AgentSpec(name = "fulfiller", description = "Ships orders from the shared space",
        goals = {"fulfill orders"})
public static class Fulfiller {
    @SpaceRef
    private Space space;

    @SpaceTake(lease = "10m", pollTimeout = "300ms")
    public Shipment ship(Order order) {
        space.write(new Progress(order.orderId(), "picking " + order.item()),
                Lease.of(Duration.ofMinutes(5)));
        return new Shipment(order.orderId(), order.item(), "fulfiller");
    }
}

@AgentSpec(name = "auditor", description = "Writes a receipt for every shipment",
        goals = {"account for shipments"})
public static class Auditor {
    @SpaceNotify
    public Receipt account(Shipment shipment) {
        return new Receipt(shipment.orderId(), shipment.item() + " shipped by " + shipment.by());
    }
}
```

Each of the two peers is built the same way as in example 02, plus a binder
over its replica of the `work` space. The demo binds one agent on each peer:

```java
AgentBinder binder = new AgentBinder(identity, runtime.id(), null, InstantSource.system());
binder.space("work", space);

binder.bind(new Fulfiller());   // on the worker peer
binder.bind(new Auditor());     // on the auditor peer
```

| Annotation | Module | Meaning |
| --- | --- | --- |
| `@AgentSpec(name, description, goals)` | `agentspaces-agent` | Names the agent and fills its AgentCard |
| `@SpaceTake(lease, pollTimeout)` | `agentspaces-agent` | Take the parameter type, complete with the return value |
| `@SpaceNotify` | `agentspaces-agent` | React to the parameter type without consuming it; write the return value |
| `@SpaceRef` | `agentspaces-agent` | Inject a `Space` field at bind time |

Source: [Quickstart.java](src/main/java/ai/badmonkey/agentspaces/examples/quickstart/Quickstart.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7591 and 7592:

```
mvn -q -pl examples/example-11-quickstart exec:java
```

The demo prints each progress marker the worker wrote through its `@SpaceRef`,
and each receipt the auditor returned from `@SpaceNotify`.

[QuickstartTest](src/test/java/ai/badmonkey/agentspaces/examples/quickstart/QuickstartTest.java)
binds both agents to one `LocalSpace` and asserts that two orders become two
shipments, two receipts, and two progress markers, and that no orders remain:

```
mvn -q -pl examples/example-11-quickstart test
```

The test shows a useful pattern: annotated agents bind to a `LocalSpace` as
readily as to a `ReplicatedSpace`, so you can unit-test an agent's behavior
with no network at all.

## Next steps

- **Move it into Spring Boot.** Add `agentspaces-spring-boot-starter`, replace
  `@AgentSpec` with the `@SpaceAgent` stereotype (which is also a `@Component`),
  and delete `startPeer`: the starter builds the peer, group, and space from
  `agentspaces.*` properties and enrolls every annotated bean. The
  [starter README](../../agentspaces-spring-boot-starter/README.md) has the
  complete YAML.
- **Add a model with Spring AI.** Inject a `ChatClient.Builder` into the
  `Fulfiller` and let the model do the work inside `ship`. The model call and the
  coordination are orthogonal: the space still hands the method one `Order` at a
  time under a lease, and the returned record still completes the take.
- **Graduate to Embabel.** When one method is not enough, make the worker an
  Embabel `@Agent` with several `@Action`s and a goal. The GOAP planner sequences
  the actions inside each worker, and the space distributes work between workers.
  The [embabel-agentspaces](../../embabel-agentspaces/README.md) extension also
  publishes each `@Agent` as an AgentCard.
- **Learn the rest of the annotations.** `@BidFunction` (example 04), `@Ballot`
  and `@OnDecision` (examples 05 and 08), `@OrderedTake` (example 12), and
  subordinate agent keys (example 13) all follow the same shape: the parameter is
  the cue and the return value is the next entry.
- **Study a complete teaching application.** The
  [PetClinic Fleet](https://github.com/badmonkeyai/agentspaces-example-apps/blob/main/agentspaces-petclinic-fleet/README.md)
  runs a veterinary clinic purely by `@SpaceNotify` choreography over one space,
  and is the gentlest of the example apps.
- **Prefer Clojure?** [agentspaces-clj](https://github.com/badmonkeyai/agentspaces-clj) binds
  the same fleet with maps, keywords, and a `fleet/start` config map that mirrors
  the Spring starter.
