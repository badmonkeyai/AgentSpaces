# Example 04: Auction

The AUCTION conflict strategy end to end. Two model workers price the same
tasks differently through `@BidFunction` cost models: the mini model is cheap
on routine work, and the premium model's flat rate pays off on hard work. The
space allocates each task to the lowest bidder during a settle window.
Cost-aware routing is a property of the coordination layer, and no dispatcher
process exists.

Examples 02 and 03 used LEASE_RACE, where the first claim wins. This example
changes one builder call, `.strategy(ConflictStrategyType.AUCTION)`, and adds
one annotated method per worker.

Narrative walk-through: [examples guide, example 04](../examples-guide.html#ex-04).

## What it demonstrates

- **Bids in the claim lattice.** Each worker's bid is a signed claim that
  replicates like any other entry, and every replica computes the same winner
  once the settle window closes.
- **Pricing as code.** A `@BidFunction` is an ordinary method over the task, so a
  cost model can use difficulty, current load, token price, or anything else the
  agent knows.
- **One bidder per space per peer.** A space consults one cost function, so
  binding a second `@BidFunction` for the same space on the same peer fails at
  bind time and names both agents, the space, and the peer. The fleet seats each
  bidder on its own peer.

## The API in this example

```java
@AgentSpec(name = "mini-model", description = "Cheap small-model worker")
public static class MiniModelWorker {
    @BidFunction(space = "model-tasks")
    public double bid(ModelTask task) {
        return 1.0 + task.difficulty() * 2.0;   // 3 .. 21
    }

    @SpaceTake(space = "model-tasks", pollTimeout = "PT0.3S")
    public ModelResult run(ModelTask task) {
        return new ModelResult(task.prompt(), task.difficulty(), "mini-model", bid(task));
    }
}
```

`PremiumModelWorker` bids a flat `12.0`. With difficulties 1 to 8, the mini
model wins difficulties 1 to 5 (bids 3 to 11) and the premium model wins 6 to 8.

The space opts into the strategy in its builder:

```java
ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "model-tasks", identity, agentName)
        .settleWindow(Duration.ofMillis(200))
        .strategy(ConflictStrategyType.AUCTION)
        .build();
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `@BidFunction(space)` | `agentspaces-agent` | The agent's cost model for one space; lowest bid wins |
| `ConflictStrategyType.AUCTION` | `agentspaces-api` | Allocation by bid within the settle window |
| `settleWindow(Duration)` | `agentspaces-space` | How long bids collect before the space allocates |
| `@SpaceTake` | `agentspaces-agent` | Runs only the tasks this agent won |

Source: [AuctionFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/auction/AuctionFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7471 to 7473:

```
mvn -q -pl examples/example-04-auction exec:java
```

The demo prints each task's difficulty, the winning worker, and its bid.

[AuctionFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/auction/AuctionFleetFlowTest.java)
runs over real TCP:

- `auctionFleetCompletesEveryTaskExactlyOnce` asserts that six tasks produce
  six results under concurrent bidding.
- `twoBiddersOnOnePeerAreRefusedByName` pins the one-cost-function rule.

Deterministic per-task allocation is pinned separately by `AuctionClusterTest`
in `agentspaces-space`, on the in-JVM `SimNetwork`.

```
mvn -q -pl examples/example-04-auction test
```

## Next steps

- **Price real models.** Put a Spring AI `ChatClient` behind each worker, one
  per provider or model tier, and compute the bid from the provider's token
  price and an estimate of the prompt's size. The space then routes each task to
  the cheapest model that is willing to take it, and adding a provider means
  starting one more peer.
- **See it at scale.** The [code-migration example app](https://github.com/badmonkeyai/agentspaces-example-apps/blob/main/agentspaces-code-fleet/README.md)
  allocates repository issues by AUCTION across specialist and generalist coding
  agents, then reviews the changes under LEASE_RACE in a second space. The
  [Party Bus](https://github.com/badmonkeyai/agentspaces-partybus) auctions each day of a trip
  among activity planners.
- **Spring Boot.** Set `strategy: AUCTION` on the space under
  `agentspaces.groups[].spaces[]`; any bean with a `@BidFunction` method enrolls
  automatically.
- **Decide together instead of bidding.** [Example 05](../example-05-quorum/README.md)
  moves from allocation to collective decisions with the aggregate and vote
  capabilities.
