# Example 03: Discovery Cards

Automatic AgentCards act as the routing table. A summarizer and a translator on
different peers publish their cards when they are bound, and a dispatcher peer
asks its advertisement cache which agent produces which entry type, then
simply writes tasks and reads results. No peer is configured with the address,
name, or skills of any other.

This is the first example that uses the annotation programming model. The two
specialists are plain classes with `@AgentSpec` and `@SpaceTake`; the
`AgentBinder` turns each into a leased worker loop and derives its AgentCard
from the method signatures.

Narrative walk-through: [examples guide, example 03](../examples-guide.html#ex-03).

## What it demonstrates

- **Cards derived from types.** A `@SpaceTake` method that takes `SummaryTask`
  and returns `Summary` publishes a card that consumes
  `...CardsFleet$SummaryTask#v1` and produces `...CardsFleet$Summary#v1`. The
  shape of data is the capability description.
- **Discovery through the ad-cache.** `DiscoveryService.find(AgentCard.class, predicate)`
  answers from a local, signature-verified, TTL-evicting cache that gossip keeps
  current.
- **Routing without a dispatcher.** The dispatcher never addresses an agent.
  It writes a task into the shared `work` space, and whichever agent's template
  matches the entry type takes it.

## The API in this example

The two specialists:

```java
@AgentSpec(name = "summarizer", description = "Summarizes documents", goals = {"summarize"})
public static class Summarizer {
    @SpaceTake(space = "work", pollTimeout = "PT0.3S")
    public Summary summarize(SummaryTask task) {
        return new Summary(task.document(), "summary of " + task.document());
    }
}
```

The parameter type is the take template, and the returned `Summary` completes
the take and lands in the space atomically. `Translator` has the same shape
over `TranslateTask` and `Translation`.

Each peer adds a `DiscoveryService` and an `AgentBinder` to example 02's assembly:

```java
DiscoveryService discovery = new DiscoveryService(runtime,
        new AdCache(codec, InstantSource.system()), codec, identity.peerId());
AgentBinder binder = new AgentBinder(identity, runtime.id(), discovery, InstantSource.system());
binder.space("work", work);

binder.bind(new Summarizer());   // starts the worker loop and publishes the card
```

The dispatcher's discovery query:

```java
String summarySchema = Summary.class.getName() + "#v1";
List<AgentCard> summarizers = dispatcher.discovery().find(AgentCard.class,
        card -> card.produces().contains(summarySchema));
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `@AgentSpec(name, description, goals)` | `agentspaces-agent` | Names the agent and fills its card |
| `@SpaceTake(space, pollTimeout)` | `agentspaces-agent` | The kill-tolerant worker: take, run, complete with the return value |
| `AgentBinder`, `bind(Object)` | `agentspaces-agent` | Turns annotated objects into worker loops and published cards |
| `DiscoveryService`, `AdCache` | `agentspaces-discovery` | Publishes, caches, verifies, and finds advertisements |
| `AgentCard.produces()`, `consumes()`, `agent()` | `agentspaces-api` | The card's typed skill description and its `AgentId` |

Source: [CardsFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/cards/CardsFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7461 to 7463:

```
mvn -q -pl examples/example-03-discovery-cards exec:java
```

[CardsFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/cards/CardsFleetFlowTest.java)
waits for the summarizer's card to reach the dispatcher over TCP gossip, asserts
that exactly one agent produces `Summary`, and then asserts that both tasks
come back completed with the right results:

```
mvn -q -pl examples/example-03-discovery-cards test
```

## Next steps

- **Let the planner use the cards.** The
  [Embabel extension](../../embabel-agentspaces/README.md) runs this idea in both
  directions. `EmbabelBinder` publishes each Embabel `@Agent` as an AgentCard,
  and `EmbabelRemoteActions` turns the fleet's foreign cards into typed planner
  actions, so an Embabel GOAP planner in one JVM can plan over agents that run on
  other machines. The [Party Bus](https://github.com/badmonkeyai/agentspaces-partybus)
  planner finds its scouts this way.
- **Discover by meaning.** [Example 10](../example-10-data-fleet/README.md)
  queries cards by natural-language description through the semantic-discovery
  capability; a Spring AI `EmbeddingModel` is a natural fit behind the pluggable
  `Embedder` SPI in place of the shipped hashing embedder.
- **Expose the cards to other frameworks.** [Example 07](../example-07-a2a-fleet/README.md)
  serves these same cards as A2A agent cards over HTTP.
- **Prove who wrote the card.** [Example 13](../example-13-signed-agents/README.md)
  gives each agent a certified key of its own, so its card carries the agent's
  public key and its entries read as `AGENT_ATTESTED`.
- **Spring Boot.** Under the starter, any `@SpaceAgent` or `@AgentSpec` bean
  publishes its card into every joined group automatically, and
  `agentspaces.card-refresh-millis` controls the refresh.
