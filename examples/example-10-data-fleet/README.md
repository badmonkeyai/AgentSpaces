# Example 10: Data Fleet

The enterprise economics of a data fleet in one demo. A connector peer holds
an orders table and advertises it as a leased AssetCard. Analyst agents
discover it by meaning through semantic discovery, then query it through the
space. The first identical query per freshness window executes against the
source; every later one reads the leased result at replica speed. The demo's
counters show the difference: executions against the source versus answers
the space already held.

This example introduces the connector SDK (`agentspaces-connect-core`) and the
`semantic-discovery` capability. Example 03 found agents by the entry types
they produce. Here the analyst asks a question in natural language and gets
back a data asset.

Narrative walk-through: [examples guide, example 10](../examples-guide.html#ex-10).

## What it demonstrates

- **Assets as advertisements.** An `AssetProvider` describes a data source
  (name, URI, description, row schema, freshness), and the `ConnectorRuntime`
  publishes it as a signed, leased AssetCard alongside the fleet's AgentCards.
- **Discovery by meaning.** `SemanticDiscovery.remoteQuery("who holds customer order history?", ...)`
  ranks advertisements by the similarity of their descriptions to the question,
  through the pluggable `Embedder` SPI.
- **Pull once, share under policy.** `DataQueryClient.fetch` writes a query into
  the space under the `data-query` protocol. The connector answers it once and
  writes a leased result; any analyst asking the same question within the
  freshness window reads that result (`fromCache=true`) and never touches the
  source.

## The API in this example

The source, as a table provider (a stand-in for a real Postgres table):

```java
TableAssetProvider provider = new TableAssetProvider("orders", "postgres://ops/public.orders",
        "Customer order history with line items and settlement status",
        "com.example.OrderRow#v1", Duration.ofMinutes(5), rows);
```

The connector peer serves it:

```java
try (ConnectorRuntime connector = new ConnectorRuntime(data, discovery, identity,
        "pg-connector", provider, runtime.id(), InstantSource.system(), Duration.ofMinutes(1))) {
    connector.start();
```

An analyst discovers it by meaning, then queries it:

```java
SemanticDiscovery semantic = new SemanticDiscovery(new CapabilityPipes(runtime, codec),
        runtime, discovery, peerId, codec, InstantSource.system(), new HashingEmbedder());

var matches = semantic.remoteQuery("who holds customer order history?", 3, Duration.ofSeconds(2));
AssetCard card = (AssetCard) matches.get(0).advertisement();

DataQueryClient client = new DataQueryClient(data, "analyst-a");
var fetched = client.fetch(card.asset(), Map.of("region", "eu"), Duration.ofSeconds(10)).orElseThrow();
fetched.fromCache();        // false the first time, true for the next analyst
fetched.result().rows();
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `TableAssetProvider`, `AssetProvider` | `agentspaces-connect-core` | Describes an asset and answers queries against it |
| `ConnectorRuntime` | `agentspaces-connect-core` | Publishes the AssetCard and serves `data-query` requests from the space |
| `DataQueryClient.fetch(asset, params, timeout)` | `agentspaces-connect-core` | Query through the space, with pull-once caching |
| `SemanticDiscovery`, `HashingEmbedder` | `agentspaces-capabilities` | Discovery by meaning over the fleet's advertisements |
| `AssetCard` | `agentspaces-api` | The leased advertisement for a data asset |

Source: [DataFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/data/DataFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7531 to 7533:

```
mvn -q -pl examples/example-10-data-fleet exec:java
```

The demo makes three fetches (EU orders twice, from two analysts, and US
orders once) and prints two source executions.

[DataFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/data/DataFleetFlowTest.java)
asserts that the semantic query finds the `orders` AssetCard, that the first
fetch executes against the source, and that the second analyst's identical
fetch comes from the space, with `provider.executions()` equal to one:

```
mvn -q -pl examples/example-10-data-fleet test
```

## Next steps

- **Swap in a real embedder.** `HashingEmbedder` keeps the demo dependency-free.
  In a Spring AI application, implement the two-method `Embedder` SPI
  (`embed(String)` and `dimensions()`) over an `EmbeddingModel` (OpenAI,
  Ollama, Bedrock, and the rest) so that discovery
  ranks by real semantic similarity. Every peer that answers queries should use
  the same embedder.
- **Connect a real source.** Implement `AssetProvider` over JDBC, a warehouse, or
  a REST/JSON API, and set the freshness window to the staleness the business can
  accept. `MaterializingAssetProvider` pushes source changes into a space as
  leased entries, for sources that should stream rather than answer queries.
- **Move bulk results efficiently.** `DataSpaces` wires the block exchange so
  results larger than 64 KiB travel content-addressed.
- **Control who serves and who reads.** `agentspaces.security.grants.connector-serve`
  limits which peers may advertise assets and whose results the fleet trusts, and
  the space's admission mode (`group`, `allowlist`, `credential`, or
  `authorizer`) limits who can read the results.
- **Give the planner a data source.** Wrap `DataQueryClient.fetch` in an Embabel
  action or a Spring AI tool method, and an agent can decide for itself when to
  ask the fleet for data, and benefit from every answer another agent already
  paid for.
