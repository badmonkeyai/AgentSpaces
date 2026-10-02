# Example 14: Gossip Learning

Decentralized model averaging. Three peers each hold a local model (a weight
vector fitted to their own data) and average it with random partners over the
fabric, round after round, until every peer holds the fleet's mean. No
parameter server, coordinator, or participant ever sees all the data
(`aspace:cap/gossip-learn`, SPEC §8).

The learner is registered on the peer's `CapabilityRuntime`, so the peer tick
drives the exchange and advertises the capability; nothing in the example
ticks anything by hand. This is the first consumer of the learning capability
outside the starter, and it keeps the shape of a learning agent visible (start
a model, read the merged model back as rounds complete) before an annotation
for it exists (`LAYER4-ANNOTATIONS.md` §2.6).

## What it demonstrates

- **Peer-to-peer averaging.** Each round, a peer offers its model to a partner
  from the group's peer sampler, and the offer/accept exchange conserves mass, so
  the fleet converges on the true element-wise mean: here `[2.0, 3.0, 2.0]` from
  `[1, 2, 3]`, `[3, 2, 1]`, and `[2, 5, 2]`.
- **Data stays local.** Only model weights cross the network. Each peer's
  training data never leaves it.
- **A capability like any other.** `capabilities.register(learning)` puts the
  learner on the peer tick and advertises it, so any peer can find the fleet's
  learners with `providersOf(GossipLearner.TYPE)`.

## The API in this example

Assembling a learner:

```java
CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
GossipLearning learning = new GossipLearning(new CapabilityPipes(runtime, codec),
        runtime.sampler(), identity.peerId(), codec, InstantSource.system());
capabilities.register(learning);   // the peer tick now drives the exchange
node.startTicking(Duration.ofMillis(250));
```

Training and reading back:

```java
learning.start("demand-forecast", new double[] {1.0, 2.0, 3.0});   // this peer's local weights

Optional<double[]> merged = learning.model("demand-forecast");   // after at least one round
long rounds = learning.round("demand-forecast");
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `GossipLearning`, `start`, `model`, `round` | `agentspaces-capabilities` | The weight-averaging learner over `double[]` models |
| `GossipLearner`, `MergeableModel`, `WeightAveraging` | `agentspaces-capabilities` | The general learner and its model SPI; `WeightAveraging` is the default |
| `GossipLearner.TYPE` | `agentspaces-capabilities` | The capability type URI, for discovery |
| `CapabilityRuntime.register`, `providersOf` | `agentspaces-capabilities` | Drives and advertises capabilities; finds their providers |
| `CapabilityPipes` | `agentspaces-capabilities` | The direct pipe binding a continuous capability exchanges over |
| `GroupRuntime.sampler()` | `agentspaces-peering` | Random partner selection from the live membership |

Source: [GossipLearningFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/learning/GossipLearningFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7801 to 7803:

```
mvn -q -pl examples/example-14-gossip-learning exec:java
```

The demo prints the expected fleet mean, then each peer's merged model and
round count once every peer has run at least eight rounds.

[GossipLearningFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/learning/GossipLearningFleetFlowTest.java)
asserts over real TCP that every peer converges within 0.05 of the fleet mean,
that every peer completed rounds, and that the learner is discoverable as a
`GossipLearner.TYPE` provider:

```
mvn -q -pl examples/example-14-gossip-learning test
```

## Next steps

- **Learn something real.** `GossipLearning` wraps the default
  `WeightAveraging` model, which suits any parameter vector: linear and logistic
  regression weights, small neural network parameters, or embedding centroids.
  For anything else (bandit statistics, sketches, bid policies), implement the
  `MergeableModel` SPI, whose `merge` must be commutative so both sides of an
  exchange agree, and run it through a `GossipLearner`. Larger models travel
  content-addressed, and `endEpoch(epoch, evaluate)` produces per-epoch
  `Evaluation`s so the fleet can see whether the merged model improves.
- **Federate across sites.** Combine this example with
  [example 06](../example-06-wan-rendezvous/README.md): sites that cannot share
  raw data (hospitals, branches, devices at the edge) can still converge on a
  shared model, with only weights crossing the WAN.
- **Close the loop with the other capabilities.** Use `aggregate`
  ([example 05](../example-05-quorum/README.md)) to sense the fleet's average
  validation loss, and a `vote` to decide when a merged model is good enough to
  promote.
- **Where LLMs fit.** Gossip learning suits small, mergeable models that sit
  beside an LLM agent: a router that learns which model tier answers a task type
  best, a cost or latency predictor behind an auction `@BidFunction`
  ([example 04](../example-04-auction/README.md)), or a relevance model over
  Spring AI embeddings. Each agent improves from the whole fleet's experience
  without a central training service.
- **Spring Boot.** `agentspaces.capabilities.gossip-learn=true` wires the default
  weight-averaging learner into every group; it defaults off because a learner
  needs a model.
- **Read the design.** SPEC §8 in [agentspaces-spec](../../../agentspaces-spec/README.md)
  specifies the offer/accept exchange, content addressing, and epoch
  evaluations.
