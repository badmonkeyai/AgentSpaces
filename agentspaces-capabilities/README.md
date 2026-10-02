# agentspaces-capabilities

Layer 4: the capability runtime and the six shipped capabilities. Each is a reference
implementation of the `CapabilityProvider` pattern; register one on a peer's
`CapabilityRuntime` and the peer's clock drives it and the fleet can find it.

**Architecture position.** Layer 4, Capability services. SPEC §8; TECH-SPEC §8.

## Key files

| Package / file | What it is |
| --- | --- |
| `runtime.CapabilityRuntime` | Registers itself on `GroupRuntime.onTick`; `register(provider)` starts, advertises, refreshes, and ticks a provider at the peer's cadence (`driverCadence`), republishing an advertisement whose protocol state changed (QA4 A4-10); `providersOf(type)` finds providers through the ad-cache. |
| `runtime.CapabilityPipes`, `PipeChannel` | Direct `PIPE_DATA` frames multiplexed by capability type, for high-rate protocols. |
| `aggregate.PushSumAggregate` | `aspace:cap/aggregate`: push-sum over `Share | Extremum | Histogram | Roster` frames; sum/avg/count/min/max/quantile over values or a template's entries; `estimate` is empty until the first exchange (an undriven estimator never passes its seed off as a fleet answer). |
| `vote.VoteCapability` | `aspace:cap/vote`: proposals and ballots as signed entries; the tally counts one voter per peer or per attested agent at the authorizer's granularity (QA4 A4-7); `onDecision(...)` fires once per closed vote; `as(view, voter)` votes as another agent of the peer. MAJORITY_GOSSIP over push-sum. |
| `orderedlog.RaftLog`, `OrderedTakes`, `RaftMessages` | `aspace:cap/ordered-log`: a Raft member over pipes with the leader lease as an advertisement, and the take coordinator behind ORDERED — arbitration by epoch alone, committed claims installed with the holder's own attestation. `OrderedTakes.over(...)` builds and wires both. |
| `learn.GossipLearner`, `GossipLearning`, `MergeableModel`, `WeightAveraging` | `aspace:cap/gossip-learn`: the mergeable-model SPI, content-addressed models, the mass-conserving offer/accept exchange, epoch evaluations. |
| `semantic.SemanticDiscovery`, `HashingEmbedder` | `aspace:cap/semantic-discovery`: rank cached advertisements by meaning, locally or fleet-wide; the embedder is a pluggable seam. |
| `keywrap.GroupKeyDistributor` | `aspace:cap/key-wrap`: sealed per-member group-key distribution under a membership, authorizer, or predicate policy. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`, `agentspaces-peering`,
`agentspaces-discovery`, `agentspaces-space`.

## Notes

- ORDERED is a coordinator, not a space strategy: build the space with the default strategy and
  take through `OrderedTakes` (or `@OrderedTake` in `agentspaces-agent`).
- The generator for the golden vectors (`../tools/golden/GoldenVectors.java`) runs on this
  module's classpath because it pins the aggregate, learner, and vote wire shapes too.
- `CapabilityClockTest` is the liveness suite: every capability advertised is also driven.
