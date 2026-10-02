# agentspaces-agent

The framework-neutral programming model: annotations on plain objects, the binder that turns
them into workers, choreography, voters, and exactly-once clerks, and the `AgentSpaces` facade
that routes beans into groups and resolves typed capability clients. The Spring and Embabel
adapters delegate here.

**Architecture position.** Programming model and hosting. SPEC §10.3, §10.5, §10.6; TECH-SPEC §9.

## Key files

| Package / file | What it is |
| --- | --- |
| `annotation.*` | One shape throughout — the method's parameter is the cue, its return value is the next entry: `@SpaceTake` (leased take, complete-or-lapse), `@SpaceNotify` (dedup, virtual thread, renewal, a returned entry written back), `@BidFunction` (one per space per peer), `@SpaceRef`; for Layer 4, `@Ballot` (the cue is the vote's `Proposal`; the return is the option, cast once as the bound agent), `@OnDecision` (once per closed vote), `@OrderedTake` (`@SpaceTake` through the space's ordered-log coordinator), `@CapabilityRef` (a typed client by field), `@ProvidesCapability`; `@AgentSpec` names the agent; `Durations` parses `"10m"` and ISO-8601. |
| `AgentBinder` | `bind(agent[, name])`: reflects the annotations, injects `@SpaceRef`/`@CapabilityRef`, starts the loops and subscriptions, builds and publishes the `AgentCard`, and fails fast at bind time when a space, vote, coordinator, or client it needs is not registered (`space`, `vote`, `ordered`, `aggregate`, `client`, `clients`). Takes an identity factory (`identity::agentIdentity` default, `identity::subordinate` to give each agent a certified key and per-agent space views). A `Contribution` return starts an aggregate epoch. |
| `AgentSpaces` | The facade: `register(group)`, `GroupContext.space/vote/ordered/provide/capability/bind`, routing a bean into every group whose spaces satisfy its bindings; `capability(Class)` resolves typed clients found through `ServiceLoader`. |
| `capability.VoteClient`, `AggregateClient`, `SemanticClient`, `CapabilityClientFactory`, `Contribution` | The typed clients (with `awaitEstimate` replacing any manual tick), their factory SPI, and the push-sum contribution return type. |
| `remote.RemoteActions`, `RemoteAction`, `Correlation` | Foreign AgentCards as invokable actions: write the input entry into the card's bound space, await the correlated result. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`, `agentspaces-discovery`,
`agentspaces-space`, `agentspaces-peering`, `agentspaces-capabilities`. No Spring.

## Notes

- `LAYER4-ANNOTATIONS.md` (workspace root) is the review that produced the Layer 4 annotations
  and the bar a new one must clear.
- Under a subordinate identity factory the binder hands an agent views of its spaces, so its
  records, ballots, and claims are `AGENT_ATTESTED`; peer-signed agents keep the registered
  handle and byte-identical cards (`AgentBinderTest` pins both).
- `AgentBinderLayer4ClusterTest` runs `@OrderedTake` over a three-member log and a
  `Contribution` converging with a second participant on `SimNetwork`.
