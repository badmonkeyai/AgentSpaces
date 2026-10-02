# Example 05: Quorum

Capability choreography, where the fleet senses a condition and then decides
what to do about it. Push-sum aggregation computes the fleet's average backlog
with no collector; the supervisor opens a scale-up proposal when the average
crosses a threshold; every member casts a signed ballot into the vote space;
and the quorum decision closes identically on every replica. Any member can
recompute the tally from the space itself, which makes the vote an auditable
record of the fleet's decision.

This is the first example that uses the Layer 4 capabilities (`aggregate` and
`vote`) and the Layer 4 annotations (`@Ballot` and `@OnDecision`). Its second
act seats a council of three agents on one peer and shows how the authorizer
decides who counts, and at what granularity.

Narrative walk-through: [examples guide, example 05](../examples-guide.html#ex-05).

## What it demonstrates

- **Aggregation without a collector.** Each peer starts a push-sum epoch with its
  local backlog (4, 30, 41, 31); gossip exchanges converge every peer on the
  mean, 26.5. No peer ever sees all four values.
- **Signed quorum votes.** A proposal names its options and quorum. Each
  ballot is a signed entry, and every replica computes the same winner and tally
  from the same ballots.
- **Voting as annotations.** A `@Ballot` method receives each matching proposal
  and returns its choice. An `@OnDecision` method receives the closed decision
  once and returns an entry for the record. Nobody polls, waits for a proposal
  to replicate, or keeps a set of proposals already voted on.
- **One member, one vote.** The vote counts one voter per peer by default. When
  the `Authorizer` grants `VOTE` to specific `AgentId`s, it speaks at AGENT
  granularity: each granted agent with its own certified key counts once, and an
  ungranted agent on the same peer is refused before it writes. A per-peer
  counter elsewhere in the fleet still sees the whole council as one member.
- **Capabilities on the peer tick.** `group.provide(aggregate)` and
  `group.provide(vote)` register, advertise, and drive both capabilities from the
  peer's own tick. The example calls no `tick()` and runs no scheduler.

## The API in this example

The members and the supervisor, as annotated agents:

```java
@AgentSpec(name = "member", description = "Votes on scaling by its own backlog", goals = {"vote"})
public static final class Member {
    @Ballot(space = "votes", prefix = "scale", lease = "10m")
    public String vote(VoteCapability.Proposal proposal) {
        return backlog > BACKLOG_THRESHOLD / 2 ? "approve" : "reject";
    }
}

@AgentSpec(name = "supervisor", description = "Records scaling decisions", goals = {"record"})
public static final class Supervisor {
    @OnDecision(space = "votes", prefix = "scale", resultLease = "10m")
    public ScalingDecision record(VoteCapability.Decision decision) {
        return new ScalingDecision(decision.proposalId(), decision.winner(),
                decision.tally().toString(), decision.granularity().name());
    }
}
```

Registering the capabilities through the `AgentSpaces` facade:

```java
AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
AgentSpaces.GroupContext group = spaces.register("quorum", runtime.id(), runtime, discovery)
        .capabilities(new CapabilityRuntime(runtime, discovery, identity));
group.space("votes", votes);
group.provide(aggregate);   // PushSumAggregate
group.provide(vote);        // VoteCapability; also backs @Ballot/@OnDecision on "votes"
```

Sensing and proposing with the typed clients:

```java
peer.aggregate().start("backlog", localBacklog);
OptionalDouble mean = supervisor.aggregate().estimate("backlog");

supervisor.vote().propose("scale-up-1", "Add two workers?",
        List.of("approve", "reject"), 4, Lease.of(Duration.ofMinutes(10)));
Optional<VoteCapability.Decision> decision = supervisor.vote().decision("scale-up-1");
```

The council in act two (`seatCouncil`, `bindCouncil`) gives each seat a
subordinate identity and a per-agent view of the one replica, with an
authorizer that grants `VOTE` to two of the three:

```java
AgentIdentity seat = host.node().identity().subordinate("finance");
Space votes = host.votes().as(seat);
Authorizer authorizer = new MembershipAuthorizer(membership, peerId,
        Map.of(Operation.VOTE, Set.of()), Map.of(Operation.VOTE, grantedAgentIds));
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `PushSumAggregate`, `start`, `estimate` | `agentspaces-capabilities` | Gossip aggregation (sum, avg, count, min, max, quantile) |
| `VoteCapability`, `propose`, `castBallot`, `tally`, `decision` | `agentspaces-capabilities` | MAJORITY_GOSSIP and QUORUM votes over a space |
| `@Ballot(space, prefix, lease)` | `agentspaces-agent` | One signed ballot per matching proposal |
| `@OnDecision(space, prefix, resultLease)` | `agentspaces-agent` | React once when a vote closes |
| `AgentSpaces`, `GroupContext.provide`, `bind` | `agentspaces-agent` | The facade: spaces, capabilities, and bindings per group |
| `MembershipAuthorizer`, `Authorizer.Granularity` | `agentspaces-api`, `agentspaces-peering` | Who may vote, counted per PEER or per AGENT |
| `PeerIdentity.subordinate(name)`, `Space.as(identity)` | `agentspaces-identity`, `agentspaces-space` | A certified agent key and that agent's view of a space |

Source: [QuorumFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/quorum/QuorumFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7481 to 7484:

```
mvn -q -pl examples/example-05-quorum exec:java
```

The demo prints the sensed average, each ballot, the decision with its tally
and granularity, and then the council's per-agent tally beside a worker's
per-peer view of the same ballots.

[QuorumFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/quorum/QuorumFleetFlowTest.java)
has four tests over real TCP:

- `fleetSensesBacklogAndDecidesByQuorum`: the average converges on 26.5 and the
  vote closes 3 to 1 for `approve` on every replica.
- `annotatedMembersVoteAndTheSupervisorRecordsTheDecision`: the same flow through
  `@Ballot` and `@OnDecision`, with the decision recorded as a `ScalingDecision`.
- `aGrantedCouncilOnOnePeerCountsPerAgentWhileTheFleetCountsPerPeer`: two granted
  seats close a quorum of two at AGENT granularity, and the intern is refused.
- `annotatedCouncilSeatsOnOnePeerCountPerGrantedAgent`: the council as
  annotated `@Ballot` seats.

```
mvn -q -pl examples/example-05-quorum test
```

## Next steps

- **Put judgment behind the ballot.** A `@Ballot` method is the natural place
  for a model call. With Spring AI, prompt a `ChatClient` with the proposal's
  text and the agent's local context, and return the chosen option with
  `.entity(...)`. With Embabel, the ballot can call an agent that gathers evidence
  first. Either way, the space records one signed ballot per voter, which gives
  a panel of LLM agents an auditable decision process.
- **See panels at scale.** The [release-audit flagship](../../../flagships/agentspaces-audit-fleet/README.md)
  adjudicates every finding by a QUORUM vote of discipline specialists, and the
  [Party Bus](../../../agentspaces-partybus/README.md) runs a safety panel and a
  travelers' vote with `@Ballot` and `@OnDecision`.
- **Apply it to a pipeline.** [Example 08](../example-08-intake-fleet/README.md)
  uses the same annotations to choose between two extraction candidates and file
  the winner with provenance.
- **Spring Boot.** The `aggregate` and `vote` capabilities default on
  (`agentspaces.capabilities.aggregate`, `agentspaces.capabilities.vote`, over the
  space named by `agentspaces.capabilities.votes-space`, default `votes`). Set
  `agentspaces.identity.agent-keys=subordinate` and list agents under
  `agentspaces.security.grants.vote` to get act two's per-agent counting.
- **Read the design.** `LAYER4-ANNOTATIONS.md` at the workspace root is the
  review that produced `@Ballot`, `@OnDecision`, and the other Layer 4 annotations.
