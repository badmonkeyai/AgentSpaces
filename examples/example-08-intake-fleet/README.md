# Example 08: Intake Fleet

An applied pipeline built from the pieces of examples 03 and 05. A
hard-to-read scan enters the intake space; two extraction agents each write a
candidate reading with a confidence score; the fleet closes a signed quorum
vote on which reading to trust; and an auditor files the winning candidate as
a `FilingEntry`. The scan, both candidates, every ballot, and the filing are
signed entries, so a compliance reviewer can replay the whole provenance
chain from the space.

The pipeline is service choreography from end to end. Each stage reacts to
the entries the previous stage produced, and no orchestrator process exists.
Every agent is an annotated plain object bound through the `AgentSpaces`
facade, and the `Voter` and `Auditor` agents hold a `@SpaceRef` so they can
read the candidates mid-method.

Narrative walk-through: [examples guide, example 08](../examples-guide.html#ex-08).

## What it demonstrates

- **Choreography with `@SpaceNotify`.** An extractor reacts to every `ScanEntry`
  without taking it, so both extractors read the same scan in parallel. Each one
  returns an `ExtractionCandidate`, which the binder writes back into the space.
- **A quality vote.** The auditor proposes "which reading of scan-001 do we
  trust?" with the candidates' extractors as options. Each `@Ballot` voter backs
  the candidate with the highest confidence.
- **Filing on decision.** The auditor's `@OnDecision` method receives the closed
  vote once, takes the consumed candidates off the space, and returns the
  `FilingEntry`, stamped with the quorum and tally.
- **Proposal-driven ballots.** Because a `@Ballot` fires on the proposal itself,
  a ballot never arrives before its proposal. The voter only waits for the
  candidates the proposal names to replicate locally.

## The API in this example

The three stages:

```java
@AgentSpec(name = "extractor", description = "Reads a scan", goals = {"extract"})
public static final class Extractor {
    @SpaceNotify(space = "intake", lease = "1h", resultLease = "10m")
    public ExtractionCandidate extract(ScanEntry scan) { ... }
}

@AgentSpec(name = "voter", description = "Backs the most confident reading", goals = {"vote"})
public static final class Voter {
    @SpaceRef("intake") Space intake;

    @Ballot(space = "intake", prefix = "scan-", lease = "10m")
    public String back(VoteCapability.Proposal proposal) {
        // read the candidates for this scan; return the most confident extractor, or null to abstain
    }
}

@AgentSpec(name = "auditor", description = "Files the reading the quorum trusts", goals = {"file"})
public static final class Auditor {
    @SpaceRef("intake") Space intake;

    @OnDecision(space = "intake", prefix = "scan-", resultLease = "1d")
    public FilingEntry file(VoteCapability.Decision decision) { ... }
}
```

The facade wiring, with the vote registered on the same space the agents use:

```java
AgentSpaces.GroupContext group = new AgentSpaces(identity, InstantSource.system())
        .register("intake", runtime.id(), runtime, null);
group.space("intake", intake);
group.vote("intake", new VoteCapability(intake, identity.agent(name), identity.peerId(),
        InstantSource.system()));
group.bind(new Extractor(name, 0.95));
group.bind(new Voter());
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `@SpaceNotify(space, lease, resultLease)` | `agentspaces-agent` | React to an entry without consuming it; the return value is the next entry |
| `@Ballot`, `@OnDecision` | `agentspaces-agent` | The vote and the reaction to its result |
| `@SpaceRef(name)` | `agentspaces-agent` | A `Space` field injected at bind time, for reads and writes inside a method |
| `GroupContext.vote(space, capability)` | `agentspaces-agent` | Names the vote that backs `@Ballot` and `@OnDecision` on a space |
| `Space.readAll(template, max)` | `agentspaces-api` | Reads every candidate for a scan, matched with `where("scanId", eq(id))` |

Source: [IntakeFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/intake/IntakeFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), on ports 7511 to 7513:

```
mvn -q -pl examples/example-08-intake-fleet exec:java
```

The careful extractor (skill 0.95) reads `TIN-88-1234567 / 12,400.00`, and the
fast extractor (skill 0.60) misreads it as `TIN-88-1284567 / 12,4O0.OO`. The
three voters back the more confident reading, and the auditor files it.

[IntakeFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/intake/IntakeFleetFlowTest.java)
asserts that the filing comes from the careful extractor, carries the
3-vote quorum in its provenance, replicates to the other peers, and leaves no
candidates behind:

```
mvn -q -pl examples/example-08-intake-fleet test
```

## Next steps

- **Use real extractors.** Each extractor is a natural home for a different
  model: a Spring AI `ChatClient` over a vision-capable model with
  `.entity(ExtractionCandidate.class)` for structured output, a traditional OCR
  engine, and a small local model, all racing on the same scan. The vote then
  becomes an ensemble decision, and a disagreement can route to a human.
- **Carry the real image.** Entries larger than 64 KiB travel content-addressed
  (by CID) over the block exchange, and the space replicates only the record. The
  [document intake example app](https://github.com/badmonkeyai/agentspaces-example-apps/blob/main/agentspaces-intake-fleet/README.md)
  builds this pipeline at full size, with page images by CID, per-field
  adjudication, a human review queue, and evidence counted per authenticated member.
- **Make the chain provable per agent.** With subordinate keys
  ([example 13](../example-13-signed-agents/README.md)), every candidate and
  ballot reads as `AGENT_ATTESTED`, so the provenance names the extractor that
  signed each reading rather than only its host peer.
- **Embabel.** An Embabel `@Agent` can own a stage: its planner can run
  "classify, extract, validate" inside the extractor, while the space carries the
  result to the voters.
- **Watch it run.** [Example 09](../example-09-fleet-console/README.md) serves a
  console that shows queue depth, work in progress, and per-worker attribution
  from the same replicated state.
