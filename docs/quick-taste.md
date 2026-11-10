# Quick taste

Written for: Java developers who build with AgentSpaces.

Direct API:

```java
Space space = LocalSpace.builder("tasks", agentId).build();

space.write(new ResearchTask("agentic memory", 3),
    Lease.of(Duration.ofMinutes(30)));

Optional<TakenEntry<ResearchTask>> taken = space.take(
    Template.of(ResearchTask.class).where("priority", gte(3)),
    Lease.of(Duration.ofMinutes(10)),
    Duration.ofSeconds(5));

taken.ifPresent(t -> space.complete(t,
    new Finding(t.entry().topic(), "…"),
    Lease.of(Duration.ofHours(1))));
```

If the taker crashes instead of completing, the take lease lapses and the task reappears for another worker.

Most applications never write that. Annotations on plain objects do the work:

```java
@SpaceTake(lease = "10m")           // kill-tolerant worker
public Shipment ship(Order order) { ... }

@SpaceNotify                        // choreography: react to X, produce Y
public RiskAssessment assess(ExtractedInvoice invoice) { ... }

@Ballot(space = "votes", lease = "2h")   // one signed vote per proposal
public String judge(VoteCapability.Proposal proposal) { ... }

@OnDecision(space = "votes", resultSpace = "audit")   // once per proposal, when the quorum closes
public AdjudicatedFinding record(VoteCapability.Decision decision) { ... }

@OrderedTake(space = "payments", lease = "30s")  // exactly-once through the ordered log
public PaymentReceipt confirm(PaymentOrder order) { ... }
```

One shape throughout: the method's parameter is the cue, its return value is the next entry. The binder handles the rest:

- Deduplication, lease renewal, and thread offload for every worker and reaction.
- Watching for proposals, casting once, and remembering what was voted.
- Running an `@OnDecision` method exactly once when a vote closes.
- Inferring the sole space of a group, so it needs no naming; durations read like Spring properties (`"10m"`).
- Under Spring Boot, the `@SpaceAgent` stereotype makes enrollment one annotation.

With `agentspaces.identity.agent-keys=subordinate` every bound agent signs with a certified key of its own. The ballot above is then the panelist's attested record, and several agents on one peer are several voters wherever the authorizer counts per agent. The example apps in `agentspaces-example-apps/` and the Party Bus are written entirely this way.
