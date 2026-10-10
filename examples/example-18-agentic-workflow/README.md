# Example 18: Agentic Workflow

A workflow built a stage at a time, every stage an annotated POJO method, and
the entries between the stages the whole of the protocol. Three peers share
five spaces. A claim is registered or rejected, three specialists examine it
in parallel, their findings are joined, the join is priced at auction and
forked into one quote task per repair shop, the quotes are gathered and the
lowest kept, severe claims are routed to a senior reviewer, a panel votes
under a deadline, an approved claim is paid exactly once, and every case
closes in a ledger while the fleet converges on its average reserve and a
supervisor reports it once it settles. There is no graph object, no workflow
engine, and no orchestrator process: each stage declares what it consumes by
its parameter type and what it produces by its return type, and the space
routes by type.

This is the worked example behind *Building AgentSpaces Workflows*, the
companion to the developer guide. Each stage below is the snippet one of its
chapters cuts.

## The flow

| Stage | Agent (file) | Binding | Space | What it shows |
| --- | --- | --- | --- | --- |
| 1 | `Registrar` (`Intake`) | `@SpaceTake` on `Claim`, returns the sealed `Registration` (`RegisteredClaim` or `Rejected`) | `intake` | The kill-tolerant stage: a crash lets the 3 s lease lapse and the claim reappears; a claim that fails the vocabulary's constraints is quarantined as `Rejected` |
| 2 | `FraudScreen`, `CoverageCheck`, `DamageEstimator` (`Intake`) | `@SpaceNotify` on `RegisteredClaim`, each returns its own finding; the estimator returns `Tagged.of(instance, tags)` | `intake` | Fan-out: one input, three specialists, outputs distinguishable by type; a return written with tags |
| 3 | `Assembler` (`Intake`) | `@SpaceJoin` over four parts, returns `ClaimAssessment` | `intake` to `assessment` | Fan-in as one annotation: once per claim, the damage finding keyed by its tag |
| 3a | `QuoteDesk` (`Quotes`) | `@SpaceNotify` on `ClaimAssessment`, returns `Entries` of one `QuoteRequest` and three `QuoteTask`s | `assessment` to `intake` | The fork: several entries from one cue, each dispatched as if returned alone; the card declares both types |
| 3b | `NorthShop`, `EastShop`, `SouthShop` (`Quotes`) | `@SpaceTake` on `QuoteTask` with `where = "shop=north"` (east, south), returns `RepairQuote` | `intake` | The route by field: each shop takes only its own tasks, one shop per peer |
| 3c | `QuoteGatherer` (`Quotes`) | `@SpaceJoin` over a `QuoteRequest` and as many `RepairQuote`s as it declares (`countedBy`), returns `RepairEstimate` | `intake` to `ledger` | The gather: wait for a number another entry declares, hand them all over, keep the lowest |
| 3d | `SeniorReviewer` (`Quotes`) | `@SpaceNotify` on `ClaimAssessment` with `where = "severity=HIGH"`, returns `SeniorReview` | `assessment` to `ledger` | The route on a reaction: the severe claim alone reaches the reviewer |
| 4 | `SeniorAdjuster`, `JuniorAdjuster` (`Pricing`) | `@BidFunction` + `@SpaceTake` on `ClaimAssessment`, return `PricedClaim` | `assessment` (AUCTION) | Cost-aware routing: the senior is cheap for severe claims, the junior for routine ones |
| 5 | `Underwriter` (`Decisions`) | `@Propose` on `PricedClaim`, returns the question; writes a leased `Reminder` first | `assessment` to `decisions` | Opening a decision as one annotation: the shape on the annotation, once per claim; the reminder's lease is the panel's deadline |
| 6 | `Panelist` ×3, `Approver` (`Decisions`) | `@Ballot` returns the option; `@OnDecision` returns `PaymentOrder` or writes `ClaimDenied` | `decisions` to `payments` / `ledger` | The gate: one ballot per peer, one reaction when the quorum closes |
| 6a | `Escalator` (`Decisions`) | `@SpaceNotify(on = EXPIRED)` on `Reminder`, returns a `Motion` or nothing | `decisions` | The deadline: a lease lapsing is the cue; an undecided claim escalates as a motion, once |
| 7 | `Payer` ×3 (`Settlement`) | `@OrderedTake` on `PaymentOrder`, returns `Payment` | `payments` | The irreversible step, completed at most once fleet-wide |
| 7a | `Exposure` (`Settlement`) | `@SpaceReduce` on `Payment` keyed by `claimant`, returns `ClaimantExposure` | `payments` | The persistent accumulator: each payment is taken and completed with the claimant's new exposure atomically, so it is counted once; the exposure is an entry any peer reads with `Reductions.current`, and its lease is the claimant's idle timeout. The ledger's reaction still sees every payment, since a reaction fires on the write and the take that follows does not retract it |
| 8 | `ReserveSensor` ×3, `Supervisor` (`Settlement`) | `@SpaceNotify` returns `Contribution`; `@OnEstimate` on the settled `Estimate` returns `ReserveReport` | `assessment`, `ledger` | Fleet sensing inside a flow, and one reaction when the estimate settles |
| 9 | `Ledger` (`Settlement`) | `@SpaceNotify` on `Payment`, `ClaimDenied`, and `Rejected`, returns `ClaimClosed` | `payments`, `intake`, `ledger` | The terminal record, under a lease that outlives the flow, for every case including the quarantined one |

The five claims the demo and the test put in exercise every path: a routine
collision (approved, priced by the junior adjuster), a claim on a lapsed
policy (denied), a staged accident (denied for fraud), a kitchen fire
(approved by two panelists against one, priced by the senior adjuster, capped
at the coverage limit, and the one claim the senior reviewer sees), and a
windscreen chip with no amount (rejected at the door, closed by the ledger).
Every assessed claim is quoted by all three shops and the east shop, which
prices lowest, wins each gather.

## The shape of data

[Claims.java](src/main/java/ai/badmonkey/agentspaces/examples/workflow/Claims.java)
mirrors a small claims vocabulary as records: the record is the class, its
components are the class's datatype properties, and an object property to
another individual travels as that individual's id (`policyId`), never as a
nested object. Every record carries the `claimId`, the one correlation key
that follows a case through every stage.

[Instance.java](src/main/java/ai/badmonkey/agentspaces/examples/workflow/Instance.java)
shows the other way an ontology reaches a space: the damage finding is a
generic instance record (`id`, `type`, `typeIri`, `properties`) of the
vocabulary's `DamageFinding` class. Templates can match `id` and `type`,
because they are components; they cannot look inside `properties`. So the
individual's id is derived from the claim it describes, and that is what the
assembler matches on:

```java
intake.read(Template.of(Instance.class).where("id", eq(Instance.damageId(claimId))));
```

On the wire each type is named `<fully.qualified.Name>#v1`, and a replica
selects candidates for a template by exact name before it decodes anything.

## What the test proves

[ClaimsFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/workflow/ClaimsFlowTest.java)
runs the three peers over real TCP and asserts:

- every case closes once, with the expected outcome, and every claim drains
  from `intake`;
- the registrar hook crashed exactly one registration and every claim was
  still registered;
- one priced claim per claim and no assessment left unpriced, with one
  `LOCAL` joiner and again with two `ORDERED` joiners on two peers;
- the fork wrote one request and three tasks per assessed claim, every task
  was taken by its own shop, and each gather kept the lowest of exactly three
  quotes;
- only the severe claim reached the senior reviewer, and the panel decided
  every claim before its reminder lapsed, so nothing escalated;
- the rejected claim closed as `REJECTED` and never reached the specialists;
- the auction sent the severe claim to the senior adjuster and the routine one
  to the junior, and the reserve is capped at the coverage limit;
- the panel split two to one on the large claim and still approved it;
- exactly one payment per approved claim at every replica, and the orders
  drained everywhere;
- the fleet's average reserve converges on the mean of the four reserves,
  and the supervisor reported it exactly once, after it settled;
- in a second fleet with no panel at all and a one-second reminder, the
  lapsed reminder escalated each claim as exactly one motion.

## The join

Fan-in is one annotation. The assembler declares the four parts of a claim
and the field that keys them; the damage finding, an ontology instance with no
`claimId` component, is keyed by the `claimId` tag the estimator wrote with
`Tagged.of(...)`:

```java
@SpaceJoin(space = "intake", key = "claimId", resultSpace = "assessment",
        parts = {@Part(RegisteredClaim.class), @Part(FraudFinding.class), @Part(CoverageFinding.class),
                 @Part(value = Instance.class, keyTag = "claimId", tags = "rdf:type=" + ...)})
public ClaimAssessment assemble(Joined j) { ... }
```

The binder subscribes to each part, seeds its state from the space on bind,
re-reads the parts by key when the last one lands, and fires the method once
per claim. How many times a claim may fire is the mode. `LOCAL`, the default
and what the demo runs, is once per bound joiner, so the demo binds one.
`ORDERED`, which `OrderedAssembler` uses and the second test runs with two
joiners on two peers, writes a `JoinTicket` entry when a claim completes and
takes it through an ordered-log coordinator, so the first committed ticket
per claim is the only one that fires, fleet-wide. A peer hosts one ordered
log per group, and this fleet's log is on `payments`, so the ordered join
lives there (its tickets go through that log) while its parts stay in
`intake`: a join's space is where its tickets live, and its parts may live
anywhere. `LEASED` sits
between: a ticket taken under the space's own lease race.

The first version of this example wrote the join by hand and fell into the
trap the annotation now avoids: it guarded against a duplicate by reading for
its own result, and the next stage's auction take had consumed that result
within milliseconds. One claim was priced twice.

## The verbs

The other shapes a workflow takes are the same two moves, a method and an
entry, with the shape declared on the annotation.

**Fork.** One cue, several entries. The quote desk returns `Entries`, and the
binder dispatches each element as if the method had returned it alone, so an
element may itself be a `Tagged` or a `Motion`. The card cannot see inside a
collection, so the annotation declares what it produces:

```java
@SpaceNotify(space = Pricing.SPACE, resultSpace = Intake.SPACE, resultLease = "1h",
        produces = {QuoteRequest.class, QuoteTask.class})
public Entries ask(ClaimAssessment assessment) {
    return Entries.of(new QuoteRequest(assessment.claimId(), SHOPS.size()),
            new QuoteTask(assessment.claimId(), "north", assessment.damageEstimate()),
            new QuoteTask(assessment.claimId(), "east", assessment.damageEstimate()),
            new QuoteTask(assessment.claimId(), "south", assessment.damageEstimate()));
}
```

**Route by field.** `where` narrows a take or a reaction to entries whose
component has a value, before the method is called and before a take claims
anything. Each shop is a class with its own literal filter, because an
annotation value is a constant; the shops are deployed one per peer:

```java
@SpaceTake(space = Intake.SPACE, where = "shop=north", lease = "30s", pollTimeout = "300ms",
        resultLease = "1h")
public RepairQuote quote(QuoteTask task) {
    return Quotes.quote(task, "north", 1.1);
}
```

`where = "severity=HIGH"` on the senior reviewer's `@SpaceNotify` is the same
filter on a reaction; `!=` negates. A filter names a component of the
parameter type, and the binder refuses one that does not at bind time.

**Gather.** A join whose part may appear several times for one key. The
request says how many quotes to expect, so the gather waits for exactly that
many and hands them all over through `Joined.all(...)`:

```java
@SpaceJoin(space = Intake.SPACE, key = "claimId", resultSpace = Settlement.LEDGER, resultLease = "24h",
        parts = {@Part(QuoteRequest.class),
                 @Part(value = RepairQuote.class, countedBy = "QuoteRequest.expected")})
public RepairEstimate pick(Joined j) {
    List<RepairQuote> quotes = j.all(RepairQuote.class);
    RepairQuote lowest = quotes.stream().min(Comparator.comparingDouble(RepairQuote::amount)).orElseThrow();
    return new RepairEstimate(j.key(), lowest.shop(), lowest.amount(), quotes.size());
}
```

`atLeast = n` on a part is the fixed form, and `settle = "300ms"` on the
join ends a gather of an unknown number once the key has been quiet for that
long. The request may land after the quotes; the count is read whenever it
arrives.

**Branch.** A sealed return declares its branches. The registrar returns
`Registration`, which permits `RegisteredClaim` and `Rejected`; the concrete
one is written and the card lists both. A rejection is an entry, so the
ledger closes it like any other case rather than the flow losing it:

```java
@SpaceTake(space = SPACE, lease = "3s", pollTimeout = "300ms", resultLease = "1h")
public Registration register(Claim claim) {
    hook.beforeRegistering(name, claim);
    if (claim.claimedAmount() <= 0) {
        return new Rejected(claim.claimId(), "claimedAmount must be positive");
    }
    ...
}
```

**Deadline.** A lease is a timer when a reaction listens for its expiry. The
underwriter writes a `Reminder` under the panel's deadline when it opens each
decision; the escalator reacts to the lapse alone and returns a
`Motion` only if the panel has not decided:

```java
@SpaceNotify(space = SPACE, on = SpaceEvent.Kind.EXPIRED)
public Motion escalate(Reminder reminder) {
    if (votes.decision(PREFIX + reminder.claimId()).isPresent()) {
        return null;                       // decided in time: nothing to escalate
    }
    return Motion.of("escalate:" + reminder.claimId(), "The panel has not decided claim "
            + reminder.claimId() + " in time; escalate to the chief underwriter?",
            List.of("escalate", "wait"), 1, Lease.of(Duration.ofMinutes(10)));
}
```

Every replica sees the lapse, and a `Motion` opens once per proposal id, so
the escalation is once per claim however many peers host an escalator.

**Settled estimate.** `@OnEstimate` fires once per epoch, when the push-sum
estimate has stayed within a tolerance for a number of protocol ticks. The
peers tick every 250 ms, so twenty ticks is five quiet seconds, longer than
the spread between the first and the last priced claim:

```java
@OnEstimate(prefix = "reserve:", settleTicks = 20, tolerance = 0.01, resultSpace = LEDGER,
        resultLease = "24h")
public ReserveReport report(PushSumAggregate.Estimate reserve) {
    return new ReserveReport(reserve.epochId(), reserve.value(), name);
}
```

The capability evaluates the rule on its own tick; nothing polls. The
procedural form is `AggregateClient.awaitSettled(epoch, settle, timeout)`.

## Running it

From the `agentspaces/` repository root (after one `mvn install -DskipTests`),
three peers on ports 7711 to 7713:

```
mvn -q -pl examples/example-18-agentic-workflow exec:java
```

The demo prints the claims as they go in, the registrar's simulated crash
(and the binder's warning that the lease will lapse), the priced claims with
the adjuster that won each, the payments with the clerk and log index, the
repair estimates with the shop that won each gather, the senior review of
the severe claim, the ledger with its rejected case, and the fleet's average
reserve.

```
mvn -q -pl examples/example-18-agentic-workflow test
```

## Next steps

- **Put a model in a stage.** The adjusters are deterministic here. A stage
  can call a Spring AI `ChatClient` and return the structured-output record as
  the next entry; `TakeLeaseAdvisor` in `agentspaces-springai` renews the take
  while the model thinks (the opscenter example's `Triage` is the reference).
- **Compose stages with a planner.** Embabel's remote actions turn each
  stage's card into a typed action; invocation is still a write and a
  correlated read ([example 07](../example-07-a2a-fleet/README.md) and the
  developer guide, chapter 3).
- **Make the join exactly-once.** Bind `OrderedAssembler` on several peers, as
  the second test does; it rides the payments log
  ([example 12](../example-12-exactly-once-desk/README.md) explains the log).
- **Attribute every ballot to an agent.** Bind the panelists with a
  subordinate identity factory ([example 13](../example-13-signed-agents/README.md))
  and count per agent.
- **Spring Boot.** The same classes bind under the starter with `@SpaceAgent`;
  the five spaces are five lines of YAML, and the vote and aggregate come from
  `agentspaces.capabilities.*`. The ordered log still wants its member set
  from a `@ProvidesCapability` bean.
