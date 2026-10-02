# Example 12: Exactly-Once Desk

Exactly-once processing for work that must never happen twice. Three clerks
share a Raft-backed ordered log over the `payments` space, and a fourth peer,
the desk, writes payment orders and never joins the log. Exactly one clerk
confirms each order, the confirmation replicates as a signed entry, and the
order drains from every replica. The desk's replica drains too, although the
desk holds no claim of its own and learns of each completion only by gossip.

LEASE_RACE (example 02) and AUCTION (example 04) decide each claim in the
space's claim lattice, which suits idempotent work. A payment is
non-idempotent, since charging it twice is the bug. This example routes the
take through the `ordered-log` capability instead: a small Raft quorum commits
exactly one claim per epoch of each order before any clerk acts, which gives
exactly-once take semantics at the price of quorum liveness (SPEC §7.4).

## What it demonstrates

- **`@OrderedTake`, the exactly-once worker.** It is `@SpaceTake` with the take
  routed through the space's ordered-log coordinator. The binder owns the loop,
  and the resubmit after a leader election.
- **ORDERED is a coordinator.** The space is built with the default strategy
  and taken through `OrderedTakes`. `ReplicatedSpace.Builder.strategy(ORDERED)`
  is rejected, because the space alone cannot provide the guarantee.
- **Registration drives the log.** Registering the coordinator on the peer's
  `CapabilityRuntime` puts the Raft log on the peer's own tick (elections,
  heartbeats, and the leader lease) and advertises it. The desk, which is not a
  log member, finds the leader through discovery alone.
- **Authenticated completions at a distance.** The claim the log commits carries
  its holder's own signature, so the desk can authenticate a completion it never
  witnessed and drain the order from its replica.

## The API in this example

The clerk, as one annotated method:

```java
@AgentSpec(name = "clerk", description = "Confirms payments exactly once", goals = {"confirm"})
public static final class Clerk {
    @OrderedTake(space = PAYMENTS, lease = "30s", pollTimeout = "2s", resultLease = "1h")
    public PaymentReceipt confirm(PaymentOrder order) {
        return new PaymentReceipt(order.orderId(), order.payee(), order.cents(),
                peer.name(), peer.raft().commitIndex());
    }
}
```

Seating a clerk: the log and its coordinator are wired together over the
payments space, and registering them is the whole of the wiring:

```java
OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(runtime, codec), codec,
        identity, name, members, InstantSource.system(), seed, payments);
group.ordered(PAYMENTS, ordered);   // registers the log on the peer tick; enables @OrderedTake
group.bind(new Clerk(peer));
```

Finding the leader from outside the log:

```java
Optional<CapabilityAdvertisement> leader = desk.discovery().find(CapabilityAdvertisement.class,
        ad -> RaftLog.TYPE.equals(ad.capabilityType()) && "leader".equals(ad.parameters().get("role")))
        .stream().findFirst();
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `@OrderedTake(space, lease, pollTimeout, resultLease)` | `agentspaces-agent` | The exactly-once worker |
| `OrderedTakes.over(...)` | `agentspaces-capabilities` | The ORDERED take coordinator, wired to its Raft log and space |
| `RaftLog`, `isLeader()`, `commitIndex()` | `agentspaces-capabilities` | The ordered log; its leader lease is an advertisement |
| `GroupContext.ordered(space, coordinator)` | `agentspaces-agent` | Registers the coordinator and backs `@OrderedTake` on the space |
| `CapabilityRuntime` | `agentspaces-capabilities` | Drives registered capabilities on the peer tick and advertises them |
| `CapabilityAdvertisement` | `agentspaces-api` | How the desk discovers `role=leader` and `leaseUntil` |

Source: [ExactlyOnceDesk.java](src/main/java/ai/badmonkey/agentspaces/examples/desk/ExactlyOnceDesk.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), clerks on ports
7601 to 7603 and the desk on 7604:

```
mvn -q -pl examples/example-12-exactly-once-desk exec:java
```

The demo prints the leader as the desk discovers it, each receipt with the
confirming clerk and log index, and the number of orders still open at the
desk, which is zero.

[ExactlyOnceDeskFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/desk/ExactlyOnceDeskFlowTest.java)
asserts over real TCP that exactly one clerk leads, that the desk discovers the
leader with its lease, that three orders yield exactly three receipts, and that
the orders drain at the desk and at every clerk:

```
mvn -q -pl examples/example-12-exactly-once-desk test
```

## Next steps

- **Choose the strategy by the cost of a duplicate.** Use LEASE_RACE for
  idempotent work (research, summaries, most model calls), AUCTION when cost or
  skill should decide who works, and ORDERED only where a duplicate side effect
  is unacceptable (payments, bookings, external writes). Raft needs a fixed
  member set and a quorum, so ORDERED costs more per take.
- **Put a side effect behind it.** The `confirm` method is where a real payment
  gateway, booking API, or ticketing system call belongs. The log guarantees that
  one clerk holds each order at a time, and the receipt records which clerk and
  which log index. A clerk that crashes after the external call but before the
  receipt lets its lease lapse, and the log reassigns the order, so pass the
  `orderId` to the external system as its idempotency key.
- **Spring Boot.** `agentspaces.capabilities.ordered-log=true` deliberately
  fails fast and asks for a `@ProvidesCapability` bean that wraps a `RaftLog`,
  because only the application knows the member set. Restrict who may vote and
  lead with `agentspaces.security.grants.raft-voter`.
- **See it in an application.** The [Party Bus](../../../agentspaces-partybus/README.md)
  confirms every booking exactly once through `@OrderedTake` clerks, alongside
  auctions and votes in the same fleet.
- **Prove which agent confirmed.** Combine this example with subordinate keys
  ([example 13](../example-13-signed-agents/README.md)): the committed claim then
  carries the clerk agent's own attestation.
- **Read the design.** SPEC §8 in [agentspaces-spec](../../../agentspaces-spec/README.md)
  specifies the ordered log and its leader lease.
