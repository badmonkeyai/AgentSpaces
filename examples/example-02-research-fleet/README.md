# Example 02: Research Fleet

The replicated-worker idiom over the full stack, Layers 0 through 3, on real
TCP. A coordinator peer publishes `ResearchTask` entries into a replicated
space, worker peers drain them under the LEASE_RACE strategy, one worker dies
mid-task, its take lease lapses, the task reappears, and the fleet finishes
everything anyway. Every write, claim, and completion travels as a signed,
verified, gossip-replicated delta.

Example 01 showed the verbs in one JVM. This example keeps the verbs
unchanged and swaps `LocalSpace` for `ReplicatedSpace`, so the difference you
see is only the assembly of a peer: an identity, a transport, a group, and a
space.

Narrative walk-through: [examples guide, example 02](../examples-guide.html#ex-02).

## What it demonstrates

- **Assembling a peer by hand.** `PeerNode` (Layer 1) listens on TCP, joins a
  group through a seed, and ticks; `ReplicatedSpace` (Layer 3) replicates over
  that group's gossip.
- **Self-certifying groups.** `GroupId.fromFounding(bytes)` derives the group ID
  from a founding document, so every peer that knows the document computes the
  same group without a registry.
- **Kill tolerance across processes.** The dead worker's claim is a leased,
  signed entry in the claim lattice. When its lease lapses, every replica agrees
  the task is free again, and a surviving worker completes it exactly once.
- **One replica per space name per node.** A second handle for an open space
  name is refused with an `IllegalStateException`; closing the space frees the
  name, and a reopened handle converges with the fleet.

## The API in this example

One peer, assembled in `startPeer`:

```java
PeerIdentity identity = PeerIdentity.generate();
PeerNode node = PeerNode.builder(identity).build();
node.listen(new TcpTransport(), "127.0.0.1:" + port);

GroupRuntime runtime = node.joinGroup(fleetGroup(),
        new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
        List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0)));

ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, name)
        .settleWindow(Duration.ofMillis(150))
        .build();
node.startTicking(Duration.ofMillis(250));
```

The group itself, derived from a founding document:

```java
GroupId groupId = GroupId.fromFounding("research-fleet-demo-v1".getBytes(UTF_8));
new GroupAdvertisement("aspace://" + groupId.value(), founder, groupId, Instant.EPOCH,
        Duration.ofDays(365), "research-fleet",
        MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE, GossipParameters.defaults());
```

The worker loop (`workerLoop`) is example 01's take and complete, called on a
`ReplicatedSpace`.

| Type or call | Module | Role here |
| --- | --- | --- |
| `PeerNode`, `PeerNode.builder(identity)` | `agentspaces-peering` | The node: transports, groups, and the tick that drives gossip and leases |
| `TcpTransport` | `agentspaces-peering` | Length-prefixed CBOR frames over TCP |
| `GroupRuntime`, `GroupMembership.Config` | `agentspaces-peering` | Leased, SWIM-style membership in one group |
| `GroupAdvertisement`, `GroupId.fromFounding` | `agentspaces-api`, `agentspaces-common` | The group's identity, policy, and default conflict strategy |
| `ReplicatedSpace.builder(runtime, name, identity, agent)` | `agentspaces-space` | The delta-CRDT space over the group's gossip |
| `settleWindow(Duration)` | `agentspaces-space` | How long a take waits for competing claims to replicate before it checks whether its own claim won |

Source: [ResearchFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/fleet/ResearchFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), run all three
peers in one process on ports 7451 to 7453:

```
mvn -q -pl examples/example-02-research-fleet exec:java
```

Or run each peer as its own process, which is the LAN demo. Start the
coordinator first; each worker seeds from it:

```
mvn -q -pl examples/example-02-research-fleet exec:java -Dexec.args="coordinator 7451"
mvn -q -pl examples/example-02-research-fleet exec:java -Dexec.args="worker 7452 7451"
mvn -q -pl examples/example-02-research-fleet exec:java -Dexec.args="worker 7453 7451"
```

Kill a worker with Ctrl-C while it holds a task and watch the other worker
pick the task up once the lease lapses.

[ResearchFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/fleet/ResearchFleetFlowTest.java)
runs over real TCP on free ports:

- `fleetDrainsTasksWithKillToleranceOverTcp` kills a worker holding a claim and
  asserts that every task completes exactly once and the coordinator converges
  on all findings.
- `aClosedSpaceReopensByNameAndConverges` pins the one-replica-per-name rule.

```
mvn -q -pl examples/example-02-research-fleet test
```

## Next steps

- **Add workers in other languages.** The coordinator mode of this example is
  the Java side of `run-trilingual-demo.sh` at the workspace root: a Python
  worker ([agentspaces-python](../../../agentspaces-python/README.md)) and a
  TypeScript worker ([agentspaces-typescript](../../../agentspaces-typescript/README.md))
  join the same group over the same wire protocol and race for the same tasks.
- **Let agents describe themselves.** [Example 03](../example-03-discovery-cards/README.md)
  adds AgentCards, so a dispatcher can find which agent produces which entry
  type without being configured with it.
- **Replace the assembly with configuration.** Under Spring Boot, the
  [starter](../../agentspaces-spring-boot-starter/README.md) builds this whole
  peer from `agentspaces.bind`, `agentspaces.groups[].founding`,
  `agentspaces.groups[].seeds`, and `agentspaces.groups[].spaces[].name`, and a
  `@SpaceAgent` class with one `@SpaceTake` method replaces `workerLoop`.
- **Make the workers real.** A research worker is a natural Embabel agent: an
  `@Agent` whose `gather` and `write` actions call models through Spring AI, with
  `@SpaceTake` on the goal action so its input arrives from this space. See the
  [developer guide's Embabel section](../developer-guide.html#embabel).
- **Harden the transport.** Swap `TcpTransport` for TLS 1.3 or QUIC (RFC 9000),
  and choose a security profile (`DEV_LOCAL`, `MTLS`, `MTLS_OIDC`, `ZERO_TRUST`);
  the [main README](../../README.md#adding-a-transport) explains the transport SPI.
