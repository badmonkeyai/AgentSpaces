# AgentSpaces

**A peer-to-peer coordination fabric for agents, services, and the people who work with them.**

AgentSpaces gives a fleet of agents, and the software services and human operators around them, a shared space to coordinate. Agents post work, claim tasks, publish results, vote on decisions, and recover from each other's failures. There is no broker, no registry, no scheduler, and no server to keep alive. Coordination is peer-to-peer, signed end to end, and replicated across the fleet.

```java
@AgentSpec(name = "fulfiller", description = "Ships orders")
public class Fulfiller {

    @SpaceTake(lease = "10m")
    public Shipment ship(Order order) {
        return new Shipment(order.orderId(), order.item(), "fulfiller");
    }
}
```

Drop that in a Spring Boot app. Start a second instance. You now have competing consumers. Kill one mid-order and the order comes back for the other. That's the whole model: shared state is leased, and failure handling is the absence of renewal.

## Why this exists

Agents are ephemeral. They crash, stall on model calls, and get built by different teams on different frameworks in different languages. Coordinating them today means stitching together a message queue, a database, a service registry, a workflow engine, and an audit log, and then discovering that none of those components were designed for agents.

AgentSpaces replaces that stack with one coordination layer built for autonomous, failure-prone, independently deployable participants.

## What you can build with it

- **Autonomous operations.** Incidents are claimed by specialist agents, investigated, escalated, and remediated. A crashed worker's task reappears automatically. Humans approve consequential actions.
- **Agentic back offices.** Business processes become shared state. Specialists join without redesigning a workflow.
- **Model markets.** Inference is discovered, bid on, and routed by cost, latency, and confidence. Stronger models are used only when uncertainty justifies the cost.
- **Self-assembling teams.** Agents form around a problem and dissolve when it's solved.
- **Edge fleets.** Robots and sensors keep coordinating when the network doesn't.
- **Enterprise meshes.** Federate existing agents, services, and data without replacing them.

## How it works

- **Peers join a group.** Each peer has a cryptographic identity.
- **Participants advertise capabilities** through signed, leased documents. Agents create AgentCards; non-agents (databases, sensors, services) create AssetCards.
- **Everyone shares a replicated tuple space**, a coordination surface with `write`, `read`, `take`, and `notify`.
- **Work is written to the space.** Workers claim it with a lease. If a worker crashes, the lease expires and the work reappears.
- **Results are signed and attributable.**

Coordination strength is selectable:

| Need | Mechanism |
|---|---|
| Simple work distribution | Lease races |
| Resource allocation | Auctions |
| Decisions | Quorum voting |
| Irreversible actions | An ordered log |

The core provides leases, gossip, and CRDT replication. The stronger semantics are Layer 4 capabilities that peers advertise and use.

**The design test:** if you add a new capable peer tomorrow, can the system become more capable without redesigning the system today?

## What makes it different

- **No central coordinator.** Peer groups, gossip, and CRDT replication mean there's no broker, registry, or workflow engine in the critical path.
- **Signed provenance.** Every entry, ballot, and claim is signed. You can always trace who did what.
- **Leases as failure handling.** Failure recovery is a property of the data model, not a separate subsystem.
- **Selectable coordination.** Use the least expensive mechanism that satisfies the workload. Uplift to auction, quorum, or ordered consensus only where consequence requires it.
- **Extensible.** New coordination patterns are advertised capabilities, not core changes.
- **Polyglot.** Java, Python, TypeScript, and Clojure peers share the same signed wire protocol.
- **Trust boundaries.** Self-certifying groups, per-agent keys, group-key encryption, and pluggable membership and authorization let fleets span organizations.

## Where it came from

The ideas behind AgentSpaces are old. Tuple spaces were described in 1985. Peer-to-peer groups were tried in the late 1990s. Two of our founders worked through that era at Bell Labs, Lucent, and Avaya and wanted to build with those systems. Neither Jini nor JXTA was ever quite right: Java-only, centralized spaces, security as an afterthought.

Agents are what changed the math. They're ephemeral, failure-prone, and built by many teams. They need the coordination model that the earlier systems imagined, implemented with tools that didn't exist then: CRDTs, Ed25519, QUIC, and signed CBOR over gossip.

You don't need to know any of that history to use AgentSpaces. But if you're curious, it's a good story.

## Quick taste

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

## Get started

Requires JDK 21+ and Maven.

```
git clone https://github.com/badmonkeyai/AgentSpaces.git agentspaces
mvn -f agentspaces/pom.xml install -DskipTests
cd agentspaces && mvn -q -pl examples/example-11-quickstart exec:java
```

Run it in five minutes. The full quickstart lives in `examples/example-11-quickstart`, the whole experience in one file.

## What this is not

Honest boundaries:

- Not a database. The space holds coordination state and small payloads.
- Not a blockchain. No Byzantine fault tolerance. The threat model is one organization's trust boundary.
- Not a replacement for MCP or A2A. MCP stays the tool layer. The A2A gateway is a bridge.
- Eventually consistent by default. `ConsistencyHint.FRESH` costs a round trip.
- CRDT convergence is not semantic agreement. A valid signature is not safe content. Quorum votes and ordered spaces exist for the cases that need agreement.
- Delivery is at least once unless the space uses the ordered strategy.
- No independent security review has finished yet. Version 0.2.0. The 46-test security regression suite and the adversarial golden vectors exist; the external review is open.
- No published performance numbers yet.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4  CAPABILITY SERVICES (advertised, pluggable)            │
│   task-auction (CBBA)  ·  vote  ·  aggregate (push-sum)         │
│   gossip-learn  ·  ordered-log (Raft quorum)  ·  custom…        │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3  AGENTSPACE (distributed tuple space)                   │
│   write/read/take/notify · templates · leases · CRDT replication│
│   conflict-resolution strategies · content-addressed payloads   │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2  ADVERTISEMENT & DISCOVERY                              │
│   typed signed documents · TTL · local ad-cache · queries       │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1  PEERING                                                │
│   peer groups · SWIM membership · gossip dissemination          │
│   rendezvous & relay roles · transport SPI                      │
├─────────────────────────────────────────────────────────────────┤
│ Layer 0  IDENTITY                                               │
│   keypair PeerID · signatures · (optional) DID interop          │
└─────────────────────────────────────────────────────────────────┘
```

Each layer depends only on the layers below it. A conforming minimal implementation comprises Layers 0 through 3 on a single LAN with the default conflict strategy; capability services and WAN topology are additive.

The full design lives in `SPEC.md` (v0.1.13) with implementation detail in `TECH-SPEC.md`, both in `agentspaces-spec/` beside this repository, together with the golden vectors they define conformance by (`golden.json`, vendored here as `tools/golden/golden.json`).

## Version 0.2.0 Capabilities

Every specification layer (0 through 4) has a working implementation, and every clause the specification does not mark as planned has a pinning test.

**Identity and peering**

- Ed25519 peer identities with self-certifying PeerIDs.
- Per-agent subordinate keys the peer certifies, so a record, ballot, or take claim is attributed to the agent that signed it (`AGENT_ATTESTED`) rather than to its host peer's word.
- Self-certifying groups: the GroupID is the hash of a founder-signed founding document, so a newcomer can join by GroupID alone and verify what a seed hands it.
- Leased membership with local suspicion.
- Two-channel gossip: rumor plus anti-entropy, paced by the group's gossip period.
- Rendezvous and relay roles.
- A language-independent wire protocol of signed CBOR envelopes (version 2, enforced).

**Transports**

- In-JVM loopback for tests.
- TCP.
- TLS 1.3 with identity-endorsed channel certificates: bare frames on attested links, and CA-issued certificates with CRL or OCSP revocation in enterprise mode.
- QUIC (RFC 9000).
- An opt-in multicast bootstrap beacon for LANs.

**Discovery**

- A signature-verifying, TTL-evicting advertisement cache per group, four times larger on rendezvous peers.
- Local-first `find` and hop-budgeted remote queries.
- A semantic index behind a pluggable embedder.
- Advertisement types: peers, groups, spaces (with admission and replication), capabilities, AgentCards (with space bindings), AssetCards, and revocations.

**The space**

- `LocalSpace` for one JVM; `ReplicatedSpace` as a delta-CRDT over gossip.
- FIFO matching within a type, and leases on everything.
- Content-addressed payloads over 64 KiB, fetched without blocking reads.
- Group content-key encryption, tag sharding, tombstone garbage collection, and `ConsistencyHint.FRESH`.
- The full event stream (WRITTEN, TAKEN, COMPLETED, EXPIRED, REAPPEARED), including WRITTEN for content-addressed entries the moment their block lands.
- All three conflict strategies: LEASE_RACE with bounded claim stamps, AUCTION with bids in the claim lattice, and ORDERED through the Raft-backed take coordinator.
- Spaces advertise themselves on creation and turn read-only when their founders' lease lapses.
- Admission by group, allowlist, leased credentials the issuer writes into the space, or the profile's `Authorizer`.
- Per-agent views (`space.as(identity)`) over one replica and one clock per node, with each entry reporting its issuer and attestation.

**Capabilities**

- `aggregate`: sum, avg, count, min, max, and quantile, over values or over a template's matching entries.
- `vote`: MAJORITY_GOSSIP, and QUORUM with a fresh-AgentCard electorate whose tally counts one voter per peer or per attested agent, at the authorizer's granularity, so one member cannot multiply itself.
- `ordered-log`: Raft, with the leader lease as an advertisement and a take coordinator whose committed claims carry the holder's own attestation.
- `gossip-learn`: a mergeable-model SPI, content-addressed models, a mass-conserving offer/accept exchange, and epoch evaluations.
- `semantic-discovery`.
- `key-wrap`: X25519 + HKDF + AES-GCM sealed key distribution.
- Every capability registered on a peer's `CapabilityRuntime` is driven by that peer's one clock and advertised to the fleet. Nothing needs a driver of its own.

**Security**

- Every advertisement, record, state transition, and take claim is signed. Forged completions, removals, leases, and back-dated claims are dropped.
- Membership policy (OPEN, INVITE, POLICY) is enforced at admission.
- Revocations eject fleet-wide, and CA revocation ejects under required attestation.
- Per-issuer rate limits, strikes, and quarantine bound abuse.
- Every privileged operation asks one `Authorizer` (`permits(peer | agent, operation, scope)`), rooted in membership with optional per-peer or per-agent grants, or in identity-provider tokens.
- Four security profiles select the posture: `DEV_LOCAL`, `MTLS`, `MTLS_OIDC`, `ZERO_TRUST`.

**Programming model**

- Annotations on plain objects, one shape throughout: the method's parameter is the cue, its return value is the next entry.
- Layer 3: `@SpaceTake` (the kill-tolerant worker), `@SpaceNotify` (choreography), `@BidFunction`, and `@SpaceRef`.
- Layer 4: `@Ballot` (one signed vote per proposal), `@OnDecision` (react once when a vote closes), `@OrderedTake` (the exactly-once worker through the ordered log), `@CapabilityRef` (a typed client injected by field), a `Contribution` return that feeds a push-sum epoch, and `@ProvidesCapability` for serving one.
- Every binding takes a `group` attribute for multi-group beans and fails fast at bind time when what it needs is not registered.
- Automatic AgentCards into every joined group, and typed capability clients (`VoteClient`, `AggregateClient`, `SemanticClient`).
- An identity factory so every bound agent can sign with a certified key of its own (`agentspaces.identity.agent-keys=subordinate`).
- A Spring Boot starter driven by `agentspaces.*` properties.
- An Embabel bridge that publishes `@Agent` metadata as cards and turns the fleet's cards into typed planner actions that deploy themselves onto the platform.

**Interfaces and clients**

- An A2A gateway: discovery, tasks, streaming, push.
- A fleet console with command and control.
- A connector SDK with catalog and materializing providers.
- Python and TypeScript peers proven byte-identical to Java against shared golden vectors covering every wire structure (agent certificates, attested records, claim proofs, state transitions, keyed AgentCards with declared actions, credential revocations, content-key epochs, and the QUORUM tally), and refusing the same hostile inputs.
- Both clients sign as agents of their own and apply revocations.

**Keys, revocation, and rotation (v0.1.13)**

- Agent certificates renew and are judged at signing time.
- Anything certified (peers, agents, agent keys, channel leaves, join credentials) is revocable under a freeze rule, and the enterprise CA can root a peer revocation with evidence.
- Group content keys rotate by epoch, and an agent can hold a content key in its own right.
- The developer guide's Security chapter walks through the options.

## Build

JDK 21+ and Maven are the only prerequisites. `BUILD.md` at the workspace root is the full runbook.

Build and test everything (the release gate, roughly 3 minutes):

```
mvn -T 1C -Pspring-it clean verify -Dgolden.required=true
```

- `-Pspring-it` adds the real Spring Boot integration test, the starter's release gate. CI activates it.
- `-Dgolden.required=true` makes the cross-language golden-vector tests fail instead of silently skipping when golden.json is missing.
- Always `clean`. An IDE compiling alongside Maven can leave error-stub classes in `target/`.
- The QUIC module's pom picks the right native classifier per OS (Linux and macOS); elsewhere its tests skip.
- In CI (and with `-Pcoverage`) JaCoCo enforces per-module line-coverage gates.
- Use `install` instead of `verify` if other projects need the snapshots in `~/.m2`.

Faster loops:

```
# one module (install its dependencies once first)
mvn -q -pl agentspaces-space -am install -DskipTests
mvn -pl agentspaces-space test
```

```
# one test class or method
mvn -pl agentspaces-peering test -Dtest='RevocationTest#aFounderRevocationEjectsFleetWideAndSurvivesForLateJoiners'
```

If you select tests across several modules, add `-Dsurefire.failIfNoSpecifiedTests=false`. Otherwise any module with no matching test fails the build.

Security regression suites (46 tests), worth running after touching an enforcement path:

```
mvn -pl agentspaces-space,agentspaces-peering,agentspaces-capabilities,agentspaces-console test \
  -Dtest='ForgedStateRejectionTest,NonMemberIsolationTest,AdversarialVectorsTest,RevocationTest,ProbeForgeryTest,GossipBusTest,SpaceAdmissionTest,DirectiveGateTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Run the first example:

```
mvn -q -pl examples/example-01-hello-space exec:java
```

The full workspace gate (about 15 minutes) is `verify-all.sh`, run from the workspace root where all repos have been checked out. It runs this reactor's gate and then every standalone project against the freshly installed libraries, which is how they are kept from drifting (QA4 A4-2) without being part of the core build:

- A check that the golden-vector copies match.
- The Python, TypeScript, and Clojure clients.
- The example apps and the Party Bus.
- agentspaces-springai.
- A compile of the perf harnesses.

`docs/BUILD-ENVIRONMENTS.md` covers the version policy, the Spring stub architecture, and regenerating the shared golden vectors.

Build success should look like:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for AgentSpaces 0.2.0:
[INFO]
[INFO] AgentSpaces :: Dependencies (BOM) .................. SUCCESS [  0.044 s]
[INFO] AgentSpaces ........................................ SUCCESS [  0.044 s]
[INFO] AgentSpaces :: Common .............................. SUCCESS [  1.393 s]
[INFO] AgentSpaces :: API ................................. SUCCESS [  0.539 s]
[INFO] AgentSpaces :: Test Support ........................ SUCCESS [  0.485 s]
[INFO] AgentSpaces :: Identity (Layer 0) .................. SUCCESS [  0.784 s]
[INFO] AgentSpaces :: OIDC Authorization .................. SUCCESS [  0.884 s]
[INFO] AgentSpaces :: Peering (Layer 1) ................... SUCCESS [  6.419 s]
[INFO] AgentSpaces :: Space (Layer 3) ..................... SUCCESS [ 12.392 s]
[INFO] AgentSpaces :: Discovery (Layer 2) ................. SUCCESS [  6.189 s]
[INFO] AgentSpaces :: Capabilities (Layer 4) .............. SUCCESS [01:15 min]
[INFO] AgentSpaces :: Agent Binding ....................... SUCCESS [ 30.222 s]
[INFO] AgentSpaces :: A2A Gateway ......................... SUCCESS [  2.752 s]
[INFO] AgentSpaces :: Connector SDK ....................... SUCCESS [  4.493 s]
[INFO] AgentSpaces :: Fleet Console ....................... SUCCESS [  2.658 s]
[INFO] AgentSpaces :: Spring API Stubs (build-time only) .. SUCCESS [  0.551 s]
[INFO] AgentSpaces :: Spring Boot Autoconfigure ........... SUCCESS [  8.695 s]
[INFO] AgentSpaces :: Spring Boot Starter ................. SUCCESS [  0.010 s]
[INFO] AgentSpaces :: Embabel Extension ................... SUCCESS [  6.826 s]
[INFO] AgentSpaces :: Examples ............................ SUCCESS [  0.002 s]
[INFO] AgentSpaces :: Example 01 :: Hello Space ........... SUCCESS [  0.677 s]
[INFO] AgentSpaces :: Example 02 :: Research Fleet ........ SUCCESS [  5.715 s]
[INFO] AgentSpaces :: Example 03 :: CardsFleet ............ SUCCESS [  2.539 s]
[INFO] AgentSpaces :: Example 04 :: AuctionFleet .......... SUCCESS [  4.957 s]
[INFO] AgentSpaces :: Example 05 :: QuorumFleet ........... SUCCESS [ 11.454 s]
[INFO] AgentSpaces :: Example 06 :: WanFleet .............. SUCCESS [  2.140 s]
[INFO] AgentSpaces :: Example 07 :: A2aFleet .............. SUCCESS [  2.055 s]
[INFO] AgentSpaces :: Example 08 :: IntakeFleet ........... SUCCESS [  4.196 s]
[INFO] AgentSpaces :: Example 09 :: FleetConsole .......... SUCCESS [  9.807 s]
[INFO] AgentSpaces :: Example 10 :: DataFleet ............. SUCCESS [  3.481 s]
[INFO] AgentSpaces :: Example 11 :: Quickstart ............ SUCCESS [  1.871 s]
[INFO] AgentSpaces :: Example 12 :: ExactlyOnceDesk ....... SUCCESS [  4.520 s]
[INFO] AgentSpaces :: Example 13 :: SignedAgents .......... SUCCESS [  7.717 s]
[INFO] AgentSpaces :: Example 14 :: GossipLearning ........ SUCCESS [  4.541 s]
[INFO] AgentSpaces :: Spring Boot Integration Test ........ SUCCESS [  2.724 s]
[INFO] AgentSpaces :: Embabel Integration Test ............ SUCCESS [  2.589 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
```

## Modules

Each module in two or three sentences. The next subsection places them in the
architecture.

| Module | Description |
|---|---|
| `agentspaces-dependencies` | The bill of materials. Applications import it to align every AgentSpaces artifact and the third-party versions the suite is tested against (Jackson, BouncyCastle, Netty QUIC, Nimbus). |
| `agentspaces-common` | The foundations every layer shares: the canonical CBOR codec, Ed25519 and X25519 helpers, the group content key with its seal counter, the hybrid logical clock with drift clamp, Base58 and multibase, and the identifier types (`PeerId`, `GroupId`, `SpaceId`, `AgentId`, `aspace://` URIs). It has no AgentSpaces dependencies. |
| `agentspaces-api` | The public contract. `Space` and its verbs, the `Template` matcher DSL, leases and entry records, the advertisement family (peer, group, space, capability, AgentCard, AssetCard, revocation), the `SecurityProfile` enum, and the SPI extension points (`Transport`, `CapabilityProvider`, `MembershipValidator`, `Authorizer`, `Embedder`, `ConflictStrategy`, `SchemaRegistry`, `PeerSampler`). |
| `agentspaces-test-support` | Deterministic test infrastructure. `TestClock` replaces wall time, `SimNetwork` is an in-JVM transport with partitions and attestation control, `TestCa` mints certificate authorities, and `Fixtures` supplies the entry types the examples use. Every cluster test in the suite runs on it. |
| `agentspaces-identity` | Layer 0. Keystore persistence, PeerID derivation, advertisement signing and verification, and the channel-authentication pieces: identity-endorsed channel certificates, CA-rooted `ChannelTrust` with CRL and OCSP checking, and serving credentials. |
| `agentspaces-peering` | Layer 1. `PeerNode` and `GroupRuntime`: the signed wire envelope and codec, leased membership with SWIM-style probing, the gossip bus, relay forwarding, founding documents and self-certifying group joins, revocation registry, strikes and quarantine, the TCP and TLS transports, and the multicast bootstrap beacon. |
| `agentspaces-transport-quic` | The QUIC (RFC 9000) transport binding on Netty's QUIC codec: one bidirectional stream per connection carrying the same length-prefixed frames, with an ephemeral certificate by default and channel attestation when built with the peer identity. Its native tests run on macOS and Linux, where OS-activated profiles pick the native classifier. |
| `agentspaces-discovery` | Layer 2. The per-group advertisement cache (`AdCache`) that verifies signatures, bounds occupancy, and evicts by TTL, and `DiscoveryService` for publishing, local `find`, rendezvous and gossip `remoteFind`, and revocation routing. Spaces and agents publish through it. |
| `agentspaces-space` | Layer 3. `LocalSpace` and `ReplicatedSpace`, the OR-Set and lease CRDT with authenticated state transitions, the signed take-claim lattice behind LEASE_RACE and AUCTION, block exchange for content-addressed payloads, group-key encryption, tag sharding, tombstone GC, and the space lifecycle (advertisement, admission, read-only). |
| `agentspaces-capabilities` | Layer 4. The capability runtime and pipes, plus the six shipped capabilities: `aggregate`, `vote`, `ordered-log` with the ORDERED take coordinator, `gossip-learn`, `semantic-discovery` with the hashing embedder, and `key-wrap`. Each is a reference implementation of the `CapabilityProvider` pattern. |
| `agentspaces-auth-oidc` | The `Authorizer` for the `MTLS_OIDC` and `ZERO_TRUST` profiles: Nimbus-validated JWTs bound to a PeerID and carrying scopes, so privileged operations answer to the organization's identity provider. |
| `agentspaces-agent` | The framework-neutral programming model. `AgentBinder` turns annotated plain objects into leased worker loops, choreography, ballots, decision reactions, and exactly-once workers (`@SpaceTake`, `@SpaceNotify`, `@BidFunction`, `@Ballot`, `@OnDecision`, `@OrderedTake`, with `@SpaceRef` and `@CapabilityRef` injection), gives each agent its signing identity (peer-signed, or a certified key of its own), publishes and refreshes AgentCards, and the `AgentSpaces` facade routes beans into every satisfied group, registers capabilities and coordinators for the annotations, resolves typed capability clients, and hosts `RemoteActions`, which turns foreign AgentCards into invokable actions. |
| `agentspaces-a2a` | The A2A gateway. It serves AgentCards as A2A agent cards, and binds the A2A task interface to a space: `message/send` writes a task entry, `tasks/get` reads state from the space, `message/stream` and `tasks/resubscribe` stream over Server-Sent Events, and push notifications reach allowlisted webhooks. |
| `agentspaces-connect-core` | The connector SDK for data-provider peers. `AssetProvider` describes and answers queries for assets advertised as AssetCards through the `data-query` protocol with pull-once caching, and `MaterializingAssetProvider` pushes source changes into a space as leased entries. `DataSpaces` wires the block exchange so bulk results travel content-addressed. |
| `agentspaces-console` | The fleet console as a served interface: a read-only peer presenting membership, discovery, and spaces over a HAL+JSON API, a Server-Sent Events activity stream, a single-file web UI, and the `ConsolePanel` SPI. Command and control (`FleetCommander`, `DirectiveGate`) dispatches token-gated directives with a signed audit log. See `docs/CONSOLE.md`. |
| `agentspaces-spring-boot-autoconfigure` | Spring Boot wiring from `agentspaces.*` properties: identity, node, transports by security profile, groups (founded or joined by GroupID), spaces with admission and block exchange, the shipped capabilities behind toggles, annotated-bean enrollment, and the refresh lifecycle. It compiles against the stub interface so it builds without Spring on the path. |
| `agentspaces-spring-boot-starter` | The one dependency a Spring Boot application adds. It brings the autoconfigure module, the capabilities, and the `@SpaceAgent` stereotype; see its README for the property reference. |
| `agentspaces-spring-stubs` | Provided-scope copies of the Spring API names the autoconfigure module compiles against, for builds that cannot reach Maven Central. DoD/enclave builds only, not included otherwise. |
| `embabel-agentspaces` | The Embabel extension. `EmbabelBinder` publishes `@Agent` metadata as AgentCards into every group, `EmbabelRemoteActions` generates a typed `@Agent` whose actions invoke the fleet's advertised capabilities, and a deployer redeploys it onto the platform as cards arrive and lapse. It reads Embabel reflectively and needs no Embabel artifact to build. |
| `agentspaces-partybus/` (separate repo) | The largest demo application, a standalone project outside this repository that consumes the published libraries: the Embabel travel planner rebuilt as a fleet of agents that uses every coordination type at once — panelists and travelers vote with `@Ballot`, leads publish with `@OnDecision`, clerks confirm bookings with `@OrderedTake` — with a vacation simulator that drives it through a week that goes wrong on purpose. Its guide is `site/partybus-guide.html`. |
| `agentspaces-perf/` (separate repo) | Standalone, on the published libraries through the `agentspaces-dependencies` BOM: JMH microbenchmarks (codec, crypto, HLC, templates, CRDT merges, frames, space verbs) and multi-peer TCP stress harnesses for throughput, propagation, and churn recovery. Built with `mvn -f ../agentspaces-perf/pom.xml package` after a `mvn install` here; see its README. |
| `examples/` | Graduated examples 01 through 14: hello space, research fleet, discovery cards, auction, quorum (with a council of `@Ballot` seats on one peer), WAN rendezvous, A2A fleet, intake fleet, fleet console, data fleet, the one-file quickstart, the exactly-once desk (`@OrderedTake`), signed agents (subordinate keys and attestation), and gossip learning. Each has a test that drives it end to end. |
| `agentspaces-example-apps/` (separate repo) | Larger example applications, standalone projects outside this repository, every agent an annotated plain object bound through the facade: a compliance intake fleet (evidence counted per authenticated member), a code-migration fleet allocating work by AUCTION, a release-audit fleet of discipline specialists voting with `@Ballot` and adjudicating with `@OnDecision`, and a veterinary clinic that coordinates purely by `@SpaceNotify` choreography over one space. |
| `agentspaces-python/`, `agentspaces-typescript/` (separate repo) | Wire-compatible non-JVM peers. They join Java fleets by seed or by GroupID with the founding document verified, write signed entries, take under the claim lattice, and read through anti-entropy, and both reproduce every shared golden vector byte for byte from their vendored copies of `agentspaces-spec/golden.json`. |
| `agentspaces-springai/` (separate repo) | AgentSpaces for Spring AI applications, standalone on the published libraries (Spring Boot 4.1, Spring AI 2.0): the fleet's AgentCards as Spring AI tools, discovery and data tools, crash-safe LLM workers (a take lease renewed per model round-trip, chat memory in a space), model access as a fleet service (`FleetChatModel` and `ModelServer`, with streaming), a Spring AI embedder for semantic discovery, fleet-wide token usage, and MCP export. No Embabel dependency; see its README and `agentspaces-springai.md`. |
| `agentspaces-clj/` (separate repo) | Idiomatic Clojure bindings outside the reactor, consuming the published artifacts: maps in and out over record entry types, a keyword `:where` DSL, and `fleet/start` from a config map mirroring the Spring starter. |
| `integration-tests/` | The real-Spring release gate: a `SpringApplication` boots the starter and works a fleet over TCP. Built under `-Pspring-it`, which CI activates. |

### Modules by layer

The specification's architecture stacks five layers, each depending only on the
layers below it, with the programming model, gateways, and tooling around them.

| Architecture position | Modules |
|---|---|
| Foundations (shared by every layer) | `agentspaces-common`, `agentspaces-api`, `agentspaces-dependencies`, `agentspaces-test-support` |
| Layer 0, Identity | `agentspaces-identity`, `agentspaces-auth-oidc` |
| Layer 1, Peering (groups, membership, gossip, transports) | `agentspaces-peering`, `agentspaces-transport-quic` |
| Layer 2, Advertisements and discovery | `agentspaces-discovery` |
| Layer 3, The AgentSpace | `agentspaces-space` |
| Layer 4, Capability services | `agentspaces-capabilities`, and any capability a third party publishes |
| Programming model and hosting | `agentspaces-agent`, `agentspaces-spring-boot-autoconfigure`, `agentspaces-spring-boot-starter`, `agentspaces-spring-stubs`, `embabel-agentspaces`, `agentspaces-clj/` |
| Gateways and served interfaces | `agentspaces-a2a`, `agentspaces-console`, `agentspaces-connect-core` |
| Non-JVM implementations of Layers 0 through 3 | `agentspaces-python/`, `agentspaces-typescript/` |
| Applications and verification | `examples/`, `integration-tests/` in this repository; `agentspaces-example-apps/`, `agentspaces-partybus/`, `agentspaces-perf/` as standalone projects beside it |
| Framework integrations | `agentspaces-springai/` (Spring AI, model-call granularity), `embabel-agentspaces` for Embabel support |

A conforming minimal implementation is Layers 0 through 3 on one LAN with the
default strategy; everything above Layer 3 and everything to the side is additive.

## Extending AgentSpaces

The core stays small. The extension points are interfaces in `agentspaces-api` and a few well-marked interfaces in the layer modules. Three kinds of extension come up most often.

### Adding a transport

A transport is anything that can carry length-prefixed byte frames between peers. The SPI is two interfaces in `agentspaces-api`:

```java
public interface Transport {
    String scheme();                                   // "tcp", "tls", "quic", "ws", "mem"
    TransportConnection dial(String address) throws IOException;
    AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException;
}

public interface TransportConnection extends AutoCloseable {
    String remoteAddress();
    void send(byte[] frame) throws IOException;
    void onReceive(Consumer<byte[]> receiver);
    default Optional<PeerId> attestedPeer() { return Optional.empty(); }
    void close();
}
```

Everything above the transport is unchanged by the choice, and the abstraction must stay that way: envelopes stay signed CBOR, membership and gossip run the same, and the space never knows. The QUIC module is the example to follow. `QuicTransport` maps one bidirectional QUIC stream to one connection, frames the same bytes the TCP transport frames, and returns `"quic"` from `scheme()`.

Two design points carry over to any new binding:

1. **Peer identity does not live in the transport.** A transport may encrypt, but the peer's identity is proven by the signed envelope layer over every transport. `attestedPeer()` is optional: return the PeerID only when the handshake itself authenticated the remote end, as the TLS and QUIC bindings do with identity-endorsed channel certificates (SPEC §5.6). Then, and only then, the node may elide per-frame signatures toward that peer. Returning empty is always safe and keeps every frame fully signed.
2. **Dialers pick by scheme and priority.** A peer advertises its endpoints as `(scheme, address, priority)` in its PeerAdvertisement. Register a listener with `node.listen(transport, bindAddress, priority)` and the node advertises it; a dial-only peer registers with `node.transport(transport)`. Dialers try a member's endpoints in ascending priority and skip schemes they lack, so a fleet can run mixed transports and migrate between them by changing priorities.

Practical notes:

- Frames are capped at 8 MiB. Refuse larger frames, and treat a corrupt length prefix as a dead connection, since the stream cannot resynchronize.
- Test a new binding with the pattern in `TcpTransportTest` and `TlsFabricTest`: two real `PeerNode`s over the transport, converging membership, then a frame round trip.
- Under the Spring starter the security profile selects TCP or TLS. A custom transport is registered programmatically on the node today.
- WebSocket is the next binding the specification names.

### Customizing security providers

Security decisions sit behind interfaces so an organization can align its own identity and policy systems with the fabric without forking it. Each seam answers one question.

**Who may join a group?** `MembershipValidator` in `agentspaces-api`.

- Decides admission for `POLICY` groups: `admit(candidate, groupAdvertisement, credentials)`.
- Plug in a DID resolver, an LDAP lookup, or a verifiable-credential check. The node calls it at admission and refuses everyone it rejects.
- `OPEN` and `INVITE` (founder-signed credentials bound to the PeerID) need no code.

**Who may perform a privileged operation?** `Authorizer` in `agentspaces-api`.

- Answers `permits(peer, operation, scope)` and, per agent, `permits(agent, operation, scope)`, with a `granularity(operation, scope)` of `PEER` or `AGENT`.
- Guards `RAFT_VOTER`, `DIRECTIVE_ISSUER`, `CONNECTOR_SERVE`, `KEY_HOLDER`, `SPACE_WRITE`, `SPACE_TAKE`, `VOTE`, `MODEL_SERVE`, and `KEY_ROTATE`. `KEY_ROTATE`, unlike the others, permits nobody without an explicit grant.
- `MembershipAuthorizer` roots decisions in group membership with optional grants that name PeerIds or `peer/agent` AgentIds. `OidcAuthorizer` in `agentspaces-auth-oidc` roots them in JWT scopes from your identity provider.
- Consumers that count identities (the QUORUM tally) count at the granularity the authorizer answers at, so counting and authorization never disagree.
- Implement the interface to consult your own policy engine. Keep it fast (it sits on dispatch paths; cache behind it) and select it with the `SecurityProfile`.

**How is the channel authenticated?** `ChannelTrust` and the `MTLS` profiles.

- The `MTLS` profiles present identity-endorsed channel certificates.
- For an enterprise CA, `ChannelTrust.of(anchors)` or `ChannelTrust.withCrls(anchors, crls)` (optionally `strictOnline()` for OCSP) attests only certificates that chain to your anchors and are not revoked.
- `TlsTcpTransport.withRefreshableTrust(...)` lets you install a new CRL without a restart.
- `agentspaces.transport.tls.require-attestation` (or `PeerNode.Builder.requireAttestation(true)` when you assemble the node yourself) makes attestation a condition of admission and evicts a member whose next handshake attests nothing, so your CA's revocation is the fleet's eject. The node registers TLS alone in that mode, since a plaintext channel attests nobody.

**Who decides a revocation is authoritative?** `RevocationRegistry.RevocationValidator`.

- Judges a gossiped `RevocationAdvertisement`. The default is founder-rooted.
- Set `PeerNode.Builder.revocationValidator(...)` to accept revocations from your identity provider's signing key instead.
- In enterprise-CA mode, `agentspaces.transport.tls.revocation-validator: founder-or-ca` (default `founder`; it needs a `trust-store`) also accepts a revocation from any member whose evidence validates to your CA. The evidence is the revoked peer's chain and the CRL that lists its leaf for an authorizing reason.
- The node roots a revocation itself when its CRLs, re-read every `crl-refresh` (5m), revoke a connected peer.

**Who may read a space?** Content keys and space admission.

- Group content keys encrypt payloads. The `key-wrap` capability seals the key per member under a policy you supply (`GroupKeyDistributor.serve(key, Predicate<PeerId>)`, or the membership default).
- Writers and takers are admitted by group, by an AgentId allowlist, by leased `SpaceCredential` entries the space's issuer writes and revokes, or by the profile's `Authorizer` (`ReplicatedSpace.Builder.admission(...)`).
- Admission is judged for the acting agent, so two agents of one peer sharing a replica through views can be admitted differently.

**How is meaning matched?** `Embedder` in `agentspaces-api`.

- Backs semantic discovery. The shipped `HashingEmbedder` needs no model.
- Swap in your own embedding service and the protocol does not change.

All of these are ordinary constructor or builder arguments on `PeerNode`, `ReplicatedSpace`, and the capability classes. The Spring starter exposes the common choices as properties under `agentspaces.security.*`, `agentspaces.transport.tls.*`, and `agentspaces.groups[].spaces[].admission`.

### Building a new capability (Layer 4)

A capability is any protocol beyond the core that a peer offers its group. The pattern is uniform and the six shipped capabilities are its reference implementations. Four steps:

1. **Mint a type URI and write the mini-spec.** Capability types look like `aspace:cap/vote`; third parties choose their own namespace. The mini-spec is the interaction itself: what a request looks like, what an answer looks like, and what the advertisement's `parameters` promise.
2. **Implement `CapabilityProvider`.**

   ```java
   public interface CapabilityProvider {
       String capabilityType();                          // "acme:cap/negotiate"
       CapabilityAdvertisement describe(GroupId group);  // type, version, binding, parameters, cost hints, TTL
       default void start() {}
       default void stop() {}
       default void tick() {}                            // one protocol step, driven by the peer's clock
       default boolean requiresTick() { return false; }  // true when the protocol has rounds to run
       default void driverCadence(Duration period) {}    // told the real cadence, so advertised leases are truthful
   }
   ```

   - `describe` returns the advertisement the runtime signs and publishes. The runtime refreshes it on its schedule, so a provider that stops refreshing simply ages out of every cache. Everything is leased.
   - A provider with rounds to run declares `requiresTick()` and does one step in `tick()`. The `CapabilityRuntime` registers itself on the peer's one clock and drives every provider from it, so a capability never brings a scheduler of its own.
3. **Choose a binding.**
   - **Space-mediated** is the canonical style: requests and responses flow as leased entries in an agreed space, which gives retries, observability, and choreography with other capabilities at no extra cost. `vote`'s QUORUM mode and the connector SDK's `data-query` work this way.
   - **Direct pipe** suits high-rate protocols: `CapabilityPipes.onCapability(type, handler)` receives frames addressed to your type and `send(peer, type, payload)` answers them over the `PIPE_DATA` kind. `aggregate`, `gossip-learn`, `semantic-discovery`, and `key-wrap` work this way.
   - Both bindings see only authenticated, admitted peers. The node has already verified the envelope and applied membership, rate limits, and revocation before your handler runs.
4. **Register and consume.**
   - Programmatically: `capabilityRuntime.register(provider)` on the group's `CapabilityRuntime`. Consumers find providers with `providersOf(type)` through the ad-cache.
   - Under the `AgentSpaces` facade or the Spring starter: annotate the provider bean with `@ProvidesCapability("acme:cap/negotiate")` and it is registered and advertised in its groups.
   - For a typed client: implement `CapabilityClientFactory<C>` (`clientType()`, `create(groupContext)`) and register it through `ServiceLoader`. `spaces.group("fleet").capability(NegotiateClient.class)` then resolves it, as `VoteClient`, `AggregateClient`, and `SemanticClient` do today, and an agent can receive it by field with `@CapabilityRef`.
   - A capability with a behaviour agents exhibit (cast, react, take) earns an annotation of the `@SpaceNotify` shape. `LAYER4-ANNOTATIONS.md` is the review that produced `@Ballot`, `@OnDecision`, and `@OrderedTake`, and the bar a new one must clear.

Test a capability the way the shipped ones are tested: a `SimNetwork` cluster with a `TestClock`, providers registered on two or more peers, and assertions on convergence after deterministic ticks (`CapabilityClusterTest` is the template). Consider the security posture as you design: an unauthenticated average can be poisoned by any participant (the specification accepts this for `aggregate` and says so), whereas signed entries in a space are attributable. Say which one your mini-spec offers.

## License

Copyright 2026 Bad Monkey, Inc.

AgentSpaces is an open source project from Bad Monkey, Inc, licensed under the Apache License 2.0; see `LICENSE`.

For questions, contact `oss@badmonkey.ai`
