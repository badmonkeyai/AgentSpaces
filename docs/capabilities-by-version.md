# Version 0.4.0 Capabilities

Written for: Java developers who build with AgentSpaces.

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
- DTLS 1.2 (RFC 6347) with Connection ID (RFC 9146), on BouncyCastle, pure Java (`agentspaces-transport-dtls`).
- Roadmap: WebSocket support

The transport layer is pluggable, so more options can be built over time, including different strategies for initial bootstrap of the P2P network. The multicast option has linits in practice. In the past, we had well known sites for peers to bootstrap from, DHCP options that reference bootstrap locations, etc. We didn't want to prescribe one of these, but leave it open for adopters to build to their network architecture.

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
