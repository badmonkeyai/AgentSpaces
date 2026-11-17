# agentspaces-space

Layer 3: the AgentSpace itself. `LocalSpace` for one JVM and `ReplicatedSpace` as a
delta-CRDT over gossip, sharing the `Space` API, leases on everything, and the one failure
idiom: state you stop renewing goes away.

**Architecture position.** Layer 3, The AgentSpace. SPEC §7, §11a.4; TECH-SPEC §7.

## Key files

| Package / file | What it is |
| --- | --- |
| `local.LocalSpace` | The complete single-JVM implementation: same verbs, leases, conflict behaviour, and events; what tests and in-process fan-out use. `SimpleSchemaRegistry` maps entry classes to schema names (`<class>#v1`). |
| `replicated.ReplicatedSpace` | The replica: `write`/`take`/`complete`/`notify` over the CRDT, one gossip stream and one reconciler per space name per node, admission per acting agent, per-agent **views** (`as(AgentIdentity)`), `readAllIssued` with attestation, `claimProof`/`signedState` accessors, tombstone GC, tag sharding, group-key encryption over a `GroupKeyRing` of content-key epochs (`Builder.keyRing` shares one ring across a group's spaces; v0.1.13), revocation refusals with the bounded `refusedAck` digest acknowledgements, content-addressed payloads (a subscriber sees a large entry's WRITTEN when its block lands), founder lease and read-only. |
| `replicated.TakeClaim`, `SpaceWire` | The take-claim lattice (epoch, bid, HLC stamp, holder, expiry; `merge` always returns an input) and the wire records: `EntryStateDto` (with the appended `agentCertificate`, and in v0.1.13 `stateCertificate`, `signer`, `signedAt`), `SignedClaim` (with the appended `holderCertificate`), `Delta`, `SyncDelta`. |
| `replicated.SpaceAdmission`, `SpaceCredential`, `CredentialIndex` | The four admission rules — `group`, `allowlist`, `credentials(issuer, index)` (leased `SpaceCredential#v1` entries written into the space), `authorizer(...)` — judged per transition for the acting agent. |
| `crdt.SpaceStateCrdt`, `EntryState`, `LwwRegister`, `Dot` | OR-Set entry state with dots bound to the issuer's peer, LWW lease register, monotone completion, the GC horizon rule. |

## Algorithms worth knowing

- **Two signatures per entry.** The record signature covers the immutable identity (`SignView`,
  which appends `keyEpoch` for a record sealed under an epoch after 0); the state signature
  covers the mutable CRDT fields (`StateSignView`, SPEC §11a.4) and is checked against the
  party the transition authorizes — the issuer, or the take-claim holder for a completion.
  Both apply the two-key rule when a certificate rides along (peer key verifies the
  certificate, agent key verifies the signature).
- **Agent-signed transitions (v0.1.13, rule A6).** On an agent-attested entry (the record
  carries an `agentCertificate`, or the claim a `holderCertificate`) the renewal,
  cancellation, or completion must be signed by that agent: the state carries `signer`,
  `signedAt`, and a `stateCertificate` covering `signedAt`, and a peer-signed state for such
  an entry is refused. Entries without certificates keep peer-signed states.
- **Content-key epochs (v0.1.13).** A sealed record names its epoch in `keyEpoch` (absent for
  epoch 0) and binds it into the associated data; writers seal under the ring's current
  epoch, readers open with the epoch the record names, and a renewal keeps the record's epoch.
- **Revocation refusals (v0.1.13).** For entries the replica does not already hold, a state
  or claim whose actor is revoked is refused under the freeze rule (SPEC §6.1), and the
  refused line is acknowledged in the digest as `a:<line>` (the bounded `refusedAck` map) so
  partners stop re-offering it. Entries already held stay.
- **Claims first, then states.** On any delta the claim merges before the state, because a
  completion authenticates against the stored claim; the ordered log installs claims with the
  holder's own proof for the same reason (QA4 A4-5).
- **Expiry index (2026-11-13).** Each entry is filed at the earliest time the sweep has
  something to do for it: its lease lapse, its claim's lapse, or its collection horizon. The
  digest, the delta and the group tick sweep only when the first of these is due, and visit
  only the due entries, so an unchanged space answers a probe in O(1) instead of a pass over
  every entry it has held. The digest's SHA-256 is cached beside the digest.
  `ExpiryIndexPropertyTest` holds the indexed sweep to the old full pass on random histories,
  at every tick, digest and delta sweep as well as at its own check points, and
  `DigestBytesPinTest` pins the digest bytes against the released 0.6.0 jars over a fixture
  with every kind of digest line (live entries, tombstones, claims, acknowledgements).
- **The per-entry horizon (SPEC §7.3, v0.1.35).** While this node and every member of the
  space's view advertise `wire.horizon=1` (`PeerNode.Builder.horizon`, on by default), a
  withdrawn entry is collected `max(5 min, MAX_DRIFT)`, ten minutes, after its own lease
  expiry instead of `max(2 x maxLease, MAX_DRIFT)`; completed and lapsed entries keep their
  horizons. Under either rule a withdrawal is never collected before `MAX_DRIFT` past its
  lease (the drift floor), so a replica whose clock trails cannot show it live again. The space reads the rule
  from its view: an unhinted member in view turns it off at once, and it comes back only after
  three fully hinted rounds and one maximum lease plus five minutes more
  (`horizonMode()`, `horizonEngageRounds(int)`). The argument is in
  `docs/proofs/per-entry-horizon.md`.
- **Waiting takes sleep (2026-11-13).** A `take` or `read` with a timeout waits on the
  space's change signal (a write, merge, renewal, cancel, claim, a sweep that lapses a claim,
  a block landing), bounded by its deadline and a one-second slice, instead of polling every
  20 ms.
- **Writer-local spaces (2026-11-13).** `Builder.local()` is for a space only its own
  writer reads. It registers nothing on the gossip bus, so it has no digest and no
  anti-entropy, and its writes publish no rumor. It publishes no space advertisement either,
  even with `advertise(...)` set, since an advertisement says the space replicates, and a
  `FRESH` read on it starts no anti-entropy round. Leases, events and the sweep work as on a
  replicated space, with the sweep run from the tick. Other members never see its entries.
- **Refuse at the door.** A foreign `spaceId`, an issuer key that does not hash to the issuing
  peer, a dot not minted by the issuer, an inflated dot set, a claim past the epoch-jump or
  hold bounds, a certificate that does not cover the signing time (the record's issue stamp,
  or the state's `signedAt`, within the HLC drift bound; a certificate that has since lapsed
  still covers what was signed inside its window): each is dropped before it costs anything.

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`, `agentspaces-peering`.

## Notes

- The conformance suites live here: `GoldenVectorsConformanceTest`, `GoldenVectorsV0110ConformanceTest`,
  `AdversarialVectorsTest`, `SignedAgentVectorsClusterTest` read `../tools/golden/golden.json`
  (then `../../agentspaces-spec/golden.json`); run with `-Dgolden.required=true`.
- `ReplicatedSpace` is one replica per space name per node by design; opening a name twice
  refuses (QA4 A4-9). Several agents on one node share it through views.
  A writer-local space skips the bus registration, so a second handle for its name is not refused.
