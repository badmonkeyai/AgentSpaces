# agentspaces-discovery

Layer 2: how a fleet finds things. A per-group advertisement cache that verifies every
signature and evicts by TTL, and the discovery service that publishes into it, answers
local-first `find`, and escalates to hop-budgeted remote queries.

**Architecture position.** Layer 2, Advertisements and discovery. SPEC §6; TECH-SPEC §6.

## Key files

| File | What it is |
| --- | --- |
| `AdCache` | The cache: signature verification before acceptance, bounded occupancy (four times larger on rendezvous peers), TTL eviction on the injected clock, unknown enum values refused rather than defaulted (SPEC §6.1b), `StoredAd` as the `ads`-stream wire form. |
| `DiscoveryService` | `publish`, `find(type, predicate)` (a local in-memory scan, call it freely), `remoteFind` (a query on the wire with a wait), rendezvous escalation in parallel, revocation routing to the registry. Owns its stream registrations and is `AutoCloseable`. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`, `agentspaces-peering`.

## Notes

- Spaces and agents publish through it automatically (a space on creation, an agent when
  bound); most fleets never call `find` — routing is the entry's type, not a lookup.
- Golden vectors `agent_card_cbor`, `signed_agent_card_cbor`, `space_ad_cbor`, and
  `agent_card_with_key_cbor` pin the advertisement bytes the cache stores.
