# agentspaces-api

The public contract. Applications program against this module and nothing else in the
suite is a hard dependency for them; the layer modules implement what is declared here and
stay swappable.

**Architecture position.** Foundations (the API every layer implements). SPEC §6 (advertisements),
§7 (the space), §8 (capabilities), §11 (authorization); TECH-SPEC §3, §7.

## Key files

| Package | What it is |
| --- | --- |
| `api.space` | `Space` — the leased tuple space: `write`, `read`, `readAll`, `readAllIssued` (with each entry's authenticated issuer and its `Attestation`, `PEER_ASSERTED` or `AGENT_ATTESTED`), `take`, `complete`, `notify`, `writer()`. `Template` and `Matchers` (the matching DSL), `Lease`, `EntryHandle`, `TakenEntry`, `Subscription`, `SpaceEvent` (WRITTEN, TAKEN, COMPLETED, EXPIRED, REAPPEARED), `ConflictStrategyType`. |
| `api.entry` | `EntryRecord` (the signed wrapper around every entry: id, space, type, payload or content reference, issuer, HLC stamp, lease, tags, signature), `EntryId`, `LeaseInfo`, `LeaseKind`. |
| `api.ad` | The advertisement family and its signed wrapper: `PeerAdvertisement`, `GroupAdvertisement`, `SpaceAdvertisement`, `CapabilityAdvertisement`, `AgentCard` (with space bindings and the appended `agentPublicKey` of an attested agent), `AssetCard`, `RevocationAdvertisement`, `SignedAdvertisement`. |
| `api.security` | `SecurityProfile` (`DEV_LOCAL`, `MTLS`, `MTLS_OIDC`, `ZERO_TRUST`), `AgentCertificate` (the peer's certification of an agent's own key, SPEC §4.2), `BearerAuthenticator`. |
| `api.spi` | The extension points: `Transport`/`TransportConnection`, `CapabilityProvider` (with `tick`/`requiresTick`/`driverCadence` so the peer's one clock drives it), `Authorizer` (`permits(peer|agent, operation, scope)` and its `Granularity`), `AgentIdentity` (who signs for an agent), `MembershipValidator`, `ConflictStrategy`, `Embedder`, `SchemaRegistry`, `PeerSampler`. |
| `api.error` | `LeaseExpiredException` (an expected coordination outcome, not a failure), `SpaceClosedException`, `AgentSpacesException`. |

## Key dependencies

`agentspaces-common` and `jackson-annotations` only (records carry `@JsonInclude`/`@JsonCreator`
where the wire needs them).

## Notes

- Appended record fields are the compatibility convention (SPEC §6.1): add at the end, omit when
  absent (`@JsonInclude(NON_NULL)`), and older decoders ignore them. `AgentCard.agentPublicKey`,
  `Space.Issued.attestation`, and `Authorizer`'s two defaults were all added this way.
- `Authorizer` is a functional interface: a lambda still implements it, agents inherit their
  peer's answer, and `granularity` is `PEER` unless overridden.
