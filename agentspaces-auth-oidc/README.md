# agentspaces-auth-oidc

The `Authorizer` for the `MTLS_OIDC` and `ZERO_TRUST` profiles: privileged operations answer to
the organization's identity provider through JWT scopes. Optional — fleets on `DEV_LOCAL`/`MTLS`
use `MembershipAuthorizer` in `agentspaces-peering` and never load this module.

**Architecture position.** Layer 0, Identity (authorization). SPEC §11; TECH-SPEC §4.

## Key files

| File | What it is |
| --- | --- |
| `OidcAuthorizer` | Validates a token against issuer, audience, and JWKS (Nimbus), requires `agentspaces_peer` to name the peer being judged, reads permitted operations from the `scope` claim (`aspace:<operation>` for every scope, `aspace:<operation>:<scope>` for one), caches the grant until `exp` (Caffeine), and receives tokens through `authorize(peer, token)` from the peer advertisement's `aspace:oidc` hint. Reports `PEER` granularity: a token binds one PeerId, so every agent on the peer inherits its scopes. |
| `BearerJwtValidator`, `JwtProcessors` | The console's and A2A gateway's bearer-principal validation, sharing the JWKS machinery. |

## Key dependencies

`nimbus-jose-jwt`, `caffeine`, `agentspaces-api`, `agentspaces-common`.

## Notes

- `OidcAuthorizerTest` documents the per-agent-token follow-on (an `agentspaces_agent` claim
  that would grant at `AGENT` granularity) as a `@Disabled` contract so it is not forgotten.
- Nimbus checks `exp` against the real clock; tests mint tokens relative to real now, and the
  injected clock governs only the grant-cache expiry.
