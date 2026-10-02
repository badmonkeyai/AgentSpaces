# agentspaces-spring-boot-autoconfigure

Spring Boot wiring from `agentspaces.*` properties: identity, node, transports by security
profile, groups (founded or joined by GroupID), spaces with admission and block exchange, the
shipped capabilities behind toggles, annotated-bean enrollment, and the lifecycle that ticks the
peer and refreshes cards. Compiles against the stub interface so it builds without Spring on the
path; applications supply real Spring Boot.

**Architecture position.** Programming model and hosting. SPEC §10.1; the starter's README is
the property reference.

## Key files

| File | What it is |
| --- | --- |
| `AgentSpacesAutoConfiguration` | The beans: `PeerIdentity`, `PeerNode`, `Authorizers`, `AgentSpaces` (with the identity factory from `agentspaces.identity.agent-keys`), the lifecycle, the post-processor, capabilities from `agentspaces.capabilities.*`, `DirectiveGates`. |
| `AgentSpacesProperties` | Every property: bind, tick, groups and spaces, transport and TLS, security profile and OIDC, grants (PeerIds or `peer/agent` AgentIds), identity, console, Embabel, capabilities, multicast. |
| `Authorizers` | Selects the answerer by profile: a `MembershipAuthorizer` per group with the configured grants, or one `OidcAuthorizer`; attaches the token channel; a multi-group union that answers per agent too. |
| `AgentSpacesA2aAutoConfiguration`, `AgentSpacesA2aLifecycle` | The A2A gateway from `agentspaces.a2a.*` (v0.1.13), off by default; a routable bind without a token or required scope fails at startup. |
| v0.1.13 security wiring | One content-key ring per group shared by its spaces, served and followed by key-wrap and persisted under `agentspaces.keystore` (`groups[].content-key-rotation.*`); renewing subordinate agents with `identity.agent-keystore` and `agent-certificate-ttl`; the CA trust built before the node (`transport.tls.crl-refresh`, `revocation-validator: founder-or-ca`); every group's authorizer wrapped in a `GroupScopedAuthorizer`. |
| `AgentSpacesBeanPostProcessor`, `SpaceAgent` | Enrolls beans after initialization; `@SpaceAgent` is `@Component` + `@AgentSpec`. |
| `AgentSpacesLifecycle`, `AgentSpacesConsoleAutoConfiguration`, `AgentSpacesConsoleLifecycle`, `ConsoleViewCustomizer`, `DirectiveGates` | Start/stop ordering, the console as an optional bean, and directive gates per registered space. |

## Key dependencies

Every runtime module (`agentspaces-agent`, `-console`, `-discovery`, `-peering`, `-capabilities`,
`-space`, `-auth-oidc`, `-identity`) and `agentspaces-spring-stubs` in **provided** scope.

## Notes

- `AgentSpacesAutoConfigurationTest` wires the real configuration by hand over TCP; the
  real-Spring gate is `integration-tests/spring-boot-it` under `-Pspring-it`.
- `agentspaces.security.profile` selects transports, channel mode, and who answers authorization
  from one property; an OIDC profile without issuer, audience, and JWKS URL fails at startup.
