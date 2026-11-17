# embabel-agentspaces

The Embabel extension: `@Agent` metadata becomes AgentCards in every joined group, and the
fleet's cards become a typed Embabel `@Agent` whose actions invoke the advertised capabilities,
redeployed onto the platform as cards arrive and lapse. Reads Embabel reflectively, so it builds
with no Embabel artifact and activates only when Embabel is on the application classpath.

**Architecture position.** Programming model and hosting. SPEC §10.4, §10.6.

## Key files

| File | What it is |
| --- | --- |
| `EmbabelDeclarationReader`, `EmbabelIntrospector` | The `AgentDeclarationReader` the core binder consults (registered through `ServiceLoader` and, configured, by the autoconfiguration): one card per `@Agent` bean, named by `@Agent(name)`, with implied takes for every map-of-1 `@Action` (`aspace.action=<agent>.<method>`) and `local` for what cannot be served. `EmbabelBinder` is deprecated and binds through the facade. |
| `EmbabelRemoteActions`, `RemoteActionInterceptor`, `RemoteActionTimeoutException` | Generates (Byte Buddy) a typed `@Agent` over the bridged groups, one action per capability (not per peer) with stable names, carrying the card's description, a normalised cost and the foreign `pre`; fires once, re-awaits within one process, withdraws a timed-out input. |
| `EmbabelRemoteActionsDeployer`, `EmbabelRemoteActionsDeployerLifecycle`, `EmbabelAgentSpacesAutoConfiguration` | Auto-deploy onto the platform (`agentspaces.embabel.auto-deploy`, on by default): redeploy when the capability set changes (debounced, rate-limited, with an absence grace), a vacant generation when every action lapses, a refusal when the platform already holds the bridge's name, a read-back after each deploy. |

## Key dependencies

`agentspaces-spring-boot-autoconfigure`, `agentspaces-agent`, `agentspaces-discovery`,
`agentspaces-identity`, `agentspaces-space`, `byte-buddy`, `agentspaces-spring-stubs` (provided).

## Notes

- The Party Bus (`agentspaces-partybus/` at the workspace root) is the largest consumer: its
  planner finds the fleet's scouts as planner actions through this bridge.
- Embabel's default platform replaces an agent deployed under the same name, so each generation
  replaces the last; when every fleet action lapses the deployer deploys a vacant generation, and it
  reads the platform back after each deploy (`generationsRetained()` counts a platform that kept more
  than one). `AgentPlatform` itself has no undeploy.
- The second generation of the bridge (2026-11-12) is designed in
  `reviews/embabel-agentspaces-review-2026-11-12.md` §7, one subsection per change, with the properties
  under `agentspaces.embabel` that keep the earlier behaviour (`card-name`, `serve-actions`,
  `advertise-unserved`, `remote-action-retry`, `on-timeout`, `groups`, `take-mode`).
- Large planner inputs and outputs (SPEC v0.1.23 §7.1): with
  `agentspaces.content.enabled=true` on the host, a `@SpaceTake` input or a result a remote action
  writes above `agentspaces.content.escalate-above` (4 MiB) travels as an artifact of the content
  swarm and arrives whole on the planner's side; nothing in this module changes for it. A planner
  that wants the swarm directly reaches it as `group.provider(Content.TYPE)`.
