# embabel-agentspaces

The Embabel extension: `@Agent` metadata becomes AgentCards in every joined group, and the
fleet's cards become a typed Embabel `@Agent` whose actions invoke the advertised capabilities,
redeployed onto the platform as cards arrive and lapse. Reads Embabel reflectively, so it builds
with no Embabel artifact and activates only when Embabel is on the application classpath.

**Architecture position.** Programming model and hosting. SPEC §10.4, §10.6.

## Key files

| File | What it is |
| --- | --- |
| `EmbabelBinder`, `EmbabelIntrospector` | Publishes `@Agent` metadata as AgentCards through `AgentBinder.adopt(card)` (so they are refreshed too) and skips beans the space binder already handles. |
| `EmbabelRemoteActions`, `RemoteActionInterceptor` | Generates (Byte Buddy) a typed `@Agent` whose one-argument actions map to the fleet's `RemoteActions`, one per foreign card and consumed/produced type pair. |
| `EmbabelRemoteActionsDeployer`, `EmbabelRemoteActionsDeployerLifecycle`, `EmbabelAgentSpacesAutoConfiguration` | Auto-deploy onto the platform (`agentspaces.embabel.auto-deploy`, on by default): redeploy when the set of actions changes, not on a refresh that changes only the issue time. |

## Key dependencies

`agentspaces-spring-boot-autoconfigure`, `agentspaces-agent`, `agentspaces-discovery`,
`agentspaces-identity`, `agentspaces-space`, `byte-buddy`, `agentspaces-spring-stubs` (provided).

## Notes

- The Party Bus (`agentspaces-partybus/` at the workspace root) is the largest consumer: its
  planner finds the fleet's scouts as planner actions through this bridge.
- Embabel's platform exposes no undeploy in the interface this module reaches; a vanished action
  stays in the previous generation until the platform restarts.
