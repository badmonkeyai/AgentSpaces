# integration-tests

`core-it` is in the core reactor: reactor-wide checks of the core (`srad/agentspaces-event-loop.md`). Today it holds
`IdleCostIT`, the idle-cost harness (EL-M-01): groups of real peers, one JVM each, on TCP with the starter's defaults,
recording idle CPU, wakes, lateness, keepalive frames, detection and newcomer times in the measurements store. It runs
only when asked, one shape per run:

```
mvn -o test -pl integration-tests/core-it -Didle.cost=true -Didle.members=10 -Didle.spaces=4 -Didle.coalesceMillis=20
```

`-Didle.ts=<agentspaces-typescript dir>` makes the last member an idle TypeScript peer (node, read-only use of the
client's build). The other properties are in the class comment.

The real-Spring release gate. `spring-boot-it` boots a `SpringApplication` on the actual Spring
Boot starter (not the stub interface) and works a fleet over TCP, which is what proves the
autoconfigure module against the real framework.

**Architecture position.** Applications and verification.

## Notes

- Built under `-Pspring-it` (CI activates it through `env.CI`), because Spring Boot resolves from
  Maven Central and network-restricted environments cannot see it.
- The release rule in `docs/BUILD-ENVIRONMENTS.md` requires this green before any release; the
  workspace's `verify-all.sh` runs it as part of the core gate.
