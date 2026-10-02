# integration-tests

The real-Spring release gate. `spring-boot-it` boots a `SpringApplication` on the actual Spring
Boot starter (not the stub interface) and works a fleet over TCP, which is what proves the
autoconfigure module against the real framework.

**Architecture position.** Applications and verification.

## Notes

- Built under `-Pspring-it` (CI activates it through `env.CI`), because Spring Boot resolves from
  Maven Central and network-restricted environments cannot see it.
- The release rule in `docs/BUILD-ENVIRONMENTS.md` requires this green before any release; the
  workspace's `verify-all.sh` runs it as part of the core gate.
