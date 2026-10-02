# agentspaces-spring-stubs

Provided-scope copies of the Spring Framework and Spring Boot names the autoconfigure and
Embabel modules compile against (`@AutoConfiguration`, `@ConditionalOnClass`, `@Bean`,
`SmartLifecycle`, `BeanPostProcessor`, ...), so those modules build in environments without
Maven Central access.

**Architecture position.** Programming model and hosting (build-time only).

## Notes

- **Never ships to applications and must only be referenced in `provided` scope.** At runtime the
  real Spring classes with the same fully qualified names take over, since annotation and
  interface references resolve by name.
- No tests, no dependencies. See `docs/BUILD-ENVIRONMENTS.md` for the stub architecture.
