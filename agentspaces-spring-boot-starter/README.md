# agentspaces-spring-boot-starter

Add this starter and Spring Boot to an application, configure the
`agentspaces` properties, and the fabric boots with the application:

```xml
<dependency>
  <groupId>ai.badmonkey.agentspaces</groupId>
  <artifactId>agentspaces-spring-boot-starter</artifactId>
</dependency>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter</artifactId>
</dependency>
```

```yaml
agentspaces:
  keystore: ./peer-keys
  bind: 127.0.0.1:7500
  groups:
    - name: research-fleet
      founding: research-fleet-v1
      seeds: ["hq.example:7500"]
      spaces:
        - name: tasks
        - name: findings
```

Turn on the served fleet console with two more properties:

```yaml
agentspaces:
  console:
    enabled: true
    port: 7590
```

The peer then serves the console UI at `http://host:7590/`, a HAL+JSON API
under `/api/v1` (walkable from the root by any hypermedia client, Spring
HATEOAS included), and a Server-Sent Events activity stream at
`/api/v1/events`. The default view watches every configured space
generically; a `ConsoleViewCustomizer` bean tells the console what the
application's entries mean (worker attribution above all), and every
`ConsolePanel` bean becomes one more section on the page and one more
document under `/api/v1/panels`:

```java
@Bean
ConsoleViewCustomizer researchConsole() {
    return (builder, spaces) -> builder
            .results("findings", Finding.class, f -> ((Finding) f).worker());
}
```

Turn on command-and-control to let operators drive the fleet from the console
(dispatch a task, cancel one the console dispatched, pause/resume/drain a
worker):

```yaml
agentspaces:
  console:
    enabled: true
    port: 7590
    command:
      enabled: true
      token: ${AGENTSPACES_CONSOLE_TOKEN}   # or blank with security.oidc.* set: JWT operators
  groups:
    - name: research-fleet
      spaces:
        - name: tasks
        - name: findings
        - name: fleet-control    # directives; workers subscribe here
        - name: c2-audit         # durable C2 audit log (optional)
```

The token gates the HTTP command routes; commands then execute under the
console peer's signed identity, with the operator name carried as attribution
(see `docs/CONSOLE.md` for the trust model). Register operator actions as
`ConsoleCommand` beans and workers honor directives with a `DirectiveGate`:

```java
@Bean
ConsoleCommand dispatchTask(/* ... */) { /* ctx.dispatch("tasks", task, lease) */ }
```

## Security profile and authorization

`agentspaces.security.profile` selects the whole posture (`dev-local`, `mtls`
(default), `mtls-oidc`, `zero-trust`), and the profile also selects who
answers "may this peer do that?" for every privileged operation: the
`Authorizer` bean. Under `dev-local` and `mtls` it is membership-rooted — one
`MembershipAuthorizer` per group over the group's admitted members, narrowed
by the optional grant lists — and under `mtls-oidc` and `zero-trust` it is an
`OidcAuthorizer` over your identity provider's JWTs, where each peer presents
its token inside its signed self-advertisement (`aspace:oidc` hint) and the
scopes `aspace:<operation>[:<scope>]` decide (`aspace:space-write:tasks`,
`aspace:directive-issuer`, ...). The starter threads the authorizer into the
key-wrap provider, spaces with `admission: authorizer`, and the
`DirectiveGates` bean workers use to obey the console (`gates.attach(space,
worker)`); pass the `Authorizer` bean (or `Authorizers.forGroup(name)`) to a
`RaftLog`, `DataQueryClient`, or `ConnectorRuntime` you wire yourself.

```yaml
agentspaces:
  security:
    profile: mtls-oidc
    oidc:
      issuer: https://idp.example.com
      audience: agentspaces-fleet
      jwks-url: https://idp.example.com/.well-known/jwks.json
      token-file: /var/run/secrets/aspace-token   # rotated by a sidecar
  groups:
    - name: research-fleet
      spaces:
        - name: tasks
          admission: authorizer      # aspace:space-write:tasks / aspace:space-take:tasks
        - name: vault
          admission: credential      # issuer-signed SpaceCredential entries admit
          credential-issuer: peer:z6Mk...   # default: this node
```

| Property | Default | Meaning |
| --- | --- | --- |
| `agentspaces.security.profile` | `mtls` | The posture; `mtls-oidc` and `zero-trust` authorize through the identity provider and require the three `oidc` settings below. |
| `agentspaces.security.oidc.issuer` | — | The token issuer the fleet trusts (matched exactly). Required under the OIDC profiles; startup fails fast naming it when missing. |
| `agentspaces.security.oidc.audience` | — | The audience tokens must carry. Required under the OIDC profiles. |
| `agentspaces.security.oidc.jwks-url` | — | The issuer's JWKS document (cached, outage-tolerant). Required under the OIDC profiles. |
| `agentspaces.security.oidc.token` | — | This node's own access token, bound to its PeerID by the `agentspaces_peer` claim; carried in its self-advertisement. |
| `agentspaces.security.oidc.token-file` | — | A file holding the token instead; re-read on every card-refresh tick (`agentspaces.card-refresh-millis`) so a sidecar can rotate it. `token` wins when both are set. |
| `agentspaces.security.grants.raft-voter` | (any member) | Membership profiles: PeerIDs permitted to vote and lead the ordered log. Empty leaves the operation open to every admitted member. |
| `agentspaces.security.grants.directive-issuer` | (any member) | PeerIDs whose command-and-control directives workers obey. On a node with `console.command.enabled=true` its own peer is added automatically, which narrows the operation to the console; worker nodes list the console's PeerID here. |
| `agentspaces.security.grants.connector-serve` | (any member) | PeerIDs whose data-query results are trusted and who may advertise assets. |
| `agentspaces.security.grants.key-holder` | (any member) | PeerIDs the key-wrap provider seals the group content key for. |
| `agentspaces.security.grants.space-write` | (any member) | PeerIDs admitted to write into `admission: authorizer` spaces. |
| `agentspaces.security.grants.space-take` | (any member) | PeerIDs admitted to take and complete in `admission: authorizer` spaces. |
| `agentspaces.groups[].spaces[].admission` | `group` | `group`, `allowlist` (with `allowed-agents`), `credential` (issuer-signed `SpaceCredential` entries admit), or `authorizer` (the profile's authorizer decides per space name). |
| `agentspaces.groups[].spaces[].credential-issuer` | this node | Under `admission: credential`, the PeerID whose signed credentials admit agents; only that node may `grant` and `revoke`. |
| `agentspaces.console.command.token` | — | The static operator secret for the command routes. When set it always wins over the identity provider. |
| `agentspaces.console.command.required-scope` | `aspace:console:operate` | With `command.token` blank and `security.oidc.*` configured, the command routes validate each request's bearer JWT against the identity provider, audit the token's `sub` as the operator, and require this scope. |

Any bean with `@AgentSpec` (or the `@SpaceAgent` stereotype) or
`@SpaceTake`/`@SpaceNotify`/`@BidFunction` methods enrolls automatically:
worker loops start, bids wire, and the bean's AgentCard publishes into the
group. The one-annotation experience is `@SpaceAgent` — a stereotype that is
both `@Component` and `@AgentSpec`, so component scanning and fleet enrollment
are the same drop:

```java
@SpaceAgent(description = "Researches topics from the shared task space")
public class Researcher {

    @SpaceTake                 // sole space inferred; durations like "10m" work
    public Finding research(ResearchTask task) {
        return new Finding(task.topic(), summarize(task));
    }
}
```

The annotations carry the whole coordination discipline. A `@SpaceNotify`
method may *return* the next entry in a flow (react to X, produce Y) — the
binder dedupes redeliveries, runs the reaction on its own virtual thread, and
writes the result. A `@SpaceRef` field of type `Space` is injected at bind
time for mid-method writes, which also makes `@Scheduled` + `@SpaceRef` the
idiomatic periodic agent. The `space` attribute is optional whenever the group
registers exactly one space, and any application can compose its own
stereotype: annotate an annotation with `@AgentSpec` and give it `name`,
`description`, and `goals` attributes. See `examples/example-11-quickstart`
for the whole experience in one file. Application code that needs more than
the annotations navigates the fluent facade:

```java
@Component
public record Coordinator(AgentSpaces spaces) {
    public void submit(String topic) {
        spaces.group("research-fleet").space("tasks")
              .write(new ResearchTask(topic, 3), Lease.of(Duration.ofMinutes(30)));
    }
}
```

This starter deliberately does not declare Spring Boot as a dependency. The
core reactor carries no Spring dependency: the autoconfigure module compiles
against a provided-scope stub interface (`agentspaces-spring-stubs`) that carries
the real Spring fully qualified names, so the application's own Spring Boot
version resolves them at runtime and the stubs never appear on an application
classpath. The Spring context integration test under `integration-tests/`
runs against real Spring Boot with `mvn clean verify -Pspring-it`; it is part
of the release gate. `docs/BUILD-ENVIRONMENTS.md` has the details.
