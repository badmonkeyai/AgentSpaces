# The Fleet Console

The console (`agentspaces-console`) is observability derived entirely from
coordination state. Membership answers "who is alive", discovery answers "who
can do what", and the replicated space answers "what is the fleet working
on"; the console is one more read-only peer that folds those three sources
into a served surface. There is no telemetry pipeline and no metrics
database, and attaching a console changes nothing about how a fleet
coordinates.

## The three surfaces

Each surface is an open standard, chosen so different consumers can build on
the console without touching its code.

**The HAL+JSON API** under `/api/v1` serves documents carrying `_links` in
the HAL convention (`application/hal+json`), so a hypermedia client, Spring
HATEOAS included, walks the whole surface from the root without hardcoding
paths: `overview`, `spaces` (and `/api/v1/spaces/{name}`), `peers`, `ads`,
`panels`, and `events`. Responses allow cross-origin reads, so an external
dashboard consumes the API directly.

**The activity stream** at `/api/v1/events` is Server-Sent Events: every
write and completion the console observes from replica state (a claimed-but-
uncompleted take is not itself a replicated event, so it shows as the drop in
`queued` and the rise in `inProgress` rather than a stream entry), with
monotone sequence numbers doubling
as SSE event ids. A reconnecting `EventSource` presents the standard
`Last-Event-ID` header and replay resumes after it, as far back as the
console's bounded ring still reaches; plain HTTP clients pass `?after=<seq>`
instead.

**The web UI** at `/` is one HTML file with no build step, populated only
through the public API. It is themed entirely by CSS custom properties (the
`--as-*` variables at the top of the file, dark mode included), so restyling
is an override and replacing the page wholesale means serving any other file
against the same API.

## Assembling one

```java
ConsoleView view = ConsoleView.builder()
        .space("tasks", replica, TaskEntry.class)
        .results("tasks", Finding.class, f -> ((Finding) f).worker())
        .members(() -> runtime.membership().allMembers())
        .discovery(discovery)
        .build();

FleetConsoleServer server = FleetConsoleServer.builder(view)
        .fleetName("research-fleet")
        .panel(myPanel)
        .build();
int port = server.start(7590);
```

Per watched space the view counts entries written and completed (from
notifications, so remote activity the replica merges is included) and reads
what is queued on demand; work in progress is the difference,
`written - completed - queued`, which is exactly the set of entries some
agent's take lease holds right now. Worker attribution comes from result
watching: result entries name their worker and the view counts completions
per worker from them.

## Extending it

The extension point is `ConsolePanel`: an id, a title, and a live JSON
document, served at `/api/v1/panels/{id}/data` and rendered by the stock UI
as a refreshed table with no panel-specific frontend code. A panel that wants
richer rendering overrides `htmlFragment()` and reads its own data endpoint
like any other client. Panels never touch the HTTP server, routing, or the
page shell, which is what keeps a console with many team-specific panels
upgradeable.

## Under Spring Boot

The starter serves the console from properties:

```yaml
agentspaces:
  console:
    enabled: true
    port: 7590
```

The auto-configured view watches every configured space generically;
`ConsoleViewCustomizer` beans refine it with the application's entry types
and attribution, every `ConsolePanel` bean in the context becomes a section,
and each piece is `@ConditionalOnMissingBean`, so an application replaces the
view or the server by declaring its own. The console starts after the fabric
(a later `SmartLifecycle` phase) and stops before it, so it never outlives
the state it reports.

## Command and control

The console is read-only until command-and-control is enabled. C2 turns the
console into a surface that mutates the fleet: an operator can dispatch a task,
cancel a task the console dispatched, broadcast a directive to workers, and run
any application-registered command. Enable it by building the server with a
commander and an operator token:

```java
FleetCommander commander = FleetCommander.builder(group::space)
        .controlSpace("fleet-control")
        .auditSpace("c2-audit")
        .view(view)
        .command(dispatchTaskCommand())
        .build();

FleetConsoleServer.builder(view)
        .commandAndControl(commander, operatorToken)
        .build();
```

**The trust model.** The operator token gates the HTTP edge: every mutating
route (`POST /api/v1/commands/*`) requires the token, presented as
`Authorization: Bearer <token>` or `X-Console-Token: <token>` and compared in
constant time; read routes stay open and GET-only. Once past the edge, a
command runs under the console peer's own fabric identity, not the operator's:
the task it dispatches, the directive it broadcasts, and the audit record it
writes are all Ed25519-signed and leased by the console peer, and the
authenticated operator name (the fixed `operator` for a static token, the
token's subject for a JWT, see below) travels as attribution metadata. So a command is cryptographically
attributable to the console peer "on behalf of" the named operator. This keeps
the fabric's signing intact and needs no browser crypto or per-operator key
enrollment; a stronger model where each operator signs commands with their own
key is a future hardening, not a wire change to the entries themselves.

**The commands.** Three come built in and one is open-ended. `dispatch`
writes an application-typed entry into a space (through a `ConsoleCommand`)
and tracks it so `cancel` can withdraw it later — the console can only cancel
entries it wrote, because removal is issuer-signed (SPEC §11a.4), so a foreign
entry cannot be cancelled from here. A directive broadcast writes a
`Directive` (PAUSE, RESUME, DRAIN, for a named worker or `*`) into the control
space; workers honor it through a `DirectiveGate` they attach to that space
and consult before taking work. And the `ConsoleCommand` SPI (the command-side
sibling of `ConsolePanel`) lets an application register its own typed
operator actions — key rotation, capability invocations, custom fleet
actions — each rendered as a form on the UI from the command's declared
fields.

**The audit log.** Every command becomes a signed, leased entry: dispatched
tasks and directives land in their spaces, and, when an audit space is
configured, a `C2Record` lands there too — so the fabric itself is the durable,
attributable command ledger. Commands also appear in the console's activity
stream (kind `command`), so an operator watching the feed sees C2 actions
alongside fleet activity.

**Operators from your identity provider.** The token gate is a
`BearerAuthenticator`, and the static shared secret is only its simplest form:
`commandAndControl(commander, token)` is `commandAndControl(commander,
BearerAuthenticator.staticToken(token, "operator"))`, so every holder of the
secret is audited as `operator`. Under `mtls-oidc` the console becomes an
OAuth2 resource server instead: build it with the OIDC module's
`BearerJwtValidator` (same issuer, audience, and JWKS as the fleet's
`OidcAuthorizer`) and every `POST /api/v1/commands/*` validates the presented
JWT per request. A token that fails validation is `401` with
`WWW-Authenticate: Bearer`; a valid token whose `scope` claim lacks
`aspace:console:operate` (or the scope set by `requiredScope(...)`) is `403`.
The audited operator on the `Directive`, the `C2Record`, and the activity
stream is the token's `sub`, never text the client typed: the request body's
`operator` field is ignored, so revoking an operator at the identity provider
ends their console access when their token lapses, and the audit ledger names
who acted. Every GET, the command catalog included, stays open under either
gate.

```java
FleetConsoleServer.builder(view)
        .commandAndControl(commander,
                BearerJwtValidator.fromJwks(issuer, audience, jwksUrl))
        .build();
```

**Under Spring Boot.** Set `agentspaces.console.command.enabled=true` and
`agentspaces.console.command.token`, and declare the control (and optional
audit) spaces among the group's spaces. Every `ConsoleCommand` bean is
collected automatically. When the token is blank and
`agentspaces.security.oidc.{issuer, audience, jwks-url}` are configured, the
starter builds the JWT gate instead and requires
`agentspaces.console.command.required-scope` (default
`aspace:console:operate`); a static token wins when both are present. With
neither, C2 stays off and the console is read-only.

## Design constraints worth knowing

The server runs on the JDK's built-in HTTP server with virtual threads, the
same footing as the A2A gateway, so the console adds no web framework to a
fleet. Example 09 (`examples/example-09-fleet-console`) assembles the whole
thing against a live TCP fleet, crash recovery included, and registers one
demonstration panel; the Spring integration test proves the property-gated
wiring under real Spring Boot.
