# agentspaces-console

The fleet console as a served interface: a read-only observability peer presenting membership,
discovery, and space state over HTTP, plus command and control with a signed audit log.

**Architecture position.** Gateways and served interfaces. SPEC §12; `docs/CONSOLE.md`.

## Key files

| File | What it is |
| --- | --- |
| `FleetConsoleServer`, `ConsoleView` | The JDK `HttpServer` interface: a HAL+JSON API, a Server-Sent Events activity stream, and a single-file web UI themed by CSS variables. |
| `ConsolePanel` | The SPI applications implement to add their own panels. |
| `FleetCommander`, `Directive`, `DirectiveGate`, `ConsoleCommand`, `CommandContext`, `ConsoleEvent` | Command and control: token-gated directives (`aspace:console:operate`) dispatched through the space, judged by the `Authorizer` (`DIRECTIVE_ISSUER`), with a signed audit trail. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-peering`, `agentspaces-discovery`,
`jackson-databind`.

## Notes

- Shares the bearer principal model with the A2A gateway: a static token or a per-request
  validated JWT yields a subject and scopes.
- Pipe-bound capabilities are not visible to the console today (QA3 A3-6, deferred).
