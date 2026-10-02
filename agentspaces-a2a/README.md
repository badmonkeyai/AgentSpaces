# agentspaces-a2a

The A2A gateway: AgentSpaces AgentCards served as A2A agent cards, and the A2A task interface
bound to a space, so A2A clients discover and drive a fleet through one standard HTTP interface.

**Architecture position.** Gateways and served interfaces. SPEC §12.

## Key files

| File | What it is |
| --- | --- |
| `A2aGateway` | The JDK `HttpServer`-based gateway: discovery endpoints, `message/send` (writes a task entry), `tasks/get` (reads state from the space), `message/stream` and `tasks/resubscribe` over Server-Sent Events, push notifications to allowlisted webhooks, an external base URL, the `failed` task state, and the bearer principal model shared with the console (`aspace:a2a:client`). |
| `A2aTaskBinding` | The binding from A2A tasks to space entries and back; deadlines measured on the injected clock. |
| `A2aAgentCard`, `A2aTranslator`, `A2aMessages` | The mechanical translation between the two card shapes and the JSON-RPC message types. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `jackson-databind`. No web framework: the JDK server.

## Notes

- Stability EXPERIMENTAL: the shape tracks the A2A specification.
- A refused token is `401`, a missing scope `403`, and the audit trail names the token subject.
