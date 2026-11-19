# agentspaces-a2a

The A2A gateway: AgentSpaces AgentCards served as A2A agent cards, and the A2A task interface
bound to a space, so A2A clients discover and drive a fleet through one standard HTTP interface.

**Architecture position.** Gateways and served interfaces. SPEC §12.

## Key files

| File | What it is |
| --- | --- |
| `A2aGateway` | The JDK `HttpServer`-based gateway: discovery endpoints, and A2A 1.0 and 0.3 at each agent's one URL, chosen per request by `A2A-Version` (none means 0.3). 1.0 has `SendMessage`, `GetTask`, `CancelTask`, `SendStreamingMessage`, `SubscribeToTask` and the four push-config methods; 0.3 has `message/send` (writes a task entry), `tasks/get` (reads state from the space), `message/stream` and `tasks/resubscribe` over Server-Sent Events. Then push notifications to allowlisted webhooks, an external base URL, the `failed` task state, and the bearer principal model shared with the console (`aspace:a2a:client`). |
| `A2aTaskBinding` | The binding from A2A tasks to space entries and back; deadlines measured on the injected clock. |
| `A2aAgentCard`, `A2aTranslator`, `A2aMessages` | The mechanical translation between the two card shapes and the JSON-RPC message types. A card is an A2A 1.0 card: `supportedInterfaces` lists 1.0 then 0.3, 0.3's top-level `url`, `preferredTransport` and `protocolVersion` describe the 0.3 interface, and the AgentSpaces identity rides in `capabilities.extensions`. |
| `A2aVersion`, `A2aWire` | The version negotiation (A2A 1.0 §3.6) and the 1.0 JSON forms: no `kind`, `TASK_STATE_*` and `ROLE_*` names, the `task` and `statusUpdate` wrappers, `google.rpc.ErrorInfo` error details. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `jackson-databind`. No web framework: the JDK server.

## Notes

- Stability EXPERIMENTAL: the shape tracks the A2A specification, 1.0.1 and 0.3.
- Under 1.0, `ListTasks` is refused as unsupported (the binding does not scope tasks to a caller, and 1.0 lists only a caller's own), and `GetExtendedAgentCard` as not configured.
- A refused token is `401`, a missing scope `403`, and the audit trail names the token subject.
