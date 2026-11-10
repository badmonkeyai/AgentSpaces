# About AgentSpaces

Written for: Java developers who build with AgentSpaces.

**A peer-to-peer coordination fabric for agents, services, and the people who work with them.**

AgentSpaces is a P2P framework to quickly discover and join P2P networks, where agents, software services, and people share assets, queries, results, and coordinate. The framework helps you build fleets of agents, tie in other software, and coordinate across them with a replicated tuple space, quorum builing, and more. Agents post work, claim tasks, publish results, vote on decisions, and recover from each other's failures. There is no broker, no registry, no scheduler, and no server to keep alive. Coordination is peer-to-peer, secured end to end, and replicated across the fleet.

Agents are ephemeral. They crash, stall on model calls, and get built by different teams on different frameworks in different languages. Coordinating them today means stitching together a message queue, a database, a service registry, a workflow engine, and an audit log, and then discovering that none of those components were designed for agents.

AgentSpaces replaces that stack with one coordination layer built for autonomous, failure-prone, independently deployable participants.

## What you can build with it

- **Autonomous operations.** Incidents are claimed by specialist agents, investigated, escalated, and remediated. A crashed worker's task reappears automatically. Humans approve consequential actions.
- **Agentic back offices.** Business processes become shared state. Specialists join without redesigning a workflow.
- **Model markets.** Inference is discovered, bid on, and routed by cost, latency, and confidence. Stronger models are used only when uncertainty justifies the cost.
- **Self-assembling teams.** Agents form around a problem and dissolve when it's solved.
- **Edge fleets.** Robots and sensors keep coordinating when the network doesn't.
- **Enterprise meshes.** Federate existing agents, services, and data without replacing them.

## How it works

- **Peers join a group.** Each peer has a cryptographic identity.
- **Participants advertise capabilities** through signed, leased documents. Agents create AgentCards; non-agents (databases, sensors, services) create AssetCards.
- **Everyone shares a replicated tuple space**, a coordination layer with `write`, `read`, `take`, and `notify`.
- **Work is written to the space.** Workers claim it with a lease. If a worker crashes, the lease expires and the work reappears.
- **Results are signed and attributable.**

Coordination strength is selectable:

| Need | Mechanism |
|---|---|
| Simple work distribution | Lease races |
| Resource allocation | Auctions |
| Decisions | Quorum voting |
| Irreversible actions | An ordered log |

The core provides leases, gossip, and CRDT replication. The stronger semantics are Layer 4 capabilities that peers advertise and use.

**The design test:** if you add a new capable peer tomorrow, can the system become more capable without redesigning the system today?

## What makes it different

- **No central coordinator.** Peer groups, gossip, and CRDT replication mean there's no broker, registry, or workflow engine in the critical path.
- **Signed provenance.** Every entry, ballot, and claim is signed. You can always trace who did what.
- **Leases as failure handling.** Failure recovery is a property of the data model rather than a separate subsystem.
- **Selectable coordination.** Use the least expensive mechanism that satisfies the workload. Uplift to auction, quorum, or ordered consensus only where consequence requires it.
- **Extensible.** New coordination patterns arrive as advertised capabilities, with no change to the core.
- **Polyglot.** Java, Python, TypeScript, Clojure, and Groovy peers share the same signed wire protocol.
- **Trust boundaries.** Self-certifying groups, per-agent keys, group-key encryption, and pluggable membership and authorization let fleets span organizations.

## Where it came from

The ideas behind AgentSpaces are old. Tuple spaces were described in 1985. Peer-to-peer groups were tried in the late 1990s. Two of our founders worked through that era at Bell Labs, Lucent, and Avaya and wanted to build with those systems. Neither Jini nor JXTA was ever quite right: Java-only, centralized spaces, security as an afterthought.

Agents are what changed the math. They're ephemeral, failure-prone, and built by many teams. They need the coordination model that the earlier systems imagined, implemented with tools that didn't exist then: CRDTs, Ed25519, QUIC, and signed CBOR over gossip. The layered approach also enables us to swap in different implementations (e.g. transport tier, security providers, etc), rather than mandate one. Twenty years ago, there was a lot of compute not being used that a P2P protocol would have enabled compelling architectures. There's even more today. So we're excited to bring this back.

You don't need to know any of that history to use AgentSpaces. But if you're curious, it's a good story.
