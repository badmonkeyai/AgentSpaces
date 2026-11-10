# What this is not

Written for: Java developers who build with AgentSpaces.

Honest boundaries:

- Not a database. The space holds coordination state and small payloads. Larger payloads get an identifer and stream directly between peers that require them. Databases and the like join the P2P network as assets, presenting an AssetCard and enabling agents to re-use their work (e.g. query results).
- Not a blockchain. No Byzantine fault tolerance. The threat model is one organization's trust boundary, with pluggable interfaces for security (similar to Spring Security), PKI, transports, and handling of A2A gateways. Someone could build a cryptographic chain across peer groups, agents, and messages.
- Not a replacement for MCP or A2A. MCP stays the tool layer that agentns are using and then sharing their work back into the P2P network. The A2A gateway is a bridge, sharing AgentCards into the peer network.
- Eventually consistent by default. `ConsistencyHint.FRESH` costs a round trip. Based upon the performance and battle rhythm of agents, we felt this was a good foundation, and you can always opt in for stonger agreements.
- Delivery is at least once unless the space uses the ordered strategy.
- CRDT convergence is not semantic agreement. A valid signature is not safe content. Quorum votes and ordered spaces exist for the cases that need agreement, and typical proper secure development practices should be followed. In other words, just because you support mTLS, doesn't mean you skip parameter validation, only we get that object to you quicker so you spend more time in the business logic and less in the plumbing.
- See [AgentSpaces-perf on GitHub](https://github.com/badmonkeyai/agentspaces-perf) for the benchmark suite. Performance is largely driven by how aggressive PKI is applied, e.g. mTLS, certs on agents for end-to-end consumere encryption or having only the transport hop-by-hop. We opted to leave this configurable, so different architectures can be built. The internet-open P2P network with cross-organization agents sharing work has a different set of requirements than an enterprise app that shares work across agents in different k8s clusters.

---
