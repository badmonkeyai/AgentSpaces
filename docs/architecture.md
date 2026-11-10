# Architecture

Written for: Java developers who build with AgentSpaces.

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4  CAPABILITY SERVICES (advertised, pluggable)            │
│   task-auction (CBBA)  ·  vote  ·  aggregate (push-sum)         │
│   gossip-learn  ·  ordered-log (Raft quorum)  ·  custom…        │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3  AGENTSPACE (distributed tuple space)                   │
│   write/read/take/notify · templates · leases · CRDT replication│
│   conflict-resolution strategies · content-addressed payloads   │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2  ADVERTISEMENT & DISCOVERY                              │
│   typed signed documents · TTL · local ad-cache · queries       │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1  PEERING                                                │
│   peer groups · SWIM membership · gossip dissemination          │
│   rendezvous & relay roles · transport SPI                      │
├─────────────────────────────────────────────────────────────────┤
│ Layer 0  IDENTITY                                               │
│   keypair PeerID · signatures · (optional) DID interop          │
└─────────────────────────────────────────────────────────────────┘
```

Each layer depends only on the layers below it. A conforming minimal implementation comprises Layers 0 through 3 on a single LAN with the default conflict strategy; capability services and WAN topology are additive.

The full design lives in `SPEC.md` (v0.1.17) with implementation detail in `TECH-SPEC.md`, both in `agentspaces-spec/` beside this repository, together with the golden vectors they define conformance by (`golden.json`, vendored here as `tools/golden/golden.json`).
