# agentspaces-peering

Layer 1: the peer node, its per-group runtimes, and everything a group needs to stay a group —
the signed wire protocol, leased membership, two-channel gossip, the TCP and TLS transports,
founding documents, revocation, and containment.

**Architecture position.** Layer 1, Peering. SPEC §5, §9, §11; TECH-SPEC §3, §5.

## Key files

| Package / file | What it is |
| --- | --- |
| `node.PeerNode` | The node: listens on transports, joins groups, and owns the **one clock** — `startTicking(period)` (default 250 ms) drives membership probing, gossip, anti-entropy, replicated-space convergence, and every registered capability. `tick()` is the manual form tests drive. |
| `node.GroupRuntime` | One joined group: membership, gossip bus, sampler, revocations, `onTick(Runnable)` registration handles, `send`, `reportMisbehavior`. |
| `node.GroupFounding`, `SignedGroupAdvertisement`, `JoinCredentials` | Self-certifying groups: the GroupId is the hash of the founder-signed founding document, verified at join and on every network-learned group advertisement; INVITE credentials. |
| `membership.GroupMembership` | Leased membership with SWIM-style ping and indirect ping-req; no third-party verdicts. |
| `membership.MembershipAuthorizer` | The membership-rooted `Authorizer`: admitted members are permitted unless grants narrow an operation; grants name PeerIds or `peer/agent` AgentIds (`parsing(...)`), and an AgentId grant puts the operation at `AGENT` granularity. |
| `membership.RevocationRegistry` | Gossiped revocations and rotation-by-successor; the founder-rooted `RevocationValidator` seam. |
| `gossip.GossipBus`, `ReconcilableState` | Rumor push with hop decay and payload-hash dedup (ASF-012), and anti-entropy over reconcilable states paced by the group's gossip period. Stream handlers and reconcilers are registration handles that refuse a duplicate stream by name (QA4 A4-9). |
| `wire.Envelope`, `Bodies`, `WireCodec` | Signed CBOR envelopes (version 2, enforced), kind bodies, mandatory verification (elided on attested channels), the 8 MiB frame cap. |
| `transport.TcpTransport`, `TlsTcpTransport` | Length-prefixed frames over TCP; TLS 1.3 with identity-endorsed channel certificates and refreshable trust. |
| `blocks.BlockExchange` | Content-addressed blocks over `BLOCK_WANT`/`BLOCK` for payloads over 64 KiB. |
| `bootstrap.MulticastBeacon` | Opt-in LAN bootstrap: a signed self-advertisement datagram outside the envelope. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`. The QUIC binding is a separate
module (`agentspaces-transport-quic`).

## Notes

- Layer 3 and Layer 4 never bring a timer: `ReplicatedSpace` registers on `gossip().reconcile(...)`
  and `CapabilityRuntime` on `GroupRuntime.onTick(...)`; both ride `PeerNode.tick()`.
- 22 test classes, all on `SimNetwork` + `TestClock`; the seeded gossip simulation pins O(log N)
  propagation.
