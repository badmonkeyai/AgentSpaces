# agentspaces-test-support

Deterministic test infrastructure shared by every module's suite and available to
applications. Every cluster test in the repository runs on it: real `PeerNode`s, no wall
clock, no sockets.

**Architecture position.** Foundations (test-scoped).

## Key files

| File | What it is |
| --- | --- |
| `TestClock` | A mutable `InstantSource`: `create()`, `startingAt(instant)`, `advance(duration)`. Every lease, gossip period, HLC, and certificate expiry in a test follows it. |
| `SimNetwork` | An in-JVM `Transport` fabric with named endpoints (`register("a")`), partitions (`partition(a, b)` / `heal()`), and channel attestation control (`attest(node, peerId)`), so attested-channel and required-attestation behaviour is testable without TLS. |
| `TestCa` | Mints certificate authorities, issued and revoked certificates, and CRLs for the enterprise-CA channel-trust tests. |
| `Fixtures` | The entry types the examples and tests share. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, BouncyCastle (`bcprov`/`bcpkix`, for `TestCa`).

## Notes

- Tick loops must advance the clock: anti-entropy paces itself by the group's gossip period on
  the node clock, so `nodes.forEach(PeerNode::tick); clock.advance(...)` is the idiom.
- The golden vectors' HLC stamp is 2025-01-01; a test that folds golden deltas into a real
  replica clocks it at `certificate_verify_at` or the record is already a collected tombstone.
