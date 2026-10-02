# agentspaces-transport-quic

The QUIC (RFC 9000) binding of the `Transport` SPI: one bidirectional QUIC stream per peer
connection carrying the same length-prefixed frames as TCP, on Netty's incubator QUIC codec
(quiche). Peers gain transport-level encryption, loss recovery, and connection migration; peer
authentication stays the signed-envelope layer, exactly as over TCP.

**Architecture position.** Layer 1, Peering (a transport). SPEC §5.5, §5.6.

## Key files

| File | What it is |
| --- | --- |
| `QuicTransport` | `scheme() = "quic"`; `dial`/`listen` mapping one stream to one `TransportConnection`; with the peer identity it presents an identity-endorsed channel certificate and reports `attestedPeer()`, otherwise an ephemeral certificate and no attestation. |
| `EphemeralCertificate` | The default self-signed certificate for unattested QUIC. |

## Key dependencies

`netty-incubator-codec-classes-quic` and the native `netty-incubator-codec-native-quic` (runtime),
`agentspaces-api`, `agentspaces-identity`, `agentspaces-common`.

## Notes

- Since 2026-10-01 the module builds and tests on macOS too: OS-activated pom profiles
  pick the published `osx-aarch_64` / `osx-x86_64` native classifier, and tests assume
  `Quic.isAvailable()` so a platform without one skips cleanly.
- CA parity with TLS (v0.1.13): `QuicTransport` takes a `ChannelTrust` (or a refreshable
  supplier), attests the full presented chain, disables resumption and early data in the
  attested mode (ASF-021), exposes `remoteChain()` for re-judging on a CRL refresh, and
  accepts any number of connections per listener. `QuicCaModeTransportTest`,
  `QuicCaModeFabricTest`, and `QuicListenerTest` pin it.
- Pipe-per-stream refinement is future work; today every capability frame shares the one stream.
