# agentspaces-transport-dtls

The DTLS binding of the `Transport` SPI: the fabric's signed frames over DTLS 1.2 (RFC 6347) on
UDP, on BouncyCastle's DTLS implementation, with Connection ID (RFC 9146) and a small recovery
sublayer that gives each association the reliable, ordered frame service every binding promises.
Pure Java: no native library, so it runs wherever the JDK runs. Use it where QUIC is unavailable
(no quiche classifier, a validated-crypto policy) or blocked, and on lossy UDP-only links.

**Architecture position.** Layer 1, Peering (a transport). SPEC §5.5 (the DTLS binding and the
recovery sublayer), §5.6.

## Using it

Add the module; the BOM manages its version and brings `bctls-jdk18on` with it.

```yaml
agentspaces:
  transport:
    dtls:
      enabled: true
      bind: 0.0.0.0:7502          # omit for dial-only
      advertise: peer-a.example:7502
      priority: 1                 # default: after agentspaces.bind's endpoint
      attestation: identity       # or none: encryption only, never attesting
```

Seeds name it as `dtls://host:7502`. In code: `node.listen(new DtlsTransport(identity), "0.0.0.0:7502", 1)`.
A node with `require-attestation` may run on `dtls` alone.

## Key files

| File | What it is |
| --- | --- |
| `DtlsTransport` | `scheme() = "dtls"`; the three identity modes of the `tls` binding plus `unattested()`; `dial`, `listen`, `options`, `cryptoProvider`, `counters`. |
| `DtlsOptions` | The tunables (mtu 1,200, window 64, idle 60 s, half-open 1,024, 8 hellos/s per source, Connection ID on, `[DTLSv1.2]`), range-checked. |
| `AspaceDtlsClient`, `AspaceDtlsServer` | BouncyCastle `DefaultTlsClient`/`DefaultTlsServer` under the policy: AEAD ECDHE suites, extended master secret required, no resumption, renegotiation, or Heartbeat, 8-byte Connection ID issued by the listener. |
| `DtlsListener` | One UDP socket: demultiplexes by Connection ID then source address, admits new sources through `DTLSVerifier` cookies, a per-source rate, and the half-open bound. |
| `DtlsAssociation` | The `TransportConnection`: drain, delivery, and timer threads around a BouncyCastle `DTLSTransport`; follows a migrated peer. |
| `PeerDatagramTransport` | BouncyCastle's `DatagramTransport` over a queue; the anti-amplification budget on an unvalidated path. |
| `Recovery`, `Pdu` | The recovery sublayer: a pure state machine on a clock (RFC 9000 ACK ranges, RFC 6298 timing, RFC 9002 loss detection) and its bounded codec. |
| `BcSupport`, `DtlsCounters` | Suites, versions, certificate conversion; counters for the console. |
| `RecordLayer`, `PlainRecordLayer` | What an association runs over: a BouncyCastle DTLS association, or bare UDP with an address token. |
| `UdpTransport` | The gated bare `udp` binding, for a closed test range, a sensor segment with no budget for a handshake, or a link encrypted underneath: the hello exchange, the token, plaintext PDUs. Frames stay signed; no confidentiality or attestation. |

## Key dependencies

`bctls-jdk18on`, `bcprov-jdk18on`, `bcpkix-jdk18on`, `bcutil-jdk18on` (one version, from the BOM),
`agentspaces-api`, `agentspaces-identity`, `agentspaces-identity-tls`, `agentspaces-common`.

## What the tests cover

| Behaviour | Tests |
| --- | --- |
| The binding: scheme, addresses, dial and listen, the unattested mode, and smoke bounds on loopback | `DtlsTransportTest` |
| The recovery sublayer: every frame once and in order up to 8 MiB or a failure, acknowledgement ranges, timing, loss detection | `RecoveryTest` |
| The recovery sublayer's bounded codec | `PduTest` |
| The three identity modes and attestation | `DtlsTransportTest`, `DtlsCaModeTransportTest` |
| Connection ID, a peer that migrates, NAT rebinding through an in-process relay | `DtlsRelayTest` |
| Hardening: the versions and suites, cookies, the per-source hello rate, the half-open bound, the anti-amplification budget, no Heartbeat or renegotiation, the extended master secret | `DtlsHardeningTest` |
| Interoperation with the JDK's JSSE engine as a second implementation | `DtlsInteropTest` |
| A fleet over `dtls` converges and carries a leased take; required attestation refuses unattested associations | `DtlsFleetTest`, `DtlsCaModeFabricTest` |
| Fleets that mix `dtls` with every other scheme, and a peer that stays reachable when one of its listeners closes | `DtlsMixedFleetTest` |
| The transport capabilities and the refusal of a non-attesting binding under required attestation | `TransportCapabilitiesTest` (peering) |
| The `agentspaces.transport.dtls.*` listener blocks of the Spring starter | `TransportBlocksAutoConfigurationTest` (autoconfigure) |
| The same blocks in Micronaut, the plain runtime and a Spring fleet | `AgentSpacesFactoryTest` (Micronaut), `PropertiesLoaderTest` (runtime), `SpringFleetIntegrationTest` (spring-boot-it) |
| The `udp` protocol of the Kubernetes and DNS-SD locators | `LocatorsTest` (agentspaces-bootstrap) |
| The Python and TypeScript clients skip `dtls` and `udp` endpoints and dial the next one they speak | `test_beacon.py`, `beacon.test.ts` |
| The gated bare `udp` binding, refused under required attestation | `UdpTransportTest`, `TransportBlocksAutoConfigurationTest.theUdpBlockIsGatedAndRefusedUnderRequiredAttestation` |
| Sealed multicast beacons, found by key holders and read by nobody else | `BeaconSealTest`, `MulticastBeaconTest.sealedBeaconsAreFoundByKeyHoldersAndReadByNobodyElse` (peering), `TransportBlocksAutoConfigurationTest.multicastSealSealsTheBeaconUnderTheGroupContentKey` |

## Notes

- **DTLS 1.2 is the default and the only negotiable version today**: BouncyCastle 1.86 does not
  negotiate DTLS 1.3 (bc-java #2439 to #2442 are in review). `protocols` makes the switch a
  property once it does.
- **DTLS 1.2 sends certificates in clear**, and the channel certificate's CN carries the identity
  key, so a passive observer learns which PeerIDs talk. Use `quic` or `tls` where that matters.
- **No congestion control** beyond the fixed window; the binding is for operator-managed links.
- **One source address, many peers**: the per-source hello rate (8/s) counts every dialer behind
  one NAT address; raise `hello-rate-per-source` for an office of peers behind one gateway.
- **Ordered delivery**: a lost fragment holds later frames for up to one retransmission timeout;
  separate lanes for capability pipes are the planned remedy.
- **Manual recipe, real loss.** On Linux, `tc qdisc add dev lo root netem loss 20%`, run two
  peers with `dtls` listeners, watch `DtlsCounters.retransmitted()` rise and the fleet keep
  converging; `tc qdisc del dev lo root` restores the link. For NAT rebinding, `DtlsRelayTest`
  drives a relay in-process.
- **Stress Testing** `DtlsConcurrentDialStressTest` runs with `-Ddtls.stress=true`. `-Ddtls.stress.rounds` sets the number of rounds and `-Ddtls.stress.load` the number of busy threads. Original bug fixed in #33, before it failed several rounds out of 600 against the old bind and passed 1,500 rounds (48,000 dials) with the fix.

