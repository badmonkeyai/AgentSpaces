# agentspaces-transport-dtls

The DTLS binding of the `Transport` SPI: the fabric's signed frames over DTLS 1.2 (RFC 6347) on
UDP, on BouncyCastle's DTLS implementation, with Connection ID (RFC 9146) and a small recovery
sublayer that gives each association the reliable, ordered frame service every binding promises.
Pure Java: no native library, so it runs wherever the JDK runs. Use it where QUIC is unavailable
(no quiche classifier, a validated-crypto policy) or blocked, and on lossy UDP-only links.

**Architecture position.** Layer 1, Peering (a transport). SPEC §5.5 (the DTLS binding and the
recovery sublayer), §5.6. Design of record: `srad/dtls-support.md`.

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
| `UdpTransport` | The gated bare `udp` binding (SRAD §1.2.2): the hello exchange, the token, plaintext PDUs. Frames stay signed; no confidentiality or attestation. |

## Key dependencies

`bctls-jdk18on`, `bcprov-jdk18on`, `bcpkix-jdk18on`, `bcutil-jdk18on` (one version, from the BOM),
`agentspaces-api`, `agentspaces-identity`, `agentspaces-identity-tls`, `agentspaces-common`.

## Requirement to test map (SRAD `srad/dtls-support.md`)

| Requirements | Tests |
| --- | --- |
| DT-B-01 to DT-B-10, DT-B-15, DT-Q-PRF-01 | `DtlsTransportTest` |
| DT-R-01 to DT-R-13, DT-Q-REL-01, DT-Q-PRF-02, DT-Q-PRF-03, DT-Q-TST-01 | `RecoveryTest` |
| DT-R-02, DT-R-10, DT-Q-SEC-07, DT-Q-TST-06 | `PduTest` |
| DT-A-01 to DT-A-04, DT-Q-SEC-06 | `DtlsTransportTest`, `DtlsCaModeTransportTest` |
| DT-K-01 to DT-K-06, DT-Q-REL-02, DT-Q-REL-03, DT-Q-REL-07, DT-Q-SEC-11, DT-B-03 | `DtlsRelayTest` |
| DT-Q-SEC-01 to DT-Q-SEC-05, DT-Q-SEC-08 | `DtlsHardeningTest` |
| DT-Q-INT-01 | `DtlsInteropTest` (the JDK's JSSE engine as the second implementation) |
| DT-Q-REL-04, DT-Q-SEC-09 | `DtlsFleetTest`, `DtlsCaModeFabricTest` |
| DT-N-05, DT-Q-REL-06, DT-Q-AVL-02, DT-Q-INT-04 | `DtlsMixedFleetTest` |
| DT-N-01 to DT-N-04 | `TransportCapabilitiesTest` (peering) |
| DT-H-01 to DT-H-08, DT-Q-AVL-01, DT-Q-MNT-02 | `TransportBlocksAutoConfigurationTest` (autoconfigure) |
| DT-H-09, DT-H-10, DT-H-11 | `AgentSpacesFactoryTest` (Micronaut), `PropertiesLoaderTest` (runtime), `SpringFleetIntegrationTest` (spring-boot-it) |
| DT-L-03, DT-L-04 | `LocatorsTest` (agentspaces-bootstrap) |
| DT-C-01 | `test_beacon.py`, `beacon.test.ts` |
| DT-U-01 to DT-U-04 | `UdpTransportTest`, `TransportBlocksAutoConfigurationTest.theUdpBlockIsGatedAndRefusedUnderRequiredAttestation` |
| DT-M-01 to DT-M-05, DT-Q-SEC-12 | `BeaconSealTest`, `MulticastBeaconTest.sealedBeaconsAreFoundByKeyHoldersAndReadByNobodyElse` (peering), `TransportBlocksAutoConfigurationTest.multicastSealSealsTheBeaconUnderTheGroupContentKey` |

## Notes

- **DTLS 1.2 is the default and the only negotiable version today**: BouncyCastle 1.86 does not
  negotiate DTLS 1.3 (bc-java #2439 to #2442 are in review). `protocols` makes the switch a
  property once it does (SRAD §1.2.3).
- **DTLS 1.2 sends certificates in clear**, and the channel certificate's CN carries the identity
  key, so a passive observer learns which PeerIDs talk. Use `quic` or `tls` where that matters.
- **No congestion control** beyond the fixed window; the binding is for operator-managed links.
- **One source address, many peers**: the per-source hello rate (8/s) counts every dialer behind
  one NAT address; raise `hello-rate-per-source` for an office of peers behind one gateway.
- **Ordered delivery**: a lost fragment holds later frames for up to one retransmission timeout;
  lanes (SRAD §1.2.1) are the planned remedy.
- **Manual recipe, real loss.** On Linux, `tc qdisc add dev lo root netem loss 20%`, run two
  peers with `dtls` listeners, watch `DtlsCounters.retransmitted()` rise and the fleet keep
  converging; `tc qdisc del dev lo root` restores the link. For NAT rebinding, `DtlsRelayTest`
  drives a relay in-process.
- **Stress Testing** `DtlsConcurrentDialStressTest` runs with `-Ddtls.stress=true`. `-Ddtls.stress.rounds` sets the number of rounds and `-Ddtls.stress.load` the number of busy threads. Original bug fixed in #33, before it failed several rounds out of 600 against the old bind and passed 1,500 rounds (48,000 dials) with the fix.

