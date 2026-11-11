# agentspaces-transport-webrtc

The `webrtc` binding of the `Transport` SPI: the fabric's signed frames over one WebRTC data
channel per peer connection, with signalling carried over the fleet itself and attestation by the
DTLS fingerprint each side signs. It lets a browser member and a JVM member, or two browser
members behind a relay, open a direct, encrypted, authenticated link where no one can dial the
other. The engine sits behind a package-private `DataChannelEngine` seam; the first engine is
libdatachannel (through `tel.schich:libdatachannel-java`), whose native library this module alone
carries, as the QUIC module alone carries quiche.

**Architecture position.** Layer 1, Peering (a transport). SPEC §5.5 (the transport SPI), §5.6
(channel authentication; the relayed-signal exception this binding relies on), and annex B (the
WebRTC formats).

## Using it

Add the module (the BOM manages its version and brings the engine and its native classifier). A
node that accepts `webrtc` registers the binding and advertises `Endpoint("webrtc", "sig", p)`;
`sig` means "reach me by signalling over the fabric", so a dialer reaches the peer by its PeerID,
not by an address.

```java
WebRtcTransport webrtc = new WebRtcTransport(identity.peerId());   // or new WebRtcTransport(peerId, options)
node.listen(webrtc, "sig", /* priority */ 1);                     // accept offers; advertise the endpoint
webrtc.signalling().attach(groupRuntime);                          // the rtc-signal pipe on this group
```

Spring and Micronaut wire it from `agentspaces.transport.webrtc.*` (below); applications rarely
construct it by hand. Capabilities: `ENCRYPTED, ATTESTING, RELIABLE, ORDERED`, so a node with
`require-attestation` may run on `webrtc` (it attests by the signed fingerprint).

```yaml
agentspaces:
  transport:
    webrtc:
      enabled: true
      priority: 1                         # advertised as webrtc "sig", after the stream endpoints
      ice-servers:                        # empty by default: only host and mDNS candidates gather
        - urls: [stun:stun.example.com:3478]
        - urls: [turns:turn.example.com:443?transport=tcp]
          username: ${TURN_USER}
          credential: ${TURN_SECRET}
      offload:                            # the relay offload: settings, not caps (below)
        frames-per-minute: 30
        retry-after: 5m
  roles: [RELAY]                          # a seed that lets members signal each other
```

The native library is published per platform under a classifier (Linux `aarch64`/`x86_64`, macOS
`macos-arm64`/`macos-x86_64`, Windows `windows-x86_64`); the POM selects it by the build OS, and
`-Dwebrtc.native.classifier=…` overrides. Where no native loads, registration fails naming the
artifact, and the `webrtc` tests skip with that reason.

## How it works

- **One connection, one channel.** `aspace/1`, negotiated id 0, ordered, reliable (SPEC annex B).
  Frames travel as one-byte-header chunks of at most `min(16 KiB, maxMessageSize) - 1` payload
  bytes; the receiver reassembles up to 8 MiB and closes on a larger frame or a malformed chunk.
  The reserved `aspace-dgram/1` channel carries the `DATAGRAM` capability and opens only when both
  ends report it.
- **Signalling.** `OFFER`, `ANSWER`, `CAND`, `END` bodies (CBOR, canonical wire v3, SPEC annex B)
  travel as signed `PIPE_DATA` of `aspace:cap/rtc-signal`, directly when the target is connected
  and inside a `RELAY_FRAME` through a RELAY-role member otherwise. Receivers ignore unknown keys,
  so later fields need no migration.
- **Attestation.** Each side's DTLS certificate fingerprints go in the signed `fp` list; a remote
  SDP whose `a=fingerprint` lines differ is refused, and DTLS refuses a certificate that differs
  from the SDP, so an open channel proves the remote PeerID signed for the key — `attestedPeer()`.
- **Bounds, none a cap on anything.** 6 offers per remote peer per minute, 64 candidates
  per session, 64 KiB per signal body, pending sessions expiring at the 15 s dial bound; a dial
  sends `END(2)` on timeout and the node falls through to the next endpoint or the relay. Nothing
  caps members, sessions, or open connections.
- **Privacy.** Candidates and SDP travel only inside signals to their target, are logged
  at `TRACE` alone, and never appear in an advertisement.
- **The relay offload.** The binding is *signalled* (`Transport.signalled()`): a dial
  is a round of signals that may take the whole dial bound, so the node never dials a `sig`
  endpoint from inside a send. A send to a member it holds no connection to takes the relay, and
  the node counts, per group and member, the frames a relay carried between them in a sliding
  minute (sent through a relay, or unwrapped from a `RELAY_FRAME` addressed to it). On its tick,
  for a member more than `frames-per-minute` (30) frames crossed with, that advertises a
  `webrtc` endpoint, has no connection here, no dial in flight, and no failed dial inside
  `retry-after` (5 minutes), the node dials that endpoint on a thread of its own, attaches the
  channel, and sends directly from then on; the relay carries none of the pair's frames.
  A dial that cannot complete ends with `END(2)` at the dial bound, loses no
  frame (the relay carried them meanwhile), and enters the backoff. Both values
  are settings (`PeerNode.Builder.relayOffload`, `agentspaces.transport.webrtc.offload.*`), not
  caps; `PeerNode.relayOffloadStats()` counts attempts, opened, and failed per group, and the
  console's traffic panel shows them beside the relay counters.

## Key files

| File | What it is |
| --- | --- |
| `WebRtcTransport` | `scheme() = "webrtc"`; capabilities; `dial(address, target)` by signalling, `listen("sig", …)` to accept; `signalling()`, `counters()`, `engineUnavailable()`. |
| `RtcSignalCapability` | Registers the `aspace:cap/rtc-signal` pipe on a node and hands sessions to the binding; verifies and bounds every signal; the offer-tie rule. |
| `WebRtcSession` | One signalling session: drives the engine, turns its events into signals, checks fingerprints, ends with an `END` code. |
| `WebRtcConnection` | The `TransportConnection`: annex-B chunks out with the 4 MiB/1 MiB backpressure, reassembly in on a delivery thread, the remote attested. |
| `DataChannelEngine` | The engine seam: peer connection, the `aspace/1` channel, local description, candidates, open, messages, buffered-amount-low, and end, with nothing of any one engine's API. |
| `LibDataChannelEngine` | The libdatachannel engine: DTLS 1.2 AEAD-ECDHE floor (1.3 where offered), its native per-connection ECDSA certificate, callbacks copied on the library thread then handed to a per-session worker. |
| `Chunks`, `Fingerprints`, `RtcSignal` | The annex-B chunk codec, the RFC 8122 fingerprint handling, and the signalling body (canonical CBOR, bounds, unknown-key tolerance). |
| `WebRtcOptions`, `WebRtcCounters` | The tunables (ICE servers, bounds), each range-checked; the counters for the console. |

## Key dependencies

`tel.schich:libdatachannel-java` and its platform native classifier, `slf4j-api` (through it);
`agentspaces-api`, `agentspaces-common`, `agentspaces-identity`, `agentspaces-identity-tls`,
`agentspaces-peering`.

## What the tests cover

| Behaviour | Tests |
| --- | --- |
| The scheme, the capabilities and the address; ICE servers; the engine seam; the signalling capability | `WebRtcTransportTest` |
| Offer, answer and candidate signalling, and the refusal of a forged answer or offer | `WebRtcTransportTest`, `LibDataChannelEngineTest` (the engine's own fingerprints) |
| Chunk capacity, the `ceil(n/C)` split, reassembly and its bounds, the channel labels | `ChunksTest` |
| Backpressure: pause above 4 MiB, resume at the low event, fail after the blocked bound | `WebRtcTransportTest` |
| The dial timeout sends `END(2)` and both sessions expire | `WebRtcTransportTest` |
| The signalling bounds: offers per peer, candidates per session, body size, unknown session, wrong origin, non-member, no acceptor | `WebRtcTransportTest` |
| Privacy and counters: no candidate on the channel name; the counters | `WebRtcTransportTest`, `WebRtcCounters` |
| The RFC 8122 fingerprint format | `FingerprintsTest` |
| The signal bodies: canonical bytes, the Java↔TypeScript pins, unknown keys, the bounds | `RtcSignalTest` |
| Two bindings over the real engine open, attest, and exchange chunked frames | `LibDataChannelEngineTest` |
| The `sig` endpoint: a send never dials it; the tick dials it with the member's PeerID; a dialer without the binding skips it | `WebRtcEndpointTest` in `agentspaces-peering` |
| The relay offload: the threshold trips the dial from the tick on a recording binding; the backoff holds for a minute of heavy traffic and lifts after `retry-after`; the settings' validation; below the threshold nothing dials | `WebRtcEndpointTest` in `agentspaces-peering`, `RelayOffloadTest` |
| With a fake engine, thirty-one frames cross the seed, the offload opens the channel, and the seed's per-target counters for the pair stop rising in both directions | `RelayOffloadTest` |
| The engine never opens: `END(2)` at the dial bound, the frames sent before, during, and after all arrive by relay, no second dial inside the backoff, one after it | `WebRtcFallbackTest` |
| The relayed-signal exception under required attestation | `RequiredAttestationRelayTest` in `agentspaces-peering` |
| Two members behind a RELAY seed: the offload opens a channel over the real engine, and the relay then carries none of their frames | `WebRtcFleetTest` (opt-in, `-Dwebrtc.fleet=true`) |
| The Spring Boot wiring of `agentspaces.transport.webrtc.*` | `WebRtcBlockAutoConfigurationTest` in `agentspaces-spring-boot-autoconfigure` |
| Two headless browsers over a Java RELAY seed | `agentspaces-typescript/packages/webrtc/test/two-browser.test.ts` |

## Notes

- The engine thread model matters: libdatachannel delivers a binary message as a transient native
  buffer valid only during the synchronous callback, so the engine copies it there and hands the
  copy to a per-session worker; a deferring executor would read freed memory.
- DTLS 1.3 negotiates only where the engine offers it; libdatachannel 0.24 offers 1.2, so the floor
  (AEAD ECDHE) holds. A pure-Java engine over the `dtls` module's handshake driver is the
  planned second engine.
