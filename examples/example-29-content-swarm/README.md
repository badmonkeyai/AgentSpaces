# Example 29: a large artifact across a fleet

A trainer publishes a checkpoint of tens of megabytes, three workers fetch it
over TCP from every holder at once, the trainer is killed part way through, and
the workers complete each other from the pieces they already hold. A bridge
peer, a member of the fleet's group and of a second group, then shares the same
artifact into the second group from one store, and a member there fetches it
from the bridge.

The capability is `aspace:cap/content-swarm` (`agentspaces-content`,
SPEC annex G): the space replicates a small signed
manifest and a leased holding per member, and the bytes move on capability pipes
over whatever transport sessions exist. The demo runs every peer on `tcp`; the
swarm never names a binding, and `ContentSwarmFleet.start` takes a map of
listeners (`tcp`, `tls`, `dtls`, `quic`, `ws`, `http`) and seeds written as
`scheme://host:port`.

```
cd examples/example-29-content-swarm
mvn -q exec:java
```

The run prints each worker's progress, the moment the trainer is killed, the
sources each worker drew from, and the bridge's share into the second group.
`ContentSwarmFlowTest` is the same fleet under the gate on free loopback ports.


## The tests

- `ContentSwarmFlowTest`: the demo's fleet on free loopback ports. The trainer
  answers one range at a time, 10 ms apart, as a slow link would; over loopback
  the workers could otherwise each take the whole artifact from it before it is
  killed.
- `ContentBindingsTest`: two peers move an artifact on each of `tcp`, `tls`,
  `dtls`, `quic`, `ws`, and `http`; then a mixed fleet, a trainer listening on
  `tcp`, `tls`, `dtls`, and `ws` with one JVM worker on each of the first
  three, and a Node peer (the TypeScript client's `content-worker`) joining on
  `ws`, fetching the checkpoint, and publishing an artifact the JVM peers fetch
  back. The Node half needs `npm run build` in `agentspaces-typescript`; it is
  skipped without it unless `-Dtsclient.required=true`. The mixed fleet also
  asserts that the busiest source's window stays at least half full and that no
  peer suspects another while pieces flow.
- `ContentRelaySitesTest`: sites behind relays, over real sockets. Two
  dial-only kits sit behind their own site relays, and both relays attach to a
  headquarters relay. Loopback cannot hide a listener, so the sites are kept
  apart by binding: site A speaks `ws`, site B `tls`, and the relays reach
  headquarters on `tcp`. A kit can dial only its own relay, so 6 MiB crosses
  three relays on a pinned path, and each relay counts every piece.
- `WorkersR2HolderTest`: a Workers cell's `R2PieceStore`, run under Node over a
  bucket that answers ranged gets as R2 does, joins a Java hub on `ws` and
  publishes 3 MiB; a Java member on `tcp` fetches every byte through the hub.
  It needs `npm run build` in `agentspaces-typescript/packages/workers`.

A peer started with no listeners is dial-only: `ContentSwarmFleet.start` adds a
binding for each seed's scheme, so it can dial out but no one can dial it.

## Manual recipes

Two cases need a real network, so they are recipes rather than tests.

### Two sites behind NAT, two relays

Each site is a private network that cannot be dialled from outside: two Docker
networks on one host, two VMs, or two routers. Put one relay on each site with a
second interface on a shared network, and one worker per site with only its
site's interface.

```
# shared network: hq-net; site networks: site-a, site-b
docker network create hq-net && docker network create site-a && docker network create site-b
# relays listen on both their site and hq-net, with the RELAY role
#   relay-a: listeners tcp 0.0.0.0:7701, roles [RELAY], seeds tcp://relay-b:7701
#   relay-b: listeners tcp 0.0.0.0:7701, roles [RELAY]
# workers listen nowhere (dial-only) and seed their own site's relay
#   worker-a: seeds tcp://relay-a:7701      worker-b: seeds tcp://relay-b:7701
```

Publish on `worker-b` and fetch on `worker-a`. Expect `worker-a`'s fetch trace to
show the source at two relay hops, `relayBytesByKind` on both relays to show
`PIPE_DATA:aspace:cap/content-swarm`, and the pinned path to stay the same for
the whole transfer. Restart `relay-b` part way through: the fetch stalls for
under a minute, repins, and completes without re-requesting a verified piece.

### A lossy DTLS link with `tc netem`

On Linux, between two hosts (or two network namespaces) running one peer each on
`dtls`:

```
sudo tc qdisc add dev eth0 root netem loss 10% delay 80ms 20ms rate 2mbit
# publish with 256 KiB pieces on the sender:  PublishOptions.pieceSizeFor(dtls capabilities)
# fetch on the receiver and watch the trace; afterwards
sudo tc qdisc del dev eth0 root
```

Expect the source's window to halve on timeouts and recover, ranges to shrink
toward 64 KiB under loss, no SWIM suspicion of either peer while pieces flow
(a lost range costs one range, not a block), and the fetch to finish within the
holding lease once the link settles.
