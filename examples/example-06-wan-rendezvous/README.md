# Example 06: WAN Rendezvous

Two sites form one fleet. The rendezvous peer at headquarters holds the only
address anyone is configured with. Peers from both sites join through it,
membership and discovery converge across sites, and remote queries prefer
rendezvous peers (SPEC §5.4, §6.3). The shared task space spans every peer, so
work written at one site completes at the other.

The earlier examples ran every peer on one LAN and seeded each peer from its
neighbor. This example introduces peer roles: one peer advertises
`PeerRole.RENDEZVOUS`, carries a discovery cache four times larger than the
default, and acts as the well-known entry point that a WAN deployment needs.

Narrative walk-through: [examples guide, example 06](../examples-guide.html#ex-06).

## What it demonstrates

- **One well-known address.** Each site peer seeds only from the rendezvous;
  SWIM-style gossip then teaches every peer about every other peer, at both sites.
- **Roles in the membership view.** `membership().withRole(PeerRole.RENDEZVOUS)`
  lets any peer find the rendezvous peers it knows, and discovery routes remote
  queries through them first.
- **Cross-site discovery.** Site A publishes a signed AgentCard for its analyst,
  and site B finds it through its own cache.
- **Cross-site work.** Site B writes a `SiteTask`, a site A worker takes and
  completes it, and site B reads the `SiteResult`.

## The API in this example

The role is a builder option on the node:

```java
PeerNode node = PeerNode.builder(identity)
        .roles(rendezvous ? Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS) : Set.of())
        .build();
```

Finding the rendezvous peers:

```java
List<PeerId> rendezvous = siteB.runtime().membership()
        .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS);
```

Publishing a card by hand, signed with the peer's key (examples 03 and 11 let
the binder do this):

```java
AgentCard card = new AgentCard(uri, peerId, groupId, Instant.now(), Duration.ofMinutes(15),
        identity.agent("analyst"), "Analyzes cross-site workloads", List.of("analyze"),
        List.of(), List.of(SiteResult.class.getName() + "#v1"), Map.of());
siteA.discovery().publish(new AdvertisementSigner().sign(card, identity));
```

| Type or call | Module | Role here |
| --- | --- | --- |
| `PeerNode.Builder.roles(Set<PeerRole>)` | `agentspaces-peering` | Declares RENDEZVOUS (or RELAY) in the peer's advertisement |
| `GroupMembership.withRole(role)`, `allMembers()` | `agentspaces-peering` | The membership view, filtered by role |
| `AgentCard`, `AdvertisementSigner` | `agentspaces-api`, `agentspaces-identity` | A hand-built, signed card |
| `DiscoveryService.publish`, `find` | `agentspaces-discovery` | Cross-site discovery through the ad-cache |

Source: [WanFleet.java](src/main/java/ai/badmonkey/agentspaces/examples/wan/WanFleet.java).

## Running it

From the `agentspaces/` repository root (after one
`mvn install -DskipTests`), five peers on
ports 7491 to 7495, with 7491 as the rendezvous:

```
mvn -q -pl examples/example-06-wan-rendezvous exec:java
```

Every peer runs on localhost, so the "sites" are logical. To try a real WAN,
run the rendezvous on a reachable host and point each site's seed endpoint at
it; nothing else changes.

[WanFleetFlowTest](src/test/java/ai/badmonkey/agentspaces/examples/wan/WanFleetFlowTest.java)
asserts that a site peer learns the full membership and exactly one rendezvous
through the one seed, and that a task written at site B completes at site A:

```
mvn -q -pl examples/example-06-wan-rendezvous test
```

## Next steps

- **Cross NAT.** Peers behind restrictive NAT register a peer with the RELAY
  role and reach the fleet through relay forwarding in `agentspaces-peering`.
  SPEC §5.4 describes both roles.
- **Encrypt payloads between sites.** A group content key encrypts entry
  payloads end to end, and the `key-wrap` capability (X25519, HKDF, AES-GCM)
  distributes that key to the members the authorizer names. Under Spring Boot,
  set `agentspaces.groups[].content-key`, and grant holders with
  `agentspaces.security.grants.key-holder`.
- **Harden the links.** Use TLS 1.3 with identity-endorsed channel certificates,
  or QUIC (RFC 9000) through `agentspaces-transport-quic`, and choose the `MTLS`
  or `ZERO_TRUST` security profile. The
  [starter README](../../agentspaces-spring-boot-starter/README.md#security-profile-and-authorization)
  lists the properties.
- **Skip seeds on a LAN.** Within one site, the multicast bootstrap beacon
  (`agentspaces.multicast.enabled=true`) lets peers find each other with no seed
  list at all.
- **Spring Boot.** `agentspaces.roles: [RENDEZVOUS]` makes a node a rendezvous,
  and `agentspaces.groups[].seeds` lists the rendezvous addresses on every other
  node.
- **Join by GroupID alone.** `agentspaces.groups[].join` lets a newcomer join a
  self-certifying group by its GroupID, verifying the founding document a seed
  hands it; the Python and TypeScript clients join the same way.
