# Extending AgentSpaces

Written for: developers who extend the core with a capability, a transport, or a host.

The core stays small. The extension points are interfaces in `agentspaces-api` and a few well-marked interfaces in the layer modules. Three kinds of extension come up most often.

## Adding a transport

A transport is anything that can carry length-prefixed byte frames between peers. The SPI is two interfaces in `agentspaces-api`:

```java
public interface Transport {
    String scheme();                                   // "tcp", "tls", "quic", "ws", "mem"
    TransportConnection dial(String address) throws IOException;
    AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException;
}

public interface TransportConnection extends AutoCloseable {
    String remoteAddress();
    void send(byte[] frame) throws IOException;
    void onReceive(Consumer<byte[]> receiver);
    default Optional<PeerId> attestedPeer() { return Optional.empty(); }
    void close();
}
```

Everything above the transport is unchanged by the choice, and the abstraction must stay that way: envelopes stay signed CBOR, membership and gossip run the same, and the space never knows. The QUIC module is the example to follow. `QuicTransport` maps one bidirectional QUIC stream to one connection, frames the same bytes the TCP transport frames, and returns `"quic"` from `scheme()`.

Two design points carry over to any new binding:

1. **Peer identity does not live in the transport.** A transport may encrypt, but the peer's identity is proven by the signed envelope layer over every transport. `attestedPeer()` is optional: return the PeerID only when the handshake itself authenticated the remote end, as the TLS and QUIC bindings do with identity-endorsed channel certificates (SPEC §5.6). Then, and only then, the node may elide per-frame signatures toward that peer. Returning empty is always safe and keeps every frame fully signed.
2. **Dialers pick by scheme and priority.** A peer advertises its endpoints as `(scheme, address, priority)` in its PeerAdvertisement. Register a listener with `node.listen(transport, bindAddress, priority)` and the node advertises it; a dial-only peer registers with `node.transport(transport)`. Dialers try a member's endpoints in ascending priority and skip schemes they lack, so a fleet can run mixed transports and migrate between them by changing priorities.

Practical notes:

- Frames are capped at 8 MiB. Refuse larger frames, and treat a corrupt length prefix as a dead connection, since the stream cannot resynchronize.
- Test a new binding with the pattern in `TcpTransportTest` and `TlsFabricTest`: two real `PeerNode`s over the transport, converging membership, then a frame round trip.
- Under the Spring starter the security profile selects TCP or TLS. A custom transport is registered programmatically on the node today.
- WebSocket is the next binding the specification names.

## Customizing security providers

Security decisions sit behind interfaces so an organization can align its own identity and policy systems with the fabric without forking it. Each seam answers one question.

**Who may join a group?** `MembershipValidator` in `agentspaces-api`.

- Decides admission for `POLICY` groups: `admit(candidate, groupAdvertisement, credentials)`.
- Plug in a DID resolver, an LDAP lookup, or a verifiable-credential check. The node calls it at admission and refuses everyone it rejects.
- `OPEN` and `INVITE` (founder-signed credentials bound to the PeerID) need no code.

**Who may perform a privileged operation?** `Authorizer` in `agentspaces-api`.

- Answers `permits(peer, operation, scope)` and, per agent, `permits(agent, operation, scope)`, with a `granularity(operation, scope)` of `PEER` or `AGENT`.
- Guards `RAFT_VOTER`, `DIRECTIVE_ISSUER`, `CONNECTOR_SERVE`, `KEY_HOLDER`, `SPACE_WRITE`, `SPACE_TAKE`, `VOTE`, `MODEL_SERVE`, and `KEY_ROTATE`. `KEY_ROTATE`, unlike the others, permits nobody without an explicit grant.
- `MembershipAuthorizer` roots decisions in group membership with optional grants that name PeerIds or `peer/agent` AgentIds. `OidcAuthorizer` in `agentspaces-auth-oidc` roots them in JWT scopes from your identity provider.
- Consumers that count identities (the QUORUM tally) count at the granularity the authorizer answers at, so counting and authorization never disagree.
- Implement the interface to consult your own policy engine. Keep it fast (it sits on dispatch paths; cache behind it) and select it with the `SecurityProfile`.

**How is the channel authenticated?** `ChannelTrust` and the `MTLS` profiles.

- The `MTLS` profiles present identity-endorsed channel certificates.
- For an enterprise CA, `ChannelTrust.of(anchors)` or `ChannelTrust.withCrls(anchors, crls)` (optionally `strictOnline()` for OCSP) attests only certificates that chain to your anchors and are not revoked.
- `TlsTcpTransport.withRefreshableTrust(...)` lets you install a new CRL without a restart.
- `agentspaces.transport.tls.require-attestation` (or `PeerNode.Builder.requireAttestation(true)` when you assemble the node yourself) makes attestation a condition of admission and evicts a member whose next handshake attests nothing, so your CA's revocation is the fleet's eject. The node registers TLS alone in that mode, since a plaintext channel attests nobody.

**Who decides a revocation is authoritative?** `RevocationRegistry.RevocationValidator`.

- Judges a gossiped `RevocationAdvertisement`. The default is founder-rooted.
- Set `PeerNode.Builder.revocationValidator(...)` to accept revocations from your identity provider's signing key instead.
- In enterprise-CA mode, `agentspaces.transport.tls.revocation-validator: founder-or-ca` (default `founder`; it needs a `trust-store`) also accepts a revocation from any member whose evidence validates to your CA. The evidence is the revoked peer's chain and the CRL that lists its leaf for an authorizing reason.
- The node roots a revocation itself when its CRLs, re-read every `crl-refresh` (5m), revoke a connected peer.

**Who may read a space?** Content keys and space admission.

- Group content keys encrypt payloads. The `key-wrap` capability seals the key per member under a policy you supply (`GroupKeyDistributor.serve(key, Predicate<PeerId>)`, or the membership default).
- Writers and takers are admitted by group, by an AgentId allowlist, by leased `SpaceCredential` entries the space's issuer writes and revokes, or by the profile's `Authorizer` (`ReplicatedSpace.Builder.admission(...)`).
- Admission is judged for the acting agent, so two agents of one peer sharing a replica through views can be admitted differently.

**How is meaning matched?** `Embedder` in `agentspaces-api`.

- Backs semantic discovery. The shipped `HashingEmbedder` needs no model.
- Swap in your own embedding service and the protocol does not change.

All of these are ordinary constructor or builder arguments on `PeerNode`, `ReplicatedSpace`, and the capability classes. The Spring starter exposes the common choices as properties under `agentspaces.security.*`, `agentspaces.transport.tls.*`, and `agentspaces.groups[].spaces[].admission`.

## Building a new capability (Layer 4)

A capability is any protocol beyond the core that a peer offers its group. The pattern is uniform and the six shipped capabilities are its reference implementations. Four steps:

1. **Mint a type URI and write the mini-spec.** Capability types look like `aspace:cap/vote`; third parties choose their own namespace. The mini-spec is the interaction itself: what a request looks like, what an answer looks like, and what the advertisement's `parameters` promise.
2. **Implement `CapabilityProvider`.**

   ```java
   public interface CapabilityProvider {
       String capabilityType();                          // "acme:cap/negotiate"
       CapabilityAdvertisement describe(GroupId group);  // type, version, binding, parameters, cost hints, TTL
       default void start() {}
       default void stop() {}
       default void tick() {}                            // one protocol step, driven by the peer's clock
       default boolean requiresTick() { return false; }  // true when the protocol has rounds to run
       default void driverCadence(Duration period) {}    // told the real cadence, so advertised leases are truthful
   }
   ```

   - `describe` returns the advertisement the runtime signs and publishes. The runtime refreshes it on its schedule, so a provider that stops refreshing simply ages out of every cache. Everything is leased.
   - A provider with rounds to run declares `requiresTick()` and does one step in `tick()`. The `CapabilityRuntime` registers itself on the peer's one clock and drives every provider from it, so a capability never brings a scheduler of its own.
3. **Choose a binding.**
   - **Space-mediated** is the canonical style: requests and responses flow as leased entries in an agreed space, which gives retries, observability, and choreography with other capabilities at no extra cost. `vote`'s QUORUM mode and the connector SDK's `data-query` work this way.
   - **Direct pipe** suits high-rate protocols: `CapabilityPipes.onCapability(type, handler)` receives frames addressed to your type and `send(peer, type, payload)` answers them over the `PIPE_DATA` kind. `aggregate`, `gossip-learn`, `semantic-discovery`, and `key-wrap` work this way.
   - Both bindings see only authenticated, admitted peers. The node has already verified the envelope and applied membership, rate limits, and revocation before your handler runs.
4. **Register and consume.**
   - Programmatically: `capabilityRuntime.register(provider)` on the group's `CapabilityRuntime`. Consumers find providers with `providersOf(type)` through the ad-cache.
   - Under the `AgentSpaces` facade or the Spring starter: annotate the provider bean with `@ProvidesCapability("acme:cap/negotiate")` and it is registered and advertised in its groups.
   - For a typed client: implement `CapabilityClientFactory<C>` (`clientType()`, `create(groupContext)`) and register it through `ServiceLoader`. `spaces.group("fleet").capability(NegotiateClient.class)` then resolves it, as `VoteClient`, `AggregateClient`, and `SemanticClient` do today, and an agent can receive it by field with `@CapabilityRef`.
   - A capability with a behaviour agents exhibit (cast, react, take) earns an annotation of the `@SpaceNotify` shape. `LAYER4-ANNOTATIONS.md` is the review that produced `@Ballot`, `@OnDecision`, and `@OrderedTake`, and the bar a new one must clear.

Test a capability the way the shipped ones are tested: a `SimNetwork` cluster with a `TestClock`, providers registered on two or more peers, and assertions on convergence after deterministic ticks (`CapabilityClusterTest` is the template). Consider the security posture as you design: an unauthenticated average can be poisoned by any participant (the specification accepts this for `aggregate` and says so), whereas signed entries in a space are attributable. Say which one your mini-spec offers.
