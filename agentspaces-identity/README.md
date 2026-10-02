# agentspaces-identity

Layer 0: who a peer is, and who an agent is. The private key never leaves `PeerIdentity`;
every other component asks it to sign.

**Architecture position.** Layer 0, Identity. SPEC §4, §5.6 (channel authentication), §4.2
(subordinate agent keys); TECH-SPEC §4 (the signature inventory).

## Key files

| File | What it is |
| --- | --- |
| `PeerIdentity` | The Ed25519 peer keypair and its self-certifying `PeerId`; `sign`, `rawPublicKey`, `agent(name)`, and the agent-identity factories: `agentIdentity(name)` (peer-signed, today's default) and `subordinate(name[, keys][, issued, ttl])` (a fresh or supplied agent key certified by this peer). |
| `FileKeystore` | Persists the peer key (`peer.key`, PKCS#8) so the PeerId survives restarts, plus the peer's X25519 key (`encryptionKeys`); loads are checked by a sign/verify probe and half-written pairs are refused. |
| `AgentKeystore`, `KeyFiles` | Persisted agent keys at `<dir>/<peer-id>/<agent>/agent.key` (and `agent-x25519.key`), atomic owner-only writes, names that could escape refused (v0.1.13). |
| `RenewingAgentIdentity` | A subordinate key whose certificate re-issues at half-life, retaining 256 earlier certificates; built by `PeerIdentity.renewingSubordinate` (v0.1.13). |
| `TrustStatus`, `ReloadingChannelTrust` | `ChannelTrust.status(chain, at[, crls])` says why a chain does or does not attest (revoked with its CRL reason, stale, expired, ...); the reloading supplier re-reads CRL files on a cadence (v0.1.13). |
| `AgentCertificates` | `sign(body, peer)` and `verify(certificate, peerKey, expectedAgent, now)`: the peer's certification of an agent key, canonical CBOR over the certificate minus its signature, valid until `issued + ttl` exclusive. The two-key rule for records and claim proofs rests on this. |
| `PeerSignedAgentIdentity`, `SubordinateAgentIdentity` | The two `AgentIdentity` implementations, package-private behind the factories. |
| `AdvertisementSigner` | Signs and verifies every advertisement type into its `SignedAdvertisement`. |
| `ChannelCertificate`, `ChannelTrust`, `ServingCredential` | Identity-endorsed TLS/QUIC channel certificates, CA-rooted trust with CRL/OCSP checking (`ChannelTrust.of/withCrls/strictOnline`), and the credential a listener serves. |

## Key dependencies

`agentspaces-api`, `agentspaces-common`, BouncyCastle (`bcpkix`/`bcprov` for certificate handling).

## Notes

- Since v0.1.13 a certificate is judged at the signing time of what it certifies, not at
  receipt: `AgentCertificates.verifyAt(cert, peerKey, agent, signingTime, receiverNow)`, with
  the signing time allowed to lead the receiver by at most the HLC drift ceiling. Certificates
  may also certify an X25519 `encryptionPublicKey` for per-agent key wrap.

- `AgentCertificateTest` is the contract-first suite for subordinate keys (wrong peer, wrong
  agent, expired, tampered, distinct keys per call).
- A subordinate identity is one per space handle unless the space is seen through a view
  (`ReplicatedSpace.as(identity)`), which is how several agents on one peer each sign.
