# agentspaces-common

The foundations every layer shares. No AgentSpaces dependencies: this is the bottom of the
stack, and everything above it — identity, peering, the space, the capabilities, both
non-JVM clients' Java twin — agrees on bytes because it agrees on what is here.

**Architecture position.** Foundations (shared by every layer). SPEC §4 (identifiers),
§9 (canonical encoding), §11 (the bounded-drift clock); TECH-SPEC §1.

## Key files

| Package | What it is |
| --- | --- |
| `common.codec` | `CborCodec`: the canonical CBOR codec (RFC 8949, definite-length arrays since wire version 2, fields in declared record order, `FAIL_ON_UNKNOWN_PROPERTIES` off so appended fields are ignored by older decoders). Every signature in the system is over bytes this codec produces, which is why the golden vectors exist. `Base58`/`Multibase` for identifiers. |
| `common.crypto` | `Ed25519` (RFC 8032 sign/verify over raw 32-byte keys, PKCS#8 loading), `X25519` + `Hkdf` + `GroupKey`/`GroupKeyWrap` (AES-256-GCM group content keys with the 2^32 seal ceiling, sealed key wrap for `key-wrap`), `Digests` (SHA-256), and the `SignatureProvider` seam (`JdkSignatureProvider` default) for an HSM-backed signer. |
| `common.hlc` | `HybridLogicalClock` / `HlcTimestamp`: the near-wall-clock timestamp with a deterministic total order and the drift clamp (a remote stamp cannot pull a node more than ten minutes ahead), the basis of take arbitration. Encoded as `physical:logical:node`. |
| `common.id` | `PeerId` (multibase of SHA-256 over the raw public key), `GroupId` and `SpaceId` (hashes of founding documents; `SpaceId.local(name)` for unadvertised spaces), `AgentId` (`peer/localName`), and the `aspace://` URI scheme. |

## Key dependencies

Jackson (`jackson-databind`, `jackson-dataformat-cbor`, `jackson-datatype-jsr310`) and the
JDK's Ed25519/X25519 providers. A Jackson bump can change canonical bytes: regenerate the golden
vectors (`docs/BUILD-ENVIRONMENTS.md`) and keep all three suites green before it lands.

## Notes

- `IdsTest` pins the PeerID/GroupID/SpaceID derivations against `tools/golden/golden.json`.
- `signature-vectors.json` in the tests are RFC 8032 known-answer tests for the Ed25519 helper.
- Nothing here knows about the network, the space, or Spring; keep it that way.
