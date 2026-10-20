# agentspaces-identity-tls

The pieces every attesting transport binding shares (SPEC §5.6): `SingleKeyManager`, which serves
the channel credential and nothing else; `AttestLaterTrustManager`, which accepts any certificate
at the TLS layer because the trust decision comes after the handshake; and `ChannelAttestation`,
the one call that turns a presented chain into an attested PeerID, in the identity-endorsed mode
or under a CA trust read afresh on every call.

**Architecture position.** Layer 0, Identity (a helper for the Layer 1 bindings). Used by `tls`
(`agentspaces-peering`) and `dtls` (`agentspaces-transport-dtls`), and planned for `wss`. One copy,
so a fix to the attestation path lands everywhere at once (SRAD `srad/dtls-support.md` DT-A-05).

## Key dependencies

`agentspaces-identity`, `agentspaces-common`; the JDK. Tested by `ChannelAttestationTest` and,
through the bindings, by every TLS and DTLS attestation test.
