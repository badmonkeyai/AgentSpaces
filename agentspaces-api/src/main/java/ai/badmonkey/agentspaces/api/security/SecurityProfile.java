/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.api.security;

import java.util.Locale;

/**
 * One named security posture for a whole deployment (security remediation plan
 * §4): an operator selects a profile and gets a coherent combination of
 * transport, channel authentication, and authorization, rather than assembling
 * individual flags and hoping they compose safely. The shipped default is
 * {@link #MTLS} — secure by default — and the insecure mode exists only as the
 * clearly-labelled {@link #DEV_LOCAL} choice.
 *
 * <p>The provenance caveat, stated once: TLS secures a point-to-point link,
 * while the fabric is multi-hop gossip. Every profile therefore keeps the
 * <em>originator</em> signatures on entries, claims, advertisements, and rumor
 * items — the provenance layer that makes gossip trustworthy across hops. What
 * a profile may drop is only the per-hop <em>envelope</em> re-signature on
 * links whose transport already authenticated the peer; {@link #ZERO_TRUST}
 * keeps even that, for relays and boundaries where the intermediary is not
 * trusted to have authenticated the origin.
 */
public enum SecurityProfile {

    /**
     * A single host, a trusted VLAN or IPSEC segment, or tests: plaintext TCP,
     * every frame envelope-signed, membership-rooted authorization. The network
     * itself is the boundary; never expose this profile beyond one.
     */
    DEV_LOCAL(false, true, false),

    /**
     * The shipped default: mutual TLS with identity-endorsed channel
     * certificates, envelope signatures elided on attested links (the
     * originator signatures always remain), membership-rooted authorization.
     */
    MTLS(true, false, false),

    /**
     * {@link #MTLS} plus authorization deferred to the organization's identity
     * provider: privileged operations require JWT scopes (the
     * {@code OidcAuthorizer} in {@code agentspaces-auth-oidc}).
     */
    MTLS_OIDC(true, false, true),

    /**
     * Untrusted intermediaries or cross-organization federation: mutual TLS,
     * OIDC authorization, and the per-frame envelope signature kept on every
     * hop, so provenance survives relays that were never trusted to
     * authenticate the origin.
     */
    ZERO_TRUST(true, true, true);

    private final boolean tlsTransport;
    private final boolean envelopeSignatureEveryHop;
    private final boolean oidcAuthorization;

    SecurityProfile(boolean tlsTransport, boolean envelopeSignatureEveryHop,
                    boolean oidcAuthorization) {
        this.tlsTransport = tlsTransport;
        this.envelopeSignatureEveryHop = envelopeSignatureEveryHop;
        this.oidcAuthorization = oidcAuthorization;
    }

    /** Whether the profile's transport is mutual TLS with channel attestation. */
    public boolean tlsTransport() {
        return tlsTransport;
    }

    /**
     * Whether every frame keeps its envelope signature even on links the
     * transport authenticated. When false, attested links negotiate bare
     * frames; content-provenance signatures are unaffected either way.
     */
    public boolean envelopeSignatureEveryHop() {
        return envelopeSignatureEveryHop;
    }

    /** Whether privileged operations defer to identity-provider tokens. */
    public boolean oidcAuthorization() {
        return oidcAuthorization;
    }

    /**
     * Parses the configuration form ({@code mtls}, {@code dev-local},
     * {@code mtls-oidc}, {@code zero-trust}).
     *
     * @param name the configured profile name
     * @return the profile
     * @throws IllegalArgumentException for an unknown name, listing the choices
     */
    public static SecurityProfile fromName(String name) {
        try {
            return valueOf(name.trim().replace('-', '_').toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown security profile '" + name
                    + "'; choose one of: dev-local, mtls, mtls-oidc, zero-trust");
        }
    }
}
