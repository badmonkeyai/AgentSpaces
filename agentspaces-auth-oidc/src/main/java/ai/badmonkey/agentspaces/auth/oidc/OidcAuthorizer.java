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
package ai.badmonkey.agentspaces.auth.oidc;

import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.PeerId;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import java.net.URL;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * The enterprise {@link Authorizer} (security remediation plan §6, the
 * {@code mtls-oidc} posture): privileged-operation policy lives in the
 * organization's identity provider, carried as OAuth2 scopes in a JWT whose
 * subject binding names the peer it authorizes. The fabric only enforces the
 * token — validated once with Nimbus JOSE+JWT against the issuer's JWKS, then
 * cached until it expires — so who may lead Raft or issue directives is
 * administered where the organization already administers everything else.
 *
 * <p>Token shape: signed JWT from the configured issuer, for the configured
 * audience, with an {@code agentspaces_peer} claim equal to the authorized
 * peer's id (a token cannot be replayed for a different peer) and a standard
 * space-delimited {@code scope} claim granting operations:
 * {@code aspace:raft-voter}, {@code aspace:directive-issuer},
 * {@code aspace:connector-serve}, {@code aspace:key-holder} — fleet-wide — or
 * {@code aspace:<operation>:<scope>} for one group or space.
 *
 * <p>Offline tolerance (plan §1.3): the JWKS is cached, so validating
 * already-seen keys survives an identity-provider partition; reaching the IdP
 * is needed to mint new trust, not to use existing trust.
 */
public final class OidcAuthorizer implements Authorizer {

    /** The scope prefix carrying AgentSpaces operations. */
    public static final String SCOPE_PREFIX = "aspace:";

    private record Granted(Instant expiresAt, Set<String> scopes) {
    }

    private final String issuer;
    private final String audience;
    private final DefaultJWTProcessor<SecurityContext> processor;
    private final InstantSource clock;
    private final Cache<PeerId, Granted> granted;

    /**
     * Creates the authorizer over an explicit key source (tests, pre-fetched
     * JWKS documents, custom caching).
     *
     * @param issuer   the token issuer this fleet trusts, matched exactly
     * @param audience the audience value tokens must carry
     * @param keys     the issuer's verification keys
     * @param clock    the time source for expiry decisions
     */
    public OidcAuthorizer(String issuer, String audience,
                          JWKSource<SecurityContext> keys, InstantSource clock) {
        this.issuer = Objects.requireNonNull(issuer, "issuer");
        this.audience = Objects.requireNonNull(audience, "audience");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.processor = JwtProcessors.processor(issuer, audience, keys,
                Set.of("exp", "sub", "scope", "agentspaces_peer"));
        this.granted = Caffeine.newBuilder().maximumSize(10_000).build();
    }

    /**
     * Creates the authorizer over a remote JWKS endpoint, with Nimbus's
     * built-in caching and outage tolerance so an unreachable identity
     * provider does not invalidate already-validated keys.
     *
     * @param issuer   the token issuer this fleet trusts, matched exactly
     * @param audience the audience value tokens must carry
     * @param jwksUrl  the issuer's JWKS document
     * @param clock    the time source for expiry decisions
     * @return the authorizer
     */
    public static OidcAuthorizer fromJwks(String issuer, String audience,
                                          URL jwksUrl, InstantSource clock) {
        return new OidcAuthorizer(issuer, audience, JwtProcessors.remoteJwks(jwksUrl), clock);
    }

    /**
     * Ingests one peer's token: validated (signature against the issuer's
     * keys, issuer, audience, expiry, and the {@code agentspaces_peer}
     * binding), then cached until it expires. Authorization questions are
     * answered from the cache; a peer without a live validated token is
     * permitted nothing.
     *
     * @param peer the peer this token claims to authorize
     * @param jwt  the compact-serialized JWT
     * @return whether the token validated and now backs the peer's grants
     */
    public boolean authorize(PeerId peer, String jwt) {
        Objects.requireNonNull(peer, "peer");
        Objects.requireNonNull(jwt, "jwt");
        JWTClaimsSet claims;
        try {
            claims = processor.process(jwt, null);
        } catch (Exception e) {
            return false; // bad signature, wrong issuer/audience, expired, malformed
        }
        try {
            if (!peer.value().equals(claims.getStringClaim("agentspaces_peer"))) {
                return false; // a token authorizes exactly the peer it names
            }
            Instant expires = claims.getExpirationTime().toInstant();
            Set<String> scopes = Set.of(claims.getStringClaim("scope").split(" "));
            granted.put(peer, new Granted(expires, scopes));
            return true;
        } catch (java.text.ParseException e) {
            return false;
        }
    }

    @Override
    public boolean permits(PeerId peer, Operation operation, String scope) {
        Objects.requireNonNull(peer, "peer");
        Objects.requireNonNull(operation, "operation");
        Granted current = granted.getIfPresent(peer);
        if (current == null) {
            return false;
        }
        if (!clock.instant().isBefore(current.expiresAt())) {
            granted.invalidate(peer);
            return false; // the token lapsed; a fresh one must be presented
        }
        String name = SCOPE_PREFIX
                + operation.name().toLowerCase(Locale.ROOT).replace('_', '-');
        return current.scopes().contains(name)
                || (scope != null && !scope.isEmpty()
                        && current.scopes().contains(name + ":" + scope));
    }

    /** The issuer this authorizer trusts. */
    public String issuer() {
        return issuer;
    }

    /** The audience tokens must carry. */
    public String audience() {
        return audience;
    }
}
