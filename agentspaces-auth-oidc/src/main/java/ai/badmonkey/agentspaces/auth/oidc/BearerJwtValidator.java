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

import ai.badmonkey.agentspaces.api.security.BearerAuthenticator;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@link BearerAuthenticator} for the {@code mtls-oidc} posture (TODO item
 * 7, TODO-EFG §5): the fleet console and the A2A gateway become OAuth2
 * resource servers, validating an identity-provider JWT on every request
 * instead of comparing a shared string. The token's {@code sub} becomes the
 * audited operator and its space-delimited {@code scope} claim the principal's
 * scopes, so a surface can demand {@code aspace:console:operate} or
 * {@code aspace:a2a:client} and the identity provider administers who holds it.
 *
 * <p>Same issuer, audience, accepted algorithms, and JWKS handling as
 * {@link OidcAuthorizer}; the difference is the required claims. Operators are
 * humans (or service accounts) rather than fabric peers, so
 * {@code agentspaces_peer} is optional here, while {@code exp}, {@code sub},
 * and a non-blank {@code scope} are required. Nothing is cached per call
 * beyond Nimbus's own JWKS cache: revoking a token's holder at the IdP ends
 * their access no later than the token's expiry.
 */
public final class BearerJwtValidator implements BearerAuthenticator {

    private final String issuer;
    private final String audience;
    private final DefaultJWTProcessor<SecurityContext> processor;

    /**
     * Creates the validator over an explicit key source (tests, pre-fetched
     * JWKS documents, custom caching).
     *
     * @param issuer   the token issuer this surface trusts, matched exactly
     * @param audience the audience value tokens must carry
     * @param keys     the issuer's verification keys
     */
    public BearerJwtValidator(String issuer, String audience, JWKSource<SecurityContext> keys) {
        this.issuer = Objects.requireNonNull(issuer, "issuer");
        this.audience = Objects.requireNonNull(audience, "audience");
        this.processor = JwtProcessors.processor(issuer, audience, keys,
                Set.of("exp", "sub", "scope"));
    }

    /**
     * Creates the validator over a remote JWKS endpoint, with Nimbus's caching
     * and outage tolerance, mirroring {@link OidcAuthorizer#fromJwks}.
     *
     * @param issuer   the token issuer this surface trusts, matched exactly
     * @param audience the audience value tokens must carry
     * @param jwksUrl  the issuer's JWKS document
     * @return the validator
     */
    public static BearerJwtValidator fromJwks(String issuer, String audience, URL jwksUrl) {
        return new BearerJwtValidator(issuer, audience, JwtProcessors.remoteJwks(jwksUrl));
    }

    /**
     * Validates one compact-serialized JWT: signature against the issuer's
     * keys, issuer, audience, expiry, and the required claims. Any failure,
     * including a token that is not a JWT at all or one whose {@code scope}
     * claim is blank, yields empty; nothing is thrown for a bad token.
     */
    @Override
    public Optional<Principal> authenticate(String bearerToken) {
        Objects.requireNonNull(bearerToken, "bearerToken");
        try {
            JWTClaimsSet claims = processor.process(bearerToken, null);
            String scope = claims.getStringClaim("scope");
            if (scope == null) {
                return Optional.empty();
            }
            Set<String> scopes = Arrays.stream(scope.trim().split("\\s+"))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
            if (scopes.isEmpty()) {
                return Optional.empty(); // a scoped principal must carry at least one scope
            }
            return Optional.of(new Principal(claims.getSubject(), scopes));
        } catch (Exception e) {
            return Optional.empty(); // bad signature, wrong issuer/audience, expired, malformed
        }
    }

    /** The issuer this validator trusts. */
    public String issuer() {
        return issuer;
    }

    /** The audience tokens must carry. */
    public String audience() {
        return audience;
    }
}
