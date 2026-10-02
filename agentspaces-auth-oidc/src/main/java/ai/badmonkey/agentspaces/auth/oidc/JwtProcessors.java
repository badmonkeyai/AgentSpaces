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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import java.net.URL;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * The one Nimbus processor construction shared by {@link OidcAuthorizer} (peer
 * tokens) and {@link BearerJwtValidator} (operator tokens), so both surfaces
 * accept the same algorithms, match issuer and audience the same way, and
 * differ only in the claims they require.
 */
final class JwtProcessors {

    /** The signature algorithms accepted from the identity provider. */
    static final Set<JWSAlgorithm> ALGORITHMS =
            Set.of(JWSAlgorithm.RS256, JWSAlgorithm.ES256, JWSAlgorithm.EdDSA);

    private JwtProcessors() {
    }

    /**
     * Builds a processor that verifies the signature against {@code keys},
     * requires the exact issuer and the audience, checks expiry against the
     * wall clock, and insists on the named claims being present.
     */
    static DefaultJWTProcessor<SecurityContext> processor(String issuer, String audience,
                                                          JWKSource<SecurityContext> keys,
                                                          Set<String> requiredClaims) {
        DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(
                ALGORITHMS, Objects.requireNonNull(keys, "keys")));
        processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                Objects.requireNonNull(audience, "audience"),
                new JWTClaimsSet.Builder()
                        .issuer(Objects.requireNonNull(issuer, "issuer")).build(),
                requiredClaims));
        return processor;
    }

    /**
     * A remote JWKS source with Nimbus's built-in caching, retry, and outage
     * tolerance, so an unreachable identity provider does not invalidate
     * already-validated keys (plan §1.3).
     */
    static JWKSource<SecurityContext> remoteJwks(URL jwksUrl) {
        return JWKSourceBuilder.create(Objects.requireNonNull(jwksUrl, "jwksUrl"))
                .cache(true)
                .retrying(true)
                .outageTolerant(Duration.ofHours(8).toMillis())
                .build();
    }
}
