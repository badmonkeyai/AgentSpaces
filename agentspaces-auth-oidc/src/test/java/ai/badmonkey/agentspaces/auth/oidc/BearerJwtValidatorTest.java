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

import ai.badmonkey.agentspaces.api.security.BearerAuthenticator.Principal;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The operator-token posture (TODO item 7): an identity-provider JWT names the
 * operator and carries their scopes; every other token is worth nothing.
 */
class BearerJwtValidatorTest {

    private static final String ISSUER = "https://idp.example.com";
    private static final String AUDIENCE = "agentspaces-console";
    private static final Instant NOW = Instant.now();

    private static RSAKey key;
    private static RSAKey rogueKey;

    private final BearerJwtValidator validator = new BearerJwtValidator(ISSUER, AUDIENCE,
            new ImmutableJWKSet<SecurityContext>(new JWKSet(key.toPublicJWK())));

    @BeforeAll
    static void keys() throws Exception {
        key = new RSAKeyGenerator(2048).keyID("idp-key").generate();
        rogueKey = new RSAKeyGenerator(2048).keyID("idp-key").generate();
    }

    private static String token(RSAKey signWith, String issuer, String audience,
                                String subject, String scope, Instant expires)
            throws Exception {
        JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(audience)
                .subject(subject)
                .expirationTime(Date.from(expires));
        if (scope != null) {
            claims.claim("scope", scope);
        }
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signWith.getKeyID()).build(),
                claims.build());
        jwt.sign(new RSASSASigner(signWith));
        return jwt.serialize();
    }

    @Test
    void aValidTokenYieldsItsSubjectAndScopes() throws Exception {
        Optional<Principal> principal = validator.authenticate(token(key, ISSUER, AUDIENCE,
                "alice", "openid  aspace:console:operate aspace:a2a:client",
                NOW.plusSeconds(3600)));
        assertThat(principal).isPresent();
        assertThat(principal.get().subject()).isEqualTo("alice");
        assertThat(principal.get().scopes())
                .containsExactlyInAnyOrder("openid", "aspace:console:operate", "aspace:a2a:client");
        assertThat(principal.get().permitsScope("aspace:console:operate")).isTrue();
        assertThat(principal.get().permitsScope("aspace:console:admin")).isFalse();
    }

    @Test
    void thePeerBindingIsOptionalForOperators() throws Exception {
        // No agentspaces_peer claim: operators are humans, not fabric peers.
        assertThat(validator.authenticate(token(key, ISSUER, AUDIENCE, "bob",
                "aspace:console:operate", NOW.plusSeconds(60)))).isPresent();
    }

    @Test
    void expiredWrongAudienceWrongIssuerRogueKeyAndGarbageAllYieldEmpty() throws Exception {
        assertThat(validator.authenticate(token(key, ISSUER, AUDIENCE, "alice",
                "aspace:console:operate", NOW.minusSeconds(3600))))
                .as("already expired").isEmpty();
        assertThat(validator.authenticate(token(key, ISSUER, "another-audience", "alice",
                "aspace:console:operate", NOW.plusSeconds(3600))))
                .as("wrong audience").isEmpty();
        assertThat(validator.authenticate(token(key, "https://evil.example.com", AUDIENCE,
                "alice", "aspace:console:operate", NOW.plusSeconds(3600))))
                .as("wrong issuer").isEmpty();
        assertThat(validator.authenticate(token(rogueKey, ISSUER, AUDIENCE, "alice",
                "aspace:console:operate", NOW.plusSeconds(3600))))
                .as("signed by a key the issuer never published").isEmpty();
        assertThat(validator.authenticate("not-a-jwt")).as("garbage").isEmpty();
        assertThat(validator.authenticate("")).as("empty").isEmpty();
    }

    @Test
    void aTokenWithoutOrWithABlankScopeClaimIsRefused() throws Exception {
        assertThat(validator.authenticate(token(key, ISSUER, AUDIENCE, "alice",
                null, NOW.plusSeconds(3600)))).as("no scope claim").isEmpty();
        assertThat(validator.authenticate(token(key, ISSUER, AUDIENCE, "alice",
                "   ", NOW.plusSeconds(3600))))
                .as("a blank scope must not pass as an unscoped principal").isEmpty();
    }
}
