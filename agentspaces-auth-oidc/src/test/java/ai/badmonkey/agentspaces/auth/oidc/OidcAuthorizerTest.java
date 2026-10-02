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
import ai.badmonkey.agentspaces.api.spi.Authorizer.Operation;
import ai.badmonkey.agentspaces.common.id.PeerId;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.InstantSource;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The OIDC authorization posture: the identity provider's token, not fabric
 * state, decides who may perform privileged operations. Tokens are validated
 * against the issuer's keys, bound to exactly one peer, scoped per operation,
 * and worthless once expired.
 */
class OidcAuthorizerTest {

    private static final String ISSUER = "https://idp.example.com";
    private static final String AUDIENCE = "agentspaces-fleet";
    // Nimbus validates exp against the real clock, so tokens are minted
    // relative to real now; the authorizer's injected clock governs only the
    // grant-cache expiry, which the lapse test drives explicitly.
    private static final Instant NOW = Instant.now();

    private static RSAKey key;
    private static RSAKey rogueKey;

    private final PeerId peer = PeerId.fromPublicKey(new byte[32]);
    private final PeerId other = PeerId.fromPublicKey(new byte[]{1, 2, 3});
    private final OidcAuthorizer authorizer = new OidcAuthorizer(ISSUER, AUDIENCE,
            new ImmutableJWKSet<SecurityContext>(new JWKSet(key.toPublicJWK())),
            InstantSource.system());

    @BeforeAll
    static void keys() throws Exception {
        key = new RSAKeyGenerator(2048).keyID("idp-key").generate();
        rogueKey = new RSAKeyGenerator(2048).keyID("idp-key").generate();
    }

    private static String token(RSAKey signWith, String issuer, String audience,
                                String peerBinding, String scope, Instant expires)
            throws Exception {
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256)
                        .keyID(signWith.getKeyID()).build(),
                new JWTClaimsSet.Builder()
                        .issuer(issuer)
                        .audience(audience)
                        .subject("agent-42")
                        .claim("agentspaces_peer", peerBinding)
                        .claim("scope", scope)
                        .expirationTime(Date.from(expires))
                        .build());
        jwt.sign(new RSASSASigner(signWith));
        return jwt.serialize();
    }

    @Test
    void scopesInAValidTokenGrantExactlyTheNamedOperations() throws Exception {
        String jwt = token(key, ISSUER, AUDIENCE, peer.value(),
                "openid aspace:raft-voter aspace:directive-issuer:fleet-a",
                NOW.plusSeconds(3600));
        assertThat(authorizer.authorize(peer, jwt)).isTrue();

        assertThat(authorizer.permits(peer, Operation.RAFT_VOTER, "")).isTrue();
        assertThat(authorizer.permits(peer, Operation.RAFT_VOTER, "any-scope")).isTrue();
        assertThat(authorizer.permits(peer, Operation.DIRECTIVE_ISSUER, "fleet-a")).isTrue();
        assertThat(authorizer.permits(peer, Operation.DIRECTIVE_ISSUER, "fleet-b")).isFalse();
        assertThat(authorizer.permits(peer, Operation.CONNECTOR_SERVE, "")).isFalse();
        assertThat(authorizer.permits(other, Operation.RAFT_VOTER, "")).isFalse();
    }

    /** TODO-EFG §3/§4 (SPEC §7.5 AUTHORIZER admission): {@code aspace:space-write:<space>} and {@code aspace:space-take:<space>} grant per space; the unscoped forms grant every space. */
    @Test
    void spaceScopesGrantPerSpace() throws Exception {
        assertThat(authorizer.authorize(peer, token(key, ISSUER, AUDIENCE, peer.value(),
                "aspace:space-write:tasks aspace:space-take:tasks", NOW.plusSeconds(3600))))
                .isTrue();
        assertThat(authorizer.permits(peer, Operation.SPACE_WRITE, "tasks")).isTrue();
        assertThat(authorizer.permits(peer, Operation.SPACE_TAKE, "tasks")).isTrue();
        assertThat(authorizer.permits(peer, Operation.SPACE_WRITE, "findings")).isFalse();
        assertThat(authorizer.permits(peer, Operation.SPACE_TAKE, "findings")).isFalse();
        assertThat(authorizer.permits(peer, Operation.SPACE_WRITE, "")).isFalse();

        // The unscoped form is every space; a write scope never implies take.
        assertThat(authorizer.authorize(other, token(key, ISSUER, AUDIENCE, other.value(),
                "aspace:space-write", NOW.plusSeconds(3600)))).isTrue();
        assertThat(authorizer.permits(other, Operation.SPACE_WRITE, "tasks")).isTrue();
        assertThat(authorizer.permits(other, Operation.SPACE_WRITE, "findings")).isTrue();
        assertThat(authorizer.permits(other, Operation.SPACE_TAKE, "tasks")).isFalse();
    }

    /** gate2-review G2-3: {@code aspace:vote} names the QUORUM electorate,
     * scoped per vote space like the space operations. */
    @Test
    void voteScopesGrantPerVoteSpace() throws Exception {
        assertThat(authorizer.authorize(peer, token(key, ISSUER, AUDIENCE, peer.value(),
                "aspace:vote:governance", NOW.plusSeconds(3600)))).isTrue();
        assertThat(authorizer.permits(peer, Operation.VOTE, "governance")).isTrue();
        assertThat(authorizer.permits(peer, Operation.VOTE, "votes")).isFalse();

        // The unscoped form counts in every vote space, and a vote scope
        // grants nothing else.
        assertThat(authorizer.authorize(other, token(key, ISSUER, AUDIENCE, other.value(),
                "aspace:vote", NOW.plusSeconds(3600)))).isTrue();
        assertThat(authorizer.permits(other, Operation.VOTE, "governance")).isTrue();
        assertThat(authorizer.permits(other, Operation.RAFT_VOTER, "")).isFalse();
    }

    /** {@code aspace:model-serve:<model>} grants serving one model; the unscoped form grants every model. */
    @Test
    void modelServeScopesGrantPerModel() throws Exception {
        assertThat(authorizer.authorize(peer, token(key, ISSUER, AUDIENCE, peer.value(),
                "aspace:model-serve:gpt-4.1-mini", NOW.plusSeconds(3600)))).isTrue();
        assertThat(authorizer.permits(peer, Operation.MODEL_SERVE, "gpt-4.1-mini")).isTrue();
        assertThat(authorizer.permits(peer, Operation.MODEL_SERVE, "claude-sonnet-5")).isFalse();

        assertThat(authorizer.authorize(other, token(key, ISSUER, AUDIENCE, other.value(),
                "aspace:model-serve", NOW.plusSeconds(3600)))).isTrue();
        assertThat(authorizer.permits(other, Operation.MODEL_SERVE, "claude-sonnet-5")).isTrue();
        assertThat(authorizer.permits(other, Operation.CONNECTOR_SERVE, "")).isFalse();
    }

    @Test
    void aTokenBoundToAnotherPeerIsRefused() throws Exception {
        String jwt = token(key, ISSUER, AUDIENCE, other.value(),
                "aspace:raft-voter", NOW.plusSeconds(3600));
        assertThat(authorizer.authorize(peer, jwt)).isFalse();
        assertThat(authorizer.permits(peer, Operation.RAFT_VOTER, "")).isFalse();
    }

    @Test
    void wrongIssuerAudienceSignatureOrExpiryAllRefuse() throws Exception {
        assertThat(authorizer.authorize(peer, token(key, "https://evil.example.com",
                AUDIENCE, peer.value(), "aspace:raft-voter", NOW.plusSeconds(3600))))
                .as("wrong issuer").isFalse();
        assertThat(authorizer.authorize(peer, token(key, ISSUER,
                "another-fleet", peer.value(), "aspace:raft-voter", NOW.plusSeconds(3600))))
                .as("wrong audience").isFalse();
        assertThat(authorizer.authorize(peer, token(rogueKey, ISSUER,
                AUDIENCE, peer.value(), "aspace:raft-voter", NOW.plusSeconds(3600))))
                .as("signed by a key the issuer never published").isFalse();
        assertThat(authorizer.authorize(peer, token(key, ISSUER,
                AUDIENCE, peer.value(), "aspace:raft-voter", NOW.minusSeconds(3600))))
                .as("already expired").isFalse();
        assertThat(authorizer.authorize(peer, "not-a-jwt")).isFalse();
    }

    @Test
    void grantsLapseWithTheTokenAndAFreshTokenRestoresThem() throws Exception {
        java.util.concurrent.atomic.AtomicReference<Instant> now =
                new java.util.concurrent.atomic.AtomicReference<>(NOW);
        OidcAuthorizer ticking = new OidcAuthorizer(ISSUER, AUDIENCE,
                new ImmutableJWKSet<SecurityContext>(new JWKSet(key.toPublicJWK())),
                () -> now.get());
        assertThat(ticking.authorize(peer, token(key, ISSUER, AUDIENCE, peer.value(),
                "aspace:key-holder", NOW.plusSeconds(600)))).isTrue();
        assertThat(ticking.permits(peer, Operation.KEY_HOLDER, "")).isTrue();

        now.set(NOW.plusSeconds(601));
        assertThat(ticking.permits(peer, Operation.KEY_HOLDER, ""))
                .as("an expired token backs nothing").isFalse();

        assertThat(ticking.authorize(peer, token(key, ISSUER, AUDIENCE, peer.value(),
                "aspace:key-holder", NOW.plusSeconds(7200)))).isTrue();
        assertThat(ticking.permits(peer, Operation.KEY_HOLDER, "")).isTrue();
    }

    /**
     * QA4 A4-7 phase 2: tokens are bound to one PeerId through {@code agentspaces_peer},
     * so the OIDC authorizer speaks at PEER granularity and every agent on the
     * peer inherits the token's scopes. A QUORUM tally under this profile
     * therefore counts one ballot per peer.
     */
    @Test
    void oidcGranularityIsPerPeer() throws Exception {
        assertThat(authorizer.granularity(Operation.VOTE, "votes"))
                .isEqualTo(Authorizer.Granularity.PEER);
    }

    /**
     * The per-agent token follow-on (TODO-QA3 §3.4 phase 2): an
     * {@code agentspaces_agent} claim would bind a token to one AgentId and let
     * the OIDC authorizer report AGENT granularity for the operations its scopes
     * name. Not implemented; this test states the contract so it is not
     * forgotten.
     */
    @Test
    @Disabled("per-agent OIDC tokens (agentspaces_agent claim) are a follow-on; tokens bind to a PeerId today")
    void aTokenWithAnAgentClaimWouldGrantAtAgentGranularity() {
    }
}
