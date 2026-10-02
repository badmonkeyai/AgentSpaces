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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Authenticates a bearer token presented to one of the fabric's HTTP surfaces
 * (the fleet console's command routes, the A2A gateway's JSON-RPC surface) and
 * names the principal behind it. This is the HTTP-edge sibling of
 * {@link ai.badmonkey.agentspaces.api.spi.Authorizer}: the authorizer answers
 * "may this <em>peer</em> do this", the authenticator answers "who is holding
 * this <em>token</em>", and the surface then decides by the principal's scopes.
 *
 * <p>Two implementations ship. {@link #staticToken(String, String)} is the
 * shared-secret gate: one constant-time comparison, a fixed subject, and no
 * scopes, so a surface applies no scope check to it. The OIDC module's
 * {@code BearerJwtValidator} validates an identity-provider JWT per request
 * and yields the token's {@code sub} and space-split {@code scope} claim, so
 * revoking an operator at the IdP ends their access when the token lapses and
 * the audit trail records who acted, not what they typed.
 */
public interface BearerAuthenticator {

    /**
     * The authenticated holder of a bearer token.
     *
     * @param subject the identity to attribute actions to (a JWT {@code sub},
     *                or the fixed name of a static token's holder)
     * @param scopes  the OAuth2 scopes the token grants; empty for a static
     *                token, which carries no scope model at all
     */
    record Principal(String subject, Set<String> scopes) {
        public Principal {
            Objects.requireNonNull(subject, "subject");
            scopes = Set.copyOf(Objects.requireNonNull(scopes, "scopes"));
        }

        /** Whether this principal carries the scope, or carries no scopes at all. */
        public boolean permitsScope(String scope) {
            return scopes.isEmpty() || scopes.contains(scope);
        }
    }

    /**
     * Authenticates one presented token.
     *
     * @param bearerToken the token exactly as presented (the value after
     *                    {@code Bearer }), never null
     * @return the principal when the token is valid, otherwise empty; an
     *         implementation never throws for a bad token
     */
    Optional<Principal> authenticate(String bearerToken);

    /**
     * The shared-secret gate: accepts exactly one token, compared in constant
     * time over its UTF-8 bytes, and names every holder {@code subject} with
     * no scopes.
     *
     * @param token   the secret operators present
     * @param subject the audited name of whoever presents it
     * @return the authenticator
     */
    static BearerAuthenticator staticToken(String token, String subject) {
        byte[] expected = Objects.requireNonNull(token, "token")
                .getBytes(StandardCharsets.UTF_8);
        Principal holder = new Principal(subject, Set.of());
        return presented -> MessageDigest.isEqual(expected,
                Objects.requireNonNull(presented, "bearerToken")
                        .getBytes(StandardCharsets.UTF_8))
                ? Optional.of(holder) : Optional.empty();
    }
}
