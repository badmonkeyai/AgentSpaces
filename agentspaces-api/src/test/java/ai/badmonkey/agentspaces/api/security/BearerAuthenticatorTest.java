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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BearerAuthenticatorTest {

    @Test
    void aStaticTokenAuthenticatesOnlyTheExactToken() {
        BearerAuthenticator authenticator = BearerAuthenticator.staticToken("s3cret", "ops");

        assertThat(authenticator.authenticate("s3cret"))
                .hasValueSatisfying(p -> assertThat(p.subject()).isEqualTo("ops"));
        assertThat(authenticator.authenticate("s3cre")).isEmpty();
        assertThat(authenticator.authenticate("s3cret ")).isEmpty();
        assertThat(authenticator.authenticate("")).isEmpty();
        assertThatThrownBy(() -> authenticator.authenticate(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BearerAuthenticator.staticToken(null, "ops"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aPrincipalWithNoScopesPermitsEveryScope() {
        BearerAuthenticator.Principal holder = new BearerAuthenticator.Principal("ops", Set.of());

        assertThat(holder.permitsScope("space:write")).isTrue();
    }

    @Test
    void aScopedPrincipalPermitsOnlyItsScopesAndCopiesThemIn() {
        Set<String> scopes = new HashSet<>(Set.of("space:read"));
        BearerAuthenticator.Principal reader = new BearerAuthenticator.Principal("reader", scopes);
        scopes.add("space:write");

        assertThat(reader.permitsScope("space:read")).isTrue();
        assertThat(reader.permitsScope("space:write")).isFalse();
        assertThatThrownBy(() -> new BearerAuthenticator.Principal(null, Set.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new BearerAuthenticator.Principal("x", null))
                .isInstanceOf(NullPointerException.class);
    }
}
