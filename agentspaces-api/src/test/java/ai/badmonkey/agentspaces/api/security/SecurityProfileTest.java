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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecurityProfileTest {

    @Test
    void eachProfileTurnsOnItsControls() {
        assertThat(SecurityProfile.DEV_LOCAL.tlsTransport()).isFalse();
        assertThat(SecurityProfile.DEV_LOCAL.envelopeSignatureEveryHop()).isTrue();
        assertThat(SecurityProfile.DEV_LOCAL.oidcAuthorization()).isFalse();

        assertThat(SecurityProfile.MTLS.tlsTransport()).isTrue();
        assertThat(SecurityProfile.MTLS.envelopeSignatureEveryHop()).isFalse();
        assertThat(SecurityProfile.MTLS.oidcAuthorization()).isFalse();

        assertThat(SecurityProfile.MTLS_OIDC.oidcAuthorization()).isTrue();

        assertThat(SecurityProfile.ZERO_TRUST.tlsTransport()).isTrue();
        assertThat(SecurityProfile.ZERO_TRUST.envelopeSignatureEveryHop()).isTrue();
        assertThat(SecurityProfile.ZERO_TRUST.oidcAuthorization()).isTrue();
    }

    @Test
    void fromNameAcceptsTheKebabCaseConfigurationSpelling() {
        assertThat(SecurityProfile.fromName("dev-local")).isEqualTo(SecurityProfile.DEV_LOCAL);
        assertThat(SecurityProfile.fromName(" mtls-oidc ")).isEqualTo(SecurityProfile.MTLS_OIDC);
        assertThat(SecurityProfile.fromName("ZERO_TRUST")).isEqualTo(SecurityProfile.ZERO_TRUST);
    }

    @Test
    void fromNameListsTheChoicesWhenTheNameIsUnknown() {
        assertThatThrownBy(() -> SecurityProfile.fromName("plaintext"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'plaintext'")
                .hasMessageContaining("dev-local, mtls, mtls-oidc, zero-trust");
    }
}
