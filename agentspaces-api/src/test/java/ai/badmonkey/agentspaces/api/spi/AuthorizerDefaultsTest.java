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
package ai.badmonkey.agentspaces.api.spi;

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * QA4 A4-7 phase 2: the {@link Authorizer} SPI grew two defaults and nothing
 * else, so every pre-existing implementation (a lambda included) keeps its
 * behaviour: agents inherit their peer's answer and the granularity is PEER.
 */
class AuthorizerDefaultsTest {

    private final PeerId granted = PeerId.fromPublicKey(new byte[] {1});
    private final PeerId other = PeerId.fromPublicKey(new byte[] {2});

    @Test
    void aLambdaAuthorizerAnswersAgentsThroughTheirPeerAtPeerGranularity() {
        Set<PeerId> voters = Set.of(granted);
        Authorizer authorizer = (peer, operation, scope) -> voters.contains(peer);

        assertThat(authorizer.permits(new AgentId(granted, "anyone"), Authorizer.Operation.VOTE, "v"))
                .isTrue();
        assertThat(authorizer.permits(new AgentId(other, "anyone"), Authorizer.Operation.VOTE, "v"))
                .isFalse();
        assertThat(authorizer.granularity(Authorizer.Operation.VOTE, "v"))
                .isEqualTo(Authorizer.Granularity.PEER);
    }
}
