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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.peering.wire.Envelope;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which inbound frames ask a group's registrants again (srad/agentspaces-event-loop.md EL-L-03, §10.21): liveness,
 * time-paced reconciliation and the gossip bus's own lazy-push upkeep do not; working frames do.
 */
class InboundWakeTest {

    @Test
    void theLazyPushsUpkeepAndAnOuterBatchAskNoRegistrantAndWorkingFramesDo() {
        Set<Envelope.Kind> quiet = EnumSet.of(Envelope.Kind.PING, Envelope.Kind.PING_REQ, Envelope.Kind.ACK,
                Envelope.Kind.RUMOR, Envelope.Kind.DIGEST, Envelope.Kind.PULL_RESP, Envelope.Kind.HAVE,
                Envelope.Kind.GRAFT, Envelope.Kind.PRUNE, Envelope.Kind.BATCH);
        for (Envelope.Kind kind : Envelope.Kind.values()) {
            assertThat(PeerNode.wakes(kind)).as("%s", kind).isEqualTo(!quiet.contains(kind));
        }
        assertThat(PeerNode.wakes(Envelope.Kind.QUERY)).isTrue();
        assertThat(PeerNode.wakes(Envelope.Kind.BLOCK)).isTrue();
    }
}
