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

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.List;

/**
 * Uniform random peer sampling over a group's live membership (spec §5.2). This is
 * the primitive from which space anti-entropy, scoped discovery queries, and every
 * Layer 4 gossip protocol draw their partners.
 */
@FunctionalInterface
public interface PeerSampler {

    /**
     * Returns up to {@code n} live members chosen uniformly at random, excluding
     * the local peer.
     *
     * @param n the maximum number of members to return
     * @return the sampled members, possibly fewer than {@code n}
     */
    List<PeerId> randomMembers(int n);
}
