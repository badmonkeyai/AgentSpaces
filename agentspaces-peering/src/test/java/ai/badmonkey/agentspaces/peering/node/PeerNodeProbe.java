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

import ai.badmonkey.agentspaces.common.id.PeerId;

/**
 * Read-only access to a node's judgement of its peers, for fleet probes outside this package (the
 * {@code FleetProbe} implementations of srad/spoiler-peers.md SP-O-01 in {@code agentspaces-spoilers},
 * which reach it through this module's test-jar). It reads what the package-private accessors already
 * expose to this package's tests; it adds no hook to the product.
 */
public final class PeerNodeProbe {

    private PeerNodeProbe() {
    }

    /** The witnessed-violation strikes a node holds against a peer now. */
    public static int strikes(PeerNode node, PeerId peer) {
        return node.strikes(peer);
    }

    /** Whether a node quarantines a peer now. */
    public static boolean quarantined(PeerNode node, PeerId peer) {
        return node.quarantined(peer);
    }
}
