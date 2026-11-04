/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.agent.flow;

import java.util.List;
import java.util.Map;

/**
 * The flow manifest (ISSUE-FlowManifest FR-1): the fabric-side desired state of a group's flow, one reserved entry
 * {@code ai.badmonkey.agentspaces.agent.flow.FlowManifest#v1} in the group's {@code flow-control} space, tagged
 * {@code flow} and {@code flowVersion}. A binding names an agent by its card name (the class the peer already
 * carries; a manifest cannot invent one), the peers that host it ({@code all}, a peer id, or {@code hint:k=v}),
 * the lane, the attribute overrides of the verb annotations, and the asset references; {@code drains} names the
 * stages to drain before rebinding under {@code policy} ({@code flush} waits for in-flight takes, {@code abort}
 * closes the loops now).
 */
public record FlowManifest(int v, String flow, long version, String hash, String group, List<Lane> lanes,
                           List<Binding> bindings, List<String> drains, String policy, String activatedBy,
                           String approval) {

    public static final int VERSION = 1;
    public static final String FLUSH = "flush";
    public static final String ABORT = "abort";
    public static final String FLOW_TAG = "flow";
    public static final String VERSION_TAG = "flowVersion";
    public static final String ALL_PEERS = "all";

    /** A lane the flow uses: its space name, conflict strategy, and settle window. */
    public record Lane(String name, String strategy, String settle) {
    }

    /** One stage: an agent (or a connective step's instance) on peers, in a lane, with overrides and assets. */
    public record Binding(String agent, String pkg, String instance, List<String> peers, String lane,
                          Map<String, String> attributes, Map<String, String> assets) {
        public Binding {
            peers = peers == null ? List.of(ALL_PEERS) : List.copyOf(peers);
            attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
            assets = assets == null ? Map.of() : Map.copyOf(assets);
            instance = instance == null || instance.isBlank() ? agent : instance;
            lane = lane == null ? "" : lane;
            pkg = pkg == null ? "" : pkg;
        }

        /** Whether this binding names the peer: {@code all}, the peer id, or a hint selector the host matched. */
        public boolean names(String peerId, Map<String, String> hints) {
            for (String p : peers) {
                if (ALL_PEERS.equals(p) || p.equals(peerId)) {
                    return true;
                }
                if (p.startsWith("hint:") && p.indexOf('=') > 5) {
                    String k = p.substring(5, p.indexOf('='));
                    String v = p.substring(p.indexOf('=') + 1);
                    if (v.equals(hints.get(k))) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public FlowManifest {
        lanes = lanes == null ? List.of() : List.copyOf(lanes);
        bindings = bindings == null ? List.of() : List.copyOf(bindings);
        drains = drains == null ? List.of() : List.copyOf(drains);
        policy = policy == null || policy.isBlank() ? FLUSH : policy;
    }

    public Map<String, String> tags() {
        return Map.of(FLOW_TAG, flow, VERSION_TAG, Long.toString(version));
    }
}
