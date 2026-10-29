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

import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Relay paths (srad/large-networks-bulk-data.md §3.7, group R): the relay graph
 * a member reads from the advertisements in its view, the shortest path through
 * it from the relays the origin can reach to the relays the target is attached
 * to, and the deterministic tie-break that makes every origin choose the same
 * path to a target. Pure: no frame is sent to compute a path.
 */
final class RelayPaths {

    /** The hint key under which a dial-only member lists the relays it holds links to. */
    static final String VIA_HINT = "relay.via";

    /** The hint key under which a relay lists the relays it holds links to. */
    static final String LINKS_HINT = "relay.links";

    /** Bound on enumerated equal-length candidates before the tie-break. */
    private static final int MAX_CANDIDATES = 64;

    private RelayPaths() {
    }

    /**
     * The graph: the relays, their links (made symmetric), and which relays each
     * member is attached to.
     *
     * @param relays the RELAY-role members of the group
     * @param links  relay to the relays it declares links to
     * @param via    member to the relays it declares attachment to
     */
    record Graph(Set<PeerId> relays, Map<PeerId, Set<PeerId>> links, Map<PeerId, Set<PeerId>> via) {

        /** Neighbours of a relay, counting a link declared from either end. */
        Set<PeerId> neighbours(PeerId relay) {
            Set<PeerId> out = new HashSet<>(links.getOrDefault(relay, Set.of()));
            for (Map.Entry<PeerId, Set<PeerId>> e : links.entrySet()) {
                if (e.getValue().contains(relay)) {
                    out.add(e.getKey());
                }
            }
            out.retainAll(relays);
            out.remove(relay);
            return out;
        }
    }

    /** Encodes a list of ids as the hint value. */
    static String encode(Collection<PeerId> ids) {
        StringBuilder sb = new StringBuilder();
        for (PeerId id : ids) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(id.value());
        }
        return sb.toString();
    }

    /** Decodes a hint value; an absent or empty value is no ids. */
    static Set<PeerId> decode(String value) {
        Set<PeerId> ids = new java.util.LinkedHashSet<>();
        if (value == null || value.isBlank()) {
            return ids;
        }
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                ids.add(PeerId.of(trimmed));
            }
        }
        return ids;
    }

    /**
     * Finds the path of relays from an origin to a target.
     *
     * @param graph   the relay graph
     * @param start   the relays the origin can reach directly (links it holds or relays it can dial)
     * @param goal    the relays that can deliver to the target (its attachments, or every relay when it listens)
     * @param maxHops the longest path accepted
     * @return the relays to traverse in order, shortest first and lowest hash among equals; empty when none
     */
    static Optional<List<PeerId>> shortest(Graph graph, Set<PeerId> start, Set<PeerId> goal, int maxHops) {
        Set<PeerId> starts = new HashSet<>(start);
        starts.retainAll(graph.relays());
        Set<PeerId> goals = new HashSet<>(goal);
        goals.retainAll(graph.relays());
        if (starts.isEmpty() || goals.isEmpty() || maxHops < 1) {
            return Optional.empty();
        }
        // Breadth first from every start, recording the distance of each relay.
        Map<PeerId, Integer> distance = new HashMap<>();
        Deque<PeerId> queue = new ArrayDeque<>();
        for (PeerId s : starts) {
            distance.put(s, 1);
            queue.add(s);
        }
        int best = Integer.MAX_VALUE;
        while (!queue.isEmpty()) {
            PeerId relay = queue.poll();
            int d = distance.get(relay);
            if (goals.contains(relay)) {
                best = Math.min(best, d);
            }
            if (d >= maxHops || d >= best) {
                continue;
            }
            for (PeerId next : graph.neighbours(relay)) {
                if (!distance.containsKey(next)) {
                    distance.put(next, d + 1);
                    queue.add(next);
                }
            }
        }
        if (best == Integer.MAX_VALUE) {
            return Optional.empty();
        }
        // Enumerate the shortest candidates (bounded) and take the lowest hash, so
        // every origin converges on the same path to a target (decision 12).
        List<List<PeerId>> candidates = new ArrayList<>();
        for (PeerId s : starts) {
            enumerate(graph, goals, distance, best, new ArrayList<>(List.of(s)), candidates);
            if (candidates.size() >= MAX_CANDIDATES) {
                break;
            }
        }
        List<PeerId> chosen = null;
        byte[] chosenHash = null;
        for (List<PeerId> candidate : candidates) {
            byte[] hash = Digests.sha256(encode(candidate).getBytes(StandardCharsets.UTF_8));
            if (chosen == null || compare(hash, chosenHash) < 0) {
                chosen = candidate;
                chosenHash = hash;
            }
        }
        return Optional.ofNullable(chosen);
    }

    private static void enumerate(Graph graph, Set<PeerId> goals, Map<PeerId, Integer> distance, int best,
                                  List<PeerId> prefix, List<List<PeerId>> out) {
        if (out.size() >= MAX_CANDIDATES) {
            return;
        }
        PeerId last = prefix.get(prefix.size() - 1);
        if (prefix.size() == best) {
            if (goals.contains(last)) {
                out.add(List.copyOf(prefix));
            }
            return;
        }
        for (PeerId next : graph.neighbours(last)) {
            Integer d = distance.get(next);
            if (d != null && d == prefix.size() + 1 && !prefix.contains(next)) {
                prefix.add(next);
                enumerate(graph, goals, distance, best, prefix, out);
                prefix.remove(prefix.size() - 1);
            }
        }
    }

    private static int compare(byte[] a, byte[] b) {
        for (int i = 0; i < a.length && i < b.length; i++) {
            int x = a[i] & 0xff;
            int y = b[i] & 0xff;
            if (x != y) {
                return x - y;
            }
        }
        return a.length - b.length;
    }

    /**
     * Checks a path carried on a frame: within the hop bound, no repeated relay,
     * the receiving relay at the hop index, and the target not on it.
     */
    static boolean wellFormed(List<PeerId> path, int hop, PeerId self, PeerId target, int maxHops) {
        if (path == null || path.isEmpty() || path.size() > maxHops || hop < 0 || hop >= path.size()) {
            return false;
        }
        if (!path.get(hop).equals(self) || path.contains(target)) {
            return false;
        }
        return new HashSet<>(path).size() == path.size();
    }
}
