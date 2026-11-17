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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.node.PeerNodeProbe;
import ai.badmonkey.agentspaces.test.FleetProbe;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * A {@link RefFleet} as the fleet oracle reads it (srad/spoiler-peers.md §3.5, SP-O-01): the honest members, each
 * one's heads of the ref and its live ref entries as its roots, its node's frame drops by reason, the members
 * (honest or spoiler) it strikes or quarantines, and the errors the fleet's threads raised.
 */
final class RefFleetProbe implements FleetProbe {

    private final RefFleet fleet;
    private final String name;
    private final Set<String> spoilers;
    /** Uncaught errors of any fleet thread; the fabric runs every member on shared threads, so each member answers with all. */
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    RefFleetProbe(RefFleet fleet, String name, Set<String> spoilers) {
        this.fleet = fleet;
        this.name = name;
        this.spoilers = Set.copyOf(spoilers);
    }

    private RefFleet.Peer peer(String member) {
        return fleet.peers.stream().filter(p -> p.name().equals(member)).findFirst().orElseThrow();
    }

    @Override
    public Set<String> members() {
        Set<String> out = new TreeSet<>();
        for (RefFleet.Peer p : fleet.peers) {
            if (!fleet.dead.contains(p.name()) && !spoilers.contains(p.name())) {
                out.add(p.name());
            }
        }
        return out;
    }

    @Override
    public Map<String, String> roots(String member) {
        RefFleet.Peer p = peer(member);
        String heads = String.join(",", new TreeSet<>(p.pdt().refs().heads(name).roots()));
        String entries = RefFleet.headEntries(p, name).stream().map(e -> e.entryId().toString())
                .sorted().collect(Collectors.joining(","));
        return Map.of("heads:" + name, heads, "space:" + RefFleet.REF_SPACE, Integer.toHexString(entries.hashCode()));
    }

    @Override
    public Map<String, Long> drops(String member) {
        PeerNode node = peer(member).node();
        return Map.of("stale", node.framesStale(), "replay", node.framesReplayed(), "undecodable", node.framesDropped(),
                "logical", node.framesLogicalRefused(), "farFuture", node.framesFarFuture());
    }

    @Override
    public Set<String> punished(String member) {
        PeerNode node = peer(member).node();
        Set<String> out = new TreeSet<>();
        for (RefFleet.Peer other : fleet.peers) {
            if (other.name().equals(member) || fleet.dead.contains(other.name())) {
                continue;
            }
            // the slow member is a peer like any other: a class A scenario strikes nobody, the spoiler included
            if (PeerNodeProbe.strikes(node, other.id()) > 0 || PeerNodeProbe.quarantined(node, other.id())) {
                out.add(other.name());
            }
        }
        return out;
    }

    @Override
    public List<Throwable> errors(String member) {
        return List.copyOf(errors);
    }
}
