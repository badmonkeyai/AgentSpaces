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
package ai.badmonkey.agentspaces.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The minimal fleet oracle of srad/spoiler-peers.md §3.5 (SP-O-01, SP-O-02), checked over a
 * {@link FleetProbe} once a fleet is quiescent:
 * <ol>
 *   <li>no honest member raised an error on its own threads;</li>
 *   <li>every member holds the same root for every replicated state;</li>
 *   <li>the frame drops since the baseline, by reason, are exactly the drops the scenario expected
 *       (its injected bad frames, and the stale or replayed frames its holds made);</li>
 *   <li>in a class A scenario, no honest member struck or quarantined another.</li>
 * </ol>
 * The fuller set (views, caches, connections, SP-O-07) comes after 0.7.0.
 */
public final class FleetInvariants {

    private final FleetProbe probe;
    private final Map<String, Map<String, Long>> baseline = new HashMap<>();

    private FleetInvariants(FleetProbe probe) {
        this.probe = Objects.requireNonNull(probe, "probe");
        for (String member : probe.members()) {
            baseline.put(member, Map.copyOf(probe.drops(member)));
        }
    }

    /**
     * Starts an oracle whose drop check counts from now: a scenario takes it after the fleet formed, so
     * the joins' own refusals are not charged to the script.
     *
     * @param probe the fleet's probe
     * @return the oracle
     */
    public static FleetInvariants baseline(FleetProbe probe) {
        return new FleetInvariants(probe);
    }

    /**
     * The invariants that do not hold, each as one line naming the member; empty when all hold.
     *
     * @param expectedDrops drops the scenario expects by reason, summed over the members (absent means 0)
     * @param classA        whether no member may be struck or quarantined
     * @return the broken invariants, the first one first
     */
    public List<String> broken(Map<String, Long> expectedDrops, boolean classA) {
        List<String> out = state(probe, false);
        Map<String, Long> seen = new TreeMap<>();
        for (String member : probe.members()) {
            Map<String, Long> before = baseline.getOrDefault(member, Map.of());
            probe.drops(member).forEach((reason, n) -> {
                long delta = n - before.getOrDefault(reason, 0L);
                if (delta != 0) {
                    seen.merge(reason, delta, Long::sum);
                }
            });
        }
        Map<String, Long> expected = new TreeMap<>();
        expectedDrops.forEach((reason, n) -> {
            if (n != 0) {
                expected.put(reason, n);
            }
        });
        if (!seen.equals(expected)) {
            out.add("frame drops by reason " + seen + ", expected " + expected);
        }
        if (classA) {
            out.addAll(punished(probe));
        }
        return out;
    }

    /**
     * The invariants that need no baseline: no member raised an error, every member holds the same roots,
     * and, when {@code noPunishment}, no member struck or quarantined another. A fleet's own quiet-point
     * check delegates here ({@code RobustnessFleet.assertInvariants}, SP-O-01).
     *
     * @param probe        the fleet's probe
     * @param noPunishment whether a strike or quarantine of an honest member breaks the fleet
     * @return the broken invariants, empty when all hold
     */
    public static List<String> state(FleetProbe probe, boolean noPunishment) {
        List<String> out = new ArrayList<>();
        for (String member : new java.util.TreeSet<>(probe.members())) {
            List<Throwable> errors = probe.errors(member);
            if (!errors.isEmpty()) {
                out.add(member + " raised " + errors.size() + " error(s), first: " + errors.get(0));
            }
        }
        Map<String, String> reference = null;
        String referenceMember = null;
        for (String member : new java.util.TreeSet<>(probe.members())) {
            Map<String, String> roots = new TreeMap<>(probe.roots(member));
            if (reference == null) {
                reference = roots;
                referenceMember = member;
            } else if (!roots.equals(reference)) {
                out.add(member + " holds roots " + roots + " where " + referenceMember + " holds " + reference);
            }
        }
        if (noPunishment) {
            out.addAll(punished(probe));
        }
        return out;
    }

    private static List<String> punished(FleetProbe probe) {
        List<String> out = new ArrayList<>();
        for (String member : new java.util.TreeSet<>(probe.members())) {
            if (!probe.punished(member).isEmpty()) {
                out.add(member + " struck or quarantined honest members " + probe.punished(member));
            }
        }
        return out;
    }

    /**
     * Fails with the first broken invariant, listing the rest.
     *
     * @param expectedDrops drops the scenario expects by reason
     * @param classA        whether no member may be struck or quarantined
     */
    public void assertHolds(Map<String, Long> expectedDrops, boolean classA) {
        List<String> broken = broken(expectedDrops, classA);
        if (!broken.isEmpty()) {
            throw new AssertionError("fleet invariant broken: " + broken.get(0)
                    + (broken.size() > 1 ? "; also " + broken.subList(1, broken.size()) : ""));
        }
    }
}
