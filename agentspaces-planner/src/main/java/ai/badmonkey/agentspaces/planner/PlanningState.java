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
package ai.badmonkey.agentspaces.planner;

import ai.badmonkey.agentspaces.constraints.Event;
import ai.badmonkey.agentspaces.constraints.Marking;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What holds for one key when a plan is made: the schemas its entries show (simple names) and the situations
 * that are true. A planner extends it with an action's effects as it searches; it never changes the fleet.
 *
 * @param types      the entry types present for the key
 * @param situations the situation names (conditions) that hold
 */
public record PlanningState(Set<String> types, Set<String> situations) {

    public PlanningState {
        types = Set.copyOf(types == null ? Set.of() : types);
        situations = Set.copyOf(situations == null ? Set.of() : situations);
    }

    public static PlanningState of(Collection<String> types, Collection<String> situations) {
        return new PlanningState(new LinkedHashSet<>(types), new LinkedHashSet<>(situations));
    }

    /** The state of a key from its events and its marking: written types present; conditions that hold. */
    public static PlanningState of(List<Event> events, Marking marking) {
        Set<String> types = new LinkedHashSet<>();
        for (Event e : events) {
            if (e.kind() == Event.Kind.WRITTEN) {
                types.add(PlanAction.simpleName(e.type()));
            }
        }
        Set<String> situations = new LinkedHashSet<>();
        if (marking != null) {
            for (Map.Entry<String, Boolean> c : marking.conditions().entrySet()) {
                if (Boolean.TRUE.equals(c.getValue())) {
                    situations.add(c.getKey());
                }
            }
        }
        return new PlanningState(types, situations);
    }

    /** Whether every term holds: a type present, a situation true, or a condition text already established. */
    public boolean holds(Collection<String> terms) {
        for (String t : terms) {
            String s = PlanAction.simpleName(t);
            if (!types.contains(s) && !situations.contains(s) && !situations.contains(t)) {
                return false;
            }
        }
        return true;
    }

    public boolean has(String term) {
        return holds(List.of(term));
    }

    /** This state with an action's effects added. */
    public PlanningState after(PlanAction action) {
        Set<String> t = new LinkedHashSet<>(types);
        Set<String> s = new LinkedHashSet<>(situations);
        for (String e : action.effects()) {
            if (action.produces().contains(e)) {
                t.add(e);
            } else {
                s.add(e);
            }
        }
        return new PlanningState(t, s);
    }
}
