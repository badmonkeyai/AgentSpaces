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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * One action a planner may put in a plan: an agent card's action with its contract read into what it needs
 * ({@code consumes} and {@code pre}) and what it brings about ({@code produces} and {@code post}). Schema names
 * are compared by their simple name, so {@code claims.Payment#v1} and {@code Payment} are the same term.
 *
 * @param name     the action's name
 * @param card     the card's agent name (the worker)
 * @param consumes the schemas the action takes (simple names)
 * @param produces the schemas it writes (simple names)
 * @param pre      the situations or conditions it needs beside its input
 * @param post     the situations it establishes beside its output
 * @param cost     a cost hint; empty when none
 * @param space    the space it takes its input in; empty when unbound
 */
public record PlanAction(String name, String card, List<String> consumes, List<String> produces, List<String> pre,
                         List<String> post, String cost, String space) {

    public PlanAction {
        consumes = simple(consumes);
        produces = simple(produces);
        pre = pre == null ? List.of() : List.copyOf(pre);
        post = simple(post);
        cost = cost == null ? "" : cost;
        space = space == null ? "" : space;
    }

    /** The actions a card advertises, contracts read; an action without a contract needs its input and brings its output. */
    public static List<PlanAction> of(AgentCard card) {
        List<PlanAction> out = new ArrayList<>();
        if (card.actions() == null) {
            return out;
        }
        for (CardAction a : card.actions()) {
            if (!a.invocable()) {
                continue;
            }
            CardAction.Contract c = a.contract();
            out.add(new PlanAction(a.name(), card.agent().localName(), a.consumes(), a.produces(),
                    c == null ? List.of() : c.pre(), c == null ? List.of() : c.post(), c == null ? "" : c.cost(),
                    a.space() == null ? card.spaceBindings().getOrDefault(a.consumes().isEmpty() ? "" : a.consumes().get(0), "") : a.space()));
        }
        return out;
    }

    /** Everything the action brings about: its outputs and what its contract establishes. */
    public Set<String> effects() {
        java.util.LinkedHashSet<String> s = new java.util.LinkedHashSet<>(produces);
        s.addAll(post);
        return s;
    }

    /** Whether the state admits the action: every input present, every precondition held. */
    public boolean applicable(PlanningState state) {
        return state.holds(consumes) && state.holds(pre);
    }

    static List<String> simple(List<String> names) {
        if (names == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>(names.size());
        for (String n : names) {
            out.add(simpleName(n));
        }
        return List.copyOf(out);
    }

    /** {@code claims.Payment#v1}, {@code claims.Payment}, and {@code Payment} name the same term. */
    public static String simpleName(String name) {
        String n = name == null ? "" : name;
        int hash = n.indexOf('#');
        if (hash >= 0) {
            n = n.substring(0, hash);
        }
        if (n.contains("(") || n.contains(" ")) {
            return n;   // a condition such as exists(Claim where amount > 1000) stays whole
        }
        int cut = Math.max(n.lastIndexOf('.'), n.lastIndexOf('$'));   // a package or an enclosing class
        return cut >= 0 ? n.substring(cut + 1) : n;
    }
}
