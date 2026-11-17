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
package ai.badmonkey.agentspaces.spoilers;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One spoiler's script (srad/spoiler-peers.md §3.2): named rules of trigger, action, optional release,
 * ceiling and count, and the seed every random choice draws from. Every matching rule fires, in order.
 *
 * <pre>{@code
 * Script s = Script.of("w3").seed(7)
 *     .rule("hold-chain").when(Trigger.frame(PIPE_DATA).from("w3").to("l").node("vg"))
 *         .act(Action.hold()).until(Trigger.afterPeriods(40)).ceiling(Ceiling.periods(80)).always()
 *     .build();
 * }</pre>
 *
 * @param spoiler the spoiler's address on the fabric
 * @param seed    the seed, printed when the script is armed and overridable with {@code -Dspoiler.seed}
 * @param rules   the rules in order
 */
public record Script(String spoiler, long seed, List<Rule> rules) {

    public Script {
        Objects.requireNonNull(spoiler, "spoiler");
        rules = List.copyOf(rules);
        Set<String> names = new LinkedHashSet<>();
        for (Rule r : rules) {
            if (!names.add(r.name())) {
                throw new IllegalArgumentException("two rules are named " + r.name());
            }
        }
    }

    /** A script with no rule: the spoiler is an honest member, the negative control (SP-C-02). */
    public static Script honest(String spoiler) {
        return new Script(spoiler, 0, List.of());
    }

    public static Builder of(String spoiler) {
        return new Builder(spoiler);
    }

    /**
     * One rule.
     *
     * @param name    its name, which the firing record and every failure print
     * @param when    the trigger
     * @param action  the action
     * @param until   the release of a hold, or null (a hold then ends at its ceiling)
     * @param ceiling the bound on a hold
     * @param times   how many matches fire it; {@link Integer#MAX_VALUE} for every match
     */
    public record Rule(String name, Trigger when, Action action, Trigger until, Ceiling ceiling, int times) {
        public Rule {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(when, "when");
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(ceiling, "ceiling");
            if (times < 1) {
                throw new IllegalArgumentException("rule " + name + " fires at least once");
            }
        }
    }

    /** Builds a script. */
    public static final class Builder {
        private final String spoiler;
        private long seed = 1;
        private final List<Rule> rules = new ArrayList<>();

        private Builder(String spoiler) {
            this.spoiler = spoiler;
        }

        public Builder seed(long value) {
            this.seed = value;
            return this;
        }

        public RuleBuilder rule(String name) {
            return new RuleBuilder(this, name);
        }

        public Script build() {
            return new Script(spoiler, seed, rules);
        }
    }

    /** Builds one rule; {@link #rule} and {@link #build} finish it. */
    public static final class RuleBuilder {
        private final Builder script;
        private final String name;
        private Trigger when;
        private Action action;
        private Trigger until;
        private Ceiling ceiling = Ceiling.periods(40);
        private int times = 1;

        private RuleBuilder(Builder script, String name) {
            this.script = script;
            this.name = name;
        }

        public RuleBuilder when(Trigger value) {
            this.when = value;
            return this;
        }

        public RuleBuilder act(Action value) {
            this.action = value;
            return this;
        }

        public RuleBuilder until(Trigger value) {
            this.until = value;
            return this;
        }

        public RuleBuilder ceiling(Ceiling value) {
            this.ceiling = value;
            return this;
        }

        public RuleBuilder times(int value) {
            this.times = value;
            return this;
        }

        public RuleBuilder always() {
            this.times = Integer.MAX_VALUE;
            return this;
        }

        private Builder done() {
            script.rules.add(new Rule(name, when, action, until, ceiling, times));
            return script;
        }

        public RuleBuilder rule(String next) {
            return done().rule(next);
        }

        public Script build() {
            return done().build();
        }
    }
}
