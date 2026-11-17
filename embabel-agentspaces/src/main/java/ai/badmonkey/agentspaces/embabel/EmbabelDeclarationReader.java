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
package ai.badmonkey.agentspaces.embabel;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.spi.AgentDeclaration;
import ai.badmonkey.agentspaces.agent.spi.AgentDeclarationReader;
import ai.badmonkey.agentspaces.agent.spi.ExtraAction;
import ai.badmonkey.agentspaces.agent.spi.ImpliedBinding;
import ai.badmonkey.agentspaces.api.ad.CardAction;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads an Embabel {@code @Agent} for the core binder (reviews/embabel-agentspaces-review-2026-11-12.md
 * §7.2, §7.3), so a bean that is both an Embabel agent and a fleet worker gets <em>one</em> card, named by
 * {@code @Agent(name)}, carrying the {@code @Agent} description and the {@code @AchievesGoal} goals.
 *
 * <p>Every {@code @Action} is classified (§7.3). One that carries a space annotation of its own is the core
 * binder's to bind, under that kind. One with no binding and exactly one entry parameter, an entry result, no
 * precondition and no other parameters is a <em>map of 1</em>: the reader asks the binder for an implied take,
 * advertised as an {@code embabel-action} with its space and served by writing the entry tagged
 * {@code aspace.action=<agent>.<method>}. One with a parameter Embabel resolves is served the same way once a
 * host registers a platform invoker (§7.8); until then it is advertised as {@code local}. What cannot be a map of
 * 1 (several entry inputs, no entry result, a precondition on local state) is advertised as {@code local}, kept
 * off the card, or, under {@code advertise-unserved: invocable}, advertised as it was before this reader.
 *
 * <p>Loaded by {@code ServiceLoader} with the defaults below; a host registers a configured instance through
 * {@code AgentSpaces.declarationReaders(List)}, which replaces the loaded one.
 */
public final class EmbabelDeclarationReader implements AgentDeclarationReader {

    /** What to do with an {@code @Action} the binder cannot serve. */
    public enum Unserved {
        /** Advertise it under the kind {@code local}: visible, never invocable. */
        LOCAL,
        /** Advertise it as an invocable {@code embabel-action} with no space, as before (a caller times out). */
        INVOCABLE,
        /** Leave it off the card. */
        OMIT
    }

    /**
     * The reader's settings.
     *
     * @param nameFromAgent   whether the card is named by {@code @Agent(name)} (true) or by the class (false)
     * @param serveActions    whether map-of-1 actions get an implied take
     * @param actionSpace     the implied takes' space, or empty for the group's sole space
     * @param actionLease     the implied takes' lease, a duration string
     * @param unserved        what to do with an action the binder cannot serve
     * @param platformInvoker whether a platform invoker is registered, so actions with Embabel parameters are served
     */
    public record Settings(boolean nameFromAgent, boolean serveActions, String actionSpace, String actionLease,
                           Unserved unserved, boolean platformInvoker) {
        public Settings {
            actionSpace = actionSpace == null ? "" : actionSpace;
            actionLease = actionLease == null || actionLease.isBlank() ? "1m" : actionLease;
            unserved = unserved == null ? Unserved.LOCAL : unserved;
        }

        /** The defaults: named by the agent, served, the sole space, a one-minute lease, unserved as local. */
        public static Settings defaults() {
            return new Settings(true, true, "", "1m", Unserved.LOCAL, false);
        }
    }

    private final Settings settings;

    /** The ServiceLoader constructor, with the default settings. */
    public EmbabelDeclarationReader() {
        this(Settings.defaults());
    }

    public EmbabelDeclarationReader(Settings settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public Optional<AgentDeclaration> read(Class<?> userClass) {
        Optional<EmbabelIntrospector.EmbabelAgent> introspected = EmbabelIntrospector.introspect(userClass);
        if (introspected.isEmpty()) {
            return Optional.empty();
        }
        EmbabelIntrospector.EmbabelAgent agent = introspected.get();
        List<ImpliedBinding> implied = new ArrayList<>();
        List<ExtraAction> extras = new ArrayList<>();
        for (Method action : agent.actions()) {
            if (EmbabelIntrospector.hasOwnBinding(action)) {
                continue; // the core binder's, under its own kind
            }
            String description = EmbabelIntrospector.actionDescription(action);
            if (description.length() > CardAction.MAX_DESCRIPTION_LENGTH) {
                description = description.substring(0, CardAction.MAX_DESCRIPTION_LENGTH);
            }
            CardAction.Contract contract = EmbabelIntrospector.actionContract(action);
            boolean mapOfOne = EmbabelIntrospector.isMapOfOne(action)
                    && (contract == null || contract.pre().isEmpty() || settings.platformInvoker)
                    && (!EmbabelIntrospector.hasEmbabelParameters(action) || settings.platformInvoker);
            if (settings.serveActions && mapOfOne) {
                implied.add(new ImpliedBinding(action, settings.actionSpace, settings.actionLease, "1s",
                        description, CardAction.EMBABEL_ACTION,
                        contract == null ? List.of() : contract.pre(),
                        contract == null ? List.of() : contract.post(),
                        contract == null ? "" : contract.cost()));
                continue;
            }
            unservedAction(action, description, contract).ifPresent(extras::add);
        }
        String name = settings.nameFromAgent ? agent.name() : "";
        return Optional.of(new AgentDeclaration(name, agent.description(), agent.goals(),
                Map.of("framework", "embabel"), implied, extras));
    }

    private Optional<ExtraAction> unservedAction(Method action, String description, CardAction.Contract contract) {
        if (settings.unserved == Unserved.OMIT) {
            return Optional.empty();
        }
        String kind = settings.unserved == Unserved.INVOCABLE ? CardAction.EMBABEL_ACTION : CardAction.LOCAL;
        return Optional.of(new ExtraAction(action, kind, description, contract));
    }

    /** Registers this reader for every binder in the JVM (the host's configured instance replaces the loaded one). */
    public EmbabelDeclarationReader register() {
        AgentBinder.registerDeclarationReader(this);
        return this;
    }
}
