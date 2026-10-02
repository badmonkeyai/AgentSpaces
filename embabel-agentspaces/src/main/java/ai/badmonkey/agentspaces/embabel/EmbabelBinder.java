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
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Publishes AgentCards from Embabel agent metadata (spec §10.4): the
 * {@code @Agent} description, the goals its {@code @AchievesGoal} actions
 * achieve, and the domain types its {@code @Action} methods consume and
 * produce, all read reflectively so no Embabel artifact is needed at build
 * time. Other peers can then ask "who can produce a Finding from a
 * ResearchTask?" with no additional code in the Embabel application.
 *
 * <p>Division of labor with the core starter: worker loops come from
 * {@code @SpaceTake} on the action (bound by the core post-processor), and
 * this binder contributes the card, one per configured group (spec §10.4:
 * "into each joined group"). When an action also carries {@code @SpaceTake}
 * or {@code @SpaceNotify}, the card's space bindings record which space the
 * consumed type is taken from in that group, so the remote-actions bridge on
 * other peers routes inputs without configuration (spec §6.1, §10.6). A bean
 * that declares {@code @AgentSpec} has chosen the explicit card and is
 * skipped here.
 */
public final class EmbabelBinder {

    private final AgentSpaces spaces;
    private final PeerIdentity identity;
    private final InstantSource clock;
    private final List<String> groupOrder;
    private final SchemaRegistry schemas = new SimpleSchemaRegistry();

    /**
     * Creates the binder.
     *
     * @param spaces     the facade whose groups cards publish into
     * @param identity   the local peer identity
     * @param clock      the time source for card freshness
     * @param groupOrder the configured group names, in configuration order;
     *                   Embabel cards publish into every one of them
     */
    public EmbabelBinder(AgentSpaces spaces, PeerIdentity identity,
                         InstantSource clock, List<String> groupOrder) {
        this.spaces = Objects.requireNonNull(spaces, "spaces");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.groupOrder = List.copyOf(Objects.requireNonNull(groupOrder, "groupOrder"));
    }

    /**
     * Publishes a card for one Embabel agent bean into every configured group
     * (spec §10.4).
     *
     * @param bean the bean
     * @return the card published into the first configured group; empty when
     *         the bean is not an Embabel agent, declares its own
     *         {@link AgentSpec}, or no group is configured
     */
    public Optional<AgentCard> bind(Object bean) {
        List<AgentCard> cards = bindAll(bean);
        return cards.isEmpty() ? Optional.empty() : Optional.of(cards.get(0));
    }

    /**
     * Publishes a card for one Embabel agent bean into every configured group
     * (spec §10.4), returning all of them in configuration order.
     *
     * @param bean the bean
     * @return the published cards, one per configured group; empty when the
     *         bean is not an Embabel agent, declares its own {@link AgentSpec},
     *         or no group is configured
     */
    public List<AgentCard> bindAll(Object bean) {
        Objects.requireNonNull(bean, "bean");
        // A bean carrying @AgentSpec directly OR through a composed stereotype
        // (@SpaceAgent) has chosen the core binder's card; publishing a second,
        // Embabel-derived one would double-card it.
        if (AgentBinder.isAgentType(bean.getClass()) || groupOrder.isEmpty()) {
            return List.of();
        }
        Optional<EmbabelIntrospector.EmbabelAgent> introspected =
                EmbabelIntrospector.introspect(bean.getClass());
        if (introspected.isEmpty()) {
            return List.of();
        }
        List<AgentCard> cards = new java.util.ArrayList<>();
        for (String groupName : groupOrder) {
            AgentSpaces.GroupContext group = spaces.group(groupName);
            AgentCard card = cardFor(introspected.get(), group);
            // Adopted by the group's binder so the card is refreshed with every
            // other bound card (spec §10.4: leased and auto-refreshed).
            group.binder().adopt(card);
            cards.add(card);
        }
        return cards;
    }

    private AgentCard cardFor(EmbabelIntrospector.EmbabelAgent agent,
                              AgentSpaces.GroupContext group) {
        Set<String> consumes = new LinkedHashSet<>();
        Set<String> produces = new LinkedHashSet<>();
        Map<String, String> spaceBindings = new LinkedHashMap<>();
        List<CardAction> actions = new java.util.ArrayList<>();
        for (Method action : agent.actions()) {
            Optional<String> boundSpace = boundSpace(action, group);
            List<String> consumed = new java.util.ArrayList<>();
            for (Class<?> parameter : action.getParameterTypes()) {
                if (isDomainType(parameter)) {
                    String schema = schemas.register(parameter);
                    consumes.add(schema);
                    consumed.add(schema);
                    boundSpace.ifPresent(space -> spaceBindings.putIfAbsent(schema, space));
                }
            }
            List<String> produced = List.of();
            if (action.getReturnType() != void.class
                    && isDomainType(action.getReturnType())) {
                produces.add(schemas.register(action.getReturnType()));
                produced = List.of(schemas.register(action.getReturnType()));
            }
            // SPEC §6.1 v0.1.13: each @Action is one declared action, with its own description.
            String description = EmbabelIntrospector.actionDescription(action);
            if (description.length() > CardAction.MAX_DESCRIPTION_LENGTH) {
                description = description.substring(0, CardAction.MAX_DESCRIPTION_LENGTH);
            }
            actions.add(new CardAction(action.getName(), description, consumed, produced,
                    boundSpace.orElse(null), CardAction.EMBABEL_ACTION));
        }
        AgentId agentId = identity.agent(agent.name());
        return new AgentCard(
                "aspace://" + group.id().value() + "/agent/"
                        + identity.peerId().value() + "/" + agent.name(),
                identity.peerId(), group.id(), clock.instant(), Duration.ofMinutes(15),
                agentId, agent.description(), agent.goals(),
                List.copyOf(consumes), List.copyOf(produces),
                Map.of("framework", "embabel"), spaceBindings, null, declared(actions));
    }

    /** Sorted by name, overloads disambiguated by their consumed schemas; null when none. */
    private static List<CardAction> declared(List<CardAction> actions) {
        if (actions.isEmpty()) {
            return null;
        }
        Map<String, Long> counts = new java.util.HashMap<>();
        actions.forEach(a -> counts.merge(a.name(), 1L, Long::sum));
        List<CardAction> named = new java.util.ArrayList<>();
        for (CardAction a : actions) {
            String name = counts.get(a.name()) > 1
                    ? a.name() + "(" + String.join(",", a.consumes()) + ")" : a.name();
            named.add(new CardAction(name, a.description(), a.consumes(), a.produces(),
                    a.space(), a.kind()));
        }
        named.sort(java.util.Comparator.comparing(CardAction::name));
        return named.size() > AgentCard.MAX_ACTIONS ? named.subList(0, AgentCard.MAX_ACTIONS) : named;
    }

    /**
     * The space an action's {@code @SpaceTake} or {@code @SpaceNotify} binds
     * its input to in this group, when known: a named space that the group
     * registers, or the group's sole space when the annotation names none. A
     * binding addressed to another group (its {@code group} attribute) is not
     * this group's.
     */
    private static Optional<String> boundSpace(Method action, AgentSpaces.GroupContext group) {
        String named = null;
        String forGroup = null;
        SpaceTake take = action.getAnnotation(SpaceTake.class);
        SpaceNotify notify = action.getAnnotation(SpaceNotify.class);
        if (take != null) {
            named = take.space();
            forGroup = take.group();
        } else if (notify != null) {
            named = notify.space();
            forGroup = notify.group();
        }
        if (named == null || !group.binder().owns(forGroup)) {
            return Optional.empty();
        }
        Set<String> registered = group.spaceNames();
        if (named.isEmpty()) {
            return registered.size() == 1 ? Optional.of(registered.iterator().next())
                    : Optional.empty();
        }
        return registered.contains(named) ? Optional.of(named) : Optional.empty();
    }

    /** Embabel infrastructure parameters (OperationContext and kin) are not entries. */
    private static boolean isDomainType(Class<?> type) {
        return !type.getName().startsWith("com.embabel.")
                && !type.isPrimitive()
                && type != String.class;
    }
}
