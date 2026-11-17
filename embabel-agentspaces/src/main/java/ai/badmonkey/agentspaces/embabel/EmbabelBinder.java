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
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.identity.PeerIdentity;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Publishes the cards of Embabel agent beans (spec §10.4). Since reviews/embabel-agentspaces-review-2026-11-12.md
 * §7.2 the <em>core</em> binder makes the one card for an Embabel bean, named by {@code @Agent(name)}, through
 * the {@link EmbabelDeclarationReader}; this class binds the bean through the facade so that happens, and
 * returns the cards the core binder made. It no longer publishes a second, Embabel-derived card.
 *
 * @deprecated since 0.7.0, for removal: bind the bean through {@link AgentSpaces#bind(Object)}; the Spring
 *             starter does so for every {@code @Agent} bean.
 */
@Deprecated(since = "0.7.0", forRemoval = true)
public final class EmbabelBinder {

    private final AgentSpaces spaces;
    private final List<String> groupOrder;

    public EmbabelBinder(AgentSpaces spaces, PeerIdentity identity, InstantSource clock, List<String> groupOrder) {
        this(spaces, identity, clock, groupOrder, null);
    }

    public EmbabelBinder(AgentSpaces spaces, PeerIdentity identity, InstantSource clock, List<String> groupOrder,
                         SchemaRegistry schemas) {
        this.spaces = Objects.requireNonNull(spaces, "spaces");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(clock, "clock");
        this.groupOrder = List.copyOf(Objects.requireNonNull(groupOrder, "groupOrder"));
        new EmbabelDeclarationReader().register();
    }

    /**
     * Binds one Embabel agent bean through the facade, so the core binder publishes its one card into every
     * group that admits it.
     *
     * @param bean the bean
     * @return the card published into the first configured group; empty when the bean is not an Embabel agent
     *         or no group is configured
     */
    public Optional<AgentCard> bind(Object bean) {
        List<AgentCard> cards = bindAll(bean);
        return cards.isEmpty() ? Optional.empty() : Optional.of(cards.get(0));
    }

    /**
     * Binds one Embabel agent bean through the facade and returns the cards the core binder made, in
     * configuration order.
     *
     * @param bean the bean
     * @return the cards, one per group the bean bound into; empty when the bean is not an Embabel agent
     */
    public List<AgentCard> bindAll(Object bean) {
        Objects.requireNonNull(bean, "bean");
        if (groupOrder.isEmpty() || EmbabelIntrospector.introspect(bean.getClass()).isEmpty()) {
            return List.of();
        }
        List<AgentCard> cards = new ArrayList<>();
        for (AgentBinder.Bound bound : spaces.bind(bean)) {
            cards.add(bound.card());
        }
        return cards;
    }
}
