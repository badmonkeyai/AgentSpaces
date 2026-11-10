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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.store.ContentStore;
import ai.badmonkey.agentspaces.store.StorePolicy;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The content stores of the starter, one per joined group (PDT 2 phase 3, P2-S-10; srad/agentspaces-pdt2.md
 * §7.2): empty until {@code agentspaces.store.enabled} is true, then filled as each group joins, over the
 * group's {@link Pdt} facade. The {@link Pdts} holder carries it, so a host reaches a group's store through
 * {@code pdts.stores().forGroup(name)}. {@link #policy(AgentSpacesProperties.StorePeer)} reads the keys of
 * appendix C into a {@link StorePolicy}, and the Micronaut factory reads the same keys through it; a
 * {@code StorePolicy} bean replaces what the keys build.
 */
public final class ContentStores {

    private final Map<String, ContentStore> byGroup = new LinkedHashMap<>();
    private volatile StorePolicy userPolicy;

    /** Sets the application's {@code StorePolicy} bean, which replaces the policy the keys build. */
    public void userPolicy(StorePolicy policy) {
        this.userPolicy = policy;
    }

    /**
     * Reads {@code agentspaces.store.*} into a policy (appendix C).
     *
     * @param config the block
     * @return the policy
     * @throws IllegalStateException naming the key whose value is missing or out of range
     */
    public static StorePolicy policy(AgentSpacesProperties.StorePeer config) {
        if (config.getCapacity() == null || config.getCapacity().isBlank()) {
            throw new IllegalStateException("agentspaces.store.capacity is required: the bytes the store holds, such as 2TiB");
        }
        long capacity = size("agentspaces.store.capacity", config.getCapacity());
        StorePolicy policy;
        try {
            policy = StorePolicy.of(capacity);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("agentspaces.store.capacity: " + e.getMessage(), e);
        }
        String accept = config.getAcceptRequesters() == null ? "" : config.getAcceptRequesters().trim().toLowerCase(Locale.ROOT);
        if (!"open".equals(accept) && !"grant".equals(accept)) {
            throw new IllegalStateException("agentspaces.store.accept-requesters: '" + config.getAcceptRequesters()
                    + "' is not open or grant");
        }
        String key = "agentspaces.store.reserve";
        try {
            policy = policy.reserve(StorePolicy.reserveOf(config.getReserve(), capacity));
            key = "agentspaces.store.max-object";
            policy = policy.maxObject(size(key, config.getMaxObject()));
            key = "agentspaces.store.lease-default";
            policy = policy.leases(config.getLeaseDefault(), config.getLeaseMax());
            key = "agentspaces.store.copies-max";
            policy = policy.copiesMax(config.getCopiesMax());
            key = "agentspaces.store.accept-kinds";
            policy = policy.acceptKinds(Set.copyOf(config.getAcceptKinds()));
            key = "agentspaces.store.tier";
            policy = policy.tier(config.getTier() == null ? "" : config.getTier().trim().toLowerCase(Locale.ROOT));
            key = "agentspaces.store.price-per-gib-month";
            policy = policy.price(config.getPricePerGibMonth());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(key + ": " + e.getMessage(), e);
        }
        return policy.acceptRequesters("open".equals(accept) ? StorePolicy.Accept.OPEN : StorePolicy.Accept.GRANT)
                .blind(config.isBlind());
    }

    private static long size(String key, String text) {
        try {
            return StorePolicy.bytes(text);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(key + ": " + e.getMessage(), e);
        }
    }

    /**
     * Builds and starts a group's store over its facade, joins it to the group's tick, and closes it with the group
     * and the node.
     *
     * @param config     the block
     * @param groupName  the group
     * @param pdt        the group's facade, attached to its swarm
     * @param discovery  the group's discovery
     * @param identity   this peer
     * @param clock      the clock
     * @param authorizer the group's authorizer, or null
     * @param runtime    the group runtime
     * @param node       the node
     * @return the store
     */
    synchronized ContentStore register(AgentSpacesProperties.StorePeer config, String groupName, Pdt pdt,
                                       DiscoveryService discovery, PeerIdentity identity, InstantSource clock,
                                       Authorizer authorizer, GroupRuntime runtime, PeerNode node) {
        StorePolicy policy = userPolicy != null ? userPolicy : policy(config);
        ContentStore.Builder builder = ContentStore.builder(pdt, discovery, identity).policy(policy).clock(clock);
        if (config.getName() != null && !config.getName().isBlank()) {
            try {
                builder.name(config.getName().trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("agentspaces.store.name: " + e.getMessage(), e);
            }
        }
        if (authorizer != null) {
            builder.authorizer(authorizer);
        }
        ContentStore store = builder.build();
        store.start();
        runtime.onTick(store::tick);
        runtime.onClose(store::close);
        node.closeWith(store::close);
        byGroup.put(groupName, store);
        return store;
    }

    /** A group's store, when the group has one. */
    public synchronized Optional<ContentStore> find(String groupName) {
        return Optional.ofNullable(byGroup.get(Objects.requireNonNull(groupName, "groupName")));
    }

    /**
     * A group's store.
     *
     * @throws IllegalStateException when the group has none
     */
    public synchronized ContentStore forGroup(String groupName) {
        return find(groupName).orElseThrow(() -> new IllegalStateException("group '" + groupName + "' has no content store"
                + (byGroup.isEmpty() ? ": set agentspaces.store.enabled=true" : "; groups with one: " + byGroup.keySet())));
    }
}
