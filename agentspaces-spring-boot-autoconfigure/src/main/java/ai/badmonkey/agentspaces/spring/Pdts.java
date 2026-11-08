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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.refs.MergeFunction;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.io.IOException;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The persistent-data-tree facades of the starter, one {@link Pdt} per joined
 * group (srad/agentspaces-pdt.md §7.2, PD-C-02), the way {@link Authorizers}
 * holds one authorizer per group. The bean exists in every context; it is empty
 * until {@code agentspaces.pdt.enabled} is true, and
 * {@code AgentSpacesAutoConfiguration} fills it as each group joins. Look a
 * facade up by group name with {@link #forGroup}, or take the sole group's with
 * {@link #primary()} (also the {@code Pdt} bean).
 *
 * <p>The holder also carries what the application contributes: a
 * {@link NodeStore} bean replaces the store {@code agentspaces.pdt.store.kind}
 * names (the ignored kind is logged), and every {@link PdtMergeFunction} bean
 * is registered on each facade (PD-R-05).
 *
 * <p>Per group the starter builds the store ({@link #buildStore}), the facade
 * over the group's ref space ({@link #build}), gives the content swarm the
 * facade's tree store, then attaches the swarm and registers the facade
 * ({@link #register}): its tick joins the group's tick and its close, with the
 * pack store's, joins the group's close (PD-S-02: the pack index is written on
 * close).
 */
public final class Pdts {

    private static final System.Logger LOG = System.getLogger(Pdts.class.getName());

    /** The store the host built per group, closed with the group when it owns a pack. */
    private record Built(NodeStore store, AutoCloseable backend) {
    }

    private final NodeStore userStore;
    private final java.util.function.Function<String, NodeStore> storesByGroup;
    private final List<PdtMergeFunction> mergeFunctions;
    private final Map<String, Pdt> byGroup = new LinkedHashMap<>();
    /** The pack backends this holder opened, closed with their group. */
    private final Map<Pdt, AutoCloseable> backends = new java.util.IdentityHashMap<>();

    /** A holder with no application beans. */
    public Pdts() {
        this((NodeStore) null, List.of());
    }

    /**
     * A holder over the application's beans.
     *
     * @param userStore      the {@code NodeStore} bean, or null
     * @param mergeFunctions the {@code PdtMergeFunction} beans, in order
     */
    public Pdts(NodeStore userStore, List<PdtMergeFunction> mergeFunctions) {
        this(userStore, null, mergeFunctions);
    }

    /**
     * A holder whose node stores another host builds per group (the Micronaut
     * factory's {@code PdtStores}, PD-C-03): the resolver answers every group's
     * store and the {@code store.*} keys are read by that host, so this holder
     * opens no store of its own.
     *
     * @param storesByGroup  the store per group name
     * @param mergeFunctions the {@code PdtMergeFunction} beans, in order
     */
    public Pdts(java.util.function.Function<String, NodeStore> storesByGroup, List<PdtMergeFunction> mergeFunctions) {
        this(null, Objects.requireNonNull(storesByGroup, "storesByGroup"), mergeFunctions);
    }

    private Pdts(NodeStore userStore, java.util.function.Function<String, NodeStore> storesByGroup, List<PdtMergeFunction> mergeFunctions) {
        this.userStore = userStore;
        this.storesByGroup = storesByGroup;
        this.mergeFunctions = List.copyOf(Objects.requireNonNull(mergeFunctions, "mergeFunctions"));
    }

    /** The application's {@code NodeStore} bean, when one is declared. */
    public Optional<NodeStore> userStore() {
        return Optional.ofNullable(userStore);
    }

    /**
     * The tree store a group's content swarm is built with when another host
     * builds the facade itself (PD-C-03): the adapter of {@code PdtSwarm.treeStore}
     * over the resolver's store for the group, so the facade that host attaches
     * later finds the swarm over its own node store. Empty for a holder without
     * a resolver; the starter's own facades give the swarm their tree store.
     *
     * @param groupName the group
     * @return the tree store, or empty
     */
    public Optional<ai.badmonkey.agentspaces.content.tree.TreeStore> treeStoreFor(String groupName) {
        if (storesByGroup == null) {
            return Optional.empty();
        }
        NodeStore store = Objects.requireNonNull(storesByGroup.apply(groupName),
                "the store resolver answered null for group " + groupName);
        return Optional.of(ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm.treeStore(store));
    }

    /** The application's merge functions. */
    public List<PdtMergeFunction> mergeFunctions() {
        return mergeFunctions;
    }

    /** The groups with a facade. */
    public synchronized Set<String> groupNames() {
        return Set.copyOf(byGroup.keySet());
    }

    /** Whether no group has a facade (the feature is off, or no group joined). */
    public synchronized boolean isEmpty() {
        return byGroup.isEmpty();
    }

    /**
     * The facade of a group.
     *
     * @param groupName the configured group name
     * @return the facade
     * @throws IllegalStateException when the group has none
     */
    public synchronized Pdt forGroup(String groupName) {
        Pdt pdt = byGroup.get(Objects.requireNonNull(groupName, "groupName"));
        if (pdt == null) {
            throw new IllegalStateException("group '" + groupName + "' has no Pdt facade"
                    + (byGroup.isEmpty() ? ": set agentspaces.pdt.enabled=true" : "; groups with one: " + byGroup.keySet()));
        }
        return pdt;
    }

    /** The facade of a group, when it has one. */
    public synchronized Optional<Pdt> find(String groupName) {
        return Optional.ofNullable(byGroup.get(Objects.requireNonNull(groupName, "groupName")));
    }

    /**
     * The sole group's facade, which the {@code Pdt} bean exposes.
     *
     * @return the facade
     * @throws IllegalStateException when no group or several groups have one
     */
    public synchronized Pdt primary() {
        if (byGroup.size() == 1) {
            return byGroup.values().iterator().next();
        }
        if (byGroup.isEmpty()) {
            throw new IllegalStateException("no Pdt facade: set agentspaces.pdt.enabled=true");
        }
        throw new IllegalStateException("several groups have a Pdt facade " + byGroup.keySet()
                + "; use Pdts.forGroup(name)");
    }

    // ------------------------------------------------------------ building

    /**
     * The node store of a group (PD-S-02, PD-Q-AVL-02): the application's bean
     * when declared, else the configured kind. A pack store lives under
     * {@code store.path/<group>}; a blank path falls back to memory with a
     * warning, or refuses under {@code store.require}, which also refuses the
     * memory kind.
     *
     * @param config    the block
     * @param groupName the group
     * @param clock     the clock of last-served marks
     * @return the store
     */
    NodeStore buildStore(AgentSpacesProperties.PersistentTrees config, String groupName, InstantSource clock) {
        return open(config, groupName, clock).store();
    }

    private Built open(AgentSpacesProperties.PersistentTrees config, String groupName, InstantSource clock) {
        AgentSpacesProperties.PersistentTrees.Store store = config.getStore();
        String kind = store.getKind() == null ? "" : store.getKind().trim().toLowerCase(java.util.Locale.ROOT);
        if (storesByGroup != null) {
            return new Built(Objects.requireNonNull(storesByGroup.apply(groupName), "the store resolver answered null for group " + groupName), null);
        }
        if (userStore != null) {
            if (!"bean".equals(kind)) {
                LOG.log(System.Logger.Level.INFO, "group ''{0}'': a NodeStore bean replaces agentspaces.pdt.store.kind={1}",
                        groupName, store.getKind());
            }
            return new Built(userStore, null);
        }
        boolean hasPath = store.getPath() != null && !store.getPath().isBlank();
        switch (kind) {
            case "bean" -> throw new IllegalStateException("agentspaces.pdt.store.kind=bean needs a NodeStore bean"
                    + " and the application declares none");
            case "pack" -> {
                if (!hasPath) {
                    if (store.isRequire()) {
                        throw new IllegalStateException("agentspaces.pdt.store.require=true needs a durable store,"
                                + " and agentspaces.pdt.store.path is blank: set the path, or declare a NodeStore bean");
                    }
                    LOG.log(System.Logger.Level.WARNING, "group ''{0}'': agentspaces.pdt.store.path is blank, so the"
                            + " trees live in memory and are lost on restart (set agentspaces.pdt.store.require=true"
                            + " to refuse this)", groupName);
                    return new Built(new NodeStore(new MemoryBlockStore(), clock), null);
                }
                Path dir = Path.of(store.getPath()).resolve(groupName.replaceAll("[^A-Za-z0-9._-]", "_"));
                try {
                    PackNodeStore pack = PackNodeStore.open(dir);
                    return new Built(new NodeStore(pack, clock), pack);
                } catch (IOException e) {
                    throw new IllegalStateException("agentspaces.pdt.store.path: cannot use " + dir + ": "
                            + e.getMessage(), e);
                }
            }
            case "memory" -> {
                if (store.isRequire()) {
                    throw new IllegalStateException("agentspaces.pdt.store.require=true needs a durable store,"
                            + " and agentspaces.pdt.store.kind=memory is not one: use kind=pack with a path, or a NodeStore bean");
                }
                return new Built(new NodeStore(new MemoryBlockStore(), clock), null);
            }
            default -> throw new IllegalStateException("agentspaces.pdt.store.kind: unknown kind '" + store.getKind()
                    + "' (pack, memory, or bean)");
        }
    }

    /**
     * Builds a group's facade over its ref space, not yet attached to the swarm
     * (§7.2): the store from {@link #buildStore}, the retention, the inline
     * delta, the follow mode, the sealing, and every configured ref mode. The
     * caller gives {@code Pdt.treeStore()} to the content swarm, then calls
     * {@link #register} with the built swarm.
     *
     * @param config     the block
     * @param groupName  the group
     * @param refSpace   the space the heads live in
     * @param clock      the clock
     * @param groupSeals whether the group has a content key; without one
     *                   {@code sealing=group-key} drops to {@code none} with a warning
     * @return the facade
     * @throws IllegalStateException when a value is out of range, naming the key
     */
    Pdt build(AgentSpacesProperties.PersistentTrees config, String groupName, Space refSpace,
              InstantSource clock, boolean groupSeals) {
        return build(config, groupName, refSpace, clock, groupSeals ? GroupKeyRing.empty() : null);
    }

    /**
     * As {@link #build(AgentSpacesProperties.PersistentTrees, String, Space, InstantSource, boolean)}
     * with the group's content-key ring, which the sealed-nodes mode seals under
     * (srad/agentspaces-pdt.md PD-P-05): without it a {@code sealed-nodes} facade could
     * neither write nor read a node. Null when the group has no content key.
     */
    public Pdt build(AgentSpacesProperties.PersistentTrees config, String groupName, Space refSpace,
                     InstantSource clock, GroupKeyRing ring) {
        boolean groupSeals = ring != null && !ring.epochs().isEmpty();
        Built built = open(config, groupName, clock);
        if (config.getInlineDelta() < 0 || config.getInlineDelta() > PRef.INLINE_DELTA_BYTES) {
            throw new IllegalStateException("agentspaces.pdt.inline-delta: " + config.getInlineDelta()
                    + " is not between 0 and " + PRef.INLINE_DELTA_BYTES + " bytes");
        }
        if (config.getRetain().getVersions() < 0) {
            throw new IllegalStateException("agentspaces.pdt.retain.versions must not be negative");
        }
        String follow = config.getFollow() == null ? "" : config.getFollow().trim().toLowerCase(java.util.Locale.ROOT);
        if (!Pdt.FOLLOW_FULL.equals(follow) && !Pdt.FOLLOW_LAZY.equals(follow)) {
            throw new IllegalStateException("agentspaces.pdt.follow: '" + config.getFollow() + "' is not full or lazy");
        }
        String sealing = config.getSealing() == null ? "" : config.getSealing().trim().toLowerCase(java.util.Locale.ROOT);
        if (!PRef.SEALING_NONE.equals(sealing) && !PRef.SEALING_GROUP_KEY.equals(sealing)
                && !PRef.SEALING_SEALED_NODES.equals(sealing)) {
            throw new IllegalStateException("agentspaces.pdt.sealing: '" + config.getSealing()
                    + "' is not none, group-key, or sealed-nodes");
        }
        if (!groupSeals && !PRef.SEALING_NONE.equals(sealing)) {
            LOG.log(System.Logger.Level.WARNING, "group ''{0}'': agentspaces.pdt.sealing={1} but the group has no"
                    + " content-key, so its heads name sealing=none", groupName, sealing);
            sealing = PRef.SEALING_NONE;
        }
        if (config.getStore().getQuota() > 0) {
            LOG.log(System.Logger.Level.INFO, "group ''{0}'': agentspaces.pdt.store.quota={1} is recorded; the facade"
                    + " of this phase applies no quota to the node store", groupName, config.getStore().getQuota());
        }
        Pdt.Builder builder = Pdt.builder(refSpace).store(built.store()).clock(clock)
                .retain(config.getRetain().getVersions(), config.getRetain().getAge())
                .loadTimeout(config.getLoadTimeout()).inlineDelta(config.getInlineDelta())
                .follow(follow).sealing(sealing);
        if (groupSeals) {
            builder.keyRing(ring);
        }
        for (Map.Entry<String, AgentSpacesProperties.PersistentTrees.RefDef> ref : config.getRefs().entrySet()) {
            AgentSpacesProperties.PersistentTrees.RefDef def = ref.getValue();
            if (def == null) {
                continue;
            }
            if (def.getMode() != null && !def.getMode().isBlank()) {
                String mode = def.getMode().trim().toLowerCase(java.util.Locale.ROOT);
                if (!PRef.MODE_MULTI.equals(mode) && !PRef.MODE_LINEAR.equals(mode)) {
                    throw new IllegalStateException("agentspaces.pdt.refs." + ref.getKey() + ".mode: '" + def.getMode()
                            + "' is not multi or linear");
                }
                builder.mode(ref.getKey(), mode);
            }
            if (def.getFollow() != null && !def.getFollow().isBlank()) {
                String refFollow = def.getFollow().trim().toLowerCase(java.util.Locale.ROOT);
                if (!Pdt.FOLLOW_FULL.equals(refFollow) && !Pdt.FOLLOW_LAZY.equals(refFollow)) {
                    throw new IllegalStateException("agentspaces.pdt.refs." + ref.getKey() + ".follow: '"
                            + def.getFollow() + "' is not full or lazy");
                }
                if (!refFollow.equals(follow)) {
                    LOG.log(System.Logger.Level.WARNING, "group ''{0}'': agentspaces.pdt.refs.{1}.follow={2} differs"
                            + " from agentspaces.pdt.follow={3}; the facade of this phase follows every ref in the"
                            + " global mode", groupName, ref.getKey(), refFollow, follow);
                }
            }
        }
        Pdt pdt = builder.build();
        if (built.backend() != null) {
            backends.put(pdt, built.backend());
        }
        return pdt;
    }

    /**
     * Attaches a facade to its group's content swarm, registers the merge
     * functions, starts it, and joins it to the group's tick and to its close:
     * the facade and the pack store it owns close when the node leaves the
     * group and when the node closes (the node's close does not leave its
     * groups), whichever comes first; both are idempotent.
     *
     * @param groupName the group
     * @param pdt       the facade from {@link #build}
     * @param content   the swarm built with {@code Content.Builder.treeStore(pdt.treeStore())}
     * @param runtime   the group runtime
     * @param node      the node
     * @return the facade
     */
    @SuppressWarnings("unchecked")
    synchronized Pdt register(String groupName, Pdt pdt, Content content, GroupRuntime runtime, PeerNode node) {
        Objects.requireNonNull(groupName, "groupName");
        Objects.requireNonNull(pdt, "pdt");
        pdt.attach(content);
        for (PdtMergeFunction merge : mergeFunctions) {
            if (merge.kind() != null) {
                pdt.mergeFor(merge.name(), merge.kind(), (MergeFunction<Object>) merge.function());
            } else {
                pdt.mergeFor(merge.name(), (MergeFunction<Object>) merge.function());
            }
        }
        pdt.start();
        runtime.onTick(pdt::tick);
        AutoCloseable backend = backends.remove(pdt);
        Runnable closer = () -> {
            pdt.close();
            if (backend != null) {
                try {
                    backend.close();
                } catch (Exception e) {
                    LOG.log(System.Logger.Level.WARNING, "group '" + groupName + "': closing the pdt store failed", e);
                }
            }
        };
        runtime.onClose(closer);
        node.closeWith(closer::run);
        byGroup.put(groupName, pdt);
        return pdt;
    }
}
