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
 *
 * <p>A host that builds its facades after the shared wiring (the Micronaut factory) hands the holder a
 * {@link Hosted} instead (PDT 2 phase 7, P2-X-04): each group's swarm takes the host's tree store, the spaces
 * under {@code agentspaces.pdt.spaces} escalate through a deferred seam ({@link #payloadsFor}), and the host
 * hands each facade it builds to {@link #adopt}. In both cases every group with trees gives its binder
 * {@link #refsFor}, so {@code @PdtRef} sites resolve through the group's facade (P2-X-03).
 */
public final class Pdts {

    /**
     * What a host that builds its own facades after the shared wiring hands the holder (the Micronaut factory,
     * PD-C-03; PDT 2 phase 7, P2-X-04, P2-X-08).
     */
    public interface Hosted {

        /**
         * The tree store a group's content swarm is built with: the bridge over the group's node store, which is
         * sealed under the ring in the {@code sealed-nodes} mode and admits received nodes through the store's quota.
         * The host builds the group's facade over the same node store later.
         *
         * @param groupName the group
         * @param ring      the group's content-key ring, or null when the group has no content key
         * @return the tree store
         */
        ai.badmonkey.agentspaces.content.tree.TreeStore treeStore(String groupName, GroupKeyRing ring);

        /**
         * The escalation of each space named under {@code agentspaces.pdt.spaces}, which the shared wiring
         * builds over {@link #payloadsFor} before the host's facade exists.
         *
         * @return the escalations by space name
         */
        Map<String, AgentSpacesProperties.PersistentTrees.SpaceEscalation> spaces();
    }

    private static final System.Logger LOG = System.getLogger(Pdts.class.getName());

    /** The store the host built per group, closed with the group when it owns a pack. */
    private record Built(NodeStore store, AutoCloseable backend) {
    }

    private final NodeStore userStore;
    private final Hosted hosted;
    private final List<PdtMergeFunction> mergeFunctions;
    private final Map<String, Pdt> byGroup = new LinkedHashMap<>();
    /** The content stores of the groups, filled when {@code agentspaces.store.enabled} is true (PDT 2 phase 3). */
    private final ContentStores stores = new ContentStores();
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
     * A holder for a host that builds its facades after the shared wiring (the Micronaut factory, PD-C-03; PDT 2
     * phase 7, P2-X-04): the host reads the {@code agentspaces.pdt.*} keys, so this holder opens no store of its
     * own, and it hands each facade to {@link #adopt} once built.
     *
     * @param hosted         the host's tree stores and space escalations
     * @param mergeFunctions the {@code PdtMergeFunction} beans, in order
     */
    public Pdts(Hosted hosted, List<PdtMergeFunction> mergeFunctions) {
        this(null, Objects.requireNonNull(hosted, "hosted"), mergeFunctions);
    }

    private Pdts(NodeStore userStore, Hosted hosted, List<PdtMergeFunction> mergeFunctions) {
        this.userStore = userStore;
        this.hosted = hosted;
        this.mergeFunctions = List.copyOf(Objects.requireNonNull(mergeFunctions, "mergeFunctions"));
    }

    /** The application's {@code NodeStore} bean, when one is declared. */
    public Optional<NodeStore> userStore() {
        return Optional.ofNullable(userStore);
    }

    /**
     * The tree store a group's content swarm is built with when another host
     * builds the facade itself (PD-C-03, P2-X-04): the host's, so the facade that
     * host attaches later finds the swarm over its own node store, sealed when its
     * sealing is {@code sealed-nodes}. Empty for a holder without a {@link Hosted};
     * the starter's own facades give the swarm their tree store.
     *
     * @param groupName the group
     * @param ring      the group's content-key ring, or null when it has no content key
     * @return the tree store, or empty
     */
    public Optional<ai.badmonkey.agentspaces.content.tree.TreeStore> treeStoreFor(String groupName, GroupKeyRing ring) {
        if (hosted == null) {
            return Optional.empty();
        }
        return Optional.of(Objects.requireNonNull(hosted.treeStore(groupName, ring),
                "the host answered no tree store for group " + groupName));
    }

    /**
     * The escalation of each space named under {@code agentspaces.pdt.spaces}: the host's for a holder with a
     * {@link Hosted} (P2-X-04), else the starter's block.
     *
     * @param config the starter's block
     * @return the escalations by space name
     */
    Map<String, AgentSpacesProperties.PersistentTrees.SpaceEscalation> escalations(AgentSpacesProperties.PersistentTrees config) {
        return hosted != null ? hosted.spaces() : config.getSpaces();
    }

    /**
     * The seam a space under {@code agentspaces.pdt.spaces} escalates through when its group's facade is built after
     * the space (PDT 2 phase 7, P2-X-04): it forwards to the facade's {@code payloads()} once {@link #adopt} has it.
     * Before then a write finds every member unable, so the space falls back to the flat swarm or refuses by name
     * under {@code require}, and a read reports its fetch failed, so the space retries it with its back-off.
     *
     * @param groupName the group
     * @return the deferred seam
     */
    ai.badmonkey.agentspaces.api.spi.BulkPayloads payloadsFor(String groupName) {
        return new DeferredPayloads(() -> find(groupName).map(Pdt::payloads));
    }

    /**
     * What builds the handles of a group's {@code @PdtRef} sites (PDT 2 phase 7, P2-X-03): the group's facade, found
     * when a bean binds, so a host that adopts its facade later still serves them.
     *
     * @param groupName the group
     * @return the resolver
     */
    ai.badmonkey.agentspaces.agent.pdt.RefResolver refsFor(String groupName) {
        Objects.requireNonNull(groupName, "groupName");
        return (name, type) -> forGroup(groupName).refFor(name, type);
    }

    /**
     * Takes a facade the host built and attached itself (PDT 2 phase 7, P2-X-04), so the deferred seams of
     * {@link #payloadsFor} and {@link #refsFor} reach it and {@link #forGroup} finds it. The host starts, ticks,
     * and closes it.
     *
     * @param groupName the group
     * @param pdt       the facade, attached to the group's swarm
     * @throws IllegalStateException for a holder without a {@link Hosted}, whose facades the starter registers
     */
    public synchronized void adopt(String groupName, Pdt pdt) {
        Objects.requireNonNull(groupName, "groupName");
        Objects.requireNonNull(pdt, "pdt");
        if (hosted == null) {
            throw new IllegalStateException("the starter registers this holder's facades itself; adopt is for a host"
                    + " that builds its own (Pdts(Hosted, mergeFunctions))");
        }
        byGroup.put(groupName, pdt);
    }

    /** The groups' content stores (PDT 2 phase 3, P2-S-10). */
    public ContentStores stores() {
        return stores;
    }

    /** Whether another host builds the facades after the shared wiring (the Micronaut factory, PD-C-03, P2-X-04). */
    boolean resolvesStores() {
        return hosted != null;
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
        return build(config, groupName, refSpace, clock, ring, null, null, null);
    }

    /**
     * As {@link #build(AgentSpacesProperties.PersistentTrees, String, Space, InstantSource, GroupKeyRing)} with the
     * group's discovery, where {@code Pdt.storeFor} finds the content stores, and the challenge deadline of
     * {@code agentspaces.store.challenge-deadline} (PDT 2 phase 3, P2-S-07, P2-S-11), and this peer's identity,
     * which signs the pins {@code Pdt.storeFor} writes (PDT 2 follow-ups, 2026-11-09, F1).
     */
    public Pdt build(AgentSpacesProperties.PersistentTrees config, String groupName, Space refSpace,
                     InstantSource clock, GroupKeyRing ring, ai.badmonkey.agentspaces.discovery.DiscoveryService discovery,
                     java.time.Duration challengeDeadline, ai.badmonkey.agentspaces.identity.PeerIdentity identity) {
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
        Pdt.Builder builder = Pdt.builder(refSpace).store(built.store()).clock(clock)
                .retain(config.getRetain().getVersions(), config.getRetain().getAge())
                .loadTimeout(config.getLoadTimeout()).inlineDelta(config.getInlineDelta())
                .follow(follow).sealing(sealing);
        if (groupSeals) {
            builder.keyRing(ring);
        }
        if (config.getStore().getQuota() > 0) {
            // PDT 2 phase 7, P2-X-08: received nodes pass the quota behind the bridge the swarm is built with
            builder.quota(config.getStore().getQuota());
        }
        if (discovery != null) {
            builder.discovery(discovery);
        }
        if (challengeDeadline != null) {
            builder.challengeDeadline(challengeDeadline);
        }
        if (identity != null) {
            builder.identity(identity);
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
                builder.follow(ref.getKey(), refFollow); // PDT 2 phase 7, P2-X-08
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
