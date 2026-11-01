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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The content space seen through the two reserved records
 * (srad/large-networks-bulk-data.md §3.1, §3.2): manifests and holdings are
 * ordinary signed, leased entries, found by tag without decoding, replicated and
 * lapsing like any entry. Because the space mints every entry id, "one manifest
 * per root" and "one holding per holder and root" are kept by the writers (they
 * read before they write, and cancel their previous holding when they replace
 * it) rather than by a derived id.
 */
public final class ContentSpaces {

    /** Bound on holders read per root. */
    static final int MAX_HOLDINGS_READ = 1_024;

    private final Space space;

    /**
     * Wraps the group's content space.
     *
     * @param space the space manifests and holdings live in
     */
    public ContentSpaces(Space space) {
        this.space = Objects.requireNonNull(space, "space");
    }

    /** The underlying space. */
    public Space space() {
        return space;
    }

    /** The manifest of a root, when the replica holds a valid one. */
    public Optional<Manifest> manifest(String root) {
        return space.readAll(Template.of(Manifest.class).whereTag(Manifest.ROOT_TAG, Matchers.eq(root)), 8)
                .stream()
                .filter(m -> root.equals(m.root()) && m.violation().isEmpty())
                .findFirst();
    }

    /** Every valid manifest in the replica, up to a limit. */
    /** The valid manifests matching a template (LB-K-01). */
    public List<Manifest> manifests(Template<Manifest> template, int limit) {
        return space.readAll(template, limit).stream()
                .filter(m -> m.violation().isEmpty())
                .toList();
    }

    public List<Manifest> manifests(int limit) {
        return space.readAll(Template.of(Manifest.class), limit).stream()
                .filter(m -> m.violation().isEmpty())
                .toList();
    }

    /** Writes a manifest with its tags. */
    public EntryHandle writeManifest(Manifest manifest, Lease lease) {
        manifest.violation().ifPresent(v -> {
            throw new IllegalArgumentException("manifest refused: " + v);
        });
        return space.write(manifest, lease, manifest.tags());
    }

    /**
     * The holdings of a root the replica currently shows, one per holder: the complete
     * one, else the one with the most pieces. A holder that cannot withdraw its earlier
     * partial holdings (a dial-only client) leaves them to lapse, and they must not
     * stand in for its current state.
     */
    public List<Holding> holdings(String root) {
        java.util.Map<ai.badmonkey.agentspaces.common.id.PeerId, Holding> best = new java.util.LinkedHashMap<>();
        for (Holding h : space.readAll(Template.of(Holding.class).whereTag(Holding.ROOT_TAG, Matchers.eq(root)),
                MAX_HOLDINGS_READ)) {
            if (!root.equals(h.root()) || h.v() != Holding.VERSION || h.holder() == null) {
                continue;
            }
            best.merge(h.holder(), h, ContentSpaces::better);
        }
        return List.copyOf(best.values());
    }

    /** Of two holdings by one holder, the complete one, else the one holding more pieces. */
    private static Holding better(Holding a, Holding b) {
        return b.complete() && !a.complete() || (b.complete() == a.complete() && b.count() > a.count()) ? b : a;
    }

    /**
     * The number of distinct holders with a complete holding, for each of many roots, from one read of
     * the replica (a collection page asks for a hundred roots at once; reading {@link #holdings} per
     * root would scan the replica once per row). A root with no complete holder is absent.
     *
     * @param roots the roots
     * @return complete holders per root
     */
    public java.util.Map<String, Integer> completeHolders(java.util.Collection<String> roots) {
        if (roots.isEmpty()) {
            return java.util.Map.of();
        }
        java.util.Set<String> wanted = new java.util.HashSet<>(roots);
        java.util.Map<String, java.util.Map<ai.badmonkey.agentspaces.common.id.PeerId, Holding>> best = new java.util.HashMap<>();
        int limit = (int) Math.min(Integer.MAX_VALUE, (long) MAX_HOLDINGS_READ * wanted.size());
        for (Holding h : space.readAll(Template.of(Holding.class).whereTag(Holding.ROOT_TAG, Matchers.in(wanted.toArray())),
                limit)) {
            if (!wanted.contains(h.root()) || h.v() != Holding.VERSION || h.holder() == null) {
                continue;
            }
            best.computeIfAbsent(h.root(), r -> new java.util.LinkedHashMap<>()).merge(h.holder(), h, ContentSpaces::better);
        }
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        best.forEach((root, byHolder) -> {
            int complete = (int) byHolder.values().stream().filter(Holding::complete).count();
            if (complete > 0) {
                counts.put(root, complete);
            }
        });
        return counts;
    }

    /** Writes a holding with its tags. */
    public EntryHandle writeHolding(Holding holding, Lease lease) {
        return space.write(holding, lease, holding.tags());
    }

    /**
     * Subscribes to holdings of a root. The listener runs on the space's event
     * thread; it should set a flag and return.
     */
    public Subscription watchHoldings(String root, Consumer<SpaceEvent<Holding>> listener, Lease lease) {
        return space.notify(Template.of(Holding.class).whereTag(Holding.ROOT_TAG, Matchers.eq(root)),
                listener::accept, lease);
    }

    /** Subscribes to every manifest written to the space. */
    public Subscription watchManifests(Consumer<SpaceEvent<Manifest>> listener, Lease lease) {
        return space.notify(Template.of(Manifest.class), listener::accept, lease);
    }
}
