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
package ai.badmonkey.agentspaces.pdt.stores;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.planner.StoreChooser;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * A requester's view of the group's content stores (PDT 2 phase 3, P2-S-07, P2-S-11; srad/agentspaces-pdt2.md
 * §3.2.2, §3.2.4, S2 and S3 of §6.4). It chooses stores through {@link StoreChooser} over the
 * {@code content-store} cards discovery holds, and writes one {@link SignedPin} naming them in the attribute
 * {@link #STORES_ATTR}, so the stores it chose take the pin. This peer signs every pin it writes as its requester,
 * a replacement after a strike included, so a store charges it only for terms it signed (PDT 2 follow-ups,
 * 2026-11-09, F1). It skips a store whose card says it waits for an authorizer ({@link #WAITING_ACCESS}), since
 * that store takes no pins (F2). It challenges a store with random picks of a
 * root, strikes a store after {@link #STRIKES} failures in a row on one root, and then pins each root it pinned on
 * that store again with one copy, under the root's need, on a store that neither holds the root nor was struck; when
 * no store fits it writes no pin and lists the root in {@link #unplaced} (PDT 2 phase 5, owner ruling R1).
 * {@link #receipts} counts only the receipts of stores that are not struck, since a receipt is a claim and a
 * challenge is the check (principle 2 of §6.1).
 */
public final class Stores {

    private static final System.Logger LOG = System.getLogger(Stores.class.getName());

    /**
     * The pin attribute naming the stores a requester chose, comma separated: a store takes a pin that
     * names it or that names none. A rewritten pin keeps it.
     */
    public static final String STORES_ATTR = "pdt.stores";

    /**
     * The lease of a pin entry awaiting a taker: the pin's lease, at most this. The requester's signature lasts as
     * long ({@link SignedPin#notAfter}), so no store takes the pin, or a copy forwarded from it, later.
     */
    public static final Duration PIN_ENTRY_LEASE = Duration.ofHours(1);

    /**
     * The {@code access} key of a {@code content-store} card whose store takes no pins yet, with the value naming
     * what it waits for ({@code authorizer}: a {@code grant} store built without one, F2).
     */
    public static final String WAITING_ACCESS = "waiting";

    /** Failures in a row that strike a store (§3.2.4). */
    public static final int STRIKES = 2;

    /** The default challenge deadline ({@code agentspaces.store.challenge-deadline}). */
    public static final Duration DEFAULT_DEADLINE = Duration.ofSeconds(2);

    /**
     * One pin written.
     *
     * @param pin    the pin entry
     * @param chosen the cards of the stores the pin names
     */
    public record Pinning(StorePin pin, List<AssetCard> chosen) {
    }

    private final Content content;
    private final NodeStore store;
    private final DiscoveryService discovery;
    private final PeerIdentity identity;
    private final InstantSource clock;
    private final Duration deadline;
    private final SecureRandom random;
    /** Failures in a row by store and root, so a pass on one root does not hide a root the store lost. */
    private final Map<String, Integer> failures = new ConcurrentHashMap<>();
    private final Set<PeerId> struck = ConcurrentHashMap.newKeySet();
    /** The last pin this peer wrote per root, with the need it was chosen under. */
    private final Map<String, Pinned> pinned = new ConcurrentHashMap<>();
    /** The roots whose replacement after a strike found no fitting store, so this peer wrote no pin for them (R1). */
    private final Set<String> unplaced = ConcurrentHashMap.newKeySet();

    /** A pin and the need, sized and sealed from the root, that chose its stores. */
    private record Pinned(StorePin pin, StoreChooser.Need need) {
    }

    /**
     * A requester's view.
     *
     * @param content   the group's content swarm
     * @param store     the requester's node store, which holds the trees it pins
     * @param discovery the group's discovery, where the store cards are; null when the host gave none
     * @param identity  this peer's identity, which signs its pins; null when the host gave none
     * @param clock     the clock of leases and deadlines
     * @param deadline  the challenge deadline
     * @param random    the source of the picks; a store that could predict them would keep only those nodes
     */
    public Stores(Content content, NodeStore store, DiscoveryService discovery, PeerIdentity identity, InstantSource clock,
                  Duration deadline, SecureRandom random) {
        this.content = Objects.requireNonNull(content, "content");
        this.store = Objects.requireNonNull(store, "store");
        this.discovery = discovery;
        if (identity != null && !identity.peerId().equals(content.self())) {
            throw new IllegalArgumentException("the identity " + identity.peerId().display() + " is not the swarm's peer "
                    + content.self().display());
        }
        this.identity = identity;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        this.random = Objects.requireNonNull(random, "random");
    }

    /**
     * Chooses stores for a root and writes the pin (P2-S-11): a tree this peer holds complete, or an
     * artifact whose manifest it knows. The pin asks for {@code need.copies()} copies and names the chosen
     * stores; this peer publishes its holding of a tree so the stores can fetch it. Writing the same root
     * again renews the lease (P2-S-04): the stores whose live receipts name the root come first and need no
     * room, so a renewal reaches the stores that hold the root.
     *
     * @param root  the root
     * @param need  the copies, tier, price, and stores to avoid; its size and sealing are read from the root
     * @param lease how long the stores should hold the root
     * @return the pin and the chosen cards
     * @throws IllegalStateException when this peer does not hold the root, has no discovery or identity, or no store fits
     */
    public Pinning storeFor(String root, StoreChooser.Need need, Duration lease) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(need, "need");
        if (discovery == null) {
            throw new IllegalStateException("no discovery service: build the facade with Pdt.Builder.discovery");
        }
        if (identity == null) {
            throw new IllegalStateException("no identity to sign the pin: build the facade with Pdt.Builder.identity");
        }
        boolean tree = Cids.parse(root).codec() == Cids.DAG_CBOR;
        long bytes;
        boolean sealed;
        if (tree) {
            if (!store.complete(root)) {
                throw new IllegalStateException("tree root " + root + " is not complete in this peer's store");
            }
            bytes = 0;
            for (String cid : store.reachable(root)) {
                bytes += store.stored(cid).map(b -> (long) b.length).orElse(0L);
            }
            sealed = store.stored(root).map(b -> Kind.SN.equals(NodeCodec.kindOf(b))).orElse(false);
            if (!content.treeRoots().contains(root)) {
                content.holdTree(root);
            }
        } else {
            Manifest manifest = content.store().manifest(root).or(() -> content.spaces().manifest(root))
                    .orElseThrow(() -> new IllegalStateException("no manifest for artifact root " + root));
            bytes = manifest.size();
            sealed = Manifest.HASHING_SEALED.equals(manifest.hashing());
        }
        Set<PeerId> avoid = new java.util.HashSet<>(need.avoid());
        avoid.addAll(struck);
        StoreChooser.Need sized = new StoreChooser.Need(bytes, need.copies(), sealed, need.tier(), need.maxPrice(), avoid);
        List<AssetCard> cards = storeCards();
        Set<PeerId> holders = new java.util.HashSet<>();
        receipts(root).forEach(r -> holders.add(r.holder()));
        List<AssetCard> chosen = new ArrayList<>(StoreChooser.choose(cards.stream().filter(c -> holders.contains(c.issuer())).toList(),
                new StoreChooser.Need(0, need.copies(), sealed, need.tier(), need.maxPrice(), avoid), clock.instant()));
        if (chosen.size() < need.copies()) {
            Set<PeerId> rest = new java.util.HashSet<>(avoid);
            chosen.forEach(c -> rest.add(c.issuer()));
            chosen.addAll(StoreChooser.choose(cards, new StoreChooser.Need(bytes, need.copies() - chosen.size(), sealed,
                    need.tier(), need.maxPrice(), rest), clock.instant()));
        }
        if (chosen.isEmpty()) {
            throw new IllegalStateException("no content store fits " + bytes + " bytes of " + root + " (" + sized + ")");
        }
        StorePin pin = new StorePin(StorePin.VERSION, root, tree ? StorePin.KIND_TREE : StorePin.KIND_ARTIFACT,
                Math.max(1, lease.toSeconds()), chosen.size(), content.self(), List.of(), Map.of(STORES_ATTR, String.join(",", chosen.stream().map(c -> c.issuer().value()).toList())));
        write(pin, sized);
        unplaced.remove(root);
        return new Pinning(pin, List.copyOf(chosen));
    }

    /**
     * Challenges a store's possession of a root (P2-S-07): up to {@link Messages#MAX_PICKS} random nodes of a
     * tree this peer holds, or random pieces of an artifact, within the deadline. {@link #STRIKES} failures in
     * a row on the root strike the store, after which its receipts stop counting and this peer pins every root it
     * pinned there again with one copy, as the class comment says.
     *
     * @param root  the root
     * @param store the store
     * @return the outcome
     */
    public CompletableFuture<Content.ChallengeResult> challenge(String root, PeerId store) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(store, "store");
        List<String> candidates;
        int most = Messages.MAX_PICKS;
        if (Cids.parse(root).codec() == Cids.DAG_CBOR) {
            candidates = new ArrayList<>(this.store.reachable(root));
        } else {
            candidates = new ArrayList<>(content.pieceCids(root));
            int pieceSize = content.store().manifest(root).or(() -> content.spaces().manifest(root))
                    .map(Manifest::pieceSize).orElse(Messages.MAX_NODES_BYTES);
            most = Math.max(1, Math.min(most, (Messages.MAX_NODES_BYTES - 1024) / pieceSize));
        }
        if (candidates.isEmpty()) {
            throw new IllegalStateException("this peer holds nothing of " + root + " to pick from");
        }
        Collections.sort(candidates);
        Collections.shuffle(candidates, random);
        List<Messages.Pick> picks = candidates.subList(0, Math.min(most, candidates.size())).stream()
                .map(Messages.Pick::node).toList();
        return content.challenge(store, root, picks, deadline).thenApply(result -> {
            record(root, store, result.passed());
            return result;
        });
    }

    /**
     * The live receipts for a root that count toward its copies: one per store, signed by the holder it
     * names, unexpired, and not from a struck store.
     *
     * @param root the root
     * @return the receipts
     */
    public List<StoreReceipt> receipts(String root) {
        return claims(root).stream().filter(r -> !struck.contains(r.holder())).toList();
    }

    /** The live receipts for a root, one per store, signed by the holder each names, struck or not. */
    private List<StoreReceipt> claims(String root) {
        Space space = content.spaces().space();
        long now = clock.millis();
        Map<PeerId, StoreReceipt> out = new LinkedHashMap<>();
        for (Space.Issued<StoreReceipt> issued : space.readAllIssued(Template.of(StoreReceipt.class)
                .whereTag(StoreReceipt.ROOT_TAG, eq(root)), 256)) {
            StoreReceipt receipt = issued.entry();
            if (receipt.violation().isEmpty() && receipt.root().equals(root) && receipt.until() > now
                    && issued.issuer().peer().equals(receipt.holder())) {
                out.merge(receipt.holder(), receipt, (a, b) -> a.until() >= b.until() ? a : b);
            }
        }
        return List.copyOf(out.values());
    }

    private List<AssetCard> storeCards() {
        return discovery.find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind"))
                && !c.access().containsKey(WAITING_ACCESS));
    }

    /**
     * The roots this peer pinned on a store it struck and could place on no other (PDT 2 phase 5, owner ruling R1 of
     * srad/agentspaces-pdt2.md §10.11): the replacement found no fitting store, so this peer wrote no pin, and every
     * signed pin it writes names its stores. A later {@link #storeFor} that pins the root takes it off the list.
     *
     * @return the roots, a snapshot
     */
    public Set<String> unplaced() {
        return Set.copyOf(unplaced);
    }

    /** Whether this peer struck a store after failed challenges. */
    public boolean struck(PeerId store) {
        return struck.contains(store);
    }

    private void record(String root, PeerId holder, boolean passed) {
        String key = holder.value() + "|" + root;
        if (passed) {
            failures.remove(key);
            return;
        }
        int inARow = failures.merge(key, 1, Integer::sum);
        if (inARow < STRIKES) {
            return;
        }
        // the roots the store claimed, read before the strike hides its receipts
        List<Pinned> lost = pinned.values().stream()
                .filter(p -> claims(p.pin().root()).stream().anyMatch(r -> r.holder().equals(holder))).toList();
        if (struck.add(holder)) {
            lost.forEach(this::replace);
        }
    }

    /**
     * S3 of §6.4: one more copy of a root whose store was struck, chosen under the root's need from the stores
     * that neither hold the root nor were struck. The replacement is new terms, so this peer signs them as their
     * requester. When no store fits, this peer writes no pin and lists the root in {@link #unplaced} (PDT 2 phase 5,
     * owner ruling R1 of §10.11): a pin that named no store would let a copy of it reach every store before its
     * deadline, each charging the signer for a lease.
     */
    private void replace(Pinned last) {
        StorePin pin = last.pin();
        Set<PeerId> avoid = new java.util.HashSet<>(last.need().avoid());
        avoid.addAll(struck);
        claims(pin.root()).forEach(r -> avoid.add(r.holder()));
        StoreChooser.Need one = new StoreChooser.Need(last.need().bytes(), 1, last.need().sealed(), last.need().tier(),
                last.need().maxPrice(), avoid);
        List<AssetCard> replacement = discovery == null ? List.of() : StoreChooser.choose(storeCards(), one, clock.instant());
        if (replacement.isEmpty()) {
            unplaced.add(pin.root());
            LOG.log(System.Logger.Level.WARNING, "no content store fits a replacement copy of " + pin.root()
                    + " after a strike (" + one + "); no pin is written");
            return;
        }
        Map<String, String> attrs = new HashMap<>(pin.attrs() == null ? Map.of() : pin.attrs());
        attrs.put(STORES_ATTR, replacement.get(0).issuer().value());
        write(new StorePin(StorePin.VERSION, pin.root(), pin.kind(), pin.lease(), 1, pin.requester(), pin.haves(), attrs),
                last.need());
    }

    /** Signs the terms as their requester and writes the pin entry, which lapses when the signature does. */
    private void write(StorePin pin, StoreChooser.Need need) {
        Optional<String> bad = pin.violation();
        if (bad.isPresent()) {
            throw new IllegalArgumentException(bad.get());
        }
        Duration entry = Duration.ofSeconds(pin.lease()).compareTo(PIN_ENTRY_LEASE) < 0 ? Duration.ofSeconds(pin.lease()) : PIN_ENTRY_LEASE;
        SignedPin signed = SignedPin.sign(pin, content.group(), clock.millis() + entry.toMillis(), identity);
        content.spaces().space().write(signed, Lease.of(entry), signed.tags());
        pinned.put(pin.root(), new Pinned(pin, need));
    }
}
