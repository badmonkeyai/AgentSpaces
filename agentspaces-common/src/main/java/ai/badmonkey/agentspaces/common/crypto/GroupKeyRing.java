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
package ai.badmonkey.agentspaces.common.crypto;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongConsumer;

/**
 * A group's content keys by epoch (SPEC §11a.3, v0.1.13, TODO-9-10-11 workstream D):
 * epoch 0 is the configured key, and each rotation mints the next epoch with a
 * cutover instant. Readers open an entry with the key of the epoch its record
 * names; writers seal under the newest epoch whose cutover has passed. Epoch
 * keys are kept, not pruned, because write leases renew without limit and an
 * entry sealed under any epoch may still be live (review B-3).
 *
 * <p>Two authorized rotators can race to mint the same epoch number with
 * different keys (review B-2). The ring keeps every key it is given for an
 * epoch, opens with whichever authenticates, and writes with the canonical one:
 * the key whose rotator's SHA-256 of {@code epoch|rotator} sorts lowest, which
 * every replica computes identically.
 *
 * <p>A writer that knows a newer epoch has cut over but does not hold its key
 * keeps writing the older epoch only for {@link #writerGrace()} after that
 * cutover; past it, sealing is refused (review M-9), so no writer extends an
 * epoch indefinitely. A missing epoch is reported to the missing-epoch
 * listeners (the content-key agent fetches it), at most once per epoch per
 * second.
 */
public final class GroupKeyRing {

    /** The most epochs a ring holds; refusal past it is loud. */
    public static final int MAX_EPOCHS = 4096;

    /** One epoch: its keys by rotator, and when writers switch to it. */
    private static final class Epoch {
        final long number;
        final Map<String, GroupKey> byRotator = new ConcurrentHashMap<>();
        /** The rotator's signed commitment to each key, opaque here, served onward with it. */
        final Map<String, byte[]> proofs = new ConcurrentHashMap<>();
        volatile Instant cutover;

        Epoch(long number, Instant cutover) {
            this.number = number;
            this.cutover = cutover;
        }
    }

    private final TreeMap<Long, Epoch> epochs = new TreeMap<>();
    /** Epoch announcements seen (cutover per epoch) whose key may not be held yet. */
    private final TreeMap<Long, Instant> announced = new TreeMap<>();
    private final List<LongConsumer> missingListeners = new CopyOnWriteArrayList<>();
    private final Map<Long, Long> lastMissingReport = new ConcurrentHashMap<>();
    private volatile Duration writerGrace = Duration.ofHours(1);

    private GroupKeyRing() {
    }

    /**
     * A ring holding one key as epoch 0, today's single content key.
     *
     * @param epochZero the configured content key
     * @return the ring
     */
    public static GroupKeyRing of(GroupKey epochZero) {
        GroupKeyRing ring = new GroupKeyRing();
        ring.install(0, "", epochZero, Instant.EPOCH);
        return ring;
    }

    /** An empty ring, for a member that has yet to fetch any epoch. */
    public static GroupKeyRing empty() {
        return new GroupKeyRing();
    }

    /**
     * Installs an epoch key minted by {@code rotator}, effective for writers at
     * {@code cutover}. A key already held for that epoch and rotator is kept.
     *
     * @param epoch   the epoch number, 0 or more
     * @param rotator the minting rotator's PeerID value ("" for epoch 0)
     * @param key     the epoch key
     * @param cutover when writers switch to this epoch
     * @return whether the ring changed
     */
    public boolean install(long epoch, String rotator, GroupKey key, Instant cutover) {
        return install(epoch, rotator, key, cutover, null);
    }

    /**
     * As {@link #install(long, String, GroupKey, Instant)}, keeping the
     * rotator's proof (an opaque signed commitment the key distributor
     * verifies and serves onward with the key).
     *
     * @param epoch   the epoch number, 0 or more
     * @param rotator the minting rotator's PeerID value ("" for epoch 0)
     * @param key     the epoch key
     * @param cutover when writers switch to this epoch
     * @param proof   the rotator's proof, or null
     * @return whether the ring changed
     */
    public boolean install(long epoch, String rotator, GroupKey key, Instant cutover, byte[] proof) {
        boolean changed = installLocked(epoch, rotator, key, cutover, proof);
        if (changed) {
            Held held = new Held(epoch, rotator, key, cutover(epoch).orElse(cutover), proof);
            for (java.util.function.Consumer<Held> listener : installListeners) {
                listener.accept(held);
            }
        }
        return changed;
    }

    private synchronized boolean installLocked(long epoch, String rotator, GroupKey key,
                                               Instant cutover, byte[] proof) {
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be non-negative: " + epoch);
        }
        Objects.requireNonNull(rotator, "rotator");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(cutover, "cutover");
        Epoch entry = epochs.get(epoch);
        if (entry == null) {
            if (epochs.size() >= MAX_EPOCHS) {
                throw new IllegalStateException("content-key ring holds " + MAX_EPOCHS
                        + " epochs; no more can be installed");
            }
            entry = new Epoch(epoch, cutover);
            epochs.put(epoch, entry);
        } else if (cutover.isBefore(entry.cutover)) {
            entry.cutover = cutover; // the earliest announced cutover stands
        }
        boolean added = entry.byRotator.putIfAbsent(rotator, key) == null;
        if (added && proof != null) {
            entry.proofs.put(rotator, proof.clone());
        }
        return added;
    }

    /**
     * Records that an epoch has been announced (its key may not be held yet),
     * which is what bounds writers still sealing under an older epoch.
     *
     * @param epoch   the announced epoch
     * @param cutover its cutover
     */
    public synchronized void announce(long epoch, Instant cutover) {
        announced.merge(epoch, cutover, (a, b) -> a.isBefore(b) ? a : b);
        if (!epochs.containsKey(epoch)) {
            reportMissing(epoch);
        }
    }

    /**
     * The epoch a writer seals under at {@code now}: the newest held epoch whose
     * cutover has passed.
     *
     * @param now the writer's clock
     * @return the epoch
     * @throws IllegalStateException when the ring holds no usable epoch, or a
     *         newer epoch cut over more than {@link #writerGrace()} ago and its
     *         key is still missing
     */
    public synchronized long writeEpoch(Instant now) {
        Long chosen = null;
        for (Epoch entry : epochs.descendingMap().values()) {
            if (!entry.cutover.isAfter(now)) {
                chosen = entry.number;
                break;
            }
        }
        if (chosen == null) {
            throw new IllegalStateException("no content-key epoch is in effect yet");
        }
        for (Map.Entry<Long, Instant> newer : announced.tailMap(chosen, false).entrySet()) {
            if (!epochs.containsKey(newer.getKey())
                    && now.isAfter(newer.getValue().plus(writerGrace))) {
                reportMissing(newer.getKey());
                throw new IllegalStateException("content key epoch " + chosen
                        + " retired; awaiting epoch " + newer.getKey());
            }
        }
        return chosen;
    }

    /**
     * The canonical key to seal under for an epoch: with several rotators'
     * keys, the one whose {@code sha256(epoch|rotator)} sorts lowest.
     *
     * @param epoch the epoch
     * @return the key, or empty when the epoch is not held
     */
    public synchronized Optional<GroupKey> sealingKey(long epoch) {
        Epoch entry = epochs.get(epoch);
        if (entry == null || entry.byRotator.isEmpty()) {
            return Optional.empty();
        }
        String best = null;
        String bestDigest = null;
        for (String rotator : entry.byRotator.keySet()) {
            String digest = digest(epoch + "|" + rotator);
            if (bestDigest == null || digest.compareTo(bestDigest) < 0) {
                best = rotator;
                bestDigest = digest;
            }
        }
        return Optional.of(entry.byRotator.get(best));
    }

    /**
     * Every key held for an epoch, to try when opening (a race between
     * rotators leaves more than one); reports the epoch missing when none is held.
     *
     * @param epoch the epoch a record names
     * @return the keys, possibly empty
     */
    public List<GroupKey> openingKeys(long epoch) {
        Epoch entry;
        synchronized (this) {
            entry = epochs.get(epoch);
        }
        if (entry == null || entry.byRotator.isEmpty()) {
            reportMissing(epoch);
            return List.of();
        }
        return List.copyOf(entry.byRotator.values());
    }

    /**
     * Reports that the keys held for an epoch open nothing a record sealed
     * under it (SPEC §11a.3, v0.1.13): another rotator's key for the same epoch
     * exists that this ring lacks, as after two authorized rotators raced to
     * mint it. The missing-epoch listeners are told, so the key agent fetches
     * the epoch again, which brings every rotator's key a holder has.
     *
     * @param epoch the epoch whose held keys did not open a record
     */
    public void reportUnopenable(long epoch) {
        reportMissing(epoch);
    }

    /** The epochs held, ascending. */
    public synchronized Set<Long> epochs() {
        return Collections.unmodifiableSet(new java.util.TreeSet<>(epochs.keySet()));
    }

    /** The newest epoch held, or -1 for an empty ring. */
    public synchronized long newestEpoch() {
        return epochs.isEmpty() ? -1 : epochs.lastKey();
    }

    /** An epoch's cutover, when held or announced. */
    public synchronized Optional<Instant> cutover(long epoch) {
        Epoch entry = epochs.get(epoch);
        if (entry != null) {
            return Optional.of(entry.cutover);
        }
        return Optional.ofNullable(announced.get(epoch));
    }

    /**
     * Every key held, as (epoch, rotator, key, cutover), for persistence and serving.
     *
     * @return the held keys, ascending by epoch
     */
    public synchronized List<Held> held() {
        List<Held> all = new ArrayList<>();
        for (Epoch entry : epochs.values()) {
            entry.byRotator.forEach((rotator, key) ->
                    all.add(new Held(entry.number, rotator, key, entry.cutover,
                            entry.proofs.get(rotator))));
        }
        return all;
    }

    /** One held key. */
    public record Held(long epoch, String rotator, GroupKey key, Instant cutover, byte[] proof) {
    }

    private final List<java.util.function.Consumer<Held>> installListeners = new CopyOnWriteArrayList<>();

    /**
     * Adds a listener told of every newly installed key (for persistence),
     * outside the ring's lock.
     *
     * @param listener the listener
     */
    public void onInstalled(java.util.function.Consumer<Held> listener) {
        installListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    private volatile boolean persistent;

    /** Marks the ring as saved on every install, so minted epochs survive a restart. */
    public void markPersistent() {
        persistent = true;
    }

    /** Whether a store saves this ring's installs (see {@link #markPersistent()}). */
    public boolean persistent() {
        return persistent;
    }

    /** How long after a newer epoch's cutover a writer may still seal under an older one. */
    public Duration writerGrace() {
        return writerGrace;
    }

    /**
     * Sets the writer grace (review M-9).
     *
     * @param grace the grace; zero refuses older epochs at the cutover
     */
    public void writerGrace(Duration grace) {
        if (grace.isNegative()) {
            throw new IllegalArgumentException("grace must not be negative: " + grace);
        }
        this.writerGrace = grace;
    }

    /**
     * Adds a listener told the epoch number whenever a needed epoch is missing.
     *
     * @param listener the listener
     */
    public void onMissingEpoch(LongConsumer listener) {
        missingListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    private void reportMissing(long epoch) {
        long now = System.nanoTime();
        boolean[] due = {false};
        // Atomic per epoch, and bounded: the epoch numbers come from member-signed
        // records, so the map must not grow with whatever numbers they name.
        if (lastMissingReport.size() >= MAX_EPOCHS && !lastMissingReport.containsKey(epoch)) {
            lastMissingReport.clear();
        }
        lastMissingReport.compute(epoch, (k, last) -> {
            if (last != null && now - last < 1_000_000_000L) {
                return last;
            }
            due[0] = true;
            return now;
        });
        if (!due[0]) {
            return;
        }
        for (LongConsumer listener : missingListeners) {
            listener.accept(epoch);
        }
    }

    private static String digest(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
