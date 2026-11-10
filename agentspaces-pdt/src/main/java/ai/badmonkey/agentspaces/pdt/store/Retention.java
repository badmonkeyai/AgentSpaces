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
package ai.badmonkey.agentspaces.pdt.store;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The retained versions per ref (srad/agentspaces-pdt.md §3.4; PD-S-03): for each ref
 * name the store keeps the last {@code versions} roots (64 by default,
 * {@code agentspaces.pdt.retain.versions}) or every root younger than {@code age}
 * (one hour by default, {@code agentspaces.pdt.retain.age}), whichever keeps more.
 * A retained root serves as a delta base: a member that still holds it fetches only
 * what a newer version added. {@link #retainedRoots()} is one of the three mark
 * sources of {@link NodeCollector}; the live heads and the pins are the others.
 *
 * <p>Age is measured against the clock given at construction, so a test can move
 * time; {@link #retainedRoots(long)} takes an explicit now. A version that falls
 * outside both rules is forgotten on the next record for its ref.
 *
 * <p>Over a {@link MetaStore} the versions persist (PDT 2 phase 1, P2-V-03): the
 * retention reads every {@value #META_PREFIX} record when it is built, and
 * {@link #flush} writes the record of each ref that changed since the last flush,
 * named by the hex of the sha-256 of the ref name's UTF-8 bytes, so a restart keeps the delta
 * bases and the collector's third mark source. {@code Pdt} flushes on its tick and
 * on close.
 */
public final class Retention {

    /** The default count of versions kept per ref. */
    public static final int DEFAULT_VERSIONS = 64;

    /** The default age kept per ref. */
    public static final Duration DEFAULT_AGE = Duration.ofHours(1);

    /** One recorded version. */
    public record Version(String root, long recordedMillis) { }

    /** The prefix of the meta record of one ref's versions (P2-V-03). */
    public static final String META_PREFIX = "retention:";

    /**
     * The meta record of one ref.
     *
     * @param v        the format version, 1
     * @param ref      the ref name
     * @param versions the versions, oldest first; empty once the ref is forgotten
     */
    record Persisted(int v, String ref, List<Version> versions) { }

    private static final ai.badmonkey.agentspaces.common.codec.CborCodec CODEC =
            ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();

    private final int versions;
    private final long ageMillis;
    private final InstantSource clock;
    private final Map<String, Deque<Version>> history = new ConcurrentHashMap<>();
    private final MetaStore meta;
    /** Refs recorded or forgotten since the last {@link #flush}. */
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();

    /**
     * A retention with explicit limits.
     *
     * @param versions how many versions to keep per ref regardless of age; at least 1
     * @param age      the age under which every version is kept; zero keeps by count alone
     * @param clock    the clock age is measured against
     */
    public Retention(int versions, Duration age, InstantSource clock) {
        this(versions, age, clock, null);
    }

    /**
     * A retention whose versions persist in a meta store, starting from what it holds (P2-V-03).
     *
     * @param versions how many versions to keep per ref regardless of age; at least 1
     * @param age      the age under which every version is kept; zero keeps by count alone
     * @param clock    the clock age is measured against
     * @param meta     where the versions persist; null keeps them in memory only
     */
    public Retention(int versions, Duration age, InstantSource clock, MetaStore meta) {
        if (versions < 1) {
            throw new IllegalArgumentException("versions must be at least 1");
        }
        Objects.requireNonNull(age, "age");
        if (age.isNegative()) {
            throw new IllegalArgumentException("age must not be negative");
        }
        this.versions = versions;
        this.ageMillis = age.toMillis();
        this.clock = Objects.requireNonNull(clock, "clock");
        this.meta = meta;
        if (meta != null) {
            for (String name : meta.metaNames()) {
                if (name.startsWith(META_PREFIX)) {
                    meta.meta(name).ifPresent(bytes -> {
                        Persisted stored = CODEC.fromBytes(bytes, Persisted.class);
                        if (!stored.versions().isEmpty()) {
                            history.put(stored.ref(), new ArrayDeque<>(stored.versions()));
                        }
                    });
                }
            }
        }
    }

    /** Writes the record of every ref that changed since the last flush; nothing without a meta store. */
    public void flush() {
        if (meta == null) {
            return;
        }
        for (String ref : List.copyOf(dirty)) {
            dirty.remove(ref);
            Deque<Version> deque = history.get(ref);
            List<Version> versionsOfRef;
            if (deque == null) {
                versionsOfRef = List.of();
            } else {
                synchronized (deque) {
                    versionsOfRef = List.copyOf(deque);
                }
            }
            // the sha-256 of the name keeps a 256-byte ref name inside the meta name bound (PDT 2 phase 1, P2-V-03)
            String name = META_PREFIX + java.util.HexFormat.of().formatHex(
                    ai.badmonkey.agentspaces.common.crypto.Digests.sha256(ref.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            meta.putMeta(name, CODEC.toBytes(new Persisted(1, ref, versionsOfRef)));
        }
    }

    /** The default limits, 64 versions or one hour, on a clock. */
    public static Retention defaults(InstantSource clock) {
        return new Retention(DEFAULT_VERSIONS, DEFAULT_AGE, clock);
    }

    /** The default limits on the system clock. */
    public static Retention defaults() {
        return defaults(InstantSource.system());
    }

    /** The count kept per ref. */
    public int versions() {
        return versions;
    }

    /** The age kept per ref. */
    public Duration age() {
        return Duration.ofMillis(ageMillis);
    }

    /**
     * Records that a ref reached a root.
     *
     * @param refName   the ref
     * @param root      the root CID
     * @param nowMillis when the version was written
     */
    public void recordVersion(String refName, String root, long nowMillis) {
        Objects.requireNonNull(refName, "refName");
        Objects.requireNonNull(root, "root");
        history.compute(refName, (name, versionsOfRef) -> {
            Deque<Version> deque = versionsOfRef == null ? new ArrayDeque<>() : versionsOfRef;
            deque.addLast(new Version(root, nowMillis));
            prune(deque, nowMillis);
            return deque;
        });
        dirty.add(refName);
    }

    /** Every retained root across every ref, as of the clock. */
    public Set<String> retainedRoots() {
        return retainedRoots(clock.millis());
    }

    /**
     * Every retained root across every ref as of a time.
     *
     * @param nowMillis the time age is measured against
     * @return the roots, in order of ref then of recording
     */
    public Set<String> retainedRoots(long nowMillis) {
        Set<String> roots = new LinkedHashSet<>();
        for (Map.Entry<String, Deque<Version>> entry : history.entrySet()) {
            synchronized (entry.getValue()) {
                for (Version version : retained(entry.getValue(), nowMillis)) {
                    roots.add(version.root());
                }
            }
        }
        return Collections.unmodifiableSet(roots);
    }

    /**
     * The retained versions of one ref as of the clock, oldest first.
     *
     * @param refName the ref
     * @return the versions, empty when the ref is unknown
     */
    public List<Version> versions(String refName) {
        return versions(refName, clock.millis());
    }

    /**
     * The retained versions of one ref as of a time, oldest first.
     *
     * @param refName   the ref
     * @param nowMillis the time age is measured against
     * @return the versions, empty when the ref is unknown
     */
    public List<Version> versions(String refName, long nowMillis) {
        Deque<Version> deque = history.get(refName);
        if (deque == null) {
            return List.of();
        }
        synchronized (deque) {
            return List.copyOf(retained(deque, nowMillis));
        }
    }

    /** The ref names recorded. */
    public Set<String> refs() {
        return Set.copyOf(history.keySet());
    }

    /**
     * Forgets every version of a ref, for a ref that was deleted.
     *
     * @param refName the ref
     * @return whether the ref was known
     */
    public boolean forget(String refName) {
        boolean known = history.remove(refName) != null;
        if (known) {
            dirty.add(refName);
        }
        return known;
    }

    /** Drops the versions that neither rule keeps; the deque is held under compute. */
    private void prune(Deque<Version> deque, long nowMillis) {
        synchronized (deque) {
            while (deque.size() > versions && !young(deque.peekFirst(), nowMillis)) {
                deque.pollFirst();
            }
        }
    }

    private List<Version> retained(Deque<Version> deque, long nowMillis) {
        List<Version> all = new ArrayList<>(deque);
        int firstByCount = Math.max(0, all.size() - versions);
        List<Version> kept = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            Version version = all.get(i);
            if (i >= firstByCount || young(version, nowMillis)) {
                kept.add(version);
            }
        }
        return kept;
    }

    private boolean young(Version version, long nowMillis) {
        return version != null && nowMillis - version.recordedMillis() < ageMillis;
    }
}
