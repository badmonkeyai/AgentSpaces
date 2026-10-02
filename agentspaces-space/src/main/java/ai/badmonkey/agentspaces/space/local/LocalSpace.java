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
package ai.badmonkey.agentspaces.space.local;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.error.SpaceClosedException;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.SpaceListener;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A complete single-JVM implementation of the {@link Space} API (plan §6): the
 * full lease semantics of the model, entirely in process. The Embabel extension,
 * the examples, and application tests all run against {@code LocalSpace} first;
 * the replicated space implements the same interface, so swapping it in later is
 * configuration, and replication stays orthogonal to application code.
 *
 * <p>Time is injected: lease expiry is evaluated against the configured
 * {@link InstantSource} lazily at every operation, so deterministic tests drive a
 * {@code TestClock} with no sleeping. For wall-clock deployments the builder can
 * additionally start a background sweeper so expiry events fire without traffic.
 *
 * <p>Listeners run on the mutating thread after the mutation commits; they must
 * return promptly (see {@link SpaceListener}).
 */
public final class LocalSpace implements Space, AutoCloseable {

    private enum State {
        AVAILABLE, TAKEN
    }

    private static final class Stored {
        EntryRecord record;
        Object entry;
        State state = State.AVAILABLE;
        long writeExpiryMillis;
        long takeExpiryMillis;
        long takeToken;
        /** Whether the writer was an agent with its own certified key (v0.1.13, {@link #as}). */
        boolean attested;
    }

    private final String name;
    private final SpaceId id;
    private final AgentId issuer;
    private final InstantSource clock;
    private final HybridLogicalClock hlc;
    private final CborCodec codec;
    private final SchemaRegistry schemas;
    private final ScheduledExecutorService sweeper;

    private final Object lock = new Object();
    private final Map<EntryId, Stored> entries = new HashMap<>();
    /**
     * The type-partitioned index (PERF1 phase 2): the same {@link Stored}
     * instances as {@code entries}, bucketed by entry class in insertion
     * order, so matching scans only the buckets a template's type accepts and
     * candidates within a type arrive first-in first-out.
     */
    private final Map<Class<?>, LinkedHashMap<EntryId, Stored>> buckets = new HashMap<>();
    private final List<Sub> subscriptions = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    private LocalSpace(Builder builder) {
        this.name = builder.name;
        this.id = SpaceId.local(builder.name);
        this.issuer = builder.issuer;
        this.clock = builder.clock;
        this.hlc = new HybridLogicalClock(builder.clock, builder.issuer.encoded());
        this.codec = CborCodec.defaultCodec();
        this.schemas = builder.schemas;
        if (builder.sweepInterval != null) {
            this.sweeper = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "local-space-sweeper-" + builder.name);
                thread.setDaemon(true);
                return thread;
            });
            long millis = builder.sweepInterval.toMillis();
            this.sweeper.scheduleAtFixedRate(this::sweepNow, millis, millis, TimeUnit.MILLISECONDS);
        } else {
            this.sweeper = null;
        }
    }

    /**
     * Starts building a space.
     *
     * @param name   the space name
     * @param issuer the agent identity local writes are attributed to
     * @return the builder
     */
    public static Builder builder(String name, AgentId issuer) {
        return new Builder(name, issuer);
    }

    /** Builder for {@link LocalSpace}. */
    public static final class Builder {
        private final String name;
        private final AgentId issuer;
        private InstantSource clock = InstantSource.system();
        private SchemaRegistry schemas = new SimpleSchemaRegistry();
        private Duration sweepInterval;

        private Builder(String name, AgentId issuer) {
            this.name = Objects.requireNonNull(name, "name");
            this.issuer = Objects.requireNonNull(issuer, "issuer");
        }

        /**
         * Injects the time source lease expiry is evaluated against.
         *
         * @param clock the time source
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Uses a specific schema registry.
         *
         * @param schemas the registry
         * @return this builder
         */
        public Builder schemaRegistry(SchemaRegistry schemas) {
            this.schemas = Objects.requireNonNull(schemas, "schemas");
            return this;
        }

        /**
         * Starts a background sweeper so expiry and reappearance events fire even
         * with no space traffic. Deterministic tests omit this and rely on lazy
         * expiry at each operation.
         *
         * @param interval the sweep period
         * @return this builder
         */
        public Builder sweepEvery(Duration interval) {
            this.sweepInterval = Objects.requireNonNull(interval, "interval");
            return this;
        }

        /** Builds the space. */
        public LocalSpace build() {
            return new LocalSpace(this);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public java.util.Optional<AgentId> writer() {
        return java.util.Optional.of(issuer);
    }

    @Override
    public SpaceId id() {
        return id;
    }

    @Override
    public <T> EntryHandle write(T entry, Lease lease) {
        return write(entry, lease, Map.of());
    }

    @Override
    public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
        return writeAs(issuer, false, entry, lease, tags);
    }

    /**
     * This space seen as another agent of the same peer (SPEC §4.2, v0.1.13): a
     * view whose writes, and results, are attributed to {@code actor}, and
     * reported {@code AGENT_ATTESTED} by {@link #readAllIssued} when the actor
     * has a key of its own. A local space signs nothing, so this is the
     * replicated space's attribution model without the cryptography, which is
     * what tests and the REPL need to see the same model.
     *
     * @param actor an agent of this space's peer
     * @return the view
     */
    public Space as(ai.badmonkey.agentspaces.api.spi.AgentIdentity actor) {
        Objects.requireNonNull(actor, "actor");
        if (!actor.id().peer().equals(issuer.peer())) {
            throw new IllegalArgumentException(actor.id().encoded() + " is not an agent of "
                    + issuer.peer().display());
        }
        return new View(actor.id(), actor.isSubordinate());
    }

    private <T> EntryHandle writeAs(AgentId writer, boolean attested, T entry, Lease lease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(tags, "tags");
        ensureOpen();
        List<Runnable> events;
        EntryId entryId = EntryId.newId();
        synchronized (lock) {
            events = sweepLocked();
            long now = nowMillis();
            Stored stored = new Stored();
            stored.entry = entry;
            stored.writeExpiryMillis = now + lease.duration().toMillis();
            stored.record = buildRecord(entryId, entry, stored.writeExpiryMillis, tags, writer);
            stored.attested = attested;
            indexPut(entryId, stored);
            events.add(eventsFor(SpaceEvent.Kind.WRITTEN, entryId, entry));
            lock.notifyAll();
        }
        deliver(events);
        return new Handle(entryId);
    }

    @Override
    public <T> Optional<T> read(Template<T> template) {
        Objects.requireNonNull(template, "template");
        ensureOpen();
        List<Runnable> events;
        Optional<T> result;
        synchronized (lock) {
            events = sweepLocked();
            result = firstMatchLocked(template);
        }
        deliver(events);
        return result;
    }

    @Override
    public <T> Optional<T> read(Template<T> template, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return awaiting(timeout, () -> read(template));
    }

    @Override
    public <T> List<T> readAll(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        ensureOpen();
        List<Runnable> events;
        List<T> results = new ArrayList<>();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Stored stored : bucket.values()) {
                    if (stored.state == State.AVAILABLE && template.matches(stored.entry)) {
                        results.add(template.type().cast(stored.entry));
                        if (results.size() == limit) {
                            break outer;
                        }
                    }
                }
            }
        }
        deliver(events);
        return results;
    }

    @Override
    public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        ensureOpen();
        List<Runnable> events;
        List<Issued<T>> results = new ArrayList<>();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Stored stored : bucket.values()) {
                    if (stored.state == State.AVAILABLE && template.matches(stored.entry)) {
                        results.add(new Issued<>(template.type().cast(stored.entry),
                                stored.record.issuer(), stored.attested
                                        ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED));
                        if (results.size() == limit) {
                            break outer;
                        }
                    }
                }
            }
        }
        deliver(events);
        return results;
    }

    @Override
    public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease, Duration timeout) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(takeLease, "takeLease");
        Objects.requireNonNull(timeout, "timeout");
        return awaiting(timeout, () -> tryTake(template, takeLease));
    }

    @Override
    public void complete(TakenEntry<?> taken) {
        completeInternal(taken, null, null);
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(resultLease, "resultLease");
        return completeInternal(taken, result, resultLease);
    }

    @Override
    public <T> Subscription notify(Template<T> template, SpaceListener<T> listener, Lease lease) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(lease, "lease");
        ensureOpen();
        Sub sub = new Sub(template, listener, nowMillis() + lease.duration().toMillis());
        subscriptions.add(sub);
        return sub;
    }

    /** Runs an expiry sweep now, firing any pending expiry or reappearance events. */
    public void sweepNow() {
        List<Runnable> events;
        synchronized (lock) {
            events = sweepLocked();
        }
        deliver(events);
    }

    /** Closes the space: pending blockers wake and further operations fail. */
    @Override
    public void close() {
        closed = true;
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    // ---------------------------------------------------------------- internals

    private <T> Optional<TakenEntry<T>> tryTake(Template<T> template, Lease takeLease) {
        ensureOpen();
        List<Runnable> events;
        Optional<TakenEntry<T>> result = Optional.empty();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Map.Entry<EntryId, Stored> candidate : bucket.entrySet()) {
                    Stored stored = candidate.getValue();
                    if (stored.state != State.AVAILABLE || !template.matches(stored.entry)) {
                        continue;
                    }
                    long now = nowMillis();
                    stored.state = State.TAKEN;
                    stored.takeExpiryMillis = now + takeLease.duration().toMillis();
                    stored.takeToken++;
                    stored.record = stored.record.withLease(
                            new LeaseInfo(issuer, stored.takeExpiryMillis, LeaseKind.TAKE));
                    T value = template.type().cast(stored.entry);
                    events.add(eventsFor(SpaceEvent.Kind.TAKEN, candidate.getKey(), stored.entry));
                    result = Optional.of(new Taken<>(candidate.getKey(), value, stored.takeToken));
                    break outer;
                }
            }
        }
        deliver(events);
        return result;
    }

    private EntryHandle completeInternal(TakenEntry<?> taken, Object result, Lease resultLease) {
        return completeInternal(taken, result, resultLease, null, false);
    }

    private EntryHandle completeInternal(TakenEntry<?> taken, Object result, Lease resultLease,
                                         AgentId resultWriter, boolean resultAttested) {
        Objects.requireNonNull(taken, "taken");
        ensureOpen();
        if (!(taken instanceof Taken<?> t)) {
            throw new IllegalArgumentException("foreign TakenEntry implementation: " + taken.getClass());
        }
        List<Runnable> events;
        EntryHandle resultHandle = null;
        EntryId resultId = result == null ? null : EntryId.newId();
        synchronized (lock) {
            events = sweepLocked();
            Stored stored = entries.get(t.entryId());
            if (stored == null || stored.state != State.TAKEN || stored.takeToken != t.token) {
                throw new LeaseExpiredException(
                        "take lease lapsed for entry " + t.entryId() + "; the entry reappeared or expired");
            }
            // Spec §7.2 atomicity: build and validate the result record BEFORE the
            // completion commits, so a result that cannot be serialized never leaves
            // the entry completed-but-resultless.
            Stored written = null;
            if (result != null) {
                long now = nowMillis();
                written = new Stored();
                written.entry = result;
                written.writeExpiryMillis = now + resultLease.duration().toMillis();
                written.record = buildRecord(resultId, result, written.writeExpiryMillis,
                        Map.of(), resultWriter == null ? issuer : resultWriter);
                written.attested = resultAttested;
            }
            indexRemove(t.entryId(), stored);
            events.add(eventsFor(SpaceEvent.Kind.COMPLETED, t.entryId(), stored.entry));
            if (written != null) {
                indexPut(resultId, written);
                events.add(eventsFor(SpaceEvent.Kind.WRITTEN, resultId, result));
                resultHandle = new Handle(resultId);
            }
            lock.notifyAll();
        }
        deliver(events);
        return resultHandle;
    }

    /**
     * Polls an operation until it yields a value or the timeout elapses, waking on
     * every space mutation. Blocking is real-time (wall clock); lease expiry stays
     * on the injected clock.
     */
    private <V> Optional<V> awaiting(Duration timeout, java.util.function.Supplier<Optional<V>> poll) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Optional<V> result = poll.get();
            if (result.isPresent()) {
                return result;
            }
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0 || closed) {
                return Optional.empty();
            }
            synchronized (lock) {
                try {
                    long waitMillis = Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNanos) + 1, 50L);
                    lock.wait(waitMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        }
    }

    private <T> Optional<T> firstMatchLocked(Template<T> template) {
        for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
            for (Stored stored : bucket.values()) {
                if (stored.state == State.AVAILABLE && template.matches(stored.entry)) {
                    return Optional.of(template.type().cast(stored.entry));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * The buckets a template's type accepts (PERF1 phase 2). The common case,
     * a concrete entry class, resolves to its single bucket; a supertype or
     * interface template walks the few distinct entry classes in use and
     * collects the assignable ones. Callers hold the lock.
     */
    private List<LinkedHashMap<EntryId, Stored>> bucketsFor(Class<?> type) {
        LinkedHashMap<EntryId, Stored> exact = buckets.get(type);
        List<LinkedHashMap<EntryId, Stored>> assignable = null;
        for (Map.Entry<Class<?>, LinkedHashMap<EntryId, Stored>> bucket : buckets.entrySet()) {
            if (bucket.getKey() != type && type.isAssignableFrom(bucket.getKey())) {
                if (assignable == null) {
                    assignable = new ArrayList<>(2);
                    if (exact != null) {
                        assignable.add(exact);
                    }
                }
                assignable.add(bucket.getValue());
            }
        }
        if (assignable != null) {
            return assignable;
        }
        return exact == null ? List.of() : List.of(exact);
    }

    /** Adds an entry to both the id map and its type bucket. Callers hold the lock. */
    private void indexPut(EntryId entryId, Stored stored) {
        entries.put(entryId, stored);
        buckets.computeIfAbsent(stored.entry.getClass(), type -> new LinkedHashMap<>())
                .put(entryId, stored);
    }

    /** Removes an entry from both maps, dropping emptied buckets. Callers hold the lock. */
    private void indexRemove(EntryId entryId, Stored stored) {
        entries.remove(entryId);
        bucketRemove(entryId, stored);
    }

    private void bucketRemove(EntryId entryId, Stored stored) {
        Class<?> type = stored.entry.getClass();
        LinkedHashMap<EntryId, Stored> bucket = buckets.get(type);
        if (bucket != null && bucket.remove(entryId) != null && bucket.isEmpty()) {
            buckets.remove(type);
        }
    }

    /** Applies lease lapses. Callers hold the lock; returned events run after release. */
    private List<Runnable> sweepLocked() {
        List<Runnable> events = new ArrayList<>();
        long now = nowMillis();
        var iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            Stored stored = e.getValue();
            if (stored.writeExpiryMillis <= now) {
                iterator.remove();
                bucketRemove(e.getKey(), stored);
                events.add(eventsFor(SpaceEvent.Kind.EXPIRED, e.getKey(), stored.entry));
                continue;
            }
            if (stored.state == State.TAKEN && stored.takeExpiryMillis <= now) {
                stored.state = State.AVAILABLE;
                stored.takeToken++;
                stored.record = stored.record.withLease(
                        new LeaseInfo(issuer, stored.writeExpiryMillis, LeaseKind.WRITE));
                events.add(eventsFor(SpaceEvent.Kind.REAPPEARED, e.getKey(), stored.entry));
            }
        }
        subscriptions.removeIf(sub -> sub.expiryMillis <= now || sub.closedFlag);
        if (!events.isEmpty()) {
            lock.notifyAll();
        }
        return events;
    }

    private EntryRecord buildRecord(EntryId entryId, Object entry, long writeExpiry,
                                    Map<String, String> tags) {
        return buildRecord(entryId, entry, writeExpiry, tags, issuer);
    }

    private EntryRecord buildRecord(EntryId entryId, Object entry, long writeExpiry,
                                    Map<String, String> tags, AgentId writer) {
        String schemaName = schemas.register(entry.getClass());
        byte[] payload = codec.toBytes(entry);
        String payloadRef = null;
        if (payload.length > EntryRecord.INLINE_PAYLOAD_LIMIT) {
            payloadRef = Multibase.base58btc(Digests.sha256(payload));
            payload = null;
        }
        return new EntryRecord(entryId, id, schemaName, payload, payloadRef, writer,
                hlc.now(), new LeaseInfo(writer, writeExpiry, LeaseKind.WRITE), tags, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Runnable eventsFor(SpaceEvent.Kind kind, EntryId entryId, Object entry) {
        // Every local write is attributed to this space's issuer (buildRecord),
        // so the event's writer identity is the space issuer by construction.
        return () -> {
            for (Sub sub : subscriptions) {
                if (!sub.closedFlag && sub.template.matches(entry)) {
                    sub.listener.onEvent(new SpaceEvent(kind, entryId, entry, issuer));
                }
            }
        };
    }

    private static void deliver(List<Runnable> events) {
        for (Runnable event : events) {
            event.run();
        }
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    private void ensureOpen() {
        if (closed) {
            throw new SpaceClosedException("space '" + name + "' is closed");
        }
    }

    private final class Handle implements EntryHandle {
        private final EntryId entryId;

        private Handle(EntryId entryId) {
            this.entryId = entryId;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            ensureOpen();
            List<Runnable> events;
            synchronized (lock) {
                events = sweepLocked();
                Stored stored = entries.get(entryId);
                if (stored == null) {
                    throw new LeaseExpiredException("write lease lapsed for entry " + entryId);
                }
                stored.writeExpiryMillis = nowMillis() + extension.toMillis();
            }
            deliver(events);
        }

        @Override
        public void cancel() {
            List<Runnable> events;
            synchronized (lock) {
                events = sweepLocked();
                Stored stored = entries.get(entryId);
                if (stored != null) {
                    indexRemove(entryId, stored);
                    events.add(eventsFor(SpaceEvent.Kind.EXPIRED, entryId, stored.entry));
                    lock.notifyAll();
                }
            }
            deliver(events);
        }
    }

    private final class Taken<T> implements TakenEntry<T> {
        private final EntryId entryId;
        private final T value;
        private final long token;

        private Taken(EntryId entryId, T value, long token) {
            this.entryId = entryId;
            this.value = value;
            this.token = token;
        }

        @Override
        public T entry() {
            return value;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            ensureOpen();
            List<Runnable> events;
            synchronized (lock) {
                events = sweepLocked();
                Stored stored = entries.get(entryId);
                if (stored == null || stored.state != State.TAKEN || stored.takeToken != token) {
                    throw new LeaseExpiredException("take lease lapsed for entry " + entryId);
                }
                stored.takeExpiryMillis = nowMillis() + extension.toMillis();
            }
            deliver(events);
        }
    }

    private final class Sub implements Subscription {
        private final Template<?> template;
        @SuppressWarnings("rawtypes")
        private final SpaceListener listener;
        private volatile long expiryMillis;
        private volatile boolean closedFlag;

        private Sub(Template<?> template, SpaceListener<?> listener, long expiryMillis) {
            this.template = template;
            this.listener = listener;
            this.expiryMillis = expiryMillis;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            if (closedFlag || expiryMillis <= nowMillis()) {
                throw new LeaseExpiredException("subscription lease lapsed");
            }
            expiryMillis = nowMillis() + extension.toMillis();
        }

        @Override
        public void close() {
            closedFlag = true;
            subscriptions.remove(this);
        }
    }

    /** The space as one agent of the same peer: its writes and results are that agent's. */
    private final class View implements Space {
        private final AgentId actor;
        private final boolean attested;

        private View(AgentId actor, boolean attested) {
            this.actor = actor;
            this.attested = attested;
        }

        @Override public String name() { return LocalSpace.this.name(); }
        @Override public SpaceId id() { return LocalSpace.this.id(); }
        @Override public java.util.Optional<AgentId> writer() { return java.util.Optional.of(actor); }
        @Override public <T> EntryHandle write(T entry, Lease lease) {
            return writeAs(actor, attested, entry, lease, Map.of());
        }
        @Override public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
            return writeAs(actor, attested, entry, lease, tags);
        }
        @Override public <T> Optional<T> read(Template<T> template) { return LocalSpace.this.read(template); }
        @Override public <T> Optional<T> read(Template<T> template, Duration timeout) {
            return LocalSpace.this.read(template, timeout);
        }
        @Override public <T> List<T> readAll(Template<T> template, int limit) {
            return LocalSpace.this.readAll(template, limit);
        }
        @Override public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
            return LocalSpace.this.readAllIssued(template, limit);
        }
        @Override public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease,
                                                          Duration timeout) {
            return LocalSpace.this.take(template, takeLease, timeout);
        }
        @Override public void complete(TakenEntry<?> taken) { LocalSpace.this.complete(taken); }
        @Override public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(resultLease, "resultLease");
            return completeInternal(taken, result, resultLease, actor, attested);
        }
        @Override public <T> Subscription notify(Template<T> template, SpaceListener<T> listener,
                                                 Lease lease) {
            return LocalSpace.this.notify(template, listener, lease);
        }
    }
}
