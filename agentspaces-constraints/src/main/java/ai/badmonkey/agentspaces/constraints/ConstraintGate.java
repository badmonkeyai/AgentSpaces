/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.constraints;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The gate (ISSUE-Constraints §9.4, FR-7 to FR-11): loads the newest complete version per book from the
 * {@code constraints} space (a {@code Vocabulary} and every {@code Constraint} carrying its hash, from an issuer
 * the authorizer permits to write the scope), keeps one key's events from the lanes it watches (seeded from what
 * the replica holds, then from each lane's notifications, so a take it cannot read back still counts; a lane is read
 * again when a version or an interest names a new type), and answers
 * two questions: may this entry be claimed, and may this result be written. A refusal writes a
 * {@link ConstraintViolated} (24 h lease) once per unit, key, and entry, and logs one line. The gate admits
 * everything when no version is loaded, when the entry names no key it can read, when the space is unreachable,
 * or when the engine throws (FR-9): a rule never stops a worker.
 */
public final class ConstraintGate implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ConstraintGate.class.getName());
    public static final Duration VIOLATION_LEASE = Duration.ofHours(24);
    private static final Duration SUBSCRIPTION = Duration.ofHours(1);

    /** The gate's answer. */
    public record Decision(boolean admitted, String unit, String key, String reason) {
        public static final Decision ADMIT = new Decision(true, "", "", "");

        public static Decision refuse(String unit, String key, String reason) {
            return new Decision(false, unit, key, reason);
        }
    }

    private final Space constraints;
    private final Authorizer authorizer;
    private final String scope;
    private final InstantSource clock;
    private final String by;
    private final int cacheKeys;
    private final Map<String, Version> versions = new ConcurrentHashMap<>();
    private final Map<String, List<Event>> eventsByKey = new LinkedHashMap<>();
    private final List<Subscription> subscriptions = new ArrayList<>();
    private final Map<String, Space> lanes = new LinkedHashMap<>();
    /** The types the lanes were read for, so a lane is read again only when a version or an interest names a new one. */
    private final Set<String> seededTypes = new LinkedHashSet<>();
    private final Set<String> violated = new LinkedHashSet<>();
    /**
     * The takes of a type nothing names yet, kept for a later version or interest (bounded): a lane no longer shows a
     * taken entry. A write is not kept here: the lane shows it while it lives and is read again when its type is named
     * (FIT 2026-11-08: keeping every write held 4096 geo views with their forty-point tracks, about 200,000 maps, in
     * each agent of the geo watch while no book named a View).
     */
    private final ArrayDeque<Event> pending = new ArrayDeque<>();
    private volatile long lastWarn;
    private volatile boolean closed;
    /** The {@link #onChange} listeners: a gated take waits on these and its space, not on a timer (EL-W-07). */
    private final List<Runnable> changeListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** How many claim judgements this gate made ({@link #claimJudgements()}). */
    private final java.util.concurrent.atomic.AtomicLong claimJudgements = new java.util.concurrent.atomic.AtomicLong();

    private ConstraintGate(Space constraints, Authorizer authorizer, String scope, InstantSource clock, String by, int cacheKeys) {
        this.constraints = constraints;
        this.authorizer = authorizer;
        this.scope = scope;
        this.clock = clock;
        this.by = by;
        this.cacheKeys = cacheKeys;
    }

    /**
     * Attaches to the group's {@code constraints} space; {@code authorizer} may be null (every issuer trusted),
     * {@code by} names the gate in the violations it writes.
     */
    public static ConstraintGate attach(Space constraints, Authorizer authorizer, String scope, InstantSource clock, String by) {
        return attach(constraints, authorizer, scope, clock, by, 4096);
    }

    public static ConstraintGate attach(Space constraints, Authorizer authorizer, String scope, InstantSource clock, String by,
                                        int cacheKeys) {
        ConstraintGate gate = new ConstraintGate(Objects.requireNonNull(constraints, "constraints"), authorizer,
                scope == null ? "constraints" : scope, Objects.requireNonNull(clock, "clock"), by == null ? "gate" : by, cacheKeys);
        gate.reload();
        try {
            gate.subscriptions.add(constraints.notify(Template.of(Object.class), ev -> gate.reload(), Lease.of(SUBSCRIPTION)));
        } catch (RuntimeException e) {
            gate.warn("constraints space does not notify: " + e);
        }
        return gate;
    }

    /** Watches a lane: its entries now are the keys' past, its events keep them current. */
    public synchronized void watch(String name, Space lane) {
        if (lanes.putIfAbsent(name, lane) != null) {
            return;
        }
        try {
            seed(name, lane);
            subscriptions.add(lane.notify(Template.of(Object.class), ev -> onLane(ev), Lease.of(SUBSCRIPTION)));
        } catch (RuntimeException e) {
            warn("lane " + name + " cannot be watched: " + e);
        }
        signalChange(); // a lane's past can enable a claim a take was refused
    }

    /** Records the entries a lane holds now (the events of the types nothing names are dropped by {@link #record}). */
    private void seed(String name, Space lane) {
        try {
            for (Space.Entry<Object> e : lane.readAllEntries(Template.of(Object.class), 5000)) {
                record(Event.of(e, Event.Kind.WRITTEN, clock.instant()), e.entryId().value());
            }
        } catch (RuntimeException e) {
            warn("lane " + name + " cannot be read: " + e);
        }
    }

    /**
     * Brings the keys' past up to the types named now (FIT 2026-11-08): when a version or an interest names a type the
     * lanes were not read for, every lane is read again (an event already held is not recorded twice), then the takes
     * kept for a later version are replayed.
     */
    private void catchUp() {
        Set<String> named = new LinkedHashSet<>(interests.keySet());
        for (Version v : versions.values()) {
            for (Constraint c : v.constraints()) {
                named.add(Event.simpleName(c.a()));
                if (!c.b().isBlank()) {
                    named.add(Event.simpleName(c.b()));
                }
            }
        }
        if (!seededTypes.containsAll(named)) {
            seededTypes.addAll(named);
            lanes.forEach(this::seed);
        }
        List<Event> replay = new ArrayList<>(pending);
        pending.clear();
        for (Event e : replay) {
            record(e, e.entryId());
        }
    }

    /** Keeps the subscriptions alive; call it from the owner's sweep. */
    public synchronized void renew() {
        for (Subscription s : subscriptions) {
            try {
                s.renew(SUBSCRIPTION);
            } catch (RuntimeException ignored) {
                // a lapsed one is re-made on the next attach
            }
        }
    }

    private void onLane(SpaceEvent<Object> ev) {
        // A completion counts as the take it ends (RBE regression probe P9, 2026-11-14): a member that joins after an
        // entry was taken and completed learns it already completed, so it hears COMPLETED and never WRITTEN or TAKEN.
        // Ignoring it left that gate without the entry, and a precedence's B behind a consumed A was refused there
        // while every member that watched it live admitted it. Where the TAKEN was heard, this is the same event again
        // and is not recorded twice.
        Event.Kind kind = switch (ev.kind()) {
            case WRITTEN, REAPPEARED -> Event.Kind.WRITTEN;
            case TAKEN, COMPLETED -> Event.Kind.CLAIMED;
            default -> null;
        };
        if (kind == null) {
            return;
        }
        Instant now = clock.instant();
        Event e;
        if (ev.details() != null) {
            e = Event.of(ev.details(), kind, now);
        } else {
            // a local space's event carries no view: the value, the id, and the tags are enough
            String wire = ev.entry() == null ? "" : ev.entry().getClass().getName();
            e = Event.of(wire, ev.entryId().value(), now, ev.entry(), ev.tags(), kind);
        }
        if (ev.kind() == SpaceEvent.Kind.TAKEN) {
            // a take is stamped when it is heard; a completion heard alone keeps its entry's issue stamp, the one place
            // in issue order a gate that watched the entry live would have its WRITTEN
            e = new Event(e.type(), e.wireType(), e.entryId(), now, e.fields(), e.tags(), kind);
        }
        record(e, ev.entryId().value());
        signalChange(); // a lane's event can enable a claim a take was refused (EL-W-07)
    }

    /** A watcher's interest beyond the constraints (a rule agent's reactions and goals): a type and its key spec. */
    public synchronized void interest(String type, String key) {
        interests.put(Event.simpleName(type), key == null || key.isBlank() ? "field:claimId" : key);
        catchUp();
    }

    private final Map<String, String> interests = new LinkedHashMap<>();

    private boolean named(String type) {
        if (interests.containsKey(type)) {
            return true;
        }
        for (Version v : versions.values()) {
            if (v.names(type)) {
                return true;
            }
        }
        return false;
    }

    private synchronized void record(Event e, String entryId) {
        Set<String> keySpecs = new LinkedHashSet<>();
        for (Version v : versions.values()) {
            if (v.names(e.type())) {
                v.constraints().forEach(c -> keySpecs.add(c.key()));
            }
        }
        if (interests.containsKey(e.type())) {
            keySpecs.add(interests.get(e.type()));
        }
        for (String spec : keySpecs) {
            String key = e.keyValue(spec);
            if (key == null) {
                continue;
            }
            List<Event> list = eventsByKey.computeIfAbsent(key, k -> new ArrayList<>());
            boolean dup = list.stream().anyMatch(x -> x.entryId().equals(entryId) && x.kind() == e.kind());
            if (!dup) {
                list.add(e);
            }
        }
        while (eventsByKey.size() > cacheKeys) {
            eventsByKey.remove(eventsByKey.keySet().iterator().next());
        }
        // a take of a type nothing names yet is kept for a later version or interest (bounded); a write is read again
        // from its lane when its type is named (FIT 2026-11-08)
        if (e.kind() == Event.Kind.CLAIMED && !named(e.type())) {
            pending.add(e);
            if (pending.size() > cacheKeys) {
                pending.removeFirst();
            }
        }
    }

    /** Re-reads the versions (FR-7): the newest complete one per book, from permitted issuers only (SR-1). */
    public synchronized void reload() {
        if (closed) {
            return;
        }
        try {
            Map<String, Vocabulary> vocabularies = new LinkedHashMap<>();
            for (Space.Entry<Vocabulary> e : constraints.readAllEntries(Template.of(Vocabulary.class), 500)) {
                if (!permitted(e.issuer())) {
                    continue;
                }
                Vocabulary v = e.value();
                Vocabulary current = vocabularies.get(v.book());
                if (current == null || v.version() > current.version()) {
                    vocabularies.put(v.book(), v);
                }
            }
            Map<String, List<Constraint>> byHash = new LinkedHashMap<>();
            for (Space.Entry<Constraint> e : constraints.readAllEntries(Template.of(Constraint.class), 2000)) {
                if (permitted(e.issuer())) {
                    String hash = e.tags().getOrDefault(RuleTags.CHARTER_VERSION, "");
                    byHash.computeIfAbsent(hash, h -> new ArrayList<>()).add(e.value());
                }
            }
            Map<String, Version> loaded = new LinkedHashMap<>();
            vocabularies.forEach((book, v) -> {
                List<Constraint> cs = byHash.getOrDefault(v.hash(), List.of()).stream()
                        .filter(c -> c.book().equals(book) && c.version() == v.version()).toList();
                loaded.put(book, new Version(v, cs));
            });
            boolean changed = !versions.equals(loaded);
            versions.clear();
            versions.putAll(loaded);
            catchUp();
            if (changed) {
                for (Runnable listener : reloadListeners) {
                    try {
                        listener.run();
                    } catch (RuntimeException e) {
                        warn("a reload listener failed: " + e);
                    }
                }
            }
        } catch (RuntimeException e) {
            warn("cannot read constraints: " + e);
        }
        // every reload follows a change to the constraints space: a version, and also a jury decision, which the
        // judgement reads there and which leaves the versions as they were (EL-W-07)
        signalChange();
    }

    private boolean permitted(AgentId issuer) {
        if (authorizer == null || issuer == null) {
            return true;
        }
        try {
            return authorizer.permits(issuer, Authorizer.Operation.SPACE_WRITE, scope);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private final java.util.List<Runnable> reloadListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Runs a listener whenever a reload changes the loaded versions (idle-loop item 2): a binder whose takes wait long
     * while no rule is loaded looks again at once when rules arrive. The listener runs on the reloading thread and must
     * be quick.
     *
     * @param listener the listener
     */
    public void onReload(Runnable listener) {
        reloadListeners.add(java.util.Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Registers a listener run after every change that can move a claim judgement (srad/agentspaces-event-loop.md
     * EL-W-07): an event of a watched lane, a lane watched, and a reload (a version, or a jury decision written to the
     * constraints space). A gated take waits on this and on its own space's changes, up to {@link #nextClaimDue()},
     * instead of looking on a timer. The listener runs on the thread that made the change, possibly under this gate's
     * lock, so it must only wake (a {@code ChangeSignal}): it must not block and must not call back into the gate.
     *
     * @param listener the listener
     * @return a handle that removes it
     */
    public AutoCloseable onChange(Runnable listener) {
        Runnable l = Objects.requireNonNull(listener, "listener");
        changeListeners.add(l);
        return () -> changeListeners.remove(l);
    }

    private void signalChange() {
        for (Runnable l : changeListeners) {
            try {
                l.run();
            } catch (RuntimeException e) {
                warn("a change listener failed: " + e);
            }
        }
    }

    /**
     * When a claim judgement can next change with no event at all, or empty for never (srad/agentspaces-event-loop.md
     * EL-W-07: a gated take waits for a change or this, under the one backstop). No claim judgement moves with time
     * alone today: a precedence, a not-coexistence and a cardinality read only the key's events, a response refuses
     * nothing (it reports an absence), a jury decision arrives as a write to the constraints space, and the
     * vocabulary's conditions ({@code older than}, {@code within}) are reported in the marking, not enforced at a
     * claim. Each of those inputs signals {@link #onChange}. A template whose refusal moved with time would report its
     * next flip here.
     *
     * @return the next instant a claim judgement changes by itself, or empty
     */
    public java.util.Optional<Instant> nextClaimDue() {
        return java.util.Optional.empty();
    }

    /**
     * How many claim judgements ({@link #beforeClaim}) this gate made: a gated take that waits on changes makes one
     * per look, so an idle one makes a few a minute, not one a second.
     *
     * @return the count
     */
    public long claimJudgements() {
        return claimJudgements.get();
    }

    public Collection<Version> loaded() {
        return List.copyOf(versions.values());
    }

    /** The keys the gate holds events for. */
    public synchronized Set<String> keys() {
        return new LinkedHashSet<>(eventsByKey.keySet());
    }

    /** The events the gate holds for a key. */
    public synchronized List<Event> events(String key) {
        return List.copyOf(eventsByKey.getOrDefault(key, List.of()));
    }

    /** How many events the gate holds, every key's and those it keeps for a type nothing names yet (FIT 2026-11-08 lifecycle tests). */
    synchronized int heldEvents() {
        int n = 0;
        for (List<Event> list : eventsByKey.values()) {
            n += list.size();
        }
        return n + pending.size();
    }

    /** The marking of a key under one loaded version, from the events the gate holds (for a console or a test). */
    public synchronized Marking marking(String book, String key) {
        Version v = versions.get(book);
        return v == null ? null : MarkingEngine.mark(v, key, events(key), clock.instant(), decisions(book, key));
    }

    private List<JuryDecision> decisions(String book, String key) {
        try {
            return constraints.readAll(Template.of(JuryDecision.class), 500).stream()
                    .filter(d -> d.book().equals(book) && d.key().equals(key)).toList();
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    /** The template narrowed to the candidate's key, so the take that follows the look claims the same key. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Template<?> narrow(Template<?> template, Space.Entry<?> candidate) {
        Event e = Event.of(candidate, Event.Kind.CLAIMED, clock.instant());
        for (Version v : versions.values()) {
            for (Constraint c : v.constraints()) {
                if (!c.gated() || c.key().isBlank()) {
                    continue;
                }
                String simple = e.type();
                if (!Event.simpleName(c.a()).equals(simple) && !Event.simpleName(c.b()).equals(simple)) {
                    continue;
                }
                String value = e.keyValue(c.key());
                if (value == null) {
                    continue;
                }
                if (c.key().startsWith("tag:")) {
                    return ((Template) template).whereTag(c.key().substring(4), ai.badmonkey.agentspaces.api.space.Matchers.eq(value));
                }
                String field = c.key().startsWith("field:") ? c.key().substring(6) : c.key();
                try {
                    return ((Template) template).where(field, ai.badmonkey.agentspaces.api.space.Matchers.eq(value));
                } catch (RuntimeException ex) {
                    return template; // the field is a tag on this entry: the plain template stands
                }
            }
        }
        return template;
    }

    /** May this entry be claimed (FR-8)? */
    public Decision beforeClaim(Space.Entry<?> candidate) {
        claimJudgements.incrementAndGet();
        if (candidate == null || versions.isEmpty()) {
            return Decision.ADMIT;
        }
        try {
            Event e = Event.of(candidate, Event.Kind.CLAIMED, clock.instant());
            return decide(e, candidate.entryId().value(), ConstraintViolated.CLAIM);
        } catch (RuntimeException ex) {
            warn("gate failed open before a claim: " + ex);
            return Decision.ADMIT;
        }
    }

    /** May this result be written (FR-8)? {@code wireType} is the type the registry names it, or null for the class name. */
    public Decision beforeResult(Object result, String wireType, Map<String, String> tags) {
        if (result == null || versions.isEmpty()) {
            return Decision.ADMIT;
        }
        try {
            Event e = Event.of(result, wireType == null ? result.getClass().getName() : wireType, tags, clock.instant(), Event.Kind.WRITTEN);
            return decide(e, "", ConstraintViolated.RESULT);
        } catch (RuntimeException ex) {
            warn("gate failed open before a result: " + ex);
            return Decision.ADMIT;
        }
    }

    private synchronized Decision decide(Event e, String entryId, String eventKind) {
        for (Version v : versions.values()) {
            if (!v.names(e.type())) {
                continue;
            }
            for (Constraint c : v.constraints()) {
                if (!c.gated()) {
                    continue;
                }
                String simple = e.type();
                // the unit judges this entry only when it is the unit's A or B under the unit's guard
                boolean isA = Guards.matches(e, c.a(), Guards.parse(c.aGuard()));
                boolean isB = !c.b().isBlank() && Guards.matches(e, c.b(), Guards.parse(c.bGuard()));
                if (!isA && !isB) {
                    continue;
                }
                String key = e.keyValue(c.key());
                if (key == null) {
                    continue; // the entry names no key the gate can read: admitted (FR-9)
                }
                Version single = new Version(v.vocabulary(), List.of(c));
                Marking m = MarkingEngine.mark(single, key, events(key), clock.instant(), decisions(v.book(), key));
                if (!m.allows(simple)) {
                    String reason = m.explain(simple);
                    refuse(v, c, key, entryId, eventKind);
                    return Decision.refuse(c.unit(), key, reason.isBlank() ? simple + " is not enabled" : reason);
                }
            }
        }
        return Decision.ADMIT;
    }

    private void refuse(Version v, Constraint c, String key, String entryId, String eventKind) {
        String mark = c.unit() + "|" + key + "|" + entryId + "|" + eventKind;
        if (!violated.add(mark)) {
            return;
        }
        if (violated.size() > cacheKeys) {
            violated.remove(violated.iterator().next());
        }
        LOG.log(System.Logger.Level.INFO, "constraint {0} of {1} refuses {2} for key {3}", c.unit(), v.book(), eventKind, key);
        try {
            constraints.write(new ConstraintViolated(ConstraintViolated.VERSION, v.book(), v.vocabulary().version(), c.unit(), key,
                    clock.instant(), by, eventKind), Lease.of(VIOLATION_LEASE), c.tags(v.hash()));
        } catch (RuntimeException e) {
            warn("cannot write the violation: " + e);
        }
    }

    private void warn(String message) {
        long now = clock.millis();
        if (now - lastWarn > 10_000) {
            lastWarn = now;
            LOG.log(System.Logger.Level.WARNING, "constraint gate: {0} (admitting)", message);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        subscriptions.forEach(s -> {
            try {
                s.close();
            } catch (RuntimeException ignored) {
                // closing
            }
        });
        subscriptions.clear();
    }
}
