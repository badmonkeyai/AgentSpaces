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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.ad.Advertisement;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The console's queryable model, folded from the three sources the fabric
 * already maintains: watched spaces (what is the fleet working on),
 * membership (who is alive), and discovery (who can do what). The view is a
 * passive observer: it subscribes to space notifications, counts what it
 * sees, and reads the rest on demand, so attaching a console changes nothing
 * about how a fleet coordinates.
 *
 * <p>Per watched space the view tracks entries written, entries completed
 * (both from notifications, so they include remote activity the replica
 * merges), and entries readable right now; work in progress is the
 * difference, {@code written - completed - queued}, which is exactly the set
 * of entries some agent's take lease holds. Worker attribution comes from
 * {@link Builder#results(String, Class, Function) result watching}: result
 * entries name their worker, and the view counts completions per worker from
 * them, the same trick example 09 introduced.
 *
 * <p>Every observation also lands in a bounded event ring for the SSE
 * stream. The ring keeps the last {@code eventCapacity} events with monotone
 * sequence numbers, which bounds console memory no matter how long the fleet
 * runs.
 */
public final class ConsoleView implements AutoCloseable {

    /** One watched space's live statistics. */
    public record SpaceStats(String name, int written, int completed, int queued,
                             int inProgress, Map<String, Integer> perWorker) {
    }

    /**
     * One group member as the console shows it. {@code channel} is the frame
     * authentication mode toward that peer (spec §5.6): {@code "attested"}
     * when frames ride an authenticated channel unsigned, {@code "signed"}
     * otherwise.
     */
    public record MemberRow(String peer, List<String> roles, boolean suspect,
                            List<String> endpoints, String channel) {
    }

    /** One advertisement as the console shows it. */
    public record AdRow(String kind, String title, String issuer, String detail) {
    }

    private static final int READ_LIMIT = 10_000;

    private final Map<String, Watched> watched;
    private final Supplier<List<GroupMembership.Member>> members;
    private final Supplier<java.util.Map<ai.badmonkey.agentspaces.common.id.PeerId, String>>
            channelModes;
    private final Supplier<List<? extends Advertisement>> ads;
    private final InstantSource clock;
    private final int eventCapacity;
    private final Deque<ConsoleEvent> events = new ArrayDeque<>();
    private final AtomicLong sequence = new AtomicLong();
    private final List<Subscription> subscriptions = new ArrayList<>();

    private ConsoleView(Builder builder) {
        this.watched = builder.watched;
        this.members = builder.members;
        this.channelModes = builder.channelModes;
        this.ads = builder.ads;
        this.clock = builder.clock;
        this.eventCapacity = builder.eventCapacity;
        Lease lease = Lease.of(Duration.ofDays(365));
        watched.forEach((name, w) -> {
            for (Class<?> type : w.taskTypes) {
                subscriptions.add(w.space.notify(Template.of(type), event ->
                        onTaskEvent(name, w, event), lease));
            }
            w.results.forEach((type, workerOf) ->
                    subscriptions.add(w.space.notify(Template.of(type), event ->
                            onResultEvent(name, w, workerOf, event), lease)));
        });
    }

    private void onTaskEvent(String name, Watched w, SpaceEvent<?> event) {
        switch (event.kind()) {
            case WRITTEN -> w.written.incrementAndGet();
            case COMPLETED -> w.completed.incrementAndGet();
            case TAKEN -> {
            }
        }
        record(name, event.kind().name().toLowerCase(java.util.Locale.ROOT),
                event.entry().getClass().getSimpleName(), "");
    }

    /** Longest worker name credited as-is; longer names truncate (ASF-031). */
    private static final int MAX_WORKER_NAME = 64;
    /** Most distinct workers tracked; overflow buckets to one label (ASF-031). */
    private static final int MAX_WORKERS = 1_024;
    private static final String OVERFLOW_WORKER = "(other)";

    private void onResultEvent(String name, Watched w,
                               Function<Object, String> workerOf, SpaceEvent<?> event) {
        if (event.kind() != SpaceEvent.Kind.WRITTEN) {
            return;
        }
        String worker = Objects.requireNonNullElse(workerOf.apply(event.entry()), "");
        if (!worker.isEmpty()) {
            // ASF-031: worker names come out of entry payloads any admitted
            // peer can write, so both the name length and the map cardinality
            // are bounded — a name-spray cannot grow the console without end.
            if (worker.length() > MAX_WORKER_NAME) {
                worker = worker.substring(0, MAX_WORKER_NAME);
            }
            if (!w.perWorker.containsKey(worker) && w.perWorker.size() >= MAX_WORKERS) {
                worker = OVERFLOW_WORKER;
            }
            w.perWorker.computeIfAbsent(worker, key -> new AtomicInteger()).incrementAndGet();
        }
        record(name, "completed", event.entry().getClass().getSimpleName(), worker);
    }

    private void record(String space, String kind, String entryType, String worker) {
        synchronized (events) {
            events.addLast(new ConsoleEvent(sequence.incrementAndGet(), clock.instant(),
                    space, kind, entryType, worker));
            while (events.size() > eventCapacity) {
                events.removeFirst();
            }
        }
    }

    /**
     * Records an operator command in the activity stream (kind {@code command}),
     * so C2 actions appear in the same feed as fleet activity. Called by
     * {@link FleetCommander} as commands execute.
     *
     * @param space    the control or target space the command acted on
     * @param action   the command action (a directive verb or command id)
     * @param operator the operator the command is attributed to
     */
    public void recordCommand(String space, String action, String operator) {
        record(space, "command", action, operator);
    }

    /** Live statistics for every watched space, in registration order. */
    public List<SpaceStats> spaces() {
        List<SpaceStats> stats = new ArrayList<>();
        watched.forEach((name, w) -> {
            int queued = 0;
            for (Class<?> type : w.taskTypes) {
                queued += w.space.readAll(Template.of(type), READ_LIMIT).size();
            }
            int written = w.written.get();
            int completed = w.completed.get();
            Map<String, Integer> perWorker = new TreeMap<>();
            w.perWorker.forEach((worker, count) -> perWorker.put(worker, count.get()));
            stats.add(new SpaceStats(name, written, completed, queued,
                    Math.max(0, written - completed - queued), perWorker));
        });
        return stats;
    }

    /** Completions per worker, folded across every watched space. */
    public Map<String, Integer> perWorker() {
        Map<String, Integer> folded = new TreeMap<>();
        watched.values().forEach(w -> w.perWorker.forEach((worker, count) ->
                folded.merge(worker, count.get(), Integer::sum)));
        return folded;
    }

    /** The group's live members, one row each. */
    public List<MemberRow> members() {
        var modes = channelModes.get();
        return members.get().stream().map(member -> new MemberRow(
                member.id().value(),
                member.roles().stream().map(Enum::name).sorted().toList(),
                member.suspect(),
                member.endpoints().stream()
                        .map(e -> e.transport() + "://" + e.address()).toList(),
                modes.getOrDefault(member.id(), "signed")))
                .toList();
    }

    /** The group's current advertisements, one row each. */
    public List<AdRow> ads() {
        return ads.get().stream().map(ConsoleView::toRow).toList();
    }

    private static AdRow toRow(Advertisement ad) {
        return switch (ad) {
            case AgentCard card -> new AdRow("agent", card.agent().localName(),
                    card.issuer().display(), card.actions() == null ? card.description()
                            : card.description() + " (actions: " + String.join(", ",
                                    card.actions().stream().map(
                                            ai.badmonkey.agentspaces.api.ad.CardAction::name).toList())
                            + ")");
            case AssetCard card -> new AdRow("asset", card.asset(),
                    card.issuer().display(), card.description() + " (" + card.uri() + ")");
            case CapabilityAdvertisement cap -> new AdRow("capability",
                    cap.capabilityType(), cap.issuer().display(),
                    "v" + cap.version() + " via " + cap.binding()
                            + (cap.parameters().containsKey("embedder")
                                    ? ", embedder " + cap.parameters().get("embedder")
                                            + (cap.parameters().containsKey("dimensions")
                                                    ? " (" + cap.parameters().get("dimensions") + " dimensions)" : "")
                                    : ""));
            case PeerAdvertisement peer -> new AdRow("peer",
                    peer.issuer().display(), peer.issuer().display(),
                    peer.roles().stream().map(Enum::name).sorted()
                            .reduce((a, b) -> a + " " + b).orElse(""));
            case SpaceAdvertisement space -> new AdRow("space", space.spaceName(),
                    space.issuer().display(), space.strategy().name());
            case GroupAdvertisement group -> new AdRow("group", group.name(),
                    group.issuer().display(), group.membershipPolicy().name());
            case ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement revocation ->
                    new AdRow("revocation", revocation.revoked().display(),
                            revocation.issuer().display(), revocation.reason()
                                    + (revocation.successor() == null ? ""
                                            : " -> " + revocation.successor().display()));
            case ai.badmonkey.agentspaces.api.ad.CredentialRevocation revocation ->
                    new AdRow("revocation", revocation.target().kind().toLowerCase() + " "
                            + revocation.target().canonical(),
                            revocation.issuer().display(), revocation.reason());
        };
    }

    /**
     * Events after the given sequence number, oldest first.
     *
     * @param afterSeq replay starts after this sequence; 0 replays the ring
     * @return the matching events
     */
    public List<ConsoleEvent> eventsAfter(long afterSeq) {
        synchronized (events) {
            return events.stream().filter(event -> event.seq() > afterSeq).toList();
        }
    }

    /** The most recent event's sequence number, 0 before any activity. */
    public long lastSeq() {
        return sequence.get();
    }

    /** Cancels the view's space subscriptions. */
    @Override
    public void close() {
        subscriptions.forEach(subscription -> {
            try {
                subscription.close();
            } catch (Exception e) {
                // Closing an already-lapsed subscription is fine.
            }
        });
    }

    /** Starts a builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Assembles a {@link ConsoleView}. */
    public static final class Builder {

        private final Map<String, Watched> watched = new LinkedHashMap<>();
        private Supplier<List<GroupMembership.Member>> members = List::of;
        private Supplier<java.util.Map<ai.badmonkey.agentspaces.common.id.PeerId, String>>
                channelModes = java.util.Map::of;
        private Supplier<List<? extends Advertisement>> ads = List::of;
        private InstantSource clock = InstantSource.system();
        private int eventCapacity = 512;

        private Builder() {
        }

        /**
         * Watches a space: entries of the given types are counted as fleet
         * work (written, completed, queued, in progress).
         *
         * @param name      the console-local space name
         * @param space     the space, typically the console peer's replica
         * @param taskTypes the entry types that constitute work
         * @return this builder
         */
        public Builder space(String name, Space space, Class<?>... taskTypes) {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(space, "space");
            Watched w = watched.computeIfAbsent(name, key -> new Watched(space));
            if (w.space != space) {
                throw new IllegalArgumentException("space '" + name + "' already watched");
            }
            for (Class<?> type : taskTypes) {
                w.taskTypes.add(Objects.requireNonNull(type, "taskType"));
            }
            return this;
        }

        /**
         * Attributes completions to workers: result entries of the given type
         * written into the named space name their worker through the
         * extractor, and the view counts them per worker.
         *
         * @param spaceName  a space registered with {@link #space}
         * @param resultType the result entry type
         * @param workerOf   extracts the worker name from a result entry
         * @return this builder
         */
        public Builder results(String spaceName, Class<?> resultType,
                               Function<Object, String> workerOf) {
            Watched w = watched.get(spaceName);
            if (w == null) {
                throw new IllegalArgumentException("unknown space '" + spaceName
                        + "'; register it with space() first");
            }
            w.results.put(Objects.requireNonNull(resultType, "resultType"),
                    Objects.requireNonNull(workerOf, "workerOf"));
            return this;
        }

        /**
         * Supplies the membership roster, typically
         * {@code runtime.membership()::allMembers}.
         *
         * @param members the live member source, called per request
         * @return this builder
         */
        /**
         * Supplies the per-peer channel-authentication modes (spec §5.6),
         * typically {@code node::channelModes}; peers absent from the map show
         * as {@code "signed"}.
         *
         * @param channelModes the mode supplier
         * @return this builder
         */
        public Builder channelModes(
                Supplier<java.util.Map<ai.badmonkey.agentspaces.common.id.PeerId, String>>
                        channelModes) {
            this.channelModes = java.util.Objects.requireNonNull(channelModes, "channelModes");
            return this;
        }

        public Builder members(Supplier<List<GroupMembership.Member>> members) {
            this.members = Objects.requireNonNull(members, "members");
            return this;
        }

        /**
         * Supplies the advertisement roster from a discovery service.
         *
         * @param discovery the console peer's discovery service
         * @return this builder
         */
        public Builder discovery(DiscoveryService discovery) {
            Objects.requireNonNull(discovery, "discovery");
            this.ads = () -> discovery.find(Advertisement.class, ad -> true);
            return this;
        }

        /**
         * Supplies the advertisement roster directly.
         *
         * @param ads the live advertisement source, called per request
         * @return this builder
         */
        public Builder ads(Supplier<List<? extends Advertisement>> ads) {
            this.ads = Objects.requireNonNull(ads, "ads");
            return this;
        }

        /**
         * Overrides the clock, for tests.
         *
         * @param clock the instant source stamped onto events
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Sizes the event ring (default 512).
         *
         * @param eventCapacity the most events kept for SSE replay
         * @return this builder
         */
        public Builder eventCapacity(int eventCapacity) {
            if (eventCapacity < 1) {
                throw new IllegalArgumentException("eventCapacity must be positive");
            }
            this.eventCapacity = eventCapacity;
            return this;
        }

        /** Builds the view and subscribes it to its spaces. */
        public ConsoleView build() {
            return new ConsoleView(this);
        }
    }

    private static final class Watched {
        private final Space space;
        private final List<Class<?>> taskTypes = new ArrayList<>();
        private final Map<Class<?>, Function<Object, String>> results = new LinkedHashMap<>();
        private final AtomicInteger written = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();
        private final Map<String, AtomicInteger> perWorker = new ConcurrentHashMap<>();

        private Watched(Space space) {
            this.space = space;
        }
    }
}
