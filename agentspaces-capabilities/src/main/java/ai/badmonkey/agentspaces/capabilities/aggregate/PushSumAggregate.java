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
package ai.badmonkey.agentspaces.capabilities.aggregate;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;

/**
 * The {@code aspace:cap/aggregate} capability (spec §8): gossip aggregation over
 * the group's capability pipes. Every participant joins an epoch with a local
 * contribution; each {@link #tick()} exchanges state with one randomly sampled
 * member; every node's estimate converges to the fleet-wide result in O(log N)
 * rounds. This is the primitive for quorum sensing (fleet battery, backlog
 * depth, spend).
 *
 * <p>Operators ({@link Mode}) and the per-node contribution each one gossips:
 * <ul>
 * <li>{@code AVG}: push-sum. Each participant holds {@code (value, weight)} =
 * {@code (local, 1)}; a tick halves the mass and pushes the other half as a
 * {@code Share}; the estimate is {@code value/weight}, the fleet mean. Mass
 * conservation makes it exact in the limit.</li>
 * <li>{@code SUM}: the {@code AVG} push-sum plus a participant roster (the set
 * of participants, gossiped by union). The estimate is {@code mean × |roster|}.</li>
 * <li>{@code COUNT}: the roster alone in value mode (the local value is
 * ignored; the estimate is the number of participants). In template mode each
 * participant contributes its local match count through push-sum and the
 * estimate is {@code mean(local counts) × |roster|}, the total number of
 * matching entries across the fleet's local views.</li>
 * <li>{@code MIN} / {@code MAX}: max-gossip. Each participant holds its local
 * extremum, pushes it whole (no halving) and keeps the min or max of what it
 * sees; idempotent, so duplicates and reordering are harmless.</li>
 * <li>{@code QUANTILE}: a fixed-bucket histogram over a declared
 * {@code [lo, hi]} range, pushed like a preference vector (push-sum on the
 * bucket vector, values outside the range clamp into the edge buckets). The
 * estimate is the point at which the linearly interpolated cumulative
 * distribution reaches {@code q}, so it is exact to within one bucket.</li>
 * </ul>
 *
 * <p>Template mode ({@link #start(String, Mode, Space, Template, ToDoubleFunction)})
 * aggregates a numeric field of the entries each node sees locally
 * ({@code space.readAll(template, 10 000)}): SUM contributes the sum of the
 * field over local matches, COUNT the number of local matches, AVG the pair
 * (local sum, local count) so the estimate is the entry-weighted fleet mean,
 * MIN/MAX the local extremum (a node with no matches contributes nothing), and
 * QUANTILE one histogram unit per local match. Over a fully replicated,
 * unsharded space every replica holds the same entries, so SUM and COUNT
 * multiply by the participant count; use them over sharded spaces or
 * per-node local spaces, and prefer AVG/MIN/MAX/QUANTILE (which are invariant to
 * replication) when every node sees the whole space.
 *
 * <p>Wire: one {@code Frame} per push carrying exactly one of {@code Share
 * {epochId, value, weight}} (push-sum), {@code Extremum{epochId, max, value}},
 * {@code Histogram{epochId, lo, hi, buckets}}, or {@code Roster{epochId,
 * members}} (8-byte participant tokens, the first eight bytes of
 * {@code sha-256(peerId)}). The {@code AVG} denominator of template mode
 * travels as an ordinary push-sum epoch named {@code <epochId>#n}.
 */
public final class PushSumAggregate implements CapabilityProvider {

    /** The capability type URI. */
    public static final String TYPE = "aspace:cap/aggregate";

    /** How many local entries template mode reads per epoch. */
    public static final int TEMPLATE_READ_LIMIT = 10_000;

    /** Rounding slack when a cumulative histogram mass is compared with {@code q}. */
    private static final double CDF_EPSILON = 1e-9;

    /** The aggregation operators (spec §8: {@code sum|avg|count|min|max|quantile}). */
    public enum Mode {
        /** Fleet total: mean × participant count. */
        SUM,
        /** Fleet mean (push-sum); the default and the pre-operator behaviour. */
        AVG,
        /** Participant count (value mode) or total matching entries (template mode). */
        COUNT,
        /** Fleet minimum (max-gossip). */
        MIN,
        /** Fleet maximum (max-gossip). */
        MAX,
        /**
         * A quantile of the fleet's values; needs a declared range, so use the
         * {@link Quantile} overloads of {@code start}.
         */
        QUANTILE
    }

    /**
     * The {@link Mode#QUANTILE} declaration: which quantile to read and the
     * histogram it is read from.
     *
     * @param q       the quantile in (0, 1); 0.5 is the median
     * @param lo      the lower edge of the histogram range
     * @param hi      the upper edge of the histogram range; greater than {@code lo}
     * @param buckets the number of equal-width buckets; positive
     */
    public record Quantile(double q, double lo, double hi, int buckets) {
        /** The default bucket count for {@link #of(double, double, double)}. */
        public static final int DEFAULT_BUCKETS = 100;

        public Quantile {
            if (!(q > 0.0 && q < 1.0)) {
                throw new IllegalArgumentException("q must be in (0,1): " + q);
            }
            if (!(hi > lo)) {
                throw new IllegalArgumentException("range needs hi > lo: [" + lo + "," + hi + "]");
            }
            if (buckets <= 0) {
                throw new IllegalArgumentException("buckets must be positive: " + buckets);
            }
        }

        /**
         * A quantile over {@link #DEFAULT_BUCKETS} equal-width buckets.
         *
         * @param q  the quantile in (0, 1)
         * @param lo the lower edge of the range
         * @param hi the upper edge of the range
         * @return the declaration
         */
        public static Quantile of(double q, double lo, double hi) {
            return new Quantile(q, lo, hi, DEFAULT_BUCKETS);
        }

        /** Returns the width of one bucket, the resolution of the estimate. */
        public double width() {
            return (hi - lo) / buckets;
        }

        int bucketOf(double value) {
            if (value <= lo) {
                return 0;
            }
            if (value >= hi) {
                return buckets - 1;
            }
            return Math.min(buckets - 1, (int) Math.floor((value - lo) / width()));
        }
    }

    // ------------------------------------------------------------------ wire

    private record Share(String epochId, double value, double weight) {
    }

    private record Extremum(String epochId, boolean max, double value) {
    }

    private record Histogram(String epochId, double lo, double hi, double[] buckets) {
    }

    private record Roster(String epochId, long[] members) {
    }

    /** Exactly one field is set per frame. */
    private record Frame(Share share, Extremum extremum, Histogram histogram, Roster roster) {
        static Frame of(Share s) {
            return new Frame(s, null, null, null);
        }

        static Frame of(Extremum e) {
            return new Frame(null, e, null, null);
        }

        static Frame of(Histogram h) {
            return new Frame(null, null, h, null);
        }

        static Frame of(Roster r) {
            return new Frame(null, null, null, r);
        }
    }

    // ----------------------------------------------------------------- state

    private static final class Epoch {
        volatile double value;
        volatile double weight;

        synchronized void add(double v, double w) {
            value += v;
            weight += w;
        }

        synchronized Share halve(String epochId) {
            value /= 2;
            weight /= 2;
            return new Share(epochId, value, weight);
        }
    }

    private static final class ExtremumState {
        final boolean max;
        /** NaN until this node holds any value. */
        volatile double value = Double.NaN;

        ExtremumState(boolean max) {
            this.max = max;
        }

        synchronized void merge(double incoming) {
            if (Double.isNaN(incoming)) {
                return;
            }
            if (Double.isNaN(value)) {
                value = incoming;
            } else {
                value = max ? Math.max(value, incoming) : Math.min(value, incoming);
            }
        }
    }

    private static final class HistogramState {
        final double lo;
        final double hi;
        final double[] buckets;

        HistogramState(double lo, double hi, int size) {
            this.lo = lo;
            this.hi = hi;
            this.buckets = new double[size];
        }

        synchronized void add(double[] incoming) {
            if (incoming.length != buckets.length) {
                return; // a different declaration of the same epoch id; ignore
            }
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] += incoming[i];
            }
        }

        synchronized Histogram halve(String epochId) {
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] /= 2;
            }
            return new Histogram(epochId, lo, hi, buckets.clone());
        }

        synchronized double[] snapshot() {
            return buckets.clone();
        }
    }

    private static final class RosterState {
        private final TreeSet<Long> members = new TreeSet<>();

        synchronized void add(long[] incoming) {
            for (long m : incoming) {
                members.add(m);
            }
        }

        synchronized long[] snapshot() {
            long[] out = new long[members.size()];
            int i = 0;
            for (long m : members) {
                out[i++] = m;
            }
            return out;
        }

        synchronized int size() {
            return members.size();
        }
    }

    /** What this node declared when it joined an epoch (absent for epochs only heard about). */
    private record Declared(Mode mode, Quantile quantile, boolean template) {
    }

    private final CapabilityPipes pipes;
    private final PeerSampler sampler;
    private final PeerId self;
    private final long selfToken;
    private final CborCodec codec;
    private final InstantSource clock;
    private final Map<String, Epoch> epochs = new ConcurrentHashMap<>();
    private final Map<String, ExtremumState> extrema = new ConcurrentHashMap<>();
    private final Map<String, HistogramState> histograms = new ConcurrentHashMap<>();
    private final Map<String, RosterState> rosters = new ConcurrentHashMap<>();
    private final Map<String, Declared> declared = new ConcurrentHashMap<>();

    /**
     * Creates the aggregator.
     *
     * @param pipes   the group's capability pipes
     * @param sampler the group's peer sampler
     * @param self    the local peer id (used in the advertisement)
     * @param codec   the CBOR codec
     * @param clock   the time source for advertisement freshness
     */
    public PushSumAggregate(CapabilityPipes pipes, PeerSampler sampler, PeerId self,
                            CborCodec codec, InstantSource clock) {
        this.pipes = Objects.requireNonNull(pipes, "pipes");
        this.sampler = Objects.requireNonNull(sampler, "sampler");
        this.self = Objects.requireNonNull(self, "self");
        this.selfToken = tokenOf(self);
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        pipes.onCapability(TYPE, this::onFrame);
    }

    /**
     * The 8-byte participant token carried in rosters: the first eight bytes of
     * {@code sha-256(peerId)} as a big-endian long.
     *
     * @param peer the peer
     * @return the token
     */
    static long tokenOf(PeerId peer) {
        byte[] digest = Digests.sha256(peer.value().getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(digest, 0, 8).getLong();
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/aggregate/" + self.value(),
                self, group, clock.instant(), Duration.ofMinutes(15),
                TYPE, "0.1", "pipe", Map.of("modes", "sum,avg,count,min,max,quantile"), Map.of());
    }

    // ----------------------------------------------------------------- start

    /**
     * Joins an aggregation epoch with this node's local value as an {@code AVG}
     * contribution (the original, operator-less form).
     *
     * @param epochId    the epoch identifier agreed among participants
     * @param localValue this node's contribution
     */
    public void start(String epochId, double localValue) {
        start(epochId, Mode.AVG, localValue);
    }

    /**
     * Joins an aggregation epoch under an operator with this node's local value.
     * For {@code COUNT} the value is ignored and the participant counts as one;
     * for {@code QUANTILE} use {@link #start(String, Quantile, double)}.
     *
     * @param epochId    the epoch identifier agreed among participants
     * @param mode       the operator every participant must agree on
     * @param localValue this node's contribution
     * @throws IllegalArgumentException for {@link Mode#QUANTILE}, which needs a range
     */
    public void start(String epochId, Mode mode, double localValue) {
        Objects.requireNonNull(epochId, "epochId");
        Objects.requireNonNull(mode, "mode");
        if (mode == Mode.QUANTILE) {
            throw new IllegalArgumentException(
                    "QUANTILE needs a range: use start(epochId, Quantile, value)");
        }
        declare(epochId, mode, null, false);
        switch (mode) {
            case AVG -> epoch(epochId).add(localValue, 1.0);
            case SUM -> {
                epoch(epochId).add(localValue, 1.0);
                roster(epochId).add(new long[]{selfToken});
            }
            case COUNT -> roster(epochId).add(new long[]{selfToken});
            case MIN, MAX -> extremum(epochId, mode == Mode.MAX).merge(localValue);
            default -> throw new IllegalStateException(mode.name());
        }
    }

    /**
     * Joins a {@code QUANTILE} epoch with this node's local value.
     *
     * @param epochId    the epoch identifier agreed among participants
     * @param quantile   the quantile and histogram range every participant must agree on
     * @param localValue this node's contribution; clamps into the range
     */
    public void start(String epochId, Quantile quantile, double localValue) {
        Objects.requireNonNull(epochId, "epochId");
        Objects.requireNonNull(quantile, "quantile");
        declare(epochId, Mode.QUANTILE, quantile, false);
        double[] unit = new double[quantile.buckets()];
        unit[quantile.bucketOf(localValue)] = 1.0;
        histogram(epochId, quantile).add(unit);
    }

    /**
     * Joins an epoch in template mode: reads the entries matching the template
     * from this node's local view of the space and contributes the field as
     * documented on the class ({@code SUM}: local sum; {@code COUNT}: local match
     * count; {@code AVG}: local sum and local count; {@code MIN}/{@code MAX}:
     * local extremum). For {@code QUANTILE} use
     * {@link #start(String, Quantile, Space, Template, ToDoubleFunction)}.
     *
     * @param epochId  the epoch identifier agreed among participants
     * @param mode     the operator every participant must agree on
     * @param space    the space to read locally
     * @param template the entries to aggregate
     * @param field    the numeric field
     * @param <T>      the entry type
     */
    public <T> void start(String epochId, Mode mode, Space space, Template<T> template,
                          ToDoubleFunction<T> field) {
        Objects.requireNonNull(epochId, "epochId");
        Objects.requireNonNull(mode, "mode");
        if (mode == Mode.QUANTILE) {
            throw new IllegalArgumentException(
                    "QUANTILE needs a range: use start(epochId, Quantile, space, template, field)");
        }
        List<T> matches = readLocal(space, template);
        double sum = 0.0;
        double min = Double.NaN;
        double max = Double.NaN;
        for (T entry : matches) {
            double v = field.applyAsDouble(entry);
            sum += v;
            min = Double.isNaN(min) ? v : Math.min(min, v);
            max = Double.isNaN(max) ? v : Math.max(max, v);
        }
        declare(epochId, mode, null, true);
        switch (mode) {
            case SUM -> {
                epoch(epochId).add(sum, 1.0);
                roster(epochId).add(new long[]{selfToken});
            }
            case COUNT -> {
                epoch(epochId).add(matches.size(), 1.0);
                roster(epochId).add(new long[]{selfToken});
            }
            case AVG -> {
                epoch(epochId).add(sum, 1.0);
                epoch(countEpoch(epochId)).add(matches.size(), 1.0);
            }
            case MIN -> extremum(epochId, false).merge(min);
            case MAX -> extremum(epochId, true).merge(max);
            default -> throw new IllegalStateException(mode.name());
        }
    }

    /**
     * Joins a {@code QUANTILE} epoch in template mode: one histogram unit per
     * local matching entry.
     *
     * @param epochId  the epoch identifier agreed among participants
     * @param quantile the quantile and histogram range every participant must agree on
     * @param space    the space to read locally
     * @param template the entries to aggregate
     * @param field    the numeric field
     * @param <T>      the entry type
     */
    public <T> void start(String epochId, Quantile quantile, Space space, Template<T> template,
                          ToDoubleFunction<T> field) {
        Objects.requireNonNull(epochId, "epochId");
        Objects.requireNonNull(quantile, "quantile");
        double[] units = new double[quantile.buckets()];
        for (T entry : readLocal(space, template)) {
            units[quantile.bucketOf(field.applyAsDouble(entry))] += 1.0;
        }
        declare(epochId, Mode.QUANTILE, quantile, true);
        histogram(epochId, quantile).add(units);
    }

    private static <T> List<T> readLocal(Space space, Template<T> template) {
        Objects.requireNonNull(space, "space");
        Objects.requireNonNull(template, "template");
        return space.readAll(template, TEMPLATE_READ_LIMIT);
    }

    private void declare(String epochId, Mode mode, Quantile quantile, boolean template) {
        Declared before = declared.putIfAbsent(epochId, new Declared(mode, quantile, template));
        if (before != null && (before.mode() != mode || before.template() != template)) {
            throw new IllegalArgumentException("epoch " + epochId + " already started as "
                    + before.mode() + (before.template() ? " (template)" : ""));
        }
    }

    // ------------------------------------------------------------------ tick

    /**
     * Runs one gossip round for every active epoch: push-sum epochs halve their
     * mass and push the other half; extrema and rosters push their whole state;
     * histograms halve and push their vector. Each item goes to one sampled member.
     */
    @Override
    public boolean requiresTick() {
        return true;
    }

    /**
     * Whether this node has taken part in at least one exchange round, by
     * ticking or by receiving a peer's contribution. Until it has, every
     * accumulator still holds nothing but this node's own seed, and reporting
     * that as a fleet figure is the silent-wrong-answer of QA3 A3-2, so the
     * estimators refuse to answer instead.
     */
    private volatile boolean exchanged;

    @Override
    public void tick() {
        exchanged = true;
        for (Map.Entry<String, Epoch> e : epochs.entrySet()) {
            sampled().ifPresent(to -> send(to, Frame.of(e.getValue().halve(e.getKey()))));
        }
        for (Map.Entry<String, ExtremumState> e : extrema.entrySet()) {
            ExtremumState state = e.getValue();
            if (Double.isNaN(state.value)) {
                continue; // nothing to say yet
            }
            sampled().ifPresent(to -> send(to,
                    Frame.of(new Extremum(e.getKey(), state.max, state.value))));
        }
        for (Map.Entry<String, HistogramState> e : histograms.entrySet()) {
            sampled().ifPresent(to -> send(to, Frame.of(e.getValue().halve(e.getKey()))));
        }
        for (Map.Entry<String, RosterState> e : rosters.entrySet()) {
            sampled().ifPresent(to -> send(to,
                    Frame.of(new Roster(e.getKey(), e.getValue().snapshot()))));
        }
    }

    private java.util.Optional<PeerId> sampled() {
        List<PeerId> target = sampler.randomMembers(1);
        return target.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(target.get(0));
    }

    private void send(PeerId to, Frame frame) {
        pipes.send(to, TYPE, codec.toBytes(frame));
    }

    // -------------------------------------------------------------- estimate

    /**
     * Returns the current estimate for an epoch under the operator this node
     * declared when it joined ({@code AVG} for an epoch this node only heard
     * about): the fleet mean, total, count, extremum, or quantile.
     *
     * @param epochId the epoch
     * @return the estimate, or empty when nothing is known yet
     */
    public OptionalDouble estimate(String epochId) {
        if (!exchanged) {
            // QA3 A3-2: before the first round this node holds only its own
            // seed, so every mode would answer with the local value dressed as
            // a fleet figure. Empty is the honest answer, and it is also the
            // signal that nothing is driving this capability.
            return OptionalDouble.empty();
        }
        Declared d = declared.get(epochId);
        Mode mode = d == null ? Mode.AVG : d.mode();
        return switch (mode) {
            case AVG -> d != null && d.template()
                    ? ratio(epochId, countEpoch(epochId))
                    : mean(epochId);
            case SUM -> scaleByRoster(epochId, mean(epochId));
            case COUNT -> d != null && d.template()
                    ? scaleByRoster(epochId, mean(epochId))
                    : rosterSize(epochId);
            case MIN, MAX -> {
                ExtremumState state = extrema.get(epochId);
                yield state == null || Double.isNaN(state.value)
                        ? OptionalDouble.empty() : OptionalDouble.of(state.value);
            }
            case QUANTILE -> estimateQuantile(epochId, d.quantile().q());
        };
    }

    /**
     * Reads any quantile from a {@code QUANTILE} epoch's histogram, not only the
     * one declared at {@code start}.
     *
     * @param epochId the epoch
     * @param q       the quantile in (0, 1)
     * @return the value at which the interpolated cumulative distribution reaches
     *         {@code q}, or empty when this node holds no histogram mass
     */
    public OptionalDouble estimateQuantile(String epochId, double q) {
        if (!(q > 0.0 && q < 1.0)) {
            throw new IllegalArgumentException("q must be in (0,1): " + q);
        }
        if (!exchanged) {
            return OptionalDouble.empty();  // QA3 A3-2, as estimate(String)
        }
        HistogramState state = histograms.get(epochId);
        if (state == null) {
            return OptionalDouble.empty();
        }
        double[] b = state.snapshot();
        double total = Arrays.stream(b).sum();
        if (total <= 0.0) {
            return OptionalDouble.empty();
        }
        double width = (state.hi - state.lo) / b.length;
        double cumulative = 0.0;
        for (int i = 0; i < b.length; i++) {
            double before = cumulative / total;
            cumulative += b[i];
            double after = cumulative / total;
            // Halved and re-added mass lands a hair below an exact boundary, so
            // a cumulative within rounding of q counts as reaching it.
            if (after >= q - CDF_EPSILON) {
                double fraction = after <= before ? 1.0
                        : Math.min(1.0, Math.max(0.0, (q - before) / (after - before)));
                return OptionalDouble.of(state.lo + (i + fraction) * width);
            }
        }
        return OptionalDouble.of(state.hi);
    }

    private OptionalDouble mean(String epochId) {
        Epoch epoch = epochs.get(epochId);
        if (epoch == null || epoch.weight == 0.0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(epoch.value / epoch.weight);
    }

    private OptionalDouble ratio(String numeratorEpoch, String denominatorEpoch) {
        OptionalDouble num = mean(numeratorEpoch);
        OptionalDouble den = mean(denominatorEpoch);
        if (num.isEmpty() || den.isEmpty() || den.getAsDouble() == 0.0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(num.getAsDouble() / den.getAsDouble());
    }

    private OptionalDouble rosterSize(String epochId) {
        RosterState roster = rosters.get(epochId);
        return roster == null ? OptionalDouble.empty() : OptionalDouble.of(roster.size());
    }

    private OptionalDouble scaleByRoster(String epochId, OptionalDouble mean) {
        OptionalDouble n = rosterSize(epochId);
        if (mean.isEmpty() || n.isEmpty()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(mean.getAsDouble() * n.getAsDouble());
    }

    private static String countEpoch(String epochId) {
        return epochId + "#n";
    }

    // --------------------------------------------------------------- receive

    private void onFrame(PeerId from, byte[] payload) {
        Frame frame;
        try {
            frame = codec.fromBytes(payload, Frame.class);
        } catch (RuntimeException e) {
            return;
        }
        if (frame == null) {
            return;
        }
        // A peer's contribution is participation just as much as our own tick:
        // this node now holds more than its own seed.
        exchanged = true;
        if (frame.share() != null && frame.share().epochId() != null) {
            Share share = frame.share();
            if (Double.isFinite(share.value()) && Double.isFinite(share.weight())
                    && share.weight() >= 0.0) {
                epoch(share.epochId()).add(share.value(), share.weight());
            }
        } else if (frame.extremum() != null && frame.extremum().epochId() != null) {
            Extremum extremum = frame.extremum();
            extremum(extremum.epochId(), extremum.max()).merge(extremum.value());
        } else if (frame.histogram() != null && frame.histogram().epochId() != null
                && frame.histogram().buckets() != null && frame.histogram().buckets().length > 0
                && frame.histogram().hi() > frame.histogram().lo()) {
            Histogram h = frame.histogram();
            histograms.computeIfAbsent(h.epochId(),
                    id -> new HistogramState(h.lo(), h.hi(), h.buckets().length)).add(h.buckets());
        } else if (frame.roster() != null && frame.roster().epochId() != null
                && frame.roster().members() != null) {
            roster(frame.roster().epochId()).add(frame.roster().members());
        }
    }

    private Epoch epoch(String epochId) {
        return epochs.computeIfAbsent(epochId, id -> new Epoch());
    }

    private ExtremumState extremum(String epochId, boolean max) {
        return extrema.computeIfAbsent(epochId, id -> new ExtremumState(max));
    }

    private HistogramState histogram(String epochId, Quantile quantile) {
        return histograms.computeIfAbsent(epochId,
                id -> new HistogramState(quantile.lo(), quantile.hi(), quantile.buckets()));
    }

    private RosterState roster(String epochId) {
        return rosters.computeIfAbsent(epochId, id -> new RosterState());
    }
}
