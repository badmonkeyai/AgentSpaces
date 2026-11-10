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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.Content;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The writer's side of eager push (PDT 2 phase 5, P2-P-01, P2-P-04; srad/agentspaces-pdt2.md §3.4, S5 of §6.4). A
 * writer with push on publishes a version whose delta passes the inline bound as today, with the attribute
 * {@link #ATTR} naming the parts it pushes, and then the delta's nodes as {@link PushBundle} rumours on the ref's
 * stream, which {@code GossipBus} carries eagerly down its Plumtree and announces lazily by {@code HAVE}. A delta
 * over {@link Config#maxBytes} or {@link Config#maxNodes}, or past the writer's budget, goes pull-only, unannounced.
 * The budget is one token bucket per writer across all its refs: {@link Config#bytesPerSecond} a second, up to
 * {@link Config#maxBytes} at once. Each bundle sent also spends its bytes from the content swarm's upload budget, as each forwarder's does. A
 * ref sealed per message ({@code group-key}) is never pushed, since every member relays a rumour, keyed or not.
 */
public final class PushPublisher {

    /**
     * The head attribute that announces a push: the canonical decimal count of its bundles (P2-P-02). Advisory, as
     * the other {@code pdt.*} attributes are: a follower that reads no count from it fetches at once.
     */
    public static final String ATTR = "pdt.push";

    /**
     * The settings of push, the same on writers and followers ({@code agentspaces.pdt.push.*}).
     *
     * @param enabled        whether this peer pushes its deltas and follows the streams of the refs it follows fully
     * @param bytesPerSecond the writer's budget, and each stream's on a follower
     * @param maxBytes       the largest delta pushed, and the most a budget holds at once
     * @param grace          the least a follower waits for an announced push before it fetches: two gossip rounds
     * @param maxGrace       the most it waits: a follower sizes each wait from the bus's repair window and the
     *                       delta's bundles, between {@code grace} and this (swarm retries, ROBUSTNESS.md §10.3 P4)
     */
    public record Config(boolean enabled, long bytesPerSecond, long maxBytes, Duration grace, Duration maxGrace) {
        public Config {
            Objects.requireNonNull(grace, "grace");
            Objects.requireNonNull(maxGrace, "maxGrace");
            if (bytesPerSecond <= 0 || maxBytes <= 0 || maxBytes > (long) PushBundle.MAX_PARTS * PushBundle.MAX_BYTES / 2) {
                throw new IllegalArgumentException("push budgets are positive and max-bytes at most "
                        + (PushBundle.MAX_PARTS / 2) + " MiB");
            }
            if (grace.isNegative() || maxGrace.compareTo(grace) < 0) {
                throw new IllegalArgumentException("the push grace is not negative and max-grace is at least the grace");
            }
        }

        /** The settings with the default ceiling on the grace, {@link #DEFAULT_MAX_GRACE} or the grace when longer. */
        public Config(boolean enabled, long bytesPerSecond, long maxBytes, Duration grace) {
            this(enabled, bytesPerSecond, maxBytes, grace,
                    grace.compareTo(DEFAULT_MAX_GRACE) > 0 ? grace : DEFAULT_MAX_GRACE);
        }

        /** The default ceiling on a follower's sized wait for an announced push. */
        public static final Duration DEFAULT_MAX_GRACE = Duration.ofSeconds(15);

        /**
         * The most nodes one delta pushes, and one root holds aside on a follower: {@code maxBytes} over the
         * {@code PushReceiver.HELD_NODE_OVERHEAD} bytes that keeping a node aside costs, so a follower keeps at most
         * twice {@code maxBytes} per root however small the nodes (PDT 2 phase 5 adversary, P2-Q-SEC-03).
         *
         * @return the nodes
         */
        public long maxNodes() {
            return maxBytes / PushReceiver.HELD_NODE_OVERHEAD;
        }

        /** The defaults of §7.2, push off: 2 MiB/s, 8 MiB, two rounds of the default one-second gossip period, 15 s at most. */
        public static Config defaults() {
            return new Config(false, 2L << 20, 8L << 20, Duration.ofSeconds(2), DEFAULT_MAX_GRACE);
        }

        /** The defaults with push on. */
        public static Config on() {
            Config d = defaults();
            return new Config(true, d.bytesPerSecond(), d.maxBytes(), d.grace(), d.maxGrace());
        }
    }

    /**
     * What a peer pushed and received (P2-P-05 reads pushed against pulled bytes).
     *
     * @param deltasPushed    deltas this writer pushed
     * @param deltasPullOnly  deltas over the inline bound this writer did not push: over max-bytes, max-nodes, or the
     *                        budget
     * @param bundlesSent     bundles this writer published
     * @param bytesPushed     node bytes this writer pushed
     * @param bundlesReceived bundles this follower took for a stream it follows
     * @param bundlesDropped  bundles dropped: outside the grammar or a bound, or past a budget
     * @param nodesStored     nodes stored from bundles, each verified against a live head's root
     * @param bytesStored     node bytes stored from bundles
     * @param nodesDropped    nodes held aside and dropped: no live head named their root within the grace, or its
     *                        root did not reach them
     * @param fallbacks       announced pushes that did not complete within the grace, so the follower fetched
     */
    public record Stats(long deltasPushed, long deltasPullOnly, long bundlesSent, long bytesPushed, long bundlesReceived,
                        long bundlesDropped, long nodesStored, long bytesStored, long nodesDropped, long fallbacks) {
    }

    /**
     * One delta ready to push, its bucket bytes taken.
     *
     * @param stream  the ref's stream
     * @param head    the head, announcing the push
     * @param bundles the encoded bundles
     * @param bytes   the node bytes
     */
    record Plan(String stream, PRef head, List<byte[]> bundles, long bytes) {
    }

    private final Content content;
    private final InstantSource clock;
    private final Config config;
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AtomicLong deltasPushed = new AtomicLong();
    private final AtomicLong deltasPullOnly = new AtomicLong();
    private final AtomicLong bundlesSent = new AtomicLong();
    private final AtomicLong bytesPushed = new AtomicLong();
    private double tokens;
    private long refilledMillis;

    PushPublisher(Content content, InstantSource clock, Config config) {
        this.content = Objects.requireNonNull(content, "content");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
        this.tokens = config.maxBytes();
        this.refilledMillis = clock.millis();
    }

    /**
     * Plans the push of a version whose delta did not ride inline: splits it into bundles and takes its bytes from
     * the budget.
     *
     * @param stream the ref's stream
     * @param head   the head about to be written, without nodes
     * @param delta  the delta's nodes, parents first
     * @return the plan, whose head carries {@link #ATTR}; empty when the delta goes pull-only
     */
    Optional<Plan> plan(String stream, PRef head, List<Map.Entry<String, byte[]>> delta) {
        if (!config.enabled() || head.nodes() != null || delta.isEmpty() || PRef.SEALING_GROUP_KEY.equals(head.sealing())) {
            return Optional.empty();
        }
        long bytes = 0;
        for (Map.Entry<String, byte[]> node : delta) {
            bytes += node.getValue().length;
        }
        if (bytes > config.maxBytes() || delta.size() > config.maxNodes()) {
            deltasPullOnly.incrementAndGet();
            return Optional.empty();
        }
        List<List<byte[]>> parts = new ArrayList<>();
        List<byte[]> part = new ArrayList<>();
        long partBytes = 0;
        for (Map.Entry<String, byte[]> node : delta) {
            byte[] b = node.getValue();
            if (!part.isEmpty() && partBytes + b.length > PushBundle.MAX_BYTES) {
                parts.add(part);
                part = new ArrayList<>();
                partBytes = 0;
            }
            part.add(b);
            partBytes += b.length;
        }
        parts.add(part);
        if (parts.size() > PushBundle.MAX_PARTS || !take(bytes)) {
            deltasPullOnly.incrementAndGet();
            return Optional.empty();
        }
        List<byte[]> bundles = new ArrayList<>(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            bundles.add(codec.toBytes(new PushBundle(PushBundle.VERSION, head.root(), head.seq(), i, parts.size(), parts.get(i))));
        }
        Map<String, Object> attrs = new LinkedHashMap<>();
        if (head.attrs() != null) {
            attrs.putAll(head.attrs());
        }
        attrs.put(ATTR, Integer.toString(parts.size()));
        return Optional.of(new Plan(stream, head.withAttrs(attrs), bundles, bytes));
    }

    /** Publishes the bundles of a plan once its head is written; stops at the first the upload budget refuses. */
    void send(Plan plan) {
        int sent = 0;
        for (int i = 0; i < plan.bundles().size(); i++) {
            byte[] bundle = plan.bundles().get(i);
            if (!content.chargeUpload(bundle.length)) {
                break;
            }
            content.gossip().publish(plan.stream(), plan.head().root() + "/" + i, bundle);
            sent++;
        }
        bundlesSent.addAndGet(sent);
        deltasPushed.incrementAndGet();
        bytesPushed.addAndGet(plan.bytes());
    }

    private synchronized boolean take(long bytes) {
        long now = clock.millis();
        tokens = Math.min(config.maxBytes(), tokens + (now - refilledMillis) * (config.bytesPerSecond() / 1000.0));
        refilledMillis = now;
        if (tokens < bytes) {
            return false;
        }
        tokens -= bytes;
        return true;
    }

    /**
     * The count of bundles a head announces, when its {@link #ATTR} holds one.
     *
     * @param head the head
     * @return the parts, or 0 when the head announces no push
     */
    static int announced(PRef head) {
        Object text = head.attrs() == null ? null : head.attrs().get(ATTR);
        if (!(text instanceof String s) || !s.matches("[1-9][0-9]?")) {
            return 0;
        }
        int parts = Integer.parseInt(s);
        return parts <= PushBundle.MAX_PARTS ? parts : 0;
    }

    long deltasPushed() {
        return deltasPushed.get();
    }

    long deltasPullOnly() {
        return deltasPullOnly.get();
    }

    long bundlesSent() {
        return bundlesSent.get();
    }

    long bytesPushed() {
        return bytesPushed.get();
    }
}
