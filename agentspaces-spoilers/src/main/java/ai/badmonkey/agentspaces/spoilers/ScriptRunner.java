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
package ai.badmonkey.agentspaces.spoilers;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.FrameHeader;
import ai.badmonkey.agentspaces.test.FrameItem;
import ai.badmonkey.agentspaces.test.LinkFilter;
import ai.badmonkey.agentspaces.test.LinkFrame;
import ai.badmonkey.agentspaces.test.SimNetwork;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Runs one {@link Script} on a {@link SimNetwork} (srad/spoiler-peers.md §3.2 to §3.4, SP-C-03 to SP-C-10):
 * a {@link LinkFilter} on every link that evaluates every rule, in order, against every frame; fires every
 * rule that matches and has count left; combines their effects as most restrictive wins; releases a hold
 * when its release trigger fires, outside every lock and after the triggering frame's own delivery;
 * records each firing in order (the firing sequence a replay compares) and each released frame's stamp
 * age; and fails a rule that never fired, a hold past its ceiling, and, at load, an action the link's
 * channel-auth mode does not allow.
 *
 * <p>Frame triggers are judged as frames are sent. Time and counter triggers, and releases waiting on a
 * space event, are judged by {@link #tick}, which the fleet calls on its own thread after each tick, so a
 * release never delivers into a member from inside one of that member's listeners.
 */
public final class ScriptRunner implements LinkFilter, AutoCloseable {

    /**
     * {@code PeerNode.MAX_FRAME_AGE_MILLIS} (private there; {@code ScriptRunnerTest} pins the two equal):
     * a frame held past it is dropped as stale by its receiver, which the runner records as expected.
     */
    public static final long FRAME_AGE_MILLIS = 300_000L;
    /** How many recent frames and events a failure prints (SP-Q-MNT-02). */
    private static final int RECENT = 32;

    /** Which side of the spoiler a rule's link is on (srad/spoiler-peers.md §3.4.2). */
    public enum Link { OUTBOUND, INBOUND, HONEST }

    /**
     * One firing, in order.
     *
     * @param index       its place in the firing sequence
     * @param rule        the rule
     * @param what        the action taken, or {@code release}
     * @param detail      the frame or event that caused it
     * @param fleetMillis the fleet clock at the firing
     */
    public record Firing(int index, String rule, String what, String detail, long fleetMillis) {
        /** The firing without its fabric sequence numbers and clock: what a replay compares (SP-Q-REL-04). */
        public String signature() {
            return rule + " " + what + " " + detail.replaceAll("#\\d+ ", "");
        }
    }

    /**
     * One release of a hold.
     *
     * @param rule          the rule
     * @param frames        the frames delivered
     * @param oldestAgeMillis the largest stamp age among them at release, by the fleet clock
     * @param pastFrameAge  how many were older than {@link #FRAME_AGE_MILLIS}, which their receivers drop as stale
     * @param cause         what released them: a trigger, or the ceiling of a hold with no release
     * @param fleetMillis   the fleet clock at release
     */
    public record Release(String rule, int frames, long oldestAgeMillis, int pastFrameAge, String cause, long fleetMillis) {
    }

    /**
     * What the run did.
     *
     * @param seed     the seed it ran with
     * @param firings  the firing sequence
     * @param releases the releases
     * @param failures the failures (vacuity, ceilings), empty on a pass
     */
    public record Report(long seed, List<Firing> firings, List<Release> releases, List<String> failures) {
        /** The firing sequence as signatures, for a replay to compare. */
        public List<String> signatures() {
            return firings.stream().map(Firing::signature).toList();
        }

        /** Drops the oracle must expect from the holds: stale frames released past the frame age (SP-O-02). */
        public Map<String, Long> expectedDrops() {
            long stale = releases.stream().mapToLong(Release::pastFrameAge).sum();
            return stale == 0 ? Map.of() : Map.of("stale", stale);
        }

        /** How many times a rule fired, its releases left out. */
        public long fired(String rule) {
            return firings.stream().filter(f -> f.rule().equals(rule) && !f.what().equals("release")).count();
        }
    }

    private static final class State {
        final Script.Rule rule;
        final String gate;
        int fired;
        long holdSince = -1;
        boolean released;
        boolean releaseDue;
        String releaseCause;
        boolean ceilingFailed;

        State(Script.Rule rule, String gate) {
            this.rule = rule;
            this.gate = gate;
        }

        boolean matching() {
            return fired < rule.times() && !released && !releaseDue;
        }

        boolean holding() {
            return holdSince >= 0 && !released;
        }
    }

    private final SimNetwork network;
    private final InstantSource clock;
    private final long periodMillis;
    private final Script script;
    private final long seed;
    private final long armedAt;
    private final List<State> states = new ArrayList<>();
    private final Map<String, String> bindings = new HashMap<>();
    private final Map<Trigger.Frame, int[]> nthCounts = new IdentityHashMap<>();
    private final Deque<String> recent = new ArrayDeque<>();
    private final List<Firing> firings = new ArrayList<>();
    private final List<Release> releases = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();
    private final List<Subscription> watches = new ArrayList<>();
    private AutoCloseable filter;
    private boolean reported;

    private ScriptRunner(SimNetwork network, InstantSource clock, Duration period, Script script) {
        this.network = network;
        this.clock = clock;
        this.periodMillis = period.toMillis();
        this.script = script;
        this.seed = Long.getLong("spoiler.seed", script.seed());
        this.armedAt = clock.millis();
        for (Script.Rule rule : script.rules()) {
            states.add(new State(rule, script.spoiler() + "/" + rule.name()));
        }
    }

    /**
     * Arms a script on a fabric: plugs the item decoder, checks every rule against the channel-auth mode,
     * prints the seed, and filters every link from now on.
     *
     * @param network the fleet's fabric
     * @param clock   the fleet clock
     * @param period  the fleet's gossip period, which {@link Ceiling#periods} and {@link Trigger#afterPeriods} count in
     * @param mode    the channel-auth mode the fleet's links run
     * @param script  the script
     * @return the running script
     * @throws IllegalArgumentException when a rule's action is not allowed on its link (SP-C-09), or a link
     *                                  action has no frame to act on
     */
    public static ScriptRunner attach(SimNetwork network, InstantSource clock, Duration period, PeerNode.ChannelAuth mode,
                                      Script script) {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(mode, "mode");
        for (Script.Rule rule : script.rules()) {
            check(script.spoiler(), rule, mode);
        }
        ScriptRunner runner = new ScriptRunner(network, clock, period, script);
        System.out.printf("spoiler %s: seed %d, %d rule(s) %s%n", script.spoiler(), runner.seed, script.rules().size(),
                script.rules().stream().map(Script.Rule::name).toList());
        network.clock(clock).decoders(null, new WireItems());
        runner.filter = network.filter(null, null, runner);
        return runner;
    }

    /** The actions each link allows under a channel-auth mode (srad/spoiler-peers.md §3.4.2). */
    static Set<Action.Type> allowed(PeerNode.ChannelAuth mode, Link link) {
        // the link actions of the 0.7.0 cut never alter a frame, so SIGNED, ATTESTED and SESSION allow the same
        // ones; rewrite and resign, which only SIGNED allows, come after 0.7.0
        Objects.requireNonNull(mode, "mode");
        return switch (link) {
            case OUTBOUND, HONEST -> EnumSet.allOf(Action.Type.class);
            // the spoiler can refuse, keep or slow what reaches it, but cannot make a member send twice
            case INBOUND -> EnumSet.of(Action.Type.DROP, Action.Type.HOLD, Action.Type.DELAY, Action.Type.COUNT);
        };
    }

    static Link linkOf(String spoiler, Trigger trigger) {
        if (trigger instanceof Trigger.Frame f) {
            if (spoiler.equals(f.from())) {
                return Link.OUTBOUND;
            }
            if (spoiler.equals(f.to())) {
                return Link.INBOUND;
            }
        }
        return Link.HONEST;
    }

    private static void check(String spoiler, Script.Rule rule, PeerNode.ChannelAuth mode) {
        if (rule.action().type() != Action.Type.COUNT && !(rule.when() instanceof Trigger.Frame)) {
            throw new IllegalArgumentException("rule " + rule.name() + ": a " + rule.action().type()
                    + " acts on a frame, so its trigger is a frame trigger");
        }
        Link link = linkOf(spoiler, rule.when());
        if (!allowed(mode, link).contains(rule.action().type())) {
            throw new IllegalArgumentException("rule " + rule.name() + ": " + rule.action().type() + " on an " + link
                    + " link is not allowed under channel auth " + mode + " (srad/spoiler-peers.md §3.4.2)");
        }
    }

    /**
     * Feeds a member's space events to the event triggers, as an application's listener would.
     *
     * @param member   the member's name
     * @param space    its space
     * @param template what to watch
     * @return this runner
     */
    public ScriptRunner watch(String member, Space space, Template<?> template) {
        watches.add(space.notify(template, event -> event(member, event), Lease.of(Duration.ofDays(1))));
        return this;
    }

    @Override
    public Verdict decide(LinkFrame frame) {
        List<State> toRelease = new ArrayList<>();
        Verdict verdict = Verdict.pass();
        synchronized (this) {
            remember(frame.toString());
            for (State s : states) {
                // a release trigger counts from arming, held frames or not: a release that fires before the rule held
                // anything ends the rule, so nothing is held after the event the hold was to wait for
                if (s.released || s.releaseDue || !(s.rule.until() instanceof Trigger.Frame until) || !matches(until, frame)) {
                    continue;
                }
                s.releaseDue = true;
                s.releaseCause = "on " + frame;
                toRelease.add(s);
            }
            for (State s : states) {
                if (!s.matching() || !(s.rule.when() instanceof Trigger.Frame when)) {
                    continue;
                }
                if (!s.rule.action().into().isEmpty() && !s.rule.action().into().contains(frame.to())) {
                    continue;
                }
                if (!matches(when, frame)) {
                    continue;
                }
                s.fired++;
                firings.add(new Firing(firings.size(), s.rule.name(), s.rule.action().type().name().toLowerCase(),
                        frame.toString(), clock.millis()));
                verdict = verdict.combine(effect(s));
                afterFired(s.rule.name(), toRelease);
            }
        }
        if (!toRelease.isEmpty()) {
            verdict = verdict.then(() -> release(toRelease));
        }
        return verdict;
    }

    private Verdict effect(State s) {
        Action a = s.rule.action();
        return switch (a.type()) {
            case DROP -> Verdict.drop();
            case HOLD, REORDER -> {
                if (s.holdSince < 0) {
                    s.holdSince = clock.millis();
                }
                yield Verdict.hold(s.gate);
            }
            case DELAY -> Verdict.delay(a.delayMillis());
            case DUPLICATE -> Verdict.duplicate(a.copies());
            case COUNT -> Verdict.pass();
        };
    }

    /** Rules waiting for another rule's first firing. */
    private void afterFired(String fired, List<State> toRelease) {
        for (State s : states) {
            if (s.holding() && !s.releaseDue && s.rule.until() instanceof Trigger.After after && after.rule().equals(fired)) {
                s.releaseDue = true;
                s.releaseCause = "after " + fired;
                toRelease.add(s);
            }
        }
    }

    private boolean matches(Trigger.Frame t, LinkFrame frame) {
        if (t.from() != null && !t.from().equals(frame.from())) {
            return false;
        }
        if (t.to() != null && !t.to().equals(frame.to())) {
            return false;
        }
        FrameHeader header = frame.header().orElse(null);
        if (!t.kinds().isEmpty()) {
            if (header == null || t.kinds().stream().noneMatch(k -> header.carriesKind(k.name()))) {
                return false;
            }
        }
        if (t.stream() != null && (header == null || !header.carriesStream(t.stream()))) {
            return false;
        }
        if (t.item() != null) {
            FrameItem hit = null;
            for (FrameItem item : frame.items()) {
                if (item.type().equals(t.item().name())
                        && (t.node() == null || t.node().equals(item.attribute("kind")))
                        && t.attrs().entrySet().stream().allMatch(a -> idMatches(a.getValue(), item.attribute(a.getKey())))
                        && (t.stream() == null || t.stream().equals(item.stream()))
                        && idMatches(t.id(), item.id())) {
                    hit = item;
                    break;
                }
            }
            if (hit == null) {
                return false;
            }
            if (t.bind() != null && hit.id() != null) {
                bindings.putIfAbsent(t.bind(), hit.id());
            }
        }
        if (t.nth() > 0) {
            int[] n = nthCounts.computeIfAbsent(t, k -> new int[1]);
            return ++n[0] == t.nth();
        }
        return true;
    }

    private boolean idMatches(Object wanted, String id) {
        if (wanted == null) {
            return true;
        }
        if (wanted instanceof Trigger.Bound b) {
            String bound = bindings.get(b.name());
            return bound != null && bound.equals(id);
        }
        if (wanted instanceof java.util.Collection<?> any) {
            return id != null && any.contains(id); // read at match time, so a scenario may fill it once it knows the ids
        }
        return wanted.toString().equals(id);
    }

    private synchronized void event(String member, SpaceEvent<?> event) {
        String id = event.entryId() == null ? null : event.entryId().toString();
        remember("event " + member + " " + event.kind() + " " + id);
        for (State s : states) {
            if (s.holding() && !s.releaseDue && s.rule.until() instanceof Trigger.Event e
                    && e.member().equals(member) && e.kind() == event.kind() && idMatches(e.entry(), id)) {
                s.releaseDue = true; // released by the next tick, on the fleet's thread, never inside a listener
                s.releaseCause = "event " + member + " " + event.kind() + " " + id;
            }
            if (s.matching() && s.rule.when() instanceof Trigger.Event e && e.member().equals(member)
                    && e.kind() == event.kind() && idMatches(e.entry(), id)) {
                s.fired++;
                firings.add(new Firing(firings.size(), s.rule.name(), "count", "event " + member + " " + event.kind(),
                        clock.millis()));
            }
        }
    }

    /**
     * Judges the time and counter triggers, the ceilings, and the releases events made due; the fleet calls
     * it on its own thread after each tick.
     *
     * @throws AssertionError when a hold passed its ceiling (SP-C-04)
     */
    public void tick() {
        List<State> toRelease = new ArrayList<>();
        String failure = null;
        synchronized (this) {
            long now = clock.millis();
            for (State s : states) {
                if (s.matching() && s.rule.action().type() == Action.Type.COUNT && due(s.rule.when(), now)) {
                    s.fired++;
                    firings.add(new Firing(firings.size(), s.rule.name(), "count", String.valueOf(s.rule.when()), now));
                    afterFired(s.rule.name(), toRelease);
                }
                if (!s.released && !s.releaseDue && s.rule.until() != null && due(s.rule.until(), now)) {
                    s.releaseDue = true;
                    s.releaseCause = String.valueOf(s.rule.until());
                }
                if (!s.holding()) {
                    if (s.releaseDue && !s.released && !toRelease.contains(s)) {
                        toRelease.add(s); // the rule ends, with nothing held
                    }
                    continue;
                }
                long ceiling = s.rule.ceiling().toMillis(periodMillis);
                if (!s.releaseDue && now - s.holdSince > ceiling) {
                    s.releaseDue = true;
                    if (s.rule.until() == null) {
                        s.releaseCause = "ceiling " + s.rule.ceiling(); // a hold with no release ends at its ceiling
                    } else {
                        s.releaseCause = "ceiling passed";
                        s.ceilingFailed = true;
                        failure = "rule " + s.rule.name() + ": its release (" + s.rule.until() + ") did not fire within its ceiling of "
                                + s.rule.ceiling() + " (held since fleet ms " + s.holdSince + ", now " + now + ")";
                        failures.add(failure);
                    }
                }
                if (s.releaseDue && !toRelease.contains(s)) {
                    toRelease.add(s);
                }
            }
        }
        release(toRelease);
        if (failure != null) {
            throw new AssertionError(describe(failure));
        }
    }

    private boolean due(Trigger trigger, long now) {
        return switch (trigger) {
            case Trigger.Time t -> t.atMillis() >= 0 ? now >= t.atMillis() : now >= armedAt + t.periods() * periodMillis;
            case Trigger.Counter c -> c.value().getAsLong() >= c.atLeast();
            default -> false;
        };
    }

    private void release(List<State> due) {
        for (State s : due) {
            List<LinkFrame> frames = network.releaseGate(s.gate, s.rule.action().order(), seed);
            long now = clock.millis();
            long oldest = 0;
            int stale = 0;
            for (LinkFrame f : frames) {
                long stamp = f.header().map(FrameHeader::stampMillis).orElse(-1L);
                if (stamp >= 0) {
                    long age = now - stamp;
                    oldest = Math.max(oldest, age);
                    if (age > FRAME_AGE_MILLIS) {
                        stale++;
                    }
                }
            }
            synchronized (this) {
                s.released = true;
                releases.add(new Release(s.rule.name(), frames.size(), oldest, stale, s.releaseCause, now));
                firings.add(new Firing(firings.size(), s.rule.name(), "release", frames.size() + " frame(s), " + s.releaseCause, now));
                remember("release " + s.rule.name() + " " + frames.size() + " frame(s)");
            }
        }
    }

    private void remember(String event) {
        recent.addLast(event);
        if (recent.size() > RECENT) {
            recent.removeFirst();
        }
    }

    private synchronized String describe(String failure) {
        StringBuilder b = new StringBuilder("spoiler ").append(script.spoiler()).append(" (seed ").append(seed).append("): ")
                .append(failure).append("\n  bindings ").append(bindings).append("\n  last ").append(recent.size())
                .append(" events:");
        recent.forEach(e -> b.append("\n    ").append(e));
        b.append("\n  firing sequence:");
        firings.forEach(f -> b.append("\n    ").append(f.index()).append(' ').append(f.signature()));
        return b.toString();
    }

    /** The bound value of a name, or null. */
    public synchronized String binding(String name) {
        return bindings.get(name);
    }

    /** The run so far, without judging it. */
    public synchronized Report report() {
        return new Report(seed, List.copyOf(firings), List.copyOf(releases), List.copyOf(failures));
    }

    /**
     * Ends the run: releases what is still held, stops filtering, and fails on a rule that never fired
     * (SP-C-04, SP-Q-REL-01), a hold that its release never ended, or any earlier failure.
     *
     * @return the report of a passing run
     * @throws AssertionError naming the first failure, the seed, the last events and the firing sequence
     */
    public Report finish() {
        List<State> stillHeld = new ArrayList<>();
        synchronized (this) {
            for (State s : states) {
                if (s.fired == 0) {
                    failures.add("rule " + s.rule.name() + " never fired");
                }
                if (s.holding()) {
                    if (s.rule.until() != null && !s.releaseDue) {
                        failures.add("rule " + s.rule.name() + ": its release (" + s.rule.until() + ") never fired");
                    }
                    s.releaseDue = true;
                    s.releaseCause = s.releaseCause == null ? "finish" : s.releaseCause;
                    stillHeld.add(s);
                }
            }
        }
        release(stillHeld);
        close();
        Report report = report();
        if (!report.failures().isEmpty()) {
            throw new AssertionError(describe(report.failures().get(0)
                    + (report.failures().size() > 1 ? "; also " + report.failures().subList(1, report.failures().size()) : "")));
        }
        return report;
    }

    /** Stops filtering and watching; frames still held stay held. */
    @Override
    public synchronized void close() {
        if (reported) {
            return;
        }
        reported = true;
        try {
            if (filter != null) {
                filter.close();
            }
        } catch (Exception ignored) {
            // removing a filter from a list does not fail
        }
        for (Subscription w : watches) {
            try {
                w.close();
            } catch (RuntimeException ignored) {
                // a closed space has cancelled its subscriptions already
            }
        }
    }
}
