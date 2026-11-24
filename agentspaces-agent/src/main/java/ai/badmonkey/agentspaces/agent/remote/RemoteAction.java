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
package ai.badmonkey.agentspaces.agent.remote;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * One remote agent's advertised capability as an invocable action (spec §14,
 * resolved): a foreign {@link AgentCard} whose consumed and produced schemas
 * resolve to local types becomes callable, and calling it is a space round-trip
 * — write the input entry where the remote agent takes from, then await a
 * result entry that {@linkplain Correlation correlates} with the request. The
 * remote agent's crash is survived the same way as ever: the take lease lapses,
 * another agent takes the entry, and the awaited result still arrives.
 *
 * <p>This is deliberately choreography, not RPC: the "call" is an ordinary
 * signed, leased entry any qualified agent may answer, and the invoker holds no
 * channel to any particular peer. The card names who advertised the
 * capability; the space decides who actually serves each request.
 */
public final class RemoteAction {

    private final AgentCard card;
    private final Class<?> inputType;
    private final Class<?> outputType;
    private final Space taskSpace;
    private final Space resultSpace;
    private final Correlation correlation;
    private final Lease writeLease;
    private final ai.badmonkey.agentspaces.api.ad.CardAction declared;

    RemoteAction(AgentCard card, Class<?> inputType, Class<?> outputType,
                 Space taskSpace, Space resultSpace, Correlation correlation,
                 Lease writeLease) {
        this(card, inputType, outputType, taskSpace, resultSpace, correlation, writeLease, null);
    }

    RemoteAction(AgentCard card, Class<?> inputType, Class<?> outputType,
                 Space taskSpace, Space resultSpace, Correlation correlation,
                 Lease writeLease, ai.badmonkey.agentspaces.api.ad.CardAction declared) {
        this(card, inputType, outputType, taskSpace, resultSpace, correlation, writeLease, declared, java.util.Map.of());
    }

    RemoteAction(AgentCard card, Class<?> inputType, Class<?> outputType,
                 Space taskSpace, Space resultSpace, Correlation correlation,
                 Lease writeLease, ai.badmonkey.agentspaces.api.ad.CardAction declared,
                 java.util.Map<String, String> tags) {
        this.tags = java.util.Map.copyOf(tags);
        this.declared = declared;
        this.card = card;
        this.inputType = inputType;
        this.outputType = outputType;
        this.taskSpace = taskSpace;
        this.resultSpace = resultSpace;
        this.correlation = correlation;
        this.writeLease = writeLease;
    }

    /** Returns the advertising agent's card. */
    /** The tags every input this action writes carries: the address of an implied action (SPEC §10.6), else none. */
    private final java.util.Map<String, String> tags;

    /** What happens to the input entry when no result arrives in time (review §7.5). */
    public enum OnTimeout {
        /** Leave it: a late worker may still do the work, and the entry lapses with its write lease. */
        LEAVE,
        /** Withdraw it, unless a worker already holds it under a live take claim. */
        WITHDRAW
    }

    public AgentCard card() {
        return card;
    }

    /** The tags every input this action writes carries. */
    public java.util.Map<String, String> tags() {
        return tags;
    }

    /** Returns the input entry type this action consumes. */
    public Class<?> inputType() {
        return inputType;
    }

    /** Returns the result entry type this action produces. */
    public Class<?> outputType() {
        return outputType;
    }

    /** Returns the space an invocation writes its input entry into. */
    public Space taskSpace() {
        return taskSpace;
    }

    /** Returns the space an invocation awaits its result entry in. */
    public Space resultSpace() {
        return resultSpace;
    }

    /**
     * The action the card declares for this pairing (spec §6.1, v0.1.13), or
     * empty for a card that declares none.
     *
     * @return the declared action
     */
    public java.util.Optional<ai.badmonkey.agentspaces.api.ad.CardAction> declared() {
        return java.util.Optional.ofNullable(declared);
    }

    /**
     * Returns a planner-friendly action name: {@code <agent>_<InputType>}. The
     * name stays type-derived even for a card that declares its actions, so
     * the planner's action names (and generated method names) are stable
     * across the v0.1.13 card change; the declared action's own name is
     * {@link #declared()}.
     */
    public String name() {
        return card.agent().localName() + "_" + inputType.getSimpleName();
    }

    /** Returns the card's human- and LLM-readable description. */
    /** Longest card text surfaced to planners; the rest is truncated (ASF-030). */
    private static final int MAX_CARD_TEXT = 500;

    public String description() {
        return sanitize(declared != null && !declared.description().isBlank()
                ? declared.description() : card.description());
    }

    /** Returns the goals the advertising agent pursues. */
    public List<String> goals() {
        return card.goals().stream().limit(16).map(RemoteAction::sanitize).toList();
    }

    /**
     * Card text is written by whatever peer published the card and flows into
     * LLM planner prompts (ASF-030): it is length-capped and stripped of
     * control characters here, and callers should treat it as untrusted
     * instructions-shaped data, never as policy.
     */
    private static String sanitize(String text) {
        if (text == null) {
            return "";
        }
        String cleaned = text.replaceAll("\\p{Cntrl}", " ");
        return cleaned.length() <= MAX_CARD_TEXT
                ? cleaned : cleaned.substring(0, MAX_CARD_TEXT) + "…";
    }

    /**
     * Invokes the action: writes the input entry and awaits a correlated
     * result.
     *
     * @param input   the input entry; must be of {@link #inputType()}
     * @param timeout how long to wait for a correlated result
     * @return the result entry, or empty when none arrived within the timeout
     */
    public Optional<Object> invoke(Object input, Duration timeout) {
        return invoke(input, timeout, OnTimeout.LEAVE);
    }

    /**
     * Invokes the action, saying what to do with the input when no result arrives in time (review §7.5):
     * {@link OnTimeout#WITHDRAW} cancels the written entry only while this replica still offers it: an entry a
     * worker holds under a take claim this replica knows is left to the worker. The space itself does not refuse
     * the cancel (an issuer's cancel wins over a take), so a claim still in flight to this replica is robbed:
     * the worker's complete fails and the work is not done twice ({@code RemoteActionWithdrawTest}).
     *
     * @param input     the input entry
     * @param timeout   how long to wait
     * @param onTimeout what to do with the input on a timeout
     * @return the result, or empty when none arrived within the timeout
     */
    public Optional<Object> invoke(Object input, Duration timeout, OnTimeout onTimeout) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(onTimeout, "onTimeout");
        if (!inputType.isInstance(input)) {
            throw new IllegalArgumentException("input must be a " + inputType.getName()
                    + ": " + input.getClass().getName());
        }
        // Results observed while we wait. The subscription starts before the
        // write so no result can slip between the write and the watch; delivery
        // is at-least-once, which the correlation check tolerates.
        ConcurrentLinkedQueue<Object> observed = new ConcurrentLinkedQueue<>();
        // A result observed signals the waiter at once (srad/agentspaces-event-loop.md §10.35); the caller's timeout is
        // real time, on the system's time source (EL-C-08). It slept between looks.
        ai.badmonkey.agentspaces.common.time.ChangeSignal arrived = new ai.badmonkey.agentspaces.common.time.ChangeSignal(
                ai.badmonkey.agentspaces.common.time.TimeSource.system());
        Template<?> resultTemplate = Template.of(outputType);
        // Entries equal to one already present are not answers to this request;
        // remember how many of each existed before the write (a multiset), so
        // the readAll backstop cannot resurrect a stale result.
        List<Object> baseline = new ArrayList<>(resultSpace.readAll(resultTemplate, 1_000));
        Subscription subscription = subscribeResults(observed, arrived);
        ai.badmonkey.agentspaces.api.space.EntryHandle written = null;
        try {
            written = tags.isEmpty() ? taskSpace.write(input, writeLease) : taskSpace.write(input, writeLease, tags);
            // The caller's timeout, on the system's time source (EL-C-08).
            long deadline = ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos() + timeout.toNanos();
            // The poll backstop backs off from 100 ms to 2 s (review §7.5, A5); a subscription event resets it.
            long backoffMillis = 100;
            while (ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos() < deadline) {
                long seen = arrived.seen(); // before the look: a result observed meanwhile ends the wait at once
                Object event;
                while ((event = observed.poll()) != null) {
                    if (correlation.matches(input, event)) {
                        return Optional.of(event);
                    }
                }
                // The poll backstop: triggers content-addressed block fetches
                // and covers a subscription that lapsed mid-wait. Anything not
                // present before the write is a candidate.
                List<Object> current = new ArrayList<>(
                        resultSpace.readAll(resultTemplate, 1_000));
                for (Object candidate : newSince(baseline, current)) {
                    if (correlation.matches(input, candidate)) {
                        return Optional.of(candidate);
                    }
                }
                long remaining = Math.max(1, (deadline - ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos()) / 1_000_000);
                try {
                    arrived.await(seen, ai.badmonkey.agentspaces.common.time.Waits.deadline(arrived.time(),
                            Duration.ofMillis(Math.min(backoffMillis, remaining))));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                backoffMillis = observed.isEmpty() ? Math.min(backoffMillis * 2, 2_000) : 100;
            }
            if (onTimeout == OnTimeout.WITHDRAW && stillOffered(written.entryId())) {
                try {
                    written.cancel();
                } catch (RuntimeException e) {
                    // a withdrawal is best effort: the write lease bounds the entry anyway
                }
            }
            return Optional.empty();
        } finally {
            subscription.close();
        }
    }

    /**
     * Invokes the action with a typed result.
     *
     * @param input      the input entry
     * @param resultType the expected result type; must be {@link #outputType()}
     *                   or a supertype of it
     * @param timeout    how long to wait
     * @param <T>        the result type
     * @return the result, or empty when none arrived within the timeout
     */
    public <T> Optional<T> invoke(Object input, Class<T> resultType, Duration timeout) {
        Objects.requireNonNull(resultType, "resultType");
        if (!resultType.isAssignableFrom(outputType)) {
            throw new IllegalArgumentException("this action produces "
                    + outputType.getName() + ", not " + resultType.getName());
        }
        return invoke(input, timeout).map(resultType::cast);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Subscription subscribeResults(ConcurrentLinkedQueue<Object> observed,
                                          ai.badmonkey.agentspaces.common.time.ChangeSignal arrived) {
        return resultSpace.notify((Template) Template.of(outputType), event -> {
            SpaceEvent<?> spaceEvent = (SpaceEvent<?>) event;
            if (spaceEvent.kind() == SpaceEvent.Kind.WRITTEN) {
                observed.add(spaceEvent.entry());
                arrived.run();
            }
        }, Lease.of(Duration.ofMinutes(10)));
    }

    /**
     * Whether the input is still on offer under its write lease (review §7.5, X8): a space lists a taken entry
     * under its take lease or not at all, so an entry a worker already holds is never withdrawn from under it.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean stillOffered(ai.badmonkey.agentspaces.api.entry.EntryId entryId) {
        try {
            List<Space.Entry<Object>> listed = taskSpace.readAllEntries((Template) Template.of(inputType), 1_000);
            for (Space.Entry<?> entry : listed) {
                if (entry.entryId().equals(entryId)) {
                    return entry.lease().kind() == ai.badmonkey.agentspaces.api.entry.LeaseKind.WRITE;
                }
            }
            return false; // gone, or held under a take claim this replica does not list
        } catch (UnsupportedOperationException e) {
            return true; // a space without entry reads: withdraw, as the handle allows
        }
    }

    /** Multiset difference: entries in {@code current} beyond their baseline count. */
    private static List<Object> newSince(List<Object> baseline, List<Object> current) {
        List<Object> remaining = new ArrayList<>(baseline);
        List<Object> fresh = new ArrayList<>();
        for (Object candidate : current) {
            if (!remaining.remove(candidate)) {
                fresh.add(candidate);
            }
        }
        return fresh;
    }

    @Override
    public String toString() {
        return "RemoteAction[" + name() + ": " + inputType.getSimpleName()
                + " -> " + outputType.getSimpleName() + " @ " + card.issuer().value() + "]";
    }
}
