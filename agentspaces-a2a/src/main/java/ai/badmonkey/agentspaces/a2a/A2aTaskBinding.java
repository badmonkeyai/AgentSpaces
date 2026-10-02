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
package ai.badmonkey.agentspaces.a2a;

import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskEntry;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskResult;
import ai.badmonkey.agentspaces.a2a.A2aMessages.Artifact;
import ai.badmonkey.agentspaces.a2a.A2aMessages.Message;
import ai.badmonkey.agentspaces.a2a.A2aMessages.Part;
import ai.badmonkey.agentspaces.a2a.A2aMessages.States;
import ai.badmonkey.agentspaces.a2a.A2aMessages.Status;
import ai.badmonkey.agentspaces.a2a.A2aMessages.Task;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * The full A2A task binding: {@code message/send}
 * becomes a leased {@link A2aTaskEntry} written into the fleet's space, agents
 * take and complete it through whatever conflict strategy the space declares,
 * and task state reads straight out of space semantics with no bookkeeping
 * protocol of its own:
 *
 * <ul>
 *   <li>the entry is readable: no agent holds it, the task is
 *       {@code submitted};</li>
 *   <li>the entry exists but is claimed: an agent holds the take lease, the
 *       task is {@code working};</li>
 *   <li>an {@link A2aTaskResult} with the task's id exists: {@code completed},
 *       and the result text is the task's artifact;</li>
 *   <li>the agent crashed: its take lease lapses, the entry reappears, and the
 *       task honestly reads {@code submitted} again, which is the space's
 *       crash-recovery idiom showing through the A2A surface;</li>
 *   <li>the entry is gone and its write lease has passed: no agent took it in
 *       time and none can now, so the task is {@code failed} with a status
 *       message saying so.</li>
 * </ul>
 *
 * <p>The binding works over any {@link Space}: a {@code LocalSpace} for one
 * process, a replicated space for a fleet. One instance serves one space.
 *
 * <p>Time is injected: the write-lease deadline recorded at {@code message/send}
 * and the comparison in {@code tasks/get} both read the binding's
 * {@link InstantSource}, which should be the same clock the space runs on, so
 * the binding's notion of "the lease has lapsed" agrees with the space's and
 * deterministic tests drive a {@code TestClock} with no sleeping. The
 * constructors without a clock use {@link InstantSource#system()}.
 */
public final class A2aTaskBinding {

    /**
     * One task the binding created. {@code deadline} is when the entry's write
     * lease lapses: past it, a missing entry cannot be a live take (the space
     * drops a taken entry along with its write lease), so the task failed.
     */
    private record Tracked(String contextId, String agent, String text,
                           EntryHandle handle, Message request, Instant deadline,
                           boolean canceled) {

        Tracked canceledCopy() {
            return new Tracked(contextId, agent, text, handle, request, deadline, true);
        }
    }

    private final Space space;
    private final Lease taskLease;
    private final Lease resultLease;
    private final InstantSource clock;
    /** Bound on tracked tasks; oldest evict, since {@code message/send} is
     * externally drivable and must not grow memory without limit (ASF-007). */
    private static final int MAX_TRACKED_TASKS = 4_096;

    private final Map<String, Tracked> tasks =
            java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(64, 0.75f, false) {
                        @Override
                        protected boolean removeEldestEntry(
                                Map.Entry<String, Tracked> eldest) {
                            return size() > MAX_TRACKED_TASKS;
                        }
                    });

    /**
     * Creates the binding with a 10 minute task lease and a 1 hour result lease
     * on the system clock.
     *
     * @param space the space fleet agents work
     */
    public A2aTaskBinding(Space space) {
        this(space, Lease.of(Duration.ofMinutes(10)), Lease.of(Duration.ofHours(1)));
    }

    /**
     * Creates the binding on the system clock.
     *
     * @param space       the space fleet agents work
     * @param taskLease   the write lease on task entries; an unworked task
     *                    vanishes when it lapses
     * @param resultLease how long completed results stay readable
     */
    public A2aTaskBinding(Space space, Lease taskLease, Lease resultLease) {
        this(space, taskLease, resultLease, InstantSource.system());
    }

    /**
     * Creates the binding on the given clock.
     *
     * @param space       the space fleet agents work
     * @param taskLease   the write lease on task entries; an unworked task
     *                    vanishes when it lapses
     * @param resultLease how long completed results stay readable
     * @param clock       the clock task deadlines are recorded and compared on;
     *                    pass the space's own clock so the two agree
     */
    public A2aTaskBinding(Space space, Lease taskLease, Lease resultLease,
                          InstantSource clock) {
        this.space = Objects.requireNonNull(space, "space");
        this.taskLease = Objects.requireNonNull(taskLease, "taskLease");
        this.resultLease = Objects.requireNonNull(resultLease, "resultLease");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Returns the result lease agents should complete tasks under. */
    public Lease resultLease() {
        return resultLease;
    }

    /**
     * Handles {@code message/send}: writes the task entry and returns the task
     * in its current state.
     *
     * @param agentName the addressed agent's local name from the card URL
     * @param text      the message text
     * @return the created task, {@code submitted} (or already further along)
     */
    public Task send(String agentName, String text) {
        Objects.requireNonNull(agentName, "agentName");
        Objects.requireNonNull(text, "text");
        String taskId = UUID.randomUUID().toString();
        String contextId = UUID.randomUUID().toString();
        Message request = new Message("user", List.of(Part.text(text)),
                UUID.randomUUID().toString(), taskId, "message");
        Instant deadline = clock.instant().plus(taskLease.duration());
        EntryHandle handle = space.write(new A2aTaskEntry(taskId, agentName, text),
                taskLease, Map.of("a2a", "true"));
        tasks.put(taskId, new Tracked(contextId, agentName, text, handle, request,
                deadline, false));
        return get(taskId).orElseThrow();
    }

    /**
     * Handles {@code tasks/get}: the task in its current state, read from the
     * space.
     *
     * @param taskId the task id
     * @return the task, or empty when this gateway never created it
     */
    public Optional<Task> get(String taskId) {
        Objects.requireNonNull(taskId, "taskId");
        Tracked tracked = tasks.get(taskId);
        if (tracked == null) {
            return Optional.empty();
        }
        Optional<ai.badmonkey.agentspaces.api.space.Space.Issued<A2aTaskResult>> result =
                space.readAllIssued(
                        Template.of(A2aTaskResult.class).where("taskId", eq(taskId)), 1)
                .stream().findFirst();
        String state;
        Message statusMessage = null;
        List<Artifact> artifacts = List.of();
        List<Message> history = List.of(tracked.request());
        Map<String, String> metadata = Map.of("agentspaces.agent", tracked.agent());
        if (result.isPresent()) {
            state = States.COMPLETED;
            A2aTaskResult answer = result.get().entry();
            artifacts = List.of(new Artifact(UUID.nameUUIDFromBytes(
                    ("artifact:" + taskId).getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .toString(), "result", List.of(Part.text(answer.text()))));
            history = List.of(tracked.request(), new Message("agent",
                    List.of(Part.text(answer.text())),
                    UUID.nameUUIDFromBytes(("reply:" + taskId)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString(),
                    taskId, "message"));
            // ASF-032: the result's provenance travels with the artifact. The
            // issuer is the space-authenticated writer; servedBy is the
            // result's own (unauthenticated) claim. A client that cares
            // whether the fleet's answer came from the expected worker checks
            // the issuer, not the claim.
            metadata = Map.of(
                    "agentspaces.agent", tracked.agent(),
                    "agentspaces.result.issuer", result.get().issuer().encoded(),
                    "agentspaces.result.servedBy", String.valueOf(answer.agent()));
        } else if (tracked.canceled()) {
            state = States.CANCELED;
        } else if (space.read(Template.of(A2aTaskEntry.class)
                .where("taskId", eq(taskId))).isPresent()) {
            state = States.SUBMITTED;
        } else if (clock.instant().isBefore(tracked.deadline())) {
            // Gone while the write lease is live: some agent holds the take.
            state = States.WORKING;
        } else {
            // Gone with the write lease: nobody took it, and nobody will.
            state = States.FAILED;
            statusMessage = new Message("agent", List.of(Part.text(
                    "no worker took the task before its lease expired")),
                    UUID.nameUUIDFromBytes(("status:" + taskId)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString(),
                    taskId, "message");
        }
        return Optional.of(new Task(taskId, tracked.contextId(),
                new Status(state, statusMessage), artifacts, history, "task", metadata));
    }

    /**
     * Handles {@code tasks/cancel}: withdraws the entry so no agent can take it,
     * and marks the task canceled. A task an agent already completed stays
     * completed; cancellation is not retroactive.
     *
     * @param taskId the task id
     * @return the task in its resulting state, or empty when unknown
     */
    public Optional<Task> cancel(String taskId) {
        Objects.requireNonNull(taskId, "taskId");
        Tracked tracked = tasks.get(taskId);
        if (tracked == null) {
            return Optional.empty();
        }
        Optional<Task> current = get(taskId);
        if (current.isPresent()
                && States.COMPLETED.equals(current.get().status().state())) {
            return current;
        }
        try {
            tracked.handle().cancel();
        } catch (RuntimeException e) {
            // Already gone (taken and completed, or lapsed): state tells the truth.
        }
        tasks.put(taskId, tracked.canceledCopy());
        return get(taskId);
    }
}
