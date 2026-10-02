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
package ai.badmonkey.agentspaces.examples.hello;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.identity.FileKeystore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;

import java.nio.file.Path;
import java.time.Duration;

import static ai.badmonkey.agentspaces.api.space.Matchers.gte;

/**
 * Example 01: one process, one space. Demonstrates the whole coordination model in
 * miniature: leased writes, template takes, atomic complete-with-result, event
 * notifications, and the crash-recovery idiom (a lapsed take lease makes the task
 * reappear). Run with:
 *
 * <pre>{@code mvn -q -pl examples/example-01-hello-space exec:java}</pre>
 */
public final class HelloSpace {

    /** A unit of work agents coordinate on. */
    public record ResearchTask(String topic, int priority) {
    }

    /** The result an agent produces. */
    public record Finding(String topic, String summary) {
    }

    private HelloSpace() {
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on interruption
     */
    public static void main(String[] args) throws Exception {
        // Layer 0: a stable peer identity from a keystore (created on first run).
        PeerIdentity identity = FileKeystore.loadOrCreate(Path.of("peer.keys"));
        System.out.println("peer  : " + identity.peerId().display());

        try (LocalSpace space = LocalSpace.builder("tasks", identity.agent("coordinator"))
                .sweepEvery(Duration.ofMillis(100))
                .build()) {

            // Watch everything that happens to research tasks.
            space.notify(Template.of(ResearchTask.class),
                    (SpaceEvent<ResearchTask> event) ->
                            System.out.println("event : " + event.kind() + " " + event.entry().topic()),
                    Lease.of(Duration.ofMinutes(5)));

            // A coordinator publishes work under a 30-minute lease.
            space.write(new ResearchTask("agentic memory", 3), Lease.of(Duration.ofMinutes(30)));
            space.write(new ResearchTask("tuple spaces", 7), Lease.of(Duration.ofMinutes(30)));

            // A worker takes the most urgent task... and crashes (never completes).
            TakenEntry<ResearchTask> doomed = space.take(
                    Template.of(ResearchTask.class).where("priority", gte(5)),
                    Lease.of(Duration.ofMillis(300)), Duration.ofSeconds(1)).orElseThrow();
            System.out.println("worker A took '" + doomed.entry().topic() + "' and crashed");

            // The take lease lapses; the task reappears for a healthy worker.
            Thread.sleep(600);
            TakenEntry<ResearchTask> retry = space.take(
                    Template.of(ResearchTask.class).where("priority", gte(5)),
                    Lease.of(Duration.ofMinutes(10)), Duration.ofSeconds(5)).orElseThrow();
            System.out.println("worker B took '" + retry.entry().topic() + "' after the lease lapsed");

            // Worker B completes the take and atomically writes its result.
            space.complete(retry,
                    new Finding(retry.entry().topic(), "write/take/complete with leases: it works"),
                    Lease.of(Duration.ofHours(1)));

            Finding finding = space.read(Template.of(Finding.class), Duration.ofSeconds(1)).orElseThrow();
            System.out.println("result: " + finding.topic() + " -> " + finding.summary());
        }
    }
}
