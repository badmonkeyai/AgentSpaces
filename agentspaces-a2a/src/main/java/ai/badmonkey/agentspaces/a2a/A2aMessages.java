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

import java.util.List;
import java.util.Map;

/**
 * The A2A task-binding shapes: the JSON documents A2A clients exchange with
 * the gateway, and the two space entry types the binding bridges them to. An
 * inbound {@code message/send} becomes an {@link A2aTaskEntry} written into
 * the fleet's space; any agent takes it through the space's conflict strategy
 * and completes it with an {@link A2aTaskResult}; {@code tasks/get} reads the
 * task's state straight out of space semantics, so "working" literally means
 * "some agent holds the take lease" and a crashed agent's task returns to
 * "submitted" when the lease lapses.
 */
public final class A2aMessages {

    private A2aMessages() {
    }

    // ------------------------------------------------------------ space entries

    /**
     * The space entry an inbound A2A message becomes. Fleet agents take these.
     *
     * @param taskId the A2A task id, the correlation key
     * @param agent  the addressed agent's local name; empty means any agent
     * @param text   the message text
     */
    public record A2aTaskEntry(String taskId, String agent, String text) {
    }

    /**
     * The result entry an agent completes an {@link A2aTaskEntry} with.
     *
     * @param taskId the A2A task id the result answers
     * @param agent  the completing agent's local name
     * @param text   the result text
     */
    public record A2aTaskResult(String taskId, String agent, String text) {
    }

    // ------------------------------------------------------------- A2A documents

    /** A2A task states the binding emits. */
    public static final class States {
        /** Written into the space; no agent holds it. */
        public static final String SUBMITTED = "submitted";
        /** Some agent holds the take lease right now. */
        public static final String WORKING = "working";
        /** A result entry exists. */
        public static final String COMPLETED = "completed";
        /** The client canceled the task and the entry was withdrawn. */
        public static final String CANCELED = "canceled";
        /** The entry's write lease lapsed before any agent took it. */
        public static final String FAILED = "failed";

        private States() {
        }
    }

    /**
     * One content part; the v0.1 binding speaks text parts.
     *
     * @param kind {@code "text"}
     * @param text the content
     */
    public record Part(String kind, String text) {

        /** Returns a text part. */
        public static Part text(String text) {
            return new Part("text", text);
        }
    }

    /**
     * An A2A message.
     *
     * @param role      {@code "user"} or {@code "agent"}
     * @param parts     the content parts
     * @param messageId unique message identifier
     * @param taskId    the task the message belongs to
     * @param kind      {@code "message"}
     */
    public record Message(String role, List<Part> parts, String messageId,
                          String taskId, String kind) {
    }

    /**
     * An A2A artifact: a task's output.
     *
     * @param artifactId unique artifact identifier
     * @param name       display name
     * @param parts      the content parts
     */
    public record Artifact(String artifactId, String name, List<Part> parts) {
    }

    /**
     * An A2A task status.
     *
     * @param state   one of {@link States}
     * @param message an agent message explaining the state, present on
     *                {@link States#FAILED}; omitted from JSON when absent
     */
    @com.fasterxml.jackson.annotation.JsonInclude(
            com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    public record Status(String state, Message message) {

        /** Returns a status with no explanatory message. */
        public Status(String state) {
            this(state, null);
        }
    }

    /**
     * The A2A task document {@code message/send} and {@code tasks/get} return.
     *
     * @param id        the task id
     * @param contextId the conversation context id
     * @param status    current status
     * @param artifacts outputs; present once completed
     * @param history   the messages so far
     * @param kind      {@code "task"}
     * @param metadata  extension fields (the AgentSpaces entry identity)
     */
    public record Task(String id, String contextId, Status status,
                       List<Artifact> artifacts, List<Message> history,
                       String kind, Map<String, String> metadata) {
    }
}
