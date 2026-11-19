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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Locale;
import java.util.Map;

/**
 * The A2A 1.0 JSON forms of the binding's documents (ProtoJSON of a2a.proto
 * 1.0): no {@code kind} discriminators, {@code TASK_STATE_*} and
 * {@code ROLE_*} enum names, a part told by its member ({@code text}), and the
 * {@code SendMessageResponse} and {@code StreamResponse} wrappers. The 0.3
 * forms are the {@link A2aMessages} records as Jackson writes them; this class
 * reads those records and writes 1.0.
 */
final class A2aWire {

    /** The domain of an A2A error's {@code google.rpc.ErrorInfo} (A2A 1.0 §9.5). */
    static final String ERROR_DOMAIN = "a2a-protocol.org";
    /** The {@code @type} of a {@code google.rpc.ErrorInfo} detail. */
    static final String ERROR_INFO = "type.googleapis.com/google.rpc.ErrorInfo";

    private final ObjectMapper json;

    A2aWire(ObjectMapper json) {
        this.json = json;
    }

    /** {@code submitted} is {@code TASK_STATE_SUBMITTED}, {@code input-required} is {@code TASK_STATE_INPUT_REQUIRED}. */
    static String state(String state) {
        return "TASK_STATE_" + state.toUpperCase(Locale.ROOT).replace('-', '_');
    }

    /** {@code user} is {@code ROLE_USER}, {@code agent} is {@code ROLE_AGENT}. */
    static String role(String role) {
        return "ROLE_" + role.toUpperCase(Locale.ROOT);
    }

    /**
     * A task (1.0 {@code Task}).
     *
     * @param task          the task
     * @param historyLength the request's {@code historyLength}, or a missing
     *                      node for the default (all of it)
     */
    ObjectNode task(A2aMessages.Task task, JsonNode historyLength) {
        ObjectNode out = json.createObjectNode();
        out.put("id", task.id());
        out.put("contextId", task.contextId());
        out.set("status", status(task.status(), task.contextId()));
        if (!task.artifacts().isEmpty()) {
            ArrayNode artifacts = out.putArray("artifacts");
            task.artifacts().forEach(a -> artifacts.add(artifact(a)));
        }
        int keep = historyLength.canConvertToInt() && historyLength.asInt() >= 0
                ? historyLength.asInt() : Integer.MAX_VALUE;
        // §3.2.4: 0 omits the history, n keeps the n most recent messages
        if (keep > 0 && !task.history().isEmpty()) {
            ArrayNode history = out.putArray("history");
            int from = Math.max(0, task.history().size() - keep);
            task.history().subList(from, task.history().size())
                    .forEach(m -> history.add(message(m, task.contextId())));
        }
        if (task.metadata() != null && !task.metadata().isEmpty()) {
            out.set("metadata", json.valueToTree(task.metadata()));
        }
        return out;
    }

    /** {@code SendMessageResponse} and a stream's first event: {@code {"task": ...}}. */
    ObjectNode taskResponse(A2aMessages.Task task, JsonNode historyLength) {
        ObjectNode out = json.createObjectNode();
        out.set("task", task(task, historyLength));
        return out;
    }

    /** A stream's status event: {@code {"statusUpdate": ...}}, with no {@code final} (1.0 closes the stream). */
    ObjectNode statusUpdate(A2aMessages.Task task) {
        ObjectNode event = json.createObjectNode();
        ObjectNode update = event.putObject("statusUpdate");
        update.put("taskId", task.id());
        update.put("contextId", task.contextId());
        update.set("status", status(task.status(), task.contextId()));
        return event;
    }

    /** A stream's artifact event: {@code {"artifactUpdate": ...}} carrying the first artifact whole. */
    ObjectNode artifactUpdate(A2aMessages.Task task) {
        ObjectNode event = json.createObjectNode();
        ObjectNode update = event.putObject("artifactUpdate");
        update.put("taskId", task.id());
        update.put("contextId", task.contextId());
        update.set("artifact", artifact(task.artifacts().get(0)));
        update.put("lastChunk", true);
        return event;
    }

    private ObjectNode status(A2aMessages.Status status, String contextId) {
        ObjectNode out = json.createObjectNode();
        out.put("state", state(status.state()));
        if (status.message() != null) {
            out.set("message", message(status.message(), contextId));
        }
        return out;
    }

    private ObjectNode message(A2aMessages.Message message, String contextId) {
        ObjectNode out = json.createObjectNode();
        out.put("messageId", message.messageId());
        out.put("contextId", contextId);
        out.put("taskId", message.taskId());
        out.put("role", role(message.role()));
        ArrayNode parts = out.putArray("parts");
        message.parts().forEach(p -> parts.add(part(p)));
        return out;
    }

    private ObjectNode artifact(A2aMessages.Artifact artifact) {
        ObjectNode out = json.createObjectNode();
        out.put("artifactId", artifact.artifactId());
        out.put("name", artifact.name());
        ArrayNode parts = out.putArray("parts");
        artifact.parts().forEach(p -> parts.add(part(p)));
        return out;
    }

    private ObjectNode part(A2aMessages.Part part) {
        ObjectNode out = json.createObjectNode();
        out.put("text", part.text());
        return out;
    }

    /**
     * An A2A error's detail list: one {@code google.rpc.ErrorInfo} with the
     * error's reason, as the 1.0 JSON-RPC binding puts it in {@code error.data}.
     */
    ArrayNode errorInfo(String reason, Map<String, String> metadata) {
        ArrayNode data = json.createArrayNode();
        ObjectNode info = data.addObject();
        info.put("@type", ERROR_INFO);
        info.put("reason", reason);
        info.put("domain", ERROR_DOMAIN);
        if (metadata != null && !metadata.isEmpty()) {
            info.set("metadata", json.valueToTree(metadata));
        }
        return data;
    }
}
