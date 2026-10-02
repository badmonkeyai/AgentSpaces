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

import java.util.List;
import java.util.Map;

/**
 * The command-and-control extension point: one operator action the console can
 * execute against the fleet. This is the command-side sibling of
 * {@link ConsolePanel} (which is read-only). An application registers commands
 * to give operators typed actions, dispatching a task into a space, triggering
 * a key rotation, invoking a capability, and the console renders a form from
 * {@link #fields()} and executes {@link #execute} on submit.
 *
 * <p>Every command runs under the console peer's own fabric identity: the
 * entries a command writes are signed and leased by the console peer, and the
 * {@link CommandContext#operator()} name travels as attribution. The HTTP layer
 * authenticates the operator (a bearer token) before any command runs; a
 * command implementation never sees an unauthenticated request. Under Spring
 * Boot, every {@code ConsoleCommand} bean is registered automatically.
 */
public interface ConsoleCommand {

    /** A form field the console renders for a command's input. */
    record Field(String name, String label, Kind kind, List<String> options,
                 boolean required) {

        /** The field's input type, which the UI renders accordingly. */
        public enum Kind {
            /** A single-line text input. */
            TEXT,
            /** A numeric input. */
            NUMBER,
            /** A dropdown over {@link Field#options()}. */
            SELECT
        }

        /** A required text field. */
        public static Field text(String name, String label) {
            return new Field(name, label, Kind.TEXT, List.of(), true);
        }

        /** A required numeric field. */
        public static Field number(String name, String label) {
            return new Field(name, label, Kind.NUMBER, List.of(), true);
        }

        /** A required dropdown field. */
        public static Field select(String name, String label, List<String> options) {
            return new Field(name, label, Kind.SELECT, List.copyOf(options), true);
        }
    }

    /** The result of executing a command. */
    record Outcome(boolean ok, String message, String entryId) {

        /** A success carrying a human-readable message. */
        public static Outcome ok(String message) {
            return new Outcome(true, message, null);
        }

        /** A success carrying the id of an entry the command dispatched. */
        public static Outcome dispatched(String message, String entryId) {
            return new Outcome(true, message, entryId);
        }

        /** A failure carrying the reason. */
        public static Outcome failed(String message) {
            return new Outcome(false, message, null);
        }
    }

    /**
     * The command's stable identifier, used in its execution URL. Lowercase
     * words joined by dashes by convention, unique per console.
     *
     * @return the identifier
     */
    String id();

    /**
     * The command's title, shown on the UI.
     *
     * @return the human-readable title
     */
    String title();

    /**
     * An optional one-line description of what the command does.
     *
     * @return the description, or the empty string
     */
    default String description() {
        return "";
    }

    /**
     * The input fields the operator fills in, rendered as a form.
     *
     * @return the fields, possibly empty for a no-argument command
     */
    default List<Field> fields() {
        return List.of();
    }

    /**
     * Executes the command. Runs on an authenticated request, under the console
     * peer's identity.
     *
     * @param args the submitted field values, keyed by {@link Field#name()}
     * @param ctx  the command context (operator, dispatch, cancel, broadcast, audit)
     * @return the outcome
     */
    Outcome execute(Map<String, String> args, CommandContext ctx);
}
