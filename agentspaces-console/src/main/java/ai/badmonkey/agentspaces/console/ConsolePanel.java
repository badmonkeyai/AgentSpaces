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

/**
 * The console's extension point: one additional titled section on the served
 * surface. A panel contributes a JSON document at
 * {@code /api/v1/panels/{id}/data}, re-read per request so it always reflects
 * live state, and the stock UI renders that document as a key-value table
 * refreshed on the overview cadence. A panel that wants richer rendering
 * overrides {@link #htmlFragment()} with markup (scripts included) that the
 * UI injects instead; the fragment reads its own data endpoint like any other
 * client.
 *
 * <p>This is deliberately the whole contract. Panels never touch the HTTP
 * server, routing, or the page shell, which is what keeps a fleet console
 * with ten team-specific panels upgradeable: the shell evolves without
 * breaking a single panel. Under Spring Boot, every {@code ConsolePanel} bean
 * in the context is registered automatically.
 */
public interface ConsolePanel {

    /**
     * The panel's stable identifier, used in its data URL. Lowercase words
     * joined by dashes by convention, unique per console.
     *
     * @return the identifier
     */
    String id();

    /**
     * The section title shown on the UI.
     *
     * @return the human-readable title
     */
    String title();

    /**
     * The panel's live document, serialized to JSON per request. Maps, lists,
     * records, strings, and numbers all serialize naturally.
     *
     * @return the current data, never null
     */
    Object data();

    /**
     * Optional custom markup for the panel body. The default, an empty
     * string, tells the UI to render {@link #data()} generically.
     *
     * @return an HTML fragment, or the empty string for default rendering
     */
    default String htmlFragment() {
        return "";
    }
}
