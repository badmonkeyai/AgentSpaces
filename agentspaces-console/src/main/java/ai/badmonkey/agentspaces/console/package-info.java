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

/**
 * The fleet console as a served surface (stability: API for
 * {@link ai.badmonkey.agentspaces.console.ConsoleView},
 * {@link ai.badmonkey.agentspaces.console.FleetConsoleServer}, and the
 * {@link ai.badmonkey.agentspaces.console.ConsolePanel} SPI).
 *
 * <p>The console is one more read-only peer: everything it shows is derived
 * from state the fabric already maintains. Membership answers "who is alive",
 * discovery answers "who can do what", and the replicated space answers
 * "what is the fleet working on". {@code ConsoleView} folds those three
 * sources into a queryable model; {@code FleetConsoleServer} serves the model
 * three ways, each an open standard a different kind of consumer builds on:
 *
 * <ul>
 *   <li>a JSON API under {@code /api/v1} whose documents carry
 *       {@code _links} in the HAL convention, so hypermedia clients
 *       (Spring HATEOAS among them) walk the surface from the root without
 *       hardcoding paths;</li>
 *   <li>a Server-Sent Events activity stream at {@code /api/v1/events},
 *       resumable through the standard {@code Last-Event-ID} header, so any
 *       {@code EventSource} follows fleet activity live;</li>
 *   <li>a single-file, no-build web UI at {@code /}, themed entirely by CSS
 *       custom properties and populated only through the public API, so
 *       replacing or restyling it means editing one HTML file.</li>
 * </ul>
 *
 * <p>Applications extend the console by registering
 * {@link ai.badmonkey.agentspaces.console.ConsolePanel} implementations:
 * each panel contributes a titled section to the UI and a JSON document at
 * {@code /api/v1/panels/{id}/data}, with an optional HTML fragment when the
 * default rendering is not enough. Under Spring Boot, {@code ConsolePanel}
 * beans are collected automatically.
 */
package ai.badmonkey.agentspaces.console;
