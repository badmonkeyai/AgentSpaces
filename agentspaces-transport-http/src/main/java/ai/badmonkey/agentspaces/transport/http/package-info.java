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
 * The HTTP bindings of the Transport SPI (SPEC §5.5; SRAD
 * srad/agentspaces-js.md §3.3): {@link ai.badmonkey.agentspaces.transport.http.WebSocketTransport ws},
 * {@link ai.badmonkey.agentspaces.transport.http.TlsWebSocketTransport wss},
 * {@link ai.badmonkey.agentspaces.transport.http.HttpExchangeTransport http}, and
 * {@link ai.badmonkey.agentspaces.transport.http.TlsHttpExchangeTransport https}, on Netty's HTTP
 * codec. {@code ws} and {@code http} share a plaintext port, {@code wss} and {@code https} a TLS port,
 * routed by path; every listener applies the browser-origin policy.
 */
package ai.badmonkey.agentspaces.transport.http;
