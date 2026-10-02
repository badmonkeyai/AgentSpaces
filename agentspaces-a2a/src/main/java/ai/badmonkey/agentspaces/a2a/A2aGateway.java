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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.security.BearerAuthenticator;
import ai.badmonkey.agentspaces.api.security.BearerAuthenticator.Principal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * The A2A gateway prototype: serves a group's discovered
 * {@link AgentCard}s as A2A agent cards over HTTP, so A2A clients see an
 * AgentSpaces fleet through the standard discovery surface. Three routes:
 *
 * <ul>
 *   <li>{@code /.well-known/agent-card.json}: the gateway's own card, its
 *       skill list the union of every agent's skills, which is how one URL
 *       represents a whole fleet to an A2A client;</li>
 *   <li>{@code /agents}: the full list of translated cards;</li>
 *   <li>{@code /agents/{name}}: one agent's card by local name.</li>
 * </ul>
 *
 * <p>The card source is a supplier, typically a closure over a discovery
 * service: {@code () -> discovery.find(AgentCard.class, c -> true)}. Cards are
 * re-read per request, so the gateway always reflects the current leased state
 * of the fleet: an agent whose card lapsed disappears from the surface.
 *
 * <p>With a {@link #taskBinding(A2aTaskBinding) task binding} attached, the
 * gateway also serves the A2A task surface as JSON-RPC 2.0 POSTed to an
 * agent's URL: {@code message/send} writes a task entry into the fleet's
 * space, {@code tasks/get} reports the task's state straight from space
 * semantics, and {@code tasks/cancel} withdraws the entry. Without a binding
 * the gateway is a read-only discovery surface.
 */
public final class A2aGateway implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(A2aGateway.class.getName());

    /**
     * The OAuth2 scope a scoped principal (an identity-provider JWT) must carry
     * to use the JSON-RPC surface; the default for {@link #requiredScope(String)}.
     */
    public static final String CLIENT_SCOPE = "aspace:a2a:client";

    private static final String JSON = "application/json; charset=utf-8";

    /** Largest accepted JSON-RPC request body (ASF-007). */
    private static final int MAX_RPC_BODY_BYTES = 64 * 1024;
    /** Longest accepted message text part (ASF-007). */
    private static final int MAX_TEXT_LENGTH = 16 * 1024;
    /** Bound on live push registrations; oldest evict (ASF-007). */
    private static final int MAX_PUSH_CONFIGS = 1_024;
    /** Concurrent SSE streams and push watchers, each (ASF-040). */
    private static final int MAX_CONCURRENT_STREAMS = 32;

    private final String fleetName;
    private final String provider;
    private final Supplier<List<AgentCard>> cards;
    private final ObjectMapper json = new ObjectMapper();
    /** Live push registrations, bounded: oldest evict so a registration flood
     * cannot grow memory without bound (ASF-007). */
    private final java.util.Map<String, PushConfig> pushConfigs =
            java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(64, 0.75f, false) {
                        @Override
                        protected boolean removeEldestEntry(
                                java.util.Map.Entry<String, PushConfig> eldest) {
                            return size() > MAX_PUSH_CONFIGS;
                        }
                    });
    private final java.net.http.HttpClient pushHttp =
            java.net.http.HttpClient.newHttpClient();
    private final java.util.concurrent.Semaphore streamSlots =
            new java.util.concurrent.Semaphore(MAX_CONCURRENT_STREAMS);
    private final java.util.concurrent.Semaphore watcherSlots =
            new java.util.concurrent.Semaphore(MAX_CONCURRENT_STREAMS);
    private final java.util.Set<String> webhookHostAllowlist =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    private volatile BearerAuthenticator bearer;
    private volatile String requiredScope = CLIENT_SCOPE;
    private volatile String externalBaseUrl;
    private volatile A2aTaskBinding binding;
    private HttpServer server;

    /**
     * One task's push-notification registration (A2A
     * {@code tasks/pushNotificationConfig/*}).
     *
     * @param url   the webhook the gateway POSTs task updates to
     * @param token echoed back as {@code X-A2A-Notification-Token} so the
     *              receiver can authenticate the callback
     */
    public record PushConfig(String url, String token) {
    }

    /**
     * Creates the gateway.
     *
     * @param fleetName the fleet's display name on the gateway card
     * @param provider  the operating organization named on every card
     * @param cards     the card source; called per request
     */
    public A2aGateway(String fleetName, String provider, Supplier<List<AgentCard>> cards) {
        this.fleetName = Objects.requireNonNull(fleetName, "fleetName");
        this.provider = Objects.requireNonNull(provider, "provider");
        this.cards = Objects.requireNonNull(cards, "cards");
    }

    /**
     * Attaches the task binding that makes the gateway a live A2A endpoint.
     *
     * @param binding the binding bridging tasks to the fleet's space
     * @return this gateway
     */
    public A2aGateway taskBinding(A2aTaskBinding binding) {
        this.binding = Objects.requireNonNull(binding, "binding");
        return this;
    }

    /**
     * Requires one static bearer token on every JSON-RPC call (ASF-007): the
     * shared-secret form of {@link #bearer(BearerAuthenticator)}, equivalent to
     * {@code bearer(BearerAuthenticator.staticToken(token, "operator"))}, so
     * the token is compared constant-time and its holder carries no scopes.
     * Discovery GETs (the agent cards) stay open — they are the published
     * surface.
     *
     * @param token the operator token A2A clients must present
     * @return this gateway
     */
    public A2aGateway operatorToken(String token) {
        return bearer(BearerAuthenticator.staticToken(
                Objects.requireNonNull(token, "token"), "operator"));
    }

    /**
     * Gates every JSON-RPC call behind a {@link BearerAuthenticator} (TODO-EFG
     * §5): mutating and task-reading methods demand
     * {@code Authorization: Bearer <token>}; a token the authenticator refuses
     * is 401 with {@code WWW-Authenticate: Bearer}, and a principal that
     * carries scopes but not the {@linkplain #requiredScope(String) required
     * one} is 403. With the OIDC module's {@code BearerJwtValidator} the
     * gateway is an OAuth2 resource server for its A2A clients. Discovery GETs
     * (the agent cards) stay open. <strong>Without any gate the RPC surface is
     * open</strong>, which is acceptable only behind the default loopback bind.
     *
     * @param authenticator the bearer gate on the RPC surface
     * @return this gateway
     */
    public A2aGateway bearer(BearerAuthenticator authenticator) {
        this.bearer = Objects.requireNonNull(authenticator, "authenticator");
        return this;
    }

    /**
     * The scope a scoped principal must carry on the RPC surface; default
     * {@link #CLIENT_SCOPE}. A principal without scopes (a static token) is
     * never scope-checked.
     *
     * @param scope the required OAuth2 scope
     * @return this gateway
     */
    public A2aGateway requiredScope(String scope) {
        Objects.requireNonNull(scope, "scope");
        if (scope.isBlank()) {
            throw new IllegalArgumentException("requiredScope must not be blank");
        }
        this.requiredScope = scope;
        return this;
    }

    /**
     * Sets the base URL every served card advertises (SPEC §12): the address
     * A2A clients reach this gateway at, which behind a TLS-terminating proxy
     * or a NAT differs from the bound socket. Unset, cards advertise
     * {@code http://<bind host>:<port>}, with {@code localhost} standing in
     * for a wildcard bind. A trailing slash is dropped.
     *
     * @param url the externally visible base URL, e.g.
     *            {@code https://fleet.example.com/a2a}
     * @return this gateway
     */
    public A2aGateway externalBaseUrl(String url) {
        String trimmed = Objects.requireNonNull(url, "url").strip();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("externalBaseUrl must not be blank");
        }
        this.externalBaseUrl = trimmed;
        return this;
    }

    /**
     * Permits push-notification webhooks on the given host, overriding the
     * default private-address denial for exactly that host (ASF-007). Public
     * hosts need no listing.
     *
     * @param host a hostname or literal address, compared case-insensitively
     * @return this gateway
     */
    public A2aGateway allowWebhookHost(String host) {
        webhookHostAllowlist.add(Objects.requireNonNull(host, "host")
                .toLowerCase(java.util.Locale.ROOT));
        return this;
    }

    /**
     * Starts serving on the loopback interface — the secure default (ASF-007).
     * Binding a routable interface is an explicit choice via
     * {@link #start(InetSocketAddress)}, and should come with an
     * {@link #operatorToken(String) operator token} or a TLS-terminating proxy.
     *
     * @param port the port to bind; 0 picks a free port
     * @return the bound port
     * @throws IOException if the port cannot be bound
     */
    public synchronized int start(int port) throws IOException {
        return start(new InetSocketAddress(
                java.net.InetAddress.getLoopbackAddress(), port));
    }

    /**
     * Starts serving on an explicit address (a routable interface is the
     * caller's deliberate choice).
     *
     * @param address the address to bind
     * @return the bound port
     * @throws IOException if the address cannot be bound
     */
    public synchronized int start(InetSocketAddress address) throws IOException {
        if (server != null) {
            throw new IllegalStateException("already started");
        }
        server = HttpServer.create(address, 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/.well-known/agent-card.json", this::serveGatewayCard);
        server.createContext("/agents", this::serveAgents);
        server.start();
        return server.getAddress().getPort();
    }

    /** Returns the bound port. */
    public synchronized int port() {
        return boundAddress().getPort();
    }

    private synchronized InetSocketAddress boundAddress() {
        if (server == null) {
            throw new IllegalStateException("not started");
        }
        return server.getAddress();
    }

    @Override
    public synchronized void close() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    /**
     * The base URL cards advertise: the configured
     * {@link #externalBaseUrl(String) external URL}, else the bound address.
     * A wildcard bind has no reachable literal, so {@code localhost} stands
     * in; an IPv6 literal is bracketed and its scope id dropped.
     */
    private String baseUrl() {
        String external = externalBaseUrl;
        if (external != null) {
            return external;
        }
        InetSocketAddress bound = boundAddress();
        java.net.InetAddress address = bound.getAddress();
        String host;
        if (address == null || address.isAnyLocalAddress()) {
            host = "localhost";
        } else {
            host = bound.getHostString();
            int scope = host.indexOf('%');
            if (scope >= 0) {
                host = host.substring(0, scope);
            }
            if (host.contains(":")) {
                host = "[" + host + "]";
            }
        }
        return "http://" + host + ":" + bound.getPort();
    }

    private A2aAgentCard.Capabilities endpointCapabilities() {
        // With a task binding attached, SSE streaming and webhook push both
        // work; discovery-only gateways advertise nothing.
        return binding == null ? A2aAgentCard.Capabilities.none()
                : new A2aAgentCard.Capabilities(true, true, false);
    }

    private List<A2aAgentCard> translated() {
        List<A2aAgentCard> result = new ArrayList<>();
        for (AgentCard card : cards.get()) {
            result.add(A2aTranslator.translate(card, baseUrl(), provider,
                    endpointCapabilities()));
        }
        return result;
    }

    private void serveGatewayCard(HttpExchange exchange) throws IOException {
        List<A2aAgentCard.Skill> allSkills = new ArrayList<>();
        translated().forEach(card -> allSkills.addAll(card.skills()));
        A2aAgentCard gatewayCard = new A2aAgentCard(
                A2aAgentCard.PROTOCOL_VERSION,
                fleetName,
                "AgentSpaces fleet gateway: one A2A surface for every agent "
                        + "currently advertising in the group.",
                baseUrl() + "/agents",
                "JSONRPC",
                "0.1",
                new A2aAgentCard.Provider(provider, baseUrl()),
                endpointCapabilities(),
                List.of("application/json"),
                List.of("application/json"),
                allSkills,
                java.util.Map.of("agentspaces.gateway", "true"));
        respond(exchange, 200, json.writeValueAsBytes(gatewayCard));
    }

    private void serveAgents(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/agents") || path.equals("/agents/")) {
            respond(exchange, 200, json.writeValueAsBytes(translated()));
            return;
        }
        String name = path.substring("/agents/".length());
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            serveRpc(exchange, name);
            return;
        }
        Optional<A2aAgentCard> card = translated().stream()
                .filter(c -> c.name().equals(name))
                .findFirst();
        if (card.isPresent()) {
            respond(exchange, 200, json.writeValueAsBytes(card.get()));
        } else {
            respond(exchange, 404, json.writeValueAsBytes(
                    java.util.Map.of("error", "no agent named '" + name + "'")));
        }
    }

    // ------------------------------------------------------- JSON-RPC task surface

    private void serveRpc(HttpExchange exchange, String agentName) throws IOException {
        BearerAuthenticator gate = bearer;
        if (gate != null) {
            Optional<Principal> principal = authenticate(gate, exchange);
            if (principal.isEmpty()) {
                exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
                respond(exchange, 401, json.writeValueAsBytes(
                        java.util.Map.of("error", "missing or invalid bearer token")));
                return;
            }
            String scope = requiredScope;
            if (!principal.get().permitsScope(scope)) {
                respond(exchange, 403, json.writeValueAsBytes(java.util.Map.of(
                        "error", "the scope '" + scope + "' is required")));
                return;
            }
        }
        JsonNode request;
        try {
            // Bounded read (ASF-007): a request beyond the cap is hostile.
            byte[] body = exchange.getRequestBody()
                    .readNBytes(MAX_RPC_BODY_BYTES + 1);
            if (body.length > MAX_RPC_BODY_BYTES) {
                respond(exchange, 200, rpcError(json.getNodeFactory().nullNode(),
                        -32600, "request body exceeds " + MAX_RPC_BODY_BYTES + " bytes"));
                return;
            }
            request = json.readTree(body);
        } catch (IOException | RuntimeException e) {
            respond(exchange, 200,
                    rpcError(json.getNodeFactory().nullNode(), -32700, "Parse error"));
            return;
        }
        JsonNode id = request.path("id");
        String method = request.path("method").asText("");
        A2aTaskBinding taskBinding = binding;
        if (taskBinding == null) {
            respond(exchange, 200, rpcError(id, -32601,
                    "this gateway serves discovery only; no task binding attached"));
            return;
        }
        try {
            switch (method) {
                case "message/send" -> {
                    Optional<String> text = firstTextPart(request.path("params"));
                    if (text.isEmpty()) {
                        respond(exchange, 200, rpcError(id, -32602,
                                "params.message.parts must contain a text part"));
                        return;
                    }
                    if (translated().stream().noneMatch(c -> c.name().equals(agentName))) {
                        respond(exchange, 200, rpcError(id, -32602,
                                "no agent named '" + agentName + "'"));
                        return;
                    }
                    respond(exchange, 200,
                            rpcResult(id, taskBinding.send(agentName, text.get())));
                }
                case "tasks/get" -> respondTask(exchange, id,
                        taskBinding.get(request.path("params").path("id").asText("")));
                case "tasks/cancel" -> respondTask(exchange, id,
                        taskBinding.cancel(request.path("params").path("id").asText("")));
                case "message/stream" -> {
                    Optional<String> text = firstTextPart(request.path("params"));
                    if (text.isEmpty()) {
                        respond(exchange, 200, rpcError(id, -32602,
                                "params.message.parts must contain a text part"));
                        return;
                    }
                    if (translated().stream().noneMatch(c -> c.name().equals(agentName))) {
                        respond(exchange, 200, rpcError(id, -32602,
                                "no agent named '" + agentName + "'"));
                        return;
                    }
                    streamTask(exchange, id, taskBinding,
                            taskBinding.send(agentName, text.get()).id());
                }
                case "tasks/resubscribe" -> {
                    String taskId = request.path("params").path("id").asText("");
                    if (taskBinding.get(taskId).isEmpty()) {
                        respond(exchange, 200, rpcError(id, -32001, "task not found"));
                        return;
                    }
                    streamTask(exchange, id, taskBinding, taskId);
                }
                case "tasks/pushNotificationConfig/set" -> {
                    String taskId = request.path("params").path("taskId").asText("");
                    String url = request.path("params").path("pushNotificationConfig")
                            .path("url").asText("");
                    String token = request.path("params").path("pushNotificationConfig")
                            .path("token").asText(null);
                    if (taskBinding.get(taskId).isEmpty()) {
                        respond(exchange, 200, rpcError(id, -32001, "task not found"));
                        return;
                    }
                    if (url.isBlank()) {
                        respond(exchange, 200, rpcError(id, -32602,
                                "pushNotificationConfig.url is required"));
                        return;
                    }
                    if (!webhookPermitted(url)) {
                        // ASF-007: deny the blind-SSRF targets by default.
                        respond(exchange, 200, rpcError(id, -32602,
                                "pushNotificationConfig.url is not a permitted webhook target"));
                        return;
                    }
                    PushConfig config = new PushConfig(url, token);
                    pushConfigs.put(taskId, config);
                    watchAndPush(taskBinding, taskId, config);
                    respond(exchange, 200, rpcResult(id, java.util.Map.of(
                            "taskId", taskId, "pushNotificationConfig", config)));
                }
                case "tasks/pushNotificationConfig/get" -> {
                    String taskId = request.path("params").path("id").asText(
                            request.path("params").path("taskId").asText(""));
                    PushConfig config = pushConfigs.get(taskId);
                    if (config == null) {
                        respond(exchange, 200, rpcError(id, -32001,
                                "no push configuration for task '" + taskId + "'"));
                        return;
                    }
                    // The callback token authenticates the gateway to the
                    // receiver; reading it back needs only the taskId, so it
                    // is redacted here (ASF-037).
                    respond(exchange, 200, rpcResult(id, java.util.Map.of(
                            "taskId", taskId, "pushNotificationConfig",
                            new PushConfig(config.url(), null))));
                }
                default -> respond(exchange, 200, rpcError(id, -32601,
                        "unknown method '" + method + "'"));
            }
        } catch (RuntimeException e) {
            // Internal detail stays server-side (ASF-036).
            LOG.log(System.Logger.Level.WARNING, "rpc '" + method + "' failed", e);
            respond(exchange, 200, rpcError(id, -32603, "internal error"));
        }
    }

    /** Authenticates the {@code Authorization: Bearer} credential against the gate. */
    private static Optional<Principal> authenticate(BearerAuthenticator gate,
                                                    HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Authorization");
        if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return Optional.empty();
        }
        String presented = header.substring(7).trim();
        if (presented.isEmpty()) {
            return Optional.empty();
        }
        return gate.authenticate(presented);
    }

    /**
     * Whether a push webhook URL is permitted (ASF-007): http(s) only, and the
     * host must not resolve to loopback, private, link-local, unspecified, or
     * multicast space — the classic blind-SSRF targets — unless the operator
     * allowlisted that exact host. Resolution happens here, once, so a DNS
     * name cannot pass the check and then re-resolve to an internal address
     * for the first POST within this process's resolver cache window.
     */
    private boolean webhookPermitted(String url) {
        java.net.URI uri;
        try {
            uri = java.net.URI.create(url);
        } catch (IllegalArgumentException e) {
            return false;
        }
        String scheme = uri.getScheme();
        String host = uri.getHost();
        if (host == null
                || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
            return false;
        }
        if (webhookHostAllowlist.contains(host.toLowerCase(java.util.Locale.ROOT))) {
            return true;
        }
        java.net.InetAddress[] resolved;
        try {
            resolved = java.net.InetAddress.getAllByName(host);
        } catch (java.net.UnknownHostException e) {
            return false;
        }
        for (java.net.InetAddress address : resolved) {
            if (address.isLoopbackAddress() || address.isSiteLocalAddress()
                    || address.isLinkLocalAddress() || address.isAnyLocalAddress()
                    || address.isMulticastAddress()
                    || isUniqueLocalOrCgn(address)) {
                return false;
            }
        }
        return true;
    }

    /** IPv6 unique-local (fc00::/7) and IPv4 CGN (100.64/10): private ranges
     * {@code InetAddress}'s own predicates do not classify. */
    private static boolean isUniqueLocalOrCgn(java.net.InetAddress address) {
        byte[] raw = address.getAddress();
        if (raw.length == 16) {
            return (raw[0] & 0xFE) == 0xFC;
        }
        return raw.length == 4 && (raw[0] & 0xFF) == 100 && (raw[1] & 0xC0) == 0x40;
    }

    private void respondTask(HttpExchange exchange, JsonNode id,
                             Optional<A2aMessages.Task> task) throws IOException {
        if (task.isPresent()) {
            respond(exchange, 200, rpcResult(id, task.get()));
        } else {
            respond(exchange, 200, rpcError(id, -32001, "task not found"));
        }
    }

    // ------------------------------------------------------------- SSE streaming

    /** How often the stream re-reads task state from the space. */
    private static final Duration STREAM_POLL = Duration.ofMillis(100);
    /** The longest a single stream stays open before the client must resubscribe. */
    private static final Duration STREAM_CAP = Duration.ofMinutes(5);

    private static boolean terminal(String state) {
        return A2aMessages.States.COMPLETED.equals(state)
                || A2aMessages.States.CANCELED.equals(state)
                || A2aMessages.States.FAILED.equals(state);
    }

    /**
     * Streams a task's life over Server-Sent Events: the initial task
     * snapshot, a status-update event per state change read out of the space,
     * an artifact-update carrying the result, and {@code final: true} on the
     * terminal event. State comes from polling the space, which keeps the
     * stream honest through worker crashes: a lapsed take lease streams a
     * {@code working} to {@code submitted} transition like any other.
     */
    private void streamTask(HttpExchange exchange, JsonNode id,
                            A2aTaskBinding taskBinding, String taskId) throws IOException {
        if (!streamSlots.tryAcquire()) {
            respond(exchange, 200, rpcError(id, -32000,
                    "too many concurrent streams; retry later"));
            return;
        }
        try {
            streamTaskHolding(exchange, id, taskBinding, taskId);
        } finally {
            streamSlots.release();
        }
    }

    private void streamTaskHolding(HttpExchange exchange, JsonNode id,
                            A2aTaskBinding taskBinding, String taskId) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream out = exchange.getResponseBody()) {
            A2aMessages.Task task = taskBinding.get(taskId).orElseThrow();
            event(out, rpcResult(id, task));
            String lastState = task.status().state();
            if (terminal(lastState)) {
                event(out, rpcResult(id, statusEvent(task, true)));
                return;
            }
            long deadline = System.nanoTime() + STREAM_CAP.toNanos();
            while (System.nanoTime() < deadline) {
                try {
                    Thread.sleep(STREAM_POLL.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                task = taskBinding.get(taskId).orElse(null);
                if (task == null) {
                    return;
                }
                if (task.status().state().equals(lastState)) {
                    continue;
                }
                lastState = task.status().state();
                boolean isFinal = terminal(lastState);
                if (isFinal && !task.artifacts().isEmpty()) {
                    event(out, rpcResult(id, artifactEvent(task)));
                }
                event(out, rpcResult(id, statusEvent(task, isFinal)));
                if (isFinal) {
                    return;
                }
            }
        } catch (IOException e) {
            // The client went away; the task carries on in the space.
        }
    }

    /**
     * Watches one task and POSTs its updates to the registered webhook (A2A
     * push notifications): the current task document on every state change,
     * with the registration's token echoed in
     * {@code X-A2A-Notification-Token}. The watcher stops on the terminal
     * update, when the registration is replaced, or at the watch cap.
     */
    private void watchAndPush(A2aTaskBinding taskBinding, String taskId,
                              PushConfig config) {
        if (!watcherSlots.tryAcquire()) {
            return; // registration stands; no watcher slot, no push (ASF-040)
        }
        Thread.ofVirtual().name("a2a-push-" + taskId).start(() -> {
            try {
                watchLoop(taskBinding, taskId, config);
            } finally {
                watcherSlots.release();
            }
        });
    }

    private void watchLoop(A2aTaskBinding taskBinding, String taskId,
                           PushConfig config) {
        String lastState = "";
        long deadline = System.nanoTime() + STREAM_CAP.toNanos();
        while (System.nanoTime() < deadline
                && pushConfigs.get(taskId) == config) {
            A2aMessages.Task task = taskBinding.get(taskId).orElse(null);
            if (task == null) {
                return;
            }
            if (!task.status().state().equals(lastState)) {
                lastState = task.status().state();
                postUpdate(config, task);
                if (terminal(lastState)) {
                    return;
                }
            }
            try {
                Thread.sleep(STREAM_POLL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void postUpdate(PushConfig config, A2aMessages.Task task) {
        try {
            java.net.http.HttpRequest.Builder request = java.net.http.HttpRequest
                    .newBuilder(java.net.URI.create(config.url()))
                    .header("Content-Type", JSON)
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(
                            json.writeValueAsBytes(task)));
            if (config.token() != null && !config.token().isBlank()) {
                request.header("X-A2A-Notification-Token", config.token());
            }
            pushHttp.send(request.build(),
                    java.net.http.HttpResponse.BodyHandlers.discarding());
        } catch (IOException | RuntimeException e) {
            // The receiver is unreachable; the next state change tries again.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ObjectNode statusEvent(A2aMessages.Task task, boolean isFinal) {
        ObjectNode event = json.createObjectNode();
        event.put("kind", "status-update");
        event.put("taskId", task.id());
        event.put("contextId", task.contextId());
        event.set("status", json.valueToTree(task.status()));
        event.put("final", isFinal);
        return event;
    }

    private ObjectNode artifactEvent(A2aMessages.Task task) {
        ObjectNode event = json.createObjectNode();
        event.put("kind", "artifact-update");
        event.put("taskId", task.id());
        event.put("contextId", task.contextId());
        event.set("artifact", json.valueToTree(task.artifacts().get(0)));
        return event;
    }

    private static void event(OutputStream out, byte[] payload) throws IOException {
        out.write("data: ".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        out.write(payload);
        out.write("\n\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        out.flush();
    }

    private static Optional<String> firstTextPart(JsonNode params) {
        for (JsonNode part : params.path("message").path("parts")) {
            if ("text".equals(part.path("kind").asText())
                    && part.path("text").isTextual()) {
                String text = part.path("text").asText();
                if (text.length() > MAX_TEXT_LENGTH) {
                    return Optional.empty(); // oversized text is refused (ASF-007)
                }
                return Optional.of(text);
            }
        }
        return Optional.empty();
    }

    private byte[] rpcResult(JsonNode id, Object result) throws IOException {
        ObjectNode response = json.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", id);
        response.set("result", json.valueToTree(result));
        return json.writeValueAsBytes(response);
    }

    private byte[] rpcError(JsonNode id, int code, String message) throws IOException {
        ObjectNode response = json.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", id);
        ObjectNode error = response.putObject("error");
        error.put("code", code);
        error.put("message", message);
        return json.writeValueAsBytes(response);
    }

    private static void respond(HttpExchange exchange, int status, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", JSON);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
        // The response body length was exact; the exchange closes with the stream.
    }
}
