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

import ai.badmonkey.agentspaces.api.security.BearerAuthenticator;
import ai.badmonkey.agentspaces.api.security.BearerAuthenticator.Principal;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * Serves a {@link ConsoleView} as the fleet console. Three surfaces, each an
 * open standard someone can build on without touching this class:
 *
 * <ul>
 *   <li>{@code /api/v1} and below: JSON documents carrying {@code _links} in
 *       the HAL convention ({@code application/hal+json}), so hypermedia
 *       clients, Spring HATEOAS included, discover the whole surface from the
 *       root. Responses allow cross-origin reads, so an external dashboard
 *       consumes the API directly.</li>
 *   <li>{@code /api/v1/events}: the activity stream over Server-Sent Events,
 *       resumable via the standard {@code Last-Event-ID} header (or an
 *       {@code after} query parameter for plain HTTP clients).</li>
 *   <li>{@code /}: the single-file UI, no build step, themed by CSS custom
 *       properties, populated only through the public API.</li>
 * </ul>
 *
 * <p>Registered {@link ConsolePanel}s appear at {@code /api/v1/panels} and as
 * sections on the UI. The server runs on the JDK's built-in HTTP server with
 * virtual threads, the same footing as the A2A gateway, so the console core
 * adds no web framework to a fleet; the Spring Boot starter wires one of
 * these from properties.
 */
public final class FleetConsoleServer implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(FleetConsoleServer.class.getName());

    /**
     * The OAuth2 scope a scoped principal (an identity-provider JWT) must carry
     * to command the fleet; the default for {@link Builder#requiredScope(String)}.
     */
    public static final String OPERATE_SCOPE = "aspace:console:operate";

    private static final String HAL_JSON = "application/hal+json; charset=utf-8";
    private static final String HTML = "text/html; charset=utf-8";
    private static final String UI_RESOURCE =
            "/ai/badmonkey/agentspaces/console/console.html";
    /** How often the event stream checks the ring for news. */
    private static final Duration STREAM_POLL = Duration.ofMillis(200);
    /** The longest a stream stays open before the client reconnects. */
    private static final Duration STREAM_CAP = Duration.ofMinutes(10);

    private final ConsoleView view;
    private final String fleetName;
    private final Map<String, ConsolePanel> panels;
    private final FleetCommander commander;
    private final BearerAuthenticator authenticator;
    private final String requiredScope;
    private final java.util.Set<String> allowedOrigins;
    private final ObjectMapper json = new ObjectMapper();
    private HttpServer server;

    private FleetConsoleServer(Builder builder) {
        this.view = builder.view;
        this.fleetName = builder.fleetName;
        // Registration order is the display order, so keep it.
        this.panels = java.util.Collections.unmodifiableMap(
                new LinkedHashMap<>(builder.panels));
        this.commander = builder.commander;
        this.authenticator = builder.authenticator;
        this.requiredScope = builder.requiredScope;
        this.allowedOrigins = java.util.Set.copyOf(builder.allowedOrigins);
    }

    /** Whether command-and-control is enabled (a commander and an authenticator are set). */
    private boolean c2Enabled() {
        return commander != null && authenticator != null;
    }

    /**
     * Starts a builder.
     *
     * @param view the model to serve
     * @return the builder
     */
    public static Builder builder(ConsoleView view) {
        return new Builder(view);
    }

    /**
     * Starts serving on the loopback interface — the secure default (ASF-028).
     * Binding a routable interface is an explicit choice via
     * {@link #start(InetSocketAddress)}; the operator token then travels plain
     * HTTP, so front a routable console with a TLS-terminating proxy.
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
        server.createContext("/", this::route);
        server.start();
        return server.getAddress().getPort();
    }

    /** The bound port. */
    public synchronized int port() {
        if (server == null) {
            throw new IllegalStateException("not started");
        }
        return server.getAddress().getPort();
    }

    /** Stops serving. */
    @Override
    public synchronized void close() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    // ----------------------------------------------------------------- routing

    private void route(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if ("POST".equals(method)) {
                handleCommand(exchange, path); // C2: the only mutating surface
                return;
            }
            if (!"GET".equals(method)) {
                respond(exchange, 405, HAL_JSON, jsonBytes(Map.of("error", "GET or POST")));
                return;
            }
            switch (path) {
                case "/", "/index.html" -> ui(exchange);
                case "/api/v1", "/api/v1/" -> halRoot(exchange);
                case "/api/v1/overview" -> overview(exchange);
                case "/api/v1/spaces" -> spaces(exchange);
                case "/api/v1/peers" -> peers(exchange);
                case "/api/v1/ads" -> ads(exchange);
                case "/api/v1/panels" -> panelIndex(exchange);
                case "/api/v1/commands" -> commandCatalog(exchange);
                case "/api/v1/events" -> events(exchange);
                default -> subResource(exchange, path);
            }
        } catch (RuntimeException e) {
            // Internal detail stays server-side (ASF-036).
            LOG.log(System.Logger.Level.WARNING,
                    "console request failed: " + exchange.getRequestURI().getPath(), e);
            respond(exchange, 500, HAL_JSON,
                    jsonBytes(Map.of("error", "internal error")));
        }
    }

    private void subResource(HttpExchange exchange, String path) throws IOException {
        if (path.startsWith("/api/v1/spaces/")) {
            String name = path.substring("/api/v1/spaces/".length());
            for (ConsoleView.SpaceStats stats : view.spaces()) {
                if (stats.name().equals(name)) {
                    Map<String, Object> doc = spaceDoc(stats);
                    doc.put("_links", links(Map.of(
                            "self", "/api/v1/spaces/" + name,
                            "spaces", "/api/v1/spaces")));
                    respond(exchange, 200, HAL_JSON, jsonBytes(doc));
                    return;
                }
            }
            respond(exchange, 404, HAL_JSON,
                    jsonBytes(Map.of("error", "no such space: " + name)));
            return;
        }
        if (path.startsWith("/api/v1/panels/")) {
            String rest = path.substring("/api/v1/panels/".length());
            int slash = rest.indexOf('/');
            String id = slash < 0 ? rest : rest.substring(0, slash);
            String facet = slash < 0 ? "data" : rest.substring(slash + 1);
            ConsolePanel panel = panels.get(id);
            if (panel == null) {
                respond(exchange, 404, HAL_JSON,
                        jsonBytes(Map.of("error", "no such panel: " + id)));
                return;
            }
            switch (facet) {
                case "data" -> respond(exchange, 200, HAL_JSON,
                        json.writeValueAsBytes(panel.data()));
                case "fragment" -> respond(exchange, 200, HTML,
                        panel.htmlFragment().getBytes(StandardCharsets.UTF_8));
                default -> respond(exchange, 404, HAL_JSON,
                        jsonBytes(Map.of("error", "no such facet: " + facet)));
            }
            return;
        }
        respond(exchange, 404, HAL_JSON, jsonBytes(Map.of("error", "not found")));
    }

    // ------------------------------------------------------- command and control

    /**
     * The command catalog: the directive verbs and every registered
     * {@link ConsoleCommand} with its form fields, plus whether C2 is enabled.
     * Readable without a token (it is only the form schema); executing a
     * command needs an authenticated bearer.
     */
    private void commandCatalog(HttpExchange exchange) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("enabled", c2Enabled());
        doc.put("directive", Map.of("actions",
                List.of("PAUSE", "RESUME", "DRAIN"), "target", "worker name or *"));
        List<Object> commandDocs = new ArrayList<>();
        if (c2Enabled()) {
            for (ConsoleCommand command : commander.commands()) {
                Map<String, Object> cmd = new LinkedHashMap<>();
                cmd.put("id", command.id());
                cmd.put("title", command.title());
                cmd.put("description", command.description());
                cmd.put("fields", command.fields().stream().map(field -> {
                    Map<String, Object> f = new LinkedHashMap<>();
                    f.put("name", field.name());
                    f.put("label", field.label());
                    f.put("kind", field.kind().name().toLowerCase(java.util.Locale.ROOT));
                    f.put("options", field.options());
                    f.put("required", field.required());
                    return f;
                }).toList());
                commandDocs.add(cmd);
            }
        }
        doc.put("commands", commandDocs);
        doc.put("_links", links(Map.of("self", "/api/v1/commands", "root", "/api/v1")));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    /**
     * The one mutating route. The bearer ({@code Authorization: Bearer} or
     * {@code X-Console-Token}) is authenticated by the configured
     * {@link BearerAuthenticator}: no principal is 401 with
     * {@code WWW-Authenticate: Bearer}; a principal that carries scopes but
     * not the {@linkplain Builder#requiredScope(String) required one} is 403
     * (a static-token principal carries no scopes and is not scope-checked).
     * The audited operator is {@code principal.subject()}: the token's
     * {@code sub} for a JWT, the fixed holder name for a static token. The
     * request body's {@code operator} field is ignored, so a self-declared
     * name can no longer appear in the audit trail; clients that still send it
     * are unaffected.
     */
    private void handleCommand(HttpExchange exchange, String path) throws IOException {
        if (!path.startsWith("/api/v1/commands/")) {
            // POST is only valid on the command routes; every other resource is read-only.
            respond(exchange, 405, HAL_JSON,
                    jsonBytes(Map.of("error", "POST only on /api/v1/commands/*")));
            return;
        }
        if (!c2Enabled()) {
            respond(exchange, 404, HAL_JSON,
                    jsonBytes(Map.of("error", "command-and-control is not enabled")));
            return;
        }
        Optional<Principal> principal = authenticate(exchange);
        if (principal.isEmpty()) {
            exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
            respond(exchange, 401, HAL_JSON,
                    jsonBytes(Map.of("error", "a valid operator token is required")));
            return;
        }
        if (!principal.get().permitsScope(requiredScope)) {
            respond(exchange, 403, HAL_JSON, jsonBytes(Map.of(
                    "error", "the scope '" + requiredScope + "' is required to command")));
            return;
        }
        String operator = principal.get().subject(); // the audited identity, never the body's
        Map<String, Object> body = readJsonBody(exchange);
        String rest = path.substring("/api/v1/commands/".length());
        ConsoleCommand.Outcome outcome;
        switch (rest) {
            case "directive" -> outcome = commander.broadcast(
                    asString(body.get("action"), ""), asString(body.get("target"), ""),
                    operator);
            case "cancel" -> outcome = commander.cancelDispatched(
                    asString(body.get("entryId"), ""), operator);
            default -> outcome = commander.execute(rest, argsOf(body.get("args")), operator);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("ok", outcome.ok());
        doc.put("message", outcome.message());
        if (outcome.entryId() != null) {
            doc.put("entryId", outcome.entryId());
        }
        doc.put("operator", operator);
        respond(exchange, outcome.ok() ? 200 : 400, HAL_JSON, jsonBytes(doc));
    }

    /** Authenticates the presented bearer (or {@code X-Console-Token}) against the configured gate. */
    private Optional<Principal> authenticate(HttpExchange exchange) {
        String presented = exchange.getRequestHeaders().getFirst("X-Console-Token");
        if (presented == null) {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (auth != null && auth.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = auth.substring(7).trim();
            }
        }
        if (presented == null || presented.isEmpty()) {
            return Optional.empty();
        }
        return authenticator.authenticate(presented);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readJsonBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            byte[] bytes = in.readAllBytes();
            if (bytes.length == 0) {
                return Map.of();
            }
            Object parsed = json.readValue(bytes, Object.class);
            return parsed instanceof Map ? (Map<String, Object>) parsed : Map.of();
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> argsOf(Object raw) {
        Map<String, String> args = new LinkedHashMap<>();
        if (raw instanceof Map<?, ?> map) {
            map.forEach((k, v) -> args.put(String.valueOf(k),
                    v == null ? "" : String.valueOf(v)));
        }
        return args;
    }

    private static String asString(Object value, String fallback) {
        if (value == null || String.valueOf(value).isBlank()) {
            return fallback;
        }
        return String.valueOf(value);
    }

    // --------------------------------------------------------------- resources

    private void ui(HttpExchange exchange) throws IOException {
        String page = uiTemplate().replace("%%FLEET%%", escapeHtml(fleetName));
        respond(exchange, 200, HTML, page.getBytes(StandardCharsets.UTF_8));
    }

    private void halRoot(HttpExchange exchange) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("fleet", fleetName);
        doc.put("commandAndControl", c2Enabled());
        Map<String, String> hrefs = new LinkedHashMap<>(Map.of(
                "self", "/api/v1",
                "overview", "/api/v1/overview",
                "spaces", "/api/v1/spaces",
                "peers", "/api/v1/peers",
                "ads", "/api/v1/ads",
                "panels", "/api/v1/panels",
                "events", "/api/v1/events",
                "ui", "/"));
        if (c2Enabled()) {
            hrefs.put("commands", "/api/v1/commands");
        }
        doc.put("_links", links(hrefs));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    private void overview(HttpExchange exchange) throws IOException {
        List<ConsoleView.SpaceStats> spaces = view.spaces();
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("fleet", fleetName);
        doc.put("members", view.members().size());
        doc.put("agents", view.ads().stream()
                .filter(ad -> "agent".equals(ad.kind())).count());
        doc.put("queued", spaces.stream().mapToInt(ConsoleView.SpaceStats::queued).sum());
        doc.put("inProgress", spaces.stream()
                .mapToInt(ConsoleView.SpaceStats::inProgress).sum());
        doc.put("completed", spaces.stream()
                .mapToInt(ConsoleView.SpaceStats::completed).sum());
        doc.put("written", spaces.stream()
                .mapToInt(ConsoleView.SpaceStats::written).sum());
        doc.put("perWorker", view.perWorker());
        doc.put("spaces", spaces.stream().map(this::spaceDoc).toList());
        doc.put("lastEventSeq", view.lastSeq());
        doc.put("_links", links(Map.of(
                "self", "/api/v1/overview",
                "root", "/api/v1")));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    private void spaces(HttpExchange exchange) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("spaces", view.spaces().stream().map(stats -> {
            Map<String, Object> space = spaceDoc(stats);
            space.put("_links", links(Map.of(
                    "self", "/api/v1/spaces/" + stats.name())));
            return space;
        }).toList());
        doc.put("_links", links(Map.of("self", "/api/v1/spaces", "root", "/api/v1")));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    private Map<String, Object> spaceDoc(ConsoleView.SpaceStats stats) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", stats.name());
        doc.put("written", stats.written());
        doc.put("queued", stats.queued());
        doc.put("inProgress", stats.inProgress());
        doc.put("completed", stats.completed());
        doc.put("perWorker", stats.perWorker());
        return doc;
    }

    private void peers(HttpExchange exchange) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("peers", view.members().stream().map(member -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("peer", member.peer());
            row.put("roles", member.roles());
            row.put("suspect", member.suspect());
            row.put("endpoints", member.endpoints());
            row.put("channel", member.channel());
            return row;
        }).toList());
        doc.put("_links", links(Map.of("self", "/api/v1/peers", "root", "/api/v1")));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    private void ads(HttpExchange exchange) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("ads", view.ads().stream().map(ad -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("kind", ad.kind());
            row.put("title", ad.title());
            row.put("issuer", ad.issuer());
            row.put("detail", ad.detail());
            return row;
        }).toList());
        doc.put("_links", links(Map.of("self", "/api/v1/ads", "root", "/api/v1")));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    private void panelIndex(HttpExchange exchange) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("panels", panels.values().stream().map(panel -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", panel.id());
            row.put("title", panel.title());
            row.put("hasFragment", !panel.htmlFragment().isEmpty());
            row.put("_links", links(Map.of(
                    "data", "/api/v1/panels/" + panel.id() + "/data",
                    "fragment", "/api/v1/panels/" + panel.id() + "/fragment")));
            return row;
        }).toList());
        doc.put("_links", links(Map.of("self", "/api/v1/panels", "root", "/api/v1")));
        respond(exchange, 200, HAL_JSON, jsonBytes(doc));
    }

    // -------------------------------------------------------------- SSE stream

    /** Concurrent SSE streams served at once (ASF-040). */
    private static final int MAX_CONCURRENT_STREAMS = 32;
    private final java.util.concurrent.Semaphore streamSlots =
            new java.util.concurrent.Semaphore(MAX_CONCURRENT_STREAMS);

    private void events(HttpExchange exchange) throws IOException {
        if (!streamSlots.tryAcquire()) {
            respond(exchange, 503, HAL_JSON,
                    jsonBytes(Map.of("error", "too many concurrent event streams")));
            return;
        }
        try {
            eventsHolding(exchange);
        } finally {
            streamSlots.release();
        }
    }

    private void eventsHolding(HttpExchange exchange) throws IOException {
        long after = resumePoint(exchange);
        exchange.getResponseHeaders().set("Content-Type",
                "text/event-stream; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        cors(exchange);
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream out = exchange.getResponseBody()) {
            long deadline = System.nanoTime() + STREAM_CAP.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsoleEvent event : view.eventsAfter(after)) {
                    after = event.seq();
                    out.write(sse(event).getBytes(StandardCharsets.UTF_8));
                }
                out.flush();
                try {
                    Thread.sleep(STREAM_POLL.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } catch (IOException e) {
            // The client went away; the fleet carries on.
        }
    }

    private long resumePoint(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Last-Event-ID");
        if (header == null) {
            String query = exchange.getRequestURI().getQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    if (param.startsWith("after=")) {
                        header = param.substring("after=".length());
                    }
                }
            }
        }
        try {
            return header == null ? 0 : Long.parseLong(header.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String sse(ConsoleEvent event) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("seq", event.seq());
        doc.put("at", event.at().toString());
        doc.put("space", event.space());
        doc.put("kind", event.kind());
        doc.put("entryType", event.entryType());
        doc.put("worker", event.worker());
        return "id: " + event.seq() + "\nevent: activity\ndata: "
                + json.writeValueAsString(doc) + "\n\n";
    }

    // ---------------------------------------------------------------- plumbing

    private static Map<String, Object> links(Map<String, String> hrefs) {
        Map<String, Object> doc = new LinkedHashMap<>();
        hrefs.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> doc.put(entry.getKey(),
                        Map.of("href", entry.getValue())));
        return doc;
    }

    private byte[] jsonBytes(Map<String, Object> doc) throws IOException {
        return json.writeValueAsBytes(doc);
    }

    private void respond(HttpExchange exchange, int status, String contentType,
                         byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        cors(exchange);
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /**
     * Cross-origin access is an allowlist, not a wildcard (ASF-028): the
     * request's Origin is echoed only when the operator granted it via
     * {@link Builder#allowOrigin(String)}. The built-in UI is same-origin and
     * needs no grant.
     */
    private void cors(HttpExchange exchange) {
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin != null && allowedOrigins.contains(origin)) {
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", origin);
            exchange.getResponseHeaders().set("Vary", "Origin");
        }
    }

    private static String uiTemplate() {
        try (InputStream in = FleetConsoleServer.class.getResourceAsStream(UI_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("console UI resource missing: " + UI_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String escapeHtml(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    /** Assembles a {@link FleetConsoleServer}. */
    public static final class Builder {

        private final ConsoleView view;
        private final Map<String, ConsolePanel> panels = new LinkedHashMap<>();
        private final java.util.Set<String> allowedOrigins = new java.util.LinkedHashSet<>();
        private String fleetName = "AgentSpaces";
        private FleetCommander commander;
        private BearerAuthenticator authenticator;
        private String requiredScope = OPERATE_SCOPE;

        private Builder(ConsoleView view) {
            this.view = Objects.requireNonNull(view, "view");
        }

        /**
         * Grants a browser origin cross-origin access to the API (ASF-028).
         * Without any grant the console sends no CORS headers at all, so only
         * same-origin pages (the built-in UI) can read it from a browser.
         *
         * @param origin an exact origin, e.g. {@code https://ops.example.com}
         * @return this builder
         */
        public Builder allowOrigin(String origin) {
            allowedOrigins.add(Objects.requireNonNull(origin, "origin"));
            return this;
        }

        /**
         * Enables command-and-control behind one static operator token: the
         * shared-secret form of {@link #commandAndControl(FleetCommander,
         * BearerAuthenticator)}, equivalent to
         * {@code BearerAuthenticator.staticToken(operatorToken, "operator")}, so
         * every holder is audited as {@code operator}. Both a commander and a
         * non-blank token are required to turn C2 on; without them the console
         * stays read-only and POST requests are refused.
         *
         * @param commander     the C2 executor
         * @param operatorToken the bearer token operators present to command
         * @return this builder
         */
        public Builder commandAndControl(FleetCommander commander, String operatorToken) {
            Objects.requireNonNull(commander, "commander");
            Objects.requireNonNull(operatorToken, "operatorToken");
            if (operatorToken.isBlank()) {
                this.commander = commander;
                this.authenticator = null; // a blank token leaves C2 off, as before
                return this;
            }
            return commandAndControl(commander,
                    BearerAuthenticator.staticToken(operatorToken, "operator"));
        }

        /**
         * Enables command-and-control behind a {@link BearerAuthenticator}: the
         * console serves bearer-gated command routes, and the UI shows a
         * command console. With the OIDC module's {@code BearerJwtValidator}
         * the console is an OAuth2 resource server: each request's JWT is
         * validated, its {@code sub} is the audited operator, and its scopes
         * must include the {@linkplain #requiredScope(String) required scope}.
         *
         * @param commander     the C2 executor
         * @param authenticator the bearer gate on the command routes
         * @return this builder
         */
        public Builder commandAndControl(FleetCommander commander,
                                         BearerAuthenticator authenticator) {
            this.commander = Objects.requireNonNull(commander, "commander");
            this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
            return this;
        }

        /**
         * The scope a scoped principal must carry to command; default
         * {@link #OPERATE_SCOPE}. A principal without any scopes (a static
         * token) is never scope-checked.
         *
         * @param scope the required OAuth2 scope
         * @return this builder
         */
        public Builder requiredScope(String scope) {
            Objects.requireNonNull(scope, "scope");
            if (scope.isBlank()) {
                throw new IllegalArgumentException("requiredScope must not be blank");
            }
            this.requiredScope = scope;
            return this;
        }

        /**
         * Names the fleet on the UI and the API root.
         *
         * @param fleetName the display name
         * @return this builder
         */
        public Builder fleetName(String fleetName) {
            this.fleetName = Objects.requireNonNull(fleetName, "fleetName");
            return this;
        }

        /**
         * Registers a panel.
         *
         * @param panel the panel
         * @return this builder
         */
        public Builder panel(ConsolePanel panel) {
            Objects.requireNonNull(panel, "panel");
            if (panels.putIfAbsent(panel.id(), panel) != null) {
                throw new IllegalArgumentException("duplicate panel id: " + panel.id());
            }
            return this;
        }

        /**
         * Registers several panels.
         *
         * @param panels the panels
         * @return this builder
         */
        public Builder panels(List<? extends ConsolePanel> panels) {
            panels.forEach(this::panel);
            return this;
        }

        /** Builds the server; call {@link #start(int)} to serve. */
        public FleetConsoleServer build() {
            return new FleetConsoleServer(this);
        }
    }
}
