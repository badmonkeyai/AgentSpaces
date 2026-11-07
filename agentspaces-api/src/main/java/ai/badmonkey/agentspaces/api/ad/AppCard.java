/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * An application advertised in a fleet (SPEC §6.1a.2, v0.1.25; ISSUE-AppCard): the tenth advertisement kind, beside
 * the AgentCard, the AssetCard, and the ModelCard. The card asserts what its issuer signs; what the app may read and
 * request is enforced by the party that admits it.
 *
 * @param id        the advertisement id
 * @param issuer    the issuing peer
 * @param group     the group
 * @param issued    when it was issued
 * @param ttl       how long it lives
 * @param name      the app's name
 * @param kind      {@code peer}, {@code fed}, {@code gateway}, or {@code realm}
 * @param shape     {@code web}, {@code desktop}, {@code static}, or {@code a2ui}
 * @param owner     who owns the app
 * @param reads     the {@code group/space} pairs it reads
 * @param actions   the action names it may request
 * @param idpClient its identity-provider client, empty when none
 * @param url       where it is reached, empty when none
 * @param pkg       the package it is deployed from, empty when none
 * @param version   its version
 * @param views     the view names it asks of its feed
 * @param ui        the surfaces it draws (A2UI), appended last and omitted when absent
 */
public record AppCard(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        String name,
        String kind,
        String shape,
        String owner,
        List<String> reads,
        List<String> actions,
        String idpClient,
        String url,
        String pkg,
        String version,
        List<String> views,
        @JsonInclude(JsonInclude.Include.NON_NULL) Ui ui,
        @JsonInclude(JsonInclude.Include.NON_NULL) Host host) implements Advertisement {

    public static final List<String> KINDS = List.of("peer", "fed", "gateway", "realm", "assembled");   // assembled: v0.1.26, drafted by a console from a group's ui declarations
    public static final List<String> SHAPES = List.of("web", "desktop", "static", "a2ui");
    public static final List<String> TRANSPORTS = List.of("views", "a2a");
    public static final int MAX_TERMS = 64;

    /**
     * The surfaces an app draws (A2UI): the protocol and its version, the catalog ids the surfaces use, the surface
     * agent's instance, the transport ({@code views} through the feed, {@code a2a} through the gateway), the surface ids.
     */
    /**
     * v0.1.28 (spiral 5, S5-P-01): where the application is served for its end users, appended last and omitted when
     * absent, so a card without it is byte-identical to v0.1.27. An agent publishes its own interface; the host that
     * serves it names the address; a console lists and links, it does not draw.
     *
     * @param url         the address end users open
     * @param port        the port the host bound, or 0 when the url says it all
     * @param hostAgent   the peer id of the app host serving it
     * @param path        the path under the host, "/" by default
     * @param signOn      {@code tenant} (the tenant's identity provider at the host's door) or {@code none}
     * @param publishedBy the agent that published it
     * @param publishedAt when
     * @param status      {@code serving} or {@code stopped}
     */
    public record Host(String url, int port, String hostAgent, String path, String signOn, String publishedBy, Instant publishedAt, String status) {
        public static final List<String> SIGN_ONS = List.of("tenant", "none");
        public static final List<String> STATUSES = List.of("serving", "stopped");

        public Host {
            Objects.requireNonNull(url, "host.url");
            if (url.isBlank()) {
                throw new IllegalArgumentException("a host names the url end users open");
            }
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("host.port is a port number or 0: " + port);
            }
            hostAgent = hostAgent == null ? "" : hostAgent;
            path = path == null || path.isBlank() ? "/" : path;
            signOn = signOn == null || signOn.isBlank() ? "tenant" : signOn;
            if (!SIGN_ONS.contains(signOn)) {
                throw new IllegalArgumentException("host.signOn must be one of " + SIGN_ONS + ": " + signOn);
            }
            publishedBy = publishedBy == null ? "" : publishedBy;
            Objects.requireNonNull(publishedAt, "host.publishedAt");
            status = status == null || status.isBlank() ? "serving" : status;
            if (!STATUSES.contains(status)) {
                throw new IllegalArgumentException("host.status must be one of " + STATUSES + ": " + status);
            }
        }
    }

    public record Ui(String protocol, String version, List<String> catalogIds, String agent, String transport, List<String> surfaces,
                     @JsonInclude(JsonInclude.Include.NON_EMPTY) List<UiDeclaration.Component> components) {

        /** The v0.1.25 shape: no components. */
        public Ui(String protocol, String version, List<String> catalogIds, String agent, String transport, List<String> surfaces) {
            this(protocol, version, catalogIds, agent, transport, surfaces, List.of());
        }

        public Ui {
            components = components == null ? List.of() : List.copyOf(components);
            if (components.size() > UiDeclaration.MAX_COMPONENTS) {
                throw new IllegalArgumentException("an AppCard's ui carries at most " + UiDeclaration.MAX_COMPONENTS + " components");
            }
            protocol = protocol == null || protocol.isBlank() ? "a2ui" : protocol;
            version = version == null || version.isBlank() ? "v0.9.1" : version;
            catalogIds = catalogIds == null ? List.of() : List.copyOf(catalogIds);
            agent = agent == null ? "" : agent;
            transport = transport == null || transport.isBlank() ? "views" : transport;
            surfaces = surfaces == null ? List.of() : List.copyOf(surfaces);
            if (!TRANSPORTS.contains(transport)) {
                throw new IllegalArgumentException("ui.transport must be one of " + TRANSPORTS + ": " + transport);
            }
            if (catalogIds.size() > MAX_TERMS || surfaces.size() > MAX_TERMS) {
                throw new IllegalArgumentException("an AppCard's ui carries at most " + MAX_TERMS + " catalogs and surfaces");
            }
        }
    }

    public AppCard {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("an AppCard needs a name");
        }
        kind = kind == null || kind.isBlank() ? "fed" : kind;
        shape = shape == null || shape.isBlank() ? "web" : shape;
        if (!KINDS.contains(kind)) {
            throw new IllegalArgumentException("kind must be one of " + KINDS + ": " + kind);
        }
        if (!SHAPES.contains(shape)) {
            throw new IllegalArgumentException("shape must be one of " + SHAPES + ": " + shape);
        }
        owner = owner == null ? "" : owner;
        reads = reads == null ? List.of() : List.copyOf(reads);
        actions = actions == null ? List.of() : List.copyOf(actions);
        idpClient = idpClient == null ? "" : idpClient;
        url = url == null ? "" : url;
        pkg = pkg == null ? "" : pkg;
        version = version == null || version.isBlank() ? "1" : version;
        views = views == null ? List.of() : List.copyOf(views);
        if (reads.size() > MAX_TERMS || actions.size() > MAX_TERMS || views.size() > MAX_TERMS) {
            throw new IllegalArgumentException("an AppCard carries at most " + MAX_TERMS + " reads, actions, and views");
        }
        if ("a2ui".equals(shape) && ui == null) {
            throw new IllegalArgumentException("an a2ui app names its surfaces in ui");
        }
    }

    /** The v0.1.27 shape: no host. */
    public AppCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl, String name, String kind, String shape, String owner,
                   List<String> reads, List<String> actions, String idpClient, String url, String pkg, String version, List<String> views, Ui ui) {
        this(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, null);
    }

    /** Whether the app draws A2UI surfaces (its own shape, or surfaces embedded in another shape). */
    public boolean drawsSurfaces() {
        return ui != null;
    }

    /** True when an app host serves the application for end users at {@link Host#url()} (named without {@code is}: a bean-style helper would leak onto the wire). */
    public boolean servesEndUsers() {
        return host != null && "serving".equals(host.status());
    }

    public AppCard withUi(Ui newUi) {
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, newUi, host);
    }

    public AppCard withHost(Host newHost) {
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, newHost);
    }
}
