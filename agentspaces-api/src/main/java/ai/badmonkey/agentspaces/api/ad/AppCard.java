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

import ai.badmonkey.agentspaces.common.codec.CanonicalMaps;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * An application advertised in a fleet (SPEC §6.1a.2, v0.1.25; ISSUE-AppCard; v0.1.32, ISSUE-AppCardOriginFormats):
 * the tenth advertisement kind, beside the AgentCard, the AssetCard, and the ModelCard. The card asserts what its
 * issuer signs; what the app may read and request is enforced by the party that admits it.
 *
 * <p>Three fields answer three questions (AppCard origin and formats, 2026-11-09): {@code kind} says how the app
 * connects and is a closed set; {@code origin} names the external system the app came from; {@code formats} lists the
 * media types and vocabulary identifiers the app reads or renders. No kind is tied to any origin. A card whose
 * encoding passes {@link CardSpill#SPILL_THRESHOLD_BYTES} travels as a stub: its {@link #SPILL_SET} fields sit in a
 * content-swarm document that {@code spill} names (SPEC §6.1a.5).
 *
 * @param id        the advertisement id
 * @param issuer    the issuing peer
 * @param group     the group
 * @param issued    when it was issued
 * @param ttl       how long it lives
 * @param name      the app's name
 * @param kind      how the app connects: {@code peer}, {@code fed}, {@code gateway}, or {@code assembled}
 * @param shape     {@code web}, {@code desktop}, {@code static}, or {@code a2ui}
 * @param owner     who owns the app
 * @param reads     the {@code group/space} pairs it reads
 * @param actions   the action names it may request
 * @param idpClient its identity-provider client, empty when none
 * @param url       where it is reached, empty when none
 * @param pkg       the package it is deployed from, empty when none
 * @param version   its version
 * @param views     the view names it asks of its feed
 * @param ui        the surfaces it draws (A2UI), omitted when absent
 * @param host      where a host serves it for end users (v0.1.28), omitted when absent
 * @param origin    the external system it came from (v0.1.32), omitted when absent
 * @param formats   the media types and vocabulary IRIs it reads or renders (v0.1.32), omitted when absent
 * @param spill     the reference to its parts document when the card is a stub (v0.1.32), omitted when absent
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
        @JsonInclude(JsonInclude.Include.NON_NULL) Host host,
        @JsonInclude(JsonInclude.Include.NON_NULL) Origin origin,
        @JsonInclude(JsonInclude.Include.NON_NULL) List<String> formats,
        @JsonInclude(JsonInclude.Include.NON_NULL) CardSpill spill) implements Advertisement {

    /**
     * How an app connects, a closed set (AppCard origin and formats, 2026-11-09): {@code peer}, the app is itself a
     * member; {@code fed}, an agent of the group feeds it its entries and views; {@code gateway}, it sits behind the
     * A2A gateway; {@code assembled}, a static app its publisher assembled from extracted data and serves as is, which
     * reads nothing live, as a published static HTML site does. A reader refuses any other value.
     */
    public static final List<String> KINDS = List.of("peer", "fed", "gateway", "assembled");
    public static final List<String> SHAPES = List.of("web", "desktop", "static", "a2ui");
    public static final List<String> TRANSPORTS = List.of("views", "a2a");
    public static final int MAX_TERMS = 64;
    /** The fields a stub moves into its parts document, in field order (SPEC §6.1a.5). */
    public static final List<String> SPILL_SET = List.of("reads", "actions", "views", "ui");
    /** The most entries {@code formats} holds. */
    public static final int MAX_FORMATS = 16;
    /** The longest {@code formats} entry, in UTF-16 code units. */
    public static final int MAX_FORMAT_LENGTH = 256;

    /** A media type under the restricted names of RFC 6838 §4.2, lower-case, with no parameter. */
    private static final Pattern MEDIA_TYPE = Pattern.compile(
            "[a-z0-9][a-z0-9!#$&^_.+-]{0,126}/[a-z0-9][a-z0-9!#$&^_.+-]{0,126}");
    /** The scheme of a vocabulary IRI (RFC 3987 §2.2). */
    private static final Pattern SCHEME = Pattern.compile("[a-z][a-z0-9+.-]*");

    /**
     * The external system an app came from and its identifier there (SPEC §6.1a.2, v0.1.32; AppCard origin and
     * formats, 2026-11-09). The values are open: {@code agentspaces-spec/REGISTRY.md} lists the registered systems
     * and their {@code ref} grammars, and a reader never refuses a system, key, or value the registry lacks when it
     * meets the grammar below. Lengths count UTF-16 code units.
     *
     * @param system  1 to 64 of {@code [a-z0-9.-]}, starting and ending with a letter or digit: a registered token
     *                (no {@code .}) or a reverse-DNS name its owner controls
     * @param ref     the app's identifier in {@code system}, 1 to 256, no control character
     * @param version 0 to 64, no control character; empty when the system names no version
     * @param attrs   null, or 1 to 8 entries keyed {@code <system>.<[a-z0-9.-]+>} (at most 128) with values of 0 to 256
     */
    public record Origin(String system, String ref, String version,
                         @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, String> attrs) {

        public static final int MAX_SYSTEM = 64;
        public static final int MAX_REF = 256;
        public static final int MAX_VERSION = 64;
        public static final int MAX_ATTRS = 8;
        public static final int MAX_ATTR_KEY = 128;
        public static final int MAX_ATTR_VALUE = 256;

        private static final Pattern SYSTEM = Pattern.compile("[a-z0-9](?:[a-z0-9.-]{0,62}[a-z0-9])?");
        private static final Pattern KEY_TAIL = Pattern.compile("[a-z0-9.-]+");

        public Origin {
            Objects.requireNonNull(system, "origin.system");
            Objects.requireNonNull(ref, "origin.ref");
            Objects.requireNonNull(version, "origin.version");
            if (!SYSTEM.matcher(system).matches()) {
                throw new IllegalArgumentException("origin.system is 1 to " + MAX_SYSTEM
                        + " of [a-z0-9.-], starting and ending with a letter or digit: " + system);
            }
            if (ref.isEmpty() || ref.length() > MAX_REF || hasControl(ref)) {
                throw new IllegalArgumentException("origin.ref is 1 to " + MAX_REF + " code units with no control character");
            }
            if (version.length() > MAX_VERSION || hasControl(version)) {
                throw new IllegalArgumentException("origin.version is 0 to " + MAX_VERSION + " code units with no control character");
            }
            if (attrs != null) {
                if (attrs.isEmpty() || attrs.size() > MAX_ATTRS) {
                    throw new IllegalArgumentException("origin.attrs holds 1 to " + MAX_ATTRS + " entries, or is absent");
                }
                String prefix = system + ".";
                for (Map.Entry<String, String> e : attrs.entrySet()) {
                    String key = e.getKey();
                    if (key == null || key.length() > MAX_ATTR_KEY || !key.startsWith(prefix)
                            || !KEY_TAIL.matcher(key.substring(prefix.length())).matches()) {
                        throw new IllegalArgumentException("an origin.attrs key is " + prefix
                                + " then [a-z0-9.-]+, at most " + MAX_ATTR_KEY + " code units: " + key);
                    }
                    String value = e.getValue();
                    if (value == null || value.length() > MAX_ATTR_VALUE || hasControl(value)) {
                        throw new IllegalArgumentException("the origin.attrs value of " + key + " is 0 to "
                                + MAX_ATTR_VALUE + " code units with no control character");
                    }
                }
                attrs = Collections.unmodifiableMap(CanonicalMaps.sorted(attrs));
            }
        }

        /**
         * The builder: a null version becomes {@code ""} and an empty or null map becomes absent.
         *
         * @param system  the origin system
         * @param ref     the app's identifier there
         * @param version its version there, or null
         * @param attrs   the registry's attributes, or null
         * @return the origin
         */
        public static Origin of(String system, String ref, String version, Map<String, String> attrs) {
            return new Origin(system, ref, version == null ? "" : version, attrs == null || attrs.isEmpty() ? null : attrs);
        }
    }

    /**
     * The fields of a card that its stub moves into the parts document (SPEC §6.1a.5): the full values of
     * {@link #SPILL_SET}, and nothing else, so the document's bytes change only when a part changes.
     *
     * @param reads   the {@code group/space} pairs
     * @param actions the action names
     * @param views   the view names
     * @param ui      the surfaces, omitted when absent
     */
    public record Parts(List<String> reads, List<String> actions, List<String> views,
                        @JsonInclude(JsonInclude.Include.NON_NULL) Ui ui) {

        public Parts {
            reads = reads == null ? List.of() : List.copyOf(reads);
            actions = actions == null ? List.of() : List.copyOf(actions);
            views = views == null ? List.of() : List.copyOf(views);
            if (reads.size() > MAX_TERMS || actions.size() > MAX_TERMS || views.size() > MAX_TERMS) {
                throw new IllegalArgumentException("an AppCard carries at most " + MAX_TERMS + " reads, actions, and views");
            }
        }

        /** The names of the fields whose value is non-empty, in field order: what {@code spill.parts} lists. */
        public List<String> nonEmpty() {
            List<String> out = new ArrayList<>(SPILL_SET.size());
            if (!reads.isEmpty()) {
                out.add("reads");
            }
            if (!actions.isEmpty()) {
                out.add("actions");
            }
            if (!views.isEmpty()) {
                out.add("views");
            }
            if (ui != null) {
                out.add("ui");
            }
            return List.copyOf(out);
        }
    }

    /**
     * v0.1.28 (ISSUE-AppHost): where the application is served for its end users, appended after {@code ui} and
     * omitted when absent, so a card without it is byte-identical to v0.1.27. An agent publishes its own interface;
     * the host that serves it names the address; a console lists and links, it does not draw.
     *
     * @param url         the address end users open
     * @param port        the port the host bound, or 0 when the url says it all
     * @param hostAgent   the peer id of the app host serving it
     * @param path        the path under the host, "/" by default
     * @param signOn      {@code idp} (the identity provider of whoever runs the host, enforced at the host's door; the
     *                    default) or {@code none} (v0.1.32; AppCard origin and formats, 2026-11-09)
     * @param publishedBy the agent that published it
     * @param publishedAt when
     * @param status      {@code serving} or {@code stopped}
     */
    public record Host(String url, int port, String hostAgent, String path, String signOn, String publishedBy, Instant publishedAt, String status) {
        public static final List<String> SIGN_ONS = List.of("idp", "none");
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
            signOn = signOn == null || signOn.isBlank() ? "idp" : signOn;
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

    /**
     * The surfaces an app draws (A2UI): the protocol and its version, the catalog ids the surfaces use, the surface
     * agent's instance, the transport ({@code views} through the feed, {@code a2a} through the gateway), the surface
     * ids, and the components of SPEC §6.1a.3.
     */
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
        if (formats != null) {
            formats = List.copyOf(formats);
            requireFormats(formats);
        }
        if (spill != null) {
            // FR-14 (AppCard origin and formats, 2026-11-09): a stub holds every spill-set field at its placeholder
            spill.requireWithin(SPILL_SET);
            if (!reads.isEmpty() || !actions.isEmpty() || !views.isEmpty() || ui != null) {
                throw new IllegalArgumentException("a stub holds [] in reads, actions, and views and no ui; its parts are in the spill document");
            }
        }
        if ("a2ui".equals(shape) && ui == null && (spill == null || !spill.parts().contains("ui"))) {
            throw new IllegalArgumentException("an a2ui app names its surfaces in ui");
        }
    }

    /** The v0.1.28 to v0.1.31 shape: no origin, formats, or spill. */
    public AppCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl, String name, String kind, String shape, String owner,
                   List<String> reads, List<String> actions, String idpClient, String url, String pkg, String version, List<String> views, Ui ui,
                   Host host) {
        this(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, host,
                null, null, null);
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

    /**
     * Whether the app came from an origin system (FR-8).
     *
     * @param system the origin system
     * @return true when {@code origin.system} equals it
     */
    public boolean fromOrigin(String system) {
        return origin != null && origin.system().equals(system);
    }

    /**
     * Whether the app came from an origin system under a ref (FR-8): a {@code ref} ending in {@code /*} matches every
     * ref that starts with the part before {@code *}.
     *
     * @param system the origin system
     * @param ref    the ref, or a prefix ending in {@code /*}
     * @return true when the origin matches
     */
    public boolean fromOrigin(String system, String ref) {
        if (!fromOrigin(system) || ref == null) {
            return false;
        }
        return ref.endsWith("/*") ? origin.ref().startsWith(ref.substring(0, ref.length() - 1)) : origin.ref().equals(ref);
    }

    /**
     * Whether the app reads or renders a format (FR-8): a media type argument is lower-cased and matches an entry
     * exactly, and {@code type/*} matches every media type of that type; an IRI matches exactly.
     *
     * @param format a media type, {@code type/*}, or a vocabulary IRI
     * @return true when {@code formats} holds it
     */
    public boolean handles(String format) {
        if (formats == null || format == null) {
            return false;
        }
        if (format.indexOf(':') >= 0) {
            return formats.contains(format);
        }
        String wanted = format.toLowerCase(Locale.ROOT);
        if (wanted.endsWith("/*")) {
            String type = wanted.substring(0, wanted.length() - 1);
            return formats.stream().anyMatch(f -> f.indexOf(':') < 0 && f.startsWith(type));
        }
        return formats.contains(wanted);
    }

    /**
     * The full values of the spill set (SPEC §6.1a.5), what a publisher writes as the parts document.
     *
     * @return the parts
     * @throws IllegalStateException on a stub, whose parts are in its document
     */
    public Parts parts() {
        if (spill != null) {
            throw new IllegalStateException("a stub's parts are in its spill document; resolve the card first");
        }
        return new Parts(reads, actions, views, ui);
    }

    /**
     * The stub of this card (SPEC §6.1a.5): every spill-set field at its placeholder and {@code spill} set.
     *
     * @param reference the parts document's reference; its {@code parts} are this card's non-empty spill-set fields
     * @return the stub
     */
    public AppCard toStub(CardSpill reference) {
        Objects.requireNonNull(reference, "reference");
        if (!reference.parts().equals(parts().nonEmpty())) {
            throw new IllegalArgumentException("a stub moves every non-empty part " + parts().nonEmpty() + ": " + reference.parts());
        }
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, List.of(), List.of(), idpClient, url, pkg, version,
                List.of(), null, host, origin, formats, reference);
    }

    /**
     * The full card of this stub (FR-15): the document's values in place and no {@code spill}.
     *
     * @param document the decoded parts document, whose non-empty fields are exactly {@code spill.parts}
     * @return the full card
     */
    public AppCard complete(Parts document) {
        Objects.requireNonNull(document, "document");
        if (spill == null) {
            throw new IllegalStateException("only a stub completes; this card carries its parts inline");
        }
        if (!document.nonEmpty().equals(spill.parts())) {
            throw new IllegalArgumentException("the parts document holds " + document.nonEmpty() + " where the stub names " + spill.parts());
        }
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, document.reads(), document.actions(), idpClient, url,
                pkg, version, document.views(), document.ui(), host, origin, formats, null);
    }

    public AppCard withUi(Ui newUi) {
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, newUi, host,
                origin, formats, spill);
    }

    public AppCard withHost(Host newHost) {
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, newHost,
                origin, formats, spill);
    }

    /** The card with its origin (v0.1.32); null clears it. */
    public AppCard withOrigin(Origin newOrigin) {
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, host,
                newOrigin, formats, spill);
    }

    /**
     * The card with its formats (v0.1.32), normalised: a media type is lower-cased, a repeated entry is dropped after
     * its first occurrence, the issuer's order stays, and an empty or null list clears the field.
     *
     * @param newFormats the formats
     * @return the card
     */
    public AppCard withFormats(List<String> newFormats) {
        List<String> normalised = null;
        if (newFormats != null && !newFormats.isEmpty()) {
            LinkedHashSet<String> distinct = new LinkedHashSet<>();
            for (String f : newFormats) {
                Objects.requireNonNull(f, "a format");
                distinct.add(f.indexOf(':') >= 0 ? f : f.toLowerCase(Locale.ROOT));
            }
            normalised = List.copyOf(distinct);
        }
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, host,
                origin, normalised, spill);
    }

    /** The card with its spill reference (v0.1.32); null clears it. */
    public AppCard withSpill(CardSpill newSpill) {
        return new AppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version, views, ui, host,
                origin, formats, newSpill);
    }

    /** FR-3: 1 to 16 distinct entries, each a lower-case media type without parameter or a vocabulary IRI. */
    private static void requireFormats(List<String> formats) {
        if (formats.isEmpty() || formats.size() > MAX_FORMATS) {
            throw new IllegalArgumentException("formats holds 1 to " + MAX_FORMATS + " entries, or is absent");
        }
        if (new LinkedHashSet<>(formats).size() != formats.size()) {
            throw new IllegalArgumentException("formats repeats an entry: " + formats);
        }
        for (String f : formats) {
            if (f.isEmpty() || f.length() > MAX_FORMAT_LENGTH) {
                throw new IllegalArgumentException("a format is 1 to " + MAX_FORMAT_LENGTH + " code units");
            }
            int colon = f.indexOf(':');
            if (colon < 0) {
                if (!MEDIA_TYPE.matcher(f).matches()) {
                    throw new IllegalArgumentException("a media type in formats is lower-case type/subtype with no parameter: " + f);
                }
            } else if (!SCHEME.matcher(f.substring(0, colon)).matches() || hasWhitespaceOrControl(f)) {
                throw new IllegalArgumentException("a vocabulary IRI in formats has a scheme and no whitespace or control character: " + f);
            }
        }
    }

    /** A control character: U+0000 to U+001F, or U+007F. */
    private static boolean hasControl(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c <= 0x1F || c == 0x7F) {
                return true;
            }
        }
        return false;
    }

    /** A control character, or a code unit of the Unicode White_Space property. */
    private static boolean hasWhitespaceOrControl(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c <= 0x20 || c == 0x7F || c == 0x85 || c == 0xA0 || c == 0x1680 || (c >= 0x2000 && c <= 0x200A)
                    || c == 0x2028 || c == 0x2029 || c == 0x202F || c == 0x205F || c == 0x3000) {
                return true;
            }
        }
        return false;
    }
}
