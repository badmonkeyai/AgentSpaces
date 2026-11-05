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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * What an agent can show (SPEC §6.1a.3, v0.1.26; ISSUE-UiComponents): the A2UI components and controls it serves,
 * the standard entries it reads and writes, and the catalogs the components come from. Appended to an
 * {@link AgentCard} as {@code ui} and omitted when absent, so a card without a declaration is byte-identical to
 * v0.1.25; the same {@link Component} list sits on an {@link AppCard.Ui}. A declaration says what an agent can show;
 * it never carries a surface message. The binders derive it from an annotation; a console assembles an application
 * from a group's declarations.
 *
 * @param protocol   {@code a2ui}
 * @param version    the A2UI version, {@code v0.9.1}
 * @param components the components, each unique by id within the card
 * @param entries    the entry type IRIs the agent reads and writes through its components
 * @param catalogIds the distinct catalog ids of the components, derived when empty
 */
public record UiDeclaration(String protocol, String version, List<Component> components, Entries entries, List<String> catalogIds) {

    public static final String PROTOCOL = "a2ui";
    public static final String VERSION = "v0.9.1";
    public static final int MAX_COMPONENTS = 32;
    public static final int MAX_CONTROLS = 16;
    public static final int MAX_ENTRIES = 64;
    public static final List<String> ROLES = List.of("panel", "control", "row");

    /**
     * One component an agent serves.
     *
     * @param id        unique within the card
     * @param catalogId the A2UI catalog the component comes from
     * @param component the component name in that catalog
     * @param entry     the standard entry type IRI behind it, empty when none
     * @param controls  the action names a person may trigger on it (actions of the card, or surface actions the agent answers)
     * @param role      {@code panel} (a self-standing block), {@code control} (an input), or {@code row} (one line in a list)
     */
    public record Component(String id, String catalogId, String component, String entry, List<String> controls, String role) {
        public Component {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(catalogId, "catalogId");
            Objects.requireNonNull(component, "component");
            if (id.isBlank() || catalogId.isBlank() || component.isBlank()) {
                throw new IllegalArgumentException("a ui component names an id, a catalog, and a component");
            }
            entry = entry == null ? "" : entry;
            controls = controls == null ? List.of() : List.copyOf(controls);
            role = role == null || role.isBlank() ? "panel" : role;
            if (!ROLES.contains(role)) {
                throw new IllegalArgumentException("a ui component's role is one of " + ROLES + ": " + role);
            }
            if (controls.size() > MAX_CONTROLS) {
                throw new IllegalArgumentException("a ui component names at most " + MAX_CONTROLS + " controls");
            }
        }
    }

    /** The standard entry type IRIs the agent reads and writes. */
    public record Entries(List<String> reads, List<String> writes) {
        public Entries {
            reads = reads == null ? List.of() : List.copyOf(reads);
            writes = writes == null ? List.of() : List.copyOf(writes);
            if (reads.size() > MAX_ENTRIES || writes.size() > MAX_ENTRIES) {
                throw new IllegalArgumentException("a ui declaration names at most " + MAX_ENTRIES + " entry types per list");
            }
        }

        public static Entries none() {
            return new Entries(List.of(), List.of());
        }
    }

    public UiDeclaration {
        protocol = protocol == null || protocol.isBlank() ? PROTOCOL : protocol;
        version = version == null || version.isBlank() ? VERSION : version;
        components = components == null ? List.of() : List.copyOf(components);
        entries = entries == null ? Entries.none() : entries;
        if (components.size() > MAX_COMPONENTS) {
            throw new IllegalArgumentException("a ui declaration carries at most " + MAX_COMPONENTS + " components");
        }
        if (components.stream().map(Component::id).distinct().count() != components.size()) {
            throw new IllegalArgumentException("ui component ids must be unique within a card");
        }
        if (catalogIds == null || catalogIds.isEmpty()) {
            Set<String> derived = new LinkedHashSet<>();
            components.forEach(c -> derived.add(c.catalogId()));
            catalogIds = List.copyOf(derived);
        } else {
            catalogIds = List.copyOf(catalogIds);
        }
    }

    /** A declaration of these components with the catalogs derived. */
    public static UiDeclaration of(List<Component> components, Entries entries) {
        return new UiDeclaration(PROTOCOL, VERSION, components, entries, List.of());
    }
}
