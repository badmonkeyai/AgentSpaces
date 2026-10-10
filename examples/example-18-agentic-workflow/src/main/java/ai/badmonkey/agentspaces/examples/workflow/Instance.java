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
package ai.badmonkey.agentspaces.examples.workflow;

import java.util.Map;
import java.util.Objects;

/**
 * An entry that is an instance of an ontology class rather than a Java type per
 * class: the individual's id, the class's short name and full IRI, and the
 * properties as a map. Templates can match {@code id} and {@code type}, because
 * they are components; they cannot look inside {@code properties}, because a
 * field predicate reads one accessor. So anything a stage must match on is
 * promoted to a component, and here the individual's id is derived from the
 * claim it describes, which is all the assembler needs.
 *
 * <p>Viewed as JSON the shape is a JSON-LD node, and the properties stay to
 * strings, numbers, booleans, lists and maps so every client decodes them.
 */
public record Instance(String id, String type, String typeIri, Map<String, Object> properties) {

    /** The one class this example carries as an instance. */
    public static final String DAMAGE_FINDING = "DamageFinding";

    public Instance {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(typeIri, "typeIri");
        properties = Map.copyOf(properties);
    }

    /** The individual id of the damage finding for a claim: the correlation key, as an IRI. */
    public static String damageId(String claimId) {
        return Claims.NAMESPACE + "damage-" + claimId;
    }

    /** A {@code DamageFinding} individual. */
    public static Instance damageFinding(String claimId, double estimate, String severity, String by) {
        return new Instance(damageId(claimId), DAMAGE_FINDING, Claims.NAMESPACE + DAMAGE_FINDING,
                Map.of("claimId", claimId, "estimate", estimate, "severity", severity, "by", by));
    }

    /** A string-valued property. */
    public String string(String property) {
        return String.valueOf(properties.get(property));
    }

    /** A numeric property, whatever numeric type the codec decoded it as. */
    public double number(String property) {
        return ((Number) properties.get(property)).doubleValue();
    }
}
