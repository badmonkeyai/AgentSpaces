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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * A space credential (SPEC §7.5, §11a.4): the payload of a reserved entry type
 * the space's credential issuer writes into the space to admit an agent for
 * the named scopes. Because it is an ordinary entry, the record signature
 * already binds it to its issuer, replicas already store and replicate it,
 * and its write lease is its validity: no new signature scheme or wire kind
 * exists. Every {@link ReplicatedSpace} registers this type, so a credential
 * decodes on every replica; only a replica whose admission rule is
 * {@code CREDENTIAL} indexes it.
 *
 * <p>The schema name is what the space's registry assigns the class
 * ({@code SimpleSchemaRegistry}: the class name plus {@code #v1}).
 *
 * @param agent  the admitted agent
 * @param scopes the scopes granted; normalized to ordinal order so the
 *               canonical CBOR is stable
 */
public record SpaceCredential(AgentId agent, Set<SpaceAdmission.Scope> scopes) {

    public SpaceCredential {
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(scopes, "scopes");
        if (scopes.isEmpty()) {
            throw new IllegalArgumentException("a space credential must grant at least one scope");
        }
        scopes = Collections.unmodifiableSet(EnumSet.copyOf(scopes));
    }

    /** Whether the credential grants the scope. */
    public boolean grants(SpaceAdmission.Scope scope) {
        return scopes.contains(scope);
    }
}
