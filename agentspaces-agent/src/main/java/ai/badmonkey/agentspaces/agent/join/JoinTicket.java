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
package ai.badmonkey.agentspaces.agent.join;

import java.util.Objects;

/**
 * The entry a {@code @SpaceJoin} in a fleet-wide mode writes when a key is
 * complete and takes before it fires (SPEC §7.1 reserved types, §10.3; issue
 * #16). An ordinary entry, signed by the joiner that wrote it, admitted by the
 * space's rule, leased, visible in the space's data map, and taken under the
 * same claim rules as any take; its tags carry the join name and the key so a
 * joiner's take template selects its own tickets without decoding the rest.
 * Named {@code ai.badmonkey.agentspaces.agent.join.JoinTicket#v1} on the wire
 * and pinned by the golden vector {@code join_ticket_record_cbor}; a peer or
 * client that knows no joins stores and replicates it as any foreign type.
 *
 * @param join the join's name, {@code <agent name>.<method name>} by default
 * @param key  the case's key
 */
public record JoinTicket(String join, String key) {

    /** The tag carrying {@link #join()}. */
    public static final String JOIN_TAG = "join";

    /** The tag carrying {@link #key()}. */
    public static final String KEY_TAG = "key";

    public JoinTicket {
        Objects.requireNonNull(join, "join");
        Objects.requireNonNull(key, "key");
    }
}
