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
package ai.badmonkey.agentspaces.test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What the fleet oracle reads from a fleet (srad/spoiler-peers.md §3.5, SP-O-01): the honest members, each
 * one's replicated-state roots, its frame drops by reason, whether it struck or quarantined another honest
 * member, and the errors its threads raised. This module only holds the contract; the probes that read
 * peering, space and PDT state live where those classes are visible (a peering probe in peering's tests,
 * the fleet probes in {@code agentspaces-spoilers}).
 */
public interface FleetProbe {

    /** The honest members by name; a spoiler is left out. */
    Set<String> members();

    /** A member's replicated states by name (a space, a ref), each as one comparable root or digest text. */
    Map<String, String> roots(String member);

    /** A member's frame drops so far by reason, such as {@code stale}, {@code replay} or {@code undecodable}. */
    Map<String, Long> drops(String member);

    /** The honest members this member holds a strike against, or quarantines, now. */
    Set<String> punished(String member);

    /** Errors a member raised on its own threads (a tick, a handler, a logger at error), oldest first. */
    List<Throwable> errors(String member);
}
