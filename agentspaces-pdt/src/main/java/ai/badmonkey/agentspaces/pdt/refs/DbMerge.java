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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.pdt.values.Merge;
import ai.badmonkey.agentspaces.pdt.values.PDb;

/**
 * The datom merge of a {@code db} ref (PDT 2 phase 2, P2-D-06; srad/agentspaces-pdt2.md §3.1.4):
 * the engine finds the lowest common base by the version graph and calls this with the two heads,
 * and the merge unions their changes since the base ({@link PDb#changesSince}). A
 * cardinality-one attribute asserted to two values goes to the resolver once, which keeps the
 * later head's value and raises the {@link PConflict}. {@code agentspaces-datascript} supplies the
 * merge, since the schema's cardinalities and DataScript's indexes live there; every reader of a
 * ref must register the same one, and it must be deterministic in its arguments.
 */
@FunctionalInterface
public interface DbMerge {

    /**
     * Merges two heads of a {@code db} ref.
     *
     * @param earlier  the head with the earlier issued HLC; its store takes the merged nodes
     * @param base     the database at their lowest common base, the empty database for two first versions
     * @param later    the head with the later issued HLC
     * @param resolver decides an identity both heads changed to different outcomes; the key names it
     * @return the merged database, whose max-tx is the larger of the two plus one
     */
    PDb merge(PDb earlier, PDb base, PDb later, Merge.Resolver<Object, Object> resolver);
}
