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
package ai.badmonkey.agentspaces.connect;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The materializing provider style (spec §6.1a): a connector that pushes
 * changes into a space as its source changes, alongside (or instead of) the
 * catalog style {@link AssetProvider} answers on demand. The provider hands
 * each change to the sink; the {@link ConnectorRuntime} writes it into the
 * change space as an {@link DataQueryEntries.AssetChange} entry leased for the
 * asset's declared freshness window, so the lease is the retention policy
 * (P2) and subscribing agents get complex event processing over the source's
 * events through {@code Space.notify} with no extra machinery.
 *
 * <p>A provider that implements both styles serves queries and pushes changes
 * from the same adapter; most connectors do (spec §6.1a).
 */
public interface MaterializingAssetProvider extends AssetProvider {

    /**
     * One change the source emitted.
     *
     * @param asset the asset name from its card, which selects the freshness
     *              window the change is retained for
     * @param key   the changed record's key within the asset (a primary key, a
     *              document id, a message offset); free-form
     * @param row   the changed record, normalized to the shape the card
     *              declares; column name to value
     */
    record Change(String asset, String key, Map<String, String> row) {

        public Change {
            Objects.requireNonNull(asset, "asset");
            Objects.requireNonNull(key, "key");
            row = Map.copyOf(Objects.requireNonNull(row, "row"));
        }
    }

    /**
     * Starts pushing this connector's changes to the sink. The provider calls
     * the sink once per change, from any thread, until the returned handle is
     * closed; the runtime calls this once at {@link ConnectorRuntime#start()}
     * and closes the handle at {@link ConnectorRuntime#close()}.
     *
     * @param sink receives each change as the source emits it
     * @return a handle that stops the feed when closed
     */
    AutoCloseable materialize(Consumer<Change> sink);
}
