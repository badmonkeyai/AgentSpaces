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
package ai.badmonkey.agentspaces.agent.content;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.WireRecord;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A content store's statement that it holds a root for a pin (PDT 2 phase 0, P2-S-02;
 * srad/agentspaces-pdt2.md §3.2.2): the result a store completes a taken {@link StorePin} with,
 * tagged {@link #ROOT_TAG} and {@link #HOLDER_TAG} as a {@link Holding} is. A receipt is a claim;
 * a possession challenge checks it (P2-S-07). Named
 * {@code ai.badmonkey.agentspaces.agent.content.StoreReceipt#v1} on the wire.
 *
 * <p>A protocol record with the annex I schema of §6.10.4, read from its own wire encoding alone
 * ({@link WireRecord}); it travels ungated and changes shape only under a new type name (P2-F-08).
 *
 * @param v      schema version, {@link #VERSION}
 * @param root   the root held
 * @param holder the holding store
 * @param bytes  the bytes the store holds for the root
 * @param until  epoch milliseconds until which the store holds the root, as {@code Manifest.created} counts
 * @param blind  whether the store holds without the content key (§3.2.5)
 */
@WireRecord
public record StoreReceipt(int v, String root, PeerId holder, long bytes, long until, boolean blind) {

    /** Schema version this record carries. */
    public static final int VERSION = 1;

    /** Tag every receipt carries: the root. */
    public static final String ROOT_TAG = "root";

    /** Tag every receipt carries: the holder. */
    public static final String HOLDER_TAG = "holder";

    public StoreReceipt {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(holder, "holder");
    }

    /** The tags a receipt entry is written with. */
    public Map<String, String> tags() {
        return Map.of(ROOT_TAG, root, HOLDER_TAG, holder.value());
    }

    /**
     * Checks the version, the root, and the signs of the counts.
     *
     * @return the first violated rule, or empty when the receipt is well formed
     */
    public Optional<String> violation() {
        if (v != VERSION) {
            return Optional.of("unsupported StoreReceipt version " + v);
        }
        if (!PRef.isCid(root)) {
            return Optional.of("root is not a CID");
        }
        if (bytes < 0) {
            return Optional.of("negative bytes");
        }
        if (until < 0) {
            return Optional.of("negative until");
        }
        return Optional.empty();
    }
}
