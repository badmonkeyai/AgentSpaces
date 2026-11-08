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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The quota of a {@link NodeStore} (srad/agentspaces-pdt.md PD-S-05;
 * {@code agentspaces.pdt.store.quota}, unbounded by default). Every write passes
 * through {@link #admit}: a node the store already holds costs nothing and is
 * admitted; a new node whose bytes would carry {@link NodeStore#bytesUsed()} past
 * the quota is refused with a {@link QuotaExceededException}, which carries the
 * swarm's over-budget reason, and the warning consumer hears about it once per
 * refusal so the console can raise its warning. A write for a pinned root passes
 * the {@code pinned} flag and is admitted regardless, because the pin says the
 * caller needs it. The wrapper evicts nothing: a reachable node is never removed
 * to make room, and only {@link NodeCollector} frees space.
 *
 * <p>The put methods mirror {@link NodeStore}'s and delegate to it after admission,
 * so a caller that holds a {@code QuotaNodeStore} writes through it and reads from
 * {@link #store()}.
 */
public final class QuotaNodeStore {

    /** The quota that admits everything. */
    public static final long UNBOUNDED = Long.MAX_VALUE;

    private final NodeStore store;
    private volatile long quotaBytes;
    private final Consumer<QuotaExceededException> warning;

    /**
     * A quota over a store.
     *
     * @param store      the store
     * @param quotaBytes the quota in bytes, or {@link #UNBOUNDED}
     * @param warning    hears every refusal; the console warning of PD-S-05 hangs here
     */
    public QuotaNodeStore(NodeStore store, long quotaBytes, Consumer<QuotaExceededException> warning) {
        this.store = Objects.requireNonNull(store, "store");
        this.warning = Objects.requireNonNull(warning, "warning");
        quota(quotaBytes);
    }

    /** An unbounded quota that warns nobody, the default of PD-S-05. */
    public static QuotaNodeStore unbounded(NodeStore store) {
        return new QuotaNodeStore(store, UNBOUNDED, refusal -> { });
    }

    /** The store written through. */
    public NodeStore store() {
        return store;
    }

    /** The quota in bytes, {@link #UNBOUNDED} when none. */
    public long quota() {
        return quotaBytes;
    }

    /**
     * Changes the quota; lowering it below the bytes held refuses new writes and evicts nothing.
     *
     * @param quotaBytes the quota in bytes, or {@link #UNBOUNDED}
     */
    public void quota(long quotaBytes) {
        if (quotaBytes < 0) {
            throw new IllegalArgumentException("quota must not be negative");
        }
        this.quotaBytes = quotaBytes;
    }

    /** The bytes still admissible, {@link Long#MAX_VALUE} when unbounded. */
    public long headroom() {
        long quota = quotaBytes;
        return quota == UNBOUNDED ? Long.MAX_VALUE : Math.max(0, quota - store.bytesUsed());
    }

    /**
     * Decides whether a write may proceed.
     *
     * @param cid    the CID, or null when not yet computed
     * @param length the bytes the write would add
     * @param pinned whether the write serves a pinned root, which is always admitted
     * @throws QuotaExceededException when the write would pass the quota
     */
    public void admit(String cid, long length, boolean pinned) {
        if (pinned || (cid != null && store.has(cid))) {
            return;
        }
        long quota = quotaBytes;
        if (quota == UNBOUNDED) {
            return;
        }
        long used = store.bytesUsed();
        if (used + length > quota) {
            QuotaExceededException refusal = new QuotaExceededException(cid, length, used, quota);
            warning.accept(refusal);
            throw refusal;
        }
    }

    /**
     * Stores bytes under their computed CID when the quota admits them.
     *
     * @param bytes the node or chunk
     * @param codec the codec
     * @return the CID
     * @throws QuotaExceededException when refused
     */
    public String put(byte[] bytes, int codec) {
        return put(bytes, codec, false);
    }

    /**
     * Stores bytes under their computed CID, admitting them regardless when pinned.
     *
     * @param bytes  the node or chunk
     * @param codec  the codec
     * @param pinned whether the write serves a pinned root
     * @return the CID
     * @throws QuotaExceededException when refused
     */
    public String put(byte[] bytes, int codec, boolean pinned) {
        Objects.requireNonNull(bytes, "bytes");
        NodeStore.Stored stored = store.prepare(bytes, codec);
        admit(stored.cid(), stored.bytes().length, pinned);
        store.putVerified(stored.cid(), stored.bytes());
        return stored.cid();
    }

    /**
     * Stores bytes that arrived with a CID when the quota admits them.
     *
     * @param cid   the claimed CID
     * @param bytes the bytes
     * @throws QuotaExceededException when refused
     */
    public void putVerified(String cid, byte[] bytes) {
        putVerified(cid, bytes, false);
    }

    /**
     * Stores bytes that arrived with a CID, admitting them regardless when pinned.
     *
     * @param cid    the claimed CID
     * @param bytes  the bytes
     * @param pinned whether the write serves a pinned root
     * @throws QuotaExceededException when refused
     */
    public void putVerified(String cid, byte[] bytes, boolean pinned) {
        Objects.requireNonNull(bytes, "bytes");
        admit(cid, bytes.length, pinned);
        store.putVerified(cid, bytes);
    }

    /**
     * Stores a batch; admission is judged on the whole batch's new bytes first, so a
     * refusal stores nothing of it.
     *
     * @param nodes  CID to bytes
     * @param pinned whether the batch serves a pinned root
     * @throws QuotaExceededException when refused
     */
    public void putAll(Map<String, byte[]> nodes, boolean pinned) {
        Objects.requireNonNull(nodes, "nodes");
        long newBytes = 0;
        for (Map.Entry<String, byte[]> entry : nodes.entrySet()) {
            if (!store.has(entry.getKey())) {
                newBytes += entry.getValue().length;
            }
        }
        admit(null, newBytes, pinned);
        store.putAll(nodes);
    }
}
