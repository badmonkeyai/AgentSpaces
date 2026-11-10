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

import ai.badmonkey.agentspaces.content.Messages;

/**
 * A write the store refused because it would carry the store past its quota
 * (srad/agentspaces-pdt.md PD-S-05). The exchange reports the refusal with the
 * swarm's over-budget reason, {@link Messages#REASON_OVER_BUDGET}, which
 * {@link #reason()} carries. The store evicts nothing to make room: a quota is a
 * ceiling on new bytes, and collection alone frees space.
 */
public final class QuotaExceededException extends ai.badmonkey.agentspaces.content.tree.StoreRefusedException {

    private static final long serialVersionUID = 1L;

    private final String cid;
    private final long requested;
    private final long bytesUsed;
    private final long quota;

    /**
     * A refusal.
     *
     * @param cid       the CID refused, or null when the bytes had no CID yet
     * @param requested the bytes the write would add
     * @param bytesUsed the bytes held when the write was refused
     * @param quota     the quota
     */
    public QuotaExceededException(String cid, long requested, long bytesUsed, long quota) {
        super("quota of " + quota + " bytes refuses " + requested + " more bytes with " + bytesUsed
                + " held" + (cid == null ? "" : " for node " + cid), Messages.REASON_OVER_BUDGET);
        this.cid = cid;
        this.requested = requested;
        this.bytesUsed = bytesUsed;
        this.quota = quota;
    }

    /** The CID refused, or null when the bytes had no CID yet. */
    public String cid() {
        return cid;
    }

    /** The bytes the write would have added. */
    public long requested() {
        return requested;
    }

    /** The bytes held when the write was refused. */
    public long bytesUsed() {
        return bytesUsed;
    }

    /** The quota. */
    public long quota() {
        return quota;
    }
}
