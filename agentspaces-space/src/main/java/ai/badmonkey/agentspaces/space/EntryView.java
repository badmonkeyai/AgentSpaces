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
package ai.badmonkey.agentspaces.space;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.Map;
import java.util.Objects;

/**
 * The library spaces' {@link Space.Entry} view over an {@link EntryRecord}
 * (issue #16 §9.2): one immutable snapshot of the record's metadata beside the
 * decoded value, built by {@code readAllEntries} and attached to delivered
 * events. Both {@code LocalSpace} and {@code ReplicatedSpace} use it, so a
 * reader sees the same shape whichever space it holds.
 *
 * @param value       the decoded entry value
 * @param entryId     the entry's identifier
 * @param issuer      the record's authenticated writer
 * @param attestation how far the issuer is proven
 * @param tags        the record's tags
 * @param lease       the lease in force on the record when the view was taken
 * @param issued      the record's issue stamp
 * @param <T>         the entry type
 */
public record EntryView<T>(T value, EntryId entryId, AgentId issuer, Space.Attestation attestation,
                           Map<String, String> tags, LeaseInfo lease, HlcTimestamp issued)
        implements Space.Entry<T> {

    public EntryView {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(attestation, "attestation");
        Objects.requireNonNull(tags, "tags");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(issued, "issued");
    }

    /**
     * Snapshots a record's metadata beside its decoded value.
     *
     * @param record      the entry record
     * @param value       the decoded value
     * @param attestation how far the record's issuer is proven
     * @param <T>         the entry type
     * @return the view
     */
    public static <T> EntryView<T> of(EntryRecord record, T value, Space.Attestation attestation) {
        return new EntryView<>(value, record.entryId(), record.issuer(), attestation,
                record.tags(), record.lease(), record.issued());
    }
}
