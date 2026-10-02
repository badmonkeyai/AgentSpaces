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
package ai.badmonkey.agentspaces.api.space;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LeaseTest {

    @Test
    void leaseMustBePositive() {
        assertThat(Lease.of(Duration.ofMinutes(5)).duration()).isEqualTo(Duration.ofMinutes(5));
        assertThatThrownBy(() -> Lease.of(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Lease.of(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void leaseInfoExpiryIsInclusiveAtTheBoundary() {
        LeaseInfo lease = new LeaseInfo(AgentId.parse("zP/agent"), 10_000L, LeaseKind.WRITE);

        assertThat(lease.expired(Instant.ofEpochMilli(9_999))).isFalse();
        assertThat(lease.expired(Instant.ofEpochMilli(10_000))).isTrue();
    }

    @Test
    void entryRecordRequiresExactlyOnePayloadForm() {
        LeaseInfo lease = new LeaseInfo(AgentId.parse("zP/agent"), 10_000L, LeaseKind.WRITE);
        HlcTimestamp issued = new HlcTimestamp(1L, 0, "n");
        SpaceId space = SpaceId.local("tasks");

        EntryRecord inline = new EntryRecord(EntryId.newId(), space, "T#v1", new byte[]{1},
                null, AgentId.parse("zP/agent"), issued, lease, Map.of(), null);
        assertThat(inline.payload()).isNotNull();

        EntryRecord referenced = new EntryRecord(EntryId.newId(), space, "T#v1", null,
                "bafy-cid", AgentId.parse("zP/agent"), issued, lease, Map.of(), null);
        assertThat(referenced.payloadRef()).isEqualTo("bafy-cid");

        assertThatThrownBy(() -> new EntryRecord(EntryId.newId(), space, "T#v1", new byte[]{1},
                "bafy-cid", AgentId.parse("zP/agent"), issued, lease, Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EntryRecord(EntryId.newId(), space, "T#v1", null,
                null, AgentId.parse("zP/agent"), issued, lease, Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EntryRecord(EntryId.newId(), space, "T#v1",
                new byte[EntryRecord.INLINE_PAYLOAD_LIMIT + 1], null,
                AgentId.parse("zP/agent"), issued, lease, Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withLeaseReplacesOnlyTheLease() {
        LeaseInfo writeLease = new LeaseInfo(AgentId.parse("zP/agent"), 10_000L, LeaseKind.WRITE);
        LeaseInfo takeLease = new LeaseInfo(AgentId.parse("zP/worker"), 20_000L, LeaseKind.TAKE);
        EntryRecord record = new EntryRecord(EntryId.newId(), SpaceId.local("tasks"), "T#v1",
                new byte[]{1}, null, AgentId.parse("zP/agent"), new HlcTimestamp(1L, 0, "n"),
                writeLease, Map.of("k", "v"), null);

        EntryRecord taken = record.withLease(takeLease);

        assertThat(taken.lease()).isEqualTo(takeLease);
        assertThat(taken.entryId()).isEqualTo(record.entryId());
        assertThat(taken.tags()).isEqualTo(record.tags());

        // v0.1.13: a sealed record's content-key epoch survives a lease change.
        EntryRecord sealed = new EntryRecord(record.entryId(), record.spaceId(), record.type(),
                record.payload(), null, record.issuer(), record.issued(), writeLease, record.tags(),
                null, 3L);
        assertThat(sealed.withLease(takeLease).keyEpoch()).isEqualTo(3L);
    }
}
