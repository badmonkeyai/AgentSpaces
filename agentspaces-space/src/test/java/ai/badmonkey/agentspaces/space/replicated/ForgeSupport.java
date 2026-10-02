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

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Test-side mirror of the signing views {@code ReplicatedSpace} keeps private
 * ({@code SignView}, {@code StateSignView}), so a test can play an admitted but
 * hostile member that produces records and state deltas signed under its own
 * key. Field names and order match the production records exactly; canonical
 * CBOR of records follows declaration order, so the bytes are identical.
 */
final class ForgeSupport {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private ForgeSupport() {
    }

    private record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                            String payloadRef, AgentId issuer, HlcTimestamp issued,
                            Map<String, String> tags,
                            // v0.1.13, omitted when null
                            @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                            Long keyEpoch) {
    }

    private record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds,
                                 List<Dot> removes, HlcTimestamp leaseStamp,
                                 LeaseInfo leaseValue, boolean completed,
                            // v0.1.13, omitted when null
                            @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                            String signer,
                            @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                            HlcTimestamp signedAt) {
    }

    /** The schema name a space assigns to an entry class. */
    static String schemaNameOf(Class<?> type) {
        return new SimpleSchemaRegistry().register(type);
    }

    /** Signs a record's immutable identity under {@code signer}, as a writer would. */
    static EntryRecord signRecord(PeerIdentity signer, EntryRecord unsigned) {
        byte[] canonical = CODEC.toBytes(new SignView(unsigned.entryId(), unsigned.spaceId(),
                unsigned.type(), unsigned.payload(), unsigned.payloadRef(), unsigned.issuer(),
                unsigned.issued(), unsigned.tags(), unsigned.keyEpoch()));
        return new EntryRecord(unsigned.entryId(), unsigned.spaceId(), unsigned.type(),
                unsigned.payload(), unsigned.payloadRef(), unsigned.issuer(), unsigned.issued(),
                unsigned.lease(), unsigned.tags(), signer.sign(canonical), unsigned.keyEpoch());
    }

    /** The canonical bytes the state signature covers (SPEC §11a.4). */
    static byte[] stateSignBytes(SpaceWire.EntryStateDto dto) {
        return CODEC.toBytes(new StateSignView(dto.record().spaceId(), dto.record().entryId(),
                sorted(dto.adds()), sorted(dto.removes()), dto.leaseStamp(), dto.leaseValue(),
                dto.completed(), dto.signer(), dto.signedAt()));
    }

    /** Returns the DTO carrying a state signature by {@code signer}. */
    static SpaceWire.EntryStateDto signState(PeerIdentity signer, SpaceWire.EntryStateDto dto) {
        return dto.withStateSig(signer.sign(stateSignBytes(dto)));
    }

    /**
     * Builds a complete, validly signed initial write authored by {@code author}:
     * a record whose issuer is {@code author}'s agent, one add dot minted by the
     * author's peer, and an issuer-signed state. Honest replicas accept it.
     */
    static SpaceWire.EntryStateDto authoredWrite(PeerIdentity author, String agentName,
                                                 SpaceId spaceId, Object entry,
                                                 HlcTimestamp issued, long expiresAtMillis) {
        AgentId issuer = author.agent(agentName);
        LeaseInfo lease = new LeaseInfo(issuer, expiresAtMillis, LeaseKind.WRITE);
        EntryRecord record = signRecord(author, new EntryRecord(EntryId.newId(), spaceId,
                schemaNameOf(entry.getClass()), CODEC.toBytes(entry), null, issuer, issued,
                lease, Map.of(), null));
        SpaceWire.EntryStateDto unsigned = new SpaceWire.EntryStateDto(record,
                author.rawPublicKey(), List.of(new Dot(author.peerId().value(), 1)), List.of(),
                issued, lease, false, null);
        return signState(author, unsigned);
    }

    /** Signs a take claim under {@code holder}, as a claimant would. */
    static SpaceWire.SignedClaim signClaim(PeerIdentity holder, TakeClaim claim) {
        return new SpaceWire.SignedClaim(claim, holder.rawPublicKey(),
                holder.sign(CODEC.toBytes(claim)));
    }

    private static List<Dot> sorted(List<Dot> dots) {
        List<Dot> copy = new ArrayList<>(dots);
        copy.sort(Comparator.comparing(Dot::replica).thenComparingLong(Dot::counter));
        return copy;
    }
}
