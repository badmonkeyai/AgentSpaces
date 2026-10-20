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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.learn.GossipLearner;
import ai.badmonkey.agentspaces.capabilities.learn.WeightAveraging;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.AgentCertificates;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.node.GroupFounding;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.node.SignedGroupAdvertisement;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.SpaceAdmission;
import ai.badmonkey.agentspaces.space.replicated.SpaceCredential;
import ai.badmonkey.agentspaces.space.replicated.SpaceWire;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Emits cross-language golden vectors for the Python and TypeScript wire
 * clients: the exact bytes the Java codec produces for fixed inputs, so the
 * other encoders can be proven byte-identical. Regenerate after any wire-shape
 * change; see docs/BUILD-ENVIRONMENTS.md ("Regenerating the golden vectors")
 * for the compile-and-run command.
 *
 * <p>Key material is stable across runs: when the output file already exists,
 * the generator reloads {@code private_key_pkcs8} (and the adversarial
 * forger's key) from it, so every signature-bearing vector stays byte-identical
 * and only vectors whose inputs actually changed differ between regenerations.
 * A fresh key is generated only when no output file exists.
 *
 * <p>Several wire records are private to their owning class
 * ({@code ReplicatedSpace.SignView}, {@code PushSumAggregate.Frame},
 * {@code GossipLearner.Exchange}, ...). The generator mirrors them with the
 * same component names, types and order, and {@link #assertMirrors} checks
 * each mirror against the real record by reflection on every run, so a drift
 * in the private record fails the generator instead of emitting stale bytes.
 */
public final class GoldenVectors {

    /** Mirrors ReplicatedSpace's private SignView; keep field order in sync. */
    record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                    String payloadRef, AgentId issuer, HlcTimestamp issued,
                    Map<String, String> tags,
                    // v0.1.13: omitted when null (epoch 0 / unencrypted)
                    @com.fasterxml.jackson.annotation.JsonInclude(
                            com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                    Long keyEpoch) {
        SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                 String payloadRef, AgentId issuer, HlcTimestamp issued, Map<String, String> tags) {
            this(entryId, spaceId, type, payload, payloadRef, issuer, issued, tags, null);
        }
    }

    /** Mirrors ReplicatedSpace.StateSignView exactly (field names and order). */
    record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds,
                         List<Dot> removes, HlcTimestamp leaseStamp, LeaseInfo leaseValue,
                         boolean completed,
                         // v0.1.13: omitted when null (a peer-signed state)
                         @com.fasterxml.jackson.annotation.JsonInclude(
                                 com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         String signer,
                         @com.fasterxml.jackson.annotation.JsonInclude(
                                 com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         HlcTimestamp signedAt) {
        StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds, List<Dot> removes,
                      HlcTimestamp leaseStamp, LeaseInfo leaseValue, boolean completed) {
            this(spaceId, entryId, adds, removes, leaseStamp, leaseValue, completed, null, null);
        }
    }

    /** Mirrors GroupKeyDistributor.EpochCommitment exactly: what a rotator signs (SPEC §11a.3). */
    record EpochCommitment(String group, long epoch, String cutover, byte[] keyDigest) {
    }

    /** Mirrors GroupKeyDistributor.WrapBinding exactly: the key-wrap v2 binding (SPEC §11a.2). */
    record WrapBinding(String group, String holder, String requester, long nonce,
                       @com.fasterxml.jackson.annotation.JsonInclude(
                               com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       Long epoch,
                       @com.fasterxml.jackson.annotation.JsonInclude(
                               com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       String rotator,
                       @com.fasterxml.jackson.annotation.JsonInclude(
                               com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       String agent) {
    }

    /** Mirrors example-02's task entry. */
    record ResearchTask(String topic, int priority) {
    }

    /**
     * Mirrors {@code ai.badmonkey.agentspaces.agent.join.JoinTicket} (issue #16,
     * SPEC §7.1 reserved types): the same two components in the same order. The
     * real record lives in agentspaces-agent, which is not on this program's
     * documented classpath, so {@link #assertMirrorsIfLoadable} checks the shape
     * whenever the agent classes happen to be present and reports a skip otherwise.
     */
    record JoinTicket(String join, String key) {
    }

    /**
     * Mirror of the WebRTC signalling body (srad/agentspaces-js.md appendix B, JS-W-03):
     * {@code agentspaces-transport-webrtc}'s {@code RtcSignal}, checked by shape when that
     * module's classes are on the classpath.
     */
    record RtcSignal(int v,
                     @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) List<String> fp,
                     @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) String mid,
                     @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) String sdp,
                     byte[] sid,
                     @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) String cand,
                     @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) Integer code,
                     String kind) {
    }

    // Mirrors of the connector entries (srad/agentspaces-js.md JS-C-01, JS-C-03): the public
    // records of agentspaces-connect-core (DataQueryEntries.DataQuery/DataResult/AssetChange and
    // AssetInvocations.AssetInvocation/AssetInvocationResult), a module outside this program's
    // documented classpath, so assertMirrorsIfLoadable checks them whenever
    // agentspaces-connect-core/target/classes is prepended to CP and notes a skip otherwise.
    record DataQuery(String queryHash, String asset, Map<String, String> parameters, String requester) {
    }

    record DataResult(String queryHash, String asset, List<Map<String, String>> rows, String error,
                      String servedBy) {
    }

    record AssetChange(String asset, String key, Map<String, String> row, String servedBy) {
    }

    record AssetInvocation(int v, String asset, String operation, Map<String, String> params,
                           String invocationId) {
    }

    record AssetInvocationResult(int v, String invocationId, String asset, String operation, boolean ok,
                                 Map<String, String> result, String error) {
    }

    // Mirrors of PushSumAggregate's private wire records (SPEC §8 aggregate,
    // TECH-SPEC §8.2): one Frame carries exactly one of the four variants.
    record Share(String epochId, double value, double weight) {
    }

    record Extremum(String epochId, boolean max, double value) {
    }

    record Histogram(String epochId, double lo, double hi, double[] buckets) {
    }

    record Roster(String epochId, long[] members) {
    }

    record Frame(Share share, Extremum extremum, Histogram histogram, Roster roster) {
    }

    /** Mirrors GossipLearner's private Exchange record (SPEC §8 gossip-learn, TECH-SPEC §8.5). */
    record Exchange(String modelId, long token, String kind, long round, byte[] inline, String cid) {
    }

    private GoldenVectors() {
    }

    public static void main(String[] args) throws Exception {
        CborCodec codec = CborCodec.defaultCodec();
        HexFormat hex = HexFormat.of();
        Path out = Path.of(args.length > 0 ? args[0] : "../agentspaces-spec/golden.json");
        Map<String, Object> previous = loadPrevious(out);

        // Mirror checks first: never emit bytes from a stale private-record mirror.
        assertMirrors(ReplicatedSpace.class, "SignView", SignView.class);
        assertMirrors(ReplicatedSpace.class, "StateSignView", StateSignView.class);
        assertMirrors(PushSumAggregate.class, "Share", Share.class);
        assertMirrors(PushSumAggregate.class, "Extremum", Extremum.class);
        assertMirrors(PushSumAggregate.class, "Histogram", Histogram.class);
        assertMirrors(PushSumAggregate.class, "Roster", Roster.class);
        assertMirrors(PushSumAggregate.class, "Frame", Frame.class);
        assertMirrors(GossipLearner.class, "Exchange", Exchange.class);
        assertMirrors(ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor.class,
                "EpochCommitment", EpochCommitment.class);
        assertMirrors(ai.badmonkey.agentspaces.capabilities.keywrap.GroupKeyDistributor.class,
                "WrapBinding", WrapBinding.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.agent.join.JoinTicket", JoinTicket.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.transport.webrtc.RtcSignal", RtcSignal.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.connect.DataQueryEntries$DataQuery", DataQuery.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.connect.DataQueryEntries$DataResult", DataResult.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.connect.DataQueryEntries$AssetChange", AssetChange.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.connect.AssetInvocations$AssetInvocation",
                AssetInvocation.class);
        assertMirrorsIfLoadable("ai.badmonkey.agentspaces.connect.AssetInvocations$AssetInvocationResult",
                AssetInvocationResult.class);

        // The golden identity: reloaded from the existing file so signatures
        // stay stable; generated fresh only on the very first run.
        PeerIdentity identity;
        byte[] privatePkcs8;
        if (previous.containsKey("private_key_pkcs8")) {
            privatePkcs8 = hex.parseHex((String) previous.get("private_key_pkcs8"));
            identity = PeerIdentity.of(new KeyPair(
                    Ed25519.publicKeyFromRaw(hex.parseHex((String) previous.get("public_key_raw"))),
                    Ed25519.privateKeyFromPkcs8(privatePkcs8)));
        } else {
            Path keyDir = Files.createTempDirectory("golden-keys");
            identity = ai.badmonkey.agentspaces.identity.FileKeystore.loadOrCreate(keyDir);
            privatePkcs8 = Files.readAllBytes(keyDir.resolve("peer.key"));
        }
        // A second, unrelated identity for the forged-signature vectors; kept
        // in the file for the same stability reason (clients never need it).
        PeerIdentity forger;
        byte[] forgerPkcs8;
        if (previous.containsKey("adv_forger_private_key_pkcs8")) {
            forgerPkcs8 = hex.parseHex((String) previous.get("adv_forger_private_key_pkcs8"));
            forger = PeerIdentity.of(new KeyPair(
                    Ed25519.publicKeyFromRaw(
                            hex.parseHex((String) previous.get("adv_forger_public_key_raw"))),
                    Ed25519.privateKeyFromPkcs8(forgerPkcs8)));
        } else {
            KeyPair pair = Ed25519.generate();
            forger = PeerIdentity.of(pair);
            forgerPkcs8 = pair.getPrivate().getEncoded();
        }
        // The subordinate agent key (QA4 A4-7 phase 1): a key the golden peer
        // certifies for its agent "python" and that signs records in the agent's
        // name. Reloaded for the same stability reason.
        KeyPair agentKeys;
        byte[] agentPkcs8;
        if (previous.containsKey("agent_private_key_pkcs8")) {
            agentPkcs8 = hex.parseHex((String) previous.get("agent_private_key_pkcs8"));
            agentKeys = new KeyPair(
                    Ed25519.publicKeyFromRaw(
                            hex.parseHex((String) previous.get("agent_public_key_raw"))),
                    Ed25519.privateKeyFromPkcs8(agentPkcs8));
        } else {
            agentKeys = Ed25519.generate();
            agentPkcs8 = agentKeys.getPrivate().getEncoded();
        }

        GroupId group = GroupId.fromFounding(
                "research-fleet-demo-v1".getBytes(StandardCharsets.UTF_8));
        PeerId self = identity.peerId();
        HlcTimestamp stamp = new HlcTimestamp(1735689600000L, 3, self.value());
        Instant issued = Instant.parse("2026-01-01T00:00:00Z");
        WireCodec wire = new WireCodec(codec);

        StringBuilder json = new StringBuilder("{\n");

        // Identity: fixed key material Python loads to reproduce signatures.
        json.append(field("private_key_pkcs8", hex.formatHex(privatePkcs8)));
        json.append(field("public_key_raw", hex.formatHex(identity.rawPublicKey())));
        json.append(field("peer_id", self.value()));
        json.append(field("group_id", group.value()));
        json.append(field("hlc_encoded", codecString(codec, stamp)));

        // A PING envelope: canonical bytes and the signed frame. Addressed to
        // self here so the golden vector exercises the non-null `to` binding.
        Envelope ping = new Envelope(WireCodec.WIRE_VERSION, group, Envelope.Kind.PING, self, self, stamp,
                codec.toBytes(new Bodies.Ping(42)));
        byte[] envelopeCbor = codec.toBytes(ping);
        json.append(field("ping_body_cbor", hex.formatHex(codec.toBytes(new Bodies.Ping(42)))));
        json.append(field("ping_envelope_cbor", hex.formatHex(envelopeCbor)));
        json.append(field("ping_frame_cbor",
                hex.formatHex(wire.encode(ping, identity))));
        json.append(field("ping_envelope_signature",
                hex.formatHex(identity.sign(envelopeCbor))));

        // The membership intro: PeerAdvertisement, SignedPeerAd payload, Rumor body.
        PeerAdvertisement ad = new PeerAdvertisement(
                "aspace://" + group.value() + "/peer/" + self.value(),
                self, group, issued, Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:7460", 0)),
                java.util.Set.of(), Map.of());
        byte[] adBytes = codec.toBytes(ad);
        json.append(field("peer_ad_cbor", hex.formatHex(adBytes)));
        byte[] signedAd = codec.toBytes(new PeerNode.SignedPeerAd(
                adBytes, identity.rawPublicKey(), identity.sign(adBytes)));
        json.append(field("signed_peer_ad_cbor", hex.formatHex(signedAd)));
        Bodies.Rumor rumor = new Bodies.Rumor("peers",
                "peer:" + self.value() + ":1767225600000", 6, signedAd);
        json.append(field("intro_rumor_body_cbor", hex.formatHex(codec.toBytes(rumor))));

        // A space write: task payload, SignView bytes, record, DTO, delta.
        ResearchTask task = new ResearchTask("golden vectors", 3);
        byte[] payload = codec.toBytes(task);
        json.append(field("task_payload_cbor", hex.formatHex(payload)));
        EntryId entryId = EntryId.of("11111111-2222-3333-4444-555555555555");
        SpaceId spaceId = SpaceId.local(group.value() + "/tasks");
        json.append(field("space_id", spaceId.value()));
        AgentId issuer = identity.agent("python");
        SignView view = new SignView(entryId, spaceId,
                "GoldenVectors$ResearchTask#v1", payload, null, issuer, stamp, Map.of());
        byte[] viewBytes = codec.toBytes(view);
        json.append(field("sign_view_cbor", hex.formatHex(viewBytes)));
        byte[] recordSig = identity.sign(viewBytes);
        json.append(field("record_signature", hex.formatHex(recordSig)));
        EntryRecord record = new EntryRecord(entryId, spaceId,
                "GoldenVectors$ResearchTask#v1", payload, null, issuer, stamp,
                new LeaseInfo(issuer, 1735693200000L, LeaseKind.WRITE), Map.of(), recordSig);
        LeaseInfo lease = new LeaseInfo(issuer, 1735693200000L, LeaseKind.WRITE);
        List<Dot> adds = List.of(new Dot(self.value(), 1));
        List<Dot> removes = List.of();
        // The state signature (SPEC §11a): the actor signs the mutable state so a
        // receiver can authenticate the transition. Field order here must match
        // ReplicatedSpace.StateSignView exactly.
        StateSignView stateView = new StateSignView(spaceId, entryId, adds, removes,
                stamp, lease, false);
        byte[] stateViewBytes = codec.toBytes(stateView);
        json.append(field("state_sign_view_cbor", hex.formatHex(stateViewBytes)));
        byte[] stateSig = identity.sign(stateViewBytes);
        json.append(field("state_signature", hex.formatHex(stateSig)));
        SpaceWire.EntryStateDto dto = new SpaceWire.EntryStateDto(record,
                identity.rawPublicKey(), adds, removes, stamp, lease, false, stateSig);
        json.append(field("entry_state_dto_cbor", hex.formatHex(codec.toBytes(dto))));
        json.append(field("delta_cbor",
                hex.formatHex(codec.toBytes(new SpaceWire.Delta(dto, null, null)))));

        // A signed take claim (the LEASE_RACE lattice value, spec §7.4).
        ai.badmonkey.agentspaces.space.replicated.TakeClaim claim =
                new ai.badmonkey.agentspaces.space.replicated.TakeClaim(
                        entryId, spaceId, 1, stamp, issuer, 0.0, 1735693200000L);
        byte[] claimBytes = codec.toBytes(claim);
        json.append(field("take_claim_cbor", hex.formatHex(claimBytes)));
        byte[] claimSig = identity.sign(claimBytes);
        json.append(field("take_claim_signature", hex.formatHex(claimSig)));
        json.append(field("claim_delta_cbor", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId,
                        new SpaceWire.SignedClaim(claim, identity.rawPublicKey(),
                                claimSig))))));
        json.append(servingVectors(dto, claim));

        // Digest body sample so Python can politely answer anti-entropy.
        json.append(field("digest_body_cbor", hex.formatHex(codec.toBytes(
                new Bodies.Digest(Map.of("peers", "x".getBytes(StandardCharsets.UTF_8)))))));

        // ------------------------------------------------------------------
        // Adversarial conformance vectors: hostile inputs every implementation
        // must reject identically (security review §6 / remediation plan WS1).
        // Each `adv_*` field is an input the consuming implementation must
        // refuse — by failed decode, failed verification, or a bounds check —
        // without crashing its reader.

        // A signed claim whose signature covers different bytes.
        byte[] wrongSig = identity.sign("not the claim".getBytes(StandardCharsets.UTF_8));
        json.append(field("adv_claim_bad_signature_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId,
                        new SpaceWire.SignedClaim(claim, identity.rawPublicKey(),
                                wrongSig))))));

        // A validly signed claim transplanted under a different claimEntry key:
        // the signed entryId binding (ASF-002) must be enforced, not the key.
        EntryId otherEntry = EntryId.of("99999999-8888-7777-6666-555555555555");
        json.append(field("adv_claim_transplanted_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, otherEntry,
                        new SpaceWire.SignedClaim(claim, identity.rawPublicKey(),
                                claimSig))))));

        // Claims whose fields a strict decoder or validator must refuse: a NaN
        // bid would win every lattice comparison by making the total order lie
        // (ASF-033), and a non-positive epoch is outside the lattice. Encoded
        // as raw maps because the typed constructors refuse to build them.
        java.util.LinkedHashMap<String, Object> nanClaim = new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        nanClaim.put("entryId", entryId.value());
        nanClaim.put("spaceId", spaceId.value());
        nanClaim.put("epoch", 1L);
        nanClaim.put("stamp", stamp.encoded());
        nanClaim.put("holder", issuer.encoded());
        nanClaim.put("bid", Double.NaN);
        nanClaim.put("expiresAtMillis", 1735693200000L);
        json.append(field("adv_claim_nan_bid_delta",
                hex.formatHex(codec.toBytes(rawDelta(entryId, nanClaim, identity, codec)))));
        java.util.LinkedHashMap<String, Object> zeroEpoch = new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim(nanClaim);
        zeroEpoch.put("epoch", 0L);
        zeroEpoch.put("bid", 0.0);
        json.append(field("adv_claim_epoch_zero_delta",
                hex.formatHex(codec.toBytes(rawDelta(entryId, zeroEpoch, identity, codec)))));

        // Validly signed claims that must fail the merge bounds (ASF-004): an
        // epoch jump far beyond any honest history, and an eternal hold.
        ai.badmonkey.agentspaces.space.replicated.TakeClaim jump =
                new ai.badmonkey.agentspaces.space.replicated.TakeClaim(
                        entryId, spaceId, 1L << 30, stamp, issuer, 0.0, 1735693200000L);
        json.append(field("adv_claim_epoch_jump_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId,
                        new SpaceWire.SignedClaim(jump, identity.rawPublicKey(),
                                identity.sign(codec.toBytes(jump))))))));
        ai.badmonkey.agentspaces.space.replicated.TakeClaim eternal =
                new ai.badmonkey.agentspaces.space.replicated.TakeClaim(
                        entryId, spaceId, 2, stamp, issuer, 0.0, Long.MAX_VALUE);
        json.append(field("adv_claim_eternal_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId,
                        new SpaceWire.SignedClaim(eternal, identity.rawPublicKey(),
                                identity.sign(codec.toBytes(eternal))))))));

        // 100-deep nested CBOR arrays: decoding as any wire structure must fail
        // bounded (no stack overflow, no reader death).
        byte[] deep = new byte[101];
        java.util.Arrays.fill(deep, 0, 100, (byte) 0x81);
        deep[100] = 0x01;
        json.append(field("adv_deep_nesting_cbor", hex.formatHex(deep)));

        // A signed frame with one flipped signature byte: must not verify.
        byte[] tampered = wire.encode(ping, identity);
        tampered[tampered.length - 1] ^= 0x01;
        json.append(field("adv_frame_bad_signature", hex.formatHex(tampered)));

        // A correctly signed frame addressed to a different peer: signature
        // verification succeeds, and the receiver must still drop it (ASF-010).
        PeerId other = PeerId.fromPublicKey(new byte[32]);
        Envelope misaddressed = new Envelope(WireCodec.WIRE_VERSION, group, Envelope.Kind.PING, self, other,
                stamp, codec.toBytes(new Bodies.Ping(43)));
        json.append(field("adv_frame_wrong_destination",
                hex.formatHex(wire.encode(misaddressed, identity))));
        json.append(field("adv_frame_wrong_destination_to", other.value()));

        // The framing cap every implementation enforces (ASF-016).
        json.append(number("max_frame_bytes", 8388608));

        // ------------------------------------------------------------------
        // v0.1.10 wire structures (QA2 workstream C).

        // The self-certifying group founding (SPEC §4.4, §5.1), produced by
        // GroupFounding.found itself so the canonical inputs are exactly the
        // production ones. Founder = the golden identity; name "golden-fleet";
        // issued 2026-01-01T00:00:00Z; ttl P1D; OPEN; LEASE_RACE; gossip (3, PT1S).
        //   founding_fields_cbor   canonical CBOR of FoundingFields
        //                          {name, founder, issued, ttl, membershipPolicy,
        //                          defaultStrategy, gossip{fanout, period}}
        //   founding_signature     founder's Ed25519 signature over founding_fields_cbor
        //   founding_document_cbor canonical CBOR of FoundingDocument {fields, signature}
        //   founding_group_id      multibase(sha-256(founding_document_cbor))
        //   founding_ad_id         "aspace://" + founding_group_id
        //   group_ad_cbor          canonical CBOR of the GroupAdvertisement
        //                          {id, issuer, group, issued, ttl, name,
        //                          membershipPolicy, defaultStrategy, gossip}
        //   signed_group_ad_cbor   SignedGroupAdvertisement {advertisement,
        //                          founderPublicKey, signature}: the GROUP_AD body
        GroupAdvertisement.GossipParameters gossip =
                new GroupAdvertisement.GossipParameters(3, Duration.ofSeconds(1));
        SignedGroupAdvertisement founding = GroupFounding.found(identity, "golden-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                gossip, issued, Duration.ofDays(1));
        GroupFounding.FoundingFields fields = GroupFounding.fieldsOf(founding.advertisement());
        byte[] fieldsBytes = codec.toBytes(fields);
        GroupId foundingGroup = founding.advertisement().group();
        require(GroupFounding.verify(founding), "founding advertisement verifies");
        require(GroupFounding.derive(fields, founding.signature()).equals(foundingGroup),
                "derived id matches");
        json.append(field("founding_fields_cbor", hex.formatHex(fieldsBytes)));
        json.append(field("founding_signature", hex.formatHex(founding.signature())));
        json.append(field("founding_document_cbor", hex.formatHex(codec.toBytes(
                new GroupFounding.FoundingDocument(fields, founding.signature())))));
        json.append(field("founding_group_id", foundingGroup.value()));
        json.append(field("founding_ad_id", founding.advertisement().id()));
        json.append(field("group_ad_cbor", hex.formatHex(codec.toBytes(founding.advertisement()))));
        byte[] signedGroupAd = codec.toBytes(founding);
        json.append(field("signed_group_ad_cbor", hex.formatHex(signedGroupAd)));

        // Join-by-GroupID bootstrap frames (SPEC §9, TECH-SPEC §3.1). PeerNode
        // sends GROUP_AD_WANT with an empty (zero-length) body and, because the
        // seed's PeerID is unknown until it answers, with `to` absent; the
        // vector addresses it to the golden peer so the non-null `to` binding
        // is exercised, exactly as ping_frame_cbor does. The GROUP_AD answer's
        // body is signed_group_ad_cbor verbatim. Both are in the founding group.
        //   group_ad_want_envelope_cbor  Envelope {ver 2, group founding_group_id,
        //                                kind GROUP_AD_WANT, from, to, stamp, body h''}
        //   group_ad_want_frame_cbor     the signed frame around it
        //   group_ad_envelope_cbor       Envelope {..., kind GROUP_AD, body signed_group_ad_cbor}
        //   group_ad_frame_cbor          the signed frame around it
        Envelope want = new Envelope(WireCodec.WIRE_VERSION, foundingGroup, Envelope.Kind.GROUP_AD_WANT, self, self,
                stamp, new byte[0]);
        json.append(field("group_ad_want_envelope_cbor", hex.formatHex(codec.toBytes(want))));
        json.append(field("group_ad_want_frame_cbor", hex.formatHex(wire.encode(want, identity))));
        Envelope answer = new Envelope(WireCodec.WIRE_VERSION, foundingGroup, Envelope.Kind.GROUP_AD, self, self,
                stamp, signedGroupAd);
        json.append(field("group_ad_envelope_cbor", hex.formatHex(codec.toBytes(answer))));
        json.append(field("group_ad_frame_cbor", hex.formatHex(wire.encode(answer, identity))));

        // Adversarial GROUP_AD answers a joiner must refuse (SPEC §5.1's four
        // checks). Both decode cleanly; both fail GroupFounding.verify.
        //   adv_group_ad_wrong_id_cbor  the founder's genuine signature over the
        //       founding fields, but `group` and `id` name the legacy golden
        //       group_id, which the founding document does not derive to
        //   adv_group_ad_forged_signature_cbor  correct group and id, the golden
        //       founder's key, and a signature by an unrelated key
        //       (adv_forger_*), so the signature check fails
        SignedGroupAdvertisement wrongId = new SignedGroupAdvertisement(
                new GroupAdvertisement(GroupFounding.URI_PREFIX + group.value(), self, group,
                        issued, Duration.ofDays(1), "golden-fleet",
                        GroupAdvertisement.MembershipPolicy.OPEN,
                        ConflictStrategyType.LEASE_RACE, gossip),
                identity.rawPublicKey(), founding.signature());
        require(!GroupFounding.verify(wrongId), "wrong-id advertisement is refused");
        json.append(field("adv_group_ad_wrong_id_cbor", hex.formatHex(codec.toBytes(wrongId))));
        SignedGroupAdvertisement forged = new SignedGroupAdvertisement(founding.advertisement(),
                identity.rawPublicKey(), forger.sign(fieldsBytes));
        require(!GroupFounding.verify(forged), "forged-signature advertisement is refused");
        json.append(field("adv_group_ad_forged_signature_cbor",
                hex.formatHex(codec.toBytes(forged))));
        json.append(field("adv_forger_private_key_pkcs8", hex.formatHex(forgerPkcs8)));
        json.append(field("adv_forger_public_key_raw", hex.formatHex(forger.rawPublicKey())));

        // Advertisements with the v0.1.10 additive fields (SPEC §6.1), in the
        // legacy golden group so they sit beside peer_ad_cbor and space_id.
        //   agent_card_cbor        AgentCard {id, issuer, group, issued, ttl, agent,
        //                          description, goals, consumes, produces, costHints,
        //                          spaceBindings}; ttl PT15M; agent = peer_id/python
        //   agent_card_signature   Ed25519 over agent_card_cbor (AdvertisementSigner.sign)
        //   signed_agent_card_cbor AdCache.StoredAd {adType "AgentCard", adBytes =
        //                          agent_card_cbor, publicKey, signature}: the
        //                          `ads`-stream / QUERY_HIT wire form
        //   space_ad_cbor          SpaceAdvertisement {id, issuer, group, issued, ttl,
        //                          spaceName, schemaHints, strategy, admission,
        //                          replication} with ALLOWLIST and TAG_SHARDED
        //   space_ad_space_id      SpaceId.fromFounding(space_ad_cbor) (SPEC §4.4)
        String taskSchema = "GoldenVectors$ResearchTask#v1";
        AgentCard card = new AgentCard(
                "aspace://" + group.value() + "/agent/" + self.value() + "/python",
                self, group, issued, Duration.ofMinutes(15), issuer,
                "Researches a topic and writes a finding",
                List.of("research"), List.of(taskSchema), List.of("GoldenVectors$Finding#v1"),
                Map.of("tokens", "low"), Map.of(taskSchema, "tasks"));
        byte[] cardBytes = codec.toBytes(card);
        json.append(field("agent_card_cbor", hex.formatHex(cardBytes)));
        SignedAdvertisement<AgentCard> signedCard = new AdvertisementSigner(codec).sign(card, identity);
        require(new AdvertisementSigner(codec).verify(signedCard), "agent card verifies");
        json.append(field("agent_card_signature", hex.formatHex(signedCard.signature())));
        AdCache cache = new AdCache(codec, InstantSource.fixed(issued));
        AdCache.StoredAd stored = cache.toStored(card, identity.rawPublicKey(),
                signedCard.signature());
        require(cache.accept(stored).isPresent(), "stored agent card is admitted");
        json.append(field("signed_agent_card_cbor", hex.formatHex(codec.toBytes(stored))));
        SpaceAdvertisement spaceAd = new SpaceAdvertisement(
                "aspace://" + group.value() + "/tasks", self, group, issued,
                Duration.ofMinutes(15), "tasks", List.of(taskSchema),
                ConflictStrategyType.LEASE_RACE, SpaceAdvertisement.Admission.ALLOWLIST,
                SpaceAdvertisement.Replication.TAG_SHARDED);
        byte[] spaceAdBytes = codec.toBytes(spaceAd);
        json.append(field("space_ad_cbor", hex.formatHex(spaceAdBytes)));
        json.append(field("space_ad_space_id", SpaceId.fromFounding(spaceAdBytes).value()));

        // A space credential entry record (SPEC §7.5 CREDENTIAL admission, v0.1.11):
        // an ordinary EntryRecord of the reserved type the space's credential
        // issuer writes to admit an agent; its write lease is the credential's
        // validity and its record signature (the same SignView the golden
        // record_signature covers) binds it to the issuer. Clients only need to
        // recognise the type name; they never issue credentials.
        //   space_credential_record_cbor  EntryRecord {entryId, spaceId (= space_id),
        //       type "ai.badmonkey.agentspaces.space.replicated.SpaceCredential#v1",
        //       payload = CBOR of SpaceCredential {agent peer_id/python, scopes
        //       ["WRITE", "TAKE"]}, payloadRef null, issuer peer_id/python, issued
        //       hlc_encoded, lease {holder, expiresAtMillis issued + PT1H, WRITE},
        //       tags {}, sig = Ed25519 over the SignView bytes}
        String credentialType = SpaceCredential.class.getName() + "#v1";
        SpaceCredential credential = new SpaceCredential(issuer,
                java.util.Set.of(SpaceAdmission.Scope.WRITE, SpaceAdmission.Scope.TAKE));
        byte[] credentialPayload = codec.toBytes(credential);
        EntryId credentialEntry = EntryId.of("22222222-3333-4444-5555-666666666666");
        byte[] credentialView = codec.toBytes(new SignView(credentialEntry, spaceId, credentialType,
                credentialPayload, null, issuer, stamp, Map.of()));
        EntryRecord credentialRecord = new EntryRecord(credentialEntry, spaceId, credentialType,
                credentialPayload, null, issuer, stamp,
                new LeaseInfo(issuer, stamp.physical() + Duration.ofHours(1).toMillis(),
                        LeaseKind.WRITE),
                Map.of(), identity.sign(credentialView));
        json.append(field("space_credential_record_cbor",
                hex.formatHex(codec.toBytes(credentialRecord))));

        // A join ticket entry record (SPEC §7.1 reserved types, §10.3; issue #16):
        // the ordinary EntryRecord a @SpaceJoin in LEASED or ORDERED mode writes
        // when a key completes and takes before it fires. Signed by the joiner
        // that wrote it over the same SignView as any record (tags included),
        // leased WRITE for the ticket's validity, tagged with the join name and
        // the key so a joiner's take template selects its own tickets without
        // decoding the payload. Clients only need to recognise the type name;
        // a peer or client that knows no joins stores and replicates it as any
        // foreign type.
        //   join_ticket_record_cbor  EntryRecord {entryId, spaceId (= space_id),
        //       type "ai.badmonkey.agentspaces.agent.join.JoinTicket#v1",
        //       payload = CBOR of JoinTicket {join "golden.join", key "golden-key"},
        //       payloadRef null, issuer peer_id/python, issued hlc_encoded,
        //       lease {holder, expiresAtMillis issued + PT10M, WRITE},
        //       tags {join "golden.join", key "golden-key"} (in that order),
        //       sig = Ed25519 over the SignView bytes, tags in that same order}
        // EntryRecord copies its tags through Map.copyOf, whose iteration order is
        // salted per JVM start, so codec.toBytes(record) would encode a two-tag
        // map in either order from one run to the next (and a Java peer
        // re-verifying a multi-tag record rebuilds the view in its own order).
        // The vector is therefore assembled below as EntryRecord's ten wire
        // fields in component order with the tags in the signed (join, key)
        // order, which is what a client that verifies over the tags as received
        // needs; the self-checks prove the bytes decode to the record built here
        // and differ from the record's own encoding in nothing but tag order.
        String joinTicketType = "ai.badmonkey.agentspaces.agent.join.JoinTicket#v1";
        JoinTicket joinTicket = new JoinTicket("golden.join", "golden-key");
        byte[] joinTicketPayload = codec.toBytes(joinTicket);
        Map<String, String> joinTicketTags = new java.util.LinkedHashMap<>();
        joinTicketTags.put("join", joinTicket.join());
        joinTicketTags.put("key", joinTicket.key());
        EntryId joinTicketEntry = EntryId.of("33333333-4444-5555-6666-777777777777");
        byte[] joinTicketView = codec.toBytes(new SignView(joinTicketEntry, spaceId, joinTicketType,
                joinTicketPayload, null, issuer, stamp, joinTicketTags));
        EntryRecord joinTicketRecord = new EntryRecord(joinTicketEntry, spaceId, joinTicketType,
                joinTicketPayload, null, issuer, stamp,
                new LeaseInfo(issuer, stamp.physical() + Duration.ofMinutes(10).toMillis(),
                        LeaseKind.WRITE),
                joinTicketTags, identity.sign(joinTicketView));
        java.util.LinkedHashMap<String, Object> joinTicketWire = new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        joinTicketWire.put("entryId", joinTicketRecord.entryId());
        joinTicketWire.put("spaceId", joinTicketRecord.spaceId());
        joinTicketWire.put("type", joinTicketRecord.type());
        joinTicketWire.put("payload", joinTicketRecord.payload());
        joinTicketWire.put("payloadRef", null);
        joinTicketWire.put("issuer", joinTicketRecord.issuer());
        joinTicketWire.put("issued", joinTicketRecord.issued());
        joinTicketWire.put("lease", joinTicketRecord.lease());
        joinTicketWire.put("tags", joinTicketTags);
        joinTicketWire.put("sig", joinTicketRecord.sig());
        byte[] joinTicketBytes = codec.toBytes(joinTicketWire);
        EntryRecord joinTicketDecoded = codec.fromBytes(joinTicketBytes, EntryRecord.class);
        require(joinTicketDecoded.entryId().equals(joinTicketEntry)
                && joinTicketDecoded.spaceId().equals(spaceId)
                && joinTicketDecoded.type().equals(joinTicketType)
                && Arrays.equals(joinTicketDecoded.payload(), joinTicketPayload)
                && joinTicketDecoded.payloadRef() == null
                && joinTicketDecoded.issuer().equals(issuer)
                && joinTicketDecoded.issued().equals(stamp)
                && joinTicketDecoded.lease().equals(joinTicketRecord.lease())
                && joinTicketDecoded.tags().equals(joinTicketTags)
                && Arrays.equals(joinTicketDecoded.sig(), joinTicketRecord.sig())
                && joinTicketDecoded.keyEpoch() == null,
                "join ticket wire form decodes to the record it was built from");
        @SuppressWarnings("unchecked")
        Map<String, Object> joinTicketWireKeys = codec.fromBytes(joinTicketBytes, Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> joinTicketRecordKeys =
                codec.fromBytes(codec.toBytes(joinTicketRecord), Map.class);
        require(List.copyOf(joinTicketWireKeys.keySet()).equals(List.copyOf(joinTicketRecordKeys.keySet())),
                "join ticket wire form carries EntryRecord's fields in component order");
        @SuppressWarnings("unchecked")
        Map<String, Object> joinTicketWireTags = (Map<String, Object>) joinTicketWireKeys.get("tags");
        // ISSUE-CanonicalMaps (wire v3): map entries sort shorter key first, then
        // bytewise, so "key" precedes "join" whatever order the map was built in.
        require(List.copyOf(joinTicketWireTags.keySet()).equals(List.of("key", "join")),
                "join ticket tags encode key then join (canonical order)");
        json.append(field("join_ticket_record_cbor", hex.formatHex(joinTicketBytes)));

        // Aggregate pipe frames (SPEC §8, TECH-SPEC §8.2): the PIPE_DATA payload
        // for capability aspace:cap/aggregate is CBOR of the Frame union with
        // exactly one variant set and the other three null. Epoch "golden".
        //   aggregate_share_frame_cbor      Frame{share: Share{"golden", 4.5, 0.5}}
        //   aggregate_extremum_frame_cbor   Frame{extremum: Extremum{"golden", max true, 4.5}}
        //   aggregate_histogram_frame_cbor  Frame{histogram: Histogram{"golden", lo 0.0,
        //                                   hi 10.0, buckets [1.0, 0.0, 2.0, 0.5]}}
        //   aggregate_roster_frame_cbor     Frame{roster: Roster{"golden", members
        //                                   [aggregate_roster_token]}}
        //   aggregate_roster_token          the golden peer's participant token as a
        //                                   signed decimal int64: the first 8 bytes of
        //                                   sha-256(peer_id UTF-8), big-endian
        long token = ByteBuffer.wrap(
                Digests.sha256(self.value().getBytes(StandardCharsets.UTF_8)), 0, 8).getLong();
        json.append(field("aggregate_share_frame_cbor", hex.formatHex(codec.toBytes(
                new Frame(new Share("golden", 4.5, 0.5), null, null, null)))));
        json.append(field("aggregate_extremum_frame_cbor", hex.formatHex(codec.toBytes(
                new Frame(null, new Extremum("golden", true, 4.5), null, null)))));
        json.append(field("aggregate_histogram_frame_cbor", hex.formatHex(codec.toBytes(
                new Frame(null, null, new Histogram("golden", 0.0, 10.0,
                        new double[] {1.0, 0.0, 2.0, 0.5}), null)))));
        json.append(field("aggregate_roster_frame_cbor", hex.formatHex(codec.toBytes(
                new Frame(null, null, null, new Roster("golden", new long[] {token}))))));
        json.append(field("aggregate_roster_token", Long.toString(token)));

        // Gossip-learn exchange frames (SPEC §8, TECH-SPEC §8.5): the PIPE_DATA
        // payload for aspace:cap/gossip-learn. Model [1.0, 2.5, -3.0] under
        // WeightAveraging (big-endian IEEE-754 doubles), well under the 64 KiB
        // inline limit, so the encoding travels `inline` and `cid` is null.
        //   learn_model_encoding        WeightAveraging.encode(model), raw bytes (hex)
        //   learn_content_id            base58btc multibase of sha-256(learn_model_encoding)
        //   learn_exchange_offer_cbor   Exchange{"golden-model", token 7, "OFFER", round 2,
        //                               inline = learn_model_encoding, cid null}
        //   learn_exchange_accept_cbor  Exchange{"golden-model", 7, "ACCEPT", round 3,
        //                               inline = learn_model_encoding, cid null}
        //   learn_exchange_busy_cbor    Exchange{"golden-model", 7, "BUSY", round 0,
        //                               inline null, cid null} (GossipLearner's BUSY shape)
        byte[] modelBytes = WeightAveraging.INSTANCE.encode(new double[] {1.0, 2.5, -3.0});
        json.append(field("learn_model_encoding", hex.formatHex(modelBytes)));
        json.append(field("learn_content_id", BlockExchange.cidOf(modelBytes)));
        json.append(field("learn_exchange_offer_cbor", hex.formatHex(codec.toBytes(
                new Exchange("golden-model", 7L, "OFFER", 2L, modelBytes, null)))));
        json.append(field("learn_exchange_accept_cbor", hex.formatHex(codec.toBytes(
                new Exchange("golden-model", 7L, "ACCEPT", 3L, modelBytes, null)))));
        json.append(field("learn_exchange_busy_cbor", hex.formatHex(codec.toBytes(
                new Exchange("golden-model", 7L, "BUSY", 0L, null, null)))));

        // Subordinate agent keys (SPEC §4.2, §11a.3; TECH-SPEC §7.2; QA4 A4-7
        // phase 1). The peer certifies an agent key; records the agent writes are
        // signed by that key while the DTO's issuerPublicKey stays the peer key
        // (it verifies the certificate and the PeerId binding) and state
        // signatures stay peer-signed. Verification is against a receiver clock
        // of certificate_verify_at, which is the record's own issue instant
        // (hlc_encoded's physical time) so that a replica pinned there holds the
        // record's lease live and the certificate inside its window at once.
        //   agent_private_key_pkcs8 / agent_public_key_raw  the agent key
        //   agent_certificate_unsigned_cbor  AgentCertificate {agent peer_id/python,
        //       agentPublicKey, issued = certificate_verify_at, ttl PT24H,
        //       peerSignature null}: the exact bytes the peer signs
        //   agent_certificate_signature  Ed25519 by the peer key over those bytes
        //   agent_certificate_cbor       the signed certificate as EntryStateDto carries it
        //   record_signature_agent_key   Ed25519 by the agent key over sign_view_cbor
        //   entry_state_with_certificate_cbor  entry_state_dto_cbor's DTO with
        //       record.sig = record_signature_agent_key, the same peer-signed
        //       stateSig, and the appended agentCertificate field
        //   delta_with_certificate_cbor  the Delta carrying it
        // Hostile variants, each a Delta a receiver must drop (TECH-SPEC §7.2's
        // two-key rule), every one decodable and differing in exactly one respect:
        //   adv_cert_wrong_peer_delta         certificate signed by adv_forger, not the issuing peer
        //   adv_cert_wrong_agent_delta        certificate names peer_id/other, the record peer_id/python
        //   adv_cert_expired_delta            certificate issued a day before certificate_verify_at with ttl PT1H
        //   adv_cert_uncertified_key_delta    record signed by adv_forger's key, which no certificate carries
        //   adv_cert_peer_signed_record_delta valid certificate, record still bears the peer's record_signature
        Instant verifyAt = Instant.ofEpochMilli(stamp.physical());
        AgentCertificates certificates = new AgentCertificates(codec);
        AgentIdentity agent = identity.subordinate("python", agentKeys, verifyAt, Duration.ofHours(24));
        AgentCertificate certificate = agent.certificate().orElseThrow();
        require(certificate.agent().equals(issuer), "certificate names the golden issuer");
        json.append(field("certificate_verify_at", verifyAt.toString()));
        json.append(field("agent_private_key_pkcs8", hex.formatHex(agentPkcs8)));
        json.append(field("agent_public_key_raw", hex.formatHex(agent.publicKey())));
        json.append(field("agent_certificate_unsigned_cbor",
                hex.formatHex(codec.toBytes(certificate.unsigned()))));
        json.append(field("agent_certificate_signature", hex.formatHex(certificate.peerSignature())));
        json.append(field("agent_certificate_cbor", hex.formatHex(codec.toBytes(certificate))));
        byte[] agentRecordSig = agent.sign(viewBytes);
        json.append(field("record_signature_agent_key", hex.formatHex(agentRecordSig)));
        EntryRecord agentRecord = new EntryRecord(entryId, spaceId,
                "GoldenVectors$ResearchTask#v1", payload, null, issuer, stamp, lease, Map.of(),
                agentRecordSig);
        SpaceWire.EntryStateDto certifiedDto = new SpaceWire.EntryStateDto(agentRecord,
                identity.rawPublicKey(), adds, removes, stamp, lease, false, stateSig, certificate);
        require(certificates.verify(certificate, identity.rawPublicKey(), issuer, verifyAt)
                && Ed25519.verifyRaw(certificate.agentPublicKey(), viewBytes, agentRecordSig),
                "certified record verifies under the two-key rule");
        json.append(field("entry_state_with_certificate_cbor", hex.formatHex(codec.toBytes(certifiedDto))));
        json.append(field("delta_with_certificate_cbor",
                hex.formatHex(codec.toBytes(new SpaceWire.Delta(certifiedDto, null, null)))));

        AgentCertificate wrongPeer = certificate.unsigned()
                .signed(forger.sign(codec.toBytes(certificate.unsigned())));
        AgentCertificate wrongAgent = certificates.sign(new AgentCertificate(identity.agent("other"),
                agent.publicKey(), verifyAt, Duration.ofHours(24), null), identity);
        AgentCertificate expired = certificates.sign(new AgentCertificate(issuer, agent.publicKey(),
                verifyAt.minus(Duration.ofDays(1)), Duration.ofHours(1), null), identity);
        Map<String, AgentCertificate> hostileCertificates = new java.util.LinkedHashMap<>();
        hostileCertificates.put("adv_cert_wrong_peer_delta", wrongPeer);
        hostileCertificates.put("adv_cert_wrong_agent_delta", wrongAgent);
        hostileCertificates.put("adv_cert_expired_delta", expired);
        for (Map.Entry<String, AgentCertificate> hostile : hostileCertificates.entrySet()) {
            require(!certificates.verify(hostile.getValue(), identity.rawPublicKey(), issuer, verifyAt),
                    hostile.getKey() + " certificate is refused");
            json.append(field(hostile.getKey(), hex.formatHex(codec.toBytes(new SpaceWire.Delta(
                    new SpaceWire.EntryStateDto(agentRecord, identity.rawPublicKey(), adds, removes,
                            stamp, lease, false, stateSig, hostile.getValue()), null, null)))));
        }
        byte[] uncertifiedSig = forger.sign(viewBytes);
        require(!Ed25519.verifyRaw(certificate.agentPublicKey(), viewBytes, uncertifiedSig),
                "uncertified key's record signature is refused");
        json.append(field("adv_cert_uncertified_key_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(new SpaceWire.EntryStateDto(new EntryRecord(entryId, spaceId,
                        "GoldenVectors$ResearchTask#v1", payload, null, issuer, stamp, lease, Map.of(),
                        uncertifiedSig), identity.rawPublicKey(), adds, removes, stamp, lease, false,
                        stateSig, certificate), null, null)))));
        require(!Ed25519.verifyRaw(certificate.agentPublicKey(), viewBytes, recordSig),
                "peer-signed record is refused once a certificate is attached");
        json.append(field("adv_cert_peer_signed_record_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(new SpaceWire.EntryStateDto(record, identity.rawPublicKey(),
                        adds, removes, stamp, lease, false, stateSig, certificate), null, null)))));

        // An AgentCard for an attested agent (SPEC §6.1, §4.2; QA4 A4-7 phase 3):
        // agent_card_cbor's card with the appended agentPublicKey = agent_public_key_raw.
        // A card without the key is byte-identical to agent_card_cbor (the field is
        // omitted when absent), which the v0.1.10 conformance test still pins.
        AgentCard keyedCard = new AgentCard(card.id(), card.issuer(), card.group(), card.issued(),
                card.ttl(), card.agent(), card.description(), card.goals(), card.consumes(),
                card.produces(), card.costHints(), card.spaceBindings(), agent.publicKey());
        require(codec.toBytes(new AgentCard(card.id(), card.issuer(), card.group(), card.issued(),
                card.ttl(), card.agent(), card.description(), card.goals(), card.consumes(),
                card.produces(), card.costHints(), card.spaceBindings(), null)).length == cardBytes.length,
                "a card without an agent key is unchanged on the wire");
        json.append(field("agent_card_with_key_cbor", hex.formatHex(codec.toBytes(keyedCard))));
        json.append(field("agent_card_with_key_signature",
                hex.formatHex(new AdvertisementSigner(codec).sign(keyedCard, identity).signature())));

        // An agent-signed take claim (TECH-SPEC §7.6, QA4 A4-7 phase 3): the golden
        // claim (take_claim_cbor) signed by the agent key, with holderKey still the
        // peer key and the certificate appended; the same two-key rule as records.
        //   take_claim_agent_signature          Ed25519 by the agent key over take_claim_cbor
        //   claim_delta_with_certificate_cbor   Delta{claimEntry, claim: SignedClaim{claim,
        //       holderKey = public_key_raw, signature = take_claim_agent_signature,
        //       holderCertificate = agent_certificate_cbor}}
        // Hostile variants every implementation must drop, each decodable:
        //   adv_claim_cert_peer_signed_delta   certificate present, signature by the peer key
        //   adv_claim_cert_expired_delta       certificate lapsed at certificate_verify_at
        //   adv_claim_cert_wrong_holder_delta  certificate names peer_id/other, claim holder peer_id/python
        byte[] claimAgentSig = agent.sign(claimBytes);
        json.append(field("take_claim_agent_signature", hex.formatHex(claimAgentSig)));
        SpaceWire.SignedClaim certifiedClaim = new SpaceWire.SignedClaim(claim, identity.rawPublicKey(),
                claimAgentSig, certificate);
        require(certifiedClaim.holderAttested() && certifiedClaim.agentAttested(),
                "certified claim is holder- and agent-attested");
        json.append(field("claim_delta_with_certificate_cbor", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId, certifiedClaim)))));
        json.append(field("adv_claim_cert_peer_signed_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId, new SpaceWire.SignedClaim(claim,
                        identity.rawPublicKey(), identity.sign(claimBytes), certificate))))));
        json.append(field("adv_claim_cert_expired_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId, new SpaceWire.SignedClaim(claim,
                        identity.rawPublicKey(), claimAgentSig, expired))))));
        json.append(field("adv_claim_cert_wrong_holder_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId, new SpaceWire.SignedClaim(claim,
                        identity.rawPublicKey(), claimAgentSig, wrongAgent))))));

        // The sybil ballots (SPEC §8 QUORUM, QA4 A4-7 phase 2): one peer, the golden
        // key, three ballots under three self-asserted names for one proposal.
        // Every implementation's QUORUM tally counts them as ONE voter at PEER
        // granularity (the counted identity is the issuer's peer), and as none
        // at AGENT granularity (a peer-asserted name is not an attested agent).
        //   vote_three_names_one_peer_proposal  the proposalId the ballots name
        //   vote_three_names_one_peer_delta_1..3  Delta{state: EntryStateDto of a
        //       Ballot{proposalId, option "approve", voter = issuer} record, type
        //       ai.badmonkey.agentspaces.capabilities.vote.VoteCapability$Ballot#v1,
        //       issuer peer_id/one|two|three, peer-signed, lease WRITE for PT1H
        //       from hlc_encoded, one add dot (peer_id, 11|12|13)}
        String voteProposal = "p-golden";
        json.append(field("vote_three_names_one_peer_proposal", voteProposal));
        String ballotType = VoteCapability.Ballot.class.getName() + "#v1";
        String[] names = {"one", "two", "three"};
        for (int i = 0; i < names.length; i++) {
            AgentId sybil = identity.agent(names[i]);
            EntryId ballotEntry = EntryId.of("33333333-4444-5555-6666-77777777777" + (i + 1));
            byte[] ballotPayload = codec.toBytes(new VoteCapability.Ballot(voteProposal, "approve",
                    sybil.encoded()));
            LeaseInfo ballotLease = new LeaseInfo(sybil, stamp.physical() + Duration.ofHours(1).toMillis(),
                    LeaseKind.WRITE);
            byte[] ballotSig = identity.sign(codec.toBytes(new SignView(ballotEntry, spaceId, ballotType,
                    ballotPayload, null, sybil, stamp, Map.of())));
            EntryRecord ballotRecord = new EntryRecord(ballotEntry, spaceId, ballotType, ballotPayload,
                    null, sybil, stamp, ballotLease, Map.of(), ballotSig);
            List<Dot> ballotAdds = List.of(new Dot(self.value(), 11 + i));
            byte[] ballotStateSig = identity.sign(codec.toBytes(new StateSignView(spaceId, ballotEntry,
                    ballotAdds, List.of(), stamp, ballotLease, false)));
            json.append(field("vote_three_names_one_peer_delta_" + (i + 1), hex.formatHex(codec.toBytes(
                    new SpaceWire.Delta(new SpaceWire.EntryStateDto(ballotRecord, identity.rawPublicKey(),
                            ballotAdds, List.of(), stamp, ballotLease, false, ballotStateSig), null, null)))));
        }

        // ------------------------------------------------------------------
        // v0.1.13 wire structures (TODO-9-10-11 Phase 3). Every field below is
        // appended or new; every vector above keeps its exact bytes.

        // B-1: the v0.1.11 certified-record vectors carry a PEER-signed state on an
        // agent-attested entry, which v0.1.13 refuses (A6). They are kept under their
        // old names until the clients move (Phase 4) and published under hostile names:
        //   adv_legacy_state_on_attested_entry_dto_cbor    = entry_state_with_certificate_cbor
        //   adv_legacy_state_on_attested_entry_delta_cbor  = delta_with_certificate_cbor
        json.append(field("adv_legacy_state_on_attested_entry_dto_cbor",
                hex.formatHex(codec.toBytes(certifiedDto))));
        json.append(field("adv_legacy_state_on_attested_entry_delta_cbor",
                hex.formatHex(codec.toBytes(new SpaceWire.Delta(certifiedDto, null, null)))));

        // Agent-signed state transitions (SPEC §11a.4, v0.1.13): the agent signs its
        // own state; StateSignView appends signer and signedAt, and the DTO appends
        // stateCertificate, signer, signedAt. signedAt = hlc_encoded, signer =
        // peer_id/python, stateCertificate = agent_certificate_cbor.
        //   state_sign_view_agent_cbor     StateSignView{..., signer, signedAt}
        //   state_signature_agent_key      Ed25519 by the agent key over it
        //   entry_state_agent_signed_cbor  the certified record's DTO, agent-signed
        //   delta_agent_signed_cbor        the Delta carrying it (every receiver accepts)
        // Hostile variants, each decodable, each refused:
        //   adv_agent_state_peer_key_delta     agent-signed fields, stateSig by the peer key
        //   adv_agent_state_wrong_signer_delta signer names peer_id/other, signed by the agent key
        StateSignView agentStateView = new StateSignView(spaceId, entryId, adds, removes, stamp, lease,
                false, issuer.encoded(), stamp);
        byte[] agentStateViewBytes = codec.toBytes(agentStateView);
        byte[] agentStateSig = agent.sign(agentStateViewBytes);
        json.append(field("state_sign_view_agent_cbor", hex.formatHex(agentStateViewBytes)));
        json.append(field("state_signature_agent_key", hex.formatHex(agentStateSig)));
        SpaceWire.EntryStateDto agentSigned = certifiedDto.withAgentSigner(issuer.encoded(), stamp, certificate)
                .withStateSig(agentStateSig);
        json.append(field("entry_state_agent_signed_cbor", hex.formatHex(codec.toBytes(agentSigned))));
        json.append(field("delta_agent_signed_cbor",
                hex.formatHex(codec.toBytes(new SpaceWire.Delta(agentSigned, null, null)))));
        json.append(field("adv_agent_state_peer_key_delta", hex.formatHex(codec.toBytes(new SpaceWire.Delta(
                certifiedDto.withAgentSigner(issuer.encoded(), stamp, certificate)
                        .withStateSig(identity.sign(agentStateViewBytes)), null, null)))));
        String otherSigner = identity.agent("other").encoded();
        json.append(field("adv_agent_state_wrong_signer_delta", hex.formatHex(codec.toBytes(new SpaceWire.Delta(
                certifiedDto.withAgentSigner(otherSigner, stamp, certificate).withStateSig(agent.sign(
                        codec.toBytes(new StateSignView(spaceId, entryId, adds, removes, stamp, lease, false,
                                otherSigner, stamp)))), null, null)))));

        // Content-key epochs (SPEC §11a.1, §11a.3, v0.1.13): a record sealed under
        // epoch 2 names it in the appended keyEpoch, and its AAD appends the epoch.
        // The key and the sealed payload are reloaded so the bytes stay stable.
        //   content_key_epoch_2             the epoch-2 key, raw 32 bytes
        //   content_key_aad_epoch_2         the UTF-8 AAD: space_id|entryId|2
        //   key_epoch_sealed_payload        AES-256-GCM nonce||ciphertext||tag of task_payload_cbor
        //   sign_view_key_epoch_cbor        SignView{..., payload = the sealed payload, keyEpoch 2}
        //   entry_record_key_epoch_cbor     the EntryRecord, peer-signed, keyEpoch 2
        //   epoch_commitment_cbor           EpochCommitment{group_id, 2, "2025-01-01T00:00:00Z",
        //                                   sha-256(content_key_epoch_2)}: what the rotator signs
        //   epoch_proof_signature           Ed25519 by the golden peer over it
        //   wrap_binding_v2_cbor            WrapBinding{group_id, holder = peer_id, requester =
        //                                   adv_forger's PeerID, nonce 7, epoch 2, rotator = peer_id,
        //                                   agent = peer_id/python}
        ai.badmonkey.agentspaces.common.crypto.GroupKey epochTwo =
                ai.badmonkey.agentspaces.common.crypto.GroupKey.fromBytes(previous.containsKey("content_key_epoch_2")
                        ? hex.parseHex((String) previous.get("content_key_epoch_2"))
                        : ai.badmonkey.agentspaces.common.crypto.GroupKey.generate().rawBytes());
        String aad = spaceId.value() + "|" + entryId.value() + "|2";
        byte[] sealedPayload = previous.containsKey("key_epoch_sealed_payload")
                ? hex.parseHex((String) previous.get("key_epoch_sealed_payload"))
                : epochTwo.encrypt(payload, aad.getBytes(StandardCharsets.UTF_8));
        require(Arrays.equals(epochTwo.decrypt(sealedPayload, aad.getBytes(StandardCharsets.UTF_8)).orElseThrow(),
                payload), "the epoch-2 payload opens under its AAD");
        json.append(field("content_key_epoch_2", hex.formatHex(epochTwo.rawBytes())));
        json.append(field("content_key_aad_epoch_2", aad));
        json.append(field("key_epoch_sealed_payload", hex.formatHex(sealedPayload)));
        byte[] epochViewBytes = codec.toBytes(new SignView(entryId, spaceId, "GoldenVectors$ResearchTask#v1",
                sealedPayload, null, issuer, stamp, Map.of(), 2L));
        json.append(field("sign_view_key_epoch_cbor", hex.formatHex(epochViewBytes)));
        json.append(field("entry_record_key_epoch_cbor", hex.formatHex(codec.toBytes(new EntryRecord(entryId,
                spaceId, "GoldenVectors$ResearchTask#v1", sealedPayload, null, issuer, stamp, lease, Map.of(),
                identity.sign(epochViewBytes), 2L)))));
        byte[] commitment = codec.toBytes(new EpochCommitment(group.value(), 2L, "2025-01-01T00:00:00Z",
                Digests.sha256(epochTwo.rawBytes())));
        json.append(field("epoch_commitment_cbor", hex.formatHex(commitment)));
        json.append(field("epoch_proof_signature", hex.formatHex(identity.sign(commitment))));
        json.append(field("wrap_binding_v2_cbor", hex.formatHex(codec.toBytes(new WrapBinding(group.value(),
                self.value(), forger.peerId().value(), 7L, 2L, self.value(), issuer.encoded())))));

        // An agent certificate that also certifies the agent's X25519 key (SPEC
        // §11a.2a, v0.1.13): the body appends encryptionPublicKey. Reloaded key.
        //   agent_encryption_private_key_pkcs8 / agent_encryption_public_key_raw
        //   agent_certificate_with_encryption_unsigned_cbor  the body the peer signs
        //   agent_certificate_with_encryption_signature      the peer's signature
        //   agent_certificate_with_encryption_cbor           the signed certificate
        java.security.KeyPair agentEncryption;
        byte[] agentEncryptionPkcs8;
        if (previous.containsKey("agent_encryption_private_key_pkcs8")) {
            agentEncryptionPkcs8 = hex.parseHex((String) previous.get("agent_encryption_private_key_pkcs8"));
            java.security.KeyFactory x25519 = java.security.KeyFactory.getInstance("X25519");
            agentEncryption = new java.security.KeyPair(
                    ai.badmonkey.agentspaces.common.crypto.X25519.publicKeyFromRaw(
                            hex.parseHex((String) previous.get("agent_encryption_public_key_raw"))),
                    x25519.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(agentEncryptionPkcs8)));
        } else {
            agentEncryption = ai.badmonkey.agentspaces.common.crypto.X25519.generate();
            agentEncryptionPkcs8 = agentEncryption.getPrivate().getEncoded();
        }
        byte[] agentEncryptionRaw = ai.badmonkey.agentspaces.common.crypto.X25519.rawPublicKey(
                agentEncryption.getPublic());
        AgentCertificate encryptionCertificate = certificates.sign(new AgentCertificate(issuer, agent.publicKey(),
                verifyAt, Duration.ofHours(24), null, agentEncryptionRaw), identity);
        require(certificates.verify(encryptionCertificate, identity.rawPublicKey(), issuer, verifyAt),
                "the encryption-key certificate verifies");
        json.append(field("agent_encryption_private_key_pkcs8", hex.formatHex(agentEncryptionPkcs8)));
        json.append(field("agent_encryption_public_key_raw", hex.formatHex(agentEncryptionRaw)));
        json.append(field("agent_certificate_with_encryption_unsigned_cbor",
                hex.formatHex(codec.toBytes(encryptionCertificate.unsigned()))));
        json.append(field("agent_certificate_with_encryption_signature",
                hex.formatHex(encryptionCertificate.peerSignature())));
        json.append(field("agent_certificate_with_encryption_cbor",
                hex.formatHex(codec.toBytes(encryptionCertificate))));

        // Credential revocations (SPEC §6.1, v0.1.13), issued by the golden peer
        // (the agent's own peer) in the legacy golden group, as the
        // credential-revocations stream carries them (SignedRevocation{adBytes,
        // publicKey, signature}).
        //   credential_revocation_agent_cbor         target AGENT peer_id/python, reason
        //       retired, issued 2026-01-01T00:00:00Z, ttl P30D, effectiveFrom an hour earlier
        //   signed_credential_revocation_agent_cbor  its signed form
        //   credential_revocation_agent_key_cbor     target AGENT_KEY peer_id/python #
        //       sha-256(agent_public_key_raw), reason key-compromise, no effectiveFrom
        //   signed_credential_revocation_agent_key_cbor
        ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target agentTarget =
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.agent(issuer);
        ai.badmonkey.agentspaces.api.ad.CredentialRevocation agentRevocation =
                new ai.badmonkey.agentspaces.api.ad.CredentialRevocation(
                        ai.badmonkey.agentspaces.api.ad.CredentialRevocation.idFor(group, agentTarget), self, group,
                        issued, Duration.ofDays(30), agentTarget,
                        ai.badmonkey.agentspaces.api.ad.CredentialRevocation.RETIRED,
                        issued.minus(Duration.ofHours(1)), null);
        ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target keyTarget =
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.agentKey(issuer,
                        Digests.sha256(agent.publicKey()));
        ai.badmonkey.agentspaces.api.ad.CredentialRevocation keyRevocation =
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.of(self, group, issued, Duration.ofDays(30),
                        keyTarget, ai.badmonkey.agentspaces.api.ad.CredentialRevocation.KEY_COMPROMISE);
        for (Map.Entry<String, ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revocation : Map.of(
                "agent", agentRevocation, "agent_key", keyRevocation).entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).toList()) {
            byte[] revocationBytes = codec.toBytes(revocation.getValue());
            json.append(field("credential_revocation_" + revocation.getKey() + "_cbor", hex.formatHex(revocationBytes)));
            json.append(field("signed_credential_revocation_" + revocation.getKey() + "_cbor", hex.formatHex(
                    codec.toBytes(new ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation(
                            revocationBytes, identity.rawPublicKey(), identity.sign(revocationBytes))))));
        }

        // A card that declares its actions and carries its agent's certificate
        // (SPEC §6.1, v0.1.13), field order agentPublicKey, actions, agentCertificate:
        // agent_card_with_key_cbor's card with one action and agent_certificate_cbor,
        // issued at certificate_verify_at: a card's certificate must cover the
        // card's own issue time (SPEC §4.2), so a receiver judges it there.
        //   agent_card_with_actions_cbor       the card
        //   agent_card_with_actions_signature  Ed25519 over it
        AgentCard actionCard = new AgentCard(card.id(), card.issuer(), card.group(), verifyAt, card.ttl(),
                card.agent(), card.description(), card.goals(), card.consumes(), card.produces(),
                card.costHints(), card.spaceBindings(), agent.publicKey()).withActions(List.of(
                        new ai.badmonkey.agentspaces.api.ad.CardAction("research",
                                "Researches a topic and writes a finding", List.of(taskSchema),
                                List.of("GoldenVectors$Finding#v1"), "tasks",
                                ai.badmonkey.agentspaces.api.ad.CardAction.TAKE)))
                .withAgentCertificate(certificate);
        AdCache cardCache = new AdCache(codec, InstantSource.fixed(verifyAt));
        require(cardCache.accept(cardCache.toStored(actionCard, identity.rawPublicKey(),
                new AdvertisementSigner(codec).sign(actionCard, identity).signature())).isPresent(),
                "the attested card with actions is admitted at its issue time");
        json.append(field("agent_card_with_actions_cbor", hex.formatHex(codec.toBytes(actionCard))));
        json.append(field("agent_card_with_actions_signature",
                hex.formatHex(new AdvertisementSigner(codec).sign(actionCard, identity).signature())));

        // ------------------------------------------------------------------
        // v0.1.13 audit item 10: the remaining v0.1.13 transitions and their
        // hostile twins. Every vector above keeps its exact bytes; these are new.
        long t0 = stamp.physical();

        // Agent-signed renew, cancel, and complete (SPEC §11a.4, v0.1.13), each on
        // the certified golden record (record_signature_agent_key), each signed by
        // the agent key under agent_certificate_cbor, as ReplicatedSpace.signedDto
        // emits them (signer = peer_id/python; signedAt one logical tick after the
        // transition's own stamp, as hlc.now() after the transition gives).
        //   state_sign_view_agent_renew_cbor  StateSignView{adds [(peer_id,1)], removes [],
        //       leaseStamp t0+PT1M (logical 0), lease {python, t0+PT2H, WRITE}, completed
        //       false, signer, signedAt t0+PT1M (logical 1)}
        //   entry_state_agent_renew_cbor / delta_agent_renew_cbor  its DTO and Delta
        //   state_sign_view_agent_cancel_cbor  the write's lease, removes = adds
        //       (withObservedRemoved), signedAt t0+PT2M
        //   entry_state_agent_cancel_cbor / delta_agent_cancel_cbor
        //   state_sign_view_agent_complete_cbor  the write's lease, completed true,
        //       signedAt t0+PT3M; signed by the claim holder (peer_id/python) whose
        //       proof is claim_delta_with_certificate_cbor's agent-signed claim
        //   entry_state_agent_complete_cbor  the completed DTO: its stateCertificate
        //       is the holder's certificate (agent_certificate_cbor)
        //   delta_agent_complete_cbor  Delta{state, claimEntry, claim = the certified
        //       claim}: the completion travels with the claim that authorizes it
        Map<String, Object[]> transitions = new java.util.LinkedHashMap<>();
        transitions.put("renew", new Object[] {List.of(new Dot(self.value(), 1)), List.of(),
                new HlcTimestamp(t0 + 60_000L, 0, self.value()),
                new LeaseInfo(issuer, t0 + Duration.ofHours(2).toMillis(), LeaseKind.WRITE), false,
                new HlcTimestamp(t0 + 60_000L, 1, self.value())});
        transitions.put("cancel", new Object[] {List.of(new Dot(self.value(), 1)),
                List.of(new Dot(self.value(), 1)), stamp, lease, false,
                new HlcTimestamp(t0 + 120_000L, 0, self.value())});
        transitions.put("complete", new Object[] {List.of(new Dot(self.value(), 1)), List.of(), stamp,
                lease, true, new HlcTimestamp(t0 + 180_000L, 0, self.value())});
        for (Map.Entry<String, Object[]> transition : transitions.entrySet()) {
            Object[] t = transition.getValue();
            @SuppressWarnings("unchecked") List<Dot> tAdds = (List<Dot>) t[0];
            @SuppressWarnings("unchecked") List<Dot> tRemoves = (List<Dot>) t[1];
            HlcTimestamp tStamp = (HlcTimestamp) t[2];
            LeaseInfo tLease = (LeaseInfo) t[3];
            boolean tCompleted = (Boolean) t[4];
            HlcTimestamp tSignedAt = (HlcTimestamp) t[5];
            byte[] tView = codec.toBytes(new StateSignView(spaceId, entryId, tAdds, tRemoves, tStamp, tLease,
                    tCompleted, issuer.encoded(), tSignedAt));
            byte[] tSig = agent.sign(tView);
            require(Ed25519.verifyRaw(certificate.agentPublicKey(), tView, tSig)
                    && certificate.covers(Instant.ofEpochMilli(tSignedAt.physical())),
                    transition.getKey() + " state verifies under the agent key at its signing time");
            SpaceWire.EntryStateDto tDto = new SpaceWire.EntryStateDto(agentRecord, identity.rawPublicKey(),
                    tAdds, tRemoves, tStamp, tLease, tCompleted, null, certificate)
                    .withAgentSigner(issuer.encoded(), tSignedAt, certificate).withStateSig(tSig);
            json.append(field("state_sign_view_agent_" + transition.getKey() + "_cbor", hex.formatHex(tView)));
            json.append(field("entry_state_agent_" + transition.getKey() + "_cbor",
                    hex.formatHex(codec.toBytes(tDto))));
            json.append(field("delta_agent_" + transition.getKey() + "_cbor", hex.formatHex(codec.toBytes(
                    tCompleted ? new SpaceWire.Delta(tDto, entryId, certifiedClaim)
                            : new SpaceWire.Delta(tDto, null, null)))));
        }

        // Hostile signing times (SPEC §4.2 / §11, v0.1.13): a certificate is judged
        // at the signing time of what it certifies, which must lie in its window and
        // lead the receiver's clock (certificate_verify_at) by at most the HLC drift
        // ceiling (HybridLogicalClock.MAX_DRIFT_MILLIS, 600000 ms); a claim's stamp
        // may predate its entry's issue stamp by at most the same allowance
        // (ReplicatedSpace.MAX_CLAIM_PREDATES_ISSUE_MILLIS). Each decodes and is
        // validly signed by the right key; only the stamp is wrong.
        //   adv_agent_state_out_of_window_delta  the agent-signed write state with
        //       signedAt t0-PT1H, before agent_certificate_cbor's window opens
        //   adv_agent_state_drift_delta          signedAt t0+PT11M: inside the window,
        //       but beyond the drift ceiling from the receiver's clock
        //   adv_claim_cert_out_of_window_delta   the agent-signed claim re-stamped
        //       t0-PT1H (outside the certificate's window), certificate attached
        //   adv_claim_predates_issue_delta       a peer-signed claim stamped
        //       t0-600001 ms: one millisecond past the allowance before the golden
        //       record's issue stamp (a receiver holding delta_cbor refuses it)
        long drift = ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock.MAX_DRIFT_MILLIS;
        Map<String, HlcTimestamp> hostileSignedAt = new java.util.LinkedHashMap<>();
        hostileSignedAt.put("adv_agent_state_out_of_window_delta",
                new HlcTimestamp(t0 - Duration.ofHours(1).toMillis(), 0, self.value()));
        hostileSignedAt.put("adv_agent_state_drift_delta",
                new HlcTimestamp(t0 + drift + 60_000L, 0, self.value()));
        for (Map.Entry<String, HlcTimestamp> hostile : hostileSignedAt.entrySet()) {
            HlcTimestamp at = hostile.getValue();
            require(certificates.verifyAt(certificate, identity.rawPublicKey(), issuer,
                    Instant.ofEpochMilli(at.physical()), verifyAt) == false,
                    hostile.getKey() + " is refused at certificate_verify_at");
            byte[] hView = codec.toBytes(new StateSignView(spaceId, entryId, adds, removes, stamp, lease, false,
                    issuer.encoded(), at));
            json.append(field(hostile.getKey(), hex.formatHex(codec.toBytes(new SpaceWire.Delta(
                    certifiedDto.withAgentSigner(issuer.encoded(), at, certificate)
                            .withStateSig(agent.sign(hView)), null, null)))));
        }
        require(certificate.covers(Instant.ofEpochMilli(t0 + drift + 60_000L)),
                "the drift vector's signing time is inside the window, so only the drift refuses it");
        ai.badmonkey.agentspaces.space.replicated.TakeClaim earlyClaim =
                new ai.badmonkey.agentspaces.space.replicated.TakeClaim(entryId, spaceId, 1,
                        new HlcTimestamp(t0 - Duration.ofHours(1).toMillis(), 0, self.value()), issuer, 0.0,
                        1735693200000L);
        require(!certificate.covers(Instant.ofEpochMilli(earlyClaim.stamp().physical())),
                "the early claim's stamp is outside the certificate's window");
        json.append(field("adv_claim_cert_out_of_window_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId, new SpaceWire.SignedClaim(earlyClaim,
                        identity.rawPublicKey(), agent.sign(codec.toBytes(earlyClaim)), certificate))))));
        ai.badmonkey.agentspaces.space.replicated.TakeClaim predating =
                new ai.badmonkey.agentspaces.space.replicated.TakeClaim(entryId, spaceId, 1,
                        new HlcTimestamp(t0 - drift - 1, 0, self.value()), issuer, 0.0, 1735693200000L);
        json.append(field("adv_claim_predates_issue_delta", hex.formatHex(codec.toBytes(
                new SpaceWire.Delta(null, entryId, new SpaceWire.SignedClaim(predating,
                        identity.rawPublicKey(), identity.sign(codec.toBytes(predating))))))));

        // A relabelled key epoch (SPEC §11a.1/§11a.3, v0.1.13): keyEpoch is signed in
        // the SignView, so a record moved to another epoch after signing must not verify.
        //   delta_key_epoch_cbor                  entry_record_key_epoch_cbor under the
        //       golden peer-signed state (state_signature): the control, which lands
        //   adv_key_epoch_relabelled_record_cbor  the same record with keyEpoch 3 and the
        //       epoch-2 signature
        //   adv_key_epoch_relabelled_delta        the Delta carrying it
        EntryRecord epochRecord = new EntryRecord(entryId, spaceId, "GoldenVectors$ResearchTask#v1", sealedPayload,
                null, issuer, stamp, lease, Map.of(), identity.sign(epochViewBytes), 2L);
        EntryRecord relabelled = new EntryRecord(entryId, spaceId, "GoldenVectors$ResearchTask#v1", sealedPayload,
                null, issuer, stamp, lease, Map.of(), epochRecord.sig(), 3L);
        require(!Ed25519.verifyRaw(identity.rawPublicKey(), codec.toBytes(new SignView(entryId, spaceId,
                "GoldenVectors$ResearchTask#v1", sealedPayload, null, issuer, stamp, Map.of(), 3L)),
                relabelled.sig()), "the relabelled record does not verify");
        json.append(field("delta_key_epoch_cbor", hex.formatHex(codec.toBytes(new SpaceWire.Delta(
                new SpaceWire.EntryStateDto(epochRecord, identity.rawPublicKey(), adds, removes, stamp, lease,
                        false, stateSig), null, null)))));
        json.append(field("adv_key_epoch_relabelled_record_cbor", hex.formatHex(codec.toBytes(relabelled))));
        json.append(field("adv_key_epoch_relabelled_delta", hex.formatHex(codec.toBytes(new SpaceWire.Delta(
                new SpaceWire.EntryStateDto(relabelled, identity.rawPublicKey(), adds, removes, stamp, lease,
                        false, stateSig), null, null)))));

        // Key wraps (SPEC §11a.2, WrapBinding v2, v0.1.13): content_key_epoch_2 sealed
        // to the agent's X25519 key (agent_encryption_public_key_raw) as
        // GroupKeyWrap.WrappedKey {ephemeralPublicKey, sealed}. The ephemeral key is
        // random, so each wrap is reloaded from the previous file.
        //   key_wrap_v2_cbor  sealed under wrap_binding_v2_cbor: the requester, opening
        //       under the binding it expects (wrap_binding_v2_cbor), recovers the key
        //   adv_key_wrap_wrong_group_cbor / adv_key_wrap_wrong_group_binding_cbor
        //       sealed under the binding with group = founding_group_id
        //   adv_key_wrap_wrong_epoch_cbor / ..._binding_cbor     epoch 3
        //   adv_key_wrap_wrong_rotator_cbor / ..._binding_cbor   rotator = adv_forger's PeerID
        // Each hostile wrap opens under its own binding (only the binding is wrong) and
        // under wrap_binding_v2_cbor does not open.
        byte[] expectedBinding = codec.toBytes(new WrapBinding(group.value(), self.value(),
                forger.peerId().value(), 7L, 2L, self.value(), issuer.encoded()));
        java.util.function.UnaryOperator<byte[]> agentAgree =
                remote -> ai.badmonkey.agentspaces.common.crypto.X25519.agree(agentEncryption.getPrivate(), remote);
        Map<String, byte[]> wraps = new java.util.LinkedHashMap<>();
        wraps.put("key_wrap_v2_cbor", expectedBinding);
        wraps.put("adv_key_wrap_wrong_group_cbor", codec.toBytes(new WrapBinding(foundingGroup.value(),
                self.value(), forger.peerId().value(), 7L, 2L, self.value(), issuer.encoded())));
        wraps.put("adv_key_wrap_wrong_epoch_cbor", codec.toBytes(new WrapBinding(group.value(), self.value(),
                forger.peerId().value(), 7L, 3L, self.value(), issuer.encoded())));
        wraps.put("adv_key_wrap_wrong_rotator_cbor", codec.toBytes(new WrapBinding(group.value(), self.value(),
                forger.peerId().value(), 7L, 2L, forger.peerId().value(), issuer.encoded())));
        for (Map.Entry<String, byte[]> wrap : wraps.entrySet()) {
            byte[] wrapBytes = previous.containsKey(wrap.getKey())
                    ? hex.parseHex((String) previous.get(wrap.getKey()))
                    : codec.toBytes(ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap.wrapBound(epochTwo,
                            agentEncryptionRaw, wrap.getValue()));
            ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap.WrappedKey wrapped = codec.fromBytes(wrapBytes,
                    ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap.WrappedKey.class);
            require(ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap.unwrapBound(wrapped, agentAgree,
                    agentEncryptionRaw, wrap.getValue()).map(k -> Arrays.equals(k.rawBytes(), epochTwo.rawBytes()))
                    .orElse(false), wrap.getKey() + " opens under its own binding");
            boolean opensAsExpected = ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap.unwrapBound(wrapped,
                    agentAgree, agentEncryptionRaw, expectedBinding).isPresent();
            require(opensAsExpected == wrap.getKey().equals("key_wrap_v2_cbor"),
                    wrap.getKey() + " opens under the expected binding only when it was sealed under it");
            json.append(field(wrap.getKey(), hex.formatHex(wrapBytes)));
            if (!wrap.getKey().equals("key_wrap_v2_cbor")) {
                json.append(field(wrap.getKey().replace("_cbor", "_binding_cbor"), hex.formatHex(wrap.getValue())));
            }
        }

        // Credential revocations of the other two target kinds (SPEC §6.1, v0.1.13),
        // issued by the golden peer, which only a group FOUNDER may do for these kinds:
        // a receiver accepts them when its group advertisement names peer_id as the
        // founder (issuer), and refuses them otherwise. Issued 2026-01-01T00:00:00Z,
        // ttl P30D, no effectiveFrom.
        //   credential_revocation_x509_leaf_cbor  target X509_LEAF {x509Issuer
        //       "CN=golden-leaf,O=Bad Monkey", x509Serial "0a1b2c3d",
        //       certificateFingerprint sha-256("golden-leaf-der")}, reason superseded
        //   credential_revocation_join_credential_cbor  target JOIN_CREDENTIAL
        //       {credentialHash sha-256("golden-join-credential")}, reason privilege-withdrawn
        //   signed_credential_revocation_<kind>_cbor  their signed forms
        // A cross-issuer revocation every receiver refuses, whoever founded the group
        // (unless adv_forger founded it): adv_forger revokes peer_id/python, an agent it
        // does not host, signed with adv_forger's own key.
        //   adv_credential_revocation_cross_issuer_cbor  signed form; target AGENT
        //       peer_id/python, reason key-compromise, issuer adv_forger's PeerID
        Map<String, ai.badmonkey.agentspaces.api.ad.CredentialRevocation> founderRevocations =
                new java.util.LinkedHashMap<>();
        founderRevocations.put("x509_leaf", ai.badmonkey.agentspaces.api.ad.CredentialRevocation.of(self, group,
                issued, Duration.ofDays(30), ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.x509Leaf(
                        "CN=golden-leaf,O=Bad Monkey", "0a1b2c3d",
                        Digests.sha256("golden-leaf-der".getBytes(StandardCharsets.UTF_8))),
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.SUPERSEDED));
        founderRevocations.put("join_credential", ai.badmonkey.agentspaces.api.ad.CredentialRevocation.of(self,
                group, issued, Duration.ofDays(30),
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.joinCredential(
                        Digests.sha256("golden-join-credential".getBytes(StandardCharsets.UTF_8))),
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.PRIVILEGE_WITHDRAWN));
        for (Map.Entry<String, ai.badmonkey.agentspaces.api.ad.CredentialRevocation> revocation
                : founderRevocations.entrySet()) {
            byte[] revocationBytes = codec.toBytes(revocation.getValue());
            json.append(field("credential_revocation_" + revocation.getKey() + "_cbor",
                    hex.formatHex(revocationBytes)));
            json.append(field("signed_credential_revocation_" + revocation.getKey() + "_cbor", hex.formatHex(
                    codec.toBytes(new ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation(
                            revocationBytes, identity.rawPublicKey(), identity.sign(revocationBytes))))));
        }
        byte[] crossIssuer = codec.toBytes(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.of(forger.peerId(),
                group, issued, Duration.ofDays(30), agentTarget,
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.KEY_COMPROMISE));
        json.append(field("adv_credential_revocation_cross_issuer_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation(
                        crossIssuer, forger.rawPublicKey(), forger.sign(crossIssuer))))));

        // A record under a renewed certificate (SPEC §4.2, v0.1.13): the agent key
        // (agent_private_key_pkcs8) as PeerIdentity.renewingSubordinate("python", ttl
        // PT24H) on a clock that starts at certificate_verify_at (its first certificate
        // is agent_certificate_cbor, byte for byte), moves to +PT13H (half-life passed:
        // the peer re-issues), then to renewed_certificate_signed_at = +PT24H30M, where
        // the agent writes. The first certificate has lapsed there; the renewed one
        // (issued +PT13H, until +PT37H) covers it, and a receiver judges it at that
        // signing time, so the record still verifies after the renewed one lapses too.
        //   renewed_agent_certificate_cbor  the renewed certificate
        //   renewed_certificate_signed_at   the write's instant (ISO-8601)
        //   sign_view_renewed_certificate_cbor  SignView{entry 44444444-5555-6666-7777-
        //       888888888888, space_id, ResearchTask, task_payload_cbor, issuer
        //       peer_id/python, issued (signed_at, logical 0, node peer_id), tags {}}
        //   entry_state_renewed_certificate_cbor  the DTO: record signed by the agent key,
        //       lease {python, signed_at+PT1H, WRITE}, adds [(peer_id, 21)], agent-signed
        //       state (signedAt logical 1), agentCertificate = stateCertificate = renewed
        //   delta_renewed_certificate_cbor  the Delta carrying it
        Instant[] renewalClock = {verifyAt};
        AgentIdentity renewing = identity.renewingSubordinate("python", agentKeys, Duration.ofHours(24),
                () -> renewalClock[0]);
        require(Arrays.equals(codec.toBytes(renewing.certificate().orElseThrow()), codec.toBytes(certificate)),
                "the renewing agent's first certificate is agent_certificate_cbor");
        renewalClock[0] = verifyAt.plus(Duration.ofHours(13));
        renewing.renewIfDue(renewalClock[0]);
        Instant renewedAt = verifyAt.plus(Duration.ofHours(24)).plus(Duration.ofMinutes(30));
        renewalClock[0] = renewedAt;
        HlcTimestamp renewedStamp = new HlcTimestamp(renewedAt.toEpochMilli(), 0, self.value());
        HlcTimestamp renewedSignedAt = new HlcTimestamp(renewedAt.toEpochMilli(), 1, self.value());
        AgentCertificate renewed = renewing.certificateCovering(renewedAt).orElseThrow();
        require(renewed.issued().equals(verifyAt.plus(Duration.ofHours(13))) && !certificate.covers(renewedAt),
                "the covering certificate is the renewal, and the first one has lapsed");
        EntryId renewedEntry = EntryId.of("44444444-5555-6666-7777-888888888888");
        byte[] renewedView = codec.toBytes(new SignView(renewedEntry, spaceId, "GoldenVectors$ResearchTask#v1",
                payload, null, issuer, renewedStamp, Map.of()));
        LeaseInfo renewedLease = new LeaseInfo(issuer, renewedAt.plus(Duration.ofHours(1)).toEpochMilli(),
                LeaseKind.WRITE);
        EntryRecord renewedRecord = new EntryRecord(renewedEntry, spaceId, "GoldenVectors$ResearchTask#v1",
                payload, null, issuer, renewedStamp, renewedLease, Map.of(), renewing.sign(renewedView));
        List<Dot> renewedAdds = List.of(new Dot(self.value(), 21));
        byte[] renewedStateView = codec.toBytes(new StateSignView(spaceId, renewedEntry, renewedAdds, List.of(),
                renewedStamp, renewedLease, false, issuer.encoded(), renewedSignedAt));
        SpaceWire.EntryStateDto renewedDto = new SpaceWire.EntryStateDto(renewedRecord, identity.rawPublicKey(),
                renewedAdds, List.of(), renewedStamp, renewedLease, false, null, renewed)
                .withAgentSigner(issuer.encoded(), renewedSignedAt, renewed)
                .withStateSig(renewing.sign(renewedStateView));
        Instant longAfter = renewedAt.plus(Duration.ofHours(30));
        require(certificates.verifyAt(renewed, identity.rawPublicKey(), issuer, renewedAt, longAfter)
                && !certificates.verify(renewed, identity.rawPublicKey(), issuer, longAfter)
                && Ed25519.verifyRaw(renewed.agentPublicKey(), renewedView, renewedRecord.sig()),
                "the renewed-certificate record verifies at its signing time, after the certificate lapsed");
        json.append(field("renewed_agent_certificate_cbor", hex.formatHex(codec.toBytes(renewed))));
        json.append(field("renewed_certificate_signed_at", renewedAt.toString()));
        json.append(field("sign_view_renewed_certificate_cbor", hex.formatHex(renewedView)));
        json.append(field("entry_state_renewed_certificate_cbor", hex.formatHex(codec.toBytes(renewedDto))));
        json.append(field("delta_renewed_certificate_cbor",
                hex.formatHex(codec.toBytes(new SpaceWire.Delta(renewedDto, null, null)))));

        // The DTLS recovery sublayer's PDUs (SPEC annex D, SRAD dtls-support appendix B), laid out
        // byte by byte from the annex rather than through the binding's codec, so a second
        // implementation checks its encoder against the text.
        java.nio.ByteBuffer ackBlock = java.nio.ByteBuffer.allocate(20)
                .putInt(100).putInt(250).putShort((short) 2).putShort((short) 10)
                .putShort((short) 8).putShort((short) 10).putShort((short) 63).putShort((short) 0);
        json.append(field("dtls_pdu_data", hex.formatHex(java.nio.ByteBuffer.allocate(22)
                .put(new byte[]{1, 1, 0, 0}).putInt(7).putInt(3).putShort((short) 1).putShort((short) 4)
                .put("aspace".getBytes(StandardCharsets.US_ASCII)).array())));
        json.append(field("dtls_pdu_data_with_ack", hex.formatHex(java.nio.ByteBuffer.allocate(37)
                .put(new byte[]{1, 1, 1, 0}).putInt(8).putInt(4).putShort((short) 0).putShort((short) 1)
                .put(ackBlock.array()).put((byte) 'x').array())));
        json.append(field("dtls_pdu_ack_three_ranges", hex.formatHex(java.nio.ByteBuffer.allocate(24)
                .put(new byte[]{1, 2, 0, 0}).put(ackBlock.array()).array())));
        // The portable field filter grammar (SPEC §7.2, srad/agentspaces-js.md JS-V-03): each case is
        // a clause, a value as JSON, and the verdict the Java FieldFilter gives; Python and TypeScript
        // must agree on every one.
        json.append(filterGrammarCases());
        json.append(sessionVectors(codec, hex));
        json.append(raftVectors(codec, hex));
        json.append(rtcVectors(codec, hex, wire.encode(ping, identity)));
        json.append(connectVectors(codec, hex));
        json.append(field("dtls_pdu_ping", "01030000"));
        json.append(field("dtls_pdu_pong", "01040000"));
        json.append(field("dtls_pdu_close_idle", "01050300"));

        // The sealed beacon datagram (SPEC §10.1 v0.1.19): HKDF-SHA-256 keys from a fixed
        // content key, the epoch tag, and one datagram sealed under a fixed nonce, computed
        // with the JDK's primitives rather than the node's BeaconSeal.
        byte[] beaconContentKey = new byte[32];
        java.util.Arrays.fill(beaconContentKey, (byte) 3);
        long beaconSecond = 1_792_320_000L;
        javax.crypto.Mac hkdf = javax.crypto.Mac.getInstance("HmacSHA256");
        hkdf.init(new javax.crypto.spec.SecretKeySpec(new byte[32], "HmacSHA256"));
        byte[] prk = hkdf.doFinal(beaconContentKey);
        hkdf.init(new javax.crypto.spec.SecretKeySpec(prk, "HmacSHA256"));
        hkdf.update("aspace-beacon-v1".getBytes(StandardCharsets.US_ASCII));
        hkdf.update((byte) 1);
        byte[] t1 = hkdf.doFinal();
        hkdf.update(t1);
        hkdf.update("aspace-beacon-v1".getBytes(StandardCharsets.US_ASCII));
        hkdf.update((byte) 2);
        byte[] t2 = hkdf.doFinal();
        javax.crypto.Mac tagMac = javax.crypto.Mac.getInstance("HmacSHA256");
        tagMac.init(new javax.crypto.spec.SecretKeySpec(t1, "HmacSHA256"));
        tagMac.update("aspace-beacon-tag".getBytes(StandardCharsets.US_ASCII));
        tagMac.update(java.nio.ByteBuffer.allocate(8).putLong(Math.floorDiv(beaconSecond, 300)).array());
        byte[] beaconTag = java.util.Arrays.copyOf(tagMac.doFinal(), 16);
        byte[] beaconNonce = new byte[12];
        for (int i = 0; i < 12; i++) {
            beaconNonce[i] = (byte) i;
        }
        byte[] beaconPlain = "a signed self-advertisement".getBytes(StandardCharsets.US_ASCII);
        javax.crypto.Cipher gcm = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
        gcm.init(javax.crypto.Cipher.ENCRYPT_MODE, new javax.crypto.spec.SecretKeySpec(t2, "AES"),
                new javax.crypto.spec.GCMParameterSpec(128, beaconNonce));
        gcm.updateAAD("ASB2".getBytes(StandardCharsets.US_ASCII));
        gcm.updateAAD(beaconTag);
        byte[] beaconSealed = gcm.doFinal(beaconPlain);
        json.append(field("beacon_seal_content_key", hex.formatHex(beaconContentKey)));
        json.append(field("beacon_seal_epoch_second", Long.toString(beaconSecond)));
        json.append(field("beacon_seal_tag", hex.formatHex(beaconTag)));
        json.append(field("beacon_seal_plaintext", hex.formatHex(beaconPlain)));
        json.append(field("beacon_seal_datagram", hex.formatHex(java.nio.ByteBuffer.allocate(32 + beaconSealed.length)
                .put("ASB2".getBytes(StandardCharsets.US_ASCII)).put(beaconTag).put(beaconNonce).put(beaconSealed)
                .array())));

        // The envelope version every frame carries and every receiver checks first (SPEC §9).
        json.append(number("wire_version", WireCodec.WIRE_VERSION));

        json.setLength(json.length() - 2); // trailing comma
        json.append("\n}\n");
        Files.createDirectories(out.getParent());
        Files.writeString(out, json.toString());
        System.out.println("wrote " + out + " (" + json.length() + " bytes)");
    }

    /** A Delta-shaped raw map around a hostile claim map, self-signed so only
     * the intended defect (NaN bid, zero epoch) causes the rejection. */
    private static Map<String, Object> rawDelta(EntryId claimEntry,
                                                Map<String, Object> claim,
                                                PeerIdentity identity, CborCodec codec) {
        java.util.LinkedHashMap<String, Object> signed = new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        signed.put("claim", claim);
        signed.put("holderKey", identity.rawPublicKey());
        signed.put("signature", identity.sign(codec.toBytes(claim)));
        java.util.LinkedHashMap<String, Object> delta = new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        delta.put("state", null);
        delta.put("claimEntry", claimEntry.value());
        delta.put("claim", signed);
        return delta;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadPrevious(Path out) throws java.io.IOException {
        if (!Files.exists(out)) {
            return Map.of();
        }
        return new ObjectMapper().readValue(out.toFile(), Map.class);
    }

    /**
     * Fails the run when a mirror record here no longer matches the private
     * record it stands in for: same component names, same order, and the same
     * simple type names (nested private types compare by simple name).
     */
    private static void assertMirrors(Class<?> owner, String realName, Class<?> mirror) {
        Class<?> real = Arrays.stream(owner.getDeclaredClasses())
                .filter(c -> c.getSimpleName().equals(realName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        owner.getSimpleName() + " has no nested class " + realName));
        assertSameShape(owner.getSimpleName() + "." + realName, real, mirror);
    }

    /**
     * Like {@link #assertMirrors} for a top-level record in a module outside this
     * program's documented classpath (agentspaces-agent's {@code JoinTicket}):
     * checks the shape when the class loads and says so when it does not, so a
     * run with the agent classes added to the classpath still catches drift.
     */
    private static void assertMirrorsIfLoadable(String realClassName, Class<?> mirror) {
        Class<?> real;
        try {
            real = Class.forName(realClassName);
        } catch (ClassNotFoundException absent) {
            System.err.println("note: " + realClassName + " is not on the classpath; the "
                    + mirror.getSimpleName() + " mirror is unchecked this run");
            return;
        }
        assertSameShape(realClassName, real, mirror);
    }

    private static void assertSameShape(String label, Class<?> real, Class<?> mirror) {
        require(real.isRecord(), label + " is a record");
        List<String> realShape = shape(real.getRecordComponents());
        List<String> mirrorShape = shape(mirror.getRecordComponents());
        if (!realShape.equals(mirrorShape)) {
            throw new IllegalStateException("mirror drift for " + label
                    + ": real " + realShape + " vs mirror " + mirrorShape);
        }
    }

    private static List<String> shape(RecordComponent[] components) {
        return Arrays.stream(components)
                .map(c -> c.getName() + ":" + c.getType().getSimpleName())
                .toList();
    }

    private static void require(boolean condition, String what) {
        if (!condition) {
            throw new IllegalStateException("golden generator self-check failed: " + what);
        }
    }

    private static String codecString(CborCodec codec, Object value) {
        // The @JsonValue string form, recovered by decoding the CBOR text item.
        byte[] bytes = codec.toBytes(value);
        return codec.fromBytes(bytes, String.class);
    }

    /**
     * The {@code session_*} vectors (SPEC §5.6 v0.1.17 session attestation; srad/agentspaces-js.md JS-H-05):
     * for fixed X25519 scalars, nonces, and PeerIDs, the hello and answer bodies, the derived key, and one
     * session frame, computed over the same helpers PeerNode uses, so the TypeScript client (and a later
     * Python one) derive the same key and verify the same tag.
     */
    private static String sessionVectors(CborCodec codec, HexFormat hex) throws Exception {
        byte[] prefix = hex.parseHex("302e020100300506032b656e04220420");
        java.security.KeyFactory factory = java.security.KeyFactory.getInstance("X25519");
        byte[] aScalar = new byte[32];
        java.util.Arrays.fill(aScalar, (byte) 1);
        byte[] bScalar = new byte[32];
        java.util.Arrays.fill(bScalar, (byte) 2);
        java.security.PrivateKey a = factory.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(
                concat(prefix, aScalar)));
        java.security.PrivateKey b = factory.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(
                concat(prefix, bScalar)));
        byte[] base = new byte[32];
        base[0] = 9;
        byte[] aPublic = ai.badmonkey.agentspaces.common.crypto.X25519.agree(a, base);
        byte[] bPublic = ai.badmonkey.agentspaces.common.crypto.X25519.agree(b, base);
        byte[] aNonce = new byte[32];
        java.util.Arrays.fill(aNonce, (byte) 0xaa);
        byte[] bNonce = new byte[32];
        java.util.Arrays.fill(bNonce, (byte) 0xbb);
        // PeerNode.deriveSessionKey: HKDF over the agreement, bound to both PeerIDs and nonces in PeerID order.
        java.io.ByteArrayOutputStream info = new java.io.ByteArrayOutputStream();
        info.writeBytes("agentspaces session v1".getBytes(StandardCharsets.UTF_8));
        info.writeBytes("zA".getBytes(StandardCharsets.UTF_8));
        info.write(0);
        info.writeBytes("zB".getBytes(StandardCharsets.UTF_8));
        info.write(0);
        info.writeBytes(aNonce);
        info.writeBytes(bNonce);
        byte[] key = ai.badmonkey.agentspaces.common.crypto.Hkdf.derive(
                ai.badmonkey.agentspaces.common.crypto.X25519.agree(a, bPublic), new byte[0], info.toByteArray(), 32);
        ai.badmonkey.agentspaces.peering.wire.WireCodec wire = new ai.badmonkey.agentspaces.peering.wire.WireCodec(codec);
        ai.badmonkey.agentspaces.peering.wire.Envelope envelope = new ai.badmonkey.agentspaces.peering.wire.Envelope(
                ai.badmonkey.agentspaces.peering.wire.WireCodec.WIRE_VERSION, GroupId.of("zGroup"),
                ai.badmonkey.agentspaces.peering.wire.Envelope.Kind.PING, PeerId.of("zA"), PeerId.of("zB"),
                new HlcTimestamp(1735689600000L, 3, "zA"), codec.toBytes(Map.of("nonce", 42)));
        StringBuilder out = new StringBuilder();
        out.append(field("session_a_scalar", hex.formatHex(aScalar)));
        out.append(field("session_b_scalar", hex.formatHex(bScalar)));
        out.append(field("session_a_public", hex.formatHex(aPublic)));
        out.append(field("session_b_public", hex.formatHex(bPublic)));
        out.append(field("session_a_nonce", hex.formatHex(aNonce)));
        out.append(field("session_b_nonce", hex.formatHex(bNonce)));
        out.append(field("session_key", hex.formatHex(key)));
        out.append(field("session_hello_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.peering.wire.Bodies.SessionHello(aPublic, aNonce, PeerId.of("zB"), null)))));
        out.append(field("session_answer_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.peering.wire.Bodies.SessionHello(bPublic, bNonce, PeerId.of("zA"), aNonce)))));
        out.append(field("session_envelope_cbor", hex.formatHex(codec.toBytes(envelope))));
        out.append(field("session_frame_cbor", hex.formatHex(wire.encodeSession(envelope, key))));
        return out.toString();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    /** The {@code filter_grammar_cases} array: clause, JSON value, and the Java verdict. */
    /**
     * The Raft frame union as the capabilities module encodes it (srad/agentspaces-js.md
     * JS-N-04): one frame per message kind with fixed fields, so a TypeScript voter or
     * client produces the bytes a Java voter reads.
     */
    private static String raftVectors(CborCodec codec, HexFormat hex) {
        byte[] command = "cmd".getBytes(StandardCharsets.UTF_8);
        var requestVote = new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.RequestVote(
                3, PeerId.of("zA"), 7, 2);
        var voteReply = new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.VoteReply(3, true);
        var appendEntries = new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.AppendEntries(
                3, PeerId.of("zA"), 7, 2,
                List.of(new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.LogEntry(3, command)), 6);
        var appendReply = new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.AppendReply(
                3, PeerId.of("zB"), true, 8);
        var clientSubmit = new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.ClientSubmit(command);
        StringBuilder out = new StringBuilder();
        out.append(field("raft_request_vote_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame(
                        requestVote, null, null, null, null)))));
        out.append(field("raft_vote_reply_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame(
                        null, voteReply, null, null, null)))));
        out.append(field("raft_append_entries_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame(
                        null, null, appendEntries, null, null)))));
        out.append(field("raft_append_reply_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame(
                        null, null, null, appendReply, null)))));
        out.append(field("raft_client_submit_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame(
                        null, null, null, null, clientSubmit)))));
        return out.toString();
    }

    /**
     * The connector entries as agentspaces-connect-core encodes them (srad/agentspaces-js.md
     * JS-C-01, JS-C-03, JS-N-01): the three {@code aspace:cap/data-query} records and the two
     * {@code aspace:cap/asset-invoke} records from fixed values, plus the canonical query hash
     * ({@code DataQueryEntries.canonicalHash}: multibase(sha-256) over the asset and the sorted
     * parameters), so {@code @agentspaces/connect} and the Python client produce the bytes a Java
     * connector or agent reads. Map-valued fields are in canonical order (wire v3).
     */
    private static String connectVectors(CborCodec codec, HexFormat hex) {
        Map<String, String> parameters = new java.util.TreeMap<>(Map.of("region", "eu", "limit", "10"));
        StringBuilder canonical = new StringBuilder("orders");
        parameters.forEach((key, value) -> canonical.append('|').append(key).append('=').append(value));
        String hash = ai.badmonkey.agentspaces.common.codec.Multibase.base58btc(
                Digests.sha256(canonical.toString().getBytes(StandardCharsets.UTF_8)));
        String invocationId = "6f1d2c3b-4a59-4e8f-9b7a-0c1d2e3f4a5b";
        StringBuilder out = new StringBuilder();
        out.append(field("data_query_canonical_hash", hash));
        out.append(field("data_query_cbor", hex.formatHex(codec.toBytes(
                new DataQuery(hash, "orders", parameters, "zPeer/analyst")))));
        out.append(field("data_result_cbor", hex.formatHex(codec.toBytes(new DataResult(hash, "orders",
                List.of(Map.of("id", "1", "region", "eu", "amount", "120.00"),
                        Map.of("id", "3", "region", "eu", "amount", "45.50")), null, "pg-connector")))));
        out.append(field("asset_change_cbor", hex.formatHex(codec.toBytes(new AssetChange("orders", "1",
                Map.of("id", "1", "region", "eu", "amount", "125.00"), "pg-connector")))));
        out.append(field("asset_invocation_cbor", hex.formatHex(codec.toBytes(new AssetInvocation(1,
                "do/Inventory", "reserveStock", Map.of("sku", "A-1", "qty", "2"), invocationId)))));
        out.append(field("asset_invocation_result_cbor", hex.formatHex(codec.toBytes(new AssetInvocationResult(1,
                invocationId, "do/Inventory", "reserveStock", true, Map.of("reserved", "2"), null)))));
        return out.toString();
    }

    /**
     * The serving half's Java encodings (srad/agentspaces-js.md JS-R-04): ReplicatedSpace's
     * stateHash and claimHash over the golden delta and claim delta (reflection, the methods
     * are private), and Double.toString on the values a bid or a filter literal may take, so a
     * TypeScript host answers a Java DIGEST with the same lines a Java member would.
     */
    private static String servingVectors(SpaceWire.EntryStateDto dto,
                                         ai.badmonkey.agentspaces.space.replicated.TakeClaim claim) throws Exception {
        StringBuilder out = new StringBuilder();
        java.lang.reflect.Method stateHash = ReplicatedSpace.class.getDeclaredMethod("stateHash",
                ai.badmonkey.agentspaces.space.crdt.EntryState.class);
        stateHash.setAccessible(true);
        out.append(field("space_state_hash", (String) stateHash.invoke(null, dto.toState())));
        java.lang.reflect.Method claimHash = ReplicatedSpace.class.getDeclaredMethod("claimHash",
                ai.badmonkey.agentspaces.space.replicated.TakeClaim.class);
        claimHash.setAccessible(true);
        out.append(field("space_claim_hash", (String) claimHash.invoke(null, claim)));
        double[] doubles = {1.0, 0.5, 100.0, -2.5, 0.001, 1e7, 1.5e-5, 123456789.0, 1e-4, 0.0, 3.25, 9999999.0,
                1e21, 0.1 + 0.2, 1.0 / 3.0, 2.5e-7, 1234567.125, Double.MAX_VALUE, Double.MIN_VALUE};
        out.append("  \"java_double_strings\": {");
        for (int i = 0; i < doubles.length; i++) {
            // keyed by the JavaScript form of the value (Number's shortest round-trip digits), which
            // Java's %s is not; the TypeScript test parses the key with Number().
            String key = new java.math.BigDecimal(doubles[i]).stripTrailingZeros().toPlainString();
            if (doubles[i] == Double.MAX_VALUE) {
                key = "1.7976931348623157e+308";
            } else if (doubles[i] == Double.MIN_VALUE) {
                key = "5e-324";
            }
            out.append(i > 0 ? ", " : "").append("\"").append(key).append("\": \"")
                    .append(Double.toString(doubles[i])).append("\"");
        }
        return out.append("},\n").toString();
    }

    /**
     * The WebRTC signalling bodies and the relay wrapper (srad/agentspaces-js.md appendix B,
     * JS-W-03, JS-N-03): OFFER, ANSWER, CAND, the trickle-end CAND, END(2) under a fixed session
     * id, and a RELAY_FRAME body carrying the golden ping frame to {@code zB}.
     */
    private static String rtcVectors(CborCodec codec, HexFormat hex, byte[] innerFrame) {
        byte[] sid = hex.parseHex("000102030405060708090a0b0c0d0e0f");
        String sdp = "v=0\r\na=fingerprint:sha-256 AB:CD\r\n";
        List<String> fp = List.of("sha-256 AB:CD");
        StringBuilder out = new StringBuilder();
        out.append(field("rtc_offer_cbor", hex.formatHex(codec.toBytes(
                new RtcSignal(1, fp, null, sdp, sid, null, null, "OFFER")))));
        out.append(field("rtc_answer_cbor", hex.formatHex(codec.toBytes(
                new RtcSignal(1, fp, null, sdp, sid, null, null, "ANSWER")))));
        out.append(field("rtc_cand_cbor", hex.formatHex(codec.toBytes(
                new RtcSignal(1, null, "0", null, sid, "candidate:1 1 UDP 1 192.0.2.1 5000 typ host", null, "CAND")))));
        out.append(field("rtc_cand_end_cbor", hex.formatHex(codec.toBytes(
                new RtcSignal(1, null, "0", null, sid, "", null, "CAND")))));
        out.append(field("rtc_end_timeout_cbor", hex.formatHex(codec.toBytes(
                new RtcSignal(1, null, null, null, sid, null, 2, "END")))));
        out.append(field("relay_frame_body_cbor", hex.formatHex(codec.toBytes(
                new ai.badmonkey.agentspaces.peering.wire.Bodies.RelayFrame(PeerId.of("zB"), innerFrame)))));
        return out.toString();
    }

    private static String filterGrammarCases() {
        Object[][] cases = {
                {"priority>=3", 3}, {"priority>=3", 2.5}, {"priority=3", 3.0}, {"priority=3", "3"},
                {"priority<10", 9}, {"priority<10", "9"}, {"priority<10", "abc"}, {"priority > 1.5", 2},
                {"topic=index", "index"}, {"topic!=index", "other"}, {"topic!=index", null},
                {"topic=index", null}, {"name<m", "alpha"}, {"name>m", "alpha"}, {"name<=alpha", "alpha"},
                {"region in eu, us", "us"}, {"region in eu, us", "ap"}, {"n in 1,2,3", 2.0},
                {"flag=true", true}, {"flag!=true", false}, {"topic=a=b", "a=b"}, {"count>=0", 0},
                {"count<-1", -2}, {"ratio>=0.5", 0.75}, {"ratio>=0.5", "0.25"},
        };
        StringBuilder out = new StringBuilder("  \"filter_grammar_cases\": [\n");
        for (int i = 0; i < cases.length; i++) {
            String clause = (String) cases[i][0];
            Object value = cases[i][1];
            String valueJson = value == null ? "null" : value instanceof String s ? "\"" + s + "\"" : String.valueOf(value);
            boolean matches = ai.badmonkey.agentspaces.api.space.FieldFilter.parse(clause).matches(value);
            out.append("    {\"filter\": \"").append(clause).append("\", \"value\": ").append(valueJson)
                    .append(", \"matches\": ").append(matches).append("}").append(i + 1 < cases.length ? ",\n" : "\n");
        }
        out.append("  ],\n  \"filter_grammar_malformed\": [");
        String[] malformed = {"!=x", "=3", "<x", " in a,b", "nonsense", "topic!", ""};
        for (int i = 0; i < malformed.length; i++) {
            try {
                ai.badmonkey.agentspaces.api.space.FieldFilter.parse(malformed[i]);
                throw new IllegalStateException("'" + malformed[i] + "' parsed");
            } catch (IllegalArgumentException expected) {
                out.append(i > 0 ? ", " : "").append("\"").append(malformed[i]).append("\"");
            }
        }
        return out.append("],\n").toString();
    }

    private static String field(String name, String value) {
        return "  \"" + name + "\": \"" + value + "\",\n";
    }

    private static String number(String name, long value) {
        return "  \"" + name + "\": " + value + ",\n";
    }
}
