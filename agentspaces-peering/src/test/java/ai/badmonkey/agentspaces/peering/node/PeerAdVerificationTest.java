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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Admission verifies the peer advertisement itself, not just the envelope
 * that carried it (spec §5.1 "every verified peer", §11, ASF-027): the
 * embedded key must hash to the ad's issuer, the signature must cover the
 * exact bytes, and the ad must be neither expired nor issued from the future.
 * Every forgery here rides a correctly signed envelope from a peer outside
 * the view, exactly like a genuine bootstrap self-introduction.
 */
class PeerAdVerificationTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zVerifiedAds");
    private final PeerIdentity founder = PeerIdentity.generate();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zVerifiedAds",
            founder.peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "verified-ads", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private PeerNode member;
    private GroupRuntime runtime;
    private TransportConnection injector;

    @BeforeEach
    void setUp() throws IOException {
        member = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1).build();
        member.listen(network.register("m"), "m");
        runtime = member.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        injector = network.register("injector").dial("m");
    }

    @AfterEach
    void tearDown() {
        member.close();
    }

    private PeerAdvertisement ad(PeerId issuer, Instant issued, Duration ttl) {
        return new PeerAdvertisement("aspace://" + groupId.value() + "/peer/" + issuer.value(),
                issuer, groupId, issued, ttl,
                List.of(new PeerAdvertisement.Endpoint("mem", "elsewhere", 0)), Set.of(), Map.of());
    }

    /** The bootstrap self-introduction: an unaddressed RUMOR on the {@code peers} stream. */
    private void introduce(PeerIdentity envelopeSigner, PeerNode.SignedPeerAd payload, int logical)
            throws IOException {
        Bodies.Rumor rumor = new Bodies.Rumor("peers", "peer:" + logical, 6, codec.toBytes(payload));
        injector.send(TestFrames.signed(envelopeSigner, groupId, Envelope.Kind.RUMOR, null,
                clock, logical, rumor));
    }

    private PeerNode.SignedPeerAd sign(PeerIdentity signer, PeerAdvertisement ad) {
        byte[] adBytes = codec.toBytes(ad);
        return new PeerNode.SignedPeerAd(adBytes, signer.rawPublicKey(), signer.sign(adBytes));
    }

    /** Spec §5.1: a correctly signed, fresh self-advertisement admits its issuer to an OPEN group. */
    @Test
    void aGenuineSelfAdvertisementIsAdmitted() throws Exception {
        PeerIdentity newcomer = PeerIdentity.generate();
        introduce(newcomer, sign(newcomer, ad(newcomer.peerId(), clock.instant(),
                Duration.ofMinutes(10))), 0);

        assertThat(runtime.membership().member(newcomer.peerId())).isPresent();
    }

    /** Spec §11: the ad signature covers the exact canonical bytes; a tampered ad admits nobody. */
    @Test
    void aTamperedAdvertisementIsRefused() throws Exception {
        PeerIdentity newcomer = PeerIdentity.generate();
        PeerNode.SignedPeerAd genuine = sign(newcomer,
                ad(newcomer.peerId(), clock.instant(), Duration.ofMinutes(10)));
        byte[] tampered = genuine.adBytes().clone();
        tampered[tampered.length / 2] ^= 0x01;

        introduce(newcomer, new PeerNode.SignedPeerAd(tampered, genuine.publicKey(),
                genuine.signature()), 0);

        assertThat(runtime.membership().allMembers()).isEmpty();
    }

    /** Spec §5.1/§11: the embedded key must hash to the ad's issuer, so an ad cannot be signed for another PeerID. */
    @Test
    void anAdvertisementWhoseKeyDoesNotHashToItsIssuerIsRefused() throws Exception {
        PeerIdentity signer = PeerIdentity.generate();
        PeerId impersonated = PeerIdentity.generate().peerId();

        introduce(signer, sign(signer, ad(impersonated, clock.instant(), Duration.ofMinutes(10))), 0);

        assertThat(runtime.membership().member(impersonated)).isEmpty();
        assertThat(runtime.membership().member(signer.peerId())).isEmpty();
    }

    /** Spec §11 (ASF-027): an ad issued beyond the skew window is refused; within it, accepted. */
    @Test
    void aFarFutureIssueStampIsRefused() throws Exception {
        PeerIdentity newcomer = PeerIdentity.generate();
        introduce(newcomer, sign(newcomer, ad(newcomer.peerId(),
                clock.instant().plus(Duration.ofMinutes(11)), Duration.ofMinutes(10))), 0);
        assertThat(runtime.membership().member(newcomer.peerId())).isEmpty();

        introduce(newcomer, sign(newcomer, ad(newcomer.peerId(),
                clock.instant().plus(Duration.ofMinutes(9)), Duration.ofMinutes(10))), 1);
        assertThat(runtime.membership().member(newcomer.peerId())).isPresent();
    }

    /** Spec §6.2 / §5.2 (P2): an expired self-advertisement is not evidence of a live peer. */
    @Test
    void anExpiredAdvertisementIsRefused() throws Exception {
        PeerIdentity newcomer = PeerIdentity.generate();
        introduce(newcomer, sign(newcomer, ad(newcomer.peerId(),
                clock.instant().minus(Duration.ofMinutes(20)), Duration.ofMinutes(10))), 0);

        assertThat(runtime.membership().member(newcomer.peerId())).isEmpty();
    }

    /** ASF-047: once the founder revokes a peer, a member forwarding the revoked peer's still-valid self-advertisement re-admits nobody (outside attestation mode this was the gap). */
    @Test
    void aForwardedSelfAdvertisementOfARevokedPeerReadmitsNobody() throws Exception {
        PeerIdentity accomplice = PeerIdentity.generate();
        PeerIdentity victim = PeerIdentity.generate();
        introduce(accomplice, sign(accomplice, ad(accomplice.peerId(), clock.instant(),
                Duration.ofMinutes(10))), 0);
        // The victim introduces itself on its own connection, which its
        // revocation will close.
        TransportConnection victimLink = network.register("victim").dial("m");
        victimLink.send(TestFrames.signed(victim, groupId, Envelope.Kind.RUMOR, null, clock, 1,
                new Bodies.Rumor("peers", "peer:1", 6, codec.toBytes(sign(victim,
                        ad(victim.peerId(), clock.instant(), Duration.ofMinutes(10)))))));
        assertThat(runtime.membership().member(victim.peerId())).isPresent();

        ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement revocation =
                new ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement(
                        "aspace://" + groupId.value() + "/revocation/" + victim.peerId().value(),
                        founder.peerId(), groupId, clock.instant(), Duration.ofDays(1),
                        victim.peerId(), "compromised", null);
        byte[] adBytes = codec.toBytes(revocation);
        assertThat(runtime.revocations().accept(new ai.badmonkey.agentspaces.peering.membership
                .RevocationRegistry.SignedRevocation(adBytes, founder.rawPublicKey(),
                founder.sign(adBytes)))).isPresent();
        assertThat(runtime.membership().member(victim.peerId())).as("evicted").isEmpty();

        // The accomplice, still a member, forwards the victim's genuine fresh ad.
        clock.advance(Duration.ofSeconds(1));
        introduce(accomplice, sign(victim, ad(victim.peerId(), clock.instant(),
                Duration.ofMinutes(10))), 2);
        assertThat(runtime.membership().member(victim.peerId()))
                .as("not re-admitted by proxy").isEmpty();
    }
}
