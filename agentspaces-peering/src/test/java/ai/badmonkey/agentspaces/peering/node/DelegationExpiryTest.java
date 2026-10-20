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

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Membership that outlives the invite window (ISSUE-PeerRobustness-2, R2-1): a
 * delegation's {@code notAfter} bounds first admission, and a member admitted in
 * time stays admissible after the window until its credential or the
 * delegation is revoked. The fleet is the §13 experiment on {@code SimNetwork}:
 * a beacon (RENDEZVOUS and RELAY) and two friends, every one admitted under an
 * invite key whose delegation has a one-minute window built from the test clock.
 */
class DelegationExpiryTest {

    /** The delegation's window, judged with the ten-minute skew: expired eleven minutes in. */
    private static final Duration WINDOW = Duration.ofMinutes(1);
    /** Well past the window plus the skew. */
    private static final int PAST_WINDOW_SECONDS = 900;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zInviteWindow");
    private final GroupAdvertisement invite = new GroupAdvertisement("aspace://zInviteWindow",
            founderId.peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "invite-window",
            GroupAdvertisement.MembershipPolicy.INVITE, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final KeyPair inviteKey = Ed25519.generate();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private String delegation(Duration window) {
        return delegation(window, "friends");
    }

    private String delegation(Duration window, String label) {
        return JoinCredentials.delegate(founderId, groupId, Ed25519.rawPublicKey(inviteKey.getPublic()),
                clock.instant().plus(window), label);
    }

    private String credential(String delegation, PeerIdentity who) {
        return JoinCredentials.issueDelegated(delegation, inviteKey.getPrivate(), who.peerId());
    }

    private PeerNode.Builder builder(long seed, PeerIdentity identity, Set<PeerAdvertisement.PeerRole> roles) {
        return PeerNode.builder(identity).clock(clock).randomSeed(seed).roles(roles);
    }

    private PeerNode listening(String address, long seed, PeerIdentity identity,
                               Set<PeerAdvertisement.PeerRole> roles) throws IOException {
        return listening(address, builder(seed, identity, roles));
    }

    private PeerNode listening(String address, PeerNode.Builder builder) throws IOException {
        PeerNode node = builder.build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    /** A dial-only node: it can reach others, nobody can dial it (the friends of §13). */
    private PeerNode dialOnly(String address, long seed, PeerIdentity identity) {
        PeerNode node = builder(seed, identity, Set.of()).build();
        node.transport(network.register(address));
        nodes.add(node);
        return node;
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
    }

    private GroupRuntime join(PeerNode node, List<PeerAdvertisement.Endpoint> seeds, String credential) {
        return node.joinGroup(invite, GroupMembership.Config.defaults(), seeds,
                credential == null ? Map.of() : Map.of(JoinCredentials.HINT_KEY, credential), null);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            for (PeerNode node : List.copyOf(nodes)) {
                node.tick();
            }
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private static List<PeerId> view(GroupRuntime runtime) {
        return runtime.membership().allMembers().stream().map(GroupMembership.Member::id).toList();
    }

    /** Captures what a logger says, for the tests that check a node explains itself. */
    private static final class Captured extends Handler {
        final List<String> messages = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record) {
            messages.add(record.getMessage());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    /** §13: Sam is partitioned for three times the TTL after the window, and is re-admitted on heal. */
    @Test
    void lapsedMemberRejoinsAfterWindow() throws Exception {
        String delegation = delegation(WINDOW);
        PeerIdentity beaconId = PeerIdentity.generate();
        PeerIdentity samId = PeerIdentity.generate();
        PeerIdentity kimId = PeerIdentity.generate();
        PeerNode beacon = listening("beacon", 1, beaconId,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS, PeerAdvertisement.PeerRole.RELAY));
        PeerNode sam = listening("sam", 2, samId, Set.of());
        PeerNode kim = listening("kim", 3, kimId, Set.of());
        GroupRuntime beaconRuntime = join(beacon, List.of(), credential(delegation, beaconId));
        GroupRuntime samRuntime = join(sam, seed("beacon"), credential(delegation, samId));
        GroupRuntime kimRuntime = join(kim, seed("beacon"), credential(delegation, kimId));

        tickAll(PAST_WINDOW_SECONDS);
        assertThat(view(beaconRuntime)).as("connected members survive the window")
                .containsExactlyInAnyOrder(sam.peerId(), kim.peerId());
        assertThat(view(kimRuntime)).containsExactlyInAnyOrder(beacon.peerId(), sam.peerId());

        network.partition("sam", "beacon");
        network.partition("sam", "kim");
        tickAll(90);
        assertThat(view(beaconRuntime)).as("Sam lapses from every view").containsExactly(kim.peerId());
        assertThat(view(kimRuntime)).containsExactly(beacon.peerId());
        assertThat(view(samRuntime)).isEmpty();
        assertThat(beacon.admissionMemory().forGroup(groupId)).as("the lapse keeps the memory (R2-1.4)")
                .containsKey(sam.peerId());

        network.heal();
        tickAll(300);
        assertThat(view(beaconRuntime)).as("the beacon re-admits Sam after the window")
                .containsExactlyInAnyOrder(sam.peerId(), kim.peerId());
        assertThat(view(kimRuntime)).as("Kim re-admits Sam after the window")
                .containsExactlyInAnyOrder(beacon.peerId(), sam.peerId());
        assertThat(view(samRuntime)).as("Sam re-admits the others after the window")
                .containsExactlyInAnyOrder(beacon.peerId(), kim.peerId());
    }

    /** §13's control: the same partition under an unexpired one-hour key heals on its own. */
    @Test
    void lapsedMemberRejoinsInsideWindow() throws Exception {
        String delegation = delegation(Duration.ofHours(1));
        PeerIdentity beaconId = PeerIdentity.generate();
        PeerIdentity samId = PeerIdentity.generate();
        PeerIdentity kimId = PeerIdentity.generate();
        PeerNode beacon = listening("beacon", 1, beaconId,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS, PeerAdvertisement.PeerRole.RELAY));
        PeerNode sam = listening("sam", 2, samId, Set.of());
        PeerNode kim = listening("kim", 3, kimId, Set.of());
        GroupRuntime beaconRuntime = join(beacon, List.of(), credential(delegation, beaconId));
        GroupRuntime samRuntime = join(sam, seed("beacon"), credential(delegation, samId));
        GroupRuntime kimRuntime = join(kim, seed("beacon"), credential(delegation, kimId));

        tickAll(60);
        network.partition("sam", "beacon");
        network.partition("sam", "kim");
        tickAll(90);
        assertThat(view(samRuntime)).isEmpty();

        network.heal();
        tickAll(300);
        assertThat(view(beaconRuntime)).containsExactlyInAnyOrder(sam.peerId(), kim.peerId());
        assertThat(view(kimRuntime)).containsExactlyInAnyOrder(beacon.peerId(), sam.peerId());
        assertThat(view(samRuntime)).containsExactlyInAnyOrder(beacon.peerId(), kim.peerId());
    }

    /** R2-1.3: a peer nobody admitted in time, presenting a credential under the expired delegation, is refused. */
    @Test
    void newcomerRefusedAfterWindow() throws Exception {
        String delegation = delegation(WINDOW);
        PeerIdentity beaconId = PeerIdentity.generate();
        PeerIdentity samId = PeerIdentity.generate();
        PeerIdentity lateId = PeerIdentity.generate();
        PeerNode beacon = listening("beacon", 1, beaconId, Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode sam = listening("sam", 2, samId, Set.of());
        GroupRuntime beaconRuntime = join(beacon, List.of(), credential(delegation, beaconId));
        GroupRuntime samRuntime = join(sam, seed("beacon"), credential(delegation, samId));
        tickAll(PAST_WINDOW_SECONDS);

        PeerNode late = listening("late", 3, lateId, Set.of());
        GroupRuntime lateRuntime = join(late, seed("beacon"), credential(delegation, lateId));
        tickAll(120);

        assertThat(view(beaconRuntime)).containsExactly(sam.peerId());
        assertThat(view(samRuntime)).containsExactly(beacon.peerId());
        assertThat(view(lateRuntime)).as("the latecomer is admitted by nobody, and admits nobody").isEmpty();
        assertThat(beacon.admissionMemory().forGroup(groupId)).doesNotContainKey(late.peerId());
    }

    /** R2-S1: a member re-presenting a newly minted credential under the expired key is a first admission, and refused. */
    @Test
    void differentCredentialSamePeerRefused() throws Exception {
        String delegation = delegation(WINDOW);
        PeerIdentity beaconId = PeerIdentity.generate();
        PeerIdentity samId = PeerIdentity.generate();
        PeerNode beacon = listening("beacon", 1, beaconId, Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode sam = listening("sam", 2, samId, Set.of());
        GroupRuntime beaconRuntime = join(beacon, List.of(), credential(delegation, beaconId));
        GroupRuntime samRuntime = join(sam, seed("beacon"), credential(delegation, samId));
        // A second delegation of the same invite key, also inside the window: a credential issued
        // under it is a different credential for the same peer (Ed25519 is deterministic, so the
        // only way to mint a different one is under a different delegation).
        String other = delegation(WINDOW, "friends-again");
        tickAll(PAST_WINDOW_SECONDS);
        assertThat(view(beaconRuntime)).containsExactly(sam.peerId());

        network.partition("sam", "beacon");
        tickAll(90);
        assertThat(view(beaconRuntime)).isEmpty();
        sam.groupHint(groupId, JoinCredentials.HINT_KEY, credential(other, samId));
        network.heal();
        tickAll(300);

        assertThat(view(beaconRuntime)).as("the beacon holds Sam under the first credential only").isEmpty();
        assertThat(view(samRuntime)).as("Sam still holds the beacon under the credential it was admitted with")
                .containsExactly(beacon.peerId());
    }

    /** R2-1.2: revoking the delegation after the window evicts a remembered member, forgets it, and refuses it. */
    @Test
    void revocationBeatsMemory() throws Exception {
        String delegation = delegation(WINDOW);
        PeerIdentity samId = PeerIdentity.generate();
        PeerIdentity kimId = PeerIdentity.generate();
        PeerNode founder = listening("founder", 1, founderId, Set.of(PeerAdvertisement.PeerRole.RELAY));
        PeerNode sam = listening("sam", 2, samId, Set.of());
        PeerNode kim = listening("kim", 3, kimId, Set.of());
        GroupRuntime founderRuntime = join(founder, List.of(), null);
        GroupRuntime samRuntime = join(sam, seed("founder"), credential(delegation, samId));
        join(kim, seed("founder"), credential(delegation, kimId));
        tickAll(PAST_WINDOW_SECONDS);
        assertThat(view(founderRuntime)).containsExactlyInAnyOrder(sam.peerId(), kim.peerId());
        assertThat(founder.admissionMemory().forGroup(groupId)).containsKeys(sam.peerId(), kim.peerId());

        assertThat(founderRuntime.revokeDelegation(delegation, CredentialRevocation.KEY_COMPROMISE)).isPresent();
        tickAll(5);
        assertThat(view(founderRuntime)).as("evicted at once").isEmpty();
        assertThat(founder.admissionMemory().forGroup(groupId)).as("and forgotten").isEmpty();

        tickAll(300);
        assertThat(view(founderRuntime)).as("the retries re-introduce them, and the memory admits nobody revoked")
                .isEmpty();
        assertThat(founder.admissionMemory().forGroup(groupId)).isEmpty();
        // The revoked pair still hold each other (the founder evicts them before it gossips, so the
        // revocation never reaches them; the founder stays in their views by answering their probes
        // as it answers any stranger's), and nobody else admits either of them.
        assertThat(view(samRuntime)).contains(kim.peerId());
    }

    /** R2-1.5: a member restarted with its keystore and state directory re-admits the others after the window; one without does not, and says why. */
    @Test
    void restartWithStateDirectory(@TempDir Path state) throws Exception {
        String delegation = delegation(WINDOW);
        PeerIdentity aId = PeerIdentity.generate();
        PeerIdentity bId = PeerIdentity.generate();
        PeerIdentity cId = PeerIdentity.generate();
        String aCredential = credential(delegation, aId);
        String bCredential = credential(delegation, bId);
        Path aState = state.resolve("a");
        PeerNode a = listening("a", builder(1, aId, Set.of()).stateDirectory(aState));
        PeerNode b = listening("b", 2, bId, Set.of());
        PeerNode c = listening("c", 3, cId, Set.of());
        join(a, List.of(), aCredential);
        GroupRuntime bRuntime = join(b, seed("a"), bCredential);
        GroupRuntime cRuntime = join(c, seed("a"), credential(delegation, cId));
        tickAll(PAST_WINDOW_SECONDS);
        Path file = aState.resolve(AdmissionMemory.DIRECTORY).resolve(groupId.value() + ".json");
        assertThat(file).as("one file per group beside the keystore").exists();
        String json = Files.readString(file, StandardCharsets.UTF_8);
        assertThat(json).contains("\"v\":1").contains(b.peerId().value()).contains(c.peerId().value())
                .doesNotContain(bCredential.substring(10, 30));

        // A restarts with the same identity and state directory: it holds nobody, then everybody again.
        a.close();
        nodes.remove(a);
        PeerNode a2 = listening("a", builder(4, aId, Set.of()).stateDirectory(aState));
        GroupRuntime a2Runtime = join(a2, seed("b"), aCredential);
        assertThat(a2.admissionMemory().forGroup(groupId)).as("loaded before the first contact")
                .containsKeys(b.peerId(), c.peerId());
        tickAll(120);
        assertThat(view(a2Runtime)).as("A re-admits the others from its file")
                .containsExactlyInAnyOrder(b.peerId(), c.peerId());
        assertThat(view(bRuntime)).as("and they re-admit A from memory")
                .containsExactlyInAnyOrder(a2.peerId(), c.peerId());

        // B restarts with no state directory: its memory is gone, and it says so when it joins.
        Captured captured = new Captured();
        Logger memoryLog = Logger.getLogger(PeerNode.class.getName());
        memoryLog.addHandler(captured);
        try {
            b.close();
            nodes.remove(b);
            PeerNode b2 = listening("b", 5, bId, Set.of());
            GroupRuntime b2Runtime = join(b2, seed("a"), bCredential);
            tickAll(120);
            assertThat(view(b2Runtime)).as("B remembers nobody, so it admits nobody after the window").isEmpty();
            assertThat(view(a2Runtime)).as("A, which remembers B under the same credential, still admits it")
                    .contains(b2.peerId());
            assertThat(captured.messages).anySatisfy(message ->
                    assertThat(message).contains("held in memory only").contains(groupId.value()));
        } finally {
            memoryLog.removeHandler(captured);
        }
    }

    /** The social beacon's deploy: a RELAY restarted after the window is re-admitted by dial-only members and relays for them again. */
    @Test
    void relayRestartAfterWindow(@TempDir Path state) throws Exception {
        String delegation = delegation(WINDOW);
        PeerIdentity beaconId = PeerIdentity.generate();
        PeerIdentity samId = PeerIdentity.generate();
        PeerIdentity kimId = PeerIdentity.generate();
        String beaconCredential = credential(delegation, beaconId);
        PeerNode beacon = listening("beacon", builder(1, beaconId,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS, PeerAdvertisement.PeerRole.RELAY))
                .stateDirectory(state));
        PeerNode sam = dialOnly("sam", 2, samId);
        PeerNode kim = dialOnly("kim", 3, kimId);
        join(beacon, List.of(), beaconCredential);
        GroupRuntime samRuntime = join(sam, seed("beacon"), credential(delegation, samId));
        GroupRuntime kimRuntime = join(kim, seed("beacon"), credential(delegation, kimId));
        tickAll(PAST_WINDOW_SECONDS);
        assertThat(view(samRuntime)).containsExactlyInAnyOrder(beacon.peerId(), kim.peerId());

        beacon.close();
        nodes.remove(beacon);
        tickAll(60);
        PeerNode beacon2 = listening("beacon", builder(4, beaconId,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS, PeerAdvertisement.PeerRole.RELAY))
                .stateDirectory(state));
        GroupRuntime beacon2Runtime = join(beacon2, List.of(), beaconCredential);
        tickAll(120);

        assertThat(view(beacon2Runtime)).as("the restarted relay re-admits the friends from its file")
                .containsExactlyInAnyOrder(sam.peerId(), kim.peerId());
        assertThat(view(samRuntime)).containsExactlyInAnyOrder(beacon2.peerId(), kim.peerId());
        assertThat(view(kimRuntime)).containsExactlyInAnyOrder(beacon2.peerId(), sam.peerId());

        List<String> kimSaw = new CopyOnWriteArrayList<>();
        kimRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> kimSaw.add(new String(body, StandardCharsets.UTF_8)));
        long forwardedBefore = beacon2.relayStats().get(groupId).framesForwarded(); // the friends' probes cross it too
        samRuntime.send(kim.peerId(), Envelope.Kind.PIPE_DATA, "through the relay".getBytes(StandardCharsets.UTF_8));
        assertThat(kimSaw).as("and relays for them again").containsExactly("through the relay");
        assertThat(beacon2.relayStats().get(groupId).framesForwarded()).isEqualTo(forwardedBefore + 1);
    }

    /** R2-1.7: the verdict tells expiry from forgery; only a delegation whose every other check passed is EXPIRED. */
    @Test
    void judgeTellsExpiryFromForgery() {
        CborCodec codec = CborCodec.defaultCodec();
        PeerIdentity guestId = PeerIdentity.generate();
        String delegation = delegation(WINDOW);
        String credential = credential(delegation, guestId);
        Instant inside = clock.instant();
        Instant past = clock.instant().plus(Duration.ofHours(1));

        assertThat(JoinCredentials.judge(credential, guestId.peerId(), invite, codec, inside))
                .isEqualTo(JoinCredentials.Verdict.VALID);
        assertThat(JoinCredentials.judge(credential, guestId.peerId(), invite, codec, past))
                .isEqualTo(JoinCredentials.Verdict.EXPIRED);
        assertThat(JoinCredentials.judge(credential, PeerIdentity.generate().peerId(), invite, codec, past))
                .as("wrong candidate under an expired delegation is INVALID, never EXPIRED")
                .isEqualTo(JoinCredentials.Verdict.INVALID);
        String forged = JoinCredentials.delegate(PeerIdentity.generate(), groupId,
                Ed25519.rawPublicKey(inviteKey.getPublic()), clock.instant().plus(WINDOW), "forged");
        assertThat(JoinCredentials.judge(credential(forged, guestId), guestId.peerId(), invite, codec, past))
                .isEqualTo(JoinCredentials.Verdict.INVALID);
        String founderIssued = JoinCredentials.issue(founderId, groupId, guestId.peerId());
        assertThat(JoinCredentials.judge(founderIssued, guestId.peerId(), invite, codec, past))
                .as("the founder-issued form has no expiry").isEqualTo(JoinCredentials.Verdict.VALID);
        assertThat(JoinCredentials.judge("garbage", guestId.peerId(), invite, codec, past))
                .isEqualTo(JoinCredentials.Verdict.INVALID);
        assertThat(JoinCredentials.judge(null, guestId.peerId(), invite, codec, past))
                .isEqualTo(JoinCredentials.Verdict.INVALID);
    }
}
