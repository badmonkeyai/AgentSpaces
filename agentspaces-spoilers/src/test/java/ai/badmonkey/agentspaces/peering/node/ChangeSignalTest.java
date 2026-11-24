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
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The change signals the phase-6 seam cuts found unguarded (srad/agentspaces-event-loop.md §10.33, EL-A-02): each
 * signal must ask its participant in the instant it is raised, not leave it to the backstop sweep (15 s since the
 * phase-6 gate). A sweep is nobody's cause, so each test reads the participant's asked runs (and, where a wider wake
 * could mask the cut, the signal's own wake count) one millisecond of virtual time after the signal.
 */
@Timeout(300)
class ChangeSignalTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final Map<String, PeerNode> nodes = new LinkedHashMap<>();
    private final Map<String, GroupRuntime> runtimes = new LinkedHashMap<>();
    private final GroupId groupId = GroupId.of("zChangeSignals");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zChangeSignals",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "change-signals", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.values().forEach(PeerNode::close);
    }

    private GroupRuntime member(String name, String seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time)
                .randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        nodes.put(name, node);
        GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        runtimes.put(name, rt);
        node.startTicking(TICK);
        return rt;
    }

    private void formAndIdle(Duration quiet) {
        int size = runtimes.size();
        long waited = 0;
        while (!runtimes.values().stream().allMatch(rt -> rt.membership().allMembers().size() == size - 1)) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the group forms within 120 s of virtual time").isLessThan(120_000);
        }
        time.advance(quiet);
    }

    private String p(String participant) {
        return groupId.value() + "/" + participant;
    }

    private static long asked(PeerNode node, String participant) {
        return node.loopStats().askedRuns().getOrDefault(participant, 0L);
    }

    private static long wakes(PeerNode node, String participant) {
        return node.loopStats().wakeCalls().getOrDefault(participant, 0L);
    }

    /** W06: a pace set at run time (GossipBus.antiEntropyPeriod) asks the group's anti-entropy in the same instant. */
    @Test
    void aPaceSetAtRunTimeAsksTheGroupsAntiEntropyAtOnce() throws Exception {
        GroupRuntime a = member("a", null);
        member("b", "a");
        formAndIdle(Duration.ofMinutes(2));
        PeerNode node = nodes.get("a");
        long before = asked(node, p("anti-entropy"));
        a.gossip().antiEntropyPeriod(a.gossip().antiEntropyPeriod());
        time.advance(Duration.ofMillis(1));
        assertThat(asked(node, p("anti-entropy")) - before)
                .as("EL-L-03: the pace change asks anti-entropy within 1 ms, not at the backstop").isEqualTo(1);
    }

    /** W07: an input of the node's card moved (a group hint) asks the group's self ad in the same instant. */
    @Test
    void aMovedCardInputAsksTheSelfAdAtOnce() throws Exception {
        member("a", null);
        member("b", "a");
        formAndIdle(Duration.ofMinutes(2));
        PeerNode node = nodes.get("a");
        long asked0 = asked(node, p("self-ad"));
        long wakes0 = wakes(node, p("self-ad"));
        node.groupHint(groupId, "seam", "cut");
        time.advance(Duration.ofMillis(1));
        assertThat(wakes(node, p("self-ad")) - wakes0).as("EL-L-09: the card change signals the self ad").isPositive();
        assertThat(asked(node, p("self-ad")) - asked0).as("and the self ad runs within 1 ms").isEqualTo(1);
    }

    /** W08: a link opening asks the reach participant (relay.via hints), which returns NEVER and has no deadline. */
    @Test
    void aLinkOpeningAsksTheReachParticipant() throws Exception {
        member("a", null);
        member("b", "a");
        formAndIdle(Duration.ofMinutes(2));
        PeerNode node = nodes.get("a");
        long wakes0 = wakes(node, p("reach"));
        member("c", "a");
        long waited = 0;
        while (wakes(node, p("reach")) == wakes0 && waited < 500) {
            time.advance(Duration.ofMillis(1));
            waited++;
        }
        assertThat(wakes(node, p("reach")) - wakes0).as("EL-L-03: a link opened signals reach").isPositive();
        assertThat(waited).as("within 500 ms of the newcomer's dial").isLessThan(500);
    }

    /** W13: a working frame (PIPE_DATA) asks the group's registrants, here one whose deadline is NEVER. */
    @Test
    void aWorkingFrameAsksARegistrantWhoseDeadlineIsNeverAtOnce() throws Exception {
        GroupRuntime a = member("a", null);
        GroupRuntime b = member("b", "a");
        formAndIdle(Duration.ofMinutes(1));
        AtomicLong runs = new AtomicLong();
        a.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> { });
        DueWork.Registration reg = a.onDue("pipe-reader", now -> {
            runs.incrementAndGet();
            return DueWork.NEVER;
        });
        time.advance(Duration.ofMillis(1)); // its first, registration run
        long before = runs.get();
        b.send(nodes.get("a").peerId(), Envelope.Kind.PIPE_DATA, Map.of("seq", 1L));
        time.advance(Duration.ofMillis(1));
        assertThat(runs.get() - before).as("EL-L-03: the working frame asks the registrant within 1 ms").isEqualTo(1);
        reg.close();
    }

    /**
     * W03 and W09a (phase-6 gate): a member that falls silent leaves the view inside a membership run; the view change
     * asks the whole group (W03), so the view participant drains the departure in that same run, and the departure
     * signals the lost-member retries (W09a, NL-7). Both used to fail no test: under a cut the departure waited for
     * the backstop sweep, now 15 s (decision 3).
     */
    @Test
    void aDepartureIsDrainedInTheRunThatSawItAndSignalsTheLostRetries() throws Exception {
        member("a", null);
        member("b", "a");
        member("c", "a");
        formAndIdle(Duration.ofMinutes(2));
        PeerNode node = nodes.get("a");
        GroupRuntime atA = runtimes.get("a");
        PeerId gone = nodes.get("c").peerId();
        long viewAsked0 = asked(node, p("view"));
        long lostWakes0 = wakes(node, p("lost"));
        nodes.remove("c").close();
        runtimes.remove("c");
        long waited = 0;
        while (atA.membership().member(gone).isPresent()) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("a drops the silent member inside its TTL plus a probe cycle").isLessThan(120_000);
        }
        time.advance(Duration.ofMillis(1));
        assertThat(atA.membership().departurePending(gone))
                .as("EL-L-03: the view change asked the view participant, which drained the departure at once")
                .isFalse();
        assertThat(asked(node, p("view")) - viewAsked0).as("the view participant was asked").isPositive();
        assertThat(wakes(node, p("lost")) - lostWakes0).as("NL-7: the departure signalled the lost retries")
                .isPositive();
    }

    /**
     * W09d (phase-6 gate): a group's admission memory turning dirty asks the persist participant, which writes it
     * within 1 ms; the participant has no deadline while the memory is clean, so a cut waited for the backstop.
     */
    @Test
    void aDirtyAdmissionMemoryIsWrittenAtOnce(@TempDir Path state) throws Exception {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).stateDirectory(state)
                .randomSeed(17).build();
        node.listen(network.register("p"), "p");
        nodes.put("p", node);
        node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        node.startTicking(TICK);
        time.advance(Duration.ofSeconds(30));
        Path file = node.admissionMemory().fileFor(groupId);
        assertThat(file).as("nothing recorded, nothing written").doesNotExist();
        long wakes0 = wakes(node, p("persist"));
        node.admissionMemory().record(groupId, PeerIdentity.generate().peerId(), "aa,dd");
        time.advance(Duration.ofMillis(1));
        assertThat(wakes(node, p("persist")) - wakes0).as("EL-L-03: the dirty mark signals persist").isEqualTo(1);
        assertThat(file).as("and the memory is written within 1 ms, not at the backstop").exists();
    }

    /** W10: trustChanged() asks the trust participant in the same instant (EL-L-14's signal path). */
    @Test
    void trustChangedAsksTheTrustParticipantAtOnce() throws Exception {
        member("a", null);
        member("b", "a");
        formAndIdle(Duration.ofSeconds(30));
        PeerNode node = nodes.get("a");
        long before = asked(node, "trust");
        node.trustChanged();
        time.advance(Duration.ofMillis(1));
        assertThat(asked(node, "trust") - before).as("EL-L-14: the signal asks the trust rejudge").isEqualTo(1);
    }

    /**
     * EL-L-14 end to end: the node's supplier is a {@code ReloadingChannelTrust}, the one the starter builds. Its CRL
     * refresh runs on the node's time source and signals the node, so the trust participant is never due on a timer
     * (it compared the supplier once a second), and a replaced CRL file asks it at the refresh instant.
     */
    @Test
    void aReloadingTrustSignalsItsRefreshAndTheNodeNeverLooksOnATimer(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        ai.badmonkey.agentspaces.test.TestCa ca = ai.badmonkey.agentspaces.test.TestCa.create();
        PeerIdentity peer = PeerIdentity.generate();
        ai.badmonkey.agentspaces.test.TestCa.Issued leaf = ca.issue(peer.rawPublicKey());
        java.nio.file.Path crl = dir.resolve("ca.crl");
        java.nio.file.Files.write(crl, ca.crl().getEncoded());
        ai.badmonkey.agentspaces.identity.ReloadingChannelTrust trust = new ai.badmonkey.agentspaces.identity.ReloadingChannelTrust(
                List.of(ca.certificate()), List.of(crl), Duration.ofMinutes(5), false, time);
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).channelTrust(trust).build();
        nodes.put("a", node);
        node.startTicking(TICK);
        time.advance(Duration.ofMillis(1));
        long due0 = node.loopStats().dueRuns().getOrDefault("trust", 0L);
        long asked0 = asked(node, "trust");
        time.advance(Duration.ofMinutes(4));
        assertThat(node.loopStats().dueRuns().getOrDefault("trust", 0L) - due0)
                .as("EL-L-14: no timed look at a signalling supplier (a look a second read 240)").isZero();
        assertThat(asked(node, "trust") - asked0).as("nothing changed, nothing asked").isZero();
        ai.badmonkey.agentspaces.identity.ChannelTrust first = trust.get();
        java.nio.file.Files.write(crl, ca.crl(leaf.certificate()).getEncoded()); // the CA revokes the peer's leaf
        time.advance(Duration.ofSeconds(59)); // the refresh at 5 min has not come
        assertThat(asked(node, "trust") - asked0).as("not before the refresh").isZero();
        time.advance(Duration.ofSeconds(1)); // the refresh: the files are read and the node is told
        assertThat(asked(node, "trust") - asked0).as("the refresh asks the trust rejudge at once").isEqualTo(1);
        assertThat(trust.get()).isNotSameAs(first);
        assertThat(trust.get().attest(leaf.chain(ca))).as("the new CRL revokes the leaf").isEmpty();
        nodes.remove("a");
        node.close();
        long asked1 = asked(node, "trust");
        java.nio.file.Files.write(crl, ca.crl().getEncoded());
        time.advance(Duration.ofMinutes(10));
        assertThat(asked(node, "trust")).as("a closed node's registration stops the refresh").isEqualTo(asked1);
    }
}
