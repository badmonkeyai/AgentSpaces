/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A probe takes each kind of hash only when a member in view would receive it (a demo console's deployments,
 * 2026-11-18): every probing tick took the text digests' hashes as well as the trie's, so a space whose generation
 * moved (a lease sweep is enough) rebuilt its sorted text digest each tick, for a form no member of an all-hinted view
 * was ever sent.
 */
class ProbeFormCostTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zProbeForm",
            PeerIdentity.generate().peerId(), GroupId.of("zProbeForm"), Instant.EPOCH, Duration.ofDays(1),
            "probe-form", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** A space-like stream: the same state on every member, with a trie hash, counting its text digests. */
    private static final class Counting implements ReconcilableState {
        final AtomicLong texts = new AtomicLong();
        private final byte[] trie = new byte[32];

        @Override public byte[] digest() {
            texts.incrementAndGet();
            return "e:the-one-entry=1".getBytes(StandardCharsets.UTF_8);
        }

        @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
        @Override public void applyDelta(byte[] delta) { }
        @Override public boolean applyDeltaChanged(byte[] delta) { return false; }
        @Override public byte[] streamHash() { return trie; }
    }

    private PeerNode node(String address, long seed, boolean audit) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .peerAdInterval(Duration.ofMinutes(5)).audit(audit).build();
        node.listen(network.register(address), address);
        node.probeHashesAlways(true); // every tick probes with hashes, the case the cost appeared in
        nodes.add(node);
        return node;
    }

    private Counting join(PeerNode node, String... seeds) {
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        Counting state = new Counting();
        node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints).gossip().reconcile("space:tasks", state);
        return state;
    }

    private void tick(int seconds) {
        for (int i = 0; i < seconds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void anAllHintedViewNeverBuildsATextDigestForItsProbes() throws Exception {
        Counting a = join(node("a", 1, true));
        Counting b = join(node("b", 2, true), "a");
        tick(10); // the members meet and read each other's cards
        long aBefore = a.texts.get();
        long bBefore = b.texts.get();
        tick(30);
        assertThat(a.texts.get() - aBefore).as("text digests a built while every member carries wire.ae=2").isZero();
        assertThat(b.texts.get() - bBefore).as("text digests b built while every member carries wire.ae=2").isZero();
    }

    @Test
    void aViewWithAnUnhintedMemberStillSendsItTheTextForm() throws Exception {
        Counting a = join(node("a", 1, true));
        join(node("b", 2, false), "a"); // b keeps the text form: a's probes to it need the text digests' hashes
        tick(10);
        long before = a.texts.get();
        tick(30);
        assertThat(a.texts.get() - before).as("a builds the text form for its unhinted partner").isPositive();
    }
}
