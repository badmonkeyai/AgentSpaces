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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.discovery.SpillStore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ISSUE-AppCardOriginFormats FR-12 at the binder (SPEC §6.1a.5; AppCard origin and formats, 2026-11-09): a card above
 * the spill threshold travels as a stub while the view reads stubs, a level-1 joiner makes the binder reissue it inline
 * at once, and the stub resolves to the card the binder keeps.
 */
class AgentBinderSpillTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zBinderSpill");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zBinderSpill", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "spill", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();
    private final Map<String, byte[]> documents = new ConcurrentHashMap<>();
    /** A store that names each document by its length and hash, which is all the binder's path needs. */
    private final SpillStore store = new SpillStore() {
        @Override
        public CompletableFuture<String> put(String name, byte[] document) {
            byte[] cid = new byte[36];
            cid[0] = 0x01;
            cid[1] = 0x55;
            cid[2] = 0x12;
            cid[3] = 0x20;
            System.arraycopy(ai.badmonkey.agentspaces.common.crypto.Digests.sha256(document), 0, cid, 4, 32);
            String root = ai.badmonkey.agentspaces.common.codec.Multibase.base32(cid);
            documents.put(root, document.clone());
            return CompletableFuture.completedFuture(root);
        }

        @Override
        public CompletableFuture<byte[]> get(String root, long size) {
            return CompletableFuture.completedFuture(documents.get(root).clone());
        }

        @Override
        public void release(String root) {
            documents.remove(root);
        }
    };

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private DiscoveryService member(String address, PeerIdentity identity, int level) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(nodes.size() + 21L).appCardLevel(level).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                nodes.isEmpty() ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "b0", 0)));
        nodes.add(node);
        return new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId()).spillStore(store);
    }

    private boolean until(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(500));
        }
        return condition.getAsBoolean();
    }

    private static Optional<AgentCard> cardAt(DiscoveryService discovery, String id) {
        return discovery.find(AgentCard.class, c -> c.id().equals(id)).stream().findFirst();
    }

    /** A card past the threshold through its actions: 64 of them, each with a long description and long schema names. */
    private AgentCard large(PeerIdentity identity) {
        List<CardAction> actions = new ArrayList<>();
        for (int i = 0; i < AgentCard.MAX_ACTIONS; i++) {
            actions.add(new CardAction(String.format("act%02d", i), "d".repeat(CardAction.MAX_DESCRIPTION_LENGTH),
                    List.of("Task", "t".repeat(300) + i), List.of("Finding", "f".repeat(300) + i), "tasks", CardAction.TAKE));
        }
        return new AgentCard("aspace://zBinderSpill/agent/" + identity.peerId().value() + "/big", identity.peerId(), groupId,
                clock.instant(), Duration.ofMinutes(15), identity.agent("big"), "Answers many kinds of task", List.of("answer"),
                List.of("Task"), List.of("Finding"), Map.of(), Map.of(), null, actions);
    }

    @Test
    void aLargeCardSpillsAndALevelOneJoinerGetsItInline() throws Exception {
        PeerIdentity a = PeerIdentity.generate();
        DiscoveryService onA = member("b0", a, 2);
        DiscoveryService onB = member("b1", PeerIdentity.generate(), 2);
        assertThat(until(() -> onA.appCardGateOpen() && nodes.get(0).groups().iterator().next().membership().allMembers().size() == 1)).isTrue();
        AgentBinder binder = new AgentBinder(a, groupId, onA, clock);
        AgentCard card = large(a);
        assertThat(codec.toBytes(card).length).isGreaterThan(CardSpill.SPILL_THRESHOLD_BYTES);
        binder.adopt(card);
        assertThat(until(() -> cardAt(onB, card.id()).map(c -> c.spill() != null).orElse(false))).isTrue();
        AgentCard stub = cardAt(onB, card.id()).orElseThrow();
        assertThat(stub.actions()).isNull();
        assertThat(stub.spill().parts()).containsExactly("actions");
        assertThat(onB.resolve(stub).join().actions()).isEqualTo(card.actions());

        DiscoveryService onC = member("b2", PeerIdentity.generate(), 1);
        assertThat(until(() -> cardAt(onC, card.id()).map(c -> c.spill() == null && c.actions() != null).orElse(false)))
                .as("the level-1 member gets the full card inline").isTrue();
        assertThat(cardAt(onC, card.id()).orElseThrow().issued()).isAfter(card.issued());
        assertThat(onA.spiller().cardsOversizeInline()).isGreaterThanOrEqualTo(1);
        assertThat(onA.spiller().cardsSpilled()).isEqualTo(1);
        binder.close();
    }
}
