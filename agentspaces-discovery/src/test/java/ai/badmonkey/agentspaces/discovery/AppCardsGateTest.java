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
package ai.badmonkey.agentspaces.discovery;

import ai.badmonkey.agentspaces.api.ad.AppCard;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
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
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ISSUE-AppCardOriginFormats FR-7, FR-8b, and FR-12 over a fleet (SPEC §6.1a.2, §6.1a.5; AppCard origin and formats,
 * 2026-11-09): a view at level 2 gets the gated fields and the stub; a level-1 joiner makes the publisher reissue at once
 * without them, inline, and receives that card; when it leaves, the full card returns; {@code remoteFind} answers the
 * origin and formats keys from a stub, and no answerer fetches a spill.
 */
class AppCardsGateTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zGate");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zGate", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "gate", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<Wired> peers = new ArrayList<>();
    /** The fleet's one content swarm, which every member's discovery reaches. */
    private final MemorySpillStore store = new MemorySpillStore();

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime, DiscoveryService discovery) {
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> p.node().close());
    }

    private Wired peer(String address, int level) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(peers.size() + 11L).appCardLevel(level).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                peers.isEmpty() ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "g0", 0)));
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId()).spillStore(store);
        Wired wired = new Wired(node, identity, runtime, discovery);
        peers.add(wired);
        return wired;
    }

    private boolean until(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
            peers.forEach(p -> p.node().tick());
            clock.advance(Duration.ofMillis(500));
        }
        return condition.getAsBoolean();
    }

    private static Optional<AppCard> cardAt(Wired peer, String id) {
        return peer.discovery().find(AppCard.class, c -> c.id().equals(id)).stream().findFirst();
    }

    private AppCard imported(Wired issuer) {
        return new AppCard("aspace://zGate/app/" + issuer.identity().peerId().value() + "/example-app", issuer.identity().peerId(), groupId,
                clock.instant(), Duration.ofMinutes(15), "example-app", "fed", "static", "example-realm", List.of(), List.of(), "",
                "https://apps.example.org/example-app.html", "", "1", List.of(), null)
                .withHost(new AppCard.Host("https://apps.example.org", 0, "", "/", "idp", "importer", clock.instant(), "serving"))
                .withOrigin(AppCard.Origin.of("realm", "example-realm/example-app", "", Map.of("realm.reads", "Item,Order")))
                .withFormats(List.of("text/html", "application/geo+json"));
    }

    @Test
    void aLevelOneJoinerGetsTheCardWithoutItsGatedFieldsAndTheFullCardReturnsWhenItLeaves() throws Exception {
        Wired a = peer("g0", 2);
        Wired b = peer("g1", 2);
        assertThat(until(() -> a.discovery().appCardGateOpen() && a.runtime().membership().allMembers().size() == 1)).isTrue();
        AppCardPublisher publisher = new AppCardPublisher(a.identity(), a.runtime(), a.discovery());
        AppCard small = imported(a);
        AppCard large = MemorySpillStore.appCardOfSize(a.identity().peerId(), groupId, "board", clock.instant(),
                CardSpill.SPILL_THRESHOLD_BYTES + 500);
        assertThat(publisher.publish(small).join()).isEqualTo(small);
        AppCard stub = publisher.publish(large).join();
        assertThat(stub.spill()).isNotNull();
        assertThat(until(() -> cardAt(b, small.id()).isPresent() && cardAt(b, large.id()).isPresent())).isTrue();
        assertThat(cardAt(b, small.id()).orElseThrow().origin()).isEqualTo(small.origin());
        assertThat(cardAt(b, large.id()).orElseThrow().spill()).isEqualTo(stub.spill());
        assertThat(publisher.appCardsDowngraded()).isZero();

        Wired c = peer("g2", 1);
        assertThat(until(() -> !a.discovery().appCardGateOpen())).isTrue();
        assertThat(until(() -> cardAt(c, small.id()).map(card -> card.origin() == null).orElse(false)
                && cardAt(c, large.id()).map(card -> card.spill() == null).orElse(false))).as("the level-1 member receives both").isTrue();
        AppCard stripped = cardAt(c, small.id()).orElseThrow();
        assertThat(stripped.formats()).isNull();
        assertThat(stripped.host()).as("an idp host reads only at level 2").isNull();
        assertThat(stripped.issued()).isAfter(small.issued());
        assertThat(cardAt(c, large.id()).orElseThrow().views()).as("inline in full").isEqualTo(large.views());
        assertThat(until(() -> cardAt(b, small.id()).map(card -> card.origin() == null).orElse(false))).isTrue();
        assertThat(publisher.appCardsDowngraded()).isGreaterThanOrEqualTo(1);
        assertThat(a.discovery().spiller().cardsOversizeInline()).isGreaterThanOrEqualTo(1);

        c.node().close();
        peers.remove(c);
        assertThat(until(() -> a.discovery().appCardGateOpen())).as("the level-1 member left the view").isTrue();
        assertThat(until(() -> cardAt(b, small.id()).map(card -> small.origin().equals(card.origin())).orElse(false)
                && cardAt(b, large.id()).map(card -> card.spill() != null).orElse(false))).as("the full cards return").isTrue();
        assertThat(cardAt(b, small.id()).orElseThrow().host()).isEqualTo(small.host());
        publisher.close();
    }

    @Test
    void remoteFindAnswersTheOriginAndFormatsKeysFromAStubWithoutFetchingIt() throws Exception {
        Wired a = peer("g0", 2);
        Wired b = peer("g1", 2);
        assertThat(until(() -> a.discovery().appCardGateOpen() && b.discovery().appCardGateOpen()
                && a.runtime().membership().allMembers().size() == 1)).isTrue();
        AppCardPublisher publisher = new AppCardPublisher(a.identity(), a.runtime(), a.discovery());
        AppCard stub = publisher.publish(MemorySpillStore.appCardOfSize(a.identity().peerId(), groupId, "board", clock.instant(),
                CardSpill.SPILL_THRESHOLD_BYTES + 500)).join();
        publisher.publish(imported(a)).join();
        assertThat(b.discovery().remoteFind(AppCard.class, Map.of("origin.system", "console"), Duration.ofSeconds(2)))
                .extracting(AppCard::id).containsExactly(stub.id());
        assertThat(b.discovery().remoteFind(AppCard.class, Map.of("origin.ref", "example-console/*"), Duration.ofSeconds(2)))
                .extracting(AppCard::id).containsExactly(stub.id());
        assertThat(b.discovery().remoteFind(AppCard.class, Map.of("formats", "application/*", "kind", "fed"), Duration.ofSeconds(2)))
                .hasSize(2);
        assertThat(b.discovery().remoteFind(AppCard.class, Map.of("formats", "text/html"), Duration.ofSeconds(2)))
                .extracting(AppCard::name).containsExactly("example-app");
        assertThat(b.discovery().find(AppCard.class, card -> card.fromOrigin("realm", "example-realm/*"))).hasSize(1);
        assertThat(store.gets).as("an answerer never fetches a spill").isEmpty();
        assertThat(b.discovery().resolve(cardAt(b, stub.id()).orElseThrow()).join().views()).hasSize(AppCard.MAX_TERMS);
        publisher.close();
    }
}
