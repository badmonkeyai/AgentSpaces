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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ISSUE-AppCardOriginFormats FR-15 (SPEC §6.1a.5; AppCard origin and formats, 2026-11-09): {@code resolve} returns a
 * card without {@code spill} as it is and the full card for a stub; a document of the wrong length, an undecodable one,
 * and one whose non-empty fields differ from {@code parts} each fail and count; a {@code size} above the fetch bound
 * fails before any fetch; no store fails the resolution; no failure evicts the cached stub.
 */
class CardSpillResolveTest {

    private final TestClock clock = TestClock.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final SimNetwork network = new SimNetwork();
    private final PeerIdentity identity = PeerIdentity.generate();
    private final MemorySpillStore store = new MemorySpillStore();
    private PeerNode node;
    private DiscoveryService discovery;
    private AppCard full;
    private AppCard stub;

    @BeforeEach
    void wire() throws Exception {
        node = PeerNode.builder(identity).clock(clock).randomSeed(3).build();
        node.listen(network.register("r"), "r");
        GroupId group = GroupId.of("zResolve");
        GroupRuntime runtime = node.joinGroup(new GroupAdvertisement("aspace://zResolve", identity.peerId(), group, Instant.EPOCH,
                Duration.ofDays(1), "resolve", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults()), GroupMembership.Config.defaults(), List.of());
        discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId()).spillStore(store);
        full = MemorySpillStore.appCardOfSize(identity.peerId(), group, "board", clock.instant(), CardSpill.SPILL_THRESHOLD_BYTES + 40);
        stub = discovery.spiller().prepare(full, true).join();
        discovery.publish(new AdvertisementSigner().sign(stub, identity));
    }

    @AfterEach
    void close() {
        node.close();
    }

    private void fails(AppCard card, String message) {
        assertThatThrownBy(() -> discovery.resolve(card).join()).isInstanceOf(CompletionException.class).hasMessageContaining(message);
        assertThat(discovery.find(AppCard.class, c -> true)).as("the cached stub stays").containsExactly(stub);
    }

    @Test
    void aFullCardIsItselfAndAStubResolvesToTheFullCard() {
        assertThat(discovery.resolve(full).join()).isSameAs(full);
        AppCard cached = discovery.find(AppCard.class, c -> c.fromOrigin("console")).get(0);
        assertThat(cached.spill()).isNotNull();
        assertThat(discovery.resolve(cached).join()).isEqualTo(full);
        assertThat(discovery.spillsRefused()).isZero();
    }

    @Test
    void aDocumentThatFailsACheckFailsTheResolution() {
        store.serve = bytes -> Arrays.copyOf(bytes, bytes.length - 1);
        fails(stub, "does not complete its stub");
        store.serve = bytes -> {
            bytes[0] = (byte) 0xff;
            return bytes;
        };
        fails(stub, "does not complete its stub");
        byte[] other = codec.toBytes(new AppCard.Parts(List.of("zResolve/items"), List.of(), List.of(), null));
        store.serve = bytes -> Arrays.copyOf(other, bytes.length);
        fails(stub, "does not complete its stub");
        store.serve = bytes -> bytes;
        // FR-15: a valid document whose length is not the stub's size fails too (AppCard origin and formats, 2026-11-09)
        AppCard misstated = stub.withSpill(new CardSpill(stub.spill().root(), stub.spill().size() + 1, stub.spill().parts()));
        fails(misstated, "does not complete its stub");
        assertThat(discovery.spillsRefused()).isEqualTo(4);
    }

    @Test
    void aStoreThatRefusesTheDocumentForItsLengthCountsARefusal() {
        // a content swarm compares the artifact's length with the stub's size before it fetches a piece, and refuses
        // the document with an IllegalArgumentException; resolve counts that as a failed check (AppCard origin and
        // formats, 2026-11-09)
        discovery.spillStore(new SpillStore() {
            @Override
            public java.util.concurrent.CompletableFuture<String> put(String name, byte[] document) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.concurrent.CompletableFuture<byte[]> get(String root, long size) {
                return java.util.concurrent.CompletableFuture.failedFuture(
                        new IllegalArgumentException("the artifact " + root + " is 9000000 bytes; the stub states " + size));
            }

            @Override
            public void release(String root) {
            }
        });
        fails(stub, "does not complete its stub");
        assertThat(discovery.spillsRefused()).isEqualTo(1);
        discovery.spillStore(new SpillStore() {
            @Override
            public java.util.concurrent.CompletableFuture<String> put(String name, byte[] document) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.concurrent.CompletableFuture<byte[]> get(String root, long size) {
                return java.util.concurrent.CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("no holder"));
            }

            @Override
            public void release(String root) {
            }
        });
        assertThatThrownBy(() -> discovery.resolve(stub).join()).isInstanceOf(CompletionException.class).hasMessageContaining("no holder");
        assertThat(discovery.spillsRefused()).as("a document nobody serves is no failed check").isEqualTo(1);
    }

    @Test
    void aSizeAboveTheFetchBoundFailsBeforeAnyFetch() {
        assertThat(discovery.maxSpillFetchBytes()).isEqualTo(CardSpill.DEFAULT_MAX_FETCH_BYTES);
        discovery.maxSpillFetchBytes(stub.spill().size() - 1);
        fails(stub, "above the fetch bound");
        assertThat(store.gets).isEmpty();
        assertThat(discovery.spillsOverLimit()).isEqualTo(1);
        assertThat(discovery.spillsRefused()).isZero();
    }

    @Test
    void noStoreFailsTheResolutionAndKeepsTheStub() {
        discovery.spillStore(null);
        fails(stub, "no content swarm");
    }

}
