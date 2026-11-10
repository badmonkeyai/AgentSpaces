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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.pdt.stores.Stores;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 follow-ups, 2026-11-09, F2: a store whose policy is {@code grant} and which has no authorizer starts,
 * listens, and waits. It takes no pins, which stay for a store that can admit them, its card says it waits for an
 * authorizer, it logs that once, and it starts admitting when an authorizer appears.
 */
class GrantWithoutAuthorizerTest {

    private final StoreFleet fleet = new StoreFleet();
    private final Logger log = Logger.getLogger(ContentStore.class.getName());
    private final List<LogRecord> logged = new CopyOnWriteArrayList<>();
    private final Handler capture = new Handler() {
        @Override
        public void publish(LogRecord record) {
            logged.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @AfterEach
    void tearDown() {
        log.removeHandler(capture);
        fleet.close();
    }

    private List<AssetCard> cards(StoreFleet.Peer peer) {
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind")));
    }

    @Test
    void aGrantStoreWithoutAnAuthorizerWaitsAndAdmitsWhenOneAppears() throws Exception {
        log.addHandler(capture);
        StoreFleet.Peer store = fleet.newPeer("store", 81, StoreFleet.Spec.store(StorePolicy.of(1L << 30)).withoutAuthorizer());
        StoreFleet.Peer writer = fleet.newPeer("writer", 82, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(store.contentStore().waiting()).isTrue();
        assertThat(store.contentStore().status().waiting()).isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> cards(writer).size() == 1)).isTrue();
        assertThat(cards(writer).get(0).access()).containsEntry(Stores.WAITING_ACCESS, "authorizer")
                .containsEntry("policy", "grant");

        // the requester's chooser passes a waiting store by, and a pin written anyway stays untaken
        String root = StoreFleet.facts(writer, "waiting", 300, 8).root();
        assertThatThrownBy(() -> writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofHours(1)))
                .hasMessageContaining("no content store fits");
        writer.content().holdTree(root);
        fleet.writePin(writer, new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(),
                Map.of(Stores.STORES_ATTR, store.id().value())), Duration.ofMinutes(10));
        var invoked = store.contentStore().invoke(store.contentStore().assetName(), ContentStore.OP_PIN,
                Map.of("root", root), "pin-1", writer.id());
        assertThat(invoked.ok()).isFalse();
        assertThat(invoked.result().get("detail")).contains("waits for an authorizer");
        fleet.runFor(Duration.ofSeconds(3));
        assertThat(store.contentStore().ledger().holds(root)).as("a waiting store takes no pins").isFalse();
        assertThat(store.contentStore().status().refused()).as("waiting is no refusal").isZero();
        assertThat(writer.contentSpace().readAll(Template.of(SignedPin.class), 4)).as("the pin stays").hasSize(1);
        assertThat(logged.stream().filter(r -> r.getMessage().contains("no authorizer")).count()).as("logged once")
                .isEqualTo(1);

        // an authorizer appears: the store admits the waiting pin and its card stops saying it waits
        store.contentStore().authorizer(StoreFleet.GRANT_ALL);
        assertThat(store.contentStore().waiting()).isFalse();
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> store.contentStore().ledger().holds(root))).isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> cards(writer).stream()
                .noneMatch(c -> c.access().containsKey(Stores.WAITING_ACCESS)))).isTrue();
        assertThat(logged.stream().filter(r -> r.getMessage().contains("no authorizer")).count()).isEqualTo(1);
    }

    @Test
    void anAuthorizerThatAppearsStillDecidesAndOneThatFailsAdmitsNothingAndCostsOneScanAPause() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 83, StoreFleet.Spec.store(StorePolicy.of(1L << 30)).withoutAuthorizer());
        StoreFleet.Peer writer = fleet.newPeer("writer", 84, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> cards(writer).size() == 1)).isTrue();
        String root = StoreFleet.facts(writer, "refused", 300, 11).root();
        writer.content().holdTree(root);
        fleet.writePin(writer, new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(),
                Map.of(Stores.STORES_ATTR, store.id().value())), Duration.ofMinutes(10));

        // the authorizer that appears refuses the writer: the store stops waiting and still takes nothing
        java.util.concurrent.atomic.AtomicBoolean gone = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.atomic.AtomicInteger asked = new java.util.concurrent.atomic.AtomicInteger();
        store.contentStore().authorizer((peer, operation, scope) -> {
            if (operation != ai.badmonkey.agentspaces.api.spi.Authorizer.Operation.CONTENT_FETCH) {
                return true;
            }
            asked.incrementAndGet();
            if (gone.get()) {
                throw new IllegalStateException("the policy service is gone");
            }
            return false;
        });
        assertThat(store.contentStore().waiting()).isFalse();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> cards(writer).stream()
                .noneMatch(c -> c.access().containsKey(Stores.WAITING_ACCESS)))).isTrue();
        fleet.runFor(Duration.ofSeconds(1));
        assertThat(store.contentStore().ledger().holds(root)).as("the authorizer refuses the writer").isFalse();

        // its backing service goes away and every question throws: the store admits nothing and backs off
        gone.set(true);
        asked.set(0);
        fleet.runFor(Duration.ofSeconds(2));
        assertThat(store.contentStore().ledger().holds(root)).isFalse();
        assertThat(writer.contentSpace().readAll(Template.of(SignedPin.class), 4)).as("the pin stays").hasSize(1);
        assertThat(asked.get()).as("one question a scan, a scan a pause").isBetween(1, 20);
    }

    @Test
    void anOpenStoreNeedsNoAuthorizer() {
        StorePolicy open = StorePolicy.of(1L << 30).acceptRequesters(StorePolicy.Accept.OPEN);
        assertThat(ContentStore.card(fleet.groupId, fleet.groupAd.issuer(), java.time.Instant.EPOCH, Duration.ofMinutes(1),
                "open", open, 1, false, Duration.ofSeconds(10), false).access()).doesNotContainKey(Stores.WAITING_ACCESS);
    }
}
