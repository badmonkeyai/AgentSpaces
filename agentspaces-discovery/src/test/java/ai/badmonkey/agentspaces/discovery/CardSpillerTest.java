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
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ISSUE-AppCardOriginFormats FR-12 and FR-13 (SPEC §6.1a.5; AppCard origin and formats, 2026-11-09): a card of 65,536
 * bytes travels inline and one of 65,537 spills; a card past 57,344 bytes warns once per id and size; a closed gate or
 * no store sends the full card inline; a refresh that changes no part reuses the root; the root is released one
 * {@code ttl} after the last card that named it.
 */
class CardSpillerTest {

    private final CborCodec codec = CborCodec.defaultCodec();
    private final TestClock clock = TestClock.create();
    private final PeerId issuer = PeerId.of("zPeerA");
    private final GroupId group = GroupId.of("zApps");
    private final MemorySpillStore store = new MemorySpillStore();
    private final CardSpiller spiller = new CardSpiller(codec, clock);
    private final List<LogRecord> warnings = new CopyOnWriteArrayList<>();
    private final Logger log = Logger.getLogger(CardSpiller.class.getName());
    private final Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            warnings.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @BeforeEach
    void listen() {
        log.addHandler(handler);
        spiller.store(store);
    }

    @AfterEach
    void stopListening() {
        log.removeHandler(handler);
    }

    private AppCard card(String name, int size) {
        AppCard card = MemorySpillStore.appCardOfSize(issuer, group, name, clock.instant(), size);
        assertThat(codec.toBytes(card)).hasSize(size);
        return card;
    }

    @Test
    void aCardAtTheThresholdTravelsInlineAndOneByteMoreSpills() {
        AppCard atThreshold = card("at", CardSpill.SPILL_THRESHOLD_BYTES);
        assertThat(spiller.prepare(atThreshold, true).join()).isSameAs(atThreshold);
        assertThat(store.puts).isEmpty();
        AppCard over = card("over", CardSpill.SPILL_THRESHOLD_BYTES + 1);
        AppCard stub = spiller.prepare(over, true).join();
        assertThat(stub.spill()).isNotNull();
        assertThat(stub.spill().parts()).containsExactly("reads", "actions", "views", "ui");
        assertThat(stub.origin()).isEqualTo(over.origin());
        assertThat(stub.formats()).isEqualTo(over.formats());
        assertThat(codec.toBytes(stub).length).isLessThan(1_024);
        assertThat(store.documents.get(stub.spill().root())).isEqualTo(codec.toBytes(over.parts()));
        assertThat(spiller.cardsSpilled()).isEqualTo(1);
        assertThat(spiller.cardsOversizeInline()).isZero();
        assertThat(spiller.oversized(over)).isTrue();
        assertThat(spiller.oversized(atThreshold)).isFalse();
    }

    @Test
    void aCardPastTheWarningWarnsOncePerIdAndSize() {
        AppCard near = card("near", CardSpill.WARN_THRESHOLD_BYTES + 1);
        spiller.prepare(near, true).join();
        spiller.prepare(near, true).join();
        assertThat(warnings).hasSize(1);
        String message = java.text.MessageFormat.format(warnings.get(0).getMessage(), warnings.get(0).getParameters());
        assertThat(message).contains(near.id()).contains("kind fed").contains("57,345").contains("65,536").contains("travels inline");
        spiller.prepare(card("near", CardSpill.WARN_THRESHOLD_BYTES + 2), true).join();
        assertThat(warnings).as("a new size warns again").hasSize(2);
        spiller.prepare(card("quiet", CardSpill.WARN_THRESHOLD_BYTES), true).join();
        assertThat(warnings).as("at the warning threshold itself nothing warns").hasSize(2);
        spiller.prepare(card("big", CardSpill.SPILL_THRESHOLD_BYTES + 10), true).join();
        assertThat(java.text.MessageFormat.format(warnings.get(2).getMessage(), warnings.get(2).getParameters()))
                .contains("spills its large parts");
    }

    @Test
    void aClosedGateOrNoStoreSendsTheFullCardInline() {
        AppCard over = card("over", CardSpill.SPILL_THRESHOLD_BYTES + 1);
        assertThat(spiller.prepare(over, false).join()).isSameAs(over);
        assertThat(spiller.cardsOversizeInline()).isEqualTo(1);
        spiller.store(null);
        assertThat(spiller.prepare(over, true).join()).isSameAs(over);
        assertThat(spiller.cardsOversizeInline()).isEqualTo(2);
        assertThat(spiller.cardsSpilled()).isZero();
        assertThat(store.puts).isEmpty();
        assertThat(java.text.MessageFormat.format(warnings.get(0).getMessage(), warnings.get(0).getParameters()))
                .contains("below the AppCard level, no content swarm serves the group");
    }

    @Test
    void aStoreThatFailsOrACardWithNothingToMoveSendsTheFullCardInline() {
        spiller.store(new SpillStore() {
            @Override
            public java.util.concurrent.CompletableFuture<String> put(String name, byte[] document) {
                return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("the swarm is down"));
            }

            @Override
            public java.util.concurrent.CompletableFuture<byte[]> get(String root, long size) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void release(String root) {
            }
        });
        AppCard over = card("over", CardSpill.SPILL_THRESHOLD_BYTES + 1);
        assertThat(spiller.prepare(over, true).join()).as("a failed put leaves the card inline").isSameAs(over);
        assertThat(spiller.cardsOversizeInline()).isEqualTo(1);
        spiller.store(store);
        AppCard named = new AppCard("aspace://zApps/app/zPeerA/named", issuer, group, clock.instant(), Duration.ofMinutes(15),
                "n".repeat(CardSpill.SPILL_THRESHOLD_BYTES), "fed", "static", "", List.of(), List.of(), "", "", "", "1", List.of(), null);
        assertThat(spiller.prepare(named, true).join()).as("no spill-set part to move").isSameAs(named);
        assertThat(spiller.cardsOversizeInline()).isEqualTo(2);
        assertThat(spiller.cardsSpilled()).isZero();
        assertThat(store.puts).isEmpty();
    }

    @Test
    void aRefreshReusesTheRootAndTheRootLapsesOneTtlAfterTheLastCardThatNamedIt() {
        AppCard first = card("board", CardSpill.SPILL_THRESHOLD_BYTES + 100);
        String root = spiller.prepare(first, true).join().spill().root();
        clock.advance(Duration.ofMinutes(10));
        AppCard refresh = MemorySpillStore.appCardOfSize(issuer, group, "board", clock.instant(), CardSpill.SPILL_THRESHOLD_BYTES + 100);
        assertThat(spiller.prepare(refresh, true).join().spill().root()).as("no part changed").isEqualTo(root);
        assertThat(store.documents).hasSize(1);
        assertThat(spiller.heldRoots()).containsExactly(root);
        clock.advance(Duration.ofMinutes(10));
        spiller.sweep();
        assertThat(store.released).as("the refresh still names it").isEmpty();
        clock.advance(Duration.ofMinutes(5));
        spiller.sweep();
        assertThat(store.released).containsExactly(root);
        assertThat(spiller.heldRoots()).isEmpty();
    }
}
