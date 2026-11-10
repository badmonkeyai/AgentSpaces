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
package ai.badmonkey.agentspaces.content.tree.sync;

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 5, P2-K-02 and P2-K-05: the asker's round loop of a reconciliation over {@code TREE_WANT.sketch} and
 * {@code TREE_END.sketch}, against the holder's {@link SketchServer}, by negentropy and by RIBLT with its fallback to
 * negentropy.
 */
class SketchSessionTest {

    private static final PeerId HOLDER = PeerIdentity.generate().peerId();
    private static final PeerId ASKER = PeerIdentity.generate().peerId();

    static String cid(String universe, int i) {
        return Cids.of((universe + ":" + i).getBytes(StandardCharsets.UTF_8), i % 3 == 0 ? Cids.RAW : Cids.DAG_CBOR);
    }

    /** Two sets of {@code n} CIDs whose differences sit at random, half on each side. */
    record Pair(List<String> asker, List<String> holder, Set<String> askerOnly, Set<String> holderOnly) {
        static Pair scattered(int n, int differences, long seed) {
            Random random = new Random(seed);
            Set<Integer> askerOnly = new java.util.HashSet<>();
            Set<Integer> holderOnly = new java.util.HashSet<>();
            while (askerOnly.size() < differences / 2) {
                askerOnly.add(random.nextInt(n));
            }
            while (holderOnly.size() < differences - differences / 2) {
                int i = random.nextInt(n);
                if (!askerOnly.contains(i)) {
                    holderOnly.add(i);
                }
            }
            List<String> asker = new ArrayList<>();
            List<String> holder = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (!holderOnly.contains(i)) {
                    asker.add(cid("u", i));
                }
                if (!askerOnly.contains(i)) {
                    holder.add(cid("u", i));
                }
            }
            return new Pair(asker, holder, map(askerOnly), map(holderOnly));
        }

        private static Set<String> map(Set<Integer> indexes) {
            Set<String> out = new java.util.HashSet<>();
            indexes.forEach(i -> out.add(cid("u", i)));
            return out;
        }
    }

    /** Hands each want to the holder at once and the answer back, as a loss-free link does. */
    static final class Link implements SketchSession.Fabric {
        final SketchServer server;
        final AtomicLong now = new AtomicLong(1_000);
        SketchSession session;
        int wants;

        Link(SketchServer server) {
            this.server = server;
        }

        @Override
        public long nowMillis() {
            return now.get();
        }

        @Override
        public int send(PeerId to, Messages.TreeWant want) {
            wants++;
            SketchServer.Answer answer = server.answer(ASKER, want.req(), want.sketch(), want.budget());
            if (answer instanceof SketchServer.Reply reply) {
                session.onEnd(to, new Messages.TreeEnd(Messages.TREE_END, want.req(), List.of(), Messages.END_SKETCH, reply.sketch()),
                        reply.sketch().length);
            } else if (answer instanceof SketchServer.Refusal refusal) {
                session.onReject(to, new Messages.Reject("", Messages.TREE_INDEX, refusal.reason()));
            }
            return want.sketch().length;
        }
    }

    static SketchSession.Outcome run(List<String> asker, List<String> holder, int expected) throws Exception {
        SketchServer server = new SketchServer(() -> holder, System::currentTimeMillis);
        Link link = new Link(server);
        SketchSession session = new SketchSession(HOLDER, 7, SketchSet.of(asker), expected, new SecureRandom(), link);
        link.session = session;
        session.start();
        assertThat(session.outcome()).isDone();
        return session.outcome().get();
    }

    /** A sketch carries digests, so the asker names a needed CID by its digest until it resolves the codec. */
    static List<String> digests(java.util.Collection<String> cids) {
        return cids.stream().map(c -> java.util.HexFormat.of().formatHex(Cids.parse(c).digest())).toList();
    }

    @Test
    void negentropyFindsBothSidesOfAScatteredDifferenceWithinTheRoundBound() throws Exception {
        Pair pair = Pair.scattered(20_000, 400, 3);
        SketchSession.Outcome outcome = run(pair.asker(), pair.holder(), 0);
        assertThat(outcome.encoding()).isEqualTo("negentropy");
        assertThat(outcome.have()).containsExactlyInAnyOrderElementsOf(pair.askerOnly());
        assertThat(digests(outcome.need())).containsExactlyInAnyOrderElementsOf(digests(pair.holderOnly()));
        assertThat(outcome.rounds()).isLessThanOrEqualTo(SketchServer.MAX_ROUNDS);
    }

    @Test
    void ribltDecodesASmallDifferenceInOneOrTwoRounds() throws Exception {
        Pair pair = Pair.scattered(20_000, 100, 4);
        SketchSession.Outcome outcome = run(pair.asker(), pair.holder(), 100);
        assertThat(outcome.encoding()).isEqualTo("riblt");
        assertThat(outcome.have()).containsExactlyInAnyOrderElementsOf(pair.askerOnly());
        assertThat(digests(outcome.need())).containsExactlyInAnyOrderElementsOf(digests(pair.holderOnly()));
        assertThat(outcome.rounds()).isLessThanOrEqualTo(2);
    }

    @Test
    void ribltPastTwiceTheExpectedDifferenceFallsBackToNegentropy() throws Exception {
        Pair pair = Pair.scattered(5_000, 600, 5);
        SketchSession.Outcome outcome = run(pair.asker(), pair.holder(), 20);
        assertThat(outcome.encoding()).as("the cap of 2 x 20 symbols stops RIBLT").isEqualTo("riblt,negentropy");
        assertThat(outcome.have()).containsExactlyInAnyOrderElementsOf(pair.askerOnly());
        assertThat(digests(outcome.need())).containsExactlyInAnyOrderElementsOf(digests(pair.holderOnly()));
    }

    @Test
    void equalSetsTakeOneRoundAndAHolderThatRefusesFailsTheSession() throws Exception {
        List<String> same = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            same.add(cid("s", i));
        }
        SketchSession.Outcome outcome = run(same, same, 0);
        assertThat(outcome.rounds()).isEqualTo(1);
        assertThat(outcome.have()).isEmpty();
        assertThat(outcome.need()).isEmpty();

        SketchSession refused = new SketchSession(HOLDER, 8, SketchSet.of(same), 0, new SecureRandom(), new SketchSession.Fabric() {
            @Override
            public long nowMillis() {
                return 0;
            }

            @Override
            public int send(PeerId to, Messages.TreeWant want) {
                return 0;
            }
        });
        refused.start();
        assertThat(refused.onReject(ASKER, new Messages.Reject("", Messages.TREE_INDEX, Messages.REASON_POLICY)))
                .as("a refusal from another member is not this session's").isFalse();
        assertThat(refused.onReject(HOLDER, new Messages.Reject("", Messages.TREE_INDEX, Messages.REASON_POLICY))).isTrue();
        assertThat(refused.outcome()).isCompletedExceptionally();
    }

    @Test
    void aSilentHolderTimesTheSessionOut() {
        SketchSession silent = new SketchSession(HOLDER, 9, SketchSet.of(List.of(cid("t", 1))), 0, new SecureRandom(),
                new SketchSession.Fabric() {
                    @Override
                    public long nowMillis() {
                        return 0;
                    }

                    @Override
                    public int send(PeerId to, Messages.TreeWant want) {
                        return want.sketch().length;
                    }
                });
        silent.start();
        silent.tick(SketchSession.ROUND_TIMEOUT_MILLIS - 1);
        assertThat(silent.outcome()).isNotDone();
        silent.tick(SketchSession.ROUND_TIMEOUT_MILLIS + 1);
        assertThat(silent.outcome()).isCompletedExceptionally();
    }
}
