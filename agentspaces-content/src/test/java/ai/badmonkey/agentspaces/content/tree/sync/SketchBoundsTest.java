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

import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * PDT 2 phase 5, P2-Q-SEC-04 and P2-K-04 (threat 4 of §6.7): a sketch message cannot exhaust a holder's memory or
 * rounds. A sketch past 64 KiB, an unknown leading byte, and a message outside the negentropy or RIBLT grammar are
 * dropped by name; a session past 64 rounds is refused; a RIBLT stream out of order is dropped, the holder keeps at
 * most a few RIBLT sessions, and a stream past its symbol cap ends with {@link Riblt#TOO_MANY}.
 */
class SketchBoundsTest {

    private static final PeerId ASKER = PeerIdentity.generate().peerId();

    private static List<String> set(int n) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(SketchSessionTest.cid("b", i));
        }
        return out;
    }

    private final AtomicLong clock = new AtomicLong(1_000);
    private final SketchServer server = new SketchServer(() -> set(2_000), clock::get);

    private static String dropped(SketchServer.Answer answer) {
        assertThat(answer).isInstanceOf(SketchServer.Dropped.class);
        return ((SketchServer.Dropped) answer).why();
    }

    @Test
    void aSketchPastSixtyFourKibAnUnknownEncodingAndABrokenGrammarAreDroppedByName() {
        byte[] large = new byte[SketchServer.MAX_SKETCH_BYTES + 1];
        large[0] = 0x61;
        assertThat(dropped(server.answer(ASKER, 1, large, SketchServer.MAX_SKETCH_BYTES))).contains("65536");
        assertThat(dropped(server.answer(ASKER, 2, new byte[]{0x40, 1}, SketchServer.MAX_SKETCH_BYTES))).contains("0x40");
        assertThat(dropped(server.answer(ASKER, 3, new byte[0], SketchServer.MAX_SKETCH_BYTES))).contains("empty");
        assertThat(dropped(server.answer(ASKER, 4, new byte[]{0x61, 0x00, 0x00, 0x09}, SketchServer.MAX_SKETCH_BYTES)))
                .contains("unexpected mode");
        assertThat(dropped(server.answer(ASKER, 5, new byte[]{0x62, 1, 2}, SketchServer.MAX_SKETCH_BYTES))).contains("prematurely");
        // another negentropy version is answered with the version byte, as the protocol's upgrade rule says
        assertThat(server.answer(ASKER, 6, new byte[]{0x63}, SketchServer.MAX_SKETCH_BYTES))
                .isEqualTo(new SketchServer.Reply(new byte[]{0x61}));
    }

    @Test
    void aSessionPastSixtyFourRoundsIsRefused() {
        byte[] first = new Negentropy(SketchSet.of(set(10)).storage(), 0).initiate();
        for (int round = 0; round < SketchServer.MAX_ROUNDS; round++) {
            assertThat(server.answer(ASKER, 9, first, SketchServer.MAX_SKETCH_BYTES)).isInstanceOf(SketchServer.Reply.class);
        }
        assertThat(server.answer(ASKER, 9, first, SketchServer.MAX_SKETCH_BYTES))
                .isEqualTo(new SketchServer.Refusal(Messages.REASON_OVER_BUDGET, "a session takes at most 64 rounds"));
        assertThat(server.answer(ASKER, 10, first, SketchServer.MAX_SKETCH_BYTES)).as("another session starts afresh")
                .isInstanceOf(SketchServer.Reply.class);
    }

    private static byte[] ribltQuery(byte[] key, long start, List<Riblt.Symbol> symbols) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(Riblt.ENCODING);
        out.writeBytes(key);
        Negentropy.writeVarInt(out, start);
        Negentropy.writeVarInt(out, symbols.size());
        symbols.forEach(s -> s.write(out));
        return out.toByteArray();
    }

    @Test
    void aRibltStreamOutOfOrderIsDroppedAndTheHolderKeepsFewSessions() {
        byte[] key = new byte[Riblt.KEY_BYTES];
        Riblt.Encoder encoder = new Riblt.Encoder(key, SketchSet.of(set(1_000)).ids());
        List<Riblt.Symbol> symbols = encoder.next(8);
        assertThat(server.answer(ASKER, 20, ribltQuery(key, 0, symbols.subList(0, 4)), SketchServer.MAX_SKETCH_BYTES))
                .isInstanceOf(SketchServer.Reply.class);
        assertThat(dropped(server.answer(ASKER, 20, ribltQuery(key, 5, symbols.subList(5, 8)), SketchServer.MAX_SKETCH_BYTES)))
                .contains("index 5");
        for (int s = 1; s < SketchServer.RIBLT_SESSIONS; s++) {
            byte[] other = new byte[Riblt.KEY_BYTES];
            other[0] = (byte) s;
            assertThat(server.answer(ASKER, 20 + s, ribltQuery(other, 0, List.of()), SketchServer.MAX_SKETCH_BYTES))
                    .isInstanceOf(SketchServer.Reply.class);
        }
        byte[] oneTooMany = new byte[Riblt.KEY_BYTES];
        oneTooMany[0] = 99;
        assertThat(server.answer(ASKER, 40, ribltQuery(oneTooMany, 0, List.of()), SketchServer.MAX_SKETCH_BYTES))
                .isInstanceOf(SketchServer.Refusal.class);
        clock.addAndGet(SketchServer.RIBLT_IDLE_MILLIS + 1);
        assertThat(server.answer(ASKER, 41, ribltQuery(oneTooMany, 0, List.of()), SketchServer.MAX_SKETCH_BYTES))
                .as("idle sessions lapse").isInstanceOf(SketchServer.Reply.class);
    }

    /**
     * PDT 2 phase 5 adversary, P2-Q-SEC-04: an asker knows its own session key, so it can make a symbol look pure. Symbol
     * 0 carries one id more than the holder's set, and the id's next index carries none, so peeling the id at 0 leaves
     * it negated at its next index, peeling it there restores it at 0, and the decoder looped for ever, its lists
     * growing until the holder ran out of memory. A coding of two sets peels at most one id per symbol.
     */
    @Test
    void aRibltStreamThatPeelsAnIdTwiceIsDroppedAndFreesItsSession() {
        byte[] key = new byte[Riblt.KEY_BYTES];
        key[0] = 7;
        byte[] stray = Digests.sha256("stray".getBytes(StandardCharsets.UTF_8));
        List<Riblt.Symbol> alone = new Riblt.Encoder(key, List.of(stray)).next(64);
        int next = 1;
        while (alone.get(next).count() == 0) {
            next++;
        }
        List<Riblt.Symbol> forged = new ArrayList<>(new Riblt.Encoder(key, SketchSet.of(set(2_000)).ids()).next(next + 1));
        Riblt.Symbol first = forged.get(0);
        byte[] sum = first.sum().clone();
        for (int i = 0; i < sum.length; i++) {
            sum[i] ^= stray[i];
        }
        forged.set(0, new Riblt.Symbol(sum, first.checksum() ^ alone.get(0).checksum(), first.count() + 1));

        SketchServer.Answer answer = assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> server.answer(ASKER, 60, ribltQuery(key, 0, forged), SketchServer.MAX_SKETCH_BYTES));
        assertThat(dropped(answer)).contains("peel");
        assertThat(server.answer(ASKER, 61, ribltQuery(key, 0, List.of()), SketchServer.MAX_SKETCH_BYTES))
                .as("the session is gone, so the key starts afresh").isInstanceOf(SketchServer.Reply.class);
    }

    @Test
    void aRibltStreamPastItsSymbolCapEndsWithTooMany() {
        byte[] key = new byte[Riblt.KEY_BYTES];
        List<String> other = new ArrayList<>();
        for (int i = 0; i < 40_000; i++) {
            other.add(SketchSessionTest.cid("z", i));
        }
        Riblt.Encoder encoder = new Riblt.Encoder(key, SketchSet.of(other).ids());
        int batch = SketchSession.SYMBOLS_PER_QUERY;
        SketchServer.Answer last = null;
        for (long start = 0; start <= SketchServer.MAX_RIBLT_SYMBOLS; start += batch) {
            last = server.answer(ASKER, 50, ribltQuery(key, start, encoder.next(batch)), SketchServer.MAX_SKETCH_BYTES);
            assertThat(last).isInstanceOf(SketchServer.Reply.class);
            if (((SketchServer.Reply) last).sketch()[1] != Riblt.MORE) {
                break;
            }
        }
        assertThat(((SketchServer.Reply) last).sketch()).containsExactly(Riblt.ENCODING, Riblt.TOO_MANY);
        assertThat(new SketchSession(ASKER, 1, SketchSet.of(other), 1, new SecureRandom(), null).symbolCap())
                .as("the asker caps at 2 x the expected difference, at least 64").isEqualTo(64);
    }
}
