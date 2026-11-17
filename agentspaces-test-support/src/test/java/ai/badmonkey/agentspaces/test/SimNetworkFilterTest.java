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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The link filters, gates and delays of srad/spoiler-peers.md SP-N-01 and SP-N-02 on the in-process fabric. */
class SimNetworkFilterTest {

    /** A frame whose "kind" is its first word, so the tests need no envelope. */
    private static final FrameDecoder WORDS = frame -> {
        String text = new String(frame, StandardCharsets.UTF_8);
        int space = text.indexOf(' ');
        return Optional.of(new FrameHeader(space < 0 ? text : text.substring(0, space), "a", null, -1, List.of(), List.of()));
    };

    private final SimNetwork network = new SimNetwork().decoders(WORDS, null);
    private final List<String> inbox = new CopyOnWriteArrayList<>();
    private TransportConnection toBob;

    private void connect() throws Exception {
        Transport alice = network.register("alice");
        Transport bob = network.register("bob");
        bob.listen("bob", c -> c.onReceive(f -> inbox.add(new String(f, StandardCharsets.UTF_8))));
        toBob = alice.dial("bob");
    }

    private void send(String text) throws Exception {
        toBob.send(text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void aFilterDropsOnlyTheKindItMatches() throws Exception {
        connect();
        network.filter("alice", "bob", f -> "RUMOR".equals(f.kind()) ? LinkFilter.Verdict.drop() : LinkFilter.Verdict.pass());
        send("RUMOR one");
        send("PING two");
        assertThat(inbox).containsExactly("PING two");
        assertThat(network.filteredDrops()).isEqualTo(1);
    }

    @Test
    void aGateHoldsUntilReleasedAndTheFollowUpRunsAfterTheFrameItself() throws Exception {
        connect();
        List<String> order = new CopyOnWriteArrayList<>();
        network.filter(null, null, f -> switch (f.kind()) {
            case "CLAIM" -> LinkFilter.Verdict.hold("claims");
            // the release runs after the completion is delivered, never from inside the filter
            case "COMPLETION" -> LinkFilter.Verdict.pass().then(() -> {
                order.add("release after " + inbox);
                network.releaseGate("claims", SimNetwork.Order.FIFO, 0);
            });
            default -> LinkFilter.Verdict.pass();
        });
        send("CLAIM 1");
        send("CLAIM 2");
        assertThat(inbox).isEmpty();
        assertThat(network.gated("claims")).extracting(LinkFrame::kind).containsExactly("CLAIM", "CLAIM");
        send("COMPLETION 3");
        assertThat(order).containsExactly("release after [COMPLETION 3]");
        assertThat(inbox).containsExactly("COMPLETION 3", "CLAIM 1", "CLAIM 2");
        assertThat(network.gated("claims")).isEmpty();
    }

    @Test
    void theMostRestrictiveVerdictWinsAndDuplicatesApplyToWhatPasses() throws Exception {
        connect();
        network.filter(null, null, f -> LinkFilter.Verdict.duplicate(2));
        AutoCloseable dropper = network.filter(null, null, f -> "DROP".equals(f.kind()) ? LinkFilter.Verdict.drop() : LinkFilter.Verdict.pass());
        send("DROP me");
        send("KEEP me");
        assertThat(inbox).containsExactly("KEEP me", "KEEP me");
        dropper.close();
        send("DROP now");
        assertThat(inbox).hasSize(4);
        LinkFilter.Verdict hold = LinkFilter.Verdict.hold("g");
        assertThat(LinkFilter.Verdict.delay(5).combine(hold).action()).isEqualTo(LinkFilter.Verdict.Action.HOLD);
        assertThat(hold.combine(LinkFilter.Verdict.drop()).action()).isEqualTo(LinkFilter.Verdict.Action.DROP);
    }

    @Test
    void aDelayReleasesOnTheFirstPumpPastItsSpanOfFleetClock() throws Exception {
        TestClock clock = TestClock.create();
        network.clock(clock);
        connect();
        network.delay("alice", "bob", 1_000);
        send("PING late");
        clock.advance(Duration.ofMillis(999));
        assertThat(network.pump()).isZero();
        assertThat(inbox).isEmpty();
        clock.advance(Duration.ofMillis(1));
        assertThat(network.pump()).isEqualTo(1);
        assertThat(inbox).containsExactly("PING late");
        assertThat(network.delayedCount()).isZero();
    }

    @Test
    void aDelayNeedsTheFleetClock() {
        assertThatThrownBy(() -> new SimNetwork().delay(null, null, 1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void withNoFilterTheOlderHoldStillQueuesTheDirection() throws Exception {
        connect();
        network.hold("alice", "bob");
        send("PING held");
        assertThat(inbox).isEmpty();
        assertThat(network.release("alice", "bob", SimNetwork.Order.FIFO, 0)).isEqualTo(1);
        assertThat(inbox).containsExactly("PING held");
    }

    @Test
    void envelopeHeadersReadKindStreamAndTheFramesABatchCarries() {
        CborCodec codec = CborCodec.defaultCodec();
        byte[] rumorBody = codec.toBytes(Map.of("streamId", "space:refs", "itemId", "i", "hopsRemaining", 3, "payload", new byte[0]));
        Map<String, Object> rumor = Map.of("ver", 3, "group", "g", "kind", "RUMOR", "from", "zA", "stamp", "1700:0:zA",
                "body", rumorBody);
        byte[] batchBody = codec.toBytes(Map.of("frames", List.of(codec.toBytes(rumor))));
        Map<String, Object> batch = Map.of("ver", 3, "group", "g", "kind", "BATCH", "from", "zA", "to", "zB",
                "stamp", "1800:2:zA", "body", batchBody);
        byte[] frame = codec.toBytes(Map.of("envelope", batch, "signature", new byte[32]));
        FrameHeader header = new EnvelopeHeaders().header(frame).orElseThrow();
        assertThat(header.kind()).isEqualTo("BATCH");
        assertThat(header.recipient()).isEqualTo("zB");
        assertThat(header.stampMillis()).isEqualTo(1800);
        assertThat(header.inner()).singleElement().satisfies(h -> {
            assertThat(h.kind()).isEqualTo("RUMOR");
            assertThat(h.streams()).containsExactly("space:refs");
        });
        assertThat(header.carriesKind("RUMOR")).isTrue();
        assertThat(header.carriesStream("space:refs")).isTrue();
        assertThat(new EnvelopeHeaders().header("not cbor".getBytes(StandardCharsets.UTF_8))).isEmpty();
    }

    @Test
    void theOracleNamesTheFirstBrokenInvariant() {
        Map<String, Long> drops = new java.util.HashMap<>(Map.of("stale", 1L));
        FleetProbe probe = new FleetProbe() {
            @Override
            public Set<String> members() {
                return Set.of("a", "b");
            }

            @Override
            public Map<String, String> roots(String member) {
                return Map.of("space:x", member.equals("a") ? "r1" : "r2");
            }

            @Override
            public Map<String, Long> drops(String member) {
                return member.equals("a") ? drops : Map.of();
            }

            @Override
            public Set<String> punished(String member) {
                return member.equals("b") ? Set.of("a") : Set.of();
            }

            @Override
            public List<Throwable> errors(String member) {
                return List.of();
            }
        };
        FleetInvariants oracle = FleetInvariants.baseline(probe);
        drops.put("stale", 3L);
        assertThat(oracle.broken(Map.of("stale", 2L), true)).hasSize(2)
                .anySatisfy(line -> assertThat(line).contains("b holds roots"))
                .anySatisfy(line -> assertThat(line).contains("struck or quarantined"));
        assertThat(oracle.broken(Map.of(), false)).anySatisfy(line -> assertThat(line).contains("frame drops"));
        assertThatThrownBy(() -> oracle.assertHolds(Map.of("stale", 2L), false)).hasMessageContaining("b holds roots");
    }
}
