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

package ai.badmonkey.agentspaces.capabilities.learn;

import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.capabilities.runtime.PipeChannel;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * srad/agentspaces-event-loop.md EL-T-04 and owner answer 7 (2026-11-22): on a started loop the learner offers each
 * model once per {@link GossipLearner#OFFER_PERIOD} (1 s), and an outstanding offer is abandoned
 * {@link GossipLearner#PENDING_TIMEOUT} (750 ms) after it went out, judged by time and not by counting runs. The
 * participant does not wake at 750 ms; a frame that arrives after it judges the timeout itself. The participant's run
 * is driven here at the deadlines it returns, as the loop would, with a peer that replies only when told to.
 */
class GossipLearnerTimeoutTest {

    private static final long MS = 1_000_000L;

    /** The learner's wire frame, mirrored field for field. */
    record Frame(String modelId, long token, String kind, long round, byte[] inline, String cid) {
    }

    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<Frame> sent = new ArrayList<>();
    private final PeerId peer = PeerIdentity.generate().peerId();
    private BiConsumer<PeerId, byte[]> inbound;
    private long mono;

    private GossipLearner<double[]> learner() {
        PipeChannel pipe = new PipeChannel() {
            @Override
            public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
                inbound = handler;
            }

            @Override
            public void send(PeerId to, String capabilityType, byte[] payload) {
                sent.add(codec.fromWire(payload, Frame.class));
            }
        };
        GossipLearner<double[]> learner = GossipLearner.builder(pipe, n -> List.of(peer),
                PeerIdentity.generate().peerId(), codec, TestClock.create(), WeightAveraging.INSTANCE).build();
        learner.attach(new DueWork.Driver() {
            @Override
            public void wake() {
            }

            @Override
            public void describeChanged() {
            }

            @Override
            public long monoNanos() {
                return mono;
            }

            @Override
            public long dueAt(long wallMillis) {
                return wallMillis;
            }

            @Override
            public boolean timed() {
                return true;
            }

            @Override
            public TimeSource time() {
                throw new UnsupportedOperationException();
            }
        });
        return learner;
    }

    private void deliver(String kind, long token, double[] model) {
        inbound.accept(peer, codec.toBytes(new Frame("m", token, kind, 0, WeightAveraging.INSTANCE.encode(model), null)));
    }

    private long runAt(GossipLearner<double[]> learner, long at) {
        mono = at;
        return learner.run(at);
    }

    @Test
    void anOfferGoesOutOnceASecondAndALateAcceptIsStale() {
        GossipLearner<double[]> learner = learner();
        assertThat(runAt(learner, 0)).as("no model, nothing due").isEqualTo(DueWork.NEVER);
        learner.start("m", new double[] {1.0});
        assertThat(runAt(learner, 0)).as("the next offer, a second out").isEqualTo(1_000 * MS);
        assertThat(sent).singleElement().satisfies(f -> assertThat(f.kind()).isEqualTo("OFFER"));
        long token = sent.get(0).token();

        // Runs between deadlines (a wake from elsewhere, the backstop) send nothing.
        assertThat(runAt(learner, 500 * MS)).isEqualTo(1_000 * MS);
        assertThat(sent).hasSize(1);

        // The accept arrives at 751 ms: past the timeout, so it is stale and merges nothing.
        mono = 751 * MS;
        deliver("ACCEPT", token, new double[] {3.0});
        assertThat(learner.model("m").orElseThrow()).containsExactly(1.0);

        assertThat(runAt(learner, 1_000 * MS)).isEqualTo(2_000 * MS);
        assertThat(sent).hasSize(2);
        long second = sent.get(1).token();
        // An accept inside the 750 ms merges.
        mono = 1_750 * MS;
        deliver("ACCEPT", second, new double[] {3.0});
        assertThat(learner.model("m").orElseThrow()).containsExactly(2.0);
    }

    @Test
    void anOfferThatArrivesAfterTheTimeoutIsAnsweredNotRefusedAsBusy() {
        GossipLearner<double[]> learner = learner();
        learner.start("m", new double[] {1.0});
        runAt(learner, 0); // our offer is out, and never answered

        mono = 700 * MS; // inside the timeout: our offer is outstanding, so theirs is refused
        deliver("OFFER", 41, new double[] {5.0});
        assertThat(sent.get(sent.size() - 1).kind()).isEqualTo("BUSY");

        mono = 751 * MS; // past it: ours is abandoned, theirs is merged and accepted
        deliver("OFFER", 42, new double[] {5.0});
        assertThat(sent.get(sent.size() - 1)).satisfies(f -> {
            assertThat(f.kind()).isEqualTo("ACCEPT");
            assertThat(f.token()).isEqualTo(42);
        });
        assertThat(learner.model("m").orElseThrow()).containsExactly(3.0);
    }

    @Test
    void aSilentPeerCostsOneOfferAndOneRunASecond() {
        GossipLearner<double[]> learner = learner();
        learner.start("m", new double[] {1.0});
        long now = 0;
        int runs = 0;
        while (now <= 10_000 * MS) {
            long next = runAt(learner, now);
            runs++;
            // Extra runs between deadlines change nothing: the timeout and the period are times.
            runAt(learner, now + 1);
            runAt(learner, now + 2);
            now = next;
        }
        assertThat(sent).as("one offer per second over 10 s (the tick offered every 250 ms)").hasSize(11);
        assertThat(runs).as("one deadline a second").isEqualTo(11);
    }
}
