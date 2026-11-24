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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The phase-2 gate of srad/agentspaces-event-loop.md (§10.15): learners whose models start at one instant must not
 * stay in step. On a started loop each learner offers once per {@link GossipLearner#OFFER_PERIOD}, counted from its
 * last offer, so three learners trained back to back (example 14) offer at the same instant every second; their
 * offers cross on the wire, each finds its own offer outstanding and answers BUSY, and no offer is ever accepted. The
 * tick hid this: each node's ticker ran on its own phase. Here the wire is scripted so that every frame of an instant
 * lands after every learner's run of that instant, the TCP race made certain: before the fix the fleet never merged;
 * a refused offer now retries at a random point inside the next half period, which breaks the step.
 */
class GossipLearnerBusyTest {

    private static final long MS = 1_000_000L;

    private final CborCodec codec = CborCodec.defaultCodec();
    private long mono;

    private record Wire(PeerId from, PeerId to, byte[] payload) {
    }

    private final class Node {
        final PeerId id = PeerIdentity.generate().peerId();
        PeerId partner;
        BiConsumer<PeerId, byte[]> inbound;
        GossipLearner<double[]> learner;
        long due;
    }

    @Test
    void learnersTrainedAtOneInstantStillMergeWhenTheirOffersCross() {
        Deque<Wire> wire = new ArrayDeque<>();
        List<Node> nodes = new ArrayList<>();
        Map<PeerId, Node> byId = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            Node n = new Node();
            nodes.add(n);
            byId.put(n.id, n);
        }
        for (int i = 0; i < 3; i++) {
            Node n = nodes.get(i);
            n.partner = nodes.get((i + 1) % 3).id; // a ring: each offer meets a node whose own offer is out
            PipeChannel pipe = new PipeChannel() {
                @Override
                public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
                    n.inbound = handler;
                }

                @Override
                public void send(PeerId to, String capabilityType, byte[] payload) {
                    wire.add(new Wire(n.id, to, payload));
                }
            };
            n.learner = GossipLearner.builder(pipe, k -> List.of(n.partner), n.id, codec, TestClock.create(),
                    WeightAveraging.INSTANCE).build();
            n.learner.attach(new DueWork.Driver() {
                @Override
                public void wake() {
                    n.due = mono; // run again at once, as the loop would
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
        }
        nodes.get(0).learner.start("m", new double[] {1.0, 2.0, 3.0});
        nodes.get(1).learner.start("m", new double[] {3.0, 2.0, 1.0});
        nodes.get(2).learner.start("m", new double[] {2.0, 5.0, 2.0});
        nodes.forEach(n -> n.due = 0);

        long end = 60_000 * MS;
        long convergedAt = -1;
        while (mono <= end && convergedAt < 0) {
            // every learner due at this instant runs before any frame of the instant lands
            for (Node n : nodes) {
                if (n.due <= mono) {
                    n.due = n.learner.run(mono);
                }
            }
            // then the wire delivers, 1 ms later, replies included, until it is quiet
            mono += MS;
            while (!wire.isEmpty()) {
                Wire w = wire.poll();
                byId.get(w.to()).inbound.accept(w.from(), w.payload());
            }
            if (converged(nodes)) {
                convergedAt = mono;
                break;
            }
            long next = nodes.stream().mapToLong(n -> n.due).min().orElseThrow();
            mono = Math.max(mono, next);
        }
        System.out.printf("[learner-busy] converged after %d ms of loop time; rounds %s%n",
                convergedAt / MS, nodes.stream().map(n -> n.learner.round("m")).toList());
        assertThat(convergedAt).as("the fleet reaches its mean within 60 s (example 14's bound)").isPositive();
    }

    private static boolean converged(List<Node> nodes) {
        double[] mean = {2.0, 3.0, 2.0};
        for (Node n : nodes) {
            double[] model = n.learner.model("m").orElseThrow();
            for (int i = 0; i < mean.length; i++) {
                if (Math.abs(model[i] - mean[i]) > 0.05) {
                    return false;
                }
            }
        }
        return true;
    }
}
