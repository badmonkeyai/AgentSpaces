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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-T-01 of srad/agentspaces-event-loop.md: a failed push keeps a hint-less member out of the fan-out's strong slots
 * for {@link GossipBus#PLAIN_FAILURE} of the bus's clock (2 s, today's 8 ticks at 250 ms), however often or seldom the
 * bus's participant runs. It was 8 runs of the participant, which on the node's loop can be minutes in a quiet group,
 * or no time at all when a burst of runs comes together.
 */
class PlainFailureExclusionTest {

    @Test
    void aFailedPushIsExcludedForTwoSecondsOfClockNotForEightRuns() {
        PeerId p1 = PeerIdentity.generate().peerId();
        PeerId p2 = PeerIdentity.generate().peerId();
        PeerId p3 = PeerIdentity.generate().peerId();
        List<PeerId> plain = List.of(p1, p2, p3); // the sampler's order: p1 is the first strong pick when eligible
        PeerSampler sampler = n -> n >= plain.size() ? plain : plain.subList(0, n);
        long[] clock = {1_000_000};
        boolean[] p1Fails = {true};
        List<PeerId> sends = new CopyOnWriteArrayList<>();
        GossipBus.FrameSender sender = new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
                sends.add(to);
            }

            @Override
            public boolean trySendRumor(PeerId to, Bodies.Rumor rumor) {
                if (to.equals(p1) && p1Fails[0]) {
                    return false;
                }
                sends.add(to);
                return true;
            }

            @Override
            public void sendDigest(PeerId to, Bodies.Digest digest) {
            }

            @Override
            public void sendPullResp(PeerId to, Bodies.PullResp resp) {
            }
        };
        GossipBus bus = new GossipBus(sampler, sender, CborCodec.defaultCodec(), 2, GossipBus.DEFAULT_HOPS,
                () -> clock[0], new java.util.Random(5));
        bus.lazyPush(peer -> false); // every member is hint-less
        bus.plainReach(peer -> true, peer -> false);
        bus.onStream("news", (from, itemId, payload) -> { });

        bus.publish("news", "i1", new byte[] {1});
        assertThat(sends).as("p1 failed; p2 took the strong slot").startsWith(p2);
        p1Fails[0] = false;

        for (int i = 0; i < 20; i++) {
            bus.tick(clock[0]); // twenty runs of the bus's participant on a frozen clock
        }
        sends.clear();
        bus.publish("news", "i2", new byte[] {2});
        assertThat(sends.get(0)).as("still excluded from the strong slot after 20 runs and 0 ms").isNotEqualTo(p1);

        clock[0] += GossipBus.PLAIN_FAILURE.toMillis() - 1;
        sends.clear();
        bus.publish("news", "i3", new byte[] {3});
        assertThat(sends.get(0)).as("still excluded at 2 s less a millisecond").isNotEqualTo(p1);

        clock[0] += 2;
        sends.clear();
        bus.publish("news", "i4", new byte[] {4});
        assertThat(sends.get(0)).as("back in the strong slot once 2 s of clock passed").isEqualTo(p1);
    }
}
