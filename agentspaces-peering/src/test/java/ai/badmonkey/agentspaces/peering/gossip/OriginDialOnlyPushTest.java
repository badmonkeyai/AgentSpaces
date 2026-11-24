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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The live mixed-language run of the event-loop program's final gate (srad/agentspaces-event-loop.md §10.49): a seed
 * whose own write went out on the fan-out's three slots reached only three of the five dial-only clients linked to
 * it, so a Python or Ruby member behind it held its seed's own notes only after an anti-entropy round, tens of
 * seconds later, while every note the seed forwarded reached it at once. A dial-only member linked here can be
 * reached by nobody else, so the origin pushes to every one of them, as a forwarder along the tree already did, and
 * the fan-out's slots go to the other hint-less members as before.
 */
class OriginDialOnlyPushTest {

    @Test
    void anOriginPushesToEveryDialOnlyMemberLinkedHereAndTheFanOutToTheRest() {
        List<PeerId> dialOnly = Stream.generate(() -> PeerIdentity.generate().peerId()).limit(5).toList();
        List<PeerId> listening = Stream.generate(() -> PeerIdentity.generate().peerId()).limit(4).toList();
        PeerId elsewhere = PeerIdentity.generate().peerId(); // dial-only, linked to another member
        List<PeerId> members = new ArrayList<>(listening);
        members.addAll(dialOnly);
        members.add(elsewhere);
        PeerSampler sampler = n -> n >= members.size() ? members : members.subList(0, n);
        List<PeerId> sends = new CopyOnWriteArrayList<>();
        GossipBus.FrameSender sender = new GossipBus.FrameSender() {
            @Override
            public void sendRumor(PeerId to, Bodies.Rumor rumor) {
                sends.add(to);
            }

            @Override
            public boolean trySendRumor(PeerId to, Bodies.Rumor rumor) {
                if (to.equals(elsewhere)) {
                    return false; // no link here, and it has no endpoint
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
        GossipBus bus = new GossipBus(sampler, sender, CborCodec.defaultCodec(), 3, GossipBus.DEFAULT_HOPS,
                () -> 1_000_000L, new java.util.Random(7));
        bus.lazyPush(peer -> false); // no member carries the tree hint: every one is plain
        Set<PeerId> dialOnlySet = Set.copyOf(dialOnly);
        bus.plainReach(peer -> !peer.equals(elsewhere), peer -> dialOnlySet.contains(peer) || peer.equals(elsewhere));
        bus.onStream("news", (from, itemId, payload) -> { });

        for (int i = 0; i < 10; i++) {
            sends.clear();
            bus.publish("news", "i" + i, new byte[] {(byte) i});
            assertThat(sends).as("write %d: every dial-only member linked here, once", i)
                    .containsAll(dialOnly).doesNotHaveDuplicates();
            assertThat(sends.stream().filter(listening::contains).count())
                    .as("write %d: the fan-out's three slots go to the other hint-less members", i).isEqualTo(3);
        }
    }
}
