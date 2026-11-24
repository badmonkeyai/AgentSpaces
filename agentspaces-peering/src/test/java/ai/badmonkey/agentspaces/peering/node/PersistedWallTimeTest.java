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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-C-13 (srad/agentspaces-event-loop.md §3.6): times written to disk or compared across a restart stay on wall time,
 * because the monotonic clock means nothing after a restart. On {@code VirtualTime} the monotonic base and the wall
 * clock part as soon as the wall steps, so a stamp taken from the wrong base, or from a wall reading cached before the
 * step, lands an hour away from the wall clock the file is read against. Both files a node persists are checked: the
 * admission memory and the retirement list. Lost-member times and the content and PDT stores take an
 * {@code InstantSource}, which has no monotonic reading at all.
 */
@Timeout(120)
class PersistedWallTimeTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration STEP = Duration.ofHours(1);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zPersistedWall");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zPersistedWall",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "persisted-wall",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode member(String name, String seed, UnaryOperator<PeerNode.Builder> options) throws IOException {
        PeerNode node = options.apply(PeerNode.builder(PeerIdentity.generate()).clock(time).time(time)
                .randomSeed(name.hashCode())).build();
        node.listen(network.register(name), name);
        nodes.add(node);
        node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        node.startTicking(TICK);
        return node;
    }

    private long wall() {
        return time.instant().toEpochMilli();
    }

    @Test
    void theAdmissionMemoryStampsWallTimeAfterAWallStep(@TempDir Path state) throws Exception {
        PeerNode node = member("a", null, b -> b.stateDirectory(state));
        time.advance(Duration.ofSeconds(30));
        long monoBefore = time.monoNanos();
        time.stepWall(STEP);
        assertThat(time.monoNanos()).as("the step moves the wall only").isEqualTo(monoBefore);

        long recordedAt = wall();
        node.admissionMemory().record(groupId, PeerIdentity.generate().peerId(), "aa,dd");
        time.advance(Duration.ofMillis(1));
        Path file = node.admissionMemory().fileFor(groupId);
        assertThat(file).as("written at once (the dirty mark signals persist)").exists();

        JsonNode peers = JSON.readTree(Files.readAllBytes(file)).get("peers");
        assertThat(peers.size()).isEqualTo(1);
        long lastSeen = peers.elements().next().get("lastSeen").asLong();
        assertThat(lastSeen).as("EL-C-13: the stamp is the wall clock after the step, not the monotonic base")
                .isEqualTo(recordedAt);
    }

    @Test
    void theRetirementListStampsWallTimeAfterAWallStep(@TempDir Path state) throws Exception {
        PeerNode a = member("a", null, b -> b.stateDirectory(state).retireAfter(Duration.ofMinutes(5)));
        PeerNode b = member("b", "a", UnaryOperator.identity());
        long waited = 0;
        while (a.group(groupId).orElseThrow().membership().allMembers().isEmpty()) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the pair forms within 60 s of virtual time").isLessThan(60_000);
        }
        time.advance(Duration.ofSeconds(10));
        time.stepWall(STEP);
        long afterStep = wall();
        network.partition("a", "b");
        time.advance(Duration.ofMinutes(7));
        assertThat(a.retired(groupId)).as("b retired 5 min after its loss").containsExactly(b.peerId());
        time.advance(Duration.ofSeconds(30)); // past the list's write period

        Path file = state.resolve(RetirementList.DIRECTORY).resolve(groupId.value() + ".json");
        assertThat(file).exists();
        JsonNode retired = JSON.readTree(Files.readAllBytes(file)).get("retired").get(0);
        long retiredAt = retired.get("retiredAt").asLong();
        long lastSeen = retired.get("lastSeen").asLong();
        assertThat(retiredAt).as("EL-C-13: retired on the wall clock after the step")
                .isBetween(afterStep + Duration.ofMinutes(5).toMillis(), wall());
        assertThat(lastSeen).as("last seen on the wall clock, no later than the retirement")
                .isBetween(afterStep - Duration.ofSeconds(30).toMillis(), retiredAt);
    }
}
