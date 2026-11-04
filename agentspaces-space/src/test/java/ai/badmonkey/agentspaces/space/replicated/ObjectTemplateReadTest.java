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


package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * v0.1.24: an Object template selects any type, so a replica that decodes a foreign type generically (a
 * console bridge, a connective step) reads it; a type-name condition narrows before decoding.
 */
class ObjectTemplateReadTest {

    public record Claim(String claimId, long amount) {
    }

    public record Payment(String claimId) {
    }

    /** A reader that has no class for the writer's types: unknown names decode as maps. */
    static final class LenientRegistry implements SchemaRegistry {
        private final SimpleSchemaRegistry base = new SimpleSchemaRegistry();

        @Override
        public String register(Class<?> entryType) {
            return base.register(entryType);
        }

        @Override
        public String schemaNameOf(Class<?> entryType) {
            return base.register(entryType);
        }

        @Override
        public Optional<Class<?>> classFor(String schemaName) {
            Optional<Class<?>> known = base.classFor(schemaName);
            return known.isPresent() ? known : Optional.of(Map.class);
        }
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zObjectRead");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zObjectRead", PeerIdentity.generate().peerId(),
            groupId, java.time.Instant.EPOCH, Duration.ofDays(1), "object-read", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace peer(String address, SchemaRegistry registry, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(address.hashCode()).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "lane", identity, "worker").clock(clock)
                .settleWindow(Duration.ZERO);
        if (registry != null) {
            builder.schemaRegistry(registry);
        }
        nodes.add(node);
        return builder.build();
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void anObjectTemplateReadsForeignEntriesAndATypeNameNarrows() throws Exception {
        ReplicatedSpace writer = peer("w", null);
        ReplicatedSpace reader = peer("r", new LenientRegistry(), "w");
        tickAll(4);
        writer.write(new Claim("c-1", 60000), Lease.of(Duration.ofMinutes(5)), Map.of("claimId", "c-1"));
        writer.write(new Payment("c-1"), Lease.of(Duration.ofMinutes(5)), Map.of("claimId", "c-1"));
        tickAll(8);
        assertThat(writer.readAll(Template.of(Object.class), 10)).as("the writer's own Object read").hasSize(2);
        assertThat(reader.readAll(Template.of(Object.class), 10)).as("the foreign reader decodes both as maps").hasSize(2)
                .allSatisfy(v -> assertThat(v).isInstanceOf(Map.class));
        assertThat(reader.readAllEntries(Template.of(Object.class).whereType("Claim"), 10)).singleElement().satisfies(e -> {
            assertThat(e.type()).endsWith("ObjectTemplateReadTest$Claim#v1");
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) e.value();
            assertThat(fields).containsEntry("amount", 60000);
        });
        assertThat(reader.readAll(Template.of(Object.class).whereType("Invoice"), 10)).isEmpty();
        assertThat(reader.readAll(Template.of(Object.class).whereTag("claimId", ai.badmonkey.agentspaces.api.space.Matchers.eq("c-1")), 10)).hasSize(2);
        // a typed template still selects its own type only
        assertThat(writer.readAll(Template.of(Claim.class), 10)).hasSize(1);
        // a take through the Object template claims by type name
        var taken = reader.take(Template.of(Object.class).whereType("Payment"), Lease.of(Duration.ofMinutes(1)), Duration.ofMillis(10));
        assertThat(taken).isPresent();
        assertThat(taken.get().entry()).isInstanceOf(Map.class);
        // the local space judges the type name the same way
        PeerIdentity id = PeerIdentity.generate();
        var local = LocalSpace.builder("l", id.agent("t")).build();
        local.write(new Claim("c-2", 1), Lease.of(Duration.ofMinutes(1)));
        assertThat(local.readAll(Template.of(Object.class).whereType("Claim"), 10)).hasSize(1);
        assertThat(local.readAll(Template.of(Object.class).whereType("Payment"), 10)).isEmpty();
    }
}
