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
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    /** The ids of the newest {@code n} entries of a read: sorted by issue stamp, newest first, then cut. */
    private static Set<EntryId> newest(List<Space.Entry<Claim>> entries, int n) {
        Set<EntryId> ids = new HashSet<>();
        entries.stream().sorted(Comparator.comparing((Space.Entry<Claim> e) -> e.issued()).reversed()).limit(n)
                .forEach(e -> ids.add(e.entryId()));
        return ids;
    }

    private static Set<EntryId> idsOf(List<Space.Entry<Claim>> entries) {
        Set<EntryId> ids = new HashSet<>();
        entries.forEach(e -> ids.add(e.entryId()));
        return ids;
    }

    /**
     * P15 (reviews/RBE-REGRESSION-MATRIX.md §3, the core part): readAll over more than its cap. A capped read promises
     * no order. It returns {@code limit} distinct entries that the template selects and that are available (live and
     * unclaimed), counted after those filters, so other types, other tags and entries in hand never use up the cap; and
     * they are the head of an uncapped read at the same moment, in the same order. A caller that wants one subset, as a
     * chat wants its newest 500, reads past the cap and sorts by the issue stamp before it cuts: that gives the same
     * subset on every replica, whether the entries came by live writes or by one batched {@code PULL_RESP}.
     */
    @Test
    void aCappedReadIsTheHeadOfTheFullReadAndOnlyASortedReadGivesTheNewest() throws Exception {
        ReplicatedSpace writer = peer("w", null);
        ReplicatedSpace batched = peer("b", null); // no seeds: learns everything from one PULL_RESP
        Lease lease = Lease.of(Duration.ofMinutes(30));
        List<EntryId> room = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            room.add(writer.write(new Claim("m-" + i, i), lease, Map.of("room", "r1")).entryId());
            if (i % 6 == 0) {
                writer.write(new Claim("other-" + i, i), lease, Map.of("room", "r2"));
                writer.write(new Payment("p-" + i), lease, Map.of("room", "r1"));
            }
        }
        assertThat(batched.applySyncDelta(writer.deltaFor(batched.digest()))).isTrue();
        assertThat(batched.knownEntries()).isEqualTo(writer.knownEntries());

        Template<Claim> chat = Template.of(Claim.class).whereTag("room", Matchers.eq("r1"));
        Set<EntryId> newest500 = new HashSet<>(room.subList(100, 600));
        for (ReplicatedSpace replica : List.of(writer, batched)) {
            List<Space.Entry<Claim>> all = replica.readAllEntries(chat, 10_000);
            assertThat(all).hasSize(600);
            List<Space.Entry<Claim>> capped = replica.readAllEntries(chat, 500);
            assertThat(capped).as("the cap, counted over the selected entries only").hasSize(500);
            assertThat(capped).extracting(Space.Entry::entryId).as("the head of the full read, in its order")
                    .containsExactlyElementsOf(all.subList(0, 500).stream().map(Space.Entry::entryId).toList());
            assertThat(idsOf(capped)).as("distinct, and all of the room").hasSize(500).isSubsetOf(room);
            assertThat(replica.readAll(chat, 500)).as("readAll returns the same head")
                    .containsExactlyElementsOf(capped.stream().map(Space.Entry::value).toList());
            assertThat(newest(all, 500)).as("sort, then cut: the newest 500").isEqualTo(newest500);
            assertThat(replica.readAll(Template.of(Claim.class).whereTag("room", Matchers.eq("r2")), 500))
                    .as("another room's entries are not cut by the first room's").hasSize(100);
        }

        // Entries in hand leave the read before the cap is counted.
        Set<EntryId> inHand = new HashSet<>();
        for (int i = 0; i < 150; i++) {
            inHand.add(writer.take(chat, Lease.of(Duration.ofMinutes(5)), Duration.ZERO).orElseThrow().entryId());
        }
        List<Space.Entry<Claim>> rest = writer.readAllEntries(chat, 10_000);
        assertThat(rest).hasSize(450);
        List<Space.Entry<Claim>> capped = writer.readAllEntries(chat, 400);
        assertThat(capped).hasSize(400);
        assertThat(idsOf(capped)).doesNotContainAnyElementsOf(inHand);
        assertThat(capped).extracting(Space.Entry::entryId)
                .containsExactlyElementsOf(rest.subList(0, 400).stream().map(Space.Entry::entryId).toList());
        assertThat(writer.readAllEntries(chat, 500)).as("fewer available than the cap: all of them").hasSize(450);
    }
}
