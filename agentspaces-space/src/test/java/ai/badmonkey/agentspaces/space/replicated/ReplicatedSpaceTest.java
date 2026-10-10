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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static ai.badmonkey.agentspaces.api.space.Matchers.gte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One replica on its own (issue #16 §9.2, §14 item 5): tag conditions apply
 * before decode on every read path and on subscriptions, {@code readAllEntries}
 * exposes the record's metadata, the four-argument {@code complete} carries
 * tags on the result, delivered events carry the metadata view, and
 * {@code recordOf} answers for a claimed entry. Cross-replica behavior is in
 * {@link ReplicatedSpaceClusterTest}.
 */
class ReplicatedSpaceTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupId groupId = GroupId.of("zSolo");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zSolo", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "solo",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private PeerIdentity identity;
    private PeerNode node;
    private ReplicatedSpace space;

    @BeforeEach
    void setUp() throws Exception {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("solo"), "solo");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of());
        space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .build();
    }

    @AfterEach
    void tearDown() {
        node.close();
    }

    /** Tag conditions select on read, the blocking read, readAll, readAllIssued and take. */
    @Test
    void tagConditionsSelectOnReadReadAllAndTake() {
        space.write(new TaskEntry("eu-task", 5), MINUTES_30, Map.of("region", "eu"));
        space.write(new TaskEntry("us-task", 5), MINUTES_30, Map.of("region", "us"));
        space.write(new TaskEntry("untagged", 5), MINUTES_30);

        assertThat(space.read(Template.of(TaskEntry.class).whereTag("region", eq("eu"))))
                .contains(new TaskEntry("eu-task", 5));
        assertThat(space.read(Template.of(TaskEntry.class).whereTag("region", eq("apac"))))
                .isEmpty();
        assertThat(space.read(Template.of(TaskEntry.class).whereTag("region", eq("apac")),
                Duration.ofMillis(60))).isEmpty();
        assertThat(space.readAll(Template.of(TaskEntry.class).hasTag("region"), 10))
                .extracting(TaskEntry::topic).containsExactlyInAnyOrder("eu-task", "us-task");
        assertThat(space.readAll(Template.of(TaskEntry.class)
                .where("priority", gte(5)).whereTag("region", eq("us")), 10))
                .extracting(TaskEntry::topic).containsExactly("us-task");
        assertThat(space.readAllIssued(Template.of(TaskEntry.class).hasTag("region"), 10))
                .extracting(i -> i.entry().topic()).containsExactlyInAnyOrder("eu-task", "us-task");

        TakenEntry<TaskEntry> taken = space.take(
                Template.of(TaskEntry.class).whereTag("region", eq("us")),
                MINUTES_10, Duration.ZERO).orElseThrow();
        assertThat(taken.entry().topic()).isEqualTo("us-task");
        assertThat(space.take(Template.of(TaskEntry.class).whereTag("region", eq("us")),
                MINUTES_10, Duration.ZERO)).isEmpty();
        assertThat(space.readAll(Template.of(TaskEntry.class).hasTag("region"), 10))
                .extracting(TaskEntry::topic).containsExactly("eu-task");
        assertThat(space.readAll(Template.of(TaskEntry.class), 10)).hasSize(2);
    }

    /** A filtered subscription sees only matching entries' events; WRITTEN and REAPPEARED carry the view. */
    @Test
    void notifyHonorsTagConditionsAndDeliveredEventsCarryDetails() {
        List<SpaceEvent<TaskEntry>> euOnly = new CopyOnWriteArrayList<>();
        List<SpaceEvent<TaskEntry>> all = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class).whereTag("region", eq("eu")), euOnly::add,
                MINUTES_30);
        space.notify(Template.of(TaskEntry.class), all::add, MINUTES_30);

        EntryHandle eu = space.write(new TaskEntry("eu-task", 1), MINUTES_30,
                Map.of("region", "eu", "tier", "gold"));
        space.write(new TaskEntry("us-task", 1), MINUTES_30, Map.of("region", "us"));
        space.write(new TaskEntry("untagged", 1), MINUTES_30);

        assertThat(all).hasSize(3);
        assertThat(euOnly).singleElement().satisfies(event -> {
            assertThat(event.kind()).isEqualTo(SpaceEvent.Kind.WRITTEN);
            assertThat(event.entryId()).isEqualTo(eu.entryId());
            assertThat(event.tags()).containsExactlyInAnyOrderEntriesOf(
                    Map.of("region", "eu", "tier", "gold"));
            assertThat(event.details()).isNotNull();
            assertThat(event.details().value()).isEqualTo(new TaskEntry("eu-task", 1));
            assertThat(event.details().entryId()).isEqualTo(eu.entryId());
            assertThat(event.details().issuer()).isEqualTo(identity.agent("worker"));
            assertThat(event.details().attestation()).isEqualTo(Space.Attestation.PEER_ASSERTED);
            assertThat(event.details().lease().kind()).isEqualTo(LeaseKind.WRITE);
            assertThat(event.details().issued()).isNotNull();
        });
        assertThat(all.get(2).details()).isNotNull();
        assertThat(all.get(2).tags()).isEmpty();

        TakenEntry<TaskEntry> taken = space.take(
                Template.of(TaskEntry.class).whereTag("region", eq("us")),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        clock.advance(Duration.ofMinutes(2));
        space.sweepNow();
        assertThat(euOnly).hasSize(1);
        assertThat(all).extracting(SpaceEvent::kind).containsExactly(SpaceEvent.Kind.WRITTEN,
                SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN,
                SpaceEvent.Kind.REAPPEARED);
        SpaceEvent<TaskEntry> reappeared = all.get(4);
        assertThat(reappeared.entryId()).isEqualTo(taken.entryId());
        assertThat(reappeared.details()).isNotNull();
        assertThat(reappeared.tags()).containsExactly(Map.entry("region", "us"));
        assertThat(reappeared.details().lease().kind()).isEqualTo(LeaseKind.WRITE);
    }

    /** readAllEntries returns the record's metadata with the readAll visibility rule; readAllIssued is unchanged. */
    @Test
    void readAllEntriesReturnsTheRecordsMetadata() {
        long before = clock.instant().toEpochMilli();
        EntryHandle plain = space.write(new TaskEntry("plain", 1), MINUTES_30,
                Map.of("region", "eu"));
        Space auditor = space.as(identity.subordinate("auditor", clock.instant(),
                Duration.ofHours(1)));
        EntryHandle attested = auditor.write(new TaskEntry("attested", 2), MINUTES_10,
                Map.of("region", "us"));

        List<Space.Entry<TaskEntry>> entries =
                space.readAllEntries(Template.of(TaskEntry.class), 10);
        assertThat(entries).hasSize(2);
        Space.Entry<TaskEntry> first = entries.stream()
                .filter(e -> e.entryId().equals(plain.entryId())).findFirst().orElseThrow();
        assertThat(first.value()).isEqualTo(new TaskEntry("plain", 1));
        assertThat(first.issuer()).isEqualTo(identity.agent("worker"));
        assertThat(first.attestation()).isEqualTo(Space.Attestation.PEER_ASSERTED);
        assertThat(first.tags()).containsExactly(Map.entry("region", "eu"));
        assertThat(first.lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(first.lease().holder()).isEqualTo(identity.agent("worker"));
        assertThat(first.lease().expiresAtMillis())
                .isEqualTo(before + MINUTES_30.duration().toMillis());
        assertThat(first.issued().physical()).isGreaterThanOrEqualTo(before);
        Space.Entry<TaskEntry> second = entries.stream()
                .filter(e -> e.entryId().equals(attested.entryId())).findFirst().orElseThrow();
        assertThat(second.issuer()).isEqualTo(identity.agent("auditor"));
        assertThat(second.attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
        assertThat(second.tags()).containsExactly(Map.entry("region", "us"));
        assertThat(second.lease().expiresAtMillis())
                .isEqualTo(before + MINUTES_10.duration().toMillis());

        assertThat(space.readAllEntries(Template.of(TaskEntry.class).hasTag("region"), 1))
                .hasSize(1);
        assertThat(space.readAllEntries(Template.of(TaskEntry.class)
                .whereTag("region", eq("us")), 10))
                .extracting(e -> e.value().topic()).containsExactly("attested");
        assertThat(auditor.readAllEntries(Template.of(TaskEntry.class), 10)).hasSize(2);
        assertThat(space.readAllIssued(Template.of(TaskEntry.class), 10)).containsExactlyInAnyOrder(
                new Space.Issued<>(new TaskEntry("plain", 1), identity.agent("worker")),
                new Space.Issued<>(new TaskEntry("attested", 2), identity.agent("auditor"),
                        Space.Attestation.AGENT_ATTESTED));

        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class)
                .where("topic", eq("plain")), MINUTES_10, Duration.ZERO).orElseThrow();
        assertThat(space.readAllEntries(Template.of(TaskEntry.class), 10))
                .extracting(e -> e.value().topic()).containsExactly("attested");
        space.complete(taken);
        assertThatThrownBy(() -> space.readAllEntries(Template.of(TaskEntry.class), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** The four-argument complete writes the result with its tags, atomically; the three-argument form writes none. */
    @Test
    void completeWithTagsWritesTheTaggedResultAtomically() {
        List<SpaceEvent<FindingEntry>> findings = new CopyOnWriteArrayList<>();
        List<SpaceEvent<TaskEntry>> tasks = new CopyOnWriteArrayList<>();
        space.notify(Template.of(FindingEntry.class), findings::add, MINUTES_30);
        space.notify(Template.of(TaskEntry.class), tasks::add, MINUTES_30);
        space.write(new TaskEntry("tagged", 1), MINUTES_30);
        space.write(new TaskEntry("plain", 2), MINUTES_30);

        TakenEntry<TaskEntry> first = space.take(Template.of(TaskEntry.class)
                .where("topic", eq("tagged")), MINUTES_10, Duration.ZERO).orElseThrow();
        EntryHandle taggedResult = space.complete(first, new FindingEntry("tagged", "done"),
                MINUTES_30, Map.of("rdf:type", "ex:Finding", "region", "eu"));
        TakenEntry<TaskEntry> second = space.take(Template.of(TaskEntry.class)
                .where("topic", eq("plain")), MINUTES_10, Duration.ZERO).orElseThrow();
        EntryHandle plainResult = space.complete(second, new FindingEntry("plain", "done"),
                MINUTES_30);

        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
        List<Space.Entry<FindingEntry>> results =
                space.readAllEntries(Template.of(FindingEntry.class), 10);
        assertThat(results).hasSize(2);
        Space.Entry<FindingEntry> tagged = results.stream()
                .filter(e -> e.entryId().equals(taggedResult.entryId())).findFirst().orElseThrow();
        assertThat(tagged.tags()).containsExactlyInAnyOrderEntriesOf(
                Map.of("rdf:type", "ex:Finding", "region", "eu"));
        Space.Entry<FindingEntry> untagged = results.stream()
                .filter(e -> e.entryId().equals(plainResult.entryId())).findFirst().orElseThrow();
        assertThat(untagged.tags()).isEmpty();
        assertThat(space.read(Template.of(FindingEntry.class).whereTag("rdf:type",
                eq("ex:Finding")))).contains(new FindingEntry("tagged", "done"));

        assertThat(tasks).extracting(SpaceEvent::kind).containsExactly(SpaceEvent.Kind.WRITTEN,
                SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED,
                SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
        assertThat(findings).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.WRITTEN);
        assertThat(findings.get(0).tags()).containsEntry("region", "eu");
        assertThat(findings.get(1).tags()).isEmpty();

        // A view completes with tags as its own agent.
        Space clerk = space.as(identity.agentIdentity("clerk"));
        space.write(new TaskEntry("job", 1), MINUTES_30);
        TakenEntry<TaskEntry> job = clerk.take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ZERO).orElseThrow();
        clerk.complete(job, new FindingEntry("job", "filed"), MINUTES_30, Map.of("by", "clerk"));
        assertThat(space.readAllEntries(Template.of(FindingEntry.class)
                .whereTag("by", eq("clerk")), 10)).singleElement().satisfies(e -> {
                    assertThat(e.issuer()).isEqualTo(identity.agent("clerk"));
                    assertThat(e.tags()).containsExactly(Map.entry("by", "clerk"));
                });
    }

    /**
     * Issue #16 §9.3 (ordered join): the record for an entry id learned from a
     * committed claim, available or claimed; empty once completed, since the
     * CRDT no longer reports a completed entry as present.
     */
    @Test
    void recordOfAnswersWhileAvailableOrClaimedAndIsEmptyAfterCompletion() {
        EntryHandle handle = space.write(new TaskEntry("ticket", 1), MINUTES_30,
                Map.of("join", "assemble", "key", "claim-7"));

        Optional<EntryRecord> available = space.recordOf(handle.entryId());
        assertThat(available).isPresent();
        assertThat(available.get().tags()).containsExactlyInAnyOrderEntriesOf(
                Map.of("join", "assemble", "key", "claim-7"));
        assertThat(available.get().lease().kind()).isEqualTo(LeaseKind.WRITE);

        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ZERO).orElseThrow();
        assertThat(space.read(Template.of(TaskEntry.class))).as("claimed: invisible to read")
                .isEmpty();
        Optional<EntryRecord> claimed = space.recordOf(handle.entryId());
        assertThat(claimed).as("claimed: the record is still held").isPresent();
        assertThat(claimed.get().entryId()).isEqualTo(handle.entryId());
        assertThat(claimed.get().tags()).containsEntry("key", "claim-7");

        space.complete(taken);
        assertThat(space.recordOf(handle.entryId())).as("completed: no longer present").isEmpty();
        assertThat(space.recordOf(handle.entryId(), true)).as("completed: still answers from the tombstone when asked").isPresent();
        assertThat(space.recordOf(ai.badmonkey.agentspaces.api.entry.EntryId.newId())).isEmpty();
    }

    /** Issue #16 §10.2: the schema name a type is registered under, for the binder's bind-time check. */
    @Test
    void schemaNameOfRegistersAndNamesTheType() {
        assertThat(space.schemaNameOf(FindingEntry.class))
                .isEqualTo(new SimpleSchemaRegistry().register(FindingEntry.class));
        assertThat(space.schemaNameOf(FindingEntry.class))
                .isEqualTo(space.schemaNameOf(FindingEntry.class));
    }
}
