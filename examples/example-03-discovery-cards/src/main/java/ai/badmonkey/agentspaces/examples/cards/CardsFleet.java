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
package ai.badmonkey.agentspaces.examples.cards;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;

/**
 * Example 03: automatic AgentCards route work. Two differently skilled agents on
 * different peers publish their cards on bind; a dispatcher peer asks the
 * ad-cache which agent produces which entry type, then writes tasks and reads
 * results, all with no static wiring between the peers.
 *
 * <p>Run: {@code mvn -q -pl examples/example-03-discovery-cards exec:java}
 */
public final class CardsFleet {

    /** A summarization request. */
    public record SummaryTask(String document) {
    }

    /** A summarization result. */
    public record Summary(String document, String text) {
    }

    /** A translation request. */
    public record TranslateTask(String text, String language) {
    }

    /** A translation result. */
    public record Translation(String text, String language, String translated) {
    }

    /** The summarization specialist. */
    @AgentSpec(name = "summarizer", description = "Summarizes documents", goals = {"summarize"})
    public static class Summarizer {
        /** Produces a summary from a task. */
        @SpaceTake(space = "work", pollTimeout = "PT0.3S")
        public Summary summarize(SummaryTask task) {
            return new Summary(task.document(), "summary of " + task.document());
        }
    }

    /** The translation specialist. */
    @AgentSpec(name = "translator", description = "Translates text", goals = {"translate"})
    public static class Translator {
        /** Produces a translation from a task. */
        @SpaceTake(space = "work", pollTimeout = "PT0.3S")
        public Translation translate(TranslateTask task) {
            return new Translation(task.text(), task.language(),
                    "[" + task.language() + "] " + task.text());
        }
    }

    /** One assembled peer of this example's fleet. */
    public record Peer(PeerNode node, GroupRuntime runtime, DiscoveryService discovery,
                       ReplicatedSpace work, AgentBinder binder) {

        /** Shuts the peer down. */
        public void close() {
            binder.close();
            work.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";

    private CardsFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "discovery-cards-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "cards-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer: node, discovery, the shared work space, and a binder.
     *
     * @param agentName the local agent name
     * @param port      the TCP port
     * @param seedPort  an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String agentName, int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", HOST + ":" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, agentName)
                .settleWindow(Duration.ofMillis(150))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), discovery,
                InstantSource.system());
        binder.space("work", work);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime, discovery, work, binder);
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("discovery-cards: skills discovered through AgentCards\n");
        Peer summarizerPeer = startPeer("summarizer-host", 7461, 0);
        Peer translatorPeer = startPeer("translator-host", 7462, 7461);
        Peer dispatcher = startPeer("dispatcher", 7463, 7461);
        try {
            summarizerPeer.binder().bind(new Summarizer());
            translatorPeer.binder().bind(new Translator());
            Thread.sleep(1500); // cards gossip through the group

            String summarySchema = Summary.class.getName() + "#v1";
            List<AgentCard> summarizers = dispatcher.discovery().find(AgentCard.class,
                    card -> card.produces().contains(summarySchema));
            System.out.println("dispatcher found " + summarizers.size()
                    + " agent(s) producing " + Summary.class.getSimpleName()
                    + ": " + summarizers.stream().map(c -> c.agent().localName()).toList());

            dispatcher.work().write(new SummaryTask("quarterly-report.pdf"),
                    Lease.of(Duration.ofMinutes(10)));
            dispatcher.work().write(new TranslateTask("hello fleet", "fr"),
                    Lease.of(Duration.ofMinutes(10)));

            Summary summary = dispatcher.work()
                    .read(Template.of(Summary.class), Duration.ofSeconds(20)).orElseThrow();
            Translation translation = dispatcher.work()
                    .read(Template.of(Translation.class), Duration.ofSeconds(20)).orElseThrow();
            System.out.println("summary    : " + summary.text());
            System.out.println("translation: " + translation.translated());
            System.out.println("\nno static wiring: the cards were the routing table.");
        } finally {
            dispatcher.close();
            translatorPeer.close();
            summarizerPeer.close();
        }
    }
}
