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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.Peer;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.Summarizer;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.Summary;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.SummaryTask;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.TranslateTask;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.Translation;
import ai.badmonkey.agentspaces.examples.cards.CardsFleet.Translator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Cards route work across peers with no static wiring, over real TCP. */
class CardsFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(60)
    void dispatcherDiscoversSkillsAndCollectsResults() throws Exception {
        int seedPort = freePort();
        Peer summarizerPeer = CardsFleet.startPeer("summarizer-host", seedPort, 0);
        Peer translatorPeer = CardsFleet.startPeer("translator-host", freePort(), seedPort);
        Peer dispatcher = CardsFleet.startPeer("dispatcher", freePort(), seedPort);
        try {
            summarizerPeer.binder().bind(new Summarizer());
            translatorPeer.binder().bind(new Translator());

            // Cards gossip to the dispatcher's cache.
            String summarySchema = Summary.class.getName() + "#v1";
            List<AgentCard> summarizers = List.of();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (summarizers.isEmpty() && System.nanoTime() < deadline) {
                summarizers = dispatcher.discovery().find(AgentCard.class,
                        card -> card.produces().contains(summarySchema));
                Thread.sleep(100);
            }
            assertThat(summarizers).hasSize(1);
            assertThat(summarizers.get(0).agent().localName()).isEqualTo("summarizer");

            dispatcher.work().write(new SummaryTask("report.pdf"),
                    Lease.of(Duration.ofMinutes(10)));
            dispatcher.work().write(new TranslateTask("hello", "fr"),
                    Lease.of(Duration.ofMinutes(10)));

            assertThat(dispatcher.work().read(Template.of(Summary.class),
                    Duration.ofSeconds(30))).isPresent();
            assertThat(dispatcher.work().read(Template.of(Translation.class),
                    Duration.ofSeconds(30)))
                    .hasValueSatisfying(t ->
                            assertThat(t.translated()).isEqualTo("[fr] hello"));
        } finally {
            dispatcher.close();
            translatorPeer.close();
            summarizerPeer.close();
        }
    }
}
