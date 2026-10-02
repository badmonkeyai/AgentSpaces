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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimNetworkTest {

    @Test
    void framesFlowBothWays() throws Exception {
        SimNetwork network = new SimNetwork();
        Transport alice = network.register("alice");
        Transport bob = network.register("bob");

        List<String> bobInbox = new ArrayList<>();
        AtomicReference<TransportConnection> bobSide = new AtomicReference<>();
        bob.listen("bob", connection -> {
            bobSide.set(connection);
            connection.onReceive(frame -> bobInbox.add(new String(frame, StandardCharsets.UTF_8)));
        });

        List<String> aliceInbox = new ArrayList<>();
        TransportConnection toBob = alice.dial("bob");
        toBob.onReceive(frame -> aliceInbox.add(new String(frame, StandardCharsets.UTF_8)));

        toBob.send("hello".getBytes(StandardCharsets.UTF_8));
        bobSide.get().send("hi back".getBytes(StandardCharsets.UTF_8));

        assertThat(bobInbox).containsExactly("hello");
        assertThat(aliceInbox).containsExactly("hi back");
        assertThat(toBob.remoteAddress()).isEqualTo("bob");
    }

    @Test
    void dialToUnknownAddressFails() {
        SimNetwork network = new SimNetwork();
        Transport alice = network.register("alice");

        assertThatThrownBy(() -> alice.dial("nowhere")).isInstanceOf(IOException.class);
    }

    @Test
    void partitionBlocksAndHealRestores() throws Exception {
        SimNetwork network = new SimNetwork();
        Transport alice = network.register("alice");
        Transport bob = network.register("bob");
        bob.listen("bob", connection -> connection.onReceive(frame -> {
        }));

        TransportConnection toBob = alice.dial("bob");
        network.partition("alice", "bob");

        assertThatThrownBy(() -> toBob.send(new byte[]{1})).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> alice.dial("bob")).isInstanceOf(IOException.class);

        network.heal();
        toBob.send(new byte[]{1});
    }

    @Test
    void closedConnectionRefusesSends() throws Exception {
        SimNetwork network = new SimNetwork();
        Transport alice = network.register("alice");
        network.register("bob").listen("bob", connection -> {
        });

        TransportConnection toBob = alice.dial("bob");
        toBob.close();

        assertThatThrownBy(() -> toBob.send(new byte[]{1})).isInstanceOf(IOException.class);
    }

    @Test
    void testClockAdvancesDeterministically() {
        TestClock clock = TestClock.startingAt(Instant.ofEpochMilli(1000));

        clock.advance(Duration.ofSeconds(5));

        assertThat(clock.instant()).isEqualTo(Instant.ofEpochMilli(6000));
        assertThatThrownBy(() -> clock.advance(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
