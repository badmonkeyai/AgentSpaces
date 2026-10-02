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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gate's authorization surface (ASF-008): only the configured console
 * peer's directives take effect, and a hostile {@code issuedMillis} cannot
 * wedge the gate against later legitimate directives.
 */
class DirectiveGateTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final AgentId console = new AgentId(PeerIdentity.generate().peerId(), "console");
    private final AgentId impostor = new AgentId(PeerIdentity.generate().peerId(), "impostor");

    @Test
    void directivesFromTheConsoleTakeEffect() throws Exception {
        try (LocalSpace control = LocalSpace.builder("control", console).build();
             DirectiveGate gate = DirectiveGate.attach(control, "w1", console.peer())) {
            control.write(new Directive(Directive.PAUSE, "w1", "op", now()), HOUR);
            assertThat(gate.paused()).isTrue();
            control.write(new Directive(Directive.RESUME, "w1", "op", now() + 1), HOUR);
            assertThat(gate.paused()).isFalse();
            control.write(new Directive(Directive.DRAIN, "*", "op", now() + 2), HOUR);
            assertThat(gate.draining()).isTrue();
        }
    }

    @Test
    void directivesFromAnyOtherPeerAreIgnored() throws Exception {
        // The space's writes are attributed to the impostor: an admitted peer
        // that is not the console. The gate must not obey it.
        try (LocalSpace control = LocalSpace.builder("control", impostor).build();
             DirectiveGate gate = DirectiveGate.attach(control, "w1", console.peer())) {
            control.write(new Directive(Directive.DRAIN, "*", "op", now()), HOUR);
            assertThat(gate.paused()).isFalse();
            assertThat(gate.draining()).isFalse();
        }
    }

    @Test
    void aFutureStampedDirectiveCannotWedgeTheGate() throws Exception {
        try (LocalSpace control = LocalSpace.builder("control", console).build();
             DirectiveGate gate = DirectiveGate.attach(control, "w1", console.peer())) {
            // Hostile stamp far in the future: ignored outright, so it neither
            // acts nor advances the freshness watermark.
            control.write(new Directive(Directive.DRAIN, "*", "op",
                    now() + Duration.ofHours(10).toMillis()), HOUR);
            assertThat(gate.draining()).isFalse();

            // A later legitimate directive still works.
            control.write(new Directive(Directive.PAUSE, "w1", "op", now()), HOUR);
            assertThat(gate.paused()).isTrue();
        }
    }

    /** TODO-EFG §4 / TODO item 6 (ASF-008): under the Authorizer form the gate obeys whichever admitted peer the fleet's authorizer permits DIRECTIVE_ISSUER in the gate's scope, and nobody else. */
    @Test
    void anAuthorizerDecidesWhoseDirectivesAreObeyed() throws Exception {
        java.util.List<String> asked = new java.util.concurrent.CopyOnWriteArrayList<>();
        Authorizer authorizer = (peer, operation, scope) -> {
            asked.add(operation + ":" + scope);
            return operation == Authorizer.Operation.DIRECTIVE_ISSUER
                    && "fleet-a".equals(scope) && console.peer().equals(peer);
        };
        // Two admitted peers write directives to their replicas of the control
        // space; a worker's gate on each judges the space-authenticated writer.
        try (LocalSpace consoleControl = LocalSpace.builder("control", console).build();
             LocalSpace impostorControl = LocalSpace.builder("control", impostor).build();
             DirectiveGate obeysConsole = DirectiveGate.attach(consoleControl, "w1",
                     authorizer, "fleet-a");
             DirectiveGate seesImpostor = DirectiveGate.attach(impostorControl, "w1",
                     authorizer, "fleet-a")) {
            consoleControl.write(new Directive(Directive.PAUSE, "w1", "op", now()), HOUR);
            impostorControl.write(new Directive(Directive.DRAIN, "*", "op", now()), HOUR);

            assertThat(obeysConsole.paused()).as("the permitted issuer's directive").isTrue();
            assertThat(seesImpostor.paused()).as("the refused issuer's directive").isFalse();
            assertThat(seesImpostor.draining()).isFalse();
            assertThat(asked).containsOnly("DIRECTIVE_ISSUER:fleet-a").hasSize(2);
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
