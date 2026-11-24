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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.peering.node.NodeLoop;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Objects;

/**
 * Publishes {@link LivenessState#BROKEN} when the watchdog escalates this node's own loop (srad/agentspaces-event-loop.md
 * §3.9, decision 4 (i), EL-H-08): three re-initialisations of the loop within 10 minutes, or a participant fenced for 2
 * minutes, which a restart of the process repairs. It keys on the node's loop faults only, never on membership or
 * isolation, which keep touching readiness alone ({@link MembershipReadiness}): restarting a node rarely brings its
 * fleet back, and restarting every node of a fleet that is gone loops.
 *
 * <p>It publishes Spring Boot's own availability event, so Actuator's liveness probe follows it when the application
 * has Actuator. It never publishes {@link LivenessState#CORRECT}: a loop that escalated stays DOWN until the process
 * restarts.
 */
public final class LoopLiveness {

    private static final System.Logger LOG = System.getLogger(LoopLiveness.class.getName());

    private final ApplicationEventPublisher publisher;
    private boolean broken;

    /**
     * Follows a node's loop health.
     *
     * @param node      the node
     * @param publisher where the availability event goes
     */
    public LoopLiveness(PeerNode node, ApplicationEventPublisher publisher) {
        this(publisher);
        Objects.requireNonNull(node, "node").onLoopHealth(this::onHealth);
    }

    LoopLiveness(ApplicationEventPublisher publisher) { // package-private for tests
        this.publisher = Objects.requireNonNull(publisher, "publisher");
    }

    /** Whether this bridge has published BROKEN. */
    public synchronized boolean broken() {
        return broken;
    }

    synchronized void onHealth(NodeLoop.Health health) {
        if (health.state() != NodeLoop.HealthState.DOWN || broken) {
            return;
        }
        broken = true;
        LOG.log(System.Logger.Level.ERROR, "the node loop's watchdog escalated " + health.reasons()
                + ": liveness BROKEN");
        AvailabilityChangeEvent.publish(publisher, this, LivenessState.BROKEN);
    }
}
