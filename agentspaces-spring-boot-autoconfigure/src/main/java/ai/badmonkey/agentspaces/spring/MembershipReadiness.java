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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Takes the application out of service while this node is isolated in a group (reviews/LOST-MEMBER-RETIREMENT-
 * 2026-11-18.md): readiness turns {@link ReadinessState#REFUSING_TRAFFIC} when a group's membership state becomes
 * {@code ISOLATED}, so a load balancer or an orchestrator's readiness probe stops sending work to a node that cannot
 * reach its fleet, and turns back to {@link ReadinessState#ACCEPTING_TRAFFIC} once no group is isolated. Liveness is
 * left alone: restarting a node rarely brings its fleet back, and restarting every node of a fleet that is gone loops.
 *
 * <p>It publishes Spring Boot's own availability events, so Actuator's readiness probe follows it when the application
 * has Actuator, and {@code ApplicationAvailability} answers it when it does not. It publishes
 * {@link ReadinessState#ACCEPTING_TRAFFIC} only to end a refusal it published itself; an application that also refuses
 * traffic for reasons of its own should publish its state again after a group rejoins. Off with
 * {@code agentspaces.health.refuse-traffic-when-isolated=false}.
 */
public final class MembershipReadiness {

    private static final System.Logger LOG = System.getLogger(MembershipReadiness.class.getName());

    private final ApplicationEventPublisher publisher;
    private final Set<GroupId> isolated = ConcurrentHashMap.newKeySet();
    private boolean refusing;

    /**
     * Follows a node's membership states.
     *
     * @param node      the node
     * @param publisher where the availability events go
     */
    public MembershipReadiness(PeerNode node, ApplicationEventPublisher publisher) {
        this(publisher);
        Objects.requireNonNull(node, "node").onMembershipStatus(this::onStatus);
    }

    MembershipReadiness(ApplicationEventPublisher publisher) { // package-private for tests
        this.publisher = Objects.requireNonNull(publisher, "publisher");
    }

    /** The groups this node is isolated in now. */
    public Set<GroupId> isolatedGroups() {
        return Set.copyOf(isolated);
    }

    /** Whether this bridge holds the application out of service. */
    public synchronized boolean refusing() {
        return refusing;
    }

    synchronized void onStatus(PeerNode.MembershipStatus status) {
        if (status.state() == PeerNode.MembershipState.ISOLATED) {
            isolated.add(status.group());
        } else {
            isolated.remove(status.group());
        }
        if (!isolated.isEmpty() && !refusing) {
            refusing = true;
            LOG.log(System.Logger.Level.WARNING, "isolated in " + isolated + ": readiness REFUSING_TRAFFIC until a"
                    + " member is back");
            AvailabilityChangeEvent.publish(publisher, this, ReadinessState.REFUSING_TRAFFIC);
        } else if (isolated.isEmpty() && refusing) {
            refusing = false;
            LOG.log(System.Logger.Level.INFO, "no group isolated any more: readiness ACCEPTING_TRAFFIC");
            AvailabilityChangeEvent.publish(publisher, this, ReadinessState.ACCEPTING_TRAFFIC);
        }
    }
}
