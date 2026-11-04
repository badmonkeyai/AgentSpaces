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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.constraints.ConstraintGate;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Installs a {@link ConstraintGate} in every group's binder when {@code agentspaces.rules.gate} is on
 * (ISSUE-Constraints FR-10, §9.4, §9.5): the group's {@code constraints} space is the configured one or one
 * opened here, every other configured space of the group is watched for the keys' events, and the node sets the
 * membership hint {@code rules.gate=true} for the group. A bound agent on such a peer has its claims and its
 * results judged by the published rules; a peer without the property gates nothing and advertises no hint.
 */
public final class ConstraintGates implements SmartLifecycle, AutoCloseable {

    private final AgentSpaces spaces;
    private final AgentSpacesProperties properties;
    private final Authorizers authorizers;
    private final PeerNode node;
    private final PeerIdentity identity;
    private final InstantSource clock;
    private final List<ConstraintGate> gates = new ArrayList<>();
    private final List<ReplicatedSpace> opened = new ArrayList<>();
    private final Map<String, ConstraintGate> byGroup = new java.util.LinkedHashMap<>();
    private volatile boolean running;

    public ConstraintGates(AgentSpaces spaces, AgentSpacesProperties properties, Authorizers authorizers, PeerNode node,
                           PeerIdentity identity, InstantSource clock) {
        this.spaces = spaces;
        this.properties = properties;
        this.authorizers = authorizers;
        this.node = node;
        this.identity = identity;
        this.clock = clock;
    }

    @Override
    public synchronized void start() {
        if (running || !properties.getRules().isGate()) {
            return;
        }
        String spaceName = properties.getRules().getSpace();
        for (String groupName : spaces.groupNames()) {
            AgentSpaces.GroupContext group = spaces.group(groupName);
            Space constraints;
            if (group.spaceNames().contains(spaceName)) {
                constraints = group.space(spaceName);
            } else {
                ReplicatedSpace space = ReplicatedSpace.builder(group.runtime(), spaceName, identity, "rules")
                        .settleWindow(Duration.ofMillis(150)).clock(clock).build();
                opened.add(space);
                constraints = space;
            }
            ConstraintGate gate = ConstraintGate.attach(constraints, authorizers == null ? null : authorizers.forGroup(groupName),
                    spaceName, clock, identity.peerId().value(), properties.getRules().getCacheKeys());
            for (String other : group.spaceNames()) {
                if (!other.equals(spaceName)) {
                    gate.watch(other, group.space(other));
                }
            }
            group.binder().constraintGate(gate);
            gates.add(gate);
            byGroup.put(groupName, gate);
            node.groupHint(group.runtime().id(), "rules.gate", "true");
        }
        running = true;
    }

    /** The gate of a group, for the console panel and the tests. */
    public ConstraintGate gate(String groupName) {
        return byGroup.get(groupName);
    }

    @Override
    public synchronized void stop() {
        running = false;
        close();
    }

    @Override
    public void close() {
        gates.forEach(ConstraintGate::close);
        gates.clear();
        opened.forEach(ReplicatedSpace::close);
        opened.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 200; // after the node and the spaces, before the bound agents take
    }
}
