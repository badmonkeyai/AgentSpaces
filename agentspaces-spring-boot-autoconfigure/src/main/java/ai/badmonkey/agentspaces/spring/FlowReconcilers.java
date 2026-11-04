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
import ai.badmonkey.agentspaces.agent.flow.AssetRefs;
import ai.badmonkey.agentspaces.agent.flow.FlowReconciler;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Installs a {@link FlowReconciler} per group when {@code agentspaces.flows.reconcile} is on (ISSUE-FlowManifest
 * FR-2, FR-9, FR-11): the catalogue is what this facade bound, the {@code flow-control} space is the configured one
 * or one opened here, asset references resolve against the group's discovery, and the node sets the
 * {@code flowVersion} and {@code flowConformance} hints after each reconciliation.
 */
public final class FlowReconcilers implements SmartLifecycle, AutoCloseable {

    private final AgentSpaces spaces;
    private final AgentSpacesProperties properties;
    private final Authorizers authorizers;
    private final PeerNode node;
    private final PeerIdentity identity;
    private final InstantSource clock;
    private final Map<String, FlowReconciler> byGroup = new LinkedHashMap<>();
    private final List<ReplicatedSpace> opened = new ArrayList<>();
    private volatile boolean running;

    public FlowReconcilers(AgentSpaces spaces, AgentSpacesProperties properties, Authorizers authorizers, PeerNode node,
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
        if (running || !properties.getFlows().isReconcile()) {
            return;
        }
        String spaceName = properties.getFlows().getSpace();
        for (String groupName : spaces.groupNames()) {
            AgentSpaces.GroupContext group = spaces.group(groupName);
            Space flowControl;
            if (group.spaceNames().contains(spaceName)) {
                flowControl = group.space(spaceName);
            } else {
                ReplicatedSpace space = ReplicatedSpace.builder(group.runtime(), spaceName, identity, "flows")
                        .settleWindow(Duration.ofMillis(150)).clock(clock).build();
                opened.add(space);
                flowControl = space;
            }
            FlowReconciler reconciler = FlowReconciler.attach(flowControl, identity.peerId().value(), Map.of(), group.binder(),
                    spaces.catalogue(), reference -> AssetRefs.resolve(group.discovery(), reference),
                    authorizers == null ? null : authorizers.forGroup(groupName), spaceName, clock,
                    (k, v) -> node.groupHint(group.runtime().id(), k, v));
            byGroup.put(groupName, reconciler);
        }
        running = true;
    }

    public FlowReconciler reconciler(String groupName) {
        return byGroup.get(groupName);
    }

    @Override
    public synchronized void stop() {
        running = false;
        close();
    }

    @Override
    public void close() {
        byGroup.values().forEach(FlowReconciler::close);
        byGroup.clear();
        opened.forEach(ReplicatedSpace::close);
        opened.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 210; // after the gates, so a manifest's bindings are gated too
    }
}
