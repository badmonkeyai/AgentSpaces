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

import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain;

import java.util.List;

/**
 * The Spring form of {@link ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChainFactory}:
 * composes the locator a group is joined with from the locators the starter
 * collected for it (the configured seeds, the multicast locator when it is on,
 * and the {@link BeaconLocator} beans that apply to the group, in that order),
 * with the group's configuration in hand. The starter's default is a
 * {@link BootstrapChain} over all of them; define a bean of this type, or of
 * the peering form, to change the composition policy for every group
 * (first-wins, a different order, a different per-locator deadline) while the
 * locators themselves stay as they are.
 */
@FunctionalInterface
public interface BootstrapChainFactory extends ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChainFactory {

    /**
     * Composes the locator for one group.
     *
     * @param group    the group's configuration
     * @param locators the seeds locator, the multicast locator when enabled, then the applicable beans
     * @return the locator the node joins with
     */
    BeaconLocator chainFor(AgentSpacesProperties.Group group, List<BeaconLocator> locators);

    /** The peering form, satisfied by name: a group object carrying the name is passed to {@link #chainFor(AgentSpacesProperties.Group, List)}. */
    @Override
    default BeaconLocator chainFor(String groupName, List<BeaconLocator> locators) {
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName(groupName);
        return chainFor(group, locators);
    }

    /** The starter's default: a chain over the locators in the given order. */
    static BootstrapChainFactory defaults() {
        return (group, locators) -> BootstrapChain.builder().addAll(locators).build();
    }

    /**
     * Adapts the peering form, so a host that composes by name alone (the
     * bootstrap project's composer, a plain-Java host) plugs into the starter.
     *
     * @param factory the peering form
     * @return the Spring form
     */
    static BootstrapChainFactory of(ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChainFactory factory) {
        if (factory instanceof BootstrapChainFactory spring) {
            return spring;
        }
        return (group, locators) -> factory.chainFor(group.getName(), locators);
    }
}
