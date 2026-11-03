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
package ai.badmonkey.agentspaces.peering.bootstrap;

import ai.badmonkey.agentspaces.api.spi.BeaconLocator;

import java.util.List;

/**
 * Composes the locator a group is joined with from the locators a host
 * collected for it: the configured seeds, the multicast locator when it is
 * on, and the host's own locators, in that order. The default is a
 * {@link BootstrapChain} over all of them. A host without Spring or Micronaut
 * uses this form directly; the Spring starter's
 * {@code ai.badmonkey.agentspaces.spring.BootstrapChainFactory} extends it
 * with the group's configuration object (review-110226-deadcode-parity §3.2).
 */
@FunctionalInterface
public interface BootstrapChainFactory {

    /**
     * Composes the locator for one group.
     *
     * @param groupName the configured group name
     * @param locators  the host's locators for the group, seeds first
     * @return the locator the node joins with
     */
    BeaconLocator chainFor(String groupName, List<BeaconLocator> locators);

    /** The default: a chain over the locators in the given order. */
    static BootstrapChainFactory defaults() {
        return (groupName, locators) -> BootstrapChain.builder().addAll(locators).build();
    }
}
