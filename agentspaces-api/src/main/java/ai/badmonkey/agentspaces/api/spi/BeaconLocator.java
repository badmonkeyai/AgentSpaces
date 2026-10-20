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
package ai.badmonkey.agentspaces.api.spi;

import java.io.IOException;

/**
 * One way of finding a beacon for a group: a configured seed list, a DNS name
 * with several addresses, an SRV or DNS-SD record set, a well-known HTTPS
 * document, a LAN multicast datagram, a registry adapter. A locator answers
 * "whom do I dial first?" with a {@link BeaconHint} and nothing more: whatever
 * it returns is dialed through the node's ordinary stranger path (bootstrap
 * budget, full advertisement verification, admission policy), so a locator
 * can never admit anyone. Implementations are stateless or cache internally
 * and are safe to call from any thread.
 *
 * <p>A locator that has nothing returns {@link BeaconHint#none()}; it throws
 * only when it could not even try (a resolver that is down), and the chain
 * records the error and continues with the next locator.
 */
public interface BeaconLocator {

    /** A short name for traces and configuration: {@code seeds}, {@code dns}, {@code dns-srv}, {@code url}. */
    String name();

    /**
     * Finds beacons for the request.
     *
     * @param request what the node knows
     * @return the hint, possibly {@link BeaconHint#none()}
     * @throws IOException when the locator could not try
     */
    BeaconHint locate(BeaconRequest request) throws IOException;

    /**
     * Whether this locator serves the named group. A host that collects
     * locators as beans applies each to every group unless it narrows itself
     * here. The default is every group.
     *
     * @param groupName the configured group name
     * @return {@code true} to be asked for that group
     */
    default boolean appliesTo(String groupName) {
        return true;
    }

    /**
     * How the node should pace, require, and trace this locator's answers.
     * The default leaves everything to the node's own settings.
     *
     * @return the policy
     */
    default LocatePolicy policy() {
        return LocatePolicy.DEFAULT;
    }
}
