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

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The multicast beacon as a locator (SRAD DB-L-08): the endpoints this node
 * learned from verified beacon datagrams for the requested group, so the
 * beacon appears in the chain's order and trace beside the other locators.
 * The datagram's own admission path is unchanged; this locator only reports
 * what it heard, each endpoint until the advertisement that carried it
 * expires.
 */
public final class MulticastLocator implements BeaconLocator {

    /** The locator's name. */
    public static final String NAME = "multicast";

    private final PeerNode node;

    /**
     * A locator over a node's beacon.
     *
     * @param node the node whose beacon hears the datagrams
     */
    public MulticastLocator(PeerNode node) {
        this.node = Objects.requireNonNull(node, "node");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public BeaconHint locate(BeaconRequest request) {
        if (request.groupId().isEmpty()) {
            return BeaconHint.none();
        }
        PeerNode.BeaconLearned learned = node.beaconLearned(request.groupId().get());
        List<PeerAdvertisement.Endpoint> endpoints = learned.endpoints();
        if (endpoints.isEmpty()) {
            return BeaconHint.none();
        }
        return new BeaconHint(endpoints, Optional.empty(), Optional.empty(), Map.of(),
                learned.ttl().isZero() ? Duration.ofSeconds(30) : learned.ttl(),
                "beacon datagrams from " + learned.peers() + " peer(s)");
    }
}
