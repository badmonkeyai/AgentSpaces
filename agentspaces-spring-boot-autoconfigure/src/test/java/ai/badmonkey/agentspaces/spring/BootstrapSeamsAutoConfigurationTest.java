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

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.JoinCredentials;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bootstrap seams of the starter (SRAD DB-C-01 to DB-C-06, DB-C-15): a
 * {@link BeaconLocator} bean is asked for the groups it applies to, a
 * {@link BootstrapChainFactory} bean composes the chain, a {@link Transport}
 * bean replaces the default of its scheme, the customizer and configurer
 * hooks run, {@code advertise} is what members learn, and an INVITE group
 * founded from properties admits a member that presents a credential.
 */
class BootstrapSeamsAutoConfigurationTest {

    private final AgentSpacesAutoConfiguration autoConfig = new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static AgentSpacesProperties properties(int port, String groupName, List<String> seeds) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(100);
        properties.getTransport().getTls().setEnabled(false); // plain tcp: the fixtures name that scheme
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName(groupName);
        group.setFounding("seams-test-v1");
        group.setSeeds(seeds);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(50);
        group.setSpaces(List.of(tasks));
        properties.setGroups(List.of(group));
        return properties;
    }

    /** A locator that records the groups it was asked for and answers one seed. */
    private static final class Recording implements BeaconLocator {
        final List<String> askedFor = new ArrayList<>();
        final String only;
        final int seedPort;

        Recording(String only, int seedPort) {
            this.only = only;
            this.seedPort = seedPort;
        }

        @Override
        public String name() {
            return "recording";
        }

        @Override
        public BeaconHint locate(BeaconRequest request) {
            askedFor.add(request.groupName());
            return seedPort == 0 ? BeaconHint.none()
                    : BeaconHint.of(List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0)),
                            "recording");
        }

        @Override
        public boolean appliesTo(String groupName) {
            return only == null || only.equals(groupName);
        }
    }

    private record Wired(PeerIdentity identity, PeerNode node, AgentSpaces spaces, AgentSpacesLifecycle lifecycle) {
    }

    private Wired wire(AgentSpacesProperties properties, List<Transport> transports,
                       List<PeerNodeCustomizer> customizers, List<PeerNodeConfigurer> configurers,
                       List<BeaconLocator> locators, BootstrapChainFactory chains) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.buildNode(properties, identity, transports, customizers, configurers, null);
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        AgentSpaces spaces = autoConfig.agentSpacesWithRegistry(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder(), java.time.InstantSource.system(),
                autoConfig.agentSpacesAgentIdentities(properties, identity, java.time.InstantSource.system()),
                null, locators, chains);
        AgentSpacesLifecycle lifecycle = autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
        closeables.add(lifecycle::stop);
        return new Wired(identity, node, spaces, lifecycle);
    }

    private static void awaitMember(Wired wired, PeerNode other, String group) throws InterruptedException {
        GroupId id = wired.spaces.group(group).runtime().id();
        for (int i = 0; i < 100; i++) {
            if (wired.spaces.group(group).runtime().membership().member(other.peerId()).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no admission within 5 s in " + id);
    }

    @Test
    void aLocatorBeanIsAskedForTheGroupsItAppliesToAndFindsTheSeed() throws Exception {
        int seedPort = freePort();
        Wired seed = wire(properties(seedPort, "fleet", List.of()), List.of(), List.of(), List.of(),
                List.of(), BootstrapChainFactory.defaults());
        seed.lifecycle.start();
        Recording applies = new Recording("fleet", seedPort);
        Recording elsewhere = new Recording("other", seedPort);
        Wired joiner = wire(properties(freePort(), "fleet", List.of()), List.of(), List.of(), List.of(),
                List.of(applies, elsewhere), BootstrapChainFactory.defaults());
        joiner.lifecycle.start();

        awaitMember(seed, joiner.node, "fleet");
        assertThat(applies.askedFor).containsExactly("fleet");
        assertThat(elsewhere.askedFor).isEmpty();
        assertThat(joiner.node.lastBeaconHint(joiner.spaces.group("fleet").runtime().id()))
                .hasValueSatisfying(h -> assertThat(h.evidence()).contains("recording"));
    }

    @Test
    void aChainFactoryBeanComposesTheLocatorAndTheSeedsStillParse() throws Exception {
        int seedPort = freePort();
        Wired seed = wire(properties(seedPort, "fleet", List.of()), List.of(), List.of(), List.of(),
                List.of(), BootstrapChainFactory.defaults());
        seed.lifecycle.start();
        List<String> composed = new ArrayList<>();
        BootstrapChainFactory factory = (group, locators) -> {
            locators.forEach(l -> composed.add(l.name()));
            return locators.get(0); // the seeds alone
        };
        Wired joiner = wire(properties(freePort(), "fleet", List.of("127.0.0.1:" + seedPort)),
                List.of(), List.of(), List.of(), List.of(new Recording(null, 0)), factory);
        joiner.lifecycle.start();

        awaitMember(seed, joiner.node, "fleet");
        assertThat(composed).containsExactly("seeds", "recording");
    }

    @Test
    void aTransportBeanReplacesTheDefaultOfItsSchemeAndHooksRun() throws Exception {
        List<String> dialed = new ArrayList<>();
        Transport counting = new Transport() {
            private final ai.badmonkey.agentspaces.peering.transport.TcpTransport tcp =
                    new ai.badmonkey.agentspaces.peering.transport.TcpTransport();

            @Override
            public String scheme() {
                return "tcp";
            }

            @Override
            public TransportConnection dial(String address) throws IOException {
                dialed.add(address);
                return tcp.dial(address);
            }

            @Override
            public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept)
                    throws IOException {
                return tcp.listen(bindAddress, onAccept);
            }
        };
        Transport extra = new Transport() {
            @Override
            public String scheme() {
                return "mem";
            }

            @Override
            public TransportConnection dial(String address) throws IOException {
                throw new IOException("never");
            }

            @Override
            public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) {
                return () -> { };
            }
        };
        int seedPort = freePort();
        Wired seed = wire(properties(seedPort, "fleet", List.of()), List.of(), List.of(), List.of(),
                List.of(), BootstrapChainFactory.defaults());
        seed.lifecycle.start();
        List<String> hooks = new ArrayList<>();
        Wired joiner = wire(properties(freePort(), "fleet", List.of("127.0.0.1:" + seedPort)),
                List.of(counting, extra),
                List.of(b -> hooks.add("customizer")),
                List.of(n -> hooks.add("configurer:" + n.transportSchemes())),
                List.of(), BootstrapChainFactory.defaults());
        joiner.lifecycle.start();

        awaitMember(seed, joiner.node, "fleet");
        assertThat(dialed).as("the application's tcp transport dialed the seed").contains("127.0.0.1:" + seedPort);
        assertThat(joiner.node.transportSchemes()).contains("tcp", "mem");
        assertThat(hooks).containsExactly("customizer", "configurer:" + joiner.node.transportSchemes());
    }

    @Test
    void theAdvertisedAddressIsWhatMembersLearnAndIsCheckedAtStartup() throws Exception {
        int seedPort = freePort();
        AgentSpacesProperties advertised = properties(seedPort, "fleet", List.of());
        advertised.setAdvertise("tcp://seed.example:" + seedPort);
        Wired seed = wire(advertised, List.of(), List.of(), List.of(), List.of(), BootstrapChainFactory.defaults());
        seed.lifecycle.start();
        assertThat(seed.node.advertisedEndpoints()).extracting(PeerAdvertisement.Endpoint::address)
                .containsExactly("seed.example:" + seedPort);
        Wired joiner = wire(properties(freePort(), "fleet", List.of("127.0.0.1:" + seedPort)),
                List.of(), List.of(), List.of(), List.of(), BootstrapChainFactory.defaults());
        joiner.lifecycle.start();
        awaitMember(seed, joiner.node, "fleet");
        awaitMember(joiner, seed.node, "fleet");
        assertThat(joiner.spaces.group("fleet").runtime().membership().member(seed.node.peerId()))
                .hasValueSatisfying(m -> assertThat(m.endpoints()).extracting(PeerAdvertisement.Endpoint::address)
                        .containsExactly("seed.example:" + seedPort));

        AgentSpacesProperties wrongScheme = properties(freePort(), "fleet", List.of());
        wrongScheme.setAdvertise("quic://x:1");
        assertThatThrownBy(() -> wire(wrongScheme, List.of(), List.of(), List.of(), List.of(),
                BootstrapChainFactory.defaults())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("quic");
        AgentSpacesProperties dialOnly = properties(0, "fleet", List.of());
        dialOnly.setBind(null);
        dialOnly.setAdvertise("x:1");
        assertThatThrownBy(() -> wire(dialOnly, List.of(), List.of(), List.of(), List.of(),
                BootstrapChainFactory.defaults())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.bind");
    }

    @Test
    void anInviteGroupFromPropertiesAdmitsACredentialAndRefusesAStranger() throws Exception {
        int founderPort = freePort();
        AgentSpacesProperties founderProps = properties(founderPort, "fleet", List.of());
        founderProps.getGroups().get(0).setMembership("invite");
        Wired founder = wire(founderProps, List.of(), List.of(), List.of(), List.of(), BootstrapChainFactory.defaults());
        founder.lifecycle.start();
        GroupId groupId = founder.spaces.group("fleet").runtime().id();

        PeerIdentity guestId = PeerIdentity.generate();
        String credential = JoinCredentials.issue(founder.identity, groupId, guestId.peerId());
        AgentSpacesProperties guestProps = properties(freePort(), "fleet", List.of("127.0.0.1:" + founderPort));
        guestProps.getGroups().get(0).setMembership("invite");
        guestProps.getGroups().get(0).setFounder(founder.identity.peerId().value());
        guestProps.getGroups().get(0).setJoinCredential(credential);
        PeerNode guestNode = autoConfig.agentSpacesNode(guestProps, guestId);
        Authorizers guestAuth = autoConfig.agentSpacesAuthorizers(guestProps, guestId);
        AgentSpaces guestSpaces = autoConfig.agentSpaces(guestProps, guestNode, guestId, guestAuth);
        AgentSpacesLifecycle guestLifecycle = autoConfig.agentSpacesLifecycle(guestProps, guestNode, guestSpaces, guestAuth);
        closeables.add(guestLifecycle::stop);
        guestLifecycle.start();
        awaitMember(founder, guestNode, "fleet");

        AgentSpacesProperties strangerProps = properties(freePort(), "fleet", List.of("127.0.0.1:" + founderPort));
        strangerProps.getGroups().get(0).setMembership("invite");
        strangerProps.getGroups().get(0).setFounder(founder.identity.peerId().value());
        Wired stranger = wire(strangerProps, List.of(), List.of(), List.of(), List.of(), BootstrapChainFactory.defaults());
        stranger.lifecycle.start();
        Thread.sleep(1_500);
        assertThat(founder.spaces.group("fleet").runtime().membership().member(stranger.node.peerId()))
                .as("no credential, no admission").isEmpty();

        AgentSpacesProperties noFounder = properties(freePort(), "fleet", List.of());
        noFounder.getGroups().get(0).setMembership("invite");
        noFounder.getGroups().get(0).setJoinCredential(credential);
        assertThatThrownBy(() -> wire(noFounder, List.of(), List.of(), List.of(), List.of(),
                BootstrapChainFactory.defaults())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("founder");
        AgentSpacesProperties badPolicy = properties(freePort(), "fleet", List.of());
        badPolicy.getGroups().get(0).setMembership("secret");
        assertThatThrownBy(() -> wire(badPolicy, List.of(), List.of(), List.of(), List.of(),
                BootstrapChainFactory.defaults())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("membership");
    }

    /** A required chain that answers nothing refuses startup and names the group (ISSUE-BootstrapCore FR-4). */
    @Test
    void aRequiredChainThatAnswersNothingRefusesStartup() throws Exception {
        BootstrapChainFactory required = (group, locators) -> ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain
                .builder().addAll(locators).expandNames(null)
                .locatePolicy(ai.badmonkey.agentspaces.api.spi.LocatePolicy.DEFAULT.withRequire(true)).build();
        assertThatThrownBy(() -> wire(properties(freePort(), "fleet", List.of()), List.of(), List.of(), List.of(),
                List.of(new Recording("fleet", 0)), required))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fleet").hasMessageContaining("bootstrap.require");
    }

    /** The locator is asked by the configured name even when the founding's name differs (FR-9). */
    @Test
    void theLocatorIsAskedByTheConfiguredNameNotTheFoundingsName() throws Exception {
        int seedPort = freePort();
        PeerIdentity founderId = PeerIdentity.generate();
        ai.badmonkey.agentspaces.peering.node.SignedGroupAdvertisement founding =
                ai.badmonkey.agentspaces.peering.node.GroupFounding.found(founderId, "Weekend hack",
                        ai.badmonkey.agentspaces.api.ad.GroupAdvertisement.MembershipPolicy.OPEN,
                        ai.badmonkey.agentspaces.api.space.ConflictStrategyType.LEASE_RACE,
                        ai.badmonkey.agentspaces.api.ad.GroupAdvertisement.GossipParameters.defaults(),
                        java.time.Instant.now(), Duration.ofDays(1));
        String document = java.util.Base64.getEncoder().encodeToString(
                ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec().toBytes(founding));
        AgentSpacesProperties seedProps = properties(seedPort, "weekend", List.of());
        seedProps.getGroups().get(0).setFounding(null);
        seedProps.getGroups().get(0).setFoundingDocument(document);
        Wired seed = wire(seedProps, List.of(), List.of(), List.of(), List.of(), BootstrapChainFactory.defaults());
        seed.lifecycle.start();
        Recording recording = new Recording("weekend", seedPort);
        AgentSpacesProperties joinerProps = properties(freePort(), "weekend", List.of());
        joinerProps.getGroups().get(0).setFounding(null);
        joinerProps.getGroups().get(0).setFoundingDocument(document);
        Wired joiner = wire(joinerProps, List.of(), List.of(), List.of(), List.of(recording), BootstrapChainFactory.defaults());
        joiner.lifecycle.start();
        awaitMember(seed, joiner.node, "weekend");
        assertThat(recording.askedFor).containsExactly("weekend");
    }
}
