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
import ai.badmonkey.agentspaces.common.id.GroupId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** The chain's merge, acceptance, deadline, and expansion rules (SRAD §3.3). */
class BootstrapChainTest {

    private static final BeaconRequest REQUEST = new BeaconRequest("fleet", Optional.empty(), "tcp", Set.of("tcp"));

    private static BeaconLocator locator(String name, BeaconHint hint) {
        return new BeaconLocator() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public BeaconHint locate(BeaconRequest request) {
                return hint;
            }
        };
    }

    private static InetAddress address(String literal) {
        try {
            return InetAddress.getByName(literal);
        } catch (java.net.UnknownHostException e) {
            throw new IllegalArgumentException(literal, e);
        }
    }

    private static PeerAdvertisement.Endpoint ep(String address, int priority) {
        return new PeerAdvertisement.Endpoint("tcp", address, priority);
    }

    @Test
    void endpointsMergeInChainOrderDeduplicatedAndRenumbered() {
        BootstrapChain chain = BootstrapChain.builder()
                .add(locator("a", BeaconHint.of(List.of(ep("10.0.0.2:7500", 1), ep("10.0.0.1:7500", 0)), "a")))
                .add(locator("b", BeaconHint.of(List.of(ep("10.0.0.1:7500", 0), ep("10.0.0.3:7500", 1)), "b")))
                .expandNames(null)
                .build();
        BeaconHint hint = chain.locate(REQUEST);
        assertThat(hint.endpoints()).extracting(PeerAdvertisement.Endpoint::address)
                .containsExactly("10.0.0.1:7500", "10.0.0.2:7500", "10.0.0.3:7500");
        assertThat(hint.endpoints()).extracting(PeerAdvertisement.Endpoint::priority).containsExactly(0, 1, 2);
        assertThat(hint.evidence()).isEqualTo("a: a; b: b");
        assertThat(chain.lastTrace()).extracting(BootstrapChain.Trace::locator).containsExactly("a", "b");
    }

    @Test
    void aFailingOrSlowLocatorContributesNothingAndStopsNobody() {
        BeaconLocator throwing = new BeaconLocator() {
            @Override
            public String name() {
                return "broken";
            }

            @Override
            public BeaconHint locate(BeaconRequest request) throws IOException {
                throw new IOException("resolver down");
            }
        };
        BeaconLocator slow = new BeaconLocator() {
            @Override
            public String name() {
                return "slow";
            }

            @Override
            public BeaconHint locate(BeaconRequest request) throws IOException {
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return BeaconHint.of(List.of(ep("never:1", 0)), "late");
            }
        };
        BootstrapChain chain = BootstrapChain.builder().add(throwing).add(slow)
                .add(locator("good", BeaconHint.of(List.of(ep("10.0.0.9:7500", 0)), "ok")))
                .timeout(Duration.ofMillis(200)).expandNames(null).build();
        BeaconHint hint = chain.locate(REQUEST);
        assertThat(hint.endpoints()).extracting(PeerAdvertisement.Endpoint::address).containsExactly("10.0.0.9:7500");
        assertThat(chain.lastTrace()).extracting(BootstrapChain.Trace::error)
                .satisfies(errors -> {
                    assertThat(errors.get(0)).contains("resolver down");
                    assertThat(errors.get(1)).contains("timeout");
                    assertThat(errors.get(2)).isNull();
                });
    }

    @Test
    void firstWinsStopsAtTheFirstLocatorWithEndpoints() {
        BootstrapChain chain = BootstrapChain.builder()
                .add(locator("empty", BeaconHint.none()))
                .add(locator("a", BeaconHint.of(List.of(ep("10.0.0.1:7500", 0)), "a")))
                .add(locator("b", BeaconHint.of(List.of(ep("10.0.0.2:7500", 0)), "b")))
                .firstWins().expandNames(null).build();
        BeaconHint hint = chain.locate(REQUEST);
        assertThat(hint.endpoints()).extracting(PeerAdvertisement.Endpoint::address).containsExactly("10.0.0.1:7500");
        assertThat(chain.lastTrace()).extracting(BootstrapChain.Trace::locator).containsExactly("empty", "a");
    }

    @Test
    void aGroupIdClaimIsHonouredOnlyFromNamedLocatorsAndNeverOverridesAKnownOne() {
        GroupId claimed = GroupId.of("zClaimed");
        BeaconHint claim = new BeaconHint(List.of(ep("10.0.0.1:7500", 0)), Optional.of(claimed),
                Optional.empty(), Map.of(), Duration.ofSeconds(90), "txt");
        BootstrapChain dns = BootstrapChain.builder().add(locator("dns-txt", claim)).expandNames(null).build();
        assertThat(dns.locate(REQUEST).groupId()).as("dns is not trusted by default").isEmpty();

        BootstrapChain trusted = BootstrapChain.builder().add(locator("dns-txt", claim))
                .acceptGroupIdFrom(Set.of("dns-txt")).expandNames(null).build();
        assertThat(trusted.locate(REQUEST).groupId()).contains(claimed);

        BeaconRequest known = new BeaconRequest("fleet", Optional.of(GroupId.of("zKnown")), "tcp", Set.of("tcp"));
        assertThat(trusted.locate(known).groupId()).contains(GroupId.of("zKnown"));
        assertThat(trusted.locate(known).ttl()).as("the shortest contributor's ttl").isEqualTo(Duration.ofSeconds(90));
    }

    @Test
    void theFirstFoundingWinsAndParametersMergeFirstWins() {
        BeaconHint first = new BeaconHint(List.of(), Optional.empty(), Optional.of(new byte[]{1}),
                Map.of("wire.session", "1", "region", "a"), Duration.ofMinutes(1), "doc-a");
        BeaconHint second = new BeaconHint(List.of(ep("10.0.0.1:7500", 0)), Optional.empty(), Optional.of(new byte[]{2}),
                Map.of("region", "b", "multicast", "239.1.1.1:1"), Duration.ofMinutes(2), "doc-b");
        BeaconHint hint = BootstrapChain.builder().add(locator("x", first)).add(locator("y", second))
                .expandNames(null).build().locate(REQUEST);
        assertThat(hint.founding()).contains(new byte[]{1});
        assertThat(hint.parameters()).containsEntry("region", "a").containsEntry("multicast", "239.1.1.1:1");
    }

    @Test
    void namesExpandToEveryAddressAsTheLastStep() throws Exception {
        BootstrapChain chain = BootstrapChain.builder()
                .add(locator("seeds", BeaconHint.of(List.of(ep("seeds.example:7500", 0), ep("10.9.9.9:7500", 1)), "s")))
                .expandNames(host -> host.equals("seeds.example")
                        ? List.of(address("10.0.0.1"), address("10.0.0.2"), address("2001:db8::17"))
                        : List.of())
                .build();
        assertThat(chain.locate(REQUEST).endpoints()).extracting(PeerAdvertisement.Endpoint::address)
                .containsExactly("10.0.0.1:7500", "10.0.0.2:7500", "[2001:db8:0:0:0:0:0:17]:7500", "10.9.9.9:7500");
    }

    @Test
    void aLocatorScopedToAnotherGroupIsNotAsked() {
        BeaconLocator scoped = new BeaconLocator() {
            @Override
            public String name() {
                return "scoped";
            }

            @Override
            public BeaconHint locate(BeaconRequest request) {
                return BeaconHint.of(List.of(ep("10.0.0.1:7500", 0)), "s");
            }

            @Override
            public boolean appliesTo(String groupName) {
                return groupName.equals("other");
            }
        };
        BootstrapChain chain = BootstrapChain.builder().add(scoped).expandNames(null).build();
        assertThat(chain.locate(REQUEST).isEmpty()).isTrue();
        assertThat(chain.lastTrace()).isEmpty();
    }

    @Test
    void seedStringsParseWithAnOptionalScheme() {
        List<PeerAdvertisement.Endpoint> parsed = SeedsLocator.parse(List.of("hq.example:7500", "tcp://a:1", " "), "tls");
        assertThat(parsed).extracting(PeerAdvertisement.Endpoint::transport).containsExactly("tls", "tcp");
        assertThat(parsed).extracting(PeerAdvertisement.Endpoint::address).containsExactly("hq.example:7500", "a:1");
        assertThat(SeedsLocator.of(List.of()).locate(REQUEST).isEmpty()).isTrue();
    }

    @Test
    void splitHandlesPortsAndIpv6Literals() {
        assertThat(DnsExpansion.split("host:7500")).containsExactly("host", "7500");
        assertThat(DnsExpansion.split("[::1]:7500")).containsExactly("::1", "7500");
        assertThat(DnsExpansion.split("::1")).containsExactly("::1", "");
        assertThat(DnsExpansion.split("host")).containsExactly("host", "");
        assertThat(DnsExpansion.isLiteral("10.0.0.1")).isTrue();
        assertThat(DnsExpansion.isLiteral("seeds.example")).isFalse();
    }
}
