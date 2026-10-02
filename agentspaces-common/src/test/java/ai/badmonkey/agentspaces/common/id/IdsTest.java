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
package ai.badmonkey.agentspaces.common.id;

import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdsTest {

    @Test
    void peerIdDerivationIsDeterministicAndMultibase() {
        byte[] raw = Ed25519.rawPublicKey(Ed25519.generate().getPublic());

        PeerId first = PeerId.fromPublicKey(raw);
        PeerId second = PeerId.fromPublicKey(raw);

        assertThat(first).isEqualTo(second);
        assertThat(first.value()).startsWith("z");
        assertThat(first.display()).startsWith("peer:");
    }

    @Test
    void differentKeysYieldDifferentPeerIds() {
        PeerId a = PeerId.fromPublicKey(Ed25519.rawPublicKey(Ed25519.generate().getPublic()));
        PeerId b = PeerId.fromPublicKey(Ed25519.rawPublicKey(Ed25519.generate().getPublic()));

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void agentIdRoundTripsItsCanonicalForm() {
        AgentId agent = new AgentId(PeerId.of("zPeer"), "researcher");

        assertThat(agent.encoded()).isEqualTo("zPeer/researcher");
        assertThat(AgentId.parse(agent.encoded())).isEqualTo(agent);
        assertThatThrownBy(() -> AgentId.parse("nozslash"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AgentId(PeerId.of("zPeer"), "bad/name"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void groupAndSpaceIdsAreSelfCertifyingHashes() {
        byte[] founding = "founding-advertisement".getBytes(StandardCharsets.UTF_8);

        GroupId group = GroupId.fromFounding(founding);
        SpaceId space = SpaceId.fromFounding(founding);

        assertThat(group.value()).startsWith("z");
        assertThat(GroupId.fromFounding(founding)).isEqualTo(group);
        assertThat(space.value()).isEqualTo(SpaceId.fromFounding(founding).value());
        assertThat(SpaceId.local("tasks")).isEqualTo(SpaceId.local("tasks"));
        assertThat(SpaceId.local("tasks")).isNotEqualTo(SpaceId.local("findings"));
    }

    /** SPEC §4.1/§4.4 KAT: PeerID, GroupID and SpaceID for the fixed key and founding bytes of agentspaces-python/python/tests/golden.json. */
    @Test
    void groupIdIsTheHashOfTheFoundingBytesExactly() throws Exception {
        // Values pinned from golden.json (public_key_raw, peer_id, group_id,
        // space_id), the same vectors the Python and TypeScript clients prove
        // themselves against, so all three implementations derive identically.
        byte[] founding = "research-fleet-demo-v1".getBytes(StandardCharsets.UTF_8);
        byte[] rawKey = java.util.HexFormat.of().parseHex(
                "5c4cb3592f4d77aef1510def4795e8f18d00d647c7f04d2cb67261d6f0b3ce3a");
        String expectedGroup = "zBQJtZ4UK3uLsxZe6Tz9PEnCrbtp3mDH2jf4hHavarmom";
        String expectedPeer = "z6M1N9PyyHGkJfLZRmQ4VjAypZUi5uuUm8UDMHK1YfieJ";
        String expectedSpace = "z2zifgVUFH2KJqQSoZxoEsBTmVtcgYqDuemnhq9NS7L3a";

        GroupId group = GroupId.fromFounding(founding);
        assertThat(group.value()).isEqualTo(expectedGroup);
        assertThat(PeerId.fromPublicKey(rawKey).value()).isEqualTo(expectedPeer);
        assertThat(SpaceId.local(expectedGroup + "/tasks").value()).isEqualTo(expectedSpace);

        // The name is nothing but the multibase of the SHA-256: no salt, no
        // prefix, so possession of the founding bytes proves the name.
        assertThat(ai.badmonkey.agentspaces.common.codec.Multibase.decode(group.value()))
                .isEqualTo(ai.badmonkey.agentspaces.common.crypto.Digests.sha256(founding));

        // When the shared vector file is reachable, it must still say the same.
        for (java.nio.file.Path candidate : java.util.List.of(
                java.nio.file.Path.of("..", "tools", "golden", "golden.json"),
                java.nio.file.Path.of("..", "..", "agentspaces-spec", "golden.json"))) {
            if (java.nio.file.Files.exists(candidate)) {
                com.fasterxml.jackson.databind.JsonNode golden =
                        new com.fasterxml.jackson.databind.ObjectMapper().readTree(candidate.toFile());
                assertThat(golden.get("group_id").asText()).isEqualTo(expectedGroup);
                assertThat(golden.get("peer_id").asText()).isEqualTo(expectedPeer);
                assertThat(golden.get("space_id").asText()).isEqualTo(expectedSpace);
                assertThat(golden.get("public_key_raw").asText())
                        .isEqualTo(java.util.HexFormat.of().formatHex(rawKey));
                break;
            }
        }
    }

    @Test
    void aspaceUriRoundTrips() {
        GroupId group = GroupId.of("zGroup");
        AspaceUri bare = AspaceUri.of(group, "tasks");
        AspaceUri withEntry = new AspaceUri(group, "tasks", "entry-42");

        assertThat(bare.encoded()).isEqualTo("aspace://zGroup/tasks");
        assertThat(withEntry.encoded()).isEqualTo("aspace://zGroup/tasks#entry-42");
        assertThat(AspaceUri.parse(bare.encoded())).isEqualTo(bare);
        assertThat(AspaceUri.parse(withEntry.encoded())).isEqualTo(withEntry);
        assertThat(withEntry.entryFragment()).contains("entry-42");
        assertThat(bare.entryFragment()).isEmpty();
    }

    @Test
    void aspaceUriRejectsMalformedInput() {
        assertThatThrownBy(() -> AspaceUri.parse("http://x/y"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AspaceUri.parse("aspace://groupOnly"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AspaceUri(GroupId.of("z"), "bad/space", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
