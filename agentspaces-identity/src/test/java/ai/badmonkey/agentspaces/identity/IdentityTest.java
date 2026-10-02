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
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdentityTest {

    @Test
    void keystoreCreatesThenReloadsTheSameIdentity(@TempDir Path dir) {
        PeerIdentity first = FileKeystore.loadOrCreate(dir);
        PeerIdentity second = FileKeystore.loadOrCreate(dir);

        assertThat(second.peerId()).isEqualTo(first.peerId());

        byte[] message = "stable identity".getBytes(StandardCharsets.UTF_8);
        assertThat(first.verify(message, second.sign(message))).isTrue();
    }

    @Test
    void distinctKeystoresYieldDistinctPeers(@TempDir Path dirA, @TempDir Path dirB) {
        assertThat(FileKeystore.loadOrCreate(dirA).peerId())
                .isNotEqualTo(FileKeystore.loadOrCreate(dirB).peerId());
    }

    @Test
    void agentIdsHangOffThePeer() {
        PeerIdentity identity = PeerIdentity.generate();

        assertThat(identity.agent("researcher").peer()).isEqualTo(identity.peerId());
        assertThat(identity.agent("researcher").localName()).isEqualTo("researcher");
    }

    @Test
    void advertisementSignatureRoundTrips() {
        PeerIdentity identity = PeerIdentity.generate();
        AdvertisementSigner signer = new AdvertisementSigner();
        AgentCard card = card(identity);

        SignedAdvertisement<AgentCard> signed = signer.sign(card, identity);

        assertThat(signer.verify(signed)).isTrue();
        assertThat(signer.verify(signed, identity.publicKey())).isTrue();
    }

    @Test
    void selfContainedVerificationRejectsAForeignKey() {
        PeerIdentity issuer = PeerIdentity.generate();
        PeerIdentity other = PeerIdentity.generate();
        AdvertisementSigner signer = new AdvertisementSigner();
        SignedAdvertisement<AgentCard> signed = signer.sign(card(issuer), issuer);

        // Same advertisement and signature, but the wrapper claims another key:
        // the key no longer hashes to the issuer PeerID, so verification fails.
        SignedAdvertisement<AgentCard> swapped = new SignedAdvertisement<>(
                signed.advertisement(), other.rawPublicKey(), signed.signature());

        assertThat(signer.verify(swapped)).isFalse();
    }

    @Test
    void tamperedAdvertisementFailsVerification() {
        PeerIdentity identity = PeerIdentity.generate();
        AdvertisementSigner signer = new AdvertisementSigner();
        SignedAdvertisement<AgentCard> signed = signer.sign(card(identity), identity);

        AgentCard forged = new AgentCard(signed.advertisement().id(), identity.peerId(),
                signed.advertisement().group(), signed.advertisement().issued(),
                signed.advertisement().ttl(), identity.agent("researcher"),
                "Forged description", List.of(), List.of(), List.of(), Map.of());
        SignedAdvertisement<AgentCard> tampered =
                new SignedAdvertisement<>(forged, identity.rawPublicKey(), signed.signature());

        assertThat(signer.verify(tampered)).isFalse();
    }

    @Test
    void signatureByTheWrongPeerFailsVerification() {
        PeerIdentity issuer = PeerIdentity.generate();
        PeerIdentity other = PeerIdentity.generate();
        AdvertisementSigner signer = new AdvertisementSigner();

        SignedAdvertisement<AgentCard> signed = signer.sign(card(issuer), issuer);

        assertThat(signer.verify(signed, other.publicKey())).isFalse();
    }

    @Test
    void signingRefusesAMismatchedIssuer() {
        PeerIdentity issuer = PeerIdentity.generate();
        PeerIdentity other = PeerIdentity.generate();

        assertThatThrownBy(() -> new AdvertisementSigner().sign(card(issuer), other))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static AgentCard card(PeerIdentity identity) {
        return new AgentCard("aspace://zG/agent/researcher", identity.peerId(),
                ai.badmonkey.agentspaces.common.id.GroupId.of("zG"),
                Instant.parse("2026-08-26T14:00:00Z"), Duration.ofMinutes(15),
                identity.agent("researcher"), "Researches topics from the shared task space",
                List.of("research"), List.of("ResearchTask#v1"), List.of("Finding#v1"),
                Map.of());
    }
}
