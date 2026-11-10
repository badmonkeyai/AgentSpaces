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
package ai.badmonkey.agentspaces.agent.content;

import ai.badmonkey.agentspaces.common.codec.CanonicalMaps;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 follow-ups, 2026-11-09, F1 (pin consent): a pin entry carries its requester's signature over the
 * pin's terms, so a store charges only the member who signed them, a forwarded copy keeps the signed terms, and
 * nobody else can name a requester.
 */
class SignedPinTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final String ROOT = "bafyreigq3u2qzvyq5r6dpq2nqq4cujdxa6wmljnrrjz4ns2nk2o3clj5vy";
    private static final GroupId GROUP = GroupId.of("zStores");
    private static final long NOT_AFTER = 1792406400000L;

    private static StorePin terms(PeerIdentity requester, int copies) {
        return new StorePin(StorePin.VERSION, ROOT, StorePin.KIND_TREE, 86400, copies, requester.peerId(), List.of(),
                Map.of("pdt.stores", "zA,zB"));
    }

    @Test
    void theRequestersSignatureCoversTheTermsTheGroupAndTheDeadline() {
        PeerIdentity requester = PeerIdentity.generate();
        SignedPin signed = SignedPin.sign(terms(requester, 2), GROUP, NOT_AFTER, requester);
        assertThat(signed.violation()).isEmpty();
        assertThat(signed.left()).isEqualTo(2);
        assertThat(signed.consented(GROUP)).isTrue();
        assertThat(signed.consented(GroupId.of("zOther"))).as("a signature for one group is no consent in another").isFalse();
        assertThat(signed.tags()).containsExactlyInAnyOrderEntriesOf(signed.pin().tags());

        byte[] bytes = CODEC.toBytes(signed);
        SignedPin read = CODEC.fromWire(bytes, SignedPin.class);
        assertThat(read).isEqualTo(signed);
        assertThat(read.consented(GROUP)).isTrue();
    }

    @Test
    void aForwardedCopyKeepsTheSignedTermsAndCountsDown() {
        PeerIdentity requester = PeerIdentity.generate();
        SignedPin signed = SignedPin.sign(terms(requester, 3), GROUP, NOT_AFTER, requester);
        SignedPin forwarded = signed.forward();
        assertThat(forwarded.left()).isEqualTo(2);
        assertThat(forwarded.pin()).isEqualTo(signed.pin());
        assertThat(forwarded.consented(GROUP)).isTrue();
        assertThat(forwarded.forward().forward().violation()).hasValue("left 0 is outside 1 to 3 copies");
    }

    @Test
    void aPinItsRequesterDidNotSignIsNoConsent() {
        PeerIdentity requester = PeerIdentity.generate();
        PeerIdentity forger = PeerIdentity.generate();
        // the forger signs terms that name another member as the requester
        SignedPin forged = SignedPin.sign(terms(forger, 1), GROUP, NOT_AFTER, forger);
        SignedPin renamed = new SignedPin(SignedPin.VERSION, terms(requester, 1), 1, NOT_AFTER, forged.key(), forged.sig());
        assertThat(renamed.violation()).isEmpty();
        assertThat(renamed.consented(GROUP)).as("the key is not the requester's").isFalse();
        SignedPin withRequesterKey = new SignedPin(SignedPin.VERSION, terms(requester, 1), 1, NOT_AFTER,
                requester.rawPublicKey(), forged.sig());
        assertThat(withRequesterKey.consented(GROUP)).as("the requester's key over another signature").isFalse();

        SignedPin honest = SignedPin.sign(terms(requester, 1), GROUP, NOT_AFTER, requester);
        StorePin moreCopies = terms(requester, 3);
        assertThat(new SignedPin(SignedPin.VERSION, moreCopies, 1, NOT_AFTER, honest.key(), honest.sig()).consented(GROUP))
                .as("changed terms").isFalse();
        assertThat(new SignedPin(SignedPin.VERSION, honest.pin(), 1, NOT_AFTER + 1, honest.key(), honest.sig()).consented(GROUP))
                .as("a later deadline").isFalse();
        assertThatThrownBy(() -> SignedPin.sign(terms(requester, 1), GROUP, NOT_AFTER, forger))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requester");
    }

    @Test
    void aForgersOwnSignatureOverTermsNamingAnotherRequesterIsNoConsent() {
        PeerIdentity requester = PeerIdentity.generate();
        PeerIdentity forger = PeerIdentity.generate();
        // the forger signs the very consent bytes of terms that name the requester, so the signature verifies
        // under the forger's key and only the binding of the key to the requester refuses it
        StorePin named = terms(requester, 1);
        byte[] consent = SignedPin.consentBytes(named, GROUP, NOT_AFTER);
        byte[] sig = forger.sign(consent);
        assertThat(Ed25519.verifyRaw(forger.rawPublicKey(), consent, sig)).isTrue();
        SignedPin forged = new SignedPin(SignedPin.VERSION, named, 1, NOT_AFTER, forger.rawPublicKey(), sig);
        assertThat(forged.violation()).isEmpty();
        assertThat(forged.consented(GROUP)).isFalse();
    }

    @Test
    void everySignedTermBreaksTheSignatureWhenChangedAndOnlyTheCopiesLeftAreFree() {
        PeerIdentity requester = PeerIdentity.generate();
        SignedPin honest = SignedPin.sign(terms(requester, 2), GROUP, NOT_AFTER, requester);
        StorePin t = honest.pin();
        String otherRoot = "bafyreihstcpt3qhkqw2kzczkq7hslg4wq4rh3ukcn6f22ndpxq3crdi6wq";
        List<StorePin> changed = List.of(
                new StorePin(t.v(), otherRoot, t.kind(), t.lease(), t.copies(), t.requester(), t.haves(), t.attrs()),
                new StorePin(t.v(), t.root(), StorePin.KIND_ARTIFACT, t.lease(), t.copies(), t.requester(), t.haves(), t.attrs()),
                new StorePin(t.v(), t.root(), t.kind(), t.lease() * 2, t.copies(), t.requester(), t.haves(), t.attrs()),
                new StorePin(t.v(), t.root(), t.kind(), t.lease(), t.copies(), t.requester(), List.of(otherRoot), t.attrs()),
                new StorePin(t.v(), t.root(), t.kind(), t.lease(), t.copies(), t.requester(), t.haves(), Map.of("pdt.stores", "zM")),
                new StorePin(t.v(), t.root(), t.kind(), t.lease(), t.copies(), t.requester(), t.haves(), null));
        for (StorePin pin : changed) {
            assertThat(new SignedPin(SignedPin.VERSION, pin, 1, NOT_AFTER, honest.key(), honest.sig()).consented(GROUP))
                    .as("changed terms " + pin).isFalse();
        }
        // the copies left travel outside the signature, so a store can forward (the open item of 10.9)
        assertThat(new SignedPin(SignedPin.VERSION, t, 1, NOT_AFTER, honest.key(), honest.sig()).consented(GROUP)).isTrue();
    }

    @Test
    void aSignedPinNamesEveryBoundItBreaks() {
        PeerIdentity requester = PeerIdentity.generate();
        SignedPin ok = SignedPin.sign(terms(requester, 2), GROUP, NOT_AFTER, requester);
        assertThat(new SignedPin(2, ok.pin(), 2, NOT_AFTER, ok.key(), ok.sig()).violation()).hasValue("unsupported SignedPin version 2");
        assertThat(new SignedPin(1, ok.pin(), 3, NOT_AFTER, ok.key(), ok.sig()).violation()).hasValue("left 3 is outside 1 to 2 copies");
        assertThat(new SignedPin(1, ok.pin(), 2, -1, ok.key(), ok.sig()).violation()).hasValue("negative notAfter");
        assertThat(new SignedPin(1, ok.pin(), 2, NOT_AFTER, new byte[31], ok.sig()).violation()).hasValue("key is not 32 bytes");
        assertThat(new SignedPin(1, ok.pin(), 2, NOT_AFTER, ok.key(), new byte[63]).violation()).hasValue("sig is not 64 bytes");
        StorePin bad = new StorePin(StorePin.VERSION, "root", StorePin.KIND_TREE, 1, 1, requester.peerId(), List.of(), null);
        assertThat(new SignedPin(1, bad, 1, NOT_AFTER, ok.key(), ok.sig()).violation()).hasValue("root is not a CID");
    }

    @Test
    void aSignedPinEncodesInSchemaOrderAndRefusesAFieldItsSchemaDoesNotName() {
        PeerIdentity requester = PeerIdentity.generate();
        SignedPin signed = SignedPin.sign(terms(requester, 2), GROUP, NOT_AFTER, requester);
        CanonicalMaps.Verbatim fields = new CanonicalMaps.Verbatim();
        fields.put("v", 1);
        fields.put("pin", signed.pin());
        fields.put("left", 2);
        fields.put("notAfter", NOT_AFTER);
        fields.put("key", signed.key());
        fields.put("sig", signed.sig());
        assertThat(CODEC.toBytes(signed)).isEqualTo(CODEC.toBytes(fields));
        fields.put("holder", "zForger");
        assertThatThrownBy(() -> CODEC.fromWire(CODEC.toBytes(fields), SignedPin.class)).isInstanceOf(UncheckedIOException.class);

        CanonicalMaps.Verbatim consent = new CanonicalMaps.Verbatim();
        consent.put("purpose", SignedPin.PURPOSE);
        consent.put("group", "zStores");
        consent.put("pin", signed.pin());
        consent.put("notAfter", NOT_AFTER);
        assertThat(SignedPin.consentBytes(signed.pin(), GROUP, NOT_AFTER)).isEqualTo(CODEC.toBytes(consent));
    }
}
