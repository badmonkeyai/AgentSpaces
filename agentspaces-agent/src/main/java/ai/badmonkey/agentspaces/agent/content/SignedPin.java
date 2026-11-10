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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.WireRecord;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The pin entry a content store takes (PDT 2 follow-ups, 2026-11-09, F1; srad/agentspaces-pdt2.md §3.2.2 and
 * §10.9): the requester's {@link StorePin}, the requester's Ed25519 signature over it, and the copies still to
 * place. Named {@code ai.badmonkey.agentspaces.agent.content.SignedPin#v1} on the wire and tagged as its pin is,
 * {@link StorePin#ROOT_TAG} and {@link StorePin#REQUESTER_TAG}.
 *
 * <p>The owner's ruling of 2026-11-09: a store charges only the member who signed the pin. The signature covers
 * the {@link #consentBytes} of the terms, the group, and {@link #notAfter}, under the key {@link #key}, whose peer
 * id is the requester's. A store that forwards the copies above one writes the same pin, signature, and deadline
 * with {@link #left} less one ({@link #forward()}), so it never speaks for the requester; any other change of the
 * terms breaks the signature. The deadline bounds a replay: no store takes the pin after it.
 *
 * <p>A protocol record with the annex I schema of §6.10.4, read from its own wire encoding alone ({@link WireRecord});
 * it travels ungated, like {@code StorePin#v1} before it, and changes shape only under a new type name (P2-F-08).
 * The pin it carries is the {@code StorePin#v1} record, byte for byte.
 *
 * @param v        schema version, {@link #VERSION}
 * @param pin      the requester's terms; {@code pin.copies()} is the copies asked
 * @param left     the copies still to place, 1 to {@code pin.copies()}
 * @param notAfter epoch milliseconds after which no store takes the pin
 * @param key      the requester's raw 32-byte Ed25519 public key
 * @param sig      the requester's 64-byte signature over {@link #consentBytes}
 */
@WireRecord
public record SignedPin(int v, StorePin pin, int left, long notAfter, byte[] key, byte[] sig) {

    /** Schema version this record carries. */
    public static final int VERSION = 1;

    /** The text the consent opens with, so the signature serves no other record of the requester's key. */
    public static final String PURPOSE = "agentspaces.store-pin";

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    /**
     * What the requester signs: the purpose, the group the pin is written in, the terms, and the deadline.
     *
     * @param purpose  {@link #PURPOSE}
     * @param group    the group's id
     * @param pin      the terms
     * @param notAfter the deadline in epoch milliseconds
     */
    record Consent(String purpose, String group, StorePin pin, long notAfter) {
    }

    public SignedPin {
        Objects.requireNonNull(pin, "pin");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(sig, "sig");
        key = key.clone();
        sig = sig.clone();
    }

    /**
     * Signs a requester's terms for every copy they ask.
     *
     * @param pin       the terms, naming the signer as requester
     * @param group     the group the pin is written in
     * @param notAfter  epoch milliseconds after which no store takes the pin
     * @param requester the requester's identity
     * @return the signed pin
     * @throws IllegalArgumentException when the terms name another requester
     */
    public static SignedPin sign(StorePin pin, GroupId group, long notAfter, PeerIdentity requester) {
        Objects.requireNonNull(pin, "pin");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(requester, "requester");
        if (!pin.requester().equals(requester.peerId())) {
            throw new IllegalArgumentException("the pin names requester " + pin.requester().display()
                    + ", and only that member signs it");
        }
        return new SignedPin(VERSION, pin, pin.copies(), notAfter, requester.rawPublicKey(),
                requester.sign(consentBytes(pin, group, notAfter)));
    }

    /**
     * The bytes a requester signs: the canonical CBOR of {@code {purpose, group, pin, notAfter}} in that order.
     *
     * @param pin      the terms
     * @param group    the group
     * @param notAfter the deadline
     * @return the bytes
     */
    public static byte[] consentBytes(StorePin pin, GroupId group, long notAfter) {
        return CODEC.toBytes(new Consent(PURPOSE, group.value(), pin, notAfter));
    }

    /**
     * Whether the requester the terms name signed them for this group and deadline: the key hashes to the
     * requester's peer id and the signature verifies over {@link #consentBytes}.
     *
     * @param group the group the pin was read in
     * @return whether the pin carries the requester's consent
     */
    public boolean consented(GroupId group) {
        Objects.requireNonNull(group, "group");
        return key.length == Ed25519.RAW_PUBLIC_KEY_LENGTH && PeerId.fromPublicKey(key).equals(pin.requester())
                && Ed25519.verifyRaw(key, consentBytes(pin, group, notAfter), sig);
    }

    /** The copy a store writes for the copies above one: the same terms and signature, one copy fewer left. */
    public SignedPin forward() {
        return new SignedPin(v, pin, left - 1, notAfter, key, sig);
    }

    /** The tags a signed pin is written with: its pin's. */
    public Map<String, String> tags() {
        return pin.tags();
    }

    /**
     * Checks the version, the pin's bounds, the copies left, the deadline's sign, and the key and signature
     * lengths. {@link #consented} checks the signature.
     *
     * @return the first violated rule, or empty when the record is well formed
     */
    public Optional<String> violation() {
        if (v != VERSION) {
            return Optional.of("unsupported SignedPin version " + v);
        }
        Optional<String> terms = pin.violation();
        if (terms.isPresent()) {
            return terms;
        }
        if (left < 1 || left > pin.copies()) {
            return Optional.of("left " + left + " is outside 1 to " + pin.copies() + " copies");
        }
        if (notAfter < 0) {
            return Optional.of("negative notAfter");
        }
        if (key.length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return Optional.of("key is not 32 bytes");
        }
        if (sig.length != 64) {
            return Optional.of("sig is not 64 bytes");
        }
        return Optional.empty();
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public byte[] sig() {
        return sig.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SignedPin s && v == s.v && left == s.left && notAfter == s.notAfter && pin.equals(s.pin)
                && Arrays.equals(key, s.key) && Arrays.equals(sig, s.sig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v, pin, left, notAfter, Arrays.hashCode(key), Arrays.hashCode(sig));
    }

    @Override
    public String toString() {
        return "SignedPin[v=" + v + ", pin=" + pin + ", left=" + left + ", notAfter=" + notAfter + "]";
    }
}
