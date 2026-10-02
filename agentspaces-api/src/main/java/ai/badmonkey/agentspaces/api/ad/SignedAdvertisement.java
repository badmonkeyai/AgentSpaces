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
package ai.badmonkey.agentspaces.api.ad;

import java.util.Objects;

/**
 * An advertisement together with its issuer's raw Ed25519 public key and the
 * signature over the advertisement's canonical CBOR bytes. Because PeerIDs are
 * self-certifying hashes of the public key (spec §4.1), carrying the key makes the
 * wrapper independently verifiable by any receiver: check that the key hashes to
 * the advertisement's issuer, then check the signature. Signing and verification
 * live in {@code agentspaces-identity}; this record is the shape that circulates.
 *
 * @param advertisement   the signed advertisement
 * @param issuerPublicKey the issuer's raw 32-byte Ed25519 public key
 * @param signature       the issuer's signature over the canonical bytes
 * @param <A>             the advertisement type
 */
public record SignedAdvertisement<A extends Advertisement>(
        A advertisement, byte[] issuerPublicKey, byte[] signature) {

    public SignedAdvertisement {
        Objects.requireNonNull(advertisement, "advertisement");
        Objects.requireNonNull(issuerPublicKey, "issuerPublicKey");
        Objects.requireNonNull(signature, "signature");
        if (issuerPublicKey.length == 0) {
            throw new IllegalArgumentException("issuerPublicKey must be non-empty");
        }
        if (signature.length == 0) {
            throw new IllegalArgumentException("signature must be non-empty");
        }
    }
}
