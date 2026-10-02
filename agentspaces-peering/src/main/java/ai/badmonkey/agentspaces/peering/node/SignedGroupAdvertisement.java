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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;

import java.util.Arrays;
import java.util.Objects;

/**
 * A self-certifying group's founding advertisement as it circulates (spec §4.4,
 * §5.1): the {@link GroupAdvertisement}, the founder's raw Ed25519 public key,
 * and the founder's signature over the canonical bytes of the advertisement's
 * founding fields ({@link GroupFounding.FoundingFields}). The advertisement's
 * GroupID is the hash of those fields plus this signature
 * ({@link GroupFounding#derive}), so any receiver holding only the GroupID can
 * check that this document is <em>the</em> founding document of that group and
 * that nobody swapped its policy: {@link GroupFounding#verify}.
 *
 * @param advertisement    the founding advertisement
 * @param founderPublicKey the founder's raw 32-byte Ed25519 public key
 * @param signature        the founder's signature over the founding fields
 */
public record SignedGroupAdvertisement(GroupAdvertisement advertisement,
                                       byte[] founderPublicKey,
                                       byte[] signature) {

    public SignedGroupAdvertisement {
        Objects.requireNonNull(advertisement, "advertisement");
        Objects.requireNonNull(founderPublicKey, "founderPublicKey");
        Objects.requireNonNull(signature, "signature");
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SignedGroupAdvertisement that
                && advertisement.equals(that.advertisement)
                && Arrays.equals(founderPublicKey, that.founderPublicKey)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(advertisement, Arrays.hashCode(founderPublicKey),
                Arrays.hashCode(signature));
    }

    @Override
    public String toString() {
        return "SignedGroupAdvertisement[" + advertisement.group() + ", founder="
                + advertisement.issuer().display() + "]";
    }
}
