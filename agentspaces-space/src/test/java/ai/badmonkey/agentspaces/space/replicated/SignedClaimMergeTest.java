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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ordered-log completion regression: every member applies a committed
 * claim under its own key (a placeholder), and the holder's own proof arrives
 * later with the completion. The merge must let the holder's proof displace the
 * placeholder, or no member but the holder can ever authenticate the completion
 * and the entry reappears for the next clerk when the lease lapses.
 */
class SignedClaimMergeTest {

    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerIdentity holder = PeerIdentity.generate();
    private final PeerIdentity bystander = PeerIdentity.generate();
    private final EntryId entryId = EntryId.of("11111111-2222-3333-4444-555555555555");
    private final SpaceId spaceId = SpaceId.local("log/tasks");

    private SpaceWire.SignedClaim signedBy(PeerIdentity signer, TakeClaim claim) {
        return new SpaceWire.SignedClaim(claim, signer.rawPublicKey(),
                signer.sign(codec.toBytes(claim)));
    }

    @Test
    void theHoldersProofDisplacesABystandersPlaceholderForTheSameClaim() {
        TakeClaim claim = new TakeClaim(entryId, spaceId, 1,
                new HlcTimestamp(4, 0, "log"), holder.agent("clerk"), 0.0, 1_000_000L);
        SpaceWire.SignedClaim placeholder = signedBy(bystander, claim);
        SpaceWire.SignedClaim proof = signedBy(holder, claim);

        assertThat(placeholder.holderAttested()).isFalse();
        assertThat(proof.holderAttested()).isTrue();
        assertThat(SpaceWire.SignedClaim.merge(placeholder, proof)).isSameAs(proof);
        assertThat(SpaceWire.SignedClaim.merge(proof, placeholder)).isSameAs(proof);
        // Two equal proofs: the existing one stays, so merging is idempotent.
        assertThat(SpaceWire.SignedClaim.merge(proof, signedBy(holder, claim))).isSameAs(proof);
    }

    @Test
    void aWinningClaimStillBeatsALosingOneRegardlessOfWhoSignedIt() {
        TakeClaim older = new TakeClaim(entryId, spaceId, 1,
                new HlcTimestamp(4, 0, "log"), holder.agent("clerk"), 0.0, 1_000_000L);
        TakeClaim newer = new TakeClaim(entryId, spaceId, 2,
                new HlcTimestamp(9, 0, "log"), bystander.agent("clerk"), 0.0, 2_000_000L);
        SpaceWire.SignedClaim olderProof = signedBy(holder, older);
        SpaceWire.SignedClaim newerPlaceholder = signedBy(holder, newer);

        assertThat(SpaceWire.SignedClaim.merge(olderProof, newerPlaceholder))
                .isSameAs(newerPlaceholder);
        assertThat(SpaceWire.SignedClaim.merge(newerPlaceholder, olderProof))
                .isSameAs(newerPlaceholder);
        assertThat(SpaceWire.SignedClaim.merge(null, olderProof)).isSameAs(olderProof);
        assertThat(SpaceWire.SignedClaim.merge(olderProof, null)).isSameAs(olderProof);
    }
}
