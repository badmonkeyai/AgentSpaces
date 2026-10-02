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

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.security.cert.CRLReason;
import java.time.Instant;
import java.util.Objects;

/**
 * What a {@link ChannelTrust} concludes about a presented chain at an instant
 * (SPEC §5.6, v0.1.13): only {@link Good} attests, and only {@link Revoked}
 * with an authorizing reason can root a peer revocation in the CA.
 */
public sealed interface TrustStatus {

    /** The chain validates to an anchor and is not revoked: the CN's PeerID is vouched for. */
    record Good(PeerId peer) implements TrustStatus {
        public Good {
            Objects.requireNonNull(peer, "peer");
        }
    }

    /**
     * The chain validates to an anchor, and a CRL signed by its issuer lists the leaf.
     *
     * @param peer      the PeerID the revoked leaf's CN binds
     * @param reason    the CRL entry's reason ({@code UNSPECIFIED} when absent)
     * @param revokedAt the CRL entry's revocation date
     */
    record Revoked(PeerId peer, CRLReason reason, Instant revokedAt) implements TrustStatus {
        public Revoked {
            Objects.requireNonNull(peer, "peer");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(revokedAt, "revokedAt");
        }

        /**
         * Whether the reason withdraws the identity itself, so the CA's word
         * may revoke the PeerID fleet-wide: a compromise, a withdrawn privilege,
         * a ceased operation, or no reason given. A superseded certificate, a
         * changed affiliation, or a hold is a statement about the certificate,
         * which attestation already refuses, not about the peer.
         *
         * @return whether this revocation can root a peer revocation
         */
        public boolean authorizesPeerRevocation() {
            return switch (reason) {
                case KEY_COMPROMISE, CA_COMPROMISE, PRIVILEGE_WITHDRAWN, CESSATION_OF_OPERATION,
                     UNSPECIFIED -> true;
                default -> false;
            };
        }
    }

    /** No usable revocation information: no CRL from the issuer, or only stale ones. */
    record Undetermined(String detail) implements TrustStatus {
    }

    /** The chain does not validate to an anchor. */
    record Untrusted(String detail) implements TrustStatus {
    }

    /** A certificate in the chain had expired at the instant asked about. */
    record Expired() implements TrustStatus {
    }

    /** A certificate in the chain was not yet valid at the instant asked about. */
    record NotYetValid() implements TrustStatus {
    }

    /** The chain is empty, unparseable, or its leaf's CN carries no identity key. */
    record Malformed() implements TrustStatus {
    }
}
