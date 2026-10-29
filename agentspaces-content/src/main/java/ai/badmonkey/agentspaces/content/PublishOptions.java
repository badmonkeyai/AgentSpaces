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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.space.Lease;

import java.time.Duration;
import java.util.Map;

/**
 * Options of {@link Content#publish}.
 *
 * @param lease     the manifest lease; the artifact stays pinned here while it lives
 * @param pieceSize the piece size (a power of two from 64 KiB to 4 MiB)
 * @param name      display name, or null
 * @param mediaType RFC 6838 media type, or null
 * @param attrs     user attributes, or null
 * @param inPlace   serve the file where it lies rather than copying it into the store (LB-T-06)
 * @param replicate write the manifest to the content space (true) or list it only through a collection (false, LB-K-04)
 * @param sealedHashing seal each piece once under the group key and hash the sealed bytes ({@code hashing: sealed},
 *                      decision 6, LB-G-03): any member can then verify, store, and serve the pieces, a keyless one
 *                      included (LB-T-05), and the root belongs to this group alone
 */
public record PublishOptions(Lease lease, int pieceSize, String name, String mediaType,
                             Map<String, String> attrs, boolean inPlace, boolean replicate, boolean sealedHashing) {

    /** The form without the hashing choice: plain hashing. */
    public PublishOptions(Lease lease, int pieceSize, String name, String mediaType,
                          Map<String, String> attrs, boolean inPlace, boolean replicate) {
        this(lease, pieceSize, name, mediaType, attrs, inPlace, replicate, false);
    }

    /** Seven days, 1 MiB pieces, no name, copied into the store, replicated. */
    public static PublishOptions defaults() {
        return new PublishOptions(Lease.of(Duration.ofDays(7)), ContentRoots.DEFAULT_PIECE_SIZE,
                null, null, null, false, true);
    }

    public PublishOptions {
        if (pieceSize < Manifest.MIN_PIECE_SIZE || pieceSize > Manifest.MAX_PIECE_SIZE
                || Integer.bitCount(pieceSize) != 1) {
            throw new IllegalArgumentException("pieceSize must be a power of two in [64 KiB, 4 MiB]: " + pieceSize);
        }
    }

    public PublishOptions withLease(Lease value) {
        return new PublishOptions(value, pieceSize, name, mediaType, attrs, inPlace, replicate, sealedHashing);
    }

    /**
     * The piece size for the bindings a fetch will cross (LB-M-04): 256 KiB when any of them may
     * lose frames or is not reliable (a windowed datagram binding, where a lost range should cost
     * little), 1 MiB otherwise.
     *
     * @param capabilities the union of the capabilities of the bindings in play
     * @return the piece size in bytes
     */
    public static int pieceSizeFor(java.util.Set<ai.badmonkey.agentspaces.api.spi.Transport.Capability> capabilities) {
        boolean lossy = capabilities.contains(ai.badmonkey.agentspaces.api.spi.Transport.Capability.DATAGRAM)
                || !capabilities.contains(ai.badmonkey.agentspaces.api.spi.Transport.Capability.RELIABLE);
        return lossy ? 256 * 1024 : ContentRoots.DEFAULT_PIECE_SIZE;
    }

    /** Seals each piece once and hashes the sealed bytes (LB-G-03); needs the group's content key. */
    public PublishOptions withSealedHashing(boolean value) {
        return new PublishOptions(lease, pieceSize, name, mediaType, attrs, inPlace, replicate, value);
    }

    public PublishOptions withPieceSize(int value) {
        return new PublishOptions(lease, value, name, mediaType, attrs, inPlace, replicate, sealedHashing);
    }

    public PublishOptions withName(String value) {
        return new PublishOptions(lease, pieceSize, value, mediaType, attrs, inPlace, replicate, sealedHashing);
    }

    public PublishOptions withMediaType(String value) {
        return new PublishOptions(lease, pieceSize, name, value, attrs, inPlace, replicate, sealedHashing);
    }

    public PublishOptions withAttrs(Map<String, String> value) {
        return new PublishOptions(lease, pieceSize, name, mediaType, value, inPlace, replicate, sealedHashing);
    }

    public PublishOptions withInPlace(boolean value) {
        return new PublishOptions(lease, pieceSize, name, mediaType, attrs, value, replicate, sealedHashing);
    }

    public PublishOptions withReplicate(boolean value) {
        return new PublishOptions(lease, pieceSize, name, mediaType, attrs, inPlace, value, sealedHashing);
    }
}
