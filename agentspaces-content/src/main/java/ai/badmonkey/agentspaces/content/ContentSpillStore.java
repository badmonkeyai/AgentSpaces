/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
import ai.badmonkey.agentspaces.discovery.SpillStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The card spill over a group's content swarm (SPEC §6.1a.5, v0.1.32; ISSUE-AppCardOriginFormats §9.8; AppCard origin
 * and formats, 2026-11-09): a parts document is an artifact in {@link CardSpill#PIECE_SIZE} pieces with manifest
 * {@code name} the card's id, media type {@code application/cbor}, and sealing {@code none}, since the card it completes
 * travels unsealed. A reader fetches it from every holder, each piece verified against the root, so a holder that
 * serves other bytes never reaches the decoder. Install it with {@code DiscoveryService.spillStore}.
 *
 * <p>A reader's fetch is bounded twice (AppCard origin and formats, 2026-11-09). The fetch expects the stub's
 * {@code size} ({@link FetchOptions#expectedSize()}), so an artifact whose manifest states another length is refused
 * before its pieces arrive, and a stub under the reader's fetch bound cannot make it fetch more. A fetch that has not completed by the store's deadline
 * fails and is cancelled, so a root no holder serves never holds the reader or one of the swarm's fetch slots.
 */
public final class ContentSpillStore implements SpillStore {

    /** The media type of a parts document. */
    public static final String MEDIA_TYPE = "application/cbor";

    /** How long a fetch of a parts document may take before it fails, unless the store is built with another deadline. */
    public static final Duration DEFAULT_FETCH_TIMEOUT = Duration.ofSeconds(30);

    private final Content content;
    private final Duration fetchTimeout;

    /**
     * Creates the store over a group's content swarm with the {@link #DEFAULT_FETCH_TIMEOUT}.
     *
     * @param content the swarm of the group whose cards spill
     */
    public ContentSpillStore(Content content) {
        this(content, DEFAULT_FETCH_TIMEOUT);
    }

    /**
     * Creates the store over a group's content swarm.
     *
     * @param content      the swarm of the group whose cards spill
     * @param fetchTimeout how long a fetch of a parts document may take before it fails and is cancelled
     */
    public ContentSpillStore(Content content, Duration fetchTimeout) {
        this.content = Objects.requireNonNull(content, "content");
        Objects.requireNonNull(fetchTimeout, "fetchTimeout");
        if (fetchTimeout.isZero() || fetchTimeout.isNegative()) {
            throw new IllegalArgumentException("the fetch timeout must be positive: " + fetchTimeout);
        }
        this.fetchTimeout = fetchTimeout;
    }

    @Override
    public CompletableFuture<String> put(String name, byte[] document) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(document, "document");
        try {
            Manifest manifest = content.publishOpaque(document, content.publishOptions().withPieceSize(CardSpill.PIECE_SIZE)
                    .withName(name).withMediaType(MEDIA_TYPE));
            return CompletableFuture.completedFuture(manifest.root());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<byte[]> get(String root, long size) {
        Objects.requireNonNull(root, "root");
        if (size < 1 || size > Integer.MAX_VALUE - 8) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("a parts document of " + size + " bytes cannot be held"));
        }
        CompletableFuture<byte[]> out = new CompletableFuture<>();
        Fetch fetch;
        try {
            // the session refuses a manifest of another length before it asks for a piece, however the manifest arrives
            fetch = content.fetch(root, FetchOptions.defaults().withExpectedSize(size));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        out.orTimeout(fetchTimeout.toMillis(), TimeUnit.MILLISECONDS).whenComplete((bytes, failure) -> {
            if (failure != null) {
                fetch.cancel();
            }
        });
        content.onFetchDone(root, () -> {
            if (!fetch.complete()) {
                out.completeExceptionally(fetch.failure().orElseGet(() -> new IOException("the fetch of " + root + " ended incomplete")));
                return;
            }
            try (SeekableByteChannel channel = fetch.open()) {
                if (channel.size() != size) {
                    // a fetch of the same root that another caller began, under another length, shares its session
                    throw lengthRefused(root, channel.size(), size);
                }
                ByteBuffer buffer = ByteBuffer.allocate((int) size);
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                    // drain the verified pieces
                }
                out.complete(buffer.array());
            } catch (IOException | RuntimeException e) {
                out.completeExceptionally(e);
            }
        });
        return out;
    }

    /** The refusal of an artifact whose length is not the stub's {@code size} ({@link SpillStore#get}). */
    private static IllegalArgumentException lengthRefused(String root, long actual, long stated) {
        return new IllegalArgumentException("the artifact " + root + " is " + actual + " bytes; the stub states " + stated);
    }

    @Override
    public void release(String root) {
        content.unpin(root);
    }
}
