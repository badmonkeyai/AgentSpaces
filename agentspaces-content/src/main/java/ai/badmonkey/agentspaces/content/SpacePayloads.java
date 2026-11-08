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
import ai.badmonkey.agentspaces.api.spi.BulkPayloads;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Map;
import java.util.List;

/**
 * The content swarm as a space's bulk-payload seam (group E): a payload above
 * the space's threshold becomes an artifact (opaque bytes, {@code application/cbor},
 * never sealed again because the space sealed it already when it seals), the
 * record carries its root, and a reader's space acquires it through a fetch
 * and decodes once complete. Attach with
 * {@code ReplicatedSpace.builder(...).bulk(new SpacePayloads(content), threshold)}.
 *
 * <p>This seam carries raw (0x55) artifact roots only: {@link #accepts} refuses a
 * dag-cbor (0x71) root, which names a persistent tree of
 * {@code ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads} and has no manifest
 * here (srad/agentspaces-pdt.md PD-E-01; SPEC §7.1 "a bulk implementation MUST
 * refuse a reference of the other codec"), so a space that holds only this seam
 * leaves such a record waiting for anti-entropy instead of opening a flat fetch
 * that can never complete.
 */
public final class SpacePayloads implements BulkPayloads {

    private final Content content;
    private final Map<String, List<Runnable>> waiting = new ConcurrentHashMap<>();

    /**
     * Wraps a content capability of the same group.
     *
     * @param content the capability
     */
    public SpacePayloads(Content content) {
        this.content = Objects.requireNonNull(content, "content");
    }

    @Override
    public String publish(byte[] payload, Lease lease) {
        PublishOptions options = PublishOptions.defaults()
                .withLease(lease)
                .withMediaType("application/cbor");
        return content.publishOpaque(payload, options).root();
    }

    /** Whether a reference is a raw artifact root this seam published or can fetch. */
    @Override
    public boolean accepts(String ref) {
        if (!BulkPayloads.isBulk(ref)) {
            return false;
        }
        try {
            return Cids.parse(ref).codec() == Cids.RAW;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public Optional<byte[]> local(String ref) {
        if (!accepts(ref)) {
            return Optional.empty();
        }
        Optional<Manifest> manifest = content.store().manifest(ref);
        if (manifest.isEmpty() || content.store().have(ref).cardinality() < manifest.get().pieces()) {
            return Optional.empty();
        }
        try (SeekableByteChannel channel = content.openPlain(ref)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(Integer.MAX_VALUE, channel.size()));
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return Optional.of(buffer.array());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public void ensure(String ref, PeerId issuer, Runnable onArrival) {
        ensure(ref, issuer, onArrival, () -> { });
    }

    @Override
    public void ensure(String ref, PeerId issuer, Runnable onArrival, Runnable onFailure) {
        if (!accepts(ref)) {
            throw new IllegalArgumentException("not a raw artifact root: " + ref);
        }
        List<Runnable> callbacks = waiting.computeIfAbsent(ref, r -> new CopyOnWriteArrayList<>());
        callbacks.add(onArrival);
        Fetch fetch = content.fetch(ref, FetchOptions.defaults().prefer(issuer));
        if (fetch.complete()) {
            fire(ref);
            return;
        }
        content.onFetchDone(ref, () -> {
            if (fetch.complete()) {
                fire(ref);
            } else if (waiting.remove(ref) != null) {
                onFailure.run();
            }
        });
    }

    private void fire(String ref) {
        List<Runnable> callbacks = waiting.remove(ref);
        if (callbacks != null) {
            callbacks.forEach(Runnable::run);
        }
    }

    @Override
    public void release(String ref) {
        content.unpin(ref);
    }
}
