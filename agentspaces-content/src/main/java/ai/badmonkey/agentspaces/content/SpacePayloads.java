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

    @Override
    public Optional<byte[]> local(String ref) {
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
            } else {
                waiting.remove(ref);
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
