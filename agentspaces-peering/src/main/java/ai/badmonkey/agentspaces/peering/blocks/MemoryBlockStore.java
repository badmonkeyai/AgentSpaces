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
package ai.badmonkey.agentspaces.peering.blocks;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** The block store of 0.4.0: blocks on the heap, gone with the process. */
public final class MemoryBlockStore implements BlockStore {

    private static final class Held {
        final byte[] bytes;
        volatile long lastAccessMillis;

        Held(byte[] bytes, long now) {
            this.bytes = bytes;
            this.lastAccessMillis = now;
        }
    }

    private final Map<String, Held> blocks = new ConcurrentHashMap<>();
    private final AtomicLong bytesUsed = new AtomicLong();

    @Override
    public Optional<byte[]> get(String cid, long nowMillis) {
        Held held = blocks.get(cid);
        if (held == null) {
            return Optional.empty();
        }
        held.lastAccessMillis = nowMillis;
        return Optional.of(held.bytes.clone());
    }

    @Override
    public boolean put(String cid, byte[] bytes, long nowMillis) {
        Held previous = blocks.putIfAbsent(cid, new Held(bytes.clone(), nowMillis));
        if (previous != null) {
            previous.lastAccessMillis = nowMillis;
            return false;
        }
        bytesUsed.addAndGet(bytes.length);
        return true;
    }

    @Override
    public long remove(String cid) {
        Held removed = blocks.remove(cid);
        if (removed == null) {
            return -1;
        }
        bytesUsed.addAndGet(-removed.bytes.length);
        return removed.bytes.length;
    }

    @Override
    public Set<String> cids() {
        return Set.copyOf(blocks.keySet());
    }

    @Override
    public long lastAccessMillis(String cid) {
        Held held = blocks.get(cid);
        return held == null ? -1 : held.lastAccessMillis;
    }

    @Override
    public int size() {
        return blocks.size();
    }

    @Override
    public long bytesUsed() {
        return bytesUsed.get();
    }

    @Override
    public String kind() {
        return "memory";
    }
}
