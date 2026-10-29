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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

/** A read-only seekable channel over a byte array, for the memory store. */
final class ByteArrayChannel implements SeekableByteChannel {

    private final byte[] bytes;
    private long position;
    private boolean open = true;

    ByteArrayChannel(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!open) {
            throw new IOException("closed");
        }
        if (position >= bytes.length) {
            return -1;
        }
        int n = (int) Math.min(dst.remaining(), bytes.length - position);
        dst.put(bytes, (int) position, n);
        position += n;
        return n;
    }

    @Override
    public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
        if (newPosition < 0) {
            throw new IllegalArgumentException("negative position");
        }
        position = newPosition;
        return this;
    }

    @Override
    public long size() {
        return bytes.length;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
