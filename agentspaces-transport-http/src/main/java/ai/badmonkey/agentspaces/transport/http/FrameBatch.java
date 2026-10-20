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
package ai.badmonkey.agentspaces.transport.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The exchange's body format (SRAD agentspaces-js appendix C): zero or more
 * frames, each preceded by its 4-byte big-endian length (1 to 8 MiB), at most
 * 64 frames and 9 MiB per body, media type
 * {@code application/vnd.agentspaces.frames}.
 */
final class FrameBatch {

    static final String MEDIA_TYPE = "application/vnd.agentspaces.frames";

    private FrameBatch() {
    }

    /**
     * Encodes frames as one body: a 4-byte big-endian length then the frame, each.
     *
     * @throws IllegalArgumentException for an empty or oversize frame, too many frames, or a
     *                                  body over {@link HttpOptions#MAX_BODY}: the bounds
     *                                  {@link #decode} enforces, judged before any copy
     */
    static byte[] encode(List<byte[]> frames) {
        if (frames.size() > HttpOptions.MAX_BATCH_FRAMES) {
            throw new IllegalArgumentException("more than " + HttpOptions.MAX_BATCH_FRAMES + " frames in one body");
        }
        long total = 0;
        for (byte[] frame : frames) {
            if (frame == null || frame.length == 0 || frame.length > HttpOptions.MAX_FRAME) {
                throw new IllegalArgumentException("frame of " + (frame == null ? "null" : frame.length)
                        + " bytes: a frame is 1 to " + HttpOptions.MAX_FRAME + " bytes");
            }
            total += 4L + frame.length;
        }
        if (total > HttpOptions.MAX_BODY) {
            throw new IllegalArgumentException("body of " + total + " bytes exceeds " + HttpOptions.MAX_BODY);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) total);
        for (byte[] frame : frames) {
            out.writeBytes(ByteBuffer.allocate(4).putInt(frame.length).array());
            out.writeBytes(frame);
        }
        return out.toByteArray();
    }

    /** Decodes a body; a malformed one (bad length, too many frames, too long) throws. */
    static List<byte[]> decode(byte[] body) throws IOException {
        if (body.length > HttpOptions.MAX_BODY) {
            throw new IOException("body exceeds " + HttpOptions.MAX_BODY + " bytes");
        }
        List<byte[]> frames = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(body);
        while (buffer.hasRemaining()) {
            if (buffer.remaining() < 4) {
                throw new IOException("truncated length prefix");
            }
            int length = buffer.getInt();
            if (length <= 0 || length > HttpOptions.MAX_FRAME || length > buffer.remaining()) {
                throw new IOException("bad frame length " + length);
            }
            byte[] frame = new byte[length];
            buffer.get(frame);
            frames.add(frame);
            if (frames.size() > HttpOptions.MAX_BATCH_FRAMES) {
                throw new IOException("more than " + HttpOptions.MAX_BATCH_FRAMES + " frames in one body");
            }
        }
        return frames;
    }
}
