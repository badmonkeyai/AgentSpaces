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
package ai.badmonkey.agentspaces.peering.transport;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The stream bindings' byte progress (SPEC v0.1.37 §9.4): bytes of a frame still arriving are told to the node, which
 * counts them as the far end's answer. A stream that hands over a frame a few bytes at a time, as a slow link does,
 * tells it once for the prefix and once for each piece that leaves the frame short; a frame already whole tells
 * nothing.
 */
class FrameProgressTest {

    /** A stream that returns at most {@code piece} bytes a read and reports nothing available, as a trickle does. */
    private static InputStream trickle(byte[] bytes, int piece) {
        return new FilterInputStream(new ByteArrayInputStream(bytes)) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return super.read(b, off, Math.min(len, piece));
            }

            @Override
            public int available() {
                return 0;
            }
        };
    }

    @Test
    void aFrameTricklingInFourPiecesIsToldAtThePrefixAndAtEachShortPiece() throws IOException {
        AtomicInteger told = new AtomicInteger();
        byte[] frame = new byte[1_000];
        TcpTransport.readFrame(new DataInputStream(trickle(new byte[1_000], 250)), frame, told::incrementAndGet);
        assertThat(told.get()).as("the prefix, then three pieces that leave it short").isEqualTo(4);
    }

    @Test
    void aFrameAlreadyWholeTellsNothing() throws IOException {
        AtomicInteger told = new AtomicInteger();
        byte[] frame = new byte[1_000];
        TcpTransport.readFrame(new DataInputStream(new ByteArrayInputStream(new byte[1_000])), frame,
                told::incrementAndGet);
        assertThat(told.get()).isZero();
    }

    @Test
    void aStreamThatEndsShortOfTheFrameFails() {
        assertThatThrownBy(() -> TcpTransport.readFrame(new DataInputStream(trickle(new byte[10], 4)), new byte[20],
                () -> { })).isInstanceOf(EOFException.class);
    }
}
