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

package ai.badmonkey.agentspaces.transport.dtls;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.IntConsumer;

/**
 * The bare {@code udp} binding's record layer (SRAD dtls-support §1.2.2): one
 * PDU per UDP payload, plaintext, with the association's 8-byte token after
 * the version and type bytes so an off-path source cannot inject PDUs. An
 * on-path attacker can; the binding is gated for that reason.
 */
final class PlainRecordLayer implements RecordLayer {

    static final int TOKEN_LENGTH = 8;

    private final PeerDatagramTransport path;
    private final long token;
    private final int mtu;

    PlainRecordLayer(PeerDatagramTransport path, long token, int mtu) {
        this.path = path;
        this.token = token;
        this.mtu = mtu;
    }

    @Override
    public int sendLimit() {
        return mtu - TOKEN_LENGTH;
    }

    @Override
    public int receiveLimit() {
        return PeerDatagramTransport.RECEIVE_LIMIT;
    }

    @Override
    public void send(byte[] pdu, int off, int len) throws IOException {
        if (len < 2) {
            throw new IOException("PDU too short");
        }
        byte[] datagram = new byte[len + TOKEN_LENGTH];
        datagram[0] = pdu[off];
        datagram[1] = pdu[off + 1];
        ByteBuffer.wrap(datagram, 2, TOKEN_LENGTH).putLong(token);
        System.arraycopy(pdu, off + 2, datagram, 2 + TOKEN_LENGTH, len - 2);
        path.send(datagram, 0, datagram.length);
    }

    @Override
    public int receive(byte[] buf, int off, int len, int waitMillis, IntConsumer recordFlags) throws IOException {
        byte[] datagram = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
        long deadline = System.nanoTime() + waitMillis * 1_000_000L;
        while (true) {
            int remaining = (int) Math.max(0, (deadline - System.nanoTime()) / 1_000_000L);
            int n = path.receive(datagram, 0, datagram.length, remaining);
            if (n < 0) {
                return -1;
            }
            if (n < 2 + TOKEN_LENGTH || ByteBuffer.wrap(datagram, 2, TOKEN_LENGTH).getLong() != token
                    || n - TOKEN_LENGTH > len) {
                if (remaining == 0) {
                    return -1;
                }
                continue; // not this association's: dropped
            }
            buf[off] = datagram[0];
            buf[off + 1] = datagram[1];
            System.arraycopy(datagram, 2 + TOKEN_LENGTH, buf, off + 2, n - 2 - TOKEN_LENGTH);
            return n - TOKEN_LENGTH;
        }
    }

    @Override
    public void close() {
        path.close();
    }
}
