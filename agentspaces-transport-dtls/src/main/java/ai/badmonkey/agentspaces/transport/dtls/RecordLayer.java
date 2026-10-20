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

import org.bouncycastle.tls.DTLSTransport;

import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * What an association's recovery sublayer runs over: one PDU per record.
 * {@link #dtls} is a BouncyCastle DTLS association; {@link PlainRecordLayer}
 * is the bare {@code udp} binding (SRAD dtls-support §1.2.2).
 */
interface RecordLayer {

    /** The largest PDU one record carries. */
    int sendLimit() throws IOException;

    /** A buffer this large holds any inbound PDU. */
    int receiveLimit() throws IOException;

    void send(byte[] pdu, int off, int len) throws IOException;

    /**
     * Receives one PDU, waiting up to {@code waitMillis}.
     *
     * @param recordFlags told the {@code DTLSRecordFlags} of each accepted record (0 when the layer has none)
     * @return the PDU's length, or -1 on timeout
     */
    int receive(byte[] buf, int off, int len, int waitMillis, IntConsumer recordFlags) throws IOException;

    void close() throws IOException;

    /** A BouncyCastle DTLS association as a record layer. */
    static RecordLayer dtls(DTLSTransport transport) {
        return new RecordLayer() {
            @Override
            public int sendLimit() throws IOException {
                return transport.getSendLimit();
            }

            @Override
            public int receiveLimit() throws IOException {
                return transport.getReceiveLimit();
            }

            @Override
            public void send(byte[] pdu, int off, int len) throws IOException {
                transport.send(pdu, off, len);
            }

            @Override
            public int receive(byte[] buf, int off, int len, int waitMillis, IntConsumer recordFlags)
                    throws IOException {
                return transport.receive(buf, off, len, waitMillis, recordFlags::accept);
            }

            @Override
            public void close() throws IOException {
                transport.close();
            }
        };
    }
}
