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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

/**
 * Test-side request/reply over raw UDP. A datagram may be dropped, late, or
 * never arrive, so a single send and a single timed receive is a flaky test on
 * a loaded machine; ask again instead, as a real client would.
 */
final class Datagrams {

    private Datagrams() {
    }

    /**
     * Sends {@code request} and waits up to a second for a reply, resending up
     * to {@code attempts} times.
     *
     * @return how many times the request was sent (at least 1)
     * @throws SocketTimeoutException if no reply arrived after every attempt
     */
    static int ask(DatagramSocket socket, byte[] request, InetSocketAddress target, DatagramPacket reply,
                   int attempts) throws IOException {
        socket.setSoTimeout(1_000);
        for (int sent = 1; ; sent++) {
            socket.send(new DatagramPacket(request, request.length, target));
            try {
                socket.receive(reply);
                return sent;
            } catch (SocketTimeoutException lost) {
                if (sent >= attempts) {
                    throw lost;
                }
            }
        }
    }
}
