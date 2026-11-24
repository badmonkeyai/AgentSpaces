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
package ai.badmonkey.agentspaces.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A free port for a test that must name its port before it binds (a seed address, a fleet's endpoints).
 *
 * <p>The usual way, binding port 0 and closing the socket, hands out a port from the host's ephemeral range, which
 * every outbound connection and every other bind to port 0 on the machine draws from too: with several test JVMs at
 * once (surefire's forks, {@code -T}) another process takes it between the close and the test's own bind often
 * enough to fail a build ({@code BindException}, found 2026-11-22). The ports here come from below the ephemeral
 * range (20000 to 32767; that range starts at 49152 on macOS and 32768 on Linux), where nothing binds by accident;
 * each JVM walks it from its own random start and never hands a port out twice; and a port is handed out only when
 * TCP and UDP both bind it on loopback and on the wildcard address.
 */
public final class Ports {

    /** The lowest port handed out. */
    public static final int LOW = 20_000;
    /** One past the highest port handed out: below every common ephemeral range. */
    public static final int HIGH = 32_768;

    private static final Set<Integer> HANDED = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger CURSOR = new AtomicInteger(
            new Random(ProcessHandle.current().pid() * 31 + System.nanoTime()).nextInt(HIGH - LOW));

    private Ports() {
    }

    /**
     * @return a port that TCP and UDP both bound a moment ago on loopback and on the wildcard address, outside the
     *         ephemeral range, never handed out before in this JVM
     * @throws UncheckedIOException when the whole range is taken
     */
    public static int free() {
        for (int tries = 0; tries < HIGH - LOW; tries++) {
            int port = LOW + Math.floorMod(CURSOR.getAndIncrement(), HIGH - LOW);
            if (HANDED.contains(port) || !bindable(port)) {
                continue;
            }
            if (HANDED.add(port)) {
                return port;
            }
        }
        throw new UncheckedIOException(new IOException("no free port in " + LOW + " to " + (HIGH - 1)));
    }

    private static boolean bindable(int port) {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket tcpAny = new ServerSocket(); ServerSocket tcpLoop = new ServerSocket();
             DatagramSocket udpAny = new DatagramSocket(null); DatagramSocket udpLoop = new DatagramSocket(null)) {
            tcpAny.setReuseAddress(false);
            tcpAny.bind(new InetSocketAddress(port));
            tcpAny.close();
            tcpLoop.setReuseAddress(false);
            tcpLoop.bind(new InetSocketAddress(loopback, port));
            udpAny.setReuseAddress(false);
            udpAny.bind(new InetSocketAddress(port));
            udpAny.close();
            udpLoop.setReuseAddress(false);
            udpLoop.bind(new InetSocketAddress(loopback, port));
            return true;
        } catch (IOException taken) {
            return false;
        }
    }
}
