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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.a2a.A2aGateway;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Starts the A2A gateway after the fabric and stops it before (TODO item 7):
 * the same phase discipline as the console lifecycle.
 */
public class AgentSpacesA2aLifecycle implements SmartLifecycle {

    private final A2aGateway gateway;
    private final String bind;
    private final int port;
    private volatile boolean running;

    /**
     * Creates the lifecycle.
     *
     * @param gateway the gateway to start and stop
     * @param bind    the address to listen on
     * @param port    the port; 0 picks a free port
     */
    public AgentSpacesA2aLifecycle(A2aGateway gateway, String bind, int port) {
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        this.bind = bind == null || bind.isBlank() ? "127.0.0.1" : bind.trim();
        this.port = port;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        try {
            gateway.start(new InetSocketAddress(bind, port));
        } catch (IOException e) {
            throw new UncheckedIOException("A2A gateway " + bind + ":" + port + " unavailable", e);
        }
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        gateway.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** The bound port, for tests and logs. */
    public int boundPort() {
        return gateway.port();
    }

    @Override
    public int getPhase() {
        // After the fabric lifecycle, like the console (stopped first on the way down).
        return Integer.MAX_VALUE - 512;
    }
}
