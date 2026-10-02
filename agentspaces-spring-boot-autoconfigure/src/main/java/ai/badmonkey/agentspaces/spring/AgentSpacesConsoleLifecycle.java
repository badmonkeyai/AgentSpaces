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

import ai.badmonkey.agentspaces.console.FleetConsoleServer;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Binds the console server to the application's lifecycle: serving begins on
 * context start, after the fabric itself is up (a later phase than
 * {@link AgentSpacesLifecycle}), and ends first on the way down, so the
 * console never outlives the state it reports.
 */
public class AgentSpacesConsoleLifecycle implements SmartLifecycle {

    private final FleetConsoleServer server;
    private final int port;
    private volatile boolean running;

    /**
     * Creates the lifecycle.
     *
     * @param server the console server to start and stop
     * @param port   the port to serve on; 0 picks a free port
     */
    public AgentSpacesConsoleLifecycle(FleetConsoleServer server, int port) {
        this.server = Objects.requireNonNull(server, "server");
        this.port = port;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        try {
            server.start(port);
        } catch (IOException e) {
            throw new UncheckedIOException("console port " + port + " unavailable", e);
        }
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        server.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** The bound port, for tests and logs. */
    public int boundPort() {
        return server.port();
    }

    @Override
    public int getPhase() {
        // After the fabric's phase: the console starts once there is a fleet
        // to show and stops before the fleet goes away.
        return Integer.MAX_VALUE - 512;
    }
}
