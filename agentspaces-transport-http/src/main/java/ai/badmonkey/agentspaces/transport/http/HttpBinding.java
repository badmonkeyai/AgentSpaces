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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What the four HTTP bindings share: options, counters, the optional TLS
 * setup, and the listeners and connections closed with the binding. Tuning
 * is valid only before the first dial or listen.
 */
abstract class HttpBinding implements Transport, AutoCloseable {

    final TlsSetup tls;
    final HttpCounters counters = new HttpCounters();
    private volatile HttpOptions options = HttpOptions.defaults();
    private volatile boolean used;
    private final Set<AutoCloseable> owned = ConcurrentHashMap.newKeySet();

    HttpBinding(TlsSetup tls) {
        this.tls = tls;
    }

    /** The binding's options. */
    public final HttpOptions options() {
        return options;
    }

    /** The binding's counters (JS-H-12). */
    public final HttpCounters counters() {
        return counters;
    }

    final void setOptions(HttpOptions options) {
        Objects.requireNonNull(options, "options");
        if (used) {
            throw new IllegalStateException("options are set before the first dial or listen");
        }
        this.options = options;
    }

    final HttpOptions use() {
        used = true;
        return options;
    }

    final void own(AutoCloseable closeable) {
        owned.add(closeable);
    }

    final void disown(AutoCloseable closeable) {
        owned.remove(closeable);
    }

    /** Closes every listener and connection this binding opened; idempotent. */
    @Override
    public void close() {
        for (AutoCloseable closeable : owned.toArray(AutoCloseable[]::new)) {
            try {
                closeable.close();
            } catch (Exception e) {
                // best effort
            }
        }
        owned.clear();
    }

    /** Registers a connection so the binding's close ends it, and forgets it when it ends. */
    final <C extends TransportConnection> C track(C connection) {
        AutoCloseable closer = connection::close;
        own(closer);
        connection.onClose(() -> disown(closer));
        return connection;
    }
}
