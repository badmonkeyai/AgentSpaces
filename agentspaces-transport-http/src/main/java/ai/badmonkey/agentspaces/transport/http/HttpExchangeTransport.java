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

import java.util.Set;

/**
 * The {@code http} binding (SRAD agentspaces-js §3.3, appendix C, JS-H-07):
 * the request and response exchange over plain HTTP/1.1, the analogue of
 * {@code tcp} for development and tests. The session id travels in clear;
 * frames stay signed or session-tagged (SPEC §5.6).
 */
public final class HttpExchangeTransport extends ExchangeBinding {

    /** The scheme. */
    public static final String SCHEME = "http";

    public HttpExchangeTransport() {
        super(null);
    }

    /** Sets the options; valid before the first dial or listen. */
    public HttpExchangeTransport options(HttpOptions options) {
        setOptions(options);
        return this;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.RELIABLE, Capability.ORDERED);
    }
}
