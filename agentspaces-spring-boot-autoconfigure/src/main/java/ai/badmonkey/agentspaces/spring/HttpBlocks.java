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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.transport.http.HttpExchangeTransport;
import ai.badmonkey.agentspaces.transport.http.HttpOptions;
import ai.badmonkey.agentspaces.transport.http.OriginPolicy;
import ai.badmonkey.agentspaces.transport.http.TlsHttpExchangeTransport;
import ai.badmonkey.agentspaces.transport.http.TlsWebSocketTransport;
import ai.badmonkey.agentspaces.transport.http.WebSocketTransport;

import java.security.GeneralSecurityException;
import java.util.function.Supplier;

/**
 * Builds the HTTP bindings from their blocks (SRAD agentspaces-js JS-H-10,
 * JS-N-04); loaded only when a block is enabled and
 * {@code agentspaces-transport-http} is present.
 */
final class HttpBlocks {

    private HttpBlocks() {
    }

    static HttpOptions options(AgentSpacesProperties.Transport.Http block) {
        HttpOptions options = HttpOptions.defaults()
                .withOrigins(OriginPolicy.of(block.getAllowedOrigins()))
                .withIdleTimeout(block.getIdleTimeout())
                .withPingInterval(block.getPingInterval())
                .withLongPoll(block.getLongPoll())
                .withOpensPerSourcePerSecond(block.getOpensPerSourcePerSecond());
        String path = block.getPath() == null || block.getPath().isBlank() ? "/aspace" : block.getPath().trim();
        return options.withPath(path).withExchangePath(path);
    }

    static Transport build(String scheme, PeerIdentity identity, AgentSpacesProperties.Transport.Http block,
                           ServingCredential serving, Supplier<ChannelTrust> trust) {
        HttpOptions options = options(block);
        try {
            return switch (scheme) {
                case "ws" -> new WebSocketTransport().options(options);
                case "http" -> new HttpExchangeTransport().options(options);
                case "wss" -> (trust == null ? new TlsWebSocketTransport(identity)
                        : TlsWebSocketTransport.withRefreshableTrust(identity, serving, trust)).options(options);
                default -> (trust == null ? new TlsHttpExchangeTransport(identity)
                        : new TlsHttpExchangeTransport(identity, serving, trust.get())).options(options);
            };
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("cannot create the " + scheme + " channel credential", e);
        }
    }
}
