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

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * An HTTP binding address: {@code host:port} optionally followed by a path
 * (ISSUE-Websocket FR-2; SRAD agentspaces-js §3.3). {@code host} may be a DNS
 * name, an IPv4 literal, or a bracketed IPv6 literal; a missing path takes the
 * binding's default ({@code /aspace} for WebSocket, {@code /aspace/h} for the
 * exchange).
 *
 * @param host the host, without brackets
 * @param port the port
 * @param path the absolute path, starting with {@code /}
 */
public record HttpAddress(String host, int port, String path) {

    public HttpAddress {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(path, "path");
        if (host.isEmpty()) {
            throw new IllegalArgumentException("empty host");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (!path.startsWith("/") || path.contains("?") || path.contains("#")) {
            throw new IllegalArgumentException("path must be absolute with no query or fragment: " + path);
        }
    }

    /**
     * Parses {@code host:port[/path]}, taking {@code defaultPath} when no path is given.
     *
     * @param address     the address text
     * @param defaultPath the path to use when none is given
     * @return the parsed address
     */
    public static HttpAddress parse(String address, String defaultPath) {
        Objects.requireNonNull(address, "address");
        String text = address.trim();
        int bracketEnd = text.startsWith("[") ? text.indexOf(']') : -1;
        int slash = text.indexOf('/', bracketEnd + 1);
        String path = slash >= 0 ? text.substring(slash) : defaultPath;
        String hostPort = slash >= 0 ? text.substring(0, slash) : text;
        String host;
        String portText;
        if (hostPort.startsWith("[")) {
            if (bracketEnd < 0 || bracketEnd + 1 >= hostPort.length() || hostPort.charAt(bracketEnd + 1) != ':') {
                throw new IllegalArgumentException("expected [v6]:port, got: " + address);
            }
            host = hostPort.substring(1, bracketEnd);
            portText = hostPort.substring(bracketEnd + 2);
        } else {
            int colon = hostPort.lastIndexOf(':');
            if (colon <= 0 || hostPort.indexOf(':') != colon) {
                throw new IllegalArgumentException("expected host:port, got: " + address);
            }
            host = hostPort.substring(0, colon);
            portText = hostPort.substring(colon + 1);
        }
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a port in " + address, e);
        }
        return new HttpAddress(host, port, path.isEmpty() ? defaultPath : path);
    }

    /** The socket address to bind or connect. */
    public InetSocketAddress socketAddress() {
        return new InetSocketAddress(host, port);
    }

    /** {@code host:port} with brackets around an IPv6 host. */
    public String authority() {
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + port;
    }

    /** {@code scheme://host:port/path}. */
    public String url(String scheme) {
        return scheme + "://" + authority() + path;
    }

    @Override
    public String toString() {
        return authority() + path;
    }
}
