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
package ai.badmonkey.agentspaces.peering.bootstrap;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Expands an endpoint whose host is a name into one endpoint per address the
 * name resolves to, in resolver order, so a seed name with three records is
 * dialed at all three rather than at whichever the transport's own resolver
 * picks. Literal addresses pass through; a name that does not resolve is
 * kept as it was, so the transport still gets its own chance at dial time.
 */
public final class DnsExpansion {

    /** The platform resolver: every address of the name. */
    public static final Function<String, List<InetAddress>> PLATFORM = host -> {
        try {
            return List.of(InetAddress.getAllByName(host));
        } catch (UnknownHostException e) {
            return List.of();
        }
    };

    private DnsExpansion() {
    }

    /**
     * Expands names to addresses.
     *
     * @param endpoints the endpoints, in dial order
     * @param resolver  the resolver (the platform's, or a test's)
     * @return the expanded endpoints, priorities renumbered by position, deduplicated
     */
    public static List<PeerAdvertisement.Endpoint> expand(List<PeerAdvertisement.Endpoint> endpoints,
                                                          Function<String, List<InetAddress>> resolver) {
        Objects.requireNonNull(resolver, "resolver");
        Set<String> seen = new LinkedHashSet<>();
        List<PeerAdvertisement.Endpoint> out = new ArrayList<>();
        for (PeerAdvertisement.Endpoint endpoint : endpoints) {
            for (String address : addressesOf(endpoint.address(), resolver)) {
                if (seen.add(endpoint.transport() + "://" + address)) {
                    out.add(new PeerAdvertisement.Endpoint(endpoint.transport(), address, out.size()));
                }
            }
        }
        return out;
    }

    /**
     * Splits {@code host:port}, {@code [v6]:port}, or a bare host.
     *
     * @param address the address
     * @return {@code {host, port-or-empty}}
     */
    public static String[] split(String address) {
        String a = address.trim();
        if (a.startsWith("[")) {
            int close = a.indexOf(']');
            if (close > 0) {
                String host = a.substring(1, close);
                String rest = a.substring(close + 1);
                return new String[]{host, rest.startsWith(":") ? rest.substring(1) : ""};
            }
        }
        int colon = a.lastIndexOf(':');
        if (colon < 0 || a.indexOf(':') != colon) {
            return new String[]{a, ""}; // no port, or a bare IPv6 literal
        }
        return new String[]{a.substring(0, colon), a.substring(colon + 1)};
    }

    private static List<String> addressesOf(String address, Function<String, List<InetAddress>> resolver) {
        String[] parts = split(address);
        String host = parts[0];
        String port = parts[1];
        if (host.isEmpty() || isLiteral(host)) {
            return List.of(address);
        }
        List<InetAddress> resolved;
        try {
            resolved = resolver.apply(host);
        } catch (RuntimeException e) {
            resolved = List.of();
        }
        if (resolved == null || resolved.isEmpty()) {
            return List.of(address);
        }
        List<String> out = new ArrayList<>();
        for (InetAddress inet : resolved) {
            String literal = inet.getHostAddress();
            if (literal.indexOf(':') >= 0) {
                int zone = literal.indexOf('%');
                if (zone > 0) {
                    literal = literal.substring(0, zone);
                }
                literal = "[" + literal + "]";
            }
            out.add(port.isEmpty() ? literal : literal + ":" + port);
        }
        return out;
    }

    /** Whether the host is an IPv4 or IPv6 literal rather than a name. */
    static boolean isLiteral(String host) {
        if (host.indexOf(':') >= 0) {
            return true; // IPv6
        }
        int dots = 0;
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if (c == '.') {
                dots++;
            } else if (c < '0' || c > '9') {
                return false;
            }
        }
        return dots == 3;
    }
}
