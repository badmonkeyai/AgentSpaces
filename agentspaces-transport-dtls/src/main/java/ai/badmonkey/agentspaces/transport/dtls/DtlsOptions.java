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

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The tunables of a {@link DtlsTransport} (SRAD appendix C), immutable, each
 * with a default and a range checked at construction.
 *
 * @param mtu                the datagram budget: the largest UDP payload sent, 576 to 9,000 bytes
 * @param window             DATA PDUs unacknowledged per association, 4 to 1,024
 * @param idleTimeout        silence after which an association fails, 10 s to 10 min; keepalive at a quarter
 * @param maxHalfOpen        concurrent unfinished handshakes per listener, 16 to 65,536
 * @param helloRatePerSource ClientHellos accepted per second from one source address, 1 to 1,000
 * @param connectionId       whether to negotiate RFC 9146 Connection ID
 * @param protocols          the DTLS versions to offer, by name; DTLSv1.2 is the one BouncyCastle negotiates today
 */
public record DtlsOptions(int mtu, int window, Duration idleTimeout, int maxHalfOpen,
                          int helloRatePerSource, boolean connectionId, List<String> protocols) {

    /** The versions this build can negotiate (SRAD §1.2.3): BouncyCastle 1.86 tops out at DTLS 1.2. */
    public static final Set<String> SUPPORTED_PROTOCOLS = Set.of("DTLSv1.2");

    /** Validates the options. */
    public DtlsOptions {
        range("mtu", mtu, 576, 9_000);
        range("window", window, 4, 1_024);
        Objects.requireNonNull(idleTimeout, "idleTimeout");
        if (idleTimeout.compareTo(Duration.ofSeconds(10)) < 0 || idleTimeout.compareTo(Duration.ofMinutes(10)) > 0) {
            throw new IllegalArgumentException("idleTimeout must be 10s to 10m, not " + idleTimeout);
        }
        range("maxHalfOpen", maxHalfOpen, 16, 65_536);
        range("helloRatePerSource", helloRatePerSource, 1, 1_000);
        protocols = List.copyOf(Objects.requireNonNull(protocols, "protocols"));
        if (protocols.isEmpty()) {
            throw new IllegalArgumentException("protocols must name at least one DTLS version");
        }
        for (String protocol : protocols) {
            if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
                throw new IllegalArgumentException("protocol '" + protocol + "' is not negotiable with this"
                        + " BouncyCastle build; supported: " + SUPPORTED_PROTOCOLS);
            }
        }
    }

    /** The defaults: 1,200-byte datagrams, 64 PDUs in flight, 60 s idle, 1,024 half-open, 8 hellos/s, CID on, DTLS 1.2. */
    public static DtlsOptions defaults() {
        return new DtlsOptions(1_200, 64, Duration.ofSeconds(60), 1_024, 8, true, List.of("DTLSv1.2"));
    }

    /** @return a copy with another datagram budget */
    public DtlsOptions withMtu(int value) {
        return new DtlsOptions(value, window, idleTimeout, maxHalfOpen, helloRatePerSource, connectionId, protocols);
    }

    /** @return a copy with another window */
    public DtlsOptions withWindow(int value) {
        return new DtlsOptions(mtu, value, idleTimeout, maxHalfOpen, helloRatePerSource, connectionId, protocols);
    }

    /** @return a copy with another idle timeout */
    public DtlsOptions withIdleTimeout(Duration value) {
        return new DtlsOptions(mtu, window, value, maxHalfOpen, helloRatePerSource, connectionId, protocols);
    }

    /** @return a copy with another half-open bound */
    public DtlsOptions withMaxHalfOpen(int value) {
        return new DtlsOptions(mtu, window, idleTimeout, value, helloRatePerSource, connectionId, protocols);
    }

    /** @return a copy with another per-source hello rate */
    public DtlsOptions withHelloRatePerSource(int value) {
        return new DtlsOptions(mtu, window, idleTimeout, maxHalfOpen, value, connectionId, protocols);
    }

    /** @return a copy with Connection ID on or off */
    public DtlsOptions withConnectionId(boolean value) {
        return new DtlsOptions(mtu, window, idleTimeout, maxHalfOpen, helloRatePerSource, value, protocols);
    }

    /** @return a copy offering other DTLS versions */
    public DtlsOptions withProtocols(List<String> value) {
        return new DtlsOptions(mtu, window, idleTimeout, maxHalfOpen, helloRatePerSource, connectionId, value);
    }

    private static void range(String name, int value, int min, int max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(name + " must be " + min + " to " + max + ", not " + value);
        }
    }
}
