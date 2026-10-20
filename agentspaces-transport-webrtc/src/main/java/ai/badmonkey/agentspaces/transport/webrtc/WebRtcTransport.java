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

package ai.badmonkey.agentspaces.transport.webrtc;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The {@code webrtc} binding of the Transport SPI (SPEC §5.5; SRAD srad/agentspaces-js.md §3.4,
 * JS-W-01): one RTCPeerConnection per peer connection with the {@code aspace/1} data channel,
 * signalling over {@link RtcSignalCapability}, attestation by the signed DTLS fingerprint. The
 * endpoint it advertises is {@code Endpoint("webrtc", "sig", p)}: the address {@code sig} tells a
 * dialer to reach the peer by signalling over the fabric, so a dial needs the member's PeerID
 * ({@link #dial(String, PeerId)}) and a listener binds to {@code sig} alone. Capabilities
 * {@code ENCRYPTED, ATTESTING, RELIABLE, ORDERED}: a node that requires attestation may register
 * it. The engine sits behind {@link DataChannelEngine}; the default is libdatachannel.
 */
public final class WebRtcTransport implements Transport, AutoCloseable {

    public static final String SCHEME = "webrtc";
    /** The one address of the binding: "reach me by signalling". */
    public static final String SIG = "sig";

    private final PeerId self;
    private final WebRtcOptions options;
    private final DataChannelEngine engine;
    private final WebRtcCounters counters = new WebRtcCounters();
    private final RtcSignalCapability signalling;
    private volatile Consumer<TransportConnection> acceptor;

    /** The binding with the defaults and the libdatachannel engine. */
    public WebRtcTransport(PeerId self) {
        this(self, WebRtcOptions.defaults());
    }

    public WebRtcTransport(PeerId self, WebRtcOptions options) {
        this(self, options, LibDataChannelEngine.instance());
    }

    WebRtcTransport(PeerId self, WebRtcOptions options, DataChannelEngine engine) {
        this.self = Objects.requireNonNull(self, "self");
        this.options = Objects.requireNonNull(options, "options");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.signalling = new RtcSignalCapability(this);
    }

    /**
     * Why the default engine cannot run on this platform, naming the artifact and what to add,
     * or empty when it can: a host checks this at startup so registration fails with the reason.
     */
    public static Optional<String> engineUnavailable() {
        return LibDataChannelEngine.instance().unavailable();
    }

    /** The signalling capability to attach to the group's pipes (JS-N-02). */
    public RtcSignalCapability signalling() {
        return signalling;
    }

    public WebRtcCounters counters() {
        return counters;
    }

    public WebRtcOptions options() {
        return options;
    }

    /** The engine's name, for the console. */
    public String engineName() {
        return engine.name();
    }

    PeerId self() {
        return self;
    }

    DataChannelEngine engine() {
        return engine;
    }

    Consumer<TransportConnection> acceptor() {
        return acceptor;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.ENCRYPTED, Capability.ATTESTING, Capability.RELIABLE, Capability.ORDERED);
    }

    /**
     * A dial here is signalling over the fabric that may take the whole dial timeout, so the
     * node sends through a relay and dials from its tick once the relayed traffic with the
     * member warrants (JS-W-11), never from inside a send.
     */
    @Override
    public boolean signalled() {
        return true;
    }

    /**
     * A {@code sig} address names no member, so this overload serves only the form
     * {@code sig/<PeerID>}; the node dials members through {@link #dial(String, PeerId)}.
     */
    @Override
    public TransportConnection dial(String address) throws IOException {
        Objects.requireNonNull(address, "address");
        if (address.startsWith(SIG + "/") && address.length() > SIG.length() + 1) {
            return dial(SIG, PeerId.of(address.substring(SIG.length() + 1)));
        }
        throw new IOException("a webrtc dial needs the target member: the address '" + address
                + "' names none (use sig/<PeerID>, or let the node dial the member's endpoint)");
    }

    /**
     * Dials a member by signalling: the offer, the answer, ICE, DTLS, and the channel open all
     * within the dial timeout, after which the remote receives {@code END(2)} and the dial fails
     * so the node falls through to the next endpoint or the relay path (JS-W-09).
     */
    @Override
    public TransportConnection dial(String address, PeerId target) throws IOException {
        Objects.requireNonNull(target, "target");
        if (!SIG.equals(address) && !(SIG + "/" + target.value()).equals(address)) {
            throw new IOException("not a webrtc address: " + address);
        }
        if (target.equals(self)) {
            throw new IOException("a peer does not dial itself over webrtc");
        }
        Optional<String> missing = engine.unavailable();
        if (missing.isPresent()) {
            throw new IOException(missing.get());
        }
        WebRtcSession session = signalling.dial(target);
        try {
            return session.opened().get(options.dialTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            session.expire();
            throw new IOException("webrtc to " + target.display() + ": no channel within " + options.dialTimeout());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof IOException io ? io : new IOException(String.valueOf(cause), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            session.end(RtcSignal.END_NORMAL, "interrupted");
            throw new IOException("interrupted while dialing " + target.display(), e);
        }
    }

    /**
     * Starts accepting offers: the only bind address is {@code sig}, which the node advertises as
     * {@code Endpoint("webrtc", "sig", p)} (JS-W-08). Fails when the engine cannot run here,
     * naming the artifact, so a host's registration fails at startup.
     */
    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        if (!SIG.equals(bindAddress)) {
            throw new IOException("the webrtc binding listens on '" + SIG + "' alone, not '" + bindAddress + "'");
        }
        Optional<String> missing = engine.unavailable();
        if (missing.isPresent()) {
            throw new IOException(missing.get());
        }
        if (acceptor != null) {
            throw new IOException("the webrtc binding already accepts");
        }
        acceptor = onAccept;
        return () -> {
            if (acceptor == onAccept) {
                acceptor = null;
            }
        };
    }

    @Override
    public void close() {
        acceptor = null;
        signalling.close();
    }

    @Override
    public String toString() {
        return "webrtc(" + engine.name() + ", " + self.display() + ")";
    }
}
