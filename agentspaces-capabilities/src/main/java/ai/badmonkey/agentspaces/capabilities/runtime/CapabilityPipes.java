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
package ai.badmonkey.agentspaces.capabilities.runtime;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Direct capability frames (spec §9, {@code PIPE_DATA} in v0.1 datagram form):
 * multiplexes one group's PIPE_DATA kind across capabilities by name, so several
 * capability protocols share the wire without stepping on each other. Streamed
 * pipes ({@code PIPE_OPEN}/{@code PIPE_CLOSE}) arrive with the QUIC binding.
 */
public final class CapabilityPipes implements PipeChannel {

    private record PipeFrame(String capability, byte[] payload) {
    }

    private final GroupRuntime runtime;
    private final CborCodec codec;
    private final Map<String, BiConsumer<PeerId, byte[]>> handlers = new ConcurrentHashMap<>();

    /**
     * Attaches the multiplexer to a group runtime. At most one instance per
     * runtime should exist; capabilities share it.
     *
     * @param runtime the group runtime
     * @param codec   the CBOR codec
     */
    public CapabilityPipes(GroupRuntime runtime, CborCodec codec) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.codec = Objects.requireNonNull(codec, "codec");
        runtime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
            PipeFrame frame;
            try {
                frame = codec.fromBytes(body, PipeFrame.class);
            } catch (RuntimeException e) {
                return;
            }
            if (frame == null || frame.capability() == null) {
                return;
            }
            BiConsumer<PeerId, byte[]> handler = handlers.get(frame.capability());
            if (handler != null) {
                handler.accept(from, frame.payload());
            }
        });
    }

    /**
     * The group these pipes carry, for capabilities that bind their messages to it.
     *
     * @return the group id
     */
    public ai.badmonkey.agentspaces.common.id.GroupId group() {
        return runtime.id();
    }

    /**
     * The group's founder, the default content-key rotation authority (SPEC §11a.3).
     *
     * @return the founder's PeerID
     */
    public PeerId founder() {
        return runtime.advertisement().issuer();
    }

    /**
     * The group's revocations (SPEC §6.1, v0.1.13), for capabilities that serve
     * or count agents.
     *
     * @return the revocation view
     */
    public ai.badmonkey.agentspaces.api.security.RevocationView revocations() {
        return runtime.revocationView();
    }

    /**
     * Registers the handler for one capability's frames.
     *
     * @param capabilityType the capability type URI
     * @param handler        receives (sender, payload)
     */
    @Override
    public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
        handlers.put(Objects.requireNonNull(capabilityType), Objects.requireNonNull(handler));
    }

    /**
     * Sends one capability frame to a peer.
     *
     * @param to             the destination peer
     * @param capabilityType the capability type URI
     * @param payload        the capability-specific payload
     */
    @Override
    public void send(PeerId to, String capabilityType, byte[] payload) {
        runtime.send(to, Envelope.Kind.PIPE_DATA,
                new PipeFrame(capabilityType, payload));
    }
}
