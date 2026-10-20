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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

/**
 * The signalling fabric between bindings under test: each binding's capability attaches to a
 * pipe here, and a signal sent to a PeerID is delivered to that peer's handler on one ordered
 * thread, as the node would deliver a verified {@code PIPE_DATA}. A tamper function stands in
 * for a forged answer; a record of every signal lets a test assert what crossed.
 */
final class FakeFabric {

    record Sent(PeerId from, PeerId to, RtcSignal signal, int bytes) {
    }

    private final CborCodec codec = CborCodec.defaultCodec();
    private final Map<PeerId, BiConsumer<PeerId, byte[]>> handlers = new ConcurrentHashMap<>();
    private final ExecutorService delivery = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
    final List<Sent> sent = new CopyOnWriteArrayList<>();
    volatile UnaryOperator<RtcSignal> tamper = s -> s;
    volatile boolean dropAll;

    RtcSignalCapability.SignalPipe pipe(PeerId self) {
        return new RtcSignalCapability.SignalPipe() {
            @Override
            public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
                handlers.put(self, handler);
            }

            @Override
            public void send(PeerId to, String capabilityType, byte[] payload) {
                RtcSignal signal = codec.fromBytes(payload, RtcSignal.class);
                sent.add(new Sent(self, to, signal, payload.length));
                if (dropAll) {
                    return;
                }
                RtcSignal delivered = tamper.apply(signal);
                byte[] bytes = delivered == signal ? payload : codec.toBytes(delivered);
                delivery.execute(() -> {
                    BiConsumer<PeerId, byte[]> handler = handlers.get(to);
                    if (handler != null) {
                        handler.accept(self, bytes);
                    }
                });
            }
        };
    }

    /** Hands a raw body to a peer's handler, as a member would send it. */
    void inject(PeerId from, PeerId to, byte[] payload) {
        delivery.execute(() -> {
            BiConsumer<PeerId, byte[]> handler = handlers.get(to);
            if (handler != null) {
                handler.accept(from, payload);
            }
        });
    }

    long count(String kind) {
        return sent.stream().filter(s -> kind.equals(s.signal().kind())).count();
    }

    void close() {
        delivery.shutdownNow();
    }
}
