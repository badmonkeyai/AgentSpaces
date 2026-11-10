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

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.spi.BulkPayloads;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The persistent trees as a space's bulk-payload seam before the group's facade exists (PDT 2 phase 7, P2-X-04):
 * the shared wiring builds the spaces under {@code agentspaces.pdt.spaces} before a host such as the Micronaut
 * factory builds and adopts the facade, so this seam forwards to the facade's {@link PdtPayloads} once
 * {@link Pdts#adopt} has it. Until then nothing is lost and nothing waits for good (ROBUSTNESS.md §10): a writer
 * finds every member unable, so its space falls back to the flat swarm or refuses by name under {@code require};
 * a reader's fetch reports a failure, so its space tries again with its back-off; a reference keeps its routing,
 * since {@link #accepts} needs no facade.
 */
final class DeferredPayloads implements BulkPayloads {

    private final Supplier<Optional<PdtPayloads>> seam;

    DeferredPayloads(Supplier<Optional<PdtPayloads>> seam) {
        this.seam = Objects.requireNonNull(seam, "seam");
    }

    @Override
    public String publish(byte[] payload, Lease lease) {
        return seam.get().orElseThrow(() -> new IllegalStateException("the group's persistent trees are not attached yet;"
                + " the host attaches its Pdt facade at startup")).publish(payload, lease);
    }

    @Override
    public Optional<byte[]> local(String ref) {
        return seam.get().flatMap(payloads -> payloads.local(ref));
    }

    @Override
    public void ensure(String ref, PeerId issuer, Runnable onArrival) {
        ensure(ref, issuer, onArrival, () -> { });
    }

    @Override
    public void ensure(String ref, PeerId issuer, Runnable onArrival, Runnable onFailure) {
        Optional<PdtPayloads> payloads = seam.get();
        if (payloads.isPresent()) {
            payloads.get().ensure(ref, issuer, onArrival, onFailure);
        } else {
            onFailure.run();
        }
    }

    @Override
    public void release(String ref) {
        seam.get().ifPresent(payloads -> payloads.release(ref));
    }

    @Override
    public boolean accepts(String ref) {
        return PdtPayloads.isPayloadRoot(ref);
    }

    @Override
    public List<PeerId> unable(Collection<PeerId> members) {
        return seam.get().map(payloads -> payloads.unable(members)).orElseGet(() -> List.copyOf(members));
    }
}
