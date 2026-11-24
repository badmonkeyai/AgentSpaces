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
package ai.badmonkey.agentspaces.api.spi;

/**
 * A capability provider that runs as its own participant on the node's loop (srad/agentspaces-event-loop.md EL-T-05):
 * it keeps its own deadlines and reports the next one, instead of being stepped every 250 ms.
 *
 * <p>The capability runtime registers one participant per provider, binds a {@link DueWork.Driver} to it with
 * {@link #attach}, and then calls {@link #run(long)} when the provider is due, when it is woken and on the loop's
 * backstop. A run that is not due does nothing (DW-1). The provider:
 * <ul>
 *   <li>calls {@link DueWork.Driver#wake()} whenever its next due moves earlier, for instance when a frame arrives that
 *       starts work (DW-2);</li>
 *   <li>calls {@link DueWork.Driver#describeChanged()} when its {@link #describe} result changed, so the runtime
 *       republishes the card in that run and not otherwise (EL-T-05).</li>
 * </ul>
 *
 * <p>While the driver is not {@linkplain DueWork.Driver#timed() timed} (a host drives the node by hand, or a direct
 * run), the runtime calls the legacy {@link #tick()} instead, one step per call (owner answer 4, 2026-11-22).
 *
 * <p>Stability: SPI, new in 0.7.0.
 */
public interface DueCapabilityProvider extends CapabilityProvider, DueWork {

    /**
     * Binds the driver of this provider's participant; called once, before the first run.
     *
     * @param driver the driver
     */
    void attach(DueWork.Driver driver);
}
