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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.api.space.Subscription;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A handle on one ref (srad/agentspaces-pdt.md §3.6, group R): the value behind
 * a name in the ref space. {@link #deref} reads the single head or the
 * deterministic merge of several (PD-R-02), {@link #swap} advances the ref
 * (PD-R-03, PD-R-04), {@link #watch} delivers each new head (PD-R-07),
 * {@link #history} and {@link #at} read earlier versions (PD-R-09), and
 * {@link #onConflict} hears the decisions of §3.6.1 (PD-R-05). Every call goes
 * to the {@link Refs} engine the handle came from; a handle is cheap and two
 * handles on one name share the engine's state.
 *
 * @param <T> the value type: a {@code PMap}, {@code PSet}, {@code PVector}, or {@code PBlob}
 */
public final class Ref<T> {

    private final Refs refs;
    private final Refs.State<T> state;

    Ref(Refs refs, Refs.State<T> state) {
        this.refs = refs;
        this.state = state;
    }

    /** The ref name. */
    public String name() {
        return state.name;
    }

    /** The kind name the heads carry: map, set, vector, blob, or tree. */
    public String kind() {
        return state.kind.name();
    }

    /**
     * The mode: {@code multi} or {@code linear}. Configured on the facade, else
     * the mode of the first head seen, else {@code multi}.
     */
    public String mode() {
        return state.mode();
    }

    /**
     * The value of the single head, or the merge of §3.6.1 when several heads
     * exist, or the empty value when the ref has no version. Never writes a head.
     * A head whose root cannot be loaded within the load timeout is ignored and
     * counted (S7 of §6.4).
     *
     * @return the value
     */
    public T deref() {
        return refs.deref(state).value();
    }

    /**
     * Reads the heads, applies {@code f} to the merged value, writes the new
     * nodes and then a head whose base lists the heads read (PD-R-03), carrying
     * the delta inline when it fits (PD-X-07). In linear mode the head is taken
     * first and the successor completes the take (PD-R-04).
     *
     * @param f the function from the current value to the next
     * @return the value written
     */
    public T swap(UnaryOperator<T> f) {
        return refs.swap(state, Objects.requireNonNull(f, "f"));
    }

    /**
     * Delivers the value of each new head as it arrives and loads (PD-R-07),
     * through {@code Space.notify} on the {@code PRef} template with the name tag.
     *
     * @param listener the listener; runs on the space's delivery thread, so it must return promptly
     * @return the subscription; close it to stop delivery
     */
    public Subscription watch(Consumer<T> listener) {
        return state.addWatcher(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Hears each conflict decision of §3.6.1 this reader makes for the ref.
     *
     * @param listener the listener
     * @return the subscription; close it to stop delivery
     */
    public Subscription onConflict(Consumer<PConflict> listener) {
        return state.addConflictListener(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Up to {@code n} earlier versions, newest first, found by walking base links
     * from the current heads over the versions this peer holds (PD-R-09).
     *
     * @param n the most versions to return
     * @return the versions
     */
    public List<Heads.Head> history(int n) {
        return refs.history(state, n);
    }

    /**
     * Any held version as a value (PD-R-09).
     *
     * @param root the version's root
     * @return the value
     * @throws ai.badmonkey.agentspaces.pdt.NodeUnavailableException when the version is not held
     */
    public T at(String root) {
        return refs.at(state, Objects.requireNonNull(root, "root"));
    }

    /** The heads a reader sees now, without waiting for a fetch. */
    public Heads heads() {
        return refs.heads(state, false);
    }

    /** The load timeout a deref waits for a head's fetch. */
    public Duration loadTimeout() {
        return refs.config().loadTimeout();
    }

    @Override
    public String toString() {
        return "Ref(" + state.name + " " + kind() + " " + mode() + ")";
    }
}
