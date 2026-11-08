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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PD-V-05: a transient's 500 changes produce one delta whose nodes are exactly the
 * final path set, in place on owned nodes, refused from another thread and after done.
 */
class EditorTest {

    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    @Test
    void fiveHundredChangesProduceOneDelta() {
        NodeStore store = NodeStore.inMemory();
        Map<Integer, Integer> model = new HashMap<>();
        Editor<Integer, Integer> builder = PMap.empty(store, INTS, INTS).editor();
        for (int i = 0; i < 2000; i++) {
            builder.assoc(i, i);
            model.put(i, i);
        }
        PMap<Integer, Integer> base = builder.done().value();
        Set<String> before = store.reachable(base.root());
        int storeBefore = store.size();

        Random random = new Random(42);
        Editor<Integer, Integer> editor = base.editor();
        for (int i = 0; i < 500; i++) {
            int op = random.nextInt(3);
            if (op == 0) {
                int key = 5000 + i;
                editor.assoc(key, -i);
                model.put(key, -i);
            } else if (op == 1) {
                int key = random.nextInt(2000);
                editor.assoc(key, i * 3);
                model.put(key, i * 3);
            } else {
                int key = random.nextInt(2000);
                editor.dissoc(key);
                model.remove(key);
            }
        }
        assertThat(editor.count()).isEqualTo(model.size());
        Editor.Result<Integer, Integer> result = editor.done();
        PMap<Integer, Integer> value = result.value();
        assertThat(value.toMap()).isEqualTo(model);
        assertThat(base.count()).isEqualTo(2000);

        Set<String> after = store.reachable(value.root());
        Set<String> delta = new HashSet<>(after);
        delta.removeAll(before);
        assertThat(result.nodes().keySet()).isEqualTo(delta);
        assertThat(store.size()).isEqualTo(storeBefore + delta.size());
        assertThat(delta.size()).isLessThan(500);
        assertThat(result.nodes()).containsKey(value.root());
    }

    @Test
    void theValueEqualsThePersistentResult() {
        NodeStore store = NodeStore.inMemory();
        PMap<Integer, Integer> persistent = PMap.empty(store, INTS, INTS);
        Editor<Integer, Integer> editor = PMap.empty(store, INTS, INTS).editor();
        for (int i = 0; i < 3000; i++) {
            persistent = persistent.assoc(i, i);
            editor.assoc(i, i);
        }
        for (int i = 0; i < 3000; i += 7) {
            persistent = persistent.dissoc(i);
            editor.dissoc(i);
        }
        editor.update(1, v -> v + 100);
        persistent = persistent.update(1, v -> v + 100);
        assertThat(editor.get(1)).contains(101);
        assertThat(editor.done().value()).isEqualTo(persistent);
    }

    @Test
    void anotherThreadAndAFinishedEditorAreRefused() throws InterruptedException {
        PMap<Integer, Integer> map = PMap.empty(NodeStore.inMemory(), INTS, INTS);
        Editor<Integer, Integer> editor = map.editor().assoc(1, 1);
        AtomicReference<Throwable> seen = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                editor.assoc(2, 2);
            } catch (Throwable t) {
                seen.set(t);
            }
        }, "other-thread");
        other.start();
        other.join();
        assertThat(seen.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("other-thread");
        assertThat(editor.count()).isEqualTo(1);
        Editor.Result<Integer, Integer> result = editor.done();
        assertThat(result.value().count()).isEqualTo(1);
        assertThatThrownBy(() -> editor.assoc(3, 3)).isInstanceOf(IllegalStateException.class).hasMessageContaining("done");
        assertThatThrownBy(editor::done).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> editor.get(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void vectorEditorProducesOneDeltaAndRefusesOtherThreads() throws InterruptedException {
        NodeStore store = NodeStore.inMemory();
        VectorEditor<Integer> builder = PVector.empty(store, INTS).editor();
        for (int i = 0; i < 2000; i++) {
            builder.conj(i);
        }
        PVector<Integer> base = builder.done().value();
        Set<String> before = store.reachable(base.root());

        VectorEditor<Integer> editor = base.editor();
        for (int i = 0; i < 500; i++) {
            if (i % 5 == 0) {
                editor.assoc(i * 3, -i);
            } else {
                editor.conj(2000 + i);
            }
        }
        editor.pop();
        assertThat(editor.nth(0)).isEqualTo(0);
        VectorEditor.Result<Integer> result = editor.done();
        Set<String> delta = new HashSet<>(store.reachable(result.value().root()));
        delta.removeAll(before);
        assertThat(result.nodes().keySet()).isEqualTo(delta);
        assertThat(result.value().count()).isEqualTo(2399);
        assertThat(result.value().nth(2398)).isEqualTo(2000 + 498);
        assertThat(result.value().nth(15)).isEqualTo(-5);
        assertThat(base.count()).isEqualTo(2000);
        assertThat(base.nth(15)).isEqualTo(15);

        VectorEditor<Integer> done = base.editor();
        AtomicReference<Throwable> seen = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                done.conj(1);
            } catch (Throwable t) {
                seen.set(t);
            }
        });
        other.start();
        other.join();
        assertThat(seen.get()).isInstanceOf(IllegalStateException.class);
        done.done();
        assertThatThrownBy(() -> done.conj(1)).isInstanceOf(IllegalStateException.class);
    }
}
