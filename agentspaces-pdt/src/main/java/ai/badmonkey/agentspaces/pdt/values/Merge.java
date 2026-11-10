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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The key-wise three-way merge of srad/agentspaces-pdt.md §3.6.1 (PD-V-09). Both
 * sides are diffed against the base with {@link Diff}, so the cost is proportional
 * to the changes. A key changed on one side takes that side. A key changed on both
 * sides to the same outcome (the same value, or removed on both) needs no decision.
 * A key changed on both sides to different outcomes goes to the {@link Resolver}
 * once; the ref layer supplies the HLC rule of §3.6.1 and records the conflict.
 * The merge policy lives here only.
 *
 * <p>PDT 2 phase 6 (P2-T-03; srad/agentspaces-pdt2.md §3.9): the built-in merge of kind
 * {@code t}. A key changed on both sides to two texts takes their union, which raises no
 * conflict, since a text is a CRDT; when the two disagree on a character the resolver decides.
 */
public final class Merge {

    /**
     * Decides a key changed on both sides to different outcomes.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    @FunctionalInterface
    public interface Resolver<K, V> {

        /**
         * @param key    the key
         * @param base   the base value, empty when the key was absent in the base
         * @param mine   the value on my side, empty when my side removed the key
         * @param theirs the value on the other side, empty when it removed the key
         * @return the value to keep, or empty to remove the key
         */
        Optional<V> resolve(K key, Optional<V> base, Optional<V> mine, Optional<V> theirs);
    }

    /** A raw resolver over references, for the set merge and the typed adapter. */
    @FunctionalInterface
    interface RawResolver {
        /** Returns the CBOR bytes to keep, or null to remove; arguments are null when absent. */
        byte[] resolve(byte[] key, byte[] base, byte[] mine, byte[] theirs);
    }

    private Merge() {
    }

    /** The merge of {@code mine} and {@code theirs} against {@code base}. */
    public static <K, V> PMap<K, V> merge3(PMap<K, V> mine, PMap<K, V> base, PMap<K, V> theirs, Resolver<K, V> resolver) {
        Objects.requireNonNull(base, "base");
        Objects.requireNonNull(theirs, "theirs");
        Objects.requireNonNull(resolver, "resolver");
        Codec<K> keys = mine.keyCodec();
        Codec<V> values = mine.valueCodec();
        RawResolver raw = (key, b, m, t) -> {
            Optional<V> kept = resolver.resolve(keys.decode(key), decode(values, b), decode(values, m), decode(values, t));
            return kept.map(values::encode).orElse(null);
        };
        return new PMap<>(merge3(mine.raw(), base.raw(), theirs.raw(), raw), keys, values);
    }

    private static <V> Optional<V> decode(Codec<V> codec, byte[] bytes) {
        return bytes == null ? Optional.empty() : Optional.of(codec.decode(bytes));
    }

    /** A key as a map key. */
    private record ByteKey(byte[] bytes) {
        @Override
        public boolean equals(Object o) {
            return o instanceof ByteKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    static RawMap merge3(RawMap mine, RawMap base, RawMap theirs, RawResolver resolver) {
        Diff.Raw mineDiff = Diff.raw(mine, base);
        Diff.Raw theirDiff = Diff.raw(theirs, base);
        Map<ByteKey, Diff.RawChange> byKey = new HashMap<>();
        for (Diff.RawChange change : mineDiff.changes()) {
            byKey.put(new ByteKey(change.key()), change);
        }
        Ctx mineCtx = mine.ctx();
        Ctx baseCtx = base.ctx();
        Ctx theirCtx = theirs.ctx();
        RawMap.RawEditor editor = mine.editor();
        for (Diff.RawChange theirChange : theirDiff.changes()) {
            byte[] key = theirChange.key();
            Diff.RawChange myChange = byKey.get(new ByteKey(key));
            if (myChange == null) {
                apply(editor, key, theirChange.after(), theirCtx);
                continue;
            }
            if (sameOutcome(myChange.after(), theirChange.after())) {
                continue;
            }
            byte[] mineValue = resolve(myChange.after(), mineCtx);
            byte[] theirValue = resolve(theirChange.after(), theirCtx);
            byte[] union = mineValue != null && theirValue != null && PText.isText(mineValue) && PText.isText(theirValue)
                    ? PText.union(mineValue, theirValue) : null;
            if (union != null) {
                editor.assoc(key, union);
                continue;
            }
            byte[] kept = resolver.resolve(key, resolve(myChange.before(), baseCtx), mineValue, theirValue);
            if (kept == null) {
                editor.dissoc(key);
            } else {
                editor.assoc(key, kept);
            }
        }
        return editor.done();
    }

    private static void apply(RawMap.RawEditor editor, byte[] key, Object after, Ctx ctx) {
        if (after == null) {
            editor.dissoc(key);
        } else {
            editor.assoc(key, Spill.resolve(after, ctx));
        }
    }

    private static boolean sameOutcome(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        return Spill.equalHashed(a, b);
    }

    private static byte[] resolve(Object ref, Ctx ctx) {
        return ref == null ? null : Spill.resolve(ref, ctx);
    }
}
