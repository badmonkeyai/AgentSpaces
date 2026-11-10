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
package ai.badmonkey.agentspaces.content.tree.sync;

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Messages;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The holder's side of a reconciliation (PDT 2 phase 5, P2-K-02, P2-K-04, P2-F-01; srad/agentspaces-pdt2.md §3.6,
 * S4 of §6.4): it answers the sketch of a {@code TREE_WANT} with {@code root ""} with the sketch of a
 * {@code TREE_END} of reason 5, over the set its supplier gives (the roots the holder holds complete, by default).
 * The leading byte of the sketch selects the encoding: {@link Negentropy#PROTOCOL_VERSION} (and every other
 * negentropy version byte in {@code 0x60..0x6f}, which the protocol answers with its own version byte) or
 * {@link Riblt#ENCODING}; any other byte is refused by name.
 *
 * <p>Bounds (P2-Q-SEC-04, threat 4 of §6.7). A sketch past {@link #MAX_SKETCH_BYTES}, or one outside its grammar, is
 * dropped with the reason, and the caller counts it. The {@code budget} of the want bounds the reply, within
 * {@link Negentropy#MIN_FRAME_SIZE_LIMIT} and {@link #MAX_SKETCH_BYTES}. One asker's session, named by its request id,
 * takes at most {@link #MAX_ROUNDS} rounds and is then refused with {@code REJECT} reason 2. Negentropy keeps no state
 * between rounds; RIBLT keeps a decoder per asker and key, at most {@link #RIBLT_SESSIONS} at once, each dropped after
 * {@link #RIBLT_IDLE_MILLIS} idle, and a stream past {@link #MAX_RIBLT_SYMBOLS} ends with {@link Riblt#TOO_MANY}. A
 * stream that peels more ids than it has symbols codes no set and is forged: it is dropped and its session freed.
 * The set is read again at most every {@link #SET_CACHE_MILLIS}. Safe for concurrent use.
 */
public final class SketchServer {

    /** The largest sketch one message carries (P2-K-04). */
    public static final int MAX_SKETCH_BYTES = 64 * 1024;
    /** The rounds one session takes at most (P2-K-04). */
    public static final int MAX_ROUNDS = 64;
    /** The symbols one RIBLT session streams at most. */
    public static final int MAX_RIBLT_SYMBOLS = 16_384;
    /** RIBLT sessions held at once. */
    public static final int RIBLT_SESSIONS = 4;
    /** A RIBLT session idle this long is dropped. */
    public static final long RIBLT_IDLE_MILLIS = 30_000;
    /** How long the set is reused before the supplier is read again. */
    static final long SET_CACHE_MILLIS = 10_000;
    /** Sessions whose rounds are counted at once; the oldest is forgotten. */
    static final int SESSIONS_COUNTED = 1024;

    /** What the holder does with one sketch. */
    public sealed interface Answer permits Reply, Refusal, Dropped {
    }

    /**
     * Answer with this sketch in a {@code TREE_END} of reason 5.
     *
     * @param sketch the reply
     */
    public record Reply(byte[] sketch) implements Answer {
        @Override
        public boolean equals(Object o) {
            return o instanceof Reply r && Arrays.equals(sketch, r.sketch);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(sketch);
        }

        @Override
        public String toString() {
            return "Reply[" + HexFormat.of().formatHex(sketch, 0, Math.min(sketch.length, 16)) + (sketch.length > 16 ? "..." : "") + "]";
        }
    }

    /**
     * Refuse with a {@code REJECT} of this reason.
     *
     * @param reason a {@code REJECT} reason
     * @param why    the reason in words
     */
    public record Refusal(int reason, String why) implements Answer {
    }

    /**
     * Drop and count the want.
     *
     * @param why the broken rule
     */
    public record Dropped(String why) implements Answer {
    }

    private record Cached(long atMillis, SketchSet set) {
    }

    private record SessionKey(PeerId asker, String key) {
    }

    private static final class RibltSession {
        final Riblt.Decoder decoder;
        long usedMillis;

        RibltSession(Riblt.Decoder decoder, long now) {
            this.decoder = decoder;
            this.usedMillis = now;
        }
    }

    private record Round(PeerId asker, int req) {
    }

    private final Supplier<? extends Collection<String>> source;
    private final LongSupplier clock;
    private volatile Cached cached;
    private final Map<Round, Integer> rounds = new LinkedHashMap<>(64, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Round, Integer> eldest) {
            return size() > SESSIONS_COUNTED;
        }
    };
    private final Map<SessionKey, RibltSession> riblt = new LinkedHashMap<>();

    /**
     * A holder's side.
     *
     * @param source the CIDs this holder reconciles
     * @param clock  epoch milliseconds
     */
    public SketchServer(Supplier<? extends Collection<String>> source, LongSupplier clock) {
        this.source = Objects.requireNonNull(source, "source");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    private SketchSet set(long now) {
        Cached c = cached;
        if (c == null || now - c.atMillis >= SET_CACHE_MILLIS) {
            c = new Cached(now, SketchSet.of(source.get()));
            cached = c;
        }
        return c.set;
    }

    /**
     * Answers one sketch.
     *
     * @param from   the asker
     * @param req    the request id, which names the asker's session
     * @param sketch the sketch of the want
     * @param budget the want's budget, which bounds the reply
     * @return the answer
     */
    public Answer answer(PeerId from, int req, byte[] sketch, long budget) {
        Objects.requireNonNull(from, "from");
        if (sketch == null || sketch.length == 0) {
            return new Dropped("an empty sketch");
        }
        if (sketch.length > MAX_SKETCH_BYTES) {
            return new Dropped("a sketch of " + sketch.length + " bytes passes " + MAX_SKETCH_BYTES);
        }
        int encoding = sketch[0] & 0xff;
        boolean negentropy = encoding >= 0x60 && encoding <= 0x6f && encoding != Riblt.ENCODING;
        if (!negentropy && encoding != Riblt.ENCODING) {
            return new Dropped("unknown sketch encoding 0x" + Integer.toHexString(encoding));
        }
        synchronized (rounds) {
            Round round = new Round(from, req);
            int taken = rounds.getOrDefault(round, 0);
            if (taken >= MAX_ROUNDS) {
                return new Refusal(Messages.REASON_OVER_BUDGET, "a session takes at most " + MAX_ROUNDS + " rounds");
            }
            rounds.put(round, taken + 1);
        }
        int limit = (int) Math.max(Negentropy.MIN_FRAME_SIZE_LIMIT, Math.min(budget, MAX_SKETCH_BYTES));
        long now = clock.getAsLong();
        try {
            if (negentropy) {
                return new Reply(new Negentropy(set(now).storage(), limit).reconcile(sketch));
            }
            return riblt(from, sketch, limit, now);
        } catch (IllegalArgumentException malformed) {
            return new Dropped(malformed.getMessage());
        }
    }

    private Answer riblt(PeerId from, byte[] sketch, int limit, long now) {
        Negentropy.Reader in = new Negentropy.Reader(sketch);
        in.next();
        byte[] key = in.bytes(Riblt.KEY_BYTES);
        long start = in.varInt();
        long count = in.varInt();
        if (count > in.remaining() / (Riblt.ID_SIZE + 9)) {
            throw new IllegalArgumentException(count + " symbols run past the sketch");
        }
        List<Riblt.Symbol> symbols = new java.util.ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            symbols.add(Riblt.Symbol.read(in));
        }
        if (in.remaining() != 0) {
            throw new IllegalArgumentException(in.remaining() + " bytes trail the symbols");
        }
        SessionKey name = new SessionKey(from, HexFormat.of().formatHex(key));
        RibltSession session;
        synchronized (riblt) {
            riblt.values().removeIf(s -> now - s.usedMillis > RIBLT_IDLE_MILLIS);
            session = riblt.get(name);
            if (session == null) {
                if (start != 0) {
                    return new Dropped("a RIBLT stream starts at index 0, got index " + start);
                }
                if (riblt.size() >= RIBLT_SESSIONS) {
                    return new Refusal(Messages.REASON_OVER_BUDGET, "this holder runs " + RIBLT_SESSIONS + " RIBLT sessions already");
                }
                session = new RibltSession(new Riblt.Decoder(key, set(now).ids()), now);
                riblt.put(name, session);
            }
        }
        synchronized (session) {
            session.usedMillis = now;
            if (start != session.decoder.symbols()) {
                return new Dropped("the next RIBLT symbol is index " + session.decoder.symbols() + ", got index " + start);
            }
            try {
                for (Riblt.Symbol symbol : symbols) {
                    session.decoder.add(symbol);
                }
            } catch (IllegalArgumentException forged) {
                synchronized (riblt) {
                    riblt.remove(name, session); // a decoder that peeled a forged stream holds nothing usable
                }
                return new Dropped(forged.getMessage());
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(Riblt.ENCODING);
            if (!session.decoder.decoded()) {
                if (session.decoder.symbols() <= MAX_RIBLT_SYMBOLS) {
                    out.write(Riblt.MORE);
                    return new Reply(out.toByteArray());
                }
                out.write(Riblt.TOO_MANY);
            } else {
                List<byte[]> askerOnly = session.decoder.remoteOnly();
                List<byte[]> holderOnly = session.decoder.localOnly();
                if (3 + 20 + (long) (askerOnly.size() + holderOnly.size()) * Riblt.ID_SIZE > limit) {
                    out.write(Riblt.TOO_MANY);
                } else {
                    out.write(Riblt.DECODED);
                    Negentropy.writeVarInt(out, askerOnly.size());
                    askerOnly.forEach(out::writeBytes);
                    Negentropy.writeVarInt(out, holderOnly.size());
                    holderOnly.forEach(out::writeBytes);
                }
            }
            synchronized (riblt) {
                riblt.remove(name, session);
            }
            return new Reply(out.toByteArray());
        }
    }
}
