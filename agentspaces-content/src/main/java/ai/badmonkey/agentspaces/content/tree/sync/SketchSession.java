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
import ai.badmonkey.agentspaces.content.tree.Cids;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The asker's side of a reconciliation (PDT 2 phase 5, P2-K-02, P2-K-05; srad/agentspaces-pdt2.md §3.6, S4 of
 * §6.4): one session with one holder, every round a {@code TREE_WANT} with {@code root ""}, no haves, and the sketch,
 * every answer a {@code TREE_END} of reason 5 with the reply in its sketch, all under one request id. The outcome is
 * two lists of CIDs: {@code have}, which this member holds and the holder lacks, and {@code need}, which the holder
 * holds and this member lacks.
 *
 * <p>With an expected difference of 0 the session runs negentropy from the start. With an expected difference
 * {@code d} it streams RIBLT symbols under a fresh random key, about 1.4 {@code d} at first and a quarter of
 * {@code d} more per round, up to {@link #symbolCap} (twice {@code d}, at least 64); a holder that answers
 * {@link Riblt#TOO_MANY}, or a stream that reaches the cap, continues by negentropy in the same session. A session
 * fails after {@link SketchServer#MAX_ROUNDS} rounds, after {@link #ROUND_TIMEOUT_MILLIS} without an answer, on a
 * refusal, or on an answer outside its grammar. Safe for the capability's threads.
 */
public final class SketchSession {

    /** How long a round waits for its answer. */
    public static final long ROUND_TIMEOUT_MILLIS = 10_000;
    /** The RIBLT symbols one query carries at most, so it stays within {@link SketchServer#MAX_SKETCH_BYTES}. */
    public static final int SYMBOLS_PER_QUERY = (SketchServer.MAX_SKETCH_BYTES - 64) / (Riblt.ID_SIZE + 8 + 10);
    /** The fewest symbols a RIBLT session may stream before it falls back. */
    static final int MIN_SYMBOL_CAP = 64;

    /** What the session needs from the capability. */
    public interface Fabric {
        /** The capability's clock. */
        long nowMillis();

        /**
         * Sends a want to the holder.
         *
         * @return the bytes the encoded want took
         */
        int send(PeerId to, Messages.TreeWant want);
    }

    /**
     * The outcome of a session.
     *
     * @param have          CIDs this member holds and the holder lacks
     * @param need          CIDs the holder holds and this member lacks, each the dag-cbor CID of the digest that came
     *                      back, since a sketch carries digests only; the caller resolves a raw root by its manifest
     * @param rounds        the rounds the session took
     * @param bytesSent     the bytes of the wants sent
     * @param bytesReceived the bytes of the answers received
     * @param encoding      {@code negentropy}, {@code riblt}, or {@code riblt,negentropy} after a fallback
     */
    public record Outcome(List<String> have, List<String> need, int rounds, long bytesSent,
                          long bytesReceived, String encoding) {
    }

    private final PeerId holder;
    private final int req;
    private final SketchSet set;
    private final int expected;
    private final SecureRandom random;
    private final Fabric fabric;
    private final CompletableFuture<Outcome> outcome = new CompletableFuture<>();
    private final List<byte[]> have = new ArrayList<>();
    private final List<byte[]> need = new ArrayList<>();
    private Negentropy negentropy;
    private Riblt.Encoder encoder;
    private byte[] key;
    private String encoding;
    private int rounds;
    private long bytesSent;
    private long bytesReceived;
    private long sentAtMillis;

    /**
     * A session.
     *
     * @param holder   the member asked
     * @param req      the request id every round carries
     * @param set      this member's set
     * @param expected the expected difference for RIBLT, or 0 for negentropy alone
     * @param random   the source of the RIBLT key
     * @param fabric   the capability
     */
    public SketchSession(PeerId holder, int req, SketchSet set, int expected, SecureRandom random, Fabric fabric) {
        this.holder = Objects.requireNonNull(holder, "holder");
        this.req = req;
        this.set = Objects.requireNonNull(set, "set");
        if (expected < 0) {
            throw new IllegalArgumentException("the expected difference is at least 0");
        }
        this.expected = expected;
        this.random = Objects.requireNonNull(random, "random");
        this.fabric = fabric;
    }

    /** The member asked. */
    public PeerId holder() {
        return holder;
    }

    /** The request id of every round. */
    public int req() {
        return req;
    }

    /** The outcome, once the session ends. */
    public CompletableFuture<Outcome> outcome() {
        return outcome;
    }

    /** The RIBLT symbols this session streams before it falls back to negentropy. */
    public int symbolCap() {
        return (int) Math.min(SketchServer.MAX_RIBLT_SYMBOLS, Math.max(MIN_SYMBOL_CAP, 2L * expected));
    }

    /** Sends the first round. */
    public synchronized void start() {
        if (expected > 0) {
            encoding = "riblt";
            key = new byte[Riblt.KEY_BYTES];
            random.nextBytes(key);
            encoder = new Riblt.Encoder(key, set.ids());
            sendSymbols((int) Math.min(symbolCap(), Math.ceil(expected * 1.4)));
        } else {
            startNegentropy();
        }
    }

    private void startNegentropy() {
        encoding = encoding == null ? "negentropy" : encoding + ",negentropy";
        encoder = null;
        negentropy = new Negentropy(set.storage(), SketchServer.MAX_SKETCH_BYTES);
        send(negentropy.initiate());
    }

    private void sendSymbols(int count) {
        int n = Math.min(Math.max(count, 1), SYMBOLS_PER_QUERY);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(Riblt.ENCODING);
        out.writeBytes(key);
        Negentropy.writeVarInt(out, encoder.produced());
        Negentropy.writeVarInt(out, n);
        encoder.next(n).forEach(s -> s.write(out));
        send(out.toByteArray());
    }

    private void send(byte[] sketch) {
        if (rounds >= SketchServer.MAX_ROUNDS) {
            fail("no agreement within " + SketchServer.MAX_ROUNDS + " rounds");
            return;
        }
        rounds++;
        sentAtMillis = fabric.nowMillis();
        bytesSent += fabric.send(holder, new Messages.TreeWant(Messages.TREE_WANT, req, "", List.of(),
                SketchServer.MAX_SKETCH_BYTES, sketch));
    }

    /**
     * Takes a {@code TREE_END} of reason 5.
     *
     * @param from  the sender
     * @param end   the answer, whose bounds the caller checked
     * @param bytes the bytes the answer took
     * @return whether the answer belongs to this session
     */
    public synchronized boolean onEnd(PeerId from, Messages.TreeEnd end, int bytes) {
        if (!holder.equals(from) || end.req() != req || outcome.isDone()) {
            return false;
        }
        bytesReceived += bytes;
        byte[] reply = end.sketch();
        if (reply.length == 0) {
            fail("an empty answer");
            return true;
        }
        if (reply.length > SketchServer.MAX_SKETCH_BYTES) {
            fail("an answer of " + reply.length + " bytes passes " + SketchServer.MAX_SKETCH_BYTES);
            return true;
        }
        try {
            if (encoder != null) {
                onRiblt(reply);
            } else {
                onNegentropy(reply);
            }
        } catch (IllegalArgumentException malformed) {
            fail("an answer outside its grammar: " + malformed.getMessage());
        }
        return true;
    }

    private void onNegentropy(byte[] reply) {
        if ((reply[0] & 0xff) != Negentropy.PROTOCOL_VERSION) {
            throw new IllegalArgumentException("a negentropy round answered with 0x" + Integer.toHexString(reply[0] & 0xff));
        }
        Optional<byte[]> next = negentropy.reconcile(reply, have, need);
        if (next.isPresent()) {
            send(next.get());
        } else {
            complete();
        }
    }

    private void onRiblt(byte[] reply) {
        Negentropy.Reader in = new Negentropy.Reader(reply);
        if (in.next() != Riblt.ENCODING) {
            throw new IllegalArgumentException("a RIBLT round answered with another encoding");
        }
        int status = in.next();
        if (status == Riblt.MORE) {
            if (encoder.produced() >= symbolCap()) {
                startNegentropy();
            } else {
                sendSymbols((int) Math.min(symbolCap() - encoder.produced(), Math.max(8, expected / 4)));
            }
        } else if (status == Riblt.TOO_MANY) {
            startNegentropy();
        } else if (status == Riblt.DECODED) {
            readIds(in, have);
            readIds(in, need);
            if (in.remaining() != 0) {
                throw new IllegalArgumentException(in.remaining() + " bytes trail the lists");
            }
            complete();
        } else {
            throw new IllegalArgumentException("unknown RIBLT status " + status);
        }
    }

    private static void readIds(Negentropy.Reader in, List<byte[]> into) {
        long count = in.varInt();
        if (count > in.remaining() / Riblt.ID_SIZE) {
            throw new IllegalArgumentException(count + " ids run past the answer");
        }
        for (long i = 0; i < count; i++) {
            into.add(in.bytes(Riblt.ID_SIZE));
        }
    }

    /**
     * Takes a {@code REJECT} of the tree index for the root {@code ""}: the holder does not reconcile (reason 3) or
     * ended the session (reason 2).
     *
     * @return whether the refusal belongs to this session
     */
    public synchronized boolean onReject(PeerId from, Messages.Reject reject) {
        if (!holder.equals(from) || outcome.isDone()) {
            return false;
        }
        fail(from.display() + " refused the round with REJECT reason " + reject.reason());
        return true;
    }

    /** Fails the session when its round waited past {@link #ROUND_TIMEOUT_MILLIS}. */
    public synchronized void tick(long now) {
        if (!outcome.isDone() && now - sentAtMillis > ROUND_TIMEOUT_MILLIS) {
            fail("no answer within " + ROUND_TIMEOUT_MILLIS + " ms");
        }
    }

    private void complete() {
        List<String> haveCids = new ArrayList<>(have.size());
        for (byte[] id : have) {
            haveCids.add(set.cid(id).orElseThrow(() -> new IllegalArgumentException("a have id this member does not hold")));
        }
        List<String> needCids = new ArrayList<>(need.size());
        for (byte[] id : need) {
            needCids.add(Cids.ofDigest(id, Cids.DAG_CBOR));
        }
        outcome.complete(new Outcome(List.copyOf(haveCids), List.copyOf(needCids), rounds, bytesSent, bytesReceived, encoding));
    }

    private void fail(String why) {
        outcome.completeExceptionally(new IllegalStateException("reconciling with " + holder.display() + ": " + why));
    }
}
