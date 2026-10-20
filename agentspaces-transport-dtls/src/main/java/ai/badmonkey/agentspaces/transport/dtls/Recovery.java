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

package ai.badmonkey.agentspaces.transport.dtls;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * The recovery sublayer (SRAD §3.5, appendix B): a selective-repeat protocol
 * that turns an unreliable, unordered datagram association into the
 * reliable, ordered frame channel the transport SPI promises. Every algorithm
 * is a published one: RFC 9000 §19.3 acknowledgement ranges and §13.2
 * acknowledgement scheduling, RFC 6298 retransmission timing, RFC 9002 §6
 * loss detection and probe backoff.
 *
 * <p>Each direction is independent. The sender numbers transmissions with a
 * packet number and frames with a frame sequence, fragments each frame,
 * keeps at most {@code window} DATA PDUs unacknowledged, and retransmits a
 * lost fragment in a new PDU with a new packet number. The receiver
 * acknowledges, reassembles, and hands frames back strictly in sequence.
 *
 * <p>Bounds: the sender starts a frame only while it is fewer than
 * {@value #MAX_FRAMES_AHEAD} frames past the oldest frame the receiver may
 * still hold and the started frames total at most {@value #MAX_BUFFERED_BYTES}
 * bytes, so an honest receiver never holds more; a receiver asked to hold
 * more fails the association with {@code CLOSE(2)}.
 *
 * <p>The class is a pure state machine: no threads, no sockets, a clock passed
 * in. Its owner serializes every call on the instance's monitor, sends PDUs
 * through {@link Output}, arms a timer at {@link #nextDeadline()}, and calls
 * {@link #onTimer()} when it fires. It carries no security function: the DTLS
 * record layer authenticates and replay-protects every PDU (SRAD §3.5).
 */
final class Recovery {

    /** Where PDUs go. A failed transmission is treated as loss. */
    interface Output {
        void transmit(byte[] pdu) throws IOException;
    }

    /** Why an association ended. */
    record Failure(int code, String reason, boolean byPeer) {
    }

    static final int MAX_FRAME = 8 * 1024 * 1024;
    static final int MAX_FRAMES_AHEAD = 64;
    static final long MAX_BUFFERED_BYTES = 16L * 1024 * 1024;
    /** {@link #offer} refuses while this many bytes wait for first transmission. */
    static final long MAX_BACKLOG_BYTES = 8L * 1024 * 1024;
    static final int PACKET_THRESHOLD = 3;
    static final int DUPLICATE_WINDOW = 1024;
    static final int MAX_BACKOFF = 6;

    static final long MILLI = 1_000_000L;
    static final long ACK_DELAY = 25 * MILLI;
    static final long INITIAL_RTO = 1_000 * MILLI;
    static final long MIN_RTO = 200 * MILLI;
    static final long MAX_RTO = 10_000 * MILLI;
    static final long GRANULARITY = MILLI;
    private static final long NONE = Long.MAX_VALUE;

    private final Output out;
    private final int fragment;
    private final int window;
    private final long idleNanos;
    private final LongSupplier clock;
    private final DtlsCounters counters;

    // ------------------------------------------------------------ sender state
    private long nextPn;
    private long nextFs;
    /** Frames not yet released, keyed by frame sequence; the first key is the oldest the receiver may hold. */
    private final TreeMap<Long, OutFrame> outgoing = new TreeMap<>();
    /** Frames with fragments never transmitted, in order. */
    private final ArrayDeque<OutFrame> fresh = new ArrayDeque<>();
    /** Fragments declared lost, sent ahead of fresh ones. */
    private final ArrayDeque<Fragment> lost = new ArrayDeque<>();
    private final TreeMap<Long, Sent> inFlight = new TreeMap<>();
    private long backlogBytes;
    private long startedBytes;
    private long srtt = -1;
    private long rttvar;
    private long latestRtt;
    private long rto = INITIAL_RTO;
    private int backoff;
    private long lastSent;
    private long lastReceived;
    private long pathProbePn = -1;
    private boolean pathValidated = true;

    // ---------------------------------------------------------- receiver state
    /** Received packet numbers as inclusive ranges, low to high. */
    private final TreeMap<Long, Long> received = new TreeMap<>();
    private long largestReceived = -1;
    private long largestReceivedAt;
    private int unackedPackets;
    private boolean ackNow;
    private long ackDeadline = NONE;
    private final TreeMap<Long, InFrame> reassembly = new TreeMap<>();
    private long nextDeliver;
    private long bufferedBytes;

    private Failure failure;

    Recovery(Output out, int fragment, int window, long idleNanos, LongSupplier clock, DtlsCounters counters) {
        if (fragment < 1 || (long) fragment * 0xFFFF < MAX_FRAME) {
            throw new IllegalArgumentException("fragment size " + fragment + " cannot carry an 8 MiB frame");
        }
        this.out = out;
        this.fragment = fragment;
        this.window = window;
        this.idleNanos = idleNanos;
        this.clock = clock;
        this.counters = counters;
        long now = clock.getAsLong();
        this.lastSent = now;
        this.lastReceived = now;
    }

    private static final class OutFrame {
        final long fs;
        final byte[] data;
        final int count;
        final BitSet acked;
        int ackedCount;
        int nextIndex;
        boolean started;

        OutFrame(long fs, byte[] data, int count) {
            this.fs = fs;
            this.data = data;
            this.count = count;
            this.acked = new BitSet(count);
        }

        boolean complete() {
            return ackedCount == count;
        }
    }

    private record Fragment(OutFrame frame, int index) {
    }

    private record Sent(OutFrame frame, int index, long sentAt) {
    }

    private static final class InFrame {
        final int count;
        final byte[][] parts;
        int have;
        int partLength = -1;
        long bytes;

        InFrame(int count) {
            this.count = count;
            this.parts = new byte[count][];
        }
    }

    // ------------------------------------------------------------- sending

    /** Whether the association has ended, and why. */
    Failure failure() {
        return failure;
    }

    /** Bytes accepted by {@link #offer} and not yet transmitted once. */
    long backlogBytes() {
        return backlogBytes;
    }

    /**
     * Queues a frame for reliable, ordered delivery.
     *
     * @return false when the backlog is full; the caller waits and retries
     * @throws IOException when the frame is empty or over 8 MiB, or the association has failed
     */
    boolean offer(byte[] frame) throws IOException {
        if (failure != null) {
            throw new IOException("association closed: " + failure.reason());
        }
        if (frame.length == 0) {
            throw new IOException("empty frame");
        }
        if (frame.length > MAX_FRAME) {
            throw new IOException("frame exceeds " + MAX_FRAME + " bytes: " + frame.length);
        }
        if (backlogBytes > 0 && backlogBytes + frame.length > MAX_BACKLOG_BYTES) {
            return false;
        }
        if (nextFs > Pdu.MAX_U32) {
            fail(Pdu.CLOSE_PROTOCOL, "frame sequence exhausted", true);
            throw new IOException("frame sequence exhausted");
        }
        int count = (frame.length + fragment - 1) / fragment;
        OutFrame outFrame = new OutFrame(nextFs++, frame, count);
        outgoing.put(outFrame.fs, outFrame);
        fresh.add(outFrame);
        backlogBytes += frame.length;
        pump();
        return true;
    }

    private void pump() {
        while (failure == null && inFlight.size() < window) {
            Fragment next = nextFragment();
            if (next == null) {
                return;
            }
            transmitData(next.frame(), next.index());
        }
    }

    private Fragment nextFragment() {
        while (!lost.isEmpty()) {
            Fragment candidate = lost.poll();
            if (!candidate.frame().acked.get(candidate.index())
                    && outgoing.get(candidate.frame().fs) == candidate.frame()) {
                counters.retransmitted.increment();
                return candidate;
            }
        }
        OutFrame head = fresh.peek();
        if (head == null) {
            return null;
        }
        if (!head.started) {
            long oldest = outgoing.firstKey();
            boolean admitted = head.fs - oldest < MAX_FRAMES_AHEAD
                    && (startedBytes == 0 || startedBytes + head.data.length <= MAX_BUFFERED_BYTES);
            if (!admitted) {
                return null;
            }
            head.started = true;
            startedBytes += head.data.length;
        }
        int index = head.nextIndex++;
        backlogBytes -= partLength(head, index);
        if (head.nextIndex == head.count) {
            fresh.poll();
        }
        return new Fragment(head, index);
    }

    private int partLength(OutFrame frame, int index) {
        return index < frame.count - 1 ? fragment : frame.data.length - fragment * (frame.count - 1);
    }

    private void transmitData(OutFrame frame, int index) {
        long now = clock.getAsLong();
        if (nextPn > Pdu.MAX_U32) {
            fail(Pdu.CLOSE_PROTOCOL, "packet numbers exhausted", true);
            return;
        }
        long pn = nextPn++;
        int length = partLength(frame, index);
        Pdu.AckBlock ack = null;
        if (ackPending()) {
            Pdu.AckBlock candidate = ackBlock(now);
            // Piggyback only when the ACK fits the datagram beside this fragment.
            if (candidate.encodedLength() <= fragment - length) {
                ack = candidate;
                ackSent();
            }
        }
        byte[] pdu = Pdu.data(pn, frame.fs, index, frame.count, ack, frame.data, index * fragment, length);
        inFlight.put(pn, new Sent(frame, index, now));
        transmit(pdu, now);
    }

    private void transmit(byte[] pdu, long now) {
        lastSent = now;
        counters.pdusSent.increment();
        try {
            out.transmit(pdu);
        } catch (IOException e) {
            // A datagram that cannot be sent is a lost datagram: recovery retransmits it.
        }
    }

    // ----------------------------------------------------------- receiving

    /**
     * Processes one inbound PDU.
     *
     * @return the frames now deliverable, in order; empty when none
     */
    List<byte[]> onPdu(byte[] buf, int off, int len) {
        if (failure != null) {
            return List.of();
        }
        long now = clock.getAsLong();
        Pdu.Decoded pdu;
        try {
            pdu = Pdu.decode(buf, off, len);
        } catch (Pdu.ProtocolError e) {
            fail(Pdu.CLOSE_PROTOCOL, e.getMessage(), true);
            return List.of();
        }
        if (pdu == null) {
            counters.unknownPduVersions.increment();
            return List.of();
        }
        lastReceived = now;
        List<byte[]> deliverable = List.of();
        switch (pdu) {
            case Pdu.Data data -> {
                if (data.ack() != null) {
                    onAck(data.ack(), now);
                }
                deliverable = onData(data, now);
            }
            case Pdu.Ack ack -> onAck(ack.block(), now);
            case Pdu.Ping ignored -> transmit(Pdu.pong(), now);
            case Pdu.Pong ignored -> {
            }
            case Pdu.Close close -> {
                fail(close.code(), "closed by peer (code " + close.code() + ")", false);
                return List.of();
            }
        }
        if (failure == null) {
            pump();
            if (ackPending() && ackNow) {
                sendAck(now);
            }
        }
        return deliverable;
    }

    private List<byte[]> onData(Pdu.Data data, long now) {
        long pn = data.pn();
        if (pn <= largestReceived - DUPLICATE_WINDOW || isReceived(pn)) {
            ackNow = true; // the peer may have missed our acknowledgement
            return List.of();
        }
        boolean inOrder = largestReceived < 0 || pn == largestReceived + 1;
        markReceived(pn);
        if (pn > largestReceived) {
            largestReceived = pn;
            largestReceivedAt = now;
        }
        unackedPackets++;
        if (!inOrder || unackedPackets >= 2) {
            ackNow = true;
        } else if (ackDeadline == NONE) {
            ackDeadline = now + ACK_DELAY;
        }

        long fs = data.fs();
        if (fs < nextDeliver) {
            return List.of(); // a retransmission of a frame already delivered
        }
        if (fs - nextDeliver >= MAX_FRAMES_AHEAD) {
            fail(Pdu.CLOSE_BOUND, "frame " + fs + " is too far ahead of " + nextDeliver, true);
            return List.of();
        }
        InFrame frame = reassembly.get(fs);
        if (frame == null) {
            frame = new InFrame(data.count());
            reassembly.put(fs, frame);
        } else if (frame.count != data.count()) {
            fail(Pdu.CLOSE_PROTOCOL, "frame " + fs + " changed its fragment count", true);
            return List.of();
        }
        int index = data.index();
        if (frame.parts[index] != null) {
            return List.of();
        }
        int length = data.payload().length;
        boolean last = index == frame.count - 1;
        if (!last) {
            if (frame.partLength < 0) {
                frame.partLength = length;
            } else if (frame.partLength != length) {
                fail(Pdu.CLOSE_PROTOCOL, "frame " + fs + " has fragments of unequal length", true);
                return List.of();
            }
            if ((long) length * (frame.count - 1) > MAX_FRAME) {
                fail(Pdu.CLOSE_PROTOCOL, "frame " + fs + " would exceed 8 MiB", true);
                return List.of();
            }
        }
        if (frame.partLength >= 0) {
            boolean lastTooLong = last ? length > frame.partLength : partLengthOf(frame) > frame.partLength;
            if (lastTooLong || (long) frame.partLength * (frame.count - 1) + (last ? length : 0) > MAX_FRAME) {
                fail(Pdu.CLOSE_PROTOCOL, "frame " + fs + " has a malformed final fragment", true);
                return List.of();
            }
        }
        if (bufferedBytes + length > MAX_BUFFERED_BYTES) {
            fail(Pdu.CLOSE_BOUND, "receive bound of " + MAX_BUFFERED_BYTES + " bytes exceeded", true);
            return List.of();
        }
        frame.parts[index] = data.payload();
        frame.have++;
        frame.bytes += length;
        bufferedBytes += length;

        List<byte[]> deliverable = new ArrayList<>();
        Map.Entry<Long, InFrame> first;
        while ((first = reassembly.firstEntry()) != null && first.getKey() == nextDeliver
                && first.getValue().have == first.getValue().count) {
            InFrame complete = reassembly.pollFirstEntry().getValue();
            byte[] assembled = new byte[Math.toIntExact(complete.bytes)];
            int at = 0;
            for (byte[] part : complete.parts) {
                System.arraycopy(part, 0, assembled, at, part.length);
                at += part.length;
            }
            bufferedBytes -= complete.bytes;
            nextDeliver++;
            counters.framesDelivered.increment();
            deliverable.add(assembled);
        }
        return deliverable;
    }

    /** Length of the final fragment if already received, else 0. */
    private static int partLengthOf(InFrame frame) {
        byte[] last = frame.parts[frame.count - 1];
        return last == null ? 0 : last.length;
    }

    private boolean isReceived(long pn) {
        Map.Entry<Long, Long> range = received.floorEntry(pn);
        return range != null && range.getValue() >= pn;
    }

    private void markReceived(long pn) {
        Map.Entry<Long, Long> below = received.floorEntry(pn);
        Map.Entry<Long, Long> above = received.ceilingEntry(pn + 1);
        long low = pn;
        long high = pn;
        if (below != null && below.getValue() == pn - 1) {
            low = below.getKey();
            received.remove(below.getKey());
        }
        if (above != null && above.getKey() == pn + 1) {
            high = above.getValue();
            received.remove(above.getKey());
        }
        received.put(low, high);
        long floor = Math.max(pn, largestReceived) - DUPLICATE_WINDOW;
        while (!received.isEmpty() && received.firstEntry().getValue() < floor) {
            received.pollFirstEntry();
        }
    }

    private boolean ackPending() {
        return unackedPackets > 0;
    }

    private Pdu.AckBlock ackBlock(long now) {
        List<long[]> newestFirst = new ArrayList<>();
        for (Map.Entry<Long, Long> range : received.descendingMap().entrySet()) {
            newestFirst.add(new long[]{range.getKey(), range.getValue()});
            if (newestFirst.size() > Pdu.MAX_RANGES) {
                break;
            }
        }
        return Pdu.block(newestFirst, Math.max(0, (now - largestReceivedAt) / 1_000));
    }

    private void sendAck(long now) {
        transmit(Pdu.ack(ackBlock(now)), now);
        ackSent();
    }

    private void ackSent() {
        unackedPackets = 0;
        ackNow = false;
        ackDeadline = NONE;
    }

    // ----------------------------------------------------- acknowledgement

    private void onAck(Pdu.AckBlock block, long now) {
        boolean progressed = false;
        Sent largest = null;
        for (long[] range : block.ranges()) {
            NavigableMap<Long, Sent> acked = inFlight.subMap(range[0], true, range[1], true);
            for (Iterator<Map.Entry<Long, Sent>> it = acked.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, Sent> entry = it.next();
                Sent sent = entry.getValue();
                it.remove();
                progressed = true;
                counters.acknowledged.increment();
                if (entry.getKey() == block.largestAcked()) {
                    largest = sent;
                }
                if (pathProbePn >= 0 && entry.getKey() >= pathProbePn) {
                    pathValidated = true;
                }
                OutFrame frame = sent.frame();
                if (!frame.acked.get(sent.index())) {
                    frame.acked.set(sent.index());
                    frame.ackedCount++;
                }
            }
        }
        if (!progressed) {
            return;
        }
        backoff = 0;
        if (largest != null) {
            updateRtt(now - largest.sentAt(), block.ackDelayMicros() * 1_000);
        }
        // Release the contiguous acknowledged prefix: the receiver has delivered it.
        Map.Entry<Long, OutFrame> first;
        while ((first = outgoing.firstEntry()) != null && first.getValue().complete()) {
            outgoing.pollFirstEntry();
            startedBytes -= first.getValue().data.length;
        }
        detectLosses(block.largestAcked(), now);
    }

    private void updateRtt(long sample, long ackDelay) {
        latestRtt = Math.max(sample, 0);
        if (srtt < 0) {
            srtt = latestRtt;
            rttvar = latestRtt / 2;
        } else {
            long adjusted = ackDelay < latestRtt ? latestRtt - ackDelay : latestRtt;
            rttvar = (3 * rttvar + Math.abs(srtt - adjusted)) / 4;
            srtt = (7 * srtt + adjusted) / 8;
        }
        rto = Math.min(MAX_RTO, Math.max(MIN_RTO, srtt + Math.max(GRANULARITY, 4 * rttvar)));
    }

    private void detectLosses(long largestAcked, long now) {
        long lossDelay = srtt < 0 ? rto : (9 * Math.max(srtt, latestRtt)) / 8;
        for (Iterator<Map.Entry<Long, Sent>> it = inFlight.headMap(largestAcked, false).entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Sent> entry = it.next();
            boolean byCount = largestAcked - entry.getKey() >= PACKET_THRESHOLD;
            boolean byTime = entry.getValue().sentAt() <= now - lossDelay;
            if (byCount || byTime) {
                it.remove();
                Sent sent = entry.getValue();
                if (!sent.frame().acked.get(sent.index())) {
                    lost.add(new Fragment(sent.frame(), sent.index()));
                }
            }
        }
    }

    // ---------------------------------------------------------------- time

    /** When {@link #onTimer()} next has work, in the clock's nanoseconds; {@link Long#MAX_VALUE} for never. */
    long nextDeadline() {
        if (failure != null) {
            return NONE;
        }
        long next = Math.min(lastReceived + idleNanos, lastSent + idleNanos / 4);
        if (ackPending()) {
            next = Math.min(next, ackDeadline);
        }
        if (!inFlight.isEmpty()) {
            next = Math.min(next, inFlight.firstEntry().getValue().sentAt() + probeTimeout());
        }
        return next;
    }

    private long probeTimeout() {
        return Math.min(MAX_RTO, rto << backoff);
    }

    /** Runs every timer that is due: idle failure, delayed ACK, probe timeout, keepalive. */
    void onTimer() {
        if (failure != null) {
            return;
        }
        long now = clock.getAsLong();
        if (now - lastReceived >= idleNanos) {
            fail(Pdu.CLOSE_IDLE, "idle for " + idleNanos / MILLI + " ms", true);
            return;
        }
        if (ackPending() && now >= ackDeadline) {
            sendAck(now);
        }
        if (!inFlight.isEmpty()) {
            Map.Entry<Long, Sent> oldest = inFlight.firstEntry();
            if (now >= oldest.getValue().sentAt() + probeTimeout()) {
                // RFC 9002 §6.2: no acknowledgement for a probe timeout; retransmit the oldest and back off.
                inFlight.pollFirstEntry();
                Sent sent = oldest.getValue();
                if (!sent.frame().acked.get(sent.index())) {
                    lost.addFirst(new Fragment(sent.frame(), sent.index()));
                }
                backoff = Math.min(backoff + 1, MAX_BACKOFF);
            }
        }
        pump();
        if (now - lastSent >= idleNanos / 4) {
            transmit(Pdu.ping(), now);
        }
    }

    // ------------------------------------------------------------- closing

    /** Closes normally, sending {@code CLOSE(0)} once. */
    void close() {
        fail(Pdu.CLOSE_NORMAL, "closed", true);
    }

    /** Fails the association, sending {@code CLOSE(code)} once when asked. */
    void fail(int code, String reason, boolean sendClose) {
        if (failure != null) {
            return;
        }
        failure = new Failure(code, reason, !sendClose);
        if (sendClose) {
            transmit(Pdu.close(code), clock.getAsLong());
        }
        switch (code) {
            case Pdu.CLOSE_PROTOCOL -> counters.failedProtocol.increment();
            case Pdu.CLOSE_BOUND -> counters.failedBound.increment();
            case Pdu.CLOSE_IDLE -> counters.failedIdle.increment();
            default -> {
            }
        }
        lost.clear();
        fresh.clear();
        inFlight.clear();
        reassembly.clear();
        outgoing.clear();
        backlogBytes = 0;
    }

    // ----------------------------------------------------- path validation

    /** The peer's address changed (Connection ID): the new path is unvalidated until data sent on it is acknowledged. */
    void pathChanged() {
        pathProbePn = nextPn;
        pathValidated = false;
    }

    /** Whether the current path has acknowledged data sent on it since the last change. */
    boolean pathValidated() {
        return pathValidated;
    }

    /** Smoothed round-trip time in nanoseconds, or -1 before the first sample. */
    long smoothedRtt() {
        return srtt;
    }

    int inFlight() {
        return inFlight.size();
    }
}
