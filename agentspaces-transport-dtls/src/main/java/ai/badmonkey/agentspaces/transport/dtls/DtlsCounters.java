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

import java.util.concurrent.atomic.LongAdder;

/**
 * Counters for one {@link DtlsTransport} (SRAD DT-B-11), as plain {@code long}
 * accessors so the console can read them with no metrics dependency. Read
 * together they tell loss ({@link #retransmitted()} rising) from attack
 * ({@link #clientHellosDropped()} rising) from roaming ({@link #migrations()}
 * rising) from misconfiguration ({@link #handshakesFailed()} rising).
 */
public final class DtlsCounters {

    final LongAdder associationsOpened = new LongAdder();
    final LongAdder handshakesFailed = new LongAdder();
    final LongAdder helloVerifyRequestsSent = new LongAdder();
    final LongAdder clientHellosDropped = new LongAdder();
    final LongAdder datagramsDropped = new LongAdder();
    final LongAdder pdusSent = new LongAdder();
    final LongAdder retransmitted = new LongAdder();
    final LongAdder acknowledged = new LongAdder();
    final LongAdder framesDelivered = new LongAdder();
    final LongAdder migrations = new LongAdder();
    final LongAdder unknownConnectionIds = new LongAdder();
    final LongAdder unknownPduVersions = new LongAdder();
    final LongAdder failedIdle = new LongAdder();
    final LongAdder failedProtocol = new LongAdder();
    final LongAdder failedBound = new LongAdder();
    final LongAdder failedSendTimeout = new LongAdder();

    /** @return associations that completed a handshake */
    public long associationsOpened() {
        return associationsOpened.sum();
    }

    /** @return handshakes that failed or timed out, either role */
    public long handshakesFailed() {
        return handshakesFailed.sum();
    }

    /** @return HelloVerifyRequests the listener answered ClientHellos with */
    public long helloVerifyRequestsSent() {
        return helloVerifyRequestsSent.sum();
    }

    /** @return ClientHellos dropped by the per-source rate or the half-open bound */
    public long clientHellosDropped() {
        return clientHellosDropped.sum();
    }

    /** @return datagrams from unknown sources that were not ClientHellos, or that overflowed a queue */
    public long datagramsDropped() {
        return datagramsDropped.sum();
    }

    /** @return recovery-sublayer PDUs sent */
    public long pdusSent() {
        return pdusSent.sum();
    }

    /** @return DATA PDUs that carried a retransmitted fragment */
    public long retransmitted() {
        return retransmitted.sum();
    }

    /** @return DATA PDUs acknowledged by the peer */
    public long acknowledged() {
        return acknowledged.sum();
    }

    /** @return frames delivered to receivers */
    public long framesDelivered() {
        return framesDelivered.sum();
    }

    /** @return associations that moved to a new peer address (Connection ID) */
    public long migrations() {
        return migrations.sum();
    }

    /** @return records naming a connection id the listener never issued */
    public long unknownConnectionIds() {
        return unknownConnectionIds.sum();
    }

    /** @return PDUs dropped for an unknown sublayer version */
    public long unknownPduVersions() {
        return unknownPduVersions.sum();
    }

    /** @return associations failed by the idle timeout */
    public long failedIdle() {
        return failedIdle.sum();
    }

    /** @return associations failed by a protocol error (a malformed PDU) */
    public long failedProtocol() {
        return failedProtocol.sum();
    }

    /** @return associations failed by the receive bound */
    public long failedBound() {
        return failedBound.sum();
    }

    /** @return associations failed because a send waited 30 s for the window */
    public long failedSendTimeout() {
        return failedSendTimeout.sum();
    }

    @Override
    public String toString() {
        return "DtlsCounters{opened=" + associationsOpened() + ", handshakesFailed=" + handshakesFailed()
                + ", hvr=" + helloVerifyRequestsSent() + ", hellosDropped=" + clientHellosDropped()
                + ", pdus=" + pdusSent() + ", retransmitted=" + retransmitted()
                + ", delivered=" + framesDelivered() + ", migrations=" + migrations() + "}";
    }
}
