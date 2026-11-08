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
package ai.badmonkey.agentspaces.pdt;

import java.util.List;
import java.util.Objects;

/**
 * A node a value needs could not be had (srad/agentspaces-pdt.md PD-V-06): the
 * store lacks it and no fetch produced it before the deadline. The message names
 * the CID, the sources tried, and, when the loader knows it, why the load stopped.
 * The values of {@code pdt.values} and the loader of {@code pdt.store} share this
 * one class.
 */
public final class NodeUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String cid;
    private final List<String> sources;

    /**
     * A refusal naming the CID and the sources tried.
     *
     * @param cid     the CID
     * @param sources the sources tried, empty when only the local store was asked
     */
    public NodeUnavailableException(String cid, List<String> sources) {
        this(cid, sources, null, null);
    }

    /**
     * A failure naming why the load stopped.
     *
     * @param cid     the CID wanted
     * @param sources the sources tried, possibly empty
     * @param why     why the load stopped: a deadline, a refusal, or an error
     */
    public NodeUnavailableException(String cid, List<String> sources, String why) {
        this(cid, sources, why, null);
    }

    /**
     * A failure with its cause.
     *
     * @param cid     the CID wanted
     * @param sources the sources tried, possibly empty
     * @param why     why the load stopped, or null
     * @param cause   the underlying failure, or null
     */
    public NodeUnavailableException(String cid, List<String> sources, String why, Throwable cause) {
        super(message(cid, sources, why), cause);
        this.cid = Objects.requireNonNull(cid, "cid");
        this.sources = sources == null ? List.of() : List.copyOf(sources);
    }

    private static String message(String cid, List<String> sources, String why) {
        String tried = sources == null || sources.isEmpty() ? "none, local store only" : String.join(", ", sources);
        return "node " + cid + " unavailable" + (why == null ? "" : ": " + why) + " (sources tried: " + tried + ")";
    }

    /** The CID that could not be had. */
    public String cid() {
        return cid;
    }

    /** The sources tried, in order. */
    public List<String> sources() {
        return sources;
    }
}
