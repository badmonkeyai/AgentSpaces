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

/**
 * A refusal by the node layer (PD-N-04, PD-N-06): the node is over a bound, a
 * link names a codec the decoder does not know, the kind is unknown, a key falls
 * outside the kind's schema, or the bytes do not hash to the CID they claim. The
 * exception carries the CID when the caller knew it and the rule that refused.
 */
public class NodeFormatException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String cid;
    private final String rule;

    /**
     * A refusal of bytes whose CID the caller did not know.
     *
     * @param rule the rule that refused, named for the operator
     */
    public NodeFormatException(String rule) {
        this(null, rule, null);
    }

    /**
     * A refusal of a named node.
     *
     * @param cid  the CID of the node, or null when unknown
     * @param rule the rule that refused, named for the operator
     */
    public NodeFormatException(String cid, String rule) {
        this(cid, rule, null);
    }

    /**
     * A refusal of a named node with an underlying cause.
     *
     * @param cid   the CID of the node, or null when unknown
     * @param rule  the rule that refused, named for the operator
     * @param cause the cause, or null
     */
    public NodeFormatException(String cid, String rule, Throwable cause) {
        super(cid == null ? rule : rule + " in node " + cid, cause);
        this.cid = cid;
        this.rule = rule;
    }

    /** The CID of the refused node, or null when the caller did not know it. */
    public String cid() {
        return cid;
    }

    /** The rule that refused. */
    public String rule() {
        return rule;
    }

    /** A copy of this exception that names the CID, when this one does not. */
    public NodeFormatException withCid(String cid) {
        if (this.cid != null || cid == null) {
            return this;
        }
        return new NodeFormatException(cid, rule, getCause());
    }
}
