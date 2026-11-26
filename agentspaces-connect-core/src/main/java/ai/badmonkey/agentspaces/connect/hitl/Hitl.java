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
package ai.badmonkey.agentspaces.connect.hitl;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The human-in-the-loop conventions (SPEC §6.1a conventions, v0.1.38): a desk of people who decide is an {@code AssetCard}
 * whose {@code shape} is {@code hitl:<kind>} and whose {@code access} map names who decides and how. An agent asks it
 * with an asset call ({@code ask}), and the desk answers with one signed {@code AssetInvocationResult} carrying the
 * decision. Nothing here is a new record or field: the meaning lives in open values the card already carries.
 *
 * <p>This class holds the names, a validator for a desk's card, the arguments' hash that binds a decision to one call,
 * and the decision as a requester or a source gate reads it.
 */
public final class Hitl {

    /** The shape prefix of a desk's card. */
    public static final String SHAPE_PREFIX = "hitl:";
    /** The kinds of desk: approve, confirm, review (may amend), choose one, or give an input. */
    public static final List<String> KINDS = List.of("approval", "confirm", "review", "choice", "input");
    /** The operations a desk's card lists in {@code invoke-ops}. */
    public static final List<String> OPS = List.of("ask", "status", "cancel", "pending", "decide");

    // access keys of a desk's card
    public static final String DECIDERS = "deciders";
    public static final String QUORUM = "quorum";
    public static final String NOT_BY = "not-by";
    public static final String DECIDE_WITHIN = "decide-within";
    public static final String ON_TIMEOUT = "on-timeout";
    public static final String STEPS = "steps";
    public static final String ASSURANCE = "assurance";
    public static final String HOURS = "hours";
    public static final String INPUT_SCHEMA = "input-schema";
    /** The space a desk takes its asks in; the group's {@value #DEFAULT_SPACE} space when absent. */
    public static final String SPACE = "space";
    public static final String DEFAULT_SPACE = "data";

    // the ask's parameters
    public static final String ACTION = "action";
    public static final String ARGUMENTS = "arguments";
    public static final String ARGS_HASH = "argsHash";
    public static final String QUESTION = "question";
    public static final String SUMMARY = "summary";
    public static final String REQUESTER = "requester";
    public static final String ON_BEHALF_OF = "onBehalfOf";
    public static final String WITHIN = "within";
    public static final String CONTEXT = "context";

    // the decision's keys
    public static final String VERDICT = "verdict";
    public static final String BY = "by";
    public static final String PROOF = "proof";
    public static final String CHANNEL = "channel";
    public static final String REASON = "reason";
    public static final String AMENDED_ARGUMENTS = "amendedArguments";
    public static final String DECIDED_AT = "decidedAt";
    public static final String VALID_UNTIL = "validUntil";

    public static final String APPROVED = "approved";
    public static final String REJECTED = "rejected";
    public static final String AMENDED = "amended";
    public static final String EXPIRED = "expired";
    public static final String CANCELLED = "cancelled";
    public static final List<String> VERDICTS = List.of(APPROVED, REJECTED, AMENDED, EXPIRED, CANCELLED);

    /** How long a decision proves an approval at a source when the desk names no {@code validUntil}. */
    public static final Duration DEFAULT_VALIDITY = Duration.ofHours(1);

    private Hitl() {
    }

    /** Whether a card is a desk's: its shape is {@code hitl:<kind>}. */
    public static boolean isDesk(AssetCard card) {
        return card != null && card.shape() != null && card.shape().startsWith(SHAPE_PREFIX);
    }

    /** The desk's kind from its shape, or empty when the card is not a desk's. */
    public static String kindOf(AssetCard card) {
        return isDesk(card) ? card.shape().substring(SHAPE_PREFIX.length()) : "";
    }

    /** The space the desk takes asks in. */
    public static String spaceOf(AssetCard card) {
        String s = card.access().get(SPACE);
        return s == null || s.isBlank() ? DEFAULT_SPACE : s.strip();
    }

    /** The longest a request waits at this desk, or empty when the card names none or names it badly. */
    public static Optional<Duration> decideWithin(AssetCard card) {
        return duration(card.access().get(DECIDE_WITHIN));
    }

    /**
     * What is wrong with a desk's card, as a reader that relies on it would see it; empty when it is well formed. A
     * reader never refuses an advertisement for these; a requester refuses to ask a desk whose card has problems.
     */
    public static List<String> problems(AssetCard card) {
        List<String> out = new ArrayList<>();
        if (!isDesk(card)) {
            out.add("the card's shape is not hitl:<kind>");
            return out;
        }
        if (!KINDS.contains(kindOf(card))) {
            out.add("the desk's kind is one of " + KINDS + ", not " + kindOf(card));
        }
        List<String> ops = ai.badmonkey.agentspaces.connect.AssetInvocations.invokeOps(card);
        if (!ops.contains("ask")) {
            out.add("the desk's invoke-ops list ask");
        }
        String quorum = card.access().get(QUORUM);
        if (quorum != null && !quorum.isBlank()) {
            try {
                if (Integer.parseInt(quorum.strip()) < 1) {
                    out.add("quorum is at least 1");
                }
            } catch (NumberFormatException e) {
                out.add("quorum is a whole number");
            }
        }
        String within = card.access().get(DECIDE_WITHIN);
        if (within != null && !within.isBlank() && duration(within).isEmpty()) {
            out.add("decide-within is an ISO 8601 duration, such as PT4H");
        }
        return out;
    }

    /**
     * The arguments of a call as text: the {@code arguments} parameter when the call carries its arguments whole (a
     * tool's, v0.1.38), else every other parameter as canonical JSON (keys sorted), without {@code approval}.
     */
    public static String argumentsOf(Map<String, String> params) {
        String whole = params.get(ARGUMENTS);
        if (whole != null) {
            return whole;
        }
        Map<String, String> sorted = new TreeMap<>(params);
        sorted.remove(ApprovalGate.PARAM);
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append(quoted(e.getKey())).append(':').append(quoted(e.getValue()));
        }
        return out.append('}').toString();
    }

    /**
     * The hash that binds a decision to one call: the lower-case hex SHA-256 of {@code <action>#<arguments>}, where the
     * action is {@code <asset>#<operation>} and the arguments are {@link #argumentsOf} the call's parameters.
     */
    public static String argsHash(String action, String arguments) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest((action + "#" + arguments).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A decision as the desk wrote it.
     *
     * @param invocationId the ask it answers
     * @param verdict      one of {@link #VERDICTS}, or {@code refused} when the desk could not take the ask
     * @param by           the deciders, as the channel proved them
     * @param proof        how each was proved
     * @param validUntil   until when a source accepts it as proof
     */
    public record Decision(String invocationId, String verdict, List<String> by, String proof, String channel, String reason,
                           String argsHash, String amendedArguments, Instant decidedAt, Instant validUntil, String error) {

        /** The decision a desk's result carries; a result with {@code ok=false} is a refused ask. */
        public static Decision of(AssetInvocationResult r) {
            Map<String, String> m = r.result();
            if (!r.ok()) {
                return new Decision(r.invocationId(), "refused", List.of(), "", "", m.getOrDefault("detail", ""), "", "", null, null,
                        r.error() == null ? "refused" : r.error());
            }
            String by = m.getOrDefault(BY, "");
            Instant decided = instant(m.get(DECIDED_AT)).orElse(null);
            Instant until = instant(m.get(VALID_UNTIL)).orElse(decided == null ? null : decided.plus(DEFAULT_VALIDITY));
            return new Decision(r.invocationId(), m.getOrDefault(VERDICT, ""), by.isBlank() ? List.of() : Arrays.stream(by.split(",")).map(String::strip).toList(),
                    m.getOrDefault(PROOF, ""), m.getOrDefault(CHANNEL, ""), m.getOrDefault(REASON, ""), m.getOrDefault(ARGS_HASH, ""),
                    m.getOrDefault(AMENDED_ARGUMENTS, ""), decided, until, null);
        }

        /** Whether a person approved: approved, or amended (the call then runs with the amended arguments). */
        public boolean approves() {
            return APPROVED.equals(verdict) || AMENDED.equals(verdict);
        }

        /** Whether this decision lets a call with this arguments' hash run now. */
        public boolean admits(String callHash, Instant now) {
            return approves() && argsHash.equals(callHash) && (validUntil == null || now.isBefore(validUntil));
        }
    }

    static Optional<Duration> duration(String text) {
        if (text == null || text.isBlank()) {
            return Optional.empty();
        }
        try {
            Duration d = Duration.parse(text.strip());
            return d.isNegative() || d.isZero() ? Optional.empty() : Optional.of(d);
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private static Optional<Instant> instant(String text) {
        if (text == null || text.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.parse(text.strip()));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private static String quoted(String s) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : (s == null ? "" : s).toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }
}
