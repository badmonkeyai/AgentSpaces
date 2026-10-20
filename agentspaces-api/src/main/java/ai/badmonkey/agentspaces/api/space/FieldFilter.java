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
package ai.badmonkey.agentspaces.api.space;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * The portable field filter grammar (SPEC §7.2; srad/agentspaces-js.md
 * JS-V-03), one clause per string: {@code field op literal}, where {@code op}
 * is one of {@code =}, {@code !=}, {@code <}, {@code <=}, {@code >},
 * {@code >=}, or {@code in} (a comma-separated list). A clause is judged
 * against the field's value as the Java, Python, and TypeScript clients all
 * judge it: when both the value's string form and the literal parse as
 * decimal numbers the comparison is numeric (so {@code 3.0} equals
 * {@code 3}), otherwise it is the string form's lexicographic order
 * ({@code Comparable} on the string); a null value matches {@code !=} alone;
 * {@code in} matches when any trimmed element equals the value. The golden
 * vector {@code filter_grammar_cases} pins the three implementations.
 *
 * @param field   the field name
 * @param op      the operator
 * @param literal the literal text (for {@code in}, the comma-separated list)
 */
public record FieldFilter(String field, String op, String literal) {

    /** The comparison operators. */
    private static final List<String> OPS = List.of("!=", "<=", ">=", "=", "<", ">");

    /** Every operator as it appears in a clause; {@code in} needs its spaces. */
    private static final List<String> OPERATORS = List.of("!=", "<=", ">=", "=", "<", ">", " in ");

    public FieldFilter {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(literal, "literal");
        if (field.isBlank()) {
            throw new IllegalArgumentException("a field filter needs a field name");
        }
        if (!OPS.contains(op) && !op.equals("in")) {
            throw new IllegalArgumentException("unknown field filter operator '" + op + "'");
        }
    }

    /**
     * Parses one clause: {@code field op literal}, spaces around the operator
     * allowed, or {@code field in a,b,c}.
     *
     * @param clause the clause text
     * @return the filter
     * @throws IllegalArgumentException when the clause has no operator or no field
     */
    public static FieldFilter parse(String clause) {
        Objects.requireNonNull(clause, "clause");
        String text = clause.trim();
        // The earliest operator in the clause is the operator; at a tie the longest
        // (so "<=" beats "<"), and "!=x" is malformed rather than the field "!".
        int best = -1;
        String bestOp = null;
        for (String op : OPERATORS) {
            int at = text.indexOf(op);
            if (at >= 0 && (best < 0 || at < best || (at == best && op.length() > bestOp.length()))) {
                best = at;
                bestOp = op;
            }
        }
        if (best <= 0) {
            throw new IllegalArgumentException("malformed field filter '" + clause
                    + "'; use \"field=value\", \"field!=value\", \"field<value\", \"field<=value\", \"field>value\","
                    + " \"field>=value\", or \"field in a,b,c\"");
        }
        return new FieldFilter(text.substring(0, best).trim(), bestOp.trim(),
                text.substring(best + bestOp.length()).trim());
    }

    /** Whether the value satisfies this clause. */
    public boolean matches(Object value) {
        if (value == null) {
            return op.equals("!=");
        }
        String form = String.valueOf(value);
        return switch (op) {
            case "=" -> compare(form, literal) == 0;
            case "!=" -> compare(form, literal) != 0;
            case "<" -> compare(form, literal) < 0;
            case "<=" -> compare(form, literal) <= 0;
            case ">" -> compare(form, literal) > 0;
            case ">=" -> compare(form, literal) >= 0;
            default -> {
                for (String element : literal.split(",", -1)) {
                    if (compare(form, element.trim()) == 0) {
                        yield true;
                    }
                }
                yield false;
            }
        };
    }

    /** Numeric when both sides are decimal numbers, else the strings' order. */
    static int compare(String form, String literal) {
        BigDecimal left = number(form);
        BigDecimal right = number(literal);
        if (left != null && right != null) {
            return left.compareTo(right);
        }
        return Integer.signum(form.compareTo(literal));
    }

    private static BigDecimal number(String text) {
        String trimmed = text.trim();
        if (trimmed.isEmpty() || !trimmed.matches("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?")) {
            return null;
        }
        try {
            return new BigDecimal(trimmed.toLowerCase(Locale.ROOT));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** This clause as a {@link Matcher}. */
    public Matcher matcher() {
        return this::matches;
    }

    @Override
    public String toString() {
        return field + (op.equals("in") ? " in " : op) + literal;
    }
}
