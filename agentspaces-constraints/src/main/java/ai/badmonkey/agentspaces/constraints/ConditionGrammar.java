/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.constraints;

import ai.badmonkey.agentspaces.api.space.FieldFilter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The condition grammar (ISSUE-Constraints FR-5, §9.2): a {@code FieldFilter} guard over one type's fields and
 * tags plus four key-level predicates, joined by {@code and}, {@code or}, {@code not}, with parentheses.
 *
 * <pre>
 * condition := term (("and" | "or") term)* | "not" term | "(" condition ")"
 * term      := "exists(" T guard? ")" | "count(" T guard? ")" op integer | "newest(" T ")." field op literal
 *            | "age(" T ")" ">" duration
 * guard     := "where" clause ("," clause)*
 * </pre>
 *
 * Parsing is total: malformed text is refused with the text in the message.
 */
public final class ConditionGrammar {

    /** A parsed condition, evaluated over one key's events at a moment. */
    public sealed interface Condition permits And, Or, Not, Exists, Count, Newest, Age {
        boolean evaluate(List<Event> events, Instant now);
    }

    public record And(Condition left, Condition right) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            return left.evaluate(events, now) && right.evaluate(events, now);
        }
    }

    public record Or(Condition left, Condition right) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            return left.evaluate(events, now) || right.evaluate(events, now);
        }
    }

    public record Not(Condition inner) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            return !inner.evaluate(events, now);
        }
    }

    public record Exists(String type, List<FieldFilter> guard) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            return events.stream().anyMatch(e -> Guards.matches(e, type, guard));
        }
    }

    public record Count(String type, List<FieldFilter> guard, String op, long n) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            long count = events.stream().filter(e -> Guards.matches(e, type, guard)).count();
            return Guards.compare(count, op, n);
        }
    }

    public record Newest(String type, String field, String op, String literal) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            Event newest = null;
            for (Event e : events) {
                if (Guards.sameType(e, type) && (newest == null || !e.issued().isBefore(newest.issued()))) {
                    newest = e;
                }
            }
            if (newest == null) {
                return false;
            }
            Object v = newest.fields().get(field);
            if (v == null) {
                v = newest.tags().get(field);
            }
            return new FieldFilter(field, op, literal).matches(v);
        }
    }

    public record Age(String type, Duration olderThan) implements Condition {
        @Override
        public boolean evaluate(List<Event> events, Instant now) {
            return events.stream().filter(e -> Guards.sameType(e, type))
                    .anyMatch(e -> Duration.between(e.issued(), now).compareTo(olderThan) > 0);
        }
    }

    private ConditionGrammar() {
    }

    /** Parses one condition; the message of a refusal carries the text. */
    public static Condition parse(String text) {
        Objects.requireNonNull(text, "text");
        Parser p = new Parser(text);
        Condition c = p.condition();
        p.end();
        return c;
    }

    /** Evaluates every named condition of a vocabulary; a condition that fails to parse evaluates to false. */
    public static Map<String, Boolean> evaluateAll(Map<String, String> conditions, List<Event> events, Instant now) {
        Map<String, Boolean> out = new java.util.LinkedHashMap<>();
        conditions.forEach((id, text) -> {
            try {
                out.put(id, parse(text).evaluate(events, now));
            } catch (RuntimeException e) {
                out.put(id, false);
            }
        });
        return out;
    }

    private static final class Parser {
        private final String text;
        private int at;

        Parser(String text) {
            this.text = text;
        }

        Condition condition() {
            Condition left = unary();
            while (true) {
                skip();
                if (word("and")) {
                    left = new And(left, unary());
                } else if (word("or")) {
                    left = new Or(left, unary());
                } else {
                    return left;
                }
            }
        }

        private Condition unary() {
            skip();
            if (word("not")) {
                return new Not(unary());
            }
            if (peek('(')) {
                at++;
                Condition inner = condition();
                expect(')');
                return inner;
            }
            return term();
        }

        private Condition term() {
            skip();
            if (word("exists")) {
                expect('(');
                String type = name();
                List<FieldFilter> guard = guard();
                expect(')');
                return new Exists(type, guard);
            }
            if (word("count")) {
                expect('(');
                String type = name();
                List<FieldFilter> guard = guard();
                expect(')');
                String op = op();
                return new Count(type, guard, op, Long.parseLong(number()));
            }
            if (word("newest")) {
                expect('(');
                String type = name();
                expect(')');
                expect('.');
                String field = name();
                String op = op();
                return new Newest(type, field, op, literal());
            }
            if (word("age")) {
                expect('(');
                String type = name();
                expect(')');
                String op = op();
                if (!op.equals(">")) {
                    throw refuse("age() takes '>' and a duration");
                }
                String dur = literal();
                try {
                    return new Age(type, Duration.parse(dur.toUpperCase(java.util.Locale.ROOT)));
                } catch (java.time.format.DateTimeParseException e) {
                    throw refuse("'" + dur + "' is not an ISO duration");
                }
            }
            throw refuse("expected exists(...), count(...), newest(...), age(...), not, or '('");
        }

        private List<FieldFilter> guard() {
            skip();
            if (!word("where")) {
                return List.of();
            }
            List<FieldFilter> out = new ArrayList<>();
            while (true) {
                skip();
                int start = at;
                int depth = 0;
                while (at < text.length()) {
                    char c = text.charAt(at);
                    if (c == '(') {
                        depth++;
                    } else if (c == ')') {
                        if (depth == 0) {
                            break;
                        }
                        depth--;
                    } else if (c == ',' && depth == 0) {
                        break;
                    }
                    at++;
                }
                String clause = text.substring(start, at).trim();
                if (clause.isEmpty()) {
                    throw refuse("an empty guard clause");
                }
                try {
                    out.add(FieldFilter.parse(clause));
                } catch (IllegalArgumentException e) {
                    throw refuse(e.getMessage());
                }
                skip();
                if (peek(',')) {
                    at++;
                    continue;
                }
                return out;
            }
        }

        private String name() {
            skip();
            int start = at;
            while (at < text.length() && (Character.isLetterOrDigit(text.charAt(at)) || "_.$#:/".indexOf(text.charAt(at)) >= 0)) {
                at++;
            }
            if (start == at) {
                throw refuse("expected a name");
            }
            return text.substring(start, at);
        }

        private String op() {
            skip();
            for (String op : List.of("!=", "<=", ">=", "=", "<", ">")) {
                if (text.startsWith(op, at)) {
                    at += op.length();
                    return op;
                }
            }
            throw refuse("expected an operator");
        }

        private String number() {
            skip();
            int start = at;
            while (at < text.length() && Character.isDigit(text.charAt(at))) {
                at++;
            }
            if (start == at) {
                throw refuse("expected a number");
            }
            return text.substring(start, at);
        }

        private String literal() {
            skip();
            if (peek('"') || peek('\'')) {
                char q = text.charAt(at++);
                int start = at;
                while (at < text.length() && text.charAt(at) != q) {
                    at++;
                }
                if (at >= text.length()) {
                    throw refuse("an unterminated literal");
                }
                return text.substring(start, at++);
            }
            int start = at;
            while (at < text.length() && !Character.isWhitespace(text.charAt(at)) && ")".indexOf(text.charAt(at)) < 0) {
                at++;
            }
            if (start == at) {
                throw refuse("expected a value");
            }
            return text.substring(start, at);
        }

        private boolean word(String w) {
            skip();
            if (text.regionMatches(true, at, w, 0, w.length())) {
                int end = at + w.length();
                if (end == text.length() || !Character.isLetterOrDigit(text.charAt(end))) {
                    at = end;
                    return true;
                }
            }
            return false;
        }

        private boolean peek(char c) {
            skip();
            return at < text.length() && text.charAt(at) == c;
        }

        private void expect(char c) {
            if (!peek(c)) {
                throw refuse("expected '" + c + "'");
            }
            at++;
        }

        private void skip() {
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
        }

        void end() {
            skip();
            if (at < text.length()) {
                throw refuse("unexpected '" + text.substring(at) + "'");
            }
        }

        private IllegalArgumentException refuse(String why) {
            return new IllegalArgumentException("malformed condition '" + text + "' at " + at + ": " + why);
        }
    }
}
