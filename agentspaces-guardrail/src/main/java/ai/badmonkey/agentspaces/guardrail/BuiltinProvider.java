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


package ai.badmonkey.agentspaces.guardrail;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The built-in checks (FR-4, PR-2: compiled patterns, no network, no retained content): {@code prompt-injection}
 * patterns, {@code personal-data} detection for the classes {@code ssn}, {@code card}, {@code email}, {@code phone},
 * {@code iban} (option {@code classes}), {@code terms} with {@code deny} and {@code allow} lists, {@code domains}
 * with {@code deny} and {@code allow} lists of hosts, and {@code schema} against a fragment's field list (options
 * {@code fields}, {@code required}). A hit answers the check's action: {@code redact} replaces each match with
 * {@code [redacted:<class>]}; {@code refuse} and {@code quarantine} name the first match's class.
 */
public final class BuiltinProvider implements GuardrailProvider {

    public static final String NAME = "builtin";
    public static final List<String> KINDS = List.of("prompt-injection", "personal-data", "terms", "domains", "schema");

    private static final List<Pattern> INJECTION = List.of(
            Pattern.compile("ignore (all |any |the )?(previous|prior|above|earlier) (instructions|prompts|messages|rules)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("disregard (your|the|all) (system|previous|prior)? ?(prompt|instructions|rules)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(reveal|print|show|repeat) (your|the) (system|hidden|secret) (prompt|instructions)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("you are now (dan|an? (unfiltered|unrestricted|jailbroken))", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(do anything now|developer mode enabled|jailbreak)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("</?(system|assistant)>", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\[(inst|/inst|system)\\]", Pattern.CASE_INSENSITIVE));

    private static final Map<String, Pattern> PERSONAL = Map.of(
            "ssn", Pattern.compile("\\b(?!000|666|9\\d\\d)\\d{3}-(?!00)\\d{2}-(?!0000)\\d{4}\\b"),
            "card", Pattern.compile("\\b(?:\\d[ -]?){13,19}\\b"),
            "email", Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"),
            "phone", Pattern.compile("(?<!\\d)(?:\\+?\\d{1,3}[ .-]?)?\\(?\\d{3}\\)?[ .-]?\\d{3}[ .-]?\\d{4}(?!\\d)"),
            "iban", Pattern.compile("\\b[A-Z]{2}\\d{2}(?:[ ]?[A-Z0-9]{4}){3,7}(?:[ ]?[A-Z0-9]{1,4})?\\b"));
    private static final Pattern URL_HOST = Pattern.compile("(?i)\\bhttps?://([A-Za-z0-9.-]+)");

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<String> kinds() {
        return KINDS;
    }

    @Override
    public Verdict check(String kind, String content, Map<String, String> context) {
        String action = context.getOrDefault("action", GuardrailPolicy.REFUSE);
        String text = content == null ? "" : content;
        return switch (kind) {
            case "prompt-injection" -> injection(text, action);
            case "personal-data" -> personal(text, action, classes(context.get("classes")));
            case "terms" -> terms(text, action, list(context.get("deny")), list(context.get("allow")));
            case "domains" -> domains(text, action, list(context.get("deny")), list(context.get("allow")));
            case "schema" -> schema(text, action, list(context.get("fields")), list(context.get("required")));
            default -> throw new IllegalArgumentException("builtin provider offers no check '" + kind + "'");
        };
    }

    static Verdict injection(String text, String action) {
        for (Pattern p : INJECTION) {
            Matcher m = p.matcher(text);
            if (m.find()) {
                String reason = "prompt injection: '" + m.group() + "'";
                return GuardrailPolicy.REDACT.equals(action) ? Verdict.redact(replaceAll(text, INJECTION, "[redacted:injection]"), reason) : outcome(action, reason);
            }
        }
        return Verdict.PASS;
    }

    static Verdict personal(String text, String action, List<String> classes) {
        String out = text;
        String first = null;
        for (String cls : classes) {
            Pattern p = PERSONAL.get(cls);
            if (p == null) {
                continue;
            }
            Matcher m = p.matcher(out);
            StringBuilder b = new StringBuilder();
            boolean hit = false;
            while (m.find()) {
                if ("card".equals(cls) && !luhn(m.group())) {
                    continue;
                }
                hit = true;
                if (first == null) {
                    first = cls;
                }
                m.appendReplacement(b, Matcher.quoteReplacement("[redacted:" + cls + "]"));
            }
            if (hit) {
                m.appendTail(b);
                out = b.toString();
            }
        }
        if (first == null) {
            return Verdict.PASS;
        }
        String reason = "personal data: " + first;
        return GuardrailPolicy.REDACT.equals(action) ? Verdict.redact(out, reason) : outcome(action, reason);
    }

    static Verdict terms(String text, String action, List<String> deny, List<String> allow) {
        String lower = text.toLowerCase(Locale.ROOT);
        for (String term : deny) {
            if (!term.isBlank() && lower.contains(term.toLowerCase(Locale.ROOT))) {
                String reason = "denied term: '" + term + "'";
                return GuardrailPolicy.REDACT.equals(action)
                        ? Verdict.redact(Pattern.compile(Pattern.quote(term), Pattern.CASE_INSENSITIVE).matcher(text).replaceAll("[redacted:term]"), reason)
                        : outcome(action, reason);
            }
        }
        if (!allow.isEmpty() && allow.stream().noneMatch(a -> !a.isBlank() && lower.contains(a.toLowerCase(Locale.ROOT)))) {
            return outcome(action, "none of the allowed terms present");
        }
        return Verdict.PASS;
    }

    static Verdict domains(String text, String action, List<String> deny, List<String> allow) {
        Matcher m = URL_HOST.matcher(text);
        while (m.find()) {
            String host = m.group(1).toLowerCase(Locale.ROOT);
            boolean denied = deny.stream().anyMatch(d -> !d.isBlank() && (host.equals(d) || host.endsWith("." + d)));
            boolean allowed = allow.isEmpty() || allow.stream().anyMatch(a -> !a.isBlank() && (host.equals(a) || host.endsWith("." + a)));
            if (denied || !allowed) {
                String reason = "domain " + host + (denied ? " is denied" : " is not allowed");
                return GuardrailPolicy.REDACT.equals(action) ? Verdict.redact(text.replace(m.group(), "[redacted:domain]"), reason) : outcome(action, reason);
            }
        }
        return Verdict.PASS;
    }

    /** A schema check over an entry's JSON: every key in {@code fields} (when given), every {@code required} key present. */
    static Verdict schema(String json, String action, List<String> fields, List<String> required) {
        List<String> keys = topLevelKeys(json);
        if (keys == null) {
            return outcome(action, "not a JSON object");
        }
        for (String r : required) {
            if (!r.isBlank() && !keys.contains(r)) {
                return outcome(action, "missing required field '" + r + "'");
            }
        }
        if (!fields.isEmpty()) {
            for (String k : keys) {
                if (!fields.contains(k)) {
                    return outcome(action, "unknown field '" + k + "'");
                }
            }
        }
        return Verdict.PASS;
    }

    /** The top-level keys of a JSON object text, without a JSON library (the core stays dependency-free). */
    static List<String> topLevelKeys(String json) {
        String s = json == null ? "" : json.trim();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            return null;
        }
        List<String> keys = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        boolean expectKey = true;
        StringBuilder key = new StringBuilder();
        for (int i = 1; i < s.length() - 1; i++) {
            char c = s.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                    if (depth == 0 && expectKey) {
                        key.append(s.charAt(i));
                    }
                } else if (c == '"') {
                    inString = false;
                    if (depth == 0 && expectKey) {
                        keys.add(key.toString());
                        key.setLength(0);
                        expectKey = false;
                    }
                } else if (depth == 0 && expectKey) {
                    key.append(c);
                }
                continue;
            }
            switch (c) {
                case '"' -> inString = true;
                case '{', '[' -> depth++;
                case '}', ']' -> depth--;
                case ',' -> {
                    if (depth == 0) {
                        expectKey = true;
                    }
                }
                default -> {
                }
            }
        }
        return keys;
    }

    static Verdict outcome(String action, String reason) {
        return switch (action) {
            case GuardrailPolicy.QUARANTINE -> Verdict.quarantine(reason);
            case GuardrailPolicy.FINDING, GuardrailPolicy.PASS -> Verdict.PASS;
            default -> Verdict.refuse(reason);
        };
    }

    static String replaceAll(String text, List<Pattern> patterns, String replacement) {
        String out = text;
        for (Pattern p : patterns) {
            out = p.matcher(out).replaceAll(Matcher.quoteReplacement(replacement));
        }
        return out;
    }

    static boolean luhn(String digits) {
        String d = digits.replaceAll("[^0-9]", "");
        if (d.length() < 13 || d.length() > 19) {
            return false;
        }
        int sum = 0;
        boolean alt = false;
        for (int i = d.length() - 1; i >= 0; i--) {
            int n = d.charAt(i) - '0';
            if (alt) {
                n *= 2;
                if (n > 9) {
                    n -= 9;
                }
            }
            sum += n;
            alt = !alt;
        }
        return sum % 10 == 0;
    }

    static List<String> classes(String option) {
        List<String> l = list(option);
        return l.isEmpty() ? List.of("ssn", "card") : l;
    }

    static List<String> list(String option) {
        if (option == null || option.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String s : option.split(",")) {
            if (!s.trim().isEmpty()) {
                out.add(s.trim());
            }
        }
        return out;
    }
}
