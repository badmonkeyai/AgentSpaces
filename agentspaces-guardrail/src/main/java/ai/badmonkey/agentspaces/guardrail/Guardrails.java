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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The one evaluation path (FR-5): the policy's checks at {@code where}, in order, each against its provider under
 * the timeout; a {@code finding} action records without changing the outcome; a {@code redact} rewrites the
 * content the later checks and the caller see; a {@code refuse} or {@code quarantine} stops the walk. A provider
 * that throws or exceeds the timeout yields the policy's {@code onError}: {@code refuse} stops with a finding
 * naming the error, {@code pass} records the finding and continues.
 */
public final class Guardrails {

    /** What the caller does: the (possibly redacted) content, the findings, and whether the content was stopped. */
    public record Outcome(String content, List<GuardrailFinding> findings, boolean stopped, String stoppedBy, String stopAction) {
        public boolean passed() {
            return !stopped;
        }

        public boolean quarantined() {
            return stopped && GuardrailPolicy.QUARANTINE.equals(stopAction);
        }
    }

    private static final ExecutorService CHECKS = Executors.newVirtualThreadPerTaskExecutor();

    private Guardrails() {
    }

    public static Outcome apply(GuardrailPolicy policy, String where, String subject, String content, List<GuardrailProvider> providers,
                                Duration timeout, Instant now) {
        Map<String, GuardrailProvider> byName = new LinkedHashMap<>();
        for (GuardrailProvider p : providers) {
            byName.putIfAbsent(p.name(), p);
        }
        String current = content == null ? "" : content;
        List<GuardrailFinding> findings = new ArrayList<>();
        if (policy == null) {
            return new Outcome(current, findings, false, "", "");
        }
        for (GuardrailPolicy.Check check : policy.at(where)) {
            String name = check.provider() + "/" + check.kind();
            GuardrailProvider provider = byName.get(check.provider());
            Verdict verdict;
            if (provider == null) {
                verdict = onError(policy, "no provider '" + check.provider() + "'");
                findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, verdict.action(), "no provider " + check.provider(), now));
                if (!verdict.passes()) {
                    return new Outcome(current, findings, true, name, verdict.action());
                }
                continue;
            }
            Map<String, String> context = new LinkedHashMap<>(check.options());
            context.put("action", check.action());
            context.put("where", where);
            context.put("subject", subject);
            String input = current;
            Future<Verdict> future = CHECKS.submit(() -> provider.check(check.kind(), input, context));
            try {
                verdict = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                verdict = onError(policy, "timed out after " + timeout);
                findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, verdict.action(), "provider timed out after " + timeout, now));
                if (!verdict.passes()) {
                    return new Outcome(current, findings, true, name, verdict.action());
                }
                continue;
            } catch (ExecutionException e) {
                verdict = onError(policy, String.valueOf(e.getCause()));
                findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, verdict.action(), "provider failed: " + e.getCause(), now));
                if (!verdict.passes()) {
                    return new Outcome(current, findings, true, name, verdict.action());
                }
                continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new Outcome(current, findings, true, name, GuardrailPolicy.REFUSE);
            }
            switch (verdict) {
                case Verdict.Pass p -> {
                }
                case Verdict.Redact r -> {
                    current = r.content();
                    findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, GuardrailPolicy.REDACT, r.reason(), now));
                }
                case Verdict.Refuse r -> {
                    findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, GuardrailPolicy.REFUSE, r.reason(), now));
                    return new Outcome(current, findings, true, name, GuardrailPolicy.REFUSE);
                }
                case Verdict.Quarantine q -> {
                    findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, GuardrailPolicy.QUARANTINE, q.reason(), now));
                    return new Outcome(current, findings, true, name, GuardrailPolicy.QUARANTINE);
                }
            }
            // a check whose action is "finding" passes the content on: the provider answered pass for it (BuiltinProvider.outcome)
            if (GuardrailPolicy.FINDING.equals(check.action()) && verdict.passes()) {
                String found = hit(provider, check, input, context);
                if (found != null) {
                    findings.add(new GuardrailFinding(policy.group(), policy.version(), name, where, subject, GuardrailPolicy.FINDING, found, now));
                }
            }
        }
        return new Outcome(current, findings, false, "", "");
    }

    /**
     * A {@code finding} check re-asks the provider with action refuse to learn whether it hit (the walk never stops on
     * it): the reason the provider gives, so the finding names what it found (spiral 6, S6-X-02: a terms check that
     * logs names the term), or null when it did not hit.
     */
    private static String hit(GuardrailProvider provider, GuardrailPolicy.Check check, String input, Map<String, String> context) {
        try {
            Map<String, String> probe = new LinkedHashMap<>(context);
            probe.put("action", GuardrailPolicy.REFUSE);
            return switch (provider.check(check.kind(), input, probe)) {
                case Verdict.Pass p -> null;
                case Verdict.Refuse r -> r.reason();
                case Verdict.Redact r -> r.reason();
                case Verdict.Quarantine q -> q.reason();
            };
        } catch (RuntimeException e) {
            return null;
        }
    }

    static Verdict onError(GuardrailPolicy policy, String detail) {
        return GuardrailPolicy.PASS.equals(policy.onError()) ? Verdict.PASS : Verdict.refuse("guardrail error: " + detail);
    }
}
