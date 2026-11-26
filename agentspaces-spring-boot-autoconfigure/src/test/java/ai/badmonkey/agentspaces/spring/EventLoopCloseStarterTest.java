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

package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.identity.SignallingChannelTrust;
import ai.badmonkey.agentspaces.test.TestCa;
import ai.badmonkey.agentspaces.test.VirtualTime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The starter's part of the event-loop program's close (srad/agentspaces-event-loop.md §10.35): the 0.6.0 signature of
 * {@code getQuic()}, the CA trust's CRL refresh on the node's time source (EL-C-06, EL-L-14), the per-process watchdog
 * and loop-pool bounds as properties (EL-O-10, I-08), the one log of a non-default {@code tick-millis} and the
 * property metadata (EL-Q-MNT-02).
 */
class EventLoopCloseStarterTest {

    @Test
    void getQuicKeepsItsZeroSixSignatureAndQuicBlockReturnsTheSameBlock() throws Exception {
        java.lang.reflect.Method old = AgentSpacesProperties.Transport.class.getMethod("getQuic");
        assertThat(old.getReturnType()).as("code compiled against 0.6.0 links to Listener getQuic()")
                .isEqualTo(AgentSpacesProperties.Transport.Listener.class);
        assertThat(old.isAnnotationPresent(Deprecated.class)).isTrue();
        AgentSpacesProperties.Transport t = new AgentSpacesProperties().getTransport();
        t.quicBlock().setDialTimeout(Duration.ofSeconds(3));
        assertThat(t.quicBlock()).isSameAs(t.getQuic());
        assertThat(((AgentSpacesProperties.Transport.Quic) t.getQuic()).getDialTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(AgentSpacesProperties.Transport.class.getMethod("setQuic", AgentSpacesProperties.Transport.Quic.class))
                .as("one setter, typed Quic, so a binder binds the dial timeout").isNotNull();
    }

    @Test
    void theCaTrustRefreshesItsCrlsOnTheNodesTimeSourceAndSignalsTheChange(@TempDir Path dir) throws Exception {
        TestCa ca = TestCa.create();
        Path trustStore = dir.resolve("trust.p12");
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setCertificateEntry("ca", ca.certificate());
        try (OutputStream out = Files.newOutputStream(trustStore)) {
            store.store(out, "changeit".toCharArray());
        }
        Path crl = dir.resolve("ca.crl");
        Files.write(crl, ca.crl().getEncoded());
        AgentSpacesProperties.Transport.Tls tls = new AgentSpacesProperties().getTransport().getTls();
        tls.setTrustStore(trustStore.toString());
        tls.setTrustStorePassword("changeit");
        tls.setCrls(List.of(crl.toString()));
        tls.setCrlRefresh(Duration.ofMinutes(5));
        VirtualTime time = VirtualTime.create();
        var trust = AgentSpacesAutoConfiguration.caTrust(tls, time);
        assertThat(trust).isInstanceOf(SignallingChannelTrust.class);
        AtomicInteger changes = new AtomicInteger();
        try (AutoCloseable listening = ((SignallingChannelTrust) trust).onChange(changes::incrementAndGet)) {
            TestCa.Issued leaf = ca.issue(ai.badmonkey.agentspaces.identity.PeerIdentity.generate().rawPublicKey());
            Files.write(crl, ca.crl(leaf.certificate()).getEncoded());
            time.advance(Duration.ofMinutes(5));
            assertThat(changes.get()).as("the refresh ran on the node's (virtual) time and told the node").isEqualTo(1);
        }
    }

    @Test
    void theWatchdogAndLoopBoundsApplyToTheProcessesTimeSource() {
        SystemTime time = SystemTime.isolated("el-close-bounds");
        AgentSpacesProperties p = new AgentSpacesProperties();
        p.getWatchdog().setPeriodMillis(3_000L);
        p.getWatchdog().setEscalateAfter(5);
        p.getLoop().setPoolMax(3);
        p.getLoop().setStretchCap(2);
        Duration graceBefore = time.watchdog().options().grace();
        AgentSpacesAutoConfiguration.applyProcessBounds(p, time);
        assertThat(time.watchdog().options().period()).isEqualTo(Duration.ofSeconds(3));
        assertThat(time.watchdog().options().escalateAfter()).isEqualTo(5);
        assertThat(time.watchdog().options().grace()).as("an unset key keeps the source's bound").isEqualTo(graceBefore);
        assertThat(time.loopPool().options().max()).isEqualTo(3);
        assertThat(time.loopPool().options().stretchCap()).isEqualTo(2);
        p.getWatchdog().setEscalateAfter(0);
        assertThatThrownBy(() -> AgentSpacesAutoConfiguration.applyProcessBounds(p, time))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("agentspaces.watchdog");
    }

    @Test
    void aNonDefaultTickMillisIsExplainedOncePerProcess() {
        AgentSpacesAutoConfiguration.resetTickMillisNote();
        AgentSpacesProperties p = new AgentSpacesProperties();
        assertThat(AgentSpacesAutoConfiguration.noteTickMillis(p)).as("the default says nothing").isFalse();
        p.setTickMillis(50);
        assertThat(AgentSpacesAutoConfiguration.noteTickMillis(p)).isTrue();
        assertThat(AgentSpacesAutoConfiguration.noteTickMillis(p)).as("once per process").isFalse();
    }

    @Test
    void theMetadataNamesEveryLoopWatchdogTtlAndTickKey() throws Exception {
        Set<String> names = new TreeSet<>();
        try (InputStream in = getClass().getResourceAsStream("/META-INF/additional-spring-configuration-metadata.json")) {
            for (JsonNode p : new ObjectMapper().readTree(in).get("properties")) {
                names.add(p.get("name").asText());
            }
        }
        assertThat(names).contains("agentspaces.tick-millis", "agentspaces.coalesce-millis",
                "agentspaces.ttl.strict", "agentspaces.ttl.expert-overrides",
                "agentspaces.watchdog.escalation", "agentspaces.watchdog.hold-limit",
                "agentspaces.watchdog.connector-hold-limit", "agentspaces.watchdog.period-millis",
                "agentspaces.watchdog.grace-millis", "agentspaces.watchdog.slow-millis",
                "agentspaces.watchdog.wedge-millis", "agentspaces.watchdog.escalate-after",
                "agentspaces.watchdog.escalate-window-millis", "agentspaces.watchdog.fence-limit-millis",
                "agentspaces.loop.pool-max", "agentspaces.loop.pool-idle-millis",
                "agentspaces.loop.grow-lateness-millis", "agentspaces.loop.stretch-lateness-millis",
                "agentspaces.loop.stretch-cap");
        // membership standing (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md): retirement, the last resort, readiness
        assertThat(names).contains("agentspaces.retirement.enabled", "agentspaces.retirement.after",
                "agentspaces.post-retirement.enabled", "agentspaces.health.refuse-traffic-when-isolated");
    }

    @Test
    void everyTtlTheFloorChecksIsAStarterProperty() {
        // the keys are properties of this starter: each has a getter path on AgentSpacesProperties (§10.35, EL-A-04)
        for (AgentSpacesAutoConfiguration.TtlCheck c : AgentSpacesAutoConfiguration.ttlChecks(new AgentSpacesProperties())) {
            Object at = new AgentSpacesProperties();
            String[] parts = c.key().substring("agentspaces.".length()).split("\\.");
            for (String part : parts) {
                String getter = "get" + java.util.Arrays.stream(part.split("-"))
                        .map(w -> Character.toUpperCase(w.charAt(0)) + w.substring(1))
                        .reduce("", String::concat);
                try {
                    at = at.getClass().getMethod(getter).invoke(at);
                } catch (ReflectiveOperationException e) {
                    throw new AssertionError(c.key() + " is not a property of the starter (no " + getter + ")", e);
                }
            }
        }
    }
}
