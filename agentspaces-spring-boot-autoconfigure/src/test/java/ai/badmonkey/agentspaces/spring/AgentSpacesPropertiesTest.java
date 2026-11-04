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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The property blocks bind and read back as plain beans (what Boot's binder does from YAML). */
class AgentSpacesPropertiesTest {

    @Test
    void theRulesAndFlowsBlocksDefaultOffAndBind() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        assertThat(p.getRules().isGate()).isFalse();
        assertThat(p.getFlows().isReconcile()).isFalse();
        p.getRules().setGate(true);
        p.getRules().setSpace("rules");
        p.getRules().setCacheKeys(500);
        p.getFlows().setReconcile(true);
        p.getFlows().setSpace("flows");
        assertThat(p.getRules().isGate()).isTrue();
        assertThat(p.getRules().getSpace()).isEqualTo("rules");
        assertThat(p.getRules().getCacheKeys()).isEqualTo(500);
        assertThat(p.getFlows().isReconcile()).isTrue();
        assertThat(p.getFlows().getSpace()).isEqualTo("flows");
    }

    @Test
    void theEmbabelA2aAndGrantBlocksBind() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        AgentSpacesProperties.Embabel e = p.getEmbabel();
        e.setRemoteActionsEnabled(true);
        e.setAutoDeploy(true);
        e.setDeployPollMillis(250);
        e.setRemoteActionTimeoutMillis(9000);
        e.setRemoteAgentName("remote");
        e.setRemoteAgentDescription("the fleet's actions");
        assertThat(e.isRemoteActionsEnabled()).isTrue();
        assertThat(e.isAutoDeploy()).isTrue();
        assertThat(e.getDeployPollMillis()).isEqualTo(250);
        assertThat(e.getRemoteActionTimeoutMillis()).isEqualTo(9000);
        assertThat(e.getRemoteAgentName()).isEqualTo("remote");
        assertThat(e.getRemoteAgentDescription()).isEqualTo("the fleet's actions");
        AgentSpacesProperties.A2a a = p.getA2a();
        a.setEnabled(true);
        a.setBind("127.0.0.1");
        a.setPort(8081);
        a.setGroup("g");
        a.setSpace("a2a");
        a.setFleetName("fleet");
        a.setProvider("acme");
        a.setExternalBaseUrl("https://a2a.example");
        a.setRequiredScope("a2a:invoke");
        a.setToken("t");
        a.setAllowedWebhookHosts(List.of("hooks.example"));
        a.setTaskLeaseMillis(1000);
        a.setResultLeaseMillis(2000);
        assertThat(a.isEnabled()).isTrue();
        assertThat(a.getBind()).isEqualTo("127.0.0.1");
        assertThat(a.getPort()).isEqualTo(8081);
        assertThat(a.getGroup()).isEqualTo("g");
        assertThat(a.getSpace()).isEqualTo("a2a");
        assertThat(a.getFleetName()).isEqualTo("fleet");
        assertThat(a.getProvider()).isEqualTo("acme");
        assertThat(a.getExternalBaseUrl()).isEqualTo("https://a2a.example");
        assertThat(a.getRequiredScope()).isEqualTo("a2a:invoke");
        assertThat(a.getToken()).isEqualTo("t");
        assertThat(a.getAllowedWebhookHosts()).containsExactly("hooks.example");
        assertThat(a.getTaskLeaseMillis()).isEqualTo(1000);
        assertThat(a.getResultLeaseMillis()).isEqualTo(2000);
        AgentSpacesProperties.Security.Grants g = p.getSecurity().getGrants();
        g.setRaftVoter(List.of("a"));
        g.setRaftClient(List.of("b"));
        g.setDirectiveIssuer(List.of("c"));
        g.setConnectorServe(List.of("d"));
        g.setKeyHolder(List.of("e"));
        g.setKeyRotator(List.of("f"));
        g.setVote(List.of("g"));
        g.setModelServe(List.of("h"));
        g.setContentFetch(List.of("i"));
        g.setSpaceWrite(List.of("j"));
        g.setSpaceTake(List.of("k"));
        assertThat(g.getRaftVoter()).containsExactly("a");
        assertThat(g.getRaftClient()).containsExactly("b");
        assertThat(g.getDirectiveIssuer()).containsExactly("c");
        assertThat(g.getConnectorServe()).containsExactly("d");
        assertThat(g.getKeyHolder()).containsExactly("e");
        assertThat(g.getKeyRotator()).containsExactly("f");
        assertThat(g.getVote()).containsExactly("g");
        assertThat(g.getModelServe()).containsExactly("h");
        assertThat(g.getContentFetch()).containsExactly("i");
        assertThat(g.getSpaceWrite()).containsExactly("j");
        assertThat(g.getSpaceTake()).containsExactly("k");
    }

    @Test
    void theContentSwarmAndTransportBlocksBind() {
        AgentSpacesProperties p = new AgentSpacesProperties();
        AgentSpacesProperties.ContentSwarm c = p.getContent();
        c.setEnabled(true);
        c.setSpace("content");
        c.setEscalateAbove(1024);
        c.setPieceSize(4096);
        c.setSeedMode("eager");
        c.setHoldingLease(Duration.ofHours(1));
        c.setMaxSessions(4);
        c.setMaxWindow(8);
        c.setMaxFetches(2);
        c.setUploadBytesPerDay(10);
        c.setDownloadBytesPerDay(20);
        c.setReplicateManifests(true);
        AgentSpacesProperties.ContentSwarm.Store store = new AgentSpacesProperties.ContentSwarm.Store();
        store.setEncryptAtRest(true);
        store.setBackends(List.of("file"));
        store.setPath("/tmp/blocks");
        store.setQuota(99);
        c.setStore(store);
        assertThat(c.isEnabled()).isTrue();
        assertThat(c.getSpace()).isEqualTo("content");
        assertThat(c.getEscalateAbove()).isEqualTo(1024);
        assertThat(c.getPieceSize()).isEqualTo(4096);
        assertThat(c.getSeedMode()).isEqualTo("eager");
        assertThat(c.getHoldingLease()).isEqualTo(Duration.ofHours(1));
        assertThat(c.getMaxSessions()).isEqualTo(4);
        assertThat(c.getMaxWindow()).isEqualTo(8);
        assertThat(c.getMaxFetches()).isEqualTo(2);
        assertThat(c.getUploadBytesPerDay()).isEqualTo(10);
        assertThat(c.getDownloadBytesPerDay()).isEqualTo(20);
        assertThat(c.isReplicateManifests()).isTrue();
        assertThat(c.getStore().isEncryptAtRest()).isTrue();
        assertThat(c.getStore().getBackends()).containsExactly("file");
        assertThat(c.getStore().getPath()).isEqualTo("/tmp/blocks");
        assertThat(c.getStore().getQuota()).isEqualTo(99);
        AgentSpacesProperties.Transport t = p.getTransport();
        AgentSpacesProperties.Transport.Http http = new AgentSpacesProperties.Transport.Http();
        http.setPath("/aspace");
        http.setAllowedOrigins(List.of("https://app.example"));
        http.setIdleTimeout(Duration.ofSeconds(30));
        http.setPingInterval(Duration.ofSeconds(10));
        http.setLongPoll(Duration.ofSeconds(20));
        http.setOpensPerSourcePerSecond(5);
        t.setWs(http);
        t.setWss(http);
        t.setHttp(http);
        t.setHttps(http);
        t.setChannelAuth("SIGNED");
        AgentSpacesProperties.Transport.Tls tls = new AgentSpacesProperties.Transport.Tls();
        tls.setEnabled(true);
        tls.setKeyStore("ks.p12");
        tls.setKeyStorePassword("ks");
        tls.setTrustStore("ts.p12");
        tls.setTrustStorePassword("ts");
        tls.setCrls(List.of("crl.pem"));
        tls.setRevocationStrict(true);
        tls.setRequireAttestation(true);
        tls.setRevocationValidator("crl");
        tls.setCrlRefresh(Duration.ofMinutes(5));
        t.setTls(tls);
        t.setWebrtc(t.getWebrtc());
        t.setUdp(t.getUdp());
        t.setQuic(t.getQuic());
        t.setDtls(t.getDtls());
        assertThat(t.getWs().getPath()).isEqualTo("/aspace");
        assertThat(t.getWss().getAllowedOrigins()).containsExactly("https://app.example");
        assertThat(t.getHttp().getIdleTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(t.getHttps().getPingInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(http.getLongPoll()).isEqualTo(Duration.ofSeconds(20));
        assertThat(http.getOpensPerSourcePerSecond()).isEqualTo(5);
        assertThat(t.getChannelAuth()).isEqualTo("SIGNED");
        assertThat(t.getTls().getEnabled()).isTrue();
        assertThat(t.getTls().getKeyStore()).isEqualTo("ks.p12");
        assertThat(t.getTls().getKeyStorePassword()).isEqualTo("ks");
        assertThat(t.getTls().getTrustStore()).isEqualTo("ts.p12");
        assertThat(t.getTls().getTrustStorePassword()).isEqualTo("ts");
        assertThat(t.getTls().getCrls()).containsExactly("crl.pem");
        assertThat(t.getTls().isRevocationStrict()).isTrue();
        assertThat(t.getTls().isRequireAttestation()).isTrue();
        assertThat(t.getTls().getRevocationValidator()).isEqualTo("crl");
        assertThat(t.getTls().getCrlRefresh()).isEqualTo(Duration.ofMinutes(5));
    }
}
