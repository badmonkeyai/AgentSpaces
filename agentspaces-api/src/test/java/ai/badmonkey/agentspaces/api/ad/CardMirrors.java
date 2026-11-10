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
package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Unchecked mirrors of the v0.1.32 card records (AppCard origin and formats, 2026-11-09): the same components in the
 * same order with no validation, so a test encodes a card the real records refuse and checks that the strict reader
 * refuses its bytes.
 */
final class CardMirrors {

    private CardMirrors() {
    }

    record RawOrigin(String system, String ref, String version, @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, String> attrs) {
    }

    record RawHost(String url, int port, String hostAgent, String path, String signOn, String publishedBy, Instant publishedAt, String status) {
    }

    record RawSpill(String root, long size, List<String> parts) {
    }

    record RawAppCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl, String name, String kind, String shape,
                      String owner, List<String> reads, List<String> actions, String idpClient, String url, String pkg, String version,
                      List<String> views,
                      @JsonInclude(JsonInclude.Include.NON_NULL) AppCard.Ui ui,
                      @JsonInclude(JsonInclude.Include.NON_NULL) RawHost host,
                      @JsonInclude(JsonInclude.Include.NON_NULL) RawOrigin origin,
                      @JsonInclude(JsonInclude.Include.NON_NULL) List<String> formats,
                      @JsonInclude(JsonInclude.Include.NON_NULL) RawSpill spill) {

        RawAppCard withKind(String value) {
            return new RawAppCard(id, issuer, group, issued, ttl, name, value, shape, owner, reads, actions, idpClient, url, pkg, version,
                    views, ui, host, origin, formats, spill);
        }

        RawAppCard withUi(AppCard.Ui value) {
            return new RawAppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version,
                    views, value, host, origin, formats, spill);
        }

        RawAppCard withHost(RawHost value) {
            return new RawAppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version,
                    views, ui, value, origin, formats, spill);
        }

        RawAppCard withOrigin(RawOrigin value) {
            return new RawAppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version,
                    views, ui, host, value, formats, spill);
        }

        RawAppCard withFormats(List<String> value) {
            return new RawAppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version,
                    views, ui, host, origin, value, spill);
        }

        RawAppCard withSpill(RawSpill value) {
            return new RawAppCard(id, issuer, group, issued, ttl, name, kind, shape, owner, reads, actions, idpClient, url, pkg, version,
                    views, ui, host, origin, formats, value);
        }
    }

    record RawAgentCard(String id, PeerId issuer, GroupId group, Instant issued, Duration ttl, AgentId agent, String description,
                        List<String> goals, List<String> consumes, List<String> produces, Map<String, String> costHints,
                        Map<String, String> spaceBindings,
                        @JsonInclude(JsonInclude.Include.NON_NULL) byte[] agentPublicKey,
                        @JsonInclude(JsonInclude.Include.NON_NULL) List<CardAction> actions,
                        @JsonInclude(JsonInclude.Include.NON_NULL) Object agentCertificate,
                        @JsonInclude(JsonInclude.Include.NON_NULL) UiDeclaration ui,
                        @JsonInclude(JsonInclude.Include.NON_NULL) RawSpill spill) {
    }

    static RawAppCard raw(AppCard c) {
        AppCard.Host h = c.host();
        AppCard.Origin o = c.origin();
        CardSpill s = c.spill();
        return new RawAppCard(c.id(), c.issuer(), c.group(), c.issued(), c.ttl(), c.name(), c.kind(), c.shape(), c.owner(), c.reads(),
                c.actions(), c.idpClient(), c.url(), c.pkg(), c.version(), c.views(), c.ui(),
                h == null ? null : new RawHost(h.url(), h.port(), h.hostAgent(), h.path(), h.signOn(), h.publishedBy(), h.publishedAt(), h.status()),
                o == null ? null : new RawOrigin(o.system(), o.ref(), o.version(), o.attrs()),
                c.formats(),
                s == null ? null : new RawSpill(s.root(), s.size(), s.parts()));
    }

    static RawAgentCard raw(AgentCard c) {
        CardSpill s = c.spill();
        return new RawAgentCard(c.id(), c.issuer(), c.group(), c.issued(), c.ttl(), c.agent(), c.description(), c.goals(), c.consumes(),
                c.produces(), c.costHints(), c.spaceBindings(), c.agentPublicKey(), c.actions(), c.agentCertificate(), c.ui(),
                s == null ? null : new RawSpill(s.root(), s.size(), s.parts()));
    }

    static byte[] bytes(Object raw) {
        return CborCodec.defaultCodec().toBytes(raw);
    }
}
