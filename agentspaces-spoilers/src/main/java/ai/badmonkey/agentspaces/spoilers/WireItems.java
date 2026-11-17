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
package ai.badmonkey.agentspaces.spoilers;

import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.replicated.SpaceWire;
import ai.badmonkey.agentspaces.test.FrameItem;
import ai.badmonkey.agentspaces.test.ItemDecoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The level-2 {@link ItemDecoder} (srad/spoiler-peers.md §3.3, SP-C-06): the space states and claims inside
 * a {@code RUMOR} or a {@code PULL_RESP}, the PDT nodes inside a content-swarm {@code NODES} answer, the
 * content requests, looking inside every frame a {@code BATCH} or {@code RELAY_FRAME} carries. It reads
 * the shipped wire records and verifies nothing: it is an observer of the in-process fabric, and the
 * honest members judge every frame as they always do.
 *
 * <p>The Java core seals no group payloads (SPEC §11a is entry-level and leaves the delta fields this
 * reader matches in the clear), so no group key is needed on {@code SimNetwork} yet; a fleet that seals
 * gets a decoder with its keys when it needs one.
 */
public final class WireItems implements ItemDecoder {

    /** The pipe frame's shape as {@code CapabilityPipes} writes it (SPEC §9 {@code PIPE_DATA}). */
    record PipeFrame(String capability, byte[] payload) {
    }

    private static final String SPACE_PREFIX = "space:";
    private static final int MAX_DEPTH = 4;

    private final CborCodec codec = CborCodec.defaultCodec();

    @Override
    public List<FrameItem> items(byte[] frame) {
        List<FrameItem> out = new ArrayList<>();
        read(frame, out, 0);
        return out;
    }

    private void read(byte[] frame, List<FrameItem> out, int depth) {
        WireCodec.Signed signed = decode(frame, WireCodec.Signed.class);
        if (signed != null && signed.envelope() != null) {
            envelope(signed.envelope(), out, depth);
        }
    }

    private void envelope(Envelope envelope, List<FrameItem> out, int depth) {
        byte[] body = envelope.body();
        switch (envelope.kind()) {
            case RUMOR -> {
                Bodies.Rumor rumor = decode(body, Bodies.Rumor.class);
                if (rumor != null && rumor.streamId() != null && rumor.streamId().startsWith(SPACE_PREFIX)) {
                    SpaceWire.Delta delta = decode(rumor.payload(), SpaceWire.Delta.class);
                    if (delta != null) {
                        state(rumor.streamId(), delta.state(), out);
                        claim(rumor.streamId(), delta.claimEntry() == null ? null : delta.claimEntry().toString(), delta.claim(), out);
                    }
                }
            }
            case PULL_RESP -> {
                Bodies.PullResp pull = decode(body, Bodies.PullResp.class);
                if (pull != null && pull.deltas() != null) {
                    for (Map.Entry<String, byte[]> e : pull.deltas().entrySet()) {
                        if (!e.getKey().startsWith(SPACE_PREFIX)) {
                            continue;
                        }
                        SpaceWire.SyncDelta sync = decode(e.getValue(), SpaceWire.SyncDelta.class);
                        if (sync == null) {
                            continue;
                        }
                        if (sync.states() != null) {
                            sync.states().forEach(s -> state(e.getKey(), s, out));
                        }
                        if (sync.claims() != null) {
                            sync.claims().forEach((entry, c) -> claim(e.getKey(), entry, c, out));
                        }
                    }
                }
            }
            case BATCH -> {
                Bodies.Batch batch = decode(body, Bodies.Batch.class);
                if (batch != null && batch.frames() != null && depth < MAX_DEPTH) {
                    for (byte[] inner : batch.frames()) {
                        Envelope e = decode(inner, Envelope.class);
                        if (e != null) {
                            envelope(e, out, depth + 1);
                        }
                    }
                }
            }
            case RELAY_FRAME -> {
                Bodies.RelayFrame relay = decode(body, Bodies.RelayFrame.class);
                if (relay != null && relay.frame() != null && depth < MAX_DEPTH) {
                    read(relay.frame(), out, depth + 1);
                }
            }
            case PIPE_DATA -> {
                PipeFrame pipe = decode(body, PipeFrame.class);
                if (pipe != null && Content.TYPE.equals(pipe.capability()) && pipe.payload() != null) {
                    content(pipe.capability(), pipe.payload(), out);
                }
            }
            default -> { }
        }
    }

    private void state(String stream, SpaceWire.EntryStateDto state, List<FrameItem> out) {
        if (state == null || state.entryIdOf() == null) {
            return;
        }
        Map<String, String> attrs = new HashMap<>();
        EntryRecord record = state.record();
        if (record != null) {
            if (record.issuer() != null) {
                attrs.put("issuer", record.issuer().toString());
                attrs.put("issuerPeer", record.issuer().peer().toString());
            }
            if (record.type() != null) {
                attrs.put("type", record.type());
            }
            String root = payloadRoot(record.payload());
            if (root != null) {
                attrs.put("root", root); // a PDT head names its version's root: what a script binds a base by
            }
        }
        attrs.put("recordless", Boolean.toString(record == null));
        attrs.put("completed", Boolean.toString(state.completed()));
        String id = state.entryIdOf().toString();
        out.add(new FrameItem(ItemKind.STATE.name(), stream, id, attrs));
        if (state.completed()) {
            out.add(new FrameItem(ItemKind.COMPLETION.name(), stream, id, attrs));
        }
        if (state.adds() != null && !state.adds().isEmpty() && state.removes() != null
                && new HashSet<>(state.removes()).containsAll(state.adds())) {
            out.add(new FrameItem(ItemKind.CANCEL.name(), stream, id, attrs));
        }
    }

    private void claim(String stream, String entry, SpaceWire.SignedClaim signed, List<FrameItem> out) {
        if (signed == null || signed.claim() == null) {
            return;
        }
        Map<String, String> attrs = new HashMap<>();
        if (signed.claim().holder() != null) {
            attrs.put("holder", signed.claim().holder().toString());
            attrs.put("holderPeer", signed.claim().holder().peer().toString());
        }
        attrs.put("epoch", Long.toString(signed.claim().epoch()));
        attrs.put("expires", Long.toString(signed.claim().expiresAtMillis()));
        out.add(new FrameItem(ItemKind.CLAIM.name(), stream, entry != null ? entry : signed.claim().entryId().toString(), attrs));
    }

    private void content(String capability, byte[] payload, List<FrameItem> out) {
        Optional<Integer> tag = Messages.peekTag(codec, payload);
        if (tag.isEmpty()) {
            return;
        }
        switch (tag.get()) {
            case Messages.NODES -> {
                Messages.Nodes nodes = decode(payload, Messages.Nodes.class);
                if (nodes == null || nodes.nodes() == null) {
                    return; // a sealed answer hides its nodes from every reader without the content key
                }
                for (byte[] node : nodes.nodes()) {
                    Map<String, String> attrs = new HashMap<>();
                    attrs.put("req", Integer.toString(nodes.req()));
                    Map<?, ?> fields = decode(node, Map.class);
                    if (fields != null && fields.get("k") != null) {
                        attrs.put("kind", fields.get("k").toString());
                        if (fields.get("r") instanceof String root) {
                            attrs.put("root", root); // a vg node names the version it describes
                        }
                    }
                    out.add(new FrameItem(ItemKind.NODE.name(), capability, Cids.of(node, Cids.DAG_CBOR), attrs));
                }
            }
            case Messages.NODE_WANT -> {
                Messages.NodeWant want = decode(payload, Messages.NodeWant.class);
                if (want != null && want.cids() != null && !want.cids().isEmpty()) {
                    out.add(new FrameItem(ItemKind.NODE_WANT.name(), capability, want.cids().get(0),
                            Map.of("cids", String.join(",", want.cids()), "req", Integer.toString(want.req()))));
                }
            }
            case Messages.TREE_WANT -> {
                Messages.TreeWant want = decode(payload, Messages.TreeWant.class);
                if (want != null) {
                    out.add(new FrameItem(ItemKind.TREE_WANT.name(), capability, want.root(),
                            Map.of("haves", want.haves() == null ? "" : String.join(",", want.haves()),
                                    "req", Integer.toString(want.req()))));
                }
            }
            default -> { }
        }
    }

    /** The {@code root} a PDT head's payload names, or null for any other payload. */
    private String payloadRoot(byte[] payload) {
        if (payload == null) {
            return null;
        }
        Map<?, ?> fields = decode(payload, Map.class);
        return fields != null && fields.get("root") instanceof String root ? root : null;
    }

    private <T> T decode(byte[] bytes, Class<T> type) {
        if (bytes == null) {
            return null;
        }
        try {
            return codec.fromBytes(bytes, type);
        } catch (RuntimeException notThisShape) {
            return null;
        }
    }
}
