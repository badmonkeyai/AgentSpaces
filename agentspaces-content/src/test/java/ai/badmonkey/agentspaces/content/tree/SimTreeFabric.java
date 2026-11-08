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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Messages;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A {@link TreeFetchSession.Fabric} over in-process holders for the session's unit
 * tests: every holder is a {@link TreeServer} on its own {@link MemoryTreeStore},
 * requests and answers queue in order and move when {@link #pump} runs, time is a
 * counter the test advances, and a holder may tamper with what it sends, go
 * silent, or hold only some roots. Nothing here depends on the capability.
 */
final class SimTreeFabric implements TreeFetchSession.Fabric {

    /** One holder. */
    final class Holder {
        final PeerId id;
        final MemoryTreeStore store = new MemoryTreeStore();
        final Set<String> roots = new LinkedHashSet<>();
        final List<Object> received = new ArrayList<>();
        final List<Object> sent = new ArrayList<>();
        UnaryOperator<Object> tamper = m -> m;
        boolean silent;
        boolean sealsBadly;
        long uploadBudget = Long.MAX_VALUE;
        final TreeServer server;

        Holder(PeerId id, int walkCap) {
            this.id = id;
            this.server = new TreeServer(store, new TreeServer.Host() {
                @Override
                public boolean mayServe(PeerId from, String cid) {
                    return true;
                }

                @Override
                public boolean withinUploadBudget(int bytes) {
                    if (uploadBudget < bytes) {
                        return false;
                    }
                    uploadBudget -= bytes;
                    return true;
                }

                @Override
                public Optional<Messages.Nodes> seal(PeerId to, int req, List<byte[]> nodes, List<String> missing) {
                    return Optional.of(Messages.Nodes.plain(req, nodes, missing));
                }

                @Override
                public void send(PeerId to, Object message) {
                    Object out = tamper.apply(message);
                    if (out == null || silent) {
                        return;
                    }
                    sent.add(out);
                    answers.addLast(new Answer(id, out));
                }

                @Override
                public long nowMillis() {
                    return now;
                }
            }, walkCap);
        }

        Holder holds(NodeFixtures.Tree tree) {
            store.putAll(tree);
            roots.add(tree.root());
            return this;
        }

        Holder holdsRoot(String root) {
            roots.add(root);
            return this;
        }

        /** Whether a request id came in as a NODE_WANT rather than a TREE_WANT. */
        boolean askedByNodeWant(int req) {
            for (Object m : received) {
                if (m instanceof Messages.NodeWant want && want.req() == req) {
                    return true;
                }
            }
            return false;
        }
    }

    private record Answer(PeerId from, Object message) {
    }

    private record Ask(PeerId to, Object message) {
    }

    final PeerId self = PeerId.of("zSelf");
    final Map<PeerId, Holder> holders = new LinkedHashMap<>();
    final List<String> strikes = new ArrayList<>();
    final List<Object> sentBySession = new ArrayList<>();
    private final Deque<Ask> asks = new ArrayDeque<>();
    private final Deque<Answer> answers = new ArrayDeque<>();
    long now = 1_000_000;
    int reqs;
    long downloadBudget = Long.MAX_VALUE;
    long downloaded;
    boolean completed;
    TreeFetchSession session;

    Holder holder(String name) {
        return holder(name, Messages.MAX_WALK_NODES);
    }

    Holder holder(String name, int walkCap) {
        Holder h = new Holder(PeerId.of(name), walkCap);
        holders.put(h.id, h);
        return h;
    }

    @Override
    public PeerId self() {
        return self;
    }

    @Override
    public long nowMillis() {
        return now;
    }

    @Override
    public void send(PeerId to, Object message) {
        sentBySession.add(message);
        asks.addLast(new Ask(to, message));
    }

    @Override
    public int nextRequestId() {
        return ++reqs;
    }

    @Override
    public List<Holding> holdings(String root) {
        List<Holding> list = new ArrayList<>();
        for (Holder h : holders.values()) {
            if (h.roots.contains(root)) {
                list.add(new Holding(Holding.VERSION, root, h.id, h.store.complete(root), null, h.store.size(), null, null));
            }
        }
        return list;
    }

    @Override
    public void strike(PeerId peer, String reason) {
        strikes.add(peer.display() + ": " + reason);
    }

    @Override
    public int hopsTo(PeerId peer) {
        return 0;
    }

    @Override
    public boolean advertisesTrees(PeerId peer) {
        return true;
    }

    @Override
    public Optional<List<byte[]>> open(PeerId from, int req, Messages.Nodes nodes) {
        Holder h = holders.get(from);
        if (h != null && h.sealsBadly) {
            return Optional.empty();
        }
        return nodes.plaintext() ? Optional.of(nodes.nodes()) : Optional.empty();
    }

    @Override
    public boolean withinDownloadBudget(long bytes) {
        downloaded += bytes;
        return downloaded < downloadBudget;
    }

    @Override
    public void onComplete(TreeFetchSession session) {
        completed = true;
    }

    /** Moves every queued request to its holder and every queued answer to the session, until nothing moves. */
    void pump() {
        boolean moved = true;
        while (moved) {
            moved = false;
            Ask ask;
            while ((ask = asks.pollFirst()) != null) {
                moved = true;
                Holder h = holders.get(ask.to);
                if (h == null) {
                    continue;
                }
                h.received.add(ask.message);
                if (ask.message instanceof Messages.NodeWant want) {
                    h.server.serve(self, want);
                } else if (ask.message instanceof Messages.TreeWant want) {
                    h.server.serve(self, want);
                }
            }
            Answer answer;
            while ((answer = answers.pollFirst()) != null) {
                moved = true;
                if (answer.message instanceof Messages.Nodes nodes) {
                    if (nodes.violation().isPresent()) {
                        session.onUnusableNodes(answer.from, nodes.req());
                    } else {
                        session.onNodes(answer.from, nodes);
                    }
                } else if (answer.message instanceof Messages.TreeEnd end) {
                    session.onTreeEnd(answer.from, end);
                } else if (answer.message instanceof Messages.Reject reject) {
                    session.onReject(answer.from, reject);
                }
            }
        }
    }

    /** Advances time, ticks the session, and pumps; one sim tick. */
    void tick(long millis) {
        now += millis;
        session.tick();
        pump();
    }

    /** Ticks until the session ends or the ticks run out; true when it ended. */
    boolean runUntilDone(int ticks, long millisPerTick) {
        for (int i = 0; i < ticks && !session.isDone(); i++) {
            tick(millisPerTick);
        }
        return session.isDone();
    }
}
