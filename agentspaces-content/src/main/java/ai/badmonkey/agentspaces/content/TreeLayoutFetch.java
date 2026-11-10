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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The {@link Fetch} of a blob tree's root (PDT 2 follow-ups, 2026-11-09, F3; P2-X-01, PD-X-09): a
 * {@code layout: tree} manifest names a root whose nodes the tree exchange moves, so {@link Content#fetch}
 * hands back this view of the {@link TreeFetch} it starts. Progress counts nodes in place of pieces, and
 * {@code pieces} stays 0, since a tree fetch learns the size of the tree as it walks it. The content swarm moves
 * the nodes without decoding them, so {@link #open()} refuses: the PDT layer reads the blob from the tree store.
 */
final class TreeLayoutFetch implements Fetch {

    private final TreeFetch tree;
    private final Supplier<Optional<Manifest>> manifest;

    /**
     * A view of a tree fetch.
     *
     * @param tree     the tree fetch of the root
     * @param manifest where the root's manifest is read, once a member has written it
     */
    TreeLayoutFetch(TreeFetch tree, Supplier<Optional<Manifest>> manifest) {
        this.tree = Objects.requireNonNull(tree, "tree");
        this.manifest = Objects.requireNonNull(manifest, "manifest");
    }

    @Override
    public String root() {
        return tree.root();
    }

    @Override
    public Optional<Manifest> manifest() {
        return manifest.get();
    }

    @Override
    public Progress progress() {
        TreeFetch.Progress p = tree.progress();
        return new Progress(p.nodesVerified(), 0, p.bytesReceived(), p.bytesBySource(), p.sourcesActive(), p.waiting());
    }

    @Override
    public boolean await(Duration timeout) throws InterruptedException {
        return tree.await(timeout);
    }

    @Override
    public boolean complete() {
        return tree.complete();
    }

    @Override
    public boolean isDoneOrFailed() {
        return tree.isDoneOrFailed();
    }

    @Override
    public Optional<Throwable> failure() {
        return tree.failure();
    }

    @Override
    public boolean cancelled() {
        return tree.cancelled();
    }

    @Override
    public void cancel() {
        tree.cancel();
    }

    @Override
    public SeekableByteChannel open() throws IOException {
        throw new IOException("root " + tree.root() + " names a blob tree: the tree store holds its nodes, and the"
                + " persistent-tree layer reads the blob from them (PBlob)");
    }

    @Override
    public List<String> trace() {
        return tree.trace();
    }
}
