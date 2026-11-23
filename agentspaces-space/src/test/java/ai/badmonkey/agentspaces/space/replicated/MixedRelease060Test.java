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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.junit.jupiter.api.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A member of this tree beside a member built from the released 0.6.0 jars, in one group over TCP on loopback
 * (srad/replication-by-entry.md RE-L-07, RE-Q-REL-06, RE-Q-SEC-04, RE-Q-INT-03; matrix probes P42 and P43). The 0.6.0
 * member runs in a class loader of its own that holds the release's jars from
 * {@code agentspaces-dependencies/target/central-staging} (the copy {@code DigestBytesPinTest} pinned its bytes
 * against) and the third-party jars of this test's class path, never a class of this tree; its code is compiled against
 * those jars when the test runs, so it calls only what the release has.
 *
 * <p>Every frame this tree's member sends or receives passes a tap on its transport, which reads each {@code DIGEST} and
 * {@code PULL_RESP} body as plain CBOR and records the field names. The test asserts what the 0.6.0 member's strict
 * reader would otherwise refuse (§9.1 P1): no body toward it carries {@code tree}, {@code ids}, {@code budget},
 * {@code sketch} or {@code remainder}, and the anti-entropy it gets is the form of v0.1.17 its own card names. It also
 * asserts that the release reads this tree's card, which carries {@code wire.ae=2}, and its hints (P43), and that both
 * replicas converge on writes and cancels from either side. A member of this tree
 * keeps the legacy horizon while a member without {@code wire.horizon=1} is in view; the test checks that this tree's
 * replica is in that mode and holds both withdrawals as tombstones.
 */
class MixedRelease060Test {

    /** The release's modules a member needs, by artifact id. */
    private static final List<String> RELEASE_MODULES = List.of("agentspaces-common", "agentspaces-api",
            "agentspaces-identity", "agentspaces-identity-tls", "agentspaces-discovery", "agentspaces-peering",
            "agentspaces-space", "agentspaces-test-support");
    private static final Path STAGING = Path.of("..", "agentspaces-dependencies", "target", "central-staging",
            "ai", "badmonkey", "agentspaces");
    private static final Set<String> V0117_DIGEST = Set.of("digests", "hashes", "want");
    private static final Set<String> V0134_PULL_RESP = Set.of("deltas");

    @Test
    void aMemberOfThisTreeAndAReleased060MemberConvergeAndTheReleaseNeverSeesANewField() throws Exception {
        List<Path> release = releaseJars();
        assumeTrue(release != null, "the released 0.6.0 jars are not on disk at " + STAGING.toAbsolutePath());
        Path work = Files.createTempDirectory("mixed060");
        try (URLClassLoader loader = legacyLoader(release, work)) {
            Class<?> member = loader.loadClass("legacy060.Legacy060Member");
            assertThat(member.getClassLoader().loadClass(PeerNode.class.getName())).as("the release's own PeerNode")
                    .isNotSameAs(PeerNode.class);

            String founding = "mixed-release-" + System.nanoTime();
            PeerIdentity founder = PeerIdentity.generate();
            GroupId groupId = GroupId.fromFounding(founding.getBytes(StandardCharsets.UTF_8));
            GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(), founder.peerId(),
                    groupId, Instant.EPOCH, Duration.ofDays(1), "mixed-release",
                    GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                    GroupAdvertisement.GossipParameters.defaults());

            Tap tap = new Tap();
            PeerIdentity identity = PeerIdentity.generate();
            PeerNode node = PeerNode.builder(identity).build(); // audit on by default: advertises wire.ae=2
            int port = freePort();
            int legacyPort = freePort();
            Object legacy = null;
            try {
                node.listen(new TappedTransport(new TcpTransport(), tap), "127.0.0.1:" + port);
                GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
                ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                        .settleWindow(Duration.ZERO).build();
                node.startTicking(Duration.ofMillis(200));
                assertThat(node.wireHints()).containsEntry(PeerNode.AE_HINT, PeerNode.AE_LEVEL);
                List<EntryHandle> ours = new ArrayList<>();
                for (int i = 0; i < 40; i++) {
                    ours.add(space.write(new TaskEntry("current-" + i, i), Lease.of(Duration.ofMinutes(30))));
                }

                legacy = call(member, null, "start", founding, founder.peerId().value(), "127.0.0.1:" + legacyPort,
                        "127.0.0.1:" + port);
                String legacyPeer = (String) call(member, legacy, "peerId");
                @SuppressWarnings("unchecked")
                Map<String, String> legacyHints = (Map<String, String>) call(member, legacy, "ownWireHints");
                assertThat(legacyHints).as("the release's own card").doesNotContainKey(PeerNode.AE_HINT)
                        .containsEntry(PeerNode.HASHES_HINT, "1");
                List<String> legacyIds = new ArrayList<>();
                for (int i = 0; i < 25; i++) {
                    legacyIds.add((String) call(member, legacy, "write", "release-" + i, 1_800_000L));
                }

                Object legacyMember = legacy;
                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (System.nanoTime() < deadline && !(space.knownEntries() == 65
                        && (int) call(member, legacyMember, "known") == 65)) {
                    Thread.sleep(100);
                }
                assertThat(space.knownEntries()).as("this tree's member holds both members' writes").isEqualTo(65);
                assertThat((int) call(member, legacy, "known")).as("the release holds both members' writes")
                        .isEqualTo(65);

                // P43: the release took this tree's card, wire.ae=2 and all, through its strict reader and read a hint
                // from it; and this tree's member reads the release's card as wire.hashes=1 without wire.ae=2.
                assertThat((boolean) call(member, legacy, "readsRecordlessHintOf", identity.peerId().value()))
                        .as("the 0.6.0 reader accepted a card carrying wire.ae=2 and read its hints").isTrue();
                assertThat(node.wireHints()).containsEntry(PeerNode.AE_HINT, "2");

                // Cancels from both sides travel as tombstones and both replicas drop the entries.
                ours.get(3).cancel();
                call(member, legacy, "cancel", legacyIds.get(5));
                deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (System.nanoTime() < deadline && !(topics(space).size() == 63
                        && ((List<?>) call(member, legacyMember, "topics")).size() == 63)) {
                    Thread.sleep(100);
                }
                @SuppressWarnings("unchecked")
                List<String> legacyTopics = (List<String>) call(member, legacy, "topics");
                assertThat(new TreeSet<>(topics(space))).as("both replicas read the same entries")
                        .isEqualTo(new TreeSet<>(legacyTopics)).hasSize(63)
                        .doesNotContain("current-3", "release-5");

                // 2 x maxLease: with a member in view that lacks wire.horizon=1 (the release), this tree's replica keeps
                // the legacy horizon, twice the space's longest lease past a withdrawal's expiry, and holds both
                // withdrawals as tombstones.
                assertThat(space.horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.LEGACY);
                assertThat(space.stats().withdrawn()).as("both cancels held as tombstones").isEqualTo(2);

                // RE-Q-REL-06, RE-Q-SEC-04: nothing toward the release carried a field its reader refuses, and what
                // went to it was the hashed form its card names, answered unbounded.
                PeerId legacyId = PeerId.of(legacyPeer);
                assertThat(tap.badFields).as("fields a 0.6.0 reader refuses, sent toward it").isEmpty();
                assertThat(tap.digestsTo.get()).as("anti-entropy reached the release").isPositive();
                assertThat(tap.hashedDigestsTo.get()).as("the hashed form of v0.1.17 its card names").isPositive();
                assertThat(tap.digestsFrom.get()).isPositive();
                assertThat(tap.pullRespTo.get()).as("the release joined late, so its 40 older entries came by PULL_RESP")
                        .isPositive();
                assertThat(tap.fieldsFrom).as("the release sends the v0.1.17 digest fields only")
                        .isSubsetOf(V0117_DIGEST);
                GossipBus.AuditSignals signals = runtime.gossip().auditSignals();
                assertThat(signals.requestsSent()).as("no audit request toward the release").isZero();
                assertThat(signals.roundsAudited()).isZero();
                assertThat(signals.idsFetched()).as("no fetch by id toward the release").isZero();
                assertThat(signals.budgetedAnswers()).as("the release never asks with a budget").isZero();
                assertThat(signals.refusals()).as("nothing the release sent was undecodable here")
                        .doesNotContainKey("undecodable");
                assertThat(runtime.membership().allMembers()).extracting(GroupMembership.Member::id)
                        .contains(legacyId);
                System.out.printf("mixed 0.6.0: %d DIGEST toward the release (%d hashed), %d from it, %d PULL_RESP "
                                + "toward it, fields toward it %s, from it %s%n", tap.digestsTo.get(),
                        tap.hashedDigestsTo.get(), tap.digestsFrom.get(), tap.pullRespTo.get(), tap.fieldsTo,
                        tap.fieldsFrom);
            } finally {
                if (legacy != null) {
                    call(member, legacy, "close");
                }
                node.close();
            }
        } finally {
            try (Stream<Path> files = Files.walk(work)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private static List<String> topics(ReplicatedSpace space) {
        List<String> out = new ArrayList<>();
        for (TaskEntry entry : space.readAll(Template.of(TaskEntry.class), 10_000)) {
            out.add(entry.topic());
        }
        return out;
    }

    // ---- the release's class loader

    /** The release's jars, or null when the staging copy is not on disk. */
    private static List<Path> releaseJars() {
        List<Path> jars = new ArrayList<>();
        for (String module : RELEASE_MODULES) {
            Path jar = STAGING.resolve(module).resolve("0.6.0").resolve(module + "-0.6.0.jar");
            if (!Files.isRegularFile(jar)) {
                return null;
            }
            jars.add(jar.toAbsolutePath().normalize());
        }
        return jars;
    }

    /**
     * Compiles the 0.6.0 member against the release's jars and loads it with them and the third-party jars of this
     * test's class path; no directory of this tree and no jar of its group id is on that path, and the parent is the
     * platform loader, so no class of this tree can be reached from it.
     */
    private static URLClassLoader legacyLoader(List<Path> release, Path work) throws Exception {
        List<Path> path = new ArrayList<>(release);
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            Path p = Path.of(entry).toAbsolutePath().normalize();
            String text = p.toString().replace('\\', '/');
            if (text.endsWith(".jar") && !text.contains("/ai/badmonkey/") && Files.isRegularFile(p)) {
                path.add(p);
            }
        }
        Path source = work.resolve("src").resolve("legacy060").resolve("Legacy060Member.java");
        Files.createDirectories(source.getParent());
        try (InputStream in = Objects.requireNonNull(MixedRelease060Test.class
                .getResourceAsStream("legacy060/Legacy060Member.java.txt"))) {
            Files.write(source, in.readAllBytes());
        }
        Path classes = Files.createDirectories(work.resolve("classes"));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assumeTrue(compiler != null, "no system Java compiler in this JVM");
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        String cp = String.join(File.pathSeparator, path.stream().map(Path::toString).toList());
        int status = compiler.run(null, null, errors, "-nowarn", "-cp", cp, "-d", classes.toString(),
                source.toString());
        assertThat(status).as("the 0.6.0 member compiles against the release: " + errors).isZero();
        List<URL> urls = new ArrayList<>();
        urls.add(classes.toUri().toURL());
        for (Path p : path) {
            urls.add(p.toUri().toURL());
        }
        return new URLClassLoader("release-0.6.0", urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader());
    }

    private static Object call(Class<?> type, Object target, String name, Object... args) throws Exception {
        for (Method method : type.getMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == args.length) {
                ClassLoader previous = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(type.getClassLoader());
                try {
                    return method.invoke(target, args);
                } finally {
                    Thread.currentThread().setContextClassLoader(previous);
                }
            }
        }
        throw new NoSuchMethodException(name);
    }

    private static int freePort() throws IOException {
        // below the ephemeral range, so no other process takes it before the test binds it (parallel forks)
        return ai.badmonkey.agentspaces.test.Ports.free();
    }

    // ---- the frame tap

    /** What this tree's member sent and received, read from the raw frames as plain CBOR. */
    private static final class Tap {
        private final CborCodec codec = CborCodec.defaultCodec();
        private final CBORMapper cbor = new CBORMapper();
        final Set<String> badFields = ConcurrentHashMap.newKeySet();
        final Set<String> fieldsTo = ConcurrentHashMap.newKeySet();
        final Set<String> fieldsFrom = ConcurrentHashMap.newKeySet();
        final AtomicLong digestsTo = new AtomicLong();
        final AtomicLong hashedDigestsTo = new AtomicLong();
        final AtomicLong digestsFrom = new AtomicLong();
        final AtomicLong pullRespTo = new AtomicLong();

        void sent(byte[] frame) {
            read(frame, true);
        }

        void received(byte[] frame) {
            read(frame, false);
        }

        private void read(byte[] frame, boolean outbound) {
            WireCodec.Signed signed;
            try {
                signed = codec.fromWire(frame, WireCodec.Signed.class);
            } catch (RuntimeException e) {
                return; // not a frame this test reads; the node's own reader judges it
            }
            Envelope envelope = signed.envelope();
            if (envelope.kind() == Envelope.Kind.BATCH) {
                Bodies.Batch batch = codec.fromWire(envelope.body(), Bodies.Batch.class);
                batch.frames().forEach(inner -> read(inner, outbound));
                return;
            }
            if (envelope.kind() != Envelope.Kind.DIGEST && envelope.kind() != Envelope.Kind.PULL_RESP) {
                return;
            }
            Set<String> fields = new TreeSet<>();
            try {
                JsonNode body = cbor.readTree(envelope.body());
                Iterator<String> names = body.fieldNames();
                names.forEachRemaining(fields::add);
                if (outbound && envelope.kind() == Envelope.Kind.DIGEST && body.hasNonNull("hashes")) {
                    hashedDigestsTo.incrementAndGet();
                }
            } catch (IOException e) {
                badFields.add("unreadable " + envelope.kind());
                return;
            }
            Set<String> allowed = envelope.kind() == Envelope.Kind.DIGEST ? V0117_DIGEST : V0134_PULL_RESP;
            if (outbound) {
                (envelope.kind() == Envelope.Kind.DIGEST ? digestsTo : pullRespTo).incrementAndGet();
                fieldsTo.addAll(fields);
                for (String field : fields) {
                    if (!allowed.contains(field)) {
                        badFields.add(envelope.kind() + "." + field);
                    }
                }
            } else if (envelope.kind() == Envelope.Kind.DIGEST) {
                digestsFrom.incrementAndGet();
                fieldsFrom.addAll(fields);
            }
        }
    }

    /** A transport whose connections show the tap every frame, both ways, before the node or the socket sees it. */
    private record TappedTransport(Transport inner, Tap tap) implements Transport {
        @Override
        public String scheme() {
            return inner.scheme();
        }

        @Override
        public Set<Capability> capabilities() {
            return inner.capabilities();
        }

        @Override
        public boolean signalled() {
            return inner.signalled();
        }

        @Override
        public TransportConnection dial(String address) throws IOException {
            return wrap(inner.dial(address));
        }

        @Override
        public TransportConnection dial(String address, PeerId target) throws IOException {
            return wrap(inner.dial(address, target));
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
            return inner.listen(bindAddress, connection -> onAccept.accept(wrap(connection)));
        }

        private TransportConnection wrap(TransportConnection c) {
            return new TransportConnection() {
                @Override
                public String remoteAddress() {
                    return c.remoteAddress();
                }

                @Override
                public java.util.Optional<PeerId> attestedPeer() {
                    return c.attestedPeer();
                }

                @Override
                public java.security.cert.X509Certificate[] remoteChain() {
                    return c.remoteChain();
                }

                @Override
                public void send(byte[] frame) throws IOException {
                    tap.sent(frame);
                    c.send(frame);
                }

                @Override
                public void onReceive(Consumer<byte[]> receiver) {
                    c.onReceive(frame -> {
                        tap.received(frame);
                        receiver.accept(frame);
                    });
                }

                @Override
                public void onClose(Runnable listener) {
                    c.onClose(listener);
                }

                @Override
                public void close() {
                    c.close();
                }
            };
        }
    }
}
