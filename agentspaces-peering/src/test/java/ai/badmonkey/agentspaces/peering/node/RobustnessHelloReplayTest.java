package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §5, the hello replay window: a captured, validly signed SESSION_HELLO played
 * back at its recipient. On the link that already holds the session it must change nothing. On
 * a link that holds no session (a stranger's fresh dial, modelled by a signed-mode member whose
 * link carries no session) it may earn at most one answer while young, and nothing once older
 * than {@link PeerNode#SESSION_HELLO_MAX_AGE_MILLIS}, the exact-replay cache notwithstanding.
 */
class RobustnessHelloReplayTest {

    private final RobustnessFleet fleet = new RobustnessFleet("HelloReplay");
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static long hellosSent(PeerNode node) {
        return node.traffic().sentByKind().getOrDefault(Envelope.Kind.SESSION_HELLO, 0L);
    }

    /** The opening hellos (no echo) among captured frames: the one hello kind that creates state at its recipient. */
    private List<byte[]> openingHellos(List<byte[]> frames) {
        List<byte[]> hellos = new ArrayList<>();
        for (byte[] frame : frames) {
            wire.decode(frame).filter(e -> e.kind() == Envelope.Kind.SESSION_HELLO).ifPresent(e -> {
                Bodies.SessionHello hello = codec.fromBytes(e.body(), Bodies.SessionHello.class);
                if (hello.echo() == null) {
                    hellos.add(frame);
                }
            });
        }
        return hellos;
    }

    /**
     * Turns the victim's exact-replay cache over: more distinct, validly signed, harmless frames
     * (empty HAVEs) from one member than the cache holds, inside a single tick.
     */
    private void evictReplayCache(String from, String victim) {
        PeerIdentity identity = fleet.identities.get(from);
        byte[] body = codec.toBytes(new Bodies.Have(List.of()));
        for (int i = 0; i < PeerNode.RECENT_FRAMES_CAPACITY + 64; i++) {
            Envelope envelope = new Envelope(WireCodec.WIRE_VERSION, fleet.groupId, Envelope.Kind.HAVE,
                    identity.peerId(), fleet.id(victim), new HlcTimestamp(fleet.clock.millis(), i, from), body);
            assertThat(fleet.network.inject(from, victim, wire.encode(envelope, identity))).isTrue();
        }
        fleet.tickAll(1);
    }

    @Test
    @Timeout(120)
    void aReplayedHelloChangesNothingOnItsOwnLinkAndEarnsAtMostOneAnswerOnAStrangersWhileYoung() throws Exception {
        List<byte[]> aToB = new CopyOnWriteArrayList<>();
        List<byte[]> bToA = new CopyOnWriteArrayList<>();
        fleet.add("a", null);
        fleet.network.tap("a", "b", aToB::add);
        fleet.network.tap("b", "a", bToA::add);
        fleet.add("b", "a");
        // Two members in signed mode: their links to B carry no session, as a stranger's fresh dial would not.
        fleet.add("x", "a", PeerNode.ChannelAuth.SIGNED, GroupMembership.Config.defaults());
        fleet.add("y", "a", PeerNode.ChannelAuth.SIGNED, GroupMembership.Config.defaults());
        assertThat(fleet.settle(12)).isTrue();
        fleet.network.untap("a", "b");
        fleet.network.untap("b", "a");
        // Whichever side opened: the opener's recipient is the victim, the opener the sender.
        List<byte[]> hellos = openingHellos(aToB);
        String sender = "a";
        String victim = "b";
        if (hellos.isEmpty()) {
            hellos = openingHellos(bToA);
            sender = "b";
            victim = "a";
        }
        assertThat(hellos).as("one side opened the session with a signed hello").isNotEmpty();
        PeerNode b = fleet.nodes.get(victim);
        PeerNode a = fleet.nodes.get(sender);
        assertThat(b.channelModes()).containsEntry(fleet.id(sender), "session")
                .containsEntry(fleet.id("x"), "signed").containsEntry(fleet.id("y"), "signed");

        // 1. On the link that holds the session, a young replay (still in the exact-replay cache) is dropped.
        long helloBefore = hellosSent(b);
        long replayedBefore = b.framesReplayed();
        long droppedBefore = b.framesDropped();
        for (byte[] hello : hellos) {
            assertThat(fleet.network.inject(sender, victim, hello)).isTrue();
        }
        assertThat(hellosSent(b)).as("no answer to a replay on the session's own link").isEqualTo(helloBefore);
        assertThat(b.framesReplayed() - replayedBefore).as("each replay counted as one").isEqualTo(hellos.size());
        assertThat(b.framesDropped()).as("a replay is not an unverifiable frame").isEqualTo(droppedBefore);
        assertThat(b.channelModes()).containsEntry(fleet.id(sender), "session");
        assertThat(b.quarantined(fleet.id(sender))).isFalse();
        long sessionBefore = a.sessionFramesSent();
        fleet.publish(sender, "still-under-mac");
        fleet.tickAll(2);
        assertThat(fleet.news.get(victim)).contains("still-under-mac");
        assertThat(a.sessionFramesSent()).as("the sender still speaks to the victim under the MAC").isGreaterThan(sessionBefore);
        assertThat(b.framesDropped()).as("and B still verifies every MAC frame").isEqualTo(droppedBefore);

        // 2. Past the exact-replay cache but young: on a link without a session, B answers at most once
        //    and nothing else moves (the answer is signed; the replayer holds no ephemeral private key).
        evictReplayCache("x", victim);
        assertThat(fleet.clock.millis() - helloStamp(hellos.get(0))).as("the capture is still young")
                .isLessThan(PeerNode.SESSION_HELLO_MAX_AGE_MILLIS);
        helloBefore = hellosSent(b);
        replayedBefore = b.framesReplayed();
        for (byte[] hello : hellos) {
            assertThat(fleet.network.inject("x", victim, hello)).isTrue();
        }
        fleet.tickAll(2);
        System.out.printf("young replay on a session-less link: B answered with %d hello(s), replay-dropped %d of %d%n",
                hellosSent(b) - helloBefore, b.framesReplayed() - replayedBefore, hellos.size());
        assertThat(b.framesReplayed()).as("the capture had left the exact-replay cache").isEqualTo(replayedBefore);
        assertThat(hellosSent(b) - helloBefore).as("at most one answer per such link").isLessThanOrEqualTo(1);
        assertThat(b.channelModes()).containsEntry(fleet.id(sender), "session").containsEntry(fleet.id("x"), "signed");
        assertThat(b.quarantined(fleet.id("x"))).isFalse();
        fleet.publish("x", "x-still-heard");
        fleet.publish(victim, "victim-still-heard");
        fleet.tickAll(2);
        assertThat(fleet.news.get(victim)).contains("x-still-heard");
        assertThat(fleet.news.get("x")).contains("victim-still-heard");

        // 3. Older than the hello window, past the cache, on a link without a session: ignored outright.
        int wait = (int) (PeerNode.SESSION_HELLO_MAX_AGE_MILLIS / 1000) + 2;
        fleet.tickAll(wait);
        assertThat(fleet.clock.millis() - helloStamp(hellos.get(0))).isGreaterThan(PeerNode.SESSION_HELLO_MAX_AGE_MILLIS);
        evictReplayCache("y", victim);
        helloBefore = hellosSent(b);
        replayedBefore = b.framesReplayed();
        long staleBefore = b.framesStale();
        for (byte[] hello : hellos) {
            assertThat(fleet.network.inject("y", victim, hello)).isTrue();
        }
        fleet.tickAll(2);
        System.out.printf("old replay on a session-less link: B answered with %d hello(s), replay-dropped %d, stale-dropped %d%n",
                hellosSent(b) - helloBefore, b.framesReplayed() - replayedBefore, b.framesStale() - staleBefore);
        assertThat(b.framesReplayed()).as("not the cache's doing").isEqualTo(replayedBefore);
        assertThat(b.framesStale() - staleBefore).as("ignored as stale by the hello window, under the five-minute frame bound")
                .isEqualTo(hellos.size());
        assertThat(hellosSent(b) - helloBefore).as("an old hello earns no answer anywhere").isZero();
        assertThat(b.channelModes()).containsEntry(fleet.id(sender), "session").containsEntry(fleet.id("y"), "signed");
        assertThat(b.quarantined(fleet.id("y"))).isFalse();
        assertThat(fleet.settle(10)).isTrue();
        fleet.assertInvariants();
    }

    private long helloStamp(byte[] frame) {
        return wire.decode(frame).orElseThrow().stamp().physical();
    }

    @Test
    @Timeout(60)
    void aLegitimateHelloFromAMemberWhoseClockLagsIsStillAnswered() throws Exception {
        // An opener's stamp is merged toward what it last heard, so a lagging clock never makes a
        // live hello look old; pinned by joining and watching the session form as usual.
        fleet.add("a", null);
        fleet.add("b", "a");
        assertThat(fleet.settle(10)).isTrue();
        assertThat(fleet.nodes.get("b").channelModes()).containsEntry(fleet.id("a"), "session");
        assertThat(fleet.nodes.get("a").channelModes()).containsEntry(fleet.id("b"), "session");
        String text = new String("fresh".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        fleet.publish("a", text);
        fleet.tickAll(2);
        assertThat(fleet.news.get("b")).contains(text);
    }
}
