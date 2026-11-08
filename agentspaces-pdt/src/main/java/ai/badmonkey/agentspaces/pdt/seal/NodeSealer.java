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
package ai.badmonkey.agentspaces.pdt.seal;

import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.crypto.Hkdf;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.GCMSIVBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The sealed-nodes mode of srad/agentspaces-pdt.md §3.8 (PD-P-05): every node is
 * sealed deterministically under a per-epoch key and named by the CID of its sealed
 * bytes, so a keyless member or relay verifies, stores, and serves nodes it cannot
 * read, and equal plaintext nodes of one epoch seal to equal bytes and share.
 *
 * <p>The pinned construction (§10.2):
 * <ul>
 *   <li>per-epoch key = HKDF-SHA256(ikm = the group content key of the epoch, salt
 *       empty, info {@value #INFO}), 32 bytes;</li>
 *   <li>cipher = AES-256-GCM-SIV (RFC 8452) with a 12-byte nonce = the first 12 bytes
 *       of HMAC-SHA256(key = the per-epoch node key, message = the plaintext node
 *       bytes), so the seal is deterministic for one key and plaintext, and AAD =
 *       {@value #AAD_PREFIX} followed by the epoch in decimal. The nonce travels in
 *       the clear, and because it is keyed it tells a keyless member nothing about
 *       the plaintext: confirming a guessed node takes the epoch's key (the first
 *       build derived the nonce from sha256(plaintext) alone, which let a keyless
 *       holder confirm a guess with one hash; the review of 2026-11-08 changed it);</li>
 *   <li>the sealed bytes are the {@code sn} node {@code {k: "sn", ct: ciphertext and
 *       tag, ep: epoch, ln: [the links of the plaintext node], nc: nonce}} in the
 *       deterministic DAG-CBOR of {@link NodeCodec}; the CID names them under the
 *       plaintext's codec ({@code dag-cbor} for a structured node, {@code raw} for a
 *       chunk), so a parent's link and a blob's chunk rule read as before;</li>
 *   <li>a reader opens with the epoch's opening keys of SPEC §11a.3
 *       ({@link GroupKeyRing#openingKeys}) and refuses an envelope whose links in the
 *       clear differ from the links of the plaintext it opened to.</li>
 * </ul>
 * The links travel in the clear so that a keyless holder can walk, fetch, prune,
 * and serve a tree (PD-X-03), which is what keyless storage is for; a keyless
 * member therefore learns the shape of a tree (its links, node count, sizes, and
 * epoch) and the equality of sealed nodes of one epoch, and nothing of their
 * contents (PD-P-06).
 *
 * <p>The static {@link #seal(byte[], long, byte[], int)} and {@link #open(byte[], Envelope)}
 * are the pure functions of the format, which the golden vector
 * {@code pdt_sealed_node} pins over a fixed test key; an instance binds them to
 * a group's key ring and clock, so {@link #seal(byte[], int)} writes under the
 * ring's write epoch and {@link #open(byte[])} tries every key the ring holds for
 * the envelope's epoch. A sealer over an empty ring opens nothing and seals
 * nothing: it is what a keyless member runs.
 */
public final class NodeSealer {

    /** The HKDF info of the per-epoch node key. */
    public static final String INFO = "aspace:pdt:node";

    /** The AAD prefix; the epoch in decimal follows it. */
    public static final String AAD_PREFIX = "aspace:pdt:node|";

    /** The nonce length AES-GCM-SIV takes. */
    public static final int NONCE_BYTES = 12;

    /** The derived key length. */
    public static final int KEY_BYTES = 32;

    /** The tag length in bits. */
    public static final int TAG_BITS = 128;

    /**
     * A decoded envelope.
     *
     * @param epoch      the key epoch the node was sealed under
     * @param nonce      the 12-byte nonce
     * @param ciphertext the ciphertext and tag
     * @param links      the links of the plaintext node, in the clear
     */
    public record Envelope(long epoch, byte[] nonce, byte[] ciphertext, List<String> links) {
    }

    private final GroupKeyRing ring;
    private final InstantSource clock;
    private final Map<GroupKey, byte[]> derived = new ConcurrentHashMap<>();

    /**
     * A sealer over a group's content key ring.
     *
     * @param ring  the ring; {@link GroupKeyRing#empty()} for a keyless member
     * @param clock the clock the write epoch is chosen at
     */
    public NodeSealer(GroupKeyRing ring, InstantSource clock) {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The ring. */
    public GroupKeyRing ring() {
        return ring;
    }

    // ------------------------------------------------------------ the pure format

    /**
     * The per-epoch node key of a content key.
     *
     * @param contentKey the group content key of the epoch
     * @return 32 bytes
     */
    public static byte[] nodeKey(GroupKey contentKey) {
        Objects.requireNonNull(contentKey, "contentKey");
        return Hkdf.derive(contentKey.rawBytes(), new byte[0], INFO.getBytes(StandardCharsets.UTF_8), KEY_BYTES);
    }

    /** The AAD of an epoch. */
    public static byte[] aad(long epoch) {
        return (AAD_PREFIX + epoch).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The deterministic nonce of a plaintext node under a node key: the first
     * {@value #NONCE_BYTES} bytes of HMAC-SHA256(nodeKey, plaintext). Keyed, so the
     * nonce in the clear is no oracle for the plaintext (PD-P-06).
     *
     * @param nodeKey   the per-epoch key of {@link #nodeKey}
     * @param plaintext the node or chunk bytes
     * @return 12 bytes
     */
    public static byte[] nonce(byte[] nodeKey, byte[] plaintext) {
        Objects.requireNonNull(nodeKey, "nodeKey");
        Objects.requireNonNull(plaintext, "plaintext");
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(nodeKey, "HmacSHA256"));
            return Arrays.copyOf(mac.doFinal(plaintext), NONCE_BYTES);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }

    /**
     * Seals a plaintext node under a derived epoch key.
     *
     * @param nodeKey   the per-epoch key of {@link #nodeKey}
     * @param epoch     the epoch, named in the AAD and the envelope
     * @param plaintext the node or chunk bytes
     * @param codec     the plaintext's codec: {@link Cids#DAG_CBOR} or {@link Cids#RAW}; a chunk has no links
     * @return the envelope bytes
     * @throws NodeFormatException when a structured plaintext node is malformed or the envelope passes its bound
     */
    public static byte[] seal(byte[] nodeKey, long epoch, byte[] plaintext, int codec) {
        Objects.requireNonNull(nodeKey, "nodeKey");
        Objects.requireNonNull(plaintext, "plaintext");
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must not be negative: " + epoch);
        }
        List<String> links = codec == Cids.RAW ? List.of() : NodeCodec.links(plaintext);
        byte[] nonce = nonce(nodeKey, plaintext);
        byte[] ciphertext = cipher(true, nodeKey, nonce, aad(epoch), plaintext)
                .orElseThrow(() -> new IllegalStateException("AES-GCM-SIV refused to seal"));
        List<Link> linkItems = new ArrayList<>(links.size());
        for (String link : links) {
            linkItems.add(new Link(link));
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.SN);
        node.put("ct", ciphertext);
        node.put("ep", epoch);
        node.put("ln", linkItems);
        node.put("nc", nonce);
        return NodeCodec.encode(node);
    }

    /**
     * Whether bytes are a sealed envelope: a well-formed node of kind {@code sn}.
     *
     * @param bytes the stored bytes
     * @return true for an envelope
     */
    public static boolean isSealed(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            return Kind.SN.equals(NodeCodec.kindOf(bytes));
        } catch (NodeFormatException e) {
            return false;
        }
    }

    /**
     * Decodes an envelope.
     *
     * @param bytes the stored bytes
     * @return the envelope
     * @throws NodeFormatException when the bytes are not a well-formed {@code sn} node
     */
    public static Envelope decode(byte[] bytes) {
        Map<String, Object> node = NodeCodec.decode(bytes);
        if (!Kind.SN.equals(node.get(NodeCodec.KIND_KEY))) {
            throw new NodeFormatException("not a sealed node envelope: kind '" + node.get(NodeCodec.KIND_KEY) + "'");
        }
        byte[] nonce = (byte[]) node.get("nc");
        byte[] ciphertext = (byte[]) node.get("ct");
        Long epoch = (Long) node.get("ep");
        List<?> links = (List<?>) node.get("ln");
        if (nonce == null || ciphertext == null || epoch == null || links == null) {
            throw new NodeFormatException("a sealed node envelope carries ct, ep, ln, and nc");
        }
        if (nonce.length != NONCE_BYTES) {
            throw new NodeFormatException("a sealed node nonce is " + NONCE_BYTES + " bytes, got " + nonce.length);
        }
        if (ciphertext.length < TAG_BITS / 8) {
            throw new NodeFormatException("a sealed node ciphertext carries at least its " + TAG_BITS / 8 + "-byte tag");
        }
        List<String> cids = new ArrayList<>(links.size());
        for (Object link : links) {
            cids.add(((Link) link).cid());
        }
        return new Envelope(epoch, nonce, ciphertext, List.copyOf(cids));
    }

    /**
     * Opens an envelope under a derived epoch key and checks that the links in the
     * clear are the links of the plaintext.
     *
     * @param nodeKey  the per-epoch key of the envelope's epoch
     * @param envelope the envelope
     * @param codec    the plaintext's codec, which the CID names
     * @return the plaintext, or empty when the key, the tag, or the AAD do not match
     * @throws NodeFormatException when the plaintext opens but its links differ from the envelope's
     */
    public static Optional<byte[]> open(byte[] nodeKey, Envelope envelope, int codec) {
        Objects.requireNonNull(nodeKey, "nodeKey");
        Objects.requireNonNull(envelope, "envelope");
        Optional<byte[]> plain = cipher(false, nodeKey, envelope.nonce(), aad(envelope.epoch()), envelope.ciphertext());
        if (plain.isEmpty()) {
            return plain;
        }
        if (!Arrays.equals(nonce(nodeKey, plain.get()), envelope.nonce())) {
            throw new NodeFormatException("sealed node nonce is not the keyed digest prefix of its plaintext");
        }
        List<String> links = codec == Cids.RAW ? List.of() : NodeCodec.links(plain.get());
        if (!links.equals(envelope.links())) {
            throw new NodeFormatException("sealed node links in the clear differ from the links of the plaintext");
        }
        return plain;
    }

    /** As {@link #open(byte[], Envelope, int)} for a structured node. */
    public static Optional<byte[]> open(byte[] nodeKey, Envelope envelope) {
        return open(nodeKey, envelope, Cids.DAG_CBOR);
    }

    // ------------------------------------------------------------ bound to a ring

    /** Whether the ring holds a key to seal under at this moment. */
    public boolean canSeal() {
        return writeEpoch().isPresent();
    }

    /** The epoch a seal would name now, when the ring holds a usable one. */
    public Optional<Long> writeEpoch() {
        long epoch;
        try {
            epoch = ring.writeEpoch(clock.instant());
        } catch (IllegalStateException none) {
            return Optional.empty();
        }
        return ring.sealingKey(epoch).map(k -> epoch);
    }

    /**
     * Seals a plaintext node under the ring's write epoch.
     *
     * @param plaintext the node or chunk bytes
     * @param codec     the plaintext's codec
     * @return the envelope bytes
     * @throws SealedNodeException when the ring holds no key to seal under
     */
    public byte[] seal(byte[] plaintext, int codec) {
        long epoch;
        GroupKey key;
        try {
            epoch = ring.writeEpoch(clock.instant());
            key = ring.sealingKey(epoch).orElse(null);
        } catch (IllegalStateException none) {
            throw new SealedNodeException(null, -1, "no content key epoch is in effect, so no node can be sealed");
        }
        if (key == null) {
            throw new SealedNodeException(null, epoch, "content key epoch " + epoch + " is not held, so no node can be sealed");
        }
        return seal(keyOf(key), epoch, plaintext, codec);
    }

    /**
     * Opens stored bytes when they are an envelope this ring can open; passes
     * through bytes that are not an envelope.
     *
     * @param cid   the CID, which names the plaintext's codec
     * @param bytes the stored bytes
     * @return the plaintext
     * @throws SealedNodeException when the bytes are an envelope and no held key of its epoch opens it
     * @throws NodeFormatException when the envelope is malformed or lies about its links
     */
    public byte[] open(String cid, byte[] bytes) {
        Objects.requireNonNull(cid, "cid");
        if (!isSealed(bytes)) {
            return bytes;
        }
        Envelope envelope = decode(bytes);
        int codec = Cids.parse(cid).codec();
        List<GroupKey> keys = ring.openingKeys(envelope.epoch());
        for (GroupKey key : keys) {
            Optional<byte[]> plain = open(keyOf(key), envelope, codec);
            if (plain.isPresent()) {
                return plain.get();
            }
        }
        if (!keys.isEmpty()) {
            ring.reportUnopenable(envelope.epoch());
        }
        throw new SealedNodeException(cid, envelope.epoch(), keys.isEmpty()
                ? "sealed under content key epoch " + envelope.epoch() + ", which this peer does not hold"
                : "sealed under content key epoch " + envelope.epoch() + " and no held key of that epoch opens it");
    }

    private byte[] keyOf(GroupKey key) {
        return derived.computeIfAbsent(key, NodeSealer::nodeKey);
    }

    private static Optional<byte[]> cipher(boolean forEncryption, byte[] key, byte[] nonce, byte[] aad, byte[] in) {
        GCMSIVBlockCipher cipher = new GCMSIVBlockCipher(AESEngine.newInstance());
        cipher.init(forEncryption, new AEADParameters(new KeyParameter(key), TAG_BITS, nonce, aad));
        byte[] out = new byte[cipher.getOutputSize(in.length)];
        int n = cipher.processBytes(in, 0, in.length, out, 0);
        try {
            n += cipher.doFinal(out, n);
        } catch (InvalidCipherTextException e) {
            return Optional.empty();
        }
        return Optional.of(n == out.length ? out : Arrays.copyOf(out, n));
    }
}
