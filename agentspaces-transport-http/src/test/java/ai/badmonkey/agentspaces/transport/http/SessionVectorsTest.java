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
package ai.badmonkey.agentspaces.transport.http;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Hkdf;
import ai.badmonkey.agentspaces.common.crypto.X25519;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Session attestation parity (srad/agentspaces-js.md JS-H-05, JS-Q-TST-04):
 * for fixed X25519 scalars, nonces, and PeerIDs the Java node's hello body,
 * key derivation, and session frame equal the bytes the TypeScript client
 * computed independently (test/session.test.ts and the vectors printed from
 * src/session.ts on 2026-10-18), so a browser peer and a Java peer derive one
 * key and verify each other's tags. The golden generator carries the same
 * vectors into golden.json (session_*).
 */
class SessionVectorsTest {

    private static final HexFormat HEX = HexFormat.of();
    private static final byte[] X25519_PKCS8_PREFIX = HEX.parseHex("302e020100300506032b656e04220420");
    static final String A_PUBLIC = "a4e09292b651c278b9772c569f5fa9bb13d906b46ab68c9df9dc2b4409f8a209";
    static final String B_PUBLIC = "ce8d3ad1ccb633ec7b70c17814a5c76ecd029685050d344745ba05870e587d59";
    static final String KEY = "ab49ab9ec1de392e3a963a44e70915db202d44db8a9b9ae0fbf8cb90097db4ae";
    static final String HELLO = "bf6c657068656d6572616c4b65795820" + A_PUBLIC
            + "656e6f6e63655820aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa6672656d6f7465627a42646563686ff6ff";
    static final String ANSWER = "bf6c657068656d6572616c4b65795820" + B_PUBLIC
            + "656e6f6e63655820bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb6672656d6f7465627a41646563686f5820"
            + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff";
    static final String FRAME = "bf68656e76656c6f7065bf63766572036567726f7570667a47726f7570646b696e646450494e476466726f6d627a4162746f627a42"
            + "657374616d7072313733353638393630303030303a333a7a4164626f64794abf656e6f6e6365182affff6f73656e6465725075626c69634b6579f6"
            + "697369676e61747572655820d0f8023ce4766a793ce7e5e9f6e93d773192fab4c2f13581dbe170118c72c685ff";

    private static PrivateKey scalar(byte fill) throws Exception {
        byte[] raw = new byte[32];
        Arrays.fill(raw, fill);
        byte[] pkcs8 = new byte[X25519_PKCS8_PREFIX.length + 32];
        System.arraycopy(X25519_PKCS8_PREFIX, 0, pkcs8, 0, X25519_PKCS8_PREFIX.length);
        System.arraycopy(raw, 0, pkcs8, X25519_PKCS8_PREFIX.length, 32);
        return KeyFactory.getInstance("X25519").generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
    }

    private static byte[] nonce(int fill) {
        byte[] nonce = new byte[32];
        Arrays.fill(nonce, (byte) fill);
        return nonce;
    }

    /** PeerNode.deriveSessionKey, restated over the public helpers it uses. */
    private static byte[] derive(PrivateKey own, String self, byte[] ownNonce, byte[] remoteKey, String remote,
                                 byte[] remoteNonce) {
        byte[] shared = X25519.agree(own, remoteKey);
        boolean selfFirst = self.compareTo(remote) < 0;
        ByteArrayOutputStream info = new ByteArrayOutputStream();
        info.writeBytes("agentspaces session v1".getBytes(StandardCharsets.UTF_8));
        info.writeBytes((selfFirst ? self : remote).getBytes(StandardCharsets.UTF_8));
        info.write(0);
        info.writeBytes((selfFirst ? remote : self).getBytes(StandardCharsets.UTF_8));
        info.write(0);
        info.writeBytes(selfFirst ? ownNonce : remoteNonce);
        info.writeBytes(selfFirst ? remoteNonce : ownNonce);
        return Hkdf.derive(shared, new byte[0], info.toByteArray(), WireCodec.SESSION_MAC_LENGTH);
    }

    @Test
    void theHelloBodiesKeyAndSessionFrameMatchTheTypeScriptClient() throws Exception {
        CborCodec codec = CborCodec.defaultCodec();
        PrivateKey a = scalar((byte) 1);
        PrivateKey b = scalar((byte) 2);
        byte[] aPublic = X25519.rawPublicKey(KeyFactory.getInstance("X25519")
                .generatePublic(new java.security.spec.XECPublicKeySpec(
                        ((java.security.interfaces.XECPrivateKey) a).getParams(), publicU(a))));
        // The public keys the TypeScript client derived from the same scalars.
        assertThat(HEX.formatHex(aPublic)).isEqualTo(A_PUBLIC);
        byte[] bPublic = HEX.parseHex(B_PUBLIC);
        byte[] fromA = derive(a, "zA", nonce(0xaa), bPublic, "zB", nonce(0xbb));
        byte[] fromB = derive(b, "zB", nonce(0xbb), HEX.parseHex(A_PUBLIC), "zA", nonce(0xaa));
        assertThat(HEX.formatHex(fromA)).isEqualTo(KEY);
        assertThat(fromB).isEqualTo(fromA);

        assertThat(HEX.formatHex(codec.toBytes(new Bodies.SessionHello(HEX.parseHex(A_PUBLIC), nonce(0xaa),
                PeerId.of("zB"), null)))).isEqualTo(HELLO);
        assertThat(HEX.formatHex(codec.toBytes(new Bodies.SessionHello(bPublic, nonce(0xbb), PeerId.of("zA"),
                nonce(0xaa))))).isEqualTo(ANSWER);

        WireCodec wire = new WireCodec(codec);
        Envelope envelope = new Envelope(WireCodec.WIRE_VERSION, GroupId.of("zGroup"), Envelope.Kind.PING,
                PeerId.of("zA"), PeerId.of("zB"), new HlcTimestamp(1735689600000L, 3, "zA"),
                codec.toBytes(Map.of("nonce", 42)));
        byte[] frame = wire.encodeSession(envelope, fromA);
        assertThat(HEX.formatHex(frame)).isEqualTo(FRAME);
        // And the Java decoder accepts the TypeScript client's frame under the session, for its peer alone.
        assertThat(wire.decodeDetailed(HEX.parseHex(FRAME), Optional.empty(),
                new WireCodec.Session(PeerId.of("zA"), fromA))).map(WireCodec.Decoded::auth)
                .contains(WireCodec.Auth.SESSION);
        assertThat(wire.decodeDetailed(HEX.parseHex(FRAME), Optional.empty(),
                new WireCodec.Session(PeerId.of("zB"), fromA))).isEmpty();
    }

    /** The public u-coordinate of an X25519 private key, as the JDK computes it. */
    private static java.math.BigInteger publicU(PrivateKey key) throws Exception {
        java.security.KeyPairGenerator generator = java.security.KeyPairGenerator.getInstance("X25519");
        // The JDK derives the public key only through agreement or generation; derive it by agreeing
        // with the base point: X25519(scalar, 9) is the public key's u-coordinate (RFC 7748 §6.1).
        byte[] base = new byte[32];
        base[0] = 9;
        byte[] u = X25519.agree(key, base);
        byte[] reversed = new byte[u.length];
        for (int i = 0; i < u.length; i++) {
            reversed[i] = u[u.length - 1 - i];
        }
        return new java.math.BigInteger(1, reversed);
    }
}
