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

package ai.badmonkey.agentspaces.transport.dtls;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.identity.tls.ChannelAttestation;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.tls.Certificate;
import org.bouncycastle.tls.DTLSClientProtocol;
import org.bouncycastle.tls.DTLSTransport;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCrypto;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCryptoProvider;

import java.io.IOException;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The DTLS binding of the transport SPI (spec §5.5; SRAD {@code srad/dtls-support.md}):
 * the fabric's frames over DTLS 1.2 (RFC 6347) on UDP, on BouncyCastle's DTLS
 * implementation, with Connection ID (RFC 9146) and a recovery sublayer that
 * makes each association a reliable, ordered frame channel. Addresses are
 * {@code host:port}; the advertised scheme is {@code dtls}.
 *
 * <p>Identity modes match the {@code tls} and {@code quic} bindings word for
 * word (spec §5.6): the identity-endorsed channel certificate by default, a
 * CA-issued credential and CA trust, or a refreshable trust consulted on every
 * handshake. A remote end presenting no certificate, or one that fails
 * attestation, still connects, unattested. {@link #unattested()} offers
 * encryption alone, for fleets that use no channel attestation; it never
 * reports {@link Capability#ATTESTING}, so a node requiring attestation
 * refuses it.
 *
 * <p>Pure Java: BouncyCastle carries DTLS, the JDK carries UDP and threads.
 * Every attestation rests on a full handshake (no resumption, ASF-021);
 * renegotiation and Heartbeat are refused; the extended master secret is
 * required.
 */
public final class DtlsTransport implements Transport, AutoCloseable {

    /** The advertised scheme. */
    public static final String SCHEME = "dtls";

    static final int DIAL_TIMEOUT_MILLIS = 10_000;

    private final ServingCredential credential;
    private final Supplier<ChannelTrust> trust;
    private final boolean attesting;
    private final DtlsCounters counters = new DtlsCounters();
    private final Set<DtlsAssociation> associations = ConcurrentHashMap.newKeySet();
    private final Set<AutoCloseable> listeners = ConcurrentHashMap.newKeySet();
    private volatile DtlsOptions options = DtlsOptions.defaults();
    private volatile Provider provider;
    private JcaTlsCrypto crypto;
    private Certificate bcCertificate;
    private ScheduledThreadPoolExecutor timers;
    private volatile boolean used;
    private volatile boolean closed;

    /**
     * Creates the transport in the default mode, generating this peer's
     * identity-endorsed channel certificate.
     *
     * @param identity the local peer identity endorsing the channel certificate
     * @throws GeneralSecurityException when certificate generation fails
     */
    public DtlsTransport(PeerIdentity identity) throws GeneralSecurityException {
        this(identity, null, null);
    }

    /**
     * Creates the transport in the enterprise-CA mode: this node serves the
     * given CA-issued credential, and peers attest only when their chain
     * validates under the trust and their leaf CN carries their PeerID. A null
     * {@code serving} keeps the identity-endorsed certificate for this node
     * while still requiring CA validation of peers; a null {@code trust} keeps
     * the identity-endorsed attestation.
     *
     * @param identity this node's peer identity
     * @param serving  the CA-issued credential to present, or null
     * @param trust    the CA trust peers must validate against, or null
     * @throws GeneralSecurityException when key material cannot be prepared
     */
    public DtlsTransport(PeerIdentity identity, ServingCredential serving, ChannelTrust trust)
            throws GeneralSecurityException {
        this(credentialFor(identity, serving), trust == null ? null : () -> trust, true);
    }

    /**
     * Creates the transport in the enterprise-CA mode with a refreshable
     * trust: the supplier is consulted on every handshake, so a refreshed CRL
     * makes a revoked peer's next handshake attest nothing.
     *
     * @param identity this node's peer identity
     * @param serving  the CA-issued credential to present, or null for the identity-endorsed certificate
     * @param trust    supplies the current CA trust; must never return null
     * @return the transport
     * @throws GeneralSecurityException when key material cannot be prepared
     */
    public static DtlsTransport withRefreshableTrust(PeerIdentity identity, ServingCredential serving,
                                                     Supplier<ChannelTrust> trust)
            throws GeneralSecurityException {
        return new DtlsTransport(credentialFor(identity, serving), Objects.requireNonNull(trust, "trust"), true);
    }

    /**
     * Creates a transport that encrypts and attests nobody (SRAD DT-B-15):
     * it presents a fresh self-signed P-256 certificate and reports no
     * attested peer, for fleets that run without channel attestation.
     *
     * @return the transport
     * @throws GeneralSecurityException when key generation fails
     */
    public static DtlsTransport unattested() throws GeneralSecurityException {
        return new DtlsTransport(ephemeralCredential(), null, false);
    }

    private DtlsTransport(ServingCredential credential, Supplier<ChannelTrust> trust, boolean attesting) {
        this.credential = credential;
        this.trust = trust;
        this.attesting = attesting;
    }

    private static ServingCredential credentialFor(PeerIdentity identity, ServingCredential serving)
            throws GeneralSecurityException {
        Objects.requireNonNull(identity, "identity");
        if (serving != null) {
            return serving;
        }
        ChannelCertificate channel = ChannelCertificate.generate(identity);
        return new ServingCredential(channel.tlsKey(), new X509Certificate[]{channel.certificate()});
    }

    private static ServingCredential ephemeralCredential() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair pair = generator.generateKeyPair();
        byte[] serial = new byte[9];
        new SecureRandom().nextBytes(serial);
        Instant now = Instant.now().minus(1, ChronoUnit.HOURS);
        X500Name name = new X500Name("CN=agentspaces-dtls-unattested");
        try {
            X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                    new JcaX509v3CertificateBuilder(name, new BigInteger(1, serial), Date.from(now),
                            Date.from(now.plus(365, ChronoUnit.DAYS)), name, pair.getPublic())
                            .build(new JcaContentSignerBuilder("SHA256withECDSA").build(pair.getPrivate())));
            return new ServingCredential(pair.getPrivate(), new X509Certificate[]{certificate});
        } catch (OperatorCreationException e) {
            throw new GeneralSecurityException("cannot sign the ephemeral certificate", e);
        }
    }

    // ------------------------------------------------------------ tuning

    /**
     * Sets the tunables; valid only before the first {@link #dial} or {@link #listen}.
     *
     * @param options the options
     * @return this transport
     * @throws IllegalStateException after first use
     */
    public DtlsTransport options(DtlsOptions options) {
        requireUnused();
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    /**
     * Sets the JCA provider behind BouncyCastle's crypto (SRAD DT-F-07), for
     * example a FIPS-validated provider; valid only before first use. The
     * default is the JDK's installed providers.
     *
     * @param provider the provider
     * @return this transport
     * @throws IllegalStateException after first use
     */
    public DtlsTransport cryptoProvider(Provider provider) {
        requireUnused();
        this.provider = Objects.requireNonNull(provider, "provider");
        return this;
    }

    /** @return the options in force */
    public DtlsOptions options() {
        return options;
    }

    /** @return this transport's counters */
    public DtlsCounters counters() {
        return counters;
    }

    private void requireUnused() {
        if (used) {
            throw new IllegalStateException("DTLS options are fixed once the transport has dialed or listened");
        }
    }

    // --------------------------------------------------------------- SPI

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return attesting
                ? Set.of(Capability.ENCRYPTED, Capability.ATTESTING, Capability.RELIABLE, Capability.ORDERED)
                : Set.of(Capability.ENCRYPTED, Capability.RELIABLE, Capability.ORDERED);
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        prepare();
        InetSocketAddress remote = BcSupport.parse(address);
        if (remote.isUnresolved()) {
            throw new IOException("cannot resolve " + address);
        }
        DtlsOptions opts = options;
        DatagramSocket socket = BcSupport.dialSocket(remote);
        PeerDatagramTransport path = new PeerDatagramTransport(
                (buf, off, len, to) -> socket.send(new DatagramPacket(buf, off, len, to)),
                opts.mtu(), remote, socket::close);
        Thread.ofVirtual().name("dtls-dial-" + remote).start(() -> readLoop(socket, path));
        AspaceDtlsClient client = new AspaceDtlsClient(crypto, credential, bcCertificate,
                BcSupport.versions(opts.protocols()), opts.connectionId(), DIAL_TIMEOUT_MILLIS);
        DTLSTransport dtls;
        try {
            dtls = new DTLSClientProtocol().connect(client, path);
        } catch (IOException | RuntimeException e) {
            counters.handshakesFailed.increment();
            path.close();
            throw new IOException("cannot reach " + address + " over DTLS: " + e.getMessage(), e);
        }
        X509Certificate[] chain = BcSupport.x509(client.presented());
        DtlsAssociation[] self = new DtlsAssociation[1];
        DtlsAssociation association = new DtlsAssociation(RecordLayer.dtls(dtls), path, attest(chain), chain, opts, timers,
                counters, () -> untrack(self[0]));
        self[0] = association;
        counters.associationsOpened.increment();
        track(association);
        association.start();
        return association;
    }

    /**
     * A dialer's socket is unconnected, so it keeps working if this host's
     * address changes (DT-K-05); it accepts datagrams from the dialed peer only.
     */
    private void readLoop(DatagramSocket socket, PeerDatagramTransport path) {
        byte[] buf = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        while (!socket.isClosed()) {
            try {
                packet.setLength(buf.length);
                socket.receive(packet);
            } catch (IOException e) {
                return;
            }
            InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
            if (!source.equals(path.remote())) {
                counters.datagramsDropped.increment();
                continue;
            }
            byte[] bytes = java.util.Arrays.copyOf(buf, packet.getLength());
            if (!path.offer(new PeerDatagramTransport.Datagram(bytes, source))) {
                counters.datagramsDropped.increment();
            }
        }
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        prepare();
        DtlsListener listener = new DtlsListener(this, BcSupport.parse(bindAddress), options, onAccept);
        listeners.add(listener);
        return () -> {
            listeners.remove(listener);
            listener.close();
        };
    }

    /**
     * Listens and returns the bound port, for binding to port 0 in tests and tools.
     *
     * @param bindAddress {@code host:port}, port 0 for any
     * @param onAccept    invoked for each accepted association
     * @return the handle and the bound port
     * @throws IOException if the address cannot be bound
     */
    Bound listenBound(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        prepare();
        DtlsListener listener = new DtlsListener(this, BcSupport.parse(bindAddress), options, onAccept);
        listeners.add(listener);
        return new Bound(listener, listener.port());
    }

    record Bound(DtlsListener listener, int port) {
    }

    /**
     * Closes every association (each sends {@code CLOSE(0)} first), then every
     * listener, then the timer. Idempotent.
     */
    @Override
    public void close() {
        closed = true;
        for (DtlsAssociation association : associations) {
            association.close();
        }
        associations.clear();
        for (AutoCloseable listener : listeners) {
            try {
                listener.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
        listeners.clear();
        synchronized (this) {
            if (timers != null) {
                timers.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------- internals

    private synchronized void prepare() throws IOException {
        if (closed) {
            throw new IOException("transport closed");
        }
        used = true;
        if (crypto != null) {
            return;
        }
        JcaTlsCryptoProvider factory = new JcaTlsCryptoProvider();
        if (provider != null) {
            factory.setProvider(provider);
        }
        crypto = factory.create(new SecureRandom());
        bcCertificate = BcSupport.certificate(crypto, credential.chain());
        BcSupport.suitesFor(crypto, credential.key()); // fail now when the provider supports no suite
        timers = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "dtls-timer");
            thread.setDaemon(true);
            return thread;
        });
        timers.setRemoveOnCancelPolicy(true);
    }

    JcaTlsCrypto crypto() {
        return crypto;
    }

    ServingCredential credential() {
        return credential;
    }

    Certificate bcCertificate() {
        return bcCertificate;
    }

    ScheduledExecutorService timers() {
        return timers;
    }

    Optional<PeerId> attest(X509Certificate[] chain) {
        return attesting ? ChannelAttestation.attest(chain, trust) : Optional.empty();
    }

    void track(DtlsAssociation association) {
        associations.add(association);
        if (closed) {
            association.close();
        }
    }

    void untrack(DtlsAssociation association) {
        if (association != null) {
            associations.remove(association);
        }
    }

    /** Live associations, for tests and the console. */
    List<DtlsAssociation> associations() {
        return List.copyOf(associations);
    }
}
