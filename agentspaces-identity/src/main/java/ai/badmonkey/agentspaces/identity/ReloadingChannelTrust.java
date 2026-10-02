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
package ai.badmonkey.agentspaces.identity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A channel trust whose CRLs are re-read from files on a cadence (SPEC §5.6,
 * v0.1.13, item 9): an operator, or a sidecar, replaces the CRL files and the
 * fabric picks them up without a restart. The supplier yields the same
 * {@link ChannelTrust} instance until a file actually changes, which is what
 * tells a node to re-judge its live connections. A file that fails to read or
 * parse keeps the last good trust and is retried at the next refresh.
 */
public final class ReloadingChannelTrust implements Supplier<ChannelTrust> {

    private static final System.Logger LOG = System.getLogger(ReloadingChannelTrust.class.getName());

    private final List<X509Certificate> authorities;
    private final List<Path> crlFiles;
    private final Duration refresh;
    private final boolean strictOnline;
    private final InstantSource clock;
    private volatile ChannelTrust current;
    private volatile List<String> lastModified;
    private volatile Instant nextCheck;

    /**
     * Loads the trust now, failing when a CRL file cannot be read.
     *
     * @param authorities  the CA certificates to anchor trust in
     * @param crlFiles     the CRL files (DER or PEM) to load and re-read
     * @param refresh      how often to check the files for changes
     * @param strictOnline whether handshakes may also fetch revocation online
     * @param clock        the clock the cadence follows
     * @throws GeneralSecurityException when a CRL file cannot be read or parsed
     */
    public ReloadingChannelTrust(List<X509Certificate> authorities, List<Path> crlFiles, Duration refresh,
                                 boolean strictOnline, InstantSource clock) throws GeneralSecurityException {
        this.authorities = List.copyOf(authorities);
        this.crlFiles = List.copyOf(crlFiles);
        this.refresh = Objects.requireNonNull(refresh, "refresh");
        if (refresh.isNegative() || refresh.isZero()) {
            throw new IllegalArgumentException("CRL refresh must be positive");
        }
        this.strictOnline = strictOnline;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lastModified = modifiedTimes();
        this.current = build(load());
        this.nextCheck = clock.instant().plus(refresh);
    }

    @Override
    public ChannelTrust get() {
        Instant now = clock.instant();
        if (crlFiles.isEmpty() || now.isBefore(nextCheck)) {
            return current;
        }
        synchronized (this) {
            if (now.isBefore(nextCheck)) {
                return current;
            }
            nextCheck = now.plus(refresh);
            List<String> modified = modifiedTimes();
            if (!modified.equals(lastModified)) {
                try {
                    current = build(load());
                    lastModified = modified;
                } catch (GeneralSecurityException | RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "cannot reload CRLs " + crlFiles
                            + "; keeping the last good trust: " + e.getMessage());
                }
            }
            return current;
        }
    }

    private ChannelTrust build(List<X509CRL> crls) {
        ChannelTrust trust = crls.isEmpty() ? ChannelTrust.of(authorities)
                : ChannelTrust.withCrls(authorities, crls);
        return strictOnline ? trust.strictOnline() : trust;
    }

    private List<X509CRL> load() throws GeneralSecurityException {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        List<X509CRL> crls = new ArrayList<>();
        for (Path file : crlFiles) {
            try (InputStream in = Files.newInputStream(file)) {
                crls.add((X509CRL) factory.generateCRL(in));
            } catch (IOException e) {
                throw new GeneralSecurityException("cannot read CRL " + file, e);
            }
        }
        return crls;
    }

    /** Each file's modification time and size: what a replacement changes. */
    private List<String> modifiedTimes() {
        List<String> times = new ArrayList<>();
        for (Path file : crlFiles) {
            try {
                times.add(Files.getLastModifiedTime(file).toMillis() + ":" + Files.size(file));
            } catch (IOException e) {
                times.add("unreadable");
            }
        }
        return times;
    }
}
