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
 *
 * <p>While anyone listens ({@link #onChange}), the files are checked every
 * {@code refresh} as a task on the clock's time source (the node's, when the
 * clock is one), and a change tells the listeners at once
 * (srad/agentspaces-event-loop.md EL-L-14): a node then re-judges on the change
 * and never looks on a timer. A {@link #get()} past the refresh still checks too.
 */
public final class ReloadingChannelTrust implements SignallingChannelTrust {

    private static final System.Logger LOG = System.getLogger(ReloadingChannelTrust.class.getName());

    private final List<X509Certificate> authorities;
    private final List<Path> crlFiles;
    private final Duration refresh;
    private final boolean strictOnline;
    private final InstantSource clock;
    private volatile ChannelTrust current;
    private volatile List<String> lastModified;
    private volatile Instant nextCheck;
    private final List<Runnable> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** The refresh task while anyone listens; guarded by this. */
    private ai.badmonkey.agentspaces.common.time.Periodic refresher;

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
        return check(false);
    }

    /**
     * Checks the files now (or when due) and returns the trust; a change tells the listeners, outside the lock.
     */
    private ChannelTrust check(boolean force) {
        ChannelTrust before;
        ChannelTrust after;
        synchronized (this) {
            before = current;
            Instant now = clock.instant();
            if (force || !now.isBefore(nextCheck)) {
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
            }
            after = current;
        }
        if (after != before) {
            for (Runnable listener : listeners) {
                try {
                    listener.run();
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "a trust-change listener failed: " + e);
                }
            }
        }
        return after;
    }

    /**
     * Registers a listener told when a refresh replaces the trust (EL-L-14). The first listener starts the refresh
     * task, every {@code refresh} on the clock's time source; closing the last one stops it.
     */
    @Override
    public AutoCloseable onChange(Runnable listener) {
        Runnable l = Objects.requireNonNull(listener, "listener");
        synchronized (this) {
            listeners.add(l);
            if (refresher == null && !crlFiles.isEmpty()) {
                refresher = ai.badmonkey.agentspaces.common.time.Periodic.every(
                        ai.badmonkey.agentspaces.common.time.Waits.base(clock), refresh, "crl-refresh", () -> {
                            check(true);
                            return true;
                        });
            }
        }
        return () -> {
            ai.badmonkey.agentspaces.common.time.Periodic stop = null;
            synchronized (this) {
                listeners.remove(l);
                if (listeners.isEmpty() && refresher != null) {
                    stop = refresher;
                    refresher = null;
                }
            }
            if (stop != null) {
                stop.close();
            }
        };
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
