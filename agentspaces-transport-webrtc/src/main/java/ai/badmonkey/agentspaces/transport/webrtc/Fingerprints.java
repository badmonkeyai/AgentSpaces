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

package ai.badmonkey.agentspaces.transport.webrtc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * The DTLS certificate fingerprints of a session description (RFC 8122 {@code a=fingerprint}
 * lines), in the form the signed {@code fp} list carries: {@code "sha-256 " + uppercase
 * colon-separated hex} (SRAD srad/agentspaces-js.md JS-W-04, appendix B). A remote SDP whose
 * fingerprint lines differ from the remote's signed {@code fp} is refused; DTLS then refuses a
 * certificate whose fingerprint differs from the SDP, so an open channel proves possession of a
 * key the remote PeerID signed for.
 */
final class Fingerprints {

    private static final Pattern LINE = Pattern.compile("^a=fingerprint:\\s*([A-Za-z0-9-]+)\\s+([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2})+)\\s*$");
    private static final Pattern ENTRY = Pattern.compile("^([A-Za-z0-9-]+)\\s+([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2})+)$");

    private Fingerprints() {
    }

    /** The fingerprints an SDP presents, normalized, in order of appearance without duplicates. */
    static List<String> of(String sdp) {
        List<String> out = new ArrayList<>(2);
        for (String raw : sdp.split("\r?\n")) {
            var m = LINE.matcher(raw.strip());
            if (m.matches()) {
                String entry = m.group(1).toLowerCase(Locale.ROOT) + " " + m.group(2).toUpperCase(Locale.ROOT);
                if (!out.contains(entry)) {
                    out.add(entry);
                }
            }
        }
        return out;
    }

    /** The normalized form of one {@code fp} entry, or empty when it is not one. */
    static Optional<String> normalize(String entry) {
        if (entry == null) {
            return Optional.empty();
        }
        var m = ENTRY.matcher(entry.strip());
        if (!m.matches()) {
            return Optional.empty();
        }
        return Optional.of(m.group(1).toLowerCase(Locale.ROOT) + " " + m.group(2).toUpperCase(Locale.ROOT));
    }

    /** Whether the SDP's fingerprint lines are exactly the signed {@code fp} list, as sets. */
    static boolean matches(String sdp, List<String> fp) {
        TreeSet<String> signed = new TreeSet<>();
        for (String entry : fp) {
            Optional<String> normalized = normalize(entry);
            if (normalized.isEmpty()) {
                return false;
            }
            signed.add(normalized.get());
        }
        TreeSet<String> presented = new TreeSet<>(of(sdp));
        return !presented.isEmpty() && presented.equals(signed);
    }
}
