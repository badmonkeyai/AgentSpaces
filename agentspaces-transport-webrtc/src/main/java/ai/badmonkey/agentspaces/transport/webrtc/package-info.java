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

/**
 * The {@code webrtc} binding of the Transport SPI (SPEC §5.5, §5.6; SRAD srad/agentspaces-js.md
 * §3.4, §3.5, §4.3, appendix B): {@link ai.badmonkey.agentspaces.transport.webrtc.WebRtcTransport}
 * over one data channel per peer connection, signalling through
 * {@link ai.badmonkey.agentspaces.transport.webrtc.RtcSignalCapability} as signed {@code PIPE_DATA}
 * frames of {@code aspace:cap/rtc-signal}, attestation by the DTLS fingerprint each side signs into
 * its signal, and the engine behind the package-private {@code DataChannelEngine} seam, whose first
 * implementation is libdatachannel through libdatachannel-java. Candidates and session
 * descriptions travel only inside signals to their target and are logged at {@code TRACE} alone.
 */
package ai.badmonkey.agentspaces.transport.webrtc;
