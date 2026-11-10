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
package ai.badmonkey.agentspaces.capabilities.vote;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * HLC round six, 2026-11-08 (owner ruling F1, appendix A of the wire encoding): a proposal and a
 * ballot are protocol records that travel as entry payloads, so a payload whose field holds an
 * item of the wrong kind, or whose fields arrive in another order, is no proposal and no ballot.
 * Jackson once read a quorum of 2.7 as 2, so a writer could count differently at each reader.
 */
class VoteWireRecordTest {

    private static final HexFormat HEX = HexFormat.of();

    private final CborCodec codec = CborCodec.defaultCodec();

    @Test
    void aProposalOrABallotInItsOwnEncodingDecodes() {
        VoteCapability.Proposal proposal = new VoteCapability.Proposal("p1", "lunch?", List.of("yes", "no"), 2);
        assertThat(codec.fromDocument(codec.toBytes(proposal), VoteCapability.Proposal.class)).isEqualTo(proposal);
        VoteCapability.Ballot ballot = new VoteCapability.Ballot("p1", "yes", "zPeer/agent");
        assertThat(codec.fromDocument(codec.toBytes(ballot), VoteCapability.Ballot.class)).isEqualTo(ballot);
    }

    @Test
    void aProposalOrABallotWithAWrongTypedOrReorderedFieldIsUndecodable() {
        String proposal = HEX.formatHex(codec.toBytes(
                new VoteCapability.Proposal("p1", "lunch?", List.of("yes", "no"), 2)));
        String quorum = "6671756f72756d" + "02";
        Map<String, String> relabels = Map.of(
                "a quorum of 2.7", proposal.replace(quorum, "6671756f72756d" + "fb400599999999999a"),
                "a quorum as text", proposal.replace(quorum, "6671756f72756d" + "6132"),
                "an unknown field", proposal.replace(quorum, quorum + "6178f5"));
        relabels.forEach((what, hex) -> assertThatThrownBy(
                () -> codec.fromDocument(HEX.parseHex(hex), VoteCapability.Proposal.class))
                .as(what).isInstanceOf(UncheckedIOException.class));
        String ballot = HEX.formatHex(codec.toBytes(new VoteCapability.Ballot("p1", "yes", "zPeer/agent")));
        String reordered = ballot.replace("6a70726f706f73616c4964" + "627031" + "666f7074696f6e" + "63796573",
                "666f7074696f6e" + "63796573" + "6a70726f706f73616c4964" + "627031");
        assertThat(reordered).isNotEqualTo(ballot);
        assertThatThrownBy(() -> codec.fromDocument(HEX.parseHex(reordered), VoteCapability.Ballot.class))
                .as("fields out of order").isInstanceOf(UncheckedIOException.class);
    }
}
