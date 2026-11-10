/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.discovery;

import ai.badmonkey.agentspaces.api.ad.Advertisement;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.AppCard;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.util.List;

/**
 * The two card kinds that spill, behind one vocabulary (SPEC §6.1a.5; AppCard origin and formats, 2026-11-09): an
 * {@link AppCard} and an {@link AgentCard} each have a parts record, a stub, and a completion.
 */
final class Spills {

    private Spills() {
    }

    /** Whether the kind spills at all. */
    static boolean spillable(Advertisement card) {
        return card instanceof AppCard || card instanceof AgentCard;
    }

    /** The card's spill reference, or null when it carries its parts inline. */
    static CardSpill spillOf(Advertisement card) {
        if (card instanceof AppCard app) {
            return app.spill();
        }
        if (card instanceof AgentCard agent) {
            return agent.spill();
        }
        return null;
    }

    /** The parts record of a full card. */
    static Object partsOf(Advertisement card) {
        if (card instanceof AppCard app) {
            return app.parts();
        }
        return ((AgentCard) card).parts();
    }

    /** The non-empty spill-set fields of a full card, in field order. */
    static List<String> nonEmptyOf(Advertisement card) {
        if (card instanceof AppCard app) {
            return app.parts().nonEmpty();
        }
        return ((AgentCard) card).parts().nonEmpty();
    }

    /** The stub of a full card. */
    @SuppressWarnings("unchecked")
    static <C extends Advertisement> C stub(C card, CardSpill reference) {
        if (card instanceof AppCard app) {
            return (C) app.toStub(reference);
        }
        return (C) ((AgentCard) card).toStub(reference);
    }

    /**
     * The full card of a stub from its parts document's bytes: decoded strictly under SPEC §9.1 as the kind's parts
     * record, its non-empty fields exactly {@code spill.parts}.
     *
     * @throws RuntimeException when the document does not complete the stub
     */
    @SuppressWarnings("unchecked")
    static <C extends Advertisement> C complete(C stub, byte[] document, CborCodec codec) {
        if (stub instanceof AppCard app) {
            return (C) app.complete(codec.fromWire(document, AppCard.Parts.class));
        }
        return (C) ((AgentCard) stub).complete(codec.fromWire(document, AgentCard.Parts.class));
    }
}
