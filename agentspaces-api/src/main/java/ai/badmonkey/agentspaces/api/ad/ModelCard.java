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


package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A model advertised to a group (SPEC §6.1b, v0.1.24; ISSUE-ModelCard FR-1): what it does ({@code tasks},
 * {@code modality}, {@code limits}), where it came from ({@code licence}, {@code provenance}: the upstream, the
 * weight root on the content swarm, the weight hash, the admitting identity), how it was judged ({@code evals}),
 * what it costs ({@code costHints}), where it may run and what it may see ({@code residency}: sites,
 * classifications, releasable), the {@code engine} that serves it, and the {@code servers} that hold it. The
 * maps are open; {@code engine} admits values a reader does not serve (CR-3). Signed and leased as every
 * advertisement; a model nobody admits never reaches a group (the platform's registry signs
 * {@code provenance.admittedBy}).
 *
 * @param id          the advertisement id, {@code aspace://<group>/model/<name>@<version>} by convention
 * @param issuer      the peer that published the card
 * @param group       the group it is published in
 * @param issued      when it was issued
 * @param ttl         how long it is valid
 * @param name        the model's name, the one a {@code ModelRequest} addresses
 * @param version     the model's version
 * @param tasks       the task kinds served: {@code chat}, {@code embed}, {@code rerank}, {@code classify},
 *                    {@code transcribe}, {@code vision}
 * @param modality    the modality, e.g. {@code text}, {@code text+image}
 * @param limits      {@code context}, {@code output}, and further limits as strings
 * @param licence     the licence identifier
 * @param provenance  {@code upstream}, {@code weightRoot}, {@code weightHash}, {@code admittedBy}
 * @param evals       the evaluation results, bounded at {@value #MAX_EVALS} entries
 * @param costHints   cost hints such as {@code pricePerThousandTokens}
 * @param residency   {@code sites}, {@code classifications}, {@code releasable}
 * @param engine      the engine that serves it: {@code provider}, {@code ollama}, {@code vllm}, {@code onnx}, or a reserved value
 * @param servers     the peers that hold it
 */
public record ModelCard(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        String name,
        String version,
        List<String> tasks,
        String modality,
        Map<String, String> limits,
        String licence,
        Map<String, String> provenance,
        Map<String, String> evals,
        Map<String, String> costHints,
        Map<String, String> residency,
        String engine,
        List<PeerId> servers) implements Advertisement {

    /** The task kinds a card may serve. */
    public static final List<String> TASKS = List.of("chat", "embed", "rerank", "classify", "transcribe", "vision");
    /** The engines a reader serves today; later values are reserved (CR-3). */
    public static final List<String> ENGINES = List.of("provider", "ollama", "vllm", "onnx", "tgi", "nim", "triton");
    /** The bound on {@code evals} (PR-1). */
    public static final int MAX_EVALS = 64;

    public ModelCard {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(name, "name");
        version = version == null ? "" : version;
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
        modality = modality == null ? "" : modality;
        limits = limits == null ? Map.of() : Map.copyOf(limits);
        licence = licence == null ? "" : licence;
        provenance = provenance == null ? Map.of() : Map.copyOf(provenance);
        evals = evals == null ? Map.of() : Map.copyOf(evals);
        if (evals.size() > MAX_EVALS) {
            throw new IllegalArgumentException("a ModelCard carries at most " + MAX_EVALS + " evals");
        }
        costHints = costHints == null ? Map.of() : Map.copyOf(costHints);
        residency = residency == null ? Map.of() : Map.copyOf(residency);
        engine = engine == null ? "" : engine;
        servers = servers == null ? List.of() : List.copyOf(servers);
    }

    /** Whether the card serves a task kind. */
    public boolean serves(String task) {
        return tasks.contains(task);
    }

    /** Whether the card may run at a site: no {@code residency.sites} means anywhere; else a comma-separated list. */
    public boolean residentAt(String site) {
        String sites = residency.get("sites");
        if (sites == null || sites.isBlank()) {
            return true;
        }
        for (String s : sites.split(",")) {
            if (s.trim().equals(site)) {
                return true;
            }
        }
        return false;
    }

    /** Whether the card may see a classification: no {@code residency.classifications} means any. */
    public boolean admitsClassification(String classification) {
        String cs = residency.get("classifications");
        if (cs == null || cs.isBlank()) {
            return true;
        }
        for (String c : cs.split(",")) {
            if (c.trim().equals(classification)) {
                return true;
            }
        }
        return false;
    }

    /** The card's price per thousand tokens from its hints, or the fallback. */
    public double pricePerThousandTokens(double fallback) {
        String p = costHints.get("pricePerThousandTokens");
        if (p == null || p.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(p);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
