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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * A collection is an {@code AssetCard} that lists the roots a holder serves
 * (srad/large-networks-bulk-data.md §3.8, group K). This provider advertises one
 * card per named collection, {@code kind=content-collection} under {@code access},
 * and answers a {@code DataQuery} with pages of manifest summaries, through the
 * connector runtime's existing choreography: wire it as the provider of a
 * {@code ConnectorRuntime} on the group's data space. Roots published with
 * {@code replicate: false} are reachable only this way, and a fetcher gets their
 * manifest with {@code MANIFEST_WANT}.
 */
public final class CollectionAssetProvider implements AssetProvider {

    /** The {@code access.kind} value every collection card carries. */
    public static final String KIND = "content-collection";

    /** The query parameters a collection understands (LB-K-03). */
    public static final String PARAM_TAG = "tag";
    public static final String PARAM_MEDIA_TYPE = "mediaType";
    public static final String PARAM_NAME_PREFIX = "namePrefix";
    public static final String PARAM_ORIGIN = "origin";
    public static final String PARAM_MAX_AGE_MILLIS = "maxAgeMillis";
    public static final String PARAM_AFTER = "after";
    public static final String PARAM_LIMIT = "limit";

    static final int DEFAULT_LIMIT = 100;
    static final int MAX_LIMIT = 1_000;

    /** One named collection: which of the holder's roots it lists. */
    public record Collection(String name, String description, Predicate<Manifest> includes, Duration freshness) {
        public Collection {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(includes, "includes");
            description = description == null ? "" : description;
            freshness = freshness == null ? Duration.ofMinutes(5) : freshness;
        }

        /** Every root the holder serves. */
        public static Collection all(String name, String description) {
            return new Collection(name, description, m -> true, null);
        }

        /** The roots carrying a tag value in {@code attrs}. */
        public static Collection tagged(String name, String description, String key, String value) {
            return new Collection(name, description,
                    m -> m.attrs() != null && value.equals(m.attrs().get(key)), null);
        }
    }

    private final Content content;
    private final List<Collection> collections;

    /**
     * Advertises collections over a content capability's store.
     *
     * @param content     the capability whose store holds the roots
     * @param collections the collections to advertise
     */
    public CollectionAssetProvider(Content content, List<Collection> collections) {
        this.content = Objects.requireNonNull(content, "content");
        this.collections = List.copyOf(collections);
        content.registerCollection(this);
    }

    /** The collections this provider advertises (LB-K-07). */
    public List<Collection> collections() {
        return collections;
    }

    /** How many roots a collection lists now (LB-K-07: the console's row count). */
    public int rowCount(Collection collection) {
        return (int) rootsOf(collection).count();
    }

    /** Whether any collection here lists this manifest's root (LB-K-07). */
    public boolean advertises(Manifest manifest) {
        return collections.stream().anyMatch(c -> rootsOf(c).anyMatch(m -> m.root().equals(manifest.root())));
    }

    /**
     * The card's description for the embedder (LB-K-08): the operator's sentence, then what the
     * collection holds now in words semantic discovery can match ("terrain tiles for sector 7"),
     * the names, media types, and attr values of up to a dozen roots.
     */
    private String describe(Collection c) {
        List<Manifest> listed = rootsOf(c).limit(12).toList();
        if (listed.isEmpty()) {
            return c.description();
        }
        java.util.Set<String> words = new java.util.LinkedHashSet<>();
        for (Manifest m : listed) {
            if (m.name() != null) {
                words.add(m.name());
            }
            if (m.mediaType() != null) {
                words.add(m.mediaType());
            }
            if (m.attrs() != null) {
                m.attrs().forEach((k, v) -> words.add(k + " " + v));
            }
        }
        return c.description() + " Holds " + rootsOf(c).count() + " artifacts: " + String.join(", ", words) + ".";
    }

    /** The asset name of a collection, as its card and a query name it. */
    public static String assetName(String collection) {
        return "content:" + collection;
    }

    @Override
    public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
        List<AssetCard> cards = new ArrayList<>();
        for (Collection c : collections) {
            int count = (int) rootsOf(c).count();
            Map<String, String> access = new LinkedHashMap<>();
            access.put("kind", KIND);
            access.put("query", String.join(",", PARAM_TAG, PARAM_MEDIA_TYPE, PARAM_NAME_PREFIX, PARAM_ORIGIN,
                    PARAM_MAX_AGE_MILLIS, PARAM_AFTER, PARAM_LIMIT));
            access.put("roots", String.valueOf(count));
            cards.add(new AssetCard(
                    "aspace://" + group.value() + "/asset/content/" + c.name() + "/" + issuer.value(),
                    issuer, group, now, Duration.ofMinutes(15),
                    assetName(c.name()),
                    "aspace://" + group.value() + "/content/" + c.name(),
                    describe(c),
                    "rows(root,name,mediaType,size,pieces,created,origin,attrs,holders)",
                    c.freshness().toString(),
                    Map.of(),
                    access));
        }
        return cards;
    }

    @Override
    public QueryResult query(String asset, Map<String, String> parameters) {
        Optional<Collection> collection = collections.stream()
                .filter(c -> assetName(c.name()).equals(asset)).findFirst();
        if (collection.isEmpty()) {
            return QueryResult.failed("unknown collection " + asset);
        }
        Map<String, String> params = parameters == null ? Map.of() : parameters;
        int limit = DEFAULT_LIMIT;
        try {
            if (params.containsKey(PARAM_LIMIT)) {
                limit = Math.max(1, Math.min(MAX_LIMIT, Integer.parseInt(params.get(PARAM_LIMIT))));
            }
        } catch (NumberFormatException e) {
            return QueryResult.failed("limit is not a number");
        }
        long maxAge = -1;
        try {
            if (params.containsKey(PARAM_MAX_AGE_MILLIS)) {
                maxAge = Long.parseLong(params.get(PARAM_MAX_AGE_MILLIS));
            }
        } catch (NumberFormatException e) {
            return QueryResult.failed("maxAgeMillis is not a number");
        }
        String tag = params.get(PARAM_TAG);
        String mediaType = params.get(PARAM_MEDIA_TYPE);
        String prefix = params.get(PARAM_NAME_PREFIX);
        String origin = params.get(PARAM_ORIGIN);
        String after = params.get(PARAM_AFTER);
        long now = System.currentTimeMillis();
        // A page reads only the manifests it needs (LB-Q-PRF-08): the roots are ordered and the
        // cursor applied by name alone, and manifests load one by one until the page is full, so a
        // page of 100 over 40,000 roots touches about 100 manifests rather than all of them.
        List<Manifest> page = new ArrayList<>();
        for (String root : servedRoots(after)) {
            if (page.size() >= limit) {
                break;
            }
            Optional<Manifest> listed = listable(collection.get(), root);
            if (listed.isEmpty()) {
                continue;
            }
            Manifest m = listed.get();
            if ((tag == null || (m.attrs() != null && m.attrs().containsValue(tag)))
                    && (mediaType == null || mediaType.equals(m.mediaType()))
                    && (prefix == null || (m.name() != null && m.name().startsWith(prefix)))
                    && (origin == null || origin.equals(m.origin().value()))
                    && (maxAge < 0 || now - m.created() <= maxAge)) {
                page.add(m);
            }
        }
        // one replica read for the whole page's holder counts
        Map<String, Integer> holders = content.spaces().completeHolders(page.stream().map(Manifest::root).toList());
        List<Map<String, String>> rows = new ArrayList<>(page.size());
        for (Manifest m : page) {
            rows.add(row(m, holders.getOrDefault(m.root(), 0)));
        }
        return QueryResult.of(rows);
    }

    /**
     * The roots this group's swarm serves, in root order, after {@code after} when it is given.
     * LB-K-06: only roots this group's swarm serves; a store shared with another group holds more.
     */
    private List<String> servedRoots(String after) {
        java.util.Set<String> held = content.store().roots();
        java.util.Set<String> here = content.rootsServedHere(held);
        return held.stream()
                .filter(here::contains)
                .filter(root -> after == null || root.compareTo(after) > 0)
                .sorted()
                .toList();
    }

    /** A root's manifest when the root is complete here and the collection includes it. */
    private Optional<Manifest> listable(Collection c, String root) {
        return content.store().manifest(root)
                .filter(m -> content.store().have(m.root()).cardinality() >= m.pieces())
                .filter(c.includes());
    }

    private java.util.stream.Stream<Manifest> rootsOf(Collection c) {
        return servedRoots(null).stream()
                .map(root -> listable(c, root))
                .flatMap(Optional::stream);
    }

    private Map<String, String> row(Manifest m, int completeHolders) {
        Map<String, String> row = new LinkedHashMap<>();
        row.put("root", m.root());
        row.put("name", m.name() == null ? "" : m.name());
        row.put("mediaType", m.mediaType() == null ? "" : m.mediaType());
        row.put("size", String.valueOf(m.size()));
        row.put("pieces", String.valueOf(m.pieces()));
        row.put("created", String.valueOf(m.created()));
        row.put("origin", m.origin().value());
        row.put("attrs", m.attrs() == null ? "" : attrsOf(m.attrs()));
        row.put("holders", String.valueOf(Math.max(1, completeHolders)));
        return row;
    }

    private static String attrsOf(Map<String, String> attrs) {
        StringBuilder sb = new StringBuilder();
        new java.util.TreeMap<>(attrs).forEach((k, v) -> {
            if (sb.length() > 0) {
                sb.append(';');
            }
            sb.append(k).append('=').append(v);
        });
        return sb.toString();
    }
}
