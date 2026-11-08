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
package ai.badmonkey.agentspaces.pdt.blob;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.values.Codec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One entry of a published directory tree (srad/agentspaces-pdt.md §3.3, PD-B-05): the
 * value a {@code PMap<String, FileEntry>} holds under a relative path, after Git's
 * tree object. {@link #toMap()} and {@link #fromMap} give the CBOR shape the map
 * embeds, with the keys {@code root}, {@code size}, {@code mode}, and
 * {@code mediaType}.
 *
 * @param root      the blob root CID; for a symbolic link, the blob of the link target path
 * @param size      the file size in bytes
 * @param mode      the Git mode: {@link #REGULAR}, {@link #EXECUTABLE}, or {@link #SYMLINK}
 * @param mediaType the media type guessed from the file name, or {@code application/octet-stream}
 */
public record FileEntry(String root, long size, String mode, String mediaType) {

    /** A regular file. */
    public static final String REGULAR = "100644";

    /** An executable file. */
    public static final String EXECUTABLE = "100755";

    /** A symbolic link whose blob holds the target path. */
    public static final String SYMLINK = "120000";

    /** The media type of a file the table does not know. */
    public static final String OCTET_STREAM = "application/octet-stream";

    /** Validates the parts. */
    public FileEntry {
        Objects.requireNonNull(root, "root");
        Cids.parse(root);
        if (size < 0) {
            throw new IllegalArgumentException("negative size " + size);
        }
        Objects.requireNonNull(mode, "mode");
        if (!REGULAR.equals(mode) && !EXECUTABLE.equals(mode) && !SYMLINK.equals(mode)) {
            throw new IllegalArgumentException("unknown mode '" + mode + "'");
        }
        Objects.requireNonNull(mediaType, "mediaType");
    }

    /** The CBOR shape, in declaration order. */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("root", root);
        map.put("size", size);
        map.put("mode", mode);
        map.put("mediaType", mediaType);
        return map;
    }

    /**
     * An entry from its CBOR shape.
     *
     * @param map the decoded map
     * @return the entry
     * @throws IllegalArgumentException when a field is missing or of the wrong type
     */
    public static FileEntry fromMap(Map<String, Object> map) {
        Objects.requireNonNull(map, "map");
        if (!(map.get("root") instanceof String root) || !(map.get("size") instanceof Number size)
                || !(map.get("mode") instanceof String mode) || !(map.get("mediaType") instanceof String mediaType)) {
            throw new IllegalArgumentException("file entry needs root, size, mode, and mediaType: " + map.keySet());
        }
        return new FileEntry(root, size.longValue(), mode, mediaType);
    }

    /**
     * The codec a {@code PMap<String, FileEntry>} embeds entries with: the canonical
     * CBOR of {@link #toMap()} and {@link #fromMap} on the way back (PD-B-05).
     */
    @SuppressWarnings("unchecked")
    public static Codec<FileEntry> codec() {
        CborCodec cbor = CborCodec.defaultCodec();
        return new Codec<>() {
            @Override
            public byte[] encode(FileEntry value) {
                return cbor.toBytes(value.toMap());
            }

            @Override
            public FileEntry decode(byte[] bytes) {
                return fromMap(cbor.fromBytes(bytes, Map.class));
            }
        };
    }

    /** Whether the entry is a symbolic link. */
    public boolean symlink() {
        return SYMLINK.equals(mode);
    }
}
