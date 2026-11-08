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
 * Blobs and files (srad/agentspaces-pdt.md §3.3, group B): {@link
 * ai.badmonkey.agentspaces.pdt.blob.FastCdc} cuts bytes into content-defined chunks
 * (PD-B-01), {@link ai.badmonkey.agentspaces.pdt.blob.BlobTree} indexes the chunks
 * as a prolly tree (PD-B-02), {@link ai.badmonkey.agentspaces.pdt.blob.PBlob} is the
 * persistent byte sequence with ranged reads and edits that share unchanged nodes
 * (PD-B-03, PD-B-04), and {@link ai.badmonkey.agentspaces.pdt.blob.TreePublisher}
 * publishes a directory as a map of {@link ai.badmonkey.agentspaces.pdt.blob.FileEntry}
 * (PD-B-05). Stability: API for every public type in this package.
 */
package ai.badmonkey.agentspaces.pdt.blob;
