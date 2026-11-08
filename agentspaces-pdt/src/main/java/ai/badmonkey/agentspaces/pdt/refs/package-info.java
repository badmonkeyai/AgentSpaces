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
 * Refs (srad/agentspaces-pdt.md §3.6, group R): a named, leased, signed
 * {@code PRef} head per version in the ref space, read and advanced through
 * {@link ai.badmonkey.agentspaces.pdt.refs.Ref} handles that the
 * {@link ai.badmonkey.agentspaces.pdt.refs.Refs} engine serves. The engine
 * folds concurrent heads with the merge of §3.6.1, carries small deltas inline
 * on the head (PD-X-07), fetches larger ones over the tree exchange with the
 * retained roots as haves, renews and rewrites head leases (PD-R-08), and
 * reports each merge decision as a
 * {@link ai.badmonkey.agentspaces.pdt.refs.PConflict}. Stability: API for
 * {@code Ref}, {@code Heads}, {@code PConflict}, and {@code MergeFunction};
 * {@code Refs} is the engine the {@code Pdt} facade owns.
 */
package ai.badmonkey.agentspaces.pdt.refs;
