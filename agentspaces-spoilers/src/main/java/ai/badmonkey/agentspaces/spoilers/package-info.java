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
 * Spoiler peers (srad/spoiler-peers.md): scripts of misbehaviour keyed to observed frames, decoded items,
 * space events, counters and fleet time, run on the in-process fabric so a race found by load reproduces
 * every run. Test scope only: this module is never deployed, and an ArchUnit rule keeps every main-scope
 * class of every other module from using it (SP-G-02).
 */
package ai.badmonkey.agentspaces.spoilers;
