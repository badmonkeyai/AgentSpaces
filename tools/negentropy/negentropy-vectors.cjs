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

// PDT 2 phase 0 (P2-K-01; srad/agentspaces-pdt2.md §3.6): the negentropy conformance vectors.
//
// Negentropy publishes no static vectors: its conformance suite drives each port through a
// stdin/stdout harness and diffs the messages byte for byte (test/test.pl, "delta tests"). This
// program records the same thing statically. It runs the reference implementation (js/Negentropy.js
// at the pinned commit below, checked by sha-256) between a client and a server over item sets
// built from a recipe, and writes every message of every round with the outcome, so the Java,
// Python, TypeScript and Rust ports of phase 5 reproduce each message exactly.
//
//   git clone https://github.com/hoytech/negentropy && git -C negentropy checkout <commit>
//   node tools/negentropy/negentropy-vectors.cjs --reference negentropy/js/Negentropy.js \
//        --out ../agentspaces-spec/negentropy-vectors.json [--check]
//
// With --check it writes nothing and exits 1 unless the file holds exactly what it computes.

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const REFERENCE = {
    repository: 'https://github.com/hoytech/negentropy',
    commit: 'fe0bb8ad4d636776e2e16c4f2369c4eae47f1b34',
    file: 'js/Negentropy.js',
    sha256: '0ccda2fb5343f97a0679442aec55e258cab09ddc775a3f8af87ada7b3c02cb50',
    license: 'MIT, (C) 2023 Doug Hoyte',
};

// Epoch seconds. The reference codes varints with 32-bit JavaScript operators (encodeVarInt uses
// `>>>= 7`, decodeVarInt `<< 7`), so a timestamp of 2^31 or more never reconciles in it: epoch
// milliseconds, which srad/agentspaces-pdt2.md §3.6 names, loop forever. Phase 5 decides the unit.
const TIMESTAMP_BASE = 1792320000;

// P2-K-04: a session ends within 64 rounds.
const MAX_ROUNDS = 64;

function args() {
    const out = { check: false };
    const argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === '--reference') out.reference = argv[++i];
        else if (argv[i] === '--out') out.out = argv[++i];
        else if (argv[i] === '--check') out.check = true;
        else throw Error('unknown argument ' + argv[i]);
    }
    if (!out.reference || !out.out) throw Error('usage: --reference <Negentropy.js> --out <file> [--check]');
    return out;
}

/** Item i of a universe: id = sha256("aspace:negentropy:<universe>:<i>"), timestamp = base + floor(i / perTimestamp). */
function item(universe, i) {
    const id = crypto.createHash('sha256').update(`aspace:negentropy:${universe.name}:${i}`, 'utf8').digest('hex');
    return { timestamp: TIMESTAMP_BASE + Math.floor(i / universe.perTimestamp), id };
}

function members(universe, ranges) {
    const out = [];
    for (const [from, to] of ranges) for (let i = from; i < to; i++) out.push(item(universe, i));
    return out;
}

async function reconcile(Negentropy, NegentropyStorageVector, c) {
    const build = (items) => {
        const storage = new NegentropyStorageVector();
        for (const it of items) storage.insert(it.timestamp, it.id);
        storage.seal();
        return new Negentropy(storage, c.frameSizeLimit);
    };
    const clientItems = members(c.universe, c.client);
    const serverItems = members(c.universe, c.server);
    const client = build(clientItems);
    const server = build(serverItems);
    const messages = [];
    let have = [];
    let need = [];
    let query = await client.initiate();
    for (;;) {
        if (messages.length / 2 >= MAX_ROUNDS) throw Error(`case ${c.name}: no agreement within ${MAX_ROUNDS} rounds`);
        messages.push({ from: 'client', hex: query });
        const [reply] = await server.reconcile(query);
        messages.push({ from: 'server', hex: reply });
        const [next, haveIds, needIds] = await client.reconcile(reply);
        have = have.concat(haveIds);
        need = need.concat(needIds);
        if (next === null) break;
        query = next;
    }
    have.sort();
    need.sort();
    const serverIds = new Set(serverItems.map((it) => it.id));
    const clientIds = new Set(clientItems.map((it) => it.id));
    const expectHave = clientItems.map((it) => it.id).filter((id) => !serverIds.has(id)).sort();
    const expectNeed = serverItems.map((it) => it.id).filter((id) => !clientIds.has(id)).sort();
    if (JSON.stringify(have) !== JSON.stringify(expectHave) || JSON.stringify(need) !== JSON.stringify(expectNeed)) {
        throw Error(`case ${c.name}: the reference reconciled to another difference`);
    }
    for (const m of messages) {
        if (c.frameSizeLimit && m.hex.length / 2 > c.frameSizeLimit) throw Error(`case ${c.name}: a frame passes the limit`);
    }
    return {
        rounds: messages.length / 2,
        bytes: messages.reduce((n, m) => n + m.hex.length / 2, 0),
        messages,
        have,
        need,
    };
}

const U_SMALL = { name: 'small', perTimestamp: 1 };
const U_SHARED = { name: 'shared-timestamps', perTimestamp: 50 };
const U_TEN_K = { name: 'ten-thousand', perTimestamp: 1 };
const U_HUNDRED_K = { name: 'hundred-thousand', perTimestamp: 1 };

/** Cases whose every message is recorded. */
const CASES = [
    { name: 'empty-both', universe: U_SMALL, client: [], server: [], frameSizeLimit: 0 },
    { name: 'empty-client', universe: U_SMALL, client: [], server: [[0, 10]], frameSizeLimit: 0 },
    { name: 'identical-10', universe: U_SMALL, client: [[0, 10]], server: [[0, 10]], frameSizeLimit: 0 },
    { name: 'identical-1000', universe: U_SMALL, client: [[0, 1000]], server: [[0, 1000]], frameSizeLimit: 0 },
    { name: 'client-extra', universe: U_SMALL, client: [[0, 100]], server: [[0, 90]], frameSizeLimit: 0 },
    { name: 'server-extra', universe: U_SMALL, client: [[0, 90]], server: [[0, 100]], frameSizeLimit: 0 },
    { name: 'disjoint-halves', universe: U_SMALL, client: [[0, 50]], server: [[50, 100]], frameSizeLimit: 0 },
    { name: 'shared-timestamps', universe: U_SHARED, client: [[0, 150], [160, 300]], server: [[0, 200], [210, 300]], frameSizeLimit: 0 },
    { name: 'ten-thousand-differ-100', universe: U_TEN_K, client: [[0, 9950]], server: [[50, 10000]], frameSizeLimit: 0 },
    { name: 'ten-thousand-differ-100-frame-4096', universe: U_TEN_K, client: [[0, 9950]], server: [[50, 10000]], frameSizeLimit: 4096 },
];

/** Cases recorded as measurements only (rounds and bytes), since their messages run to hundreds of KiB. */
const MEASURED = [
    { name: 'hundred-thousand-differ-1000', universe: U_HUNDRED_K, client: [[0, 99500]], server: [[500, 100000]], frameSizeLimit: 0 },
    { name: 'hundred-thousand-differ-1000-frame-4096', universe: U_HUNDRED_K, client: [[0, 99500]], server: [[500, 100000]], frameSizeLimit: 4096 },
];

async function upgrade(Negentropy, NegentropyStorageVector) {
    // test/protoversion.pl of the reference: one item, a full-range empty IdList, and a message of version 2
    const storage = new NegentropyStorageVector();
    storage.insert(12345, 'ee'.repeat(32));
    storage.seal();
    const server = new Negentropy(storage, 0);
    const [v1] = await server.reconcile('6100000200');
    const [v2] = await server.reconcile('62aabbccddeeff');
    return [
        { name: 'full-range-empty-idlist', server: [[12345, 'ee'.repeat(32)]], message: '6100000200', reply: v1 },
        { name: 'version-2-falls-back', server: [[12345, 'ee'.repeat(32)]], message: '62aabbccddeeff', reply: v2 },
    ];
}

async function main() {
    const opts = args();
    const source = fs.readFileSync(opts.reference);
    const digest = crypto.createHash('sha256').update(source).digest('hex');
    if (digest !== REFERENCE.sha256) throw Error(`the reference at ${opts.reference} has sha-256 ${digest}, not the pinned ${REFERENCE.sha256}`);
    const { Negentropy, NegentropyStorageVector } = require(path.resolve(opts.reference));

    const cases = [];
    for (const c of CASES) {
        const run = await reconcile(Negentropy, NegentropyStorageVector, c);
        cases.push({ name: c.name, universe: c.universe.name, frameSizeLimit: c.frameSizeLimit, client: c.client, server: c.server, ...run });
    }
    const measurements = [];
    for (const c of MEASURED) {
        const run = await reconcile(Negentropy, NegentropyStorageVector, c);
        measurements.push({ name: c.name, universe: c.universe.name, frameSizeLimit: c.frameSizeLimit, client: c.client,
            server: c.server, rounds: run.rounds, bytes: run.bytes, have: run.have.length, need: run.need.length,
            largestMessage: Math.max(...run.messages.map((m) => m.hex.length / 2)) });
    }
    const doc = {
        description: 'PDT 2 phase 0 (P2-K-01, srad/agentspaces-pdt2.md §3.6): negentropy v1 conformance vectors, '
            + 'every message of every round as the reference implementation writes it, for the store '
            + 'reconciliation that TREE_WANT.sketch and TREE_END.sketch carry. Generated by '
            + 'agentspaces/tools/negentropy/negentropy-vectors.cjs; do not edit by hand.',
        reference: REFERENCE,
        crossCheck: 'messages recorded from the upstream reference implementation itself; no upstream static vector file exists (its suite diffs ports live through test/harness), so phase 5 also runs each port in the upstream harness',
        protocol: {
            versionByte: '61',
            idBytes: 32,
            fingerprint: 'the first 16 bytes of sha-256 over (the sum of the ids mod 2^256, each a 32-byte little-endian integer) followed by the varint count',
            buckets: 16,
            idListBelow: 32,
            frameSizeLimit: '0 for none, else at least 4096; a message stops 200 bytes short of it',
            infinityTimestamp: 'encoded as 0; other timestamps as 1 + the offset from the previous bound in the message',
            timestampUnit: 'epoch seconds: the reference implementation codes varints with 32-bit operators and never reconciles a timestamp of 2^31 or more, so these vectors cannot use the epoch milliseconds of srad/agentspaces-pdt2.md §3.6 (an open item for phase 5)',
            maxRounds: MAX_ROUNDS,
        },
        recipe: {
            id: 'sha-256 over the UTF-8 text "aspace:negentropy:<universe>:<i>", as hex',
            timestamp: `${TIMESTAMP_BASE} + floor(i / perTimestamp)`,
            universes: [U_SMALL, U_SHARED, U_TEN_K, U_HUNDRED_K],
            sets: 'client and server name half-open ranges [from, to) of item indexes in their universe',
            roles: 'the client initiates; each server reply goes back to the client, which accumulates have (ids it holds that the server lacks) and need (ids the server holds that it lacks) until its reconcile returns null',
        },
        cases,
        measurements,
        upgrade: await upgrade(Negentropy, NegentropyStorageVector),
    };
    const text = JSON.stringify(doc, null, 1) + '\n';
    if (opts.check) {
        const held = fs.existsSync(opts.out) ? fs.readFileSync(opts.out, 'utf8') : '';
        if (held !== text) {
            console.error(`${opts.out} differs from what the reference computes`);
            process.exit(1);
        }
        console.log(`${opts.out}: every vector equal`);
        return;
    }
    fs.writeFileSync(opts.out, text);
    console.log(`${opts.out}: ${cases.length} cases, ${measurements.length} measurements, 2 upgrade vectors`);
    for (const m of measurements) console.log(`  ${m.name}: ${m.rounds} rounds, ${m.bytes} bytes, largest ${m.largestMessage}`);
    for (const c of cases) console.log(`  ${c.name}: ${c.rounds} rounds, ${c.bytes} bytes, have ${c.have.length}, need ${c.need.length}`);
}

main().catch((e) => {
    console.error(e.stack || String(e));
    process.exit(1);
});
