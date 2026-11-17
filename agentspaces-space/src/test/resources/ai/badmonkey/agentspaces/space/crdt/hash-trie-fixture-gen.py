# Generates the provisional trie fixture for HashTrieFixtureTest from an independent fold of annex I.6
# (the same fold as phase 0's annex check). Deterministic: seeded. Regenerate from this directory with
#   python3 -I hash-trie-fixture-gen.py > hash-trie-fixture.txt
# Provisional until phase 4's vector generator writes audit-vectors.json; never touches golden.json.
import hashlib, random, sys
T = 32
def key(i): return hashlib.sha256(i.encode()).hexdigest()
def node(slots, depth):
    n = len(slots)
    if n <= T or depth == 64:
        lines = sorted({l.encode() for ls in slots.values() for l in ls})
        pre = b'\n'.join(lines)
    else:
        pre = b''
        for c in '0123456789abcdef':
            sub = {i: v for i, v in slots.items() if key(i)[depth] == c}
            if not sub: pre += bytes(16) + (0).to_bytes(8, 'big')
            else:
                fp, cnt, _ = node(sub, depth + 1); pre += fp + cnt.to_bytes(8, 'big')
    full = hashlib.sha256(pre).digest()
    return full[:16], n, full

def control(s): return any(ord(ch) < 0x20 or ord(ch) == 0x7f for ch in s)

def audited(spec):
    # spec: (id, state, claim, dropped, lapsed, acks)
    i, st, cl, dr, lapsed, acks = spec
    out = set()
    hide = lapsed and st is not None
    if not hide:
        h = st if st is not None else dr
        if h is not None: out.add('e:%s=%s' % (i, h))
        if cl is not None: out.add('c:%s=%s' % (i, cl))
    out |= set(acks)
    return sorted(l for l in out if not control(l))

rnd = random.Random(20261114)
ALPH = 'abcdefghijklmnopqrstuvwxyz0123456789-=:.' 
EXOTIC = ['é', '中', '', '￯', '\U0001F600', '\U00010348']
def rid():
    n = rnd.randint(1, 12)
    s = ''.join(rnd.choice(ALPH) for _ in range(n))
    if rnd.random() < 0.15: s += rnd.choice(EXOTIC)
    if rnd.random() < 0.02: s += '\x01'
    return s
def rh(): return ''.join(rnd.choice('0123456789abcdef') for _ in range(rnd.randint(1, 8)))

def rspec(i):
    st = rh() if rnd.random() < 0.8 else None
    cl = rh() if rnd.random() < 0.3 else None
    dr = rh() if rnd.random() < 0.1 else None
    lapsed = rnd.random() < 0.1
    acks = set()
    for _ in range(rnd.choice([0, 0, 0, 1, 2])):
        kind = rnd.choice('ec')
        held = st if kind == 'e' else cl
        h = held if (held is not None and rnd.random() < 0.4) else rh()
        acks.add('%s:%s=%s' % (kind, i, h))
    if st is None and cl is None and dr is None and not acks:
        st = rh()
    return (i, st, cl, dr, lapsed, sorted(acks))

def ground(prefix, n, tag):
    out, k = [], 0
    while len(out) < n:
        i = '%s-%d' % (tag, k); k += 1
        if key(i).startswith(prefix): out.append(i)
    return out

def hexs(s): return s.encode().hex() if s != '' else '.'
def opt(x): return '-' if x is None else x

def emit(name, specs, w):
    specs = {s[0]: s for s in specs}
    slots = {i: audited(s) for i, s in specs.items()}
    slots = {i: ls for i, ls in slots.items() if ls}
    w.write('set %s\n' % name)
    for i, st, cl, dr, lapsed, acks in specs.values():
        w.write('slot %s %s %s %s %d %s\n' % (hexs(i), opt(st), opt(cl), opt(dr), 1 if lapsed else 0,
                ','.join(a.encode().hex() for a in acks) if acks else '-'))
    full = node(slots, 0)[2] if slots else hashlib.sha256(b'').digest()
    w.write('stream %s\n' % full.hex())
    def walk(path):
        sub = {i: v for i, v in slots.items() if key(i).startswith(path)}
        if not sub:
            w.write('node %s 0 -\n' % (path or '.')); return
        fp, cnt, _ = node(sub, len(path))
        w.write('node %s %d %s\n' % (path or '.', cnt, fp.hex()))
        if cnt > T and len(path) < 64:
            empty_done = False
            for c in '0123456789abcdef':
                if any(key(i).startswith(path + c) for i in sub): walk(path + c)
                elif not empty_done: walk(path + c); empty_done = True
        else:
            first = sorted(key(i) for i in sub)[0]
            p2 = first[:len(path) + 1]
            sub2 = {i: v for i, v in sub.items() if key(i).startswith(p2)}
            fp2, cnt2, _ = node(sub2, len(p2))
            w.write('node %s %d %s\n' % (p2, cnt2, fp2.hex()))
    walk('')
    w.write('end\n')

w = sys.stdout
w.write('# Provisional cross-check of HashTrie against an independent Python fold of SPEC annex I.6\n')
w.write('# (generator hash-trie-fixture-gen.py beside this file, seed 20261114). Phase 4 replaces it with audit-vectors.json\n')
w.write('# written by the vector generator; this file is not a normative vector and golden.json is untouched.\n')
w.write('# slot <id utf8 hex> <stateHash|-> <claimHash|-> <droppedHash|-> <lapsed 0|1> <ack utf8 hex,...|->\n')
w.write('# node <path nibbles|.> <audited count> <fingerprint hex|- when empty>\n')
emit('empty', [], w)
emit('illustration', [('a', '1', '2', None, False, [])], w)
for n in [1, 2, 7, 31, 32, 33, 34, 50, 64, 200, 700]:
    ids = set()
    while len(ids) < n: ids.add(rid())
    emit('random-%d' % n, [rspec(i) for i in sorted(ids)], w)
deep = ground('abc', 40, 'd3') + ground('ab', 6, 'd2') + ground('a', 4, 'd1')
emit('deep-abc', [(i, rh(), None, None, False, []) for i in deep], w)
emit('boundary-5e-33', [(i, rh(), None, None, False, []) for i in ground('5e', 33, 'b')], w)
emit('boundary-5e-32', [(i, rh(), None, None, False, []) for i in ground('5e', 32, 'b')], w)
# 33 slots under 5e, one of them lapsed with nothing else audited: 32 audited, so the node at 5e is a leaf.
b = ground('5e', 33, 'b')
emit('lapsed-5e', [(i, 'h%d' % k, None, None, k == 0, []) for k, i in enumerate(b)], w)
