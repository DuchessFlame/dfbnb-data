"""exe_settings.py - read game settings (GMST) and their formulas out of Fallout76.exe.

WHY (Kat 27 Oct 2026): the ESM only holds setting VALUES. Formulas live in the game's
code. Some settings only exist in the exe (e.g. fSneakAgilitySkillMin = 0), and the exe
default is replaced by the ESM value when the ESM has one (fXPModMult: exe 0.1, ESM 0.03).
Fallout 76 works a lot of maths out on Bethesda's SERVERS (carry weight, AP, sneak /
detection), so a setting with no client code readers = the formula is server-side and
can't be read from any file on the PC.

USAGE
  python src/exe_settings.py                      -> dist/exe_settings.json + .tsv (every setting)
  python src/exe_settings.py fSneakAgilitySkillMax  -> also prints the client code that uses it
  --exe "D:/steamlibrary/steamapps/common/Fallout76/Fallout76.exe" (default)

COLUMNS  name | exe_default | esm_value | in_esm | client_readers
  client_readers = code places that read the setting, other than the setting's own
  set-up / clean-up code. 0 = not used by the PC game (server-side or unused).
Needs numpy; capstone (pip install capstone) only for printing code.
"""
import sys, os, re, struct, json, csv, argparse
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
ap = argparse.ArgumentParser(); ap.add_argument('names', nargs='*')
ap.add_argument('--exe', default='D:/steamlibrary/steamapps/common/Fallout76/Fallout76.exe')
a = ap.parse_args()
exe = a.exe
if not os.path.exists(exe):  # Claude's Linux shell mounts the folder under ~/mnt
    alt = os.path.expanduser('~/mnt/Fallout76/Fallout76.exe')
    if os.path.exists(alt): exe = alt
b = open(exe, 'rb').read()

pe = struct.unpack_from('<I', b, 0x3c)[0]; nsec = struct.unpack_from('<H', b, pe + 6)[0]
osz = struct.unpack_from('<H', b, pe + 20)[0]; opt = pe + 24
BASE = struct.unpack_from('<Q', b, opt + 24)[0]; S = {}
for i in range(nsec):
    o = opt + osz + 40 * i; nm = b[o:o + 8].rstrip(b'\0').decode(errors='replace')
    vs, va, rs, ro = struct.unpack_from('<IIII', b, o + 8); S[nm] = (va, vs, ro, rs)
def off2va(off):
    for va, vs, ro, rs in S.values():
        if ro <= off < ro + rs: return BASE + va + off - ro
def va2off(v):
    r = v - BASE
    for va, vs, ro, rs in S.values():
        if va <= r < va + rs: return ro + r - va

# 1) setting objects: {?, value(+8), current(+0x10), name*(+0x18)} in .data, name string in .rdata
dva, dvs, dro, drs = S['.data']; rva_, rvs, rro, rrs = S['.rdata']
data = np.frombuffer(b[dro:dro + (drs // 8) * 8], dtype='<u8')
lo, hi = BASE + rva_, BASE + rva_ + rrs
cand = np.nonzero((data >= lo) & (data < hi))[0]
settings = {}
for i in cand:
    p = int(data[i]); so = va2off(p)
    m = re.match(rb'([fiubsr][A-Z][A-Za-z0-9_]{2,80})\0', b[so:so + 90])
    if not m: continue
    name = m.group(1).decode(); objoff = dro + int(i) * 8 - 0x18
    if objoff < dro: continue
    t = name[0]; raw = b[objoff + 8: objoff + 16]
    val = (round(struct.unpack('<f', raw[:4])[0], 6) if t == 'f' else struct.unpack('<i', raw[:4])[0] if t in 'iu'
           else bool(raw[0]) if t == 'b' else None)
    settings.setdefault(name, {'name': name, 'exe_default': val, 'obj': off2va(objoff)})

# 2) every RIP-relative target in .text, counted
tva, tvs, tro, trs = S['.text']; counts = {}
want = np.array(sorted({s['obj'] + k for s in settings.values() for k in (0, 8, 0x10)}), dtype=np.int64)
where = {}
for sh in range(4):
    arr = np.frombuffer(b[tro + sh: tro + sh + ((trs - sh - 4) // 4) * 4], dtype='<i4').astype(np.int64)
    ip = BASE + tva + np.arange(len(arr), dtype=np.int64) * 4 + sh + 4
    for tail in (0, 1, 4):
        tg = ip + tail + arr
        hit = np.nonzero(np.isin(tg, want))[0]
        for h in hit: where.setdefault(int(tg[h]), set()).add(int(ip[h] - 4))
for s in settings.values():
    refs = set()
    for k in (0, 8, 0x10): refs |= where.get(s['obj'] + k, set())
    refs = sorted(refs)
    def setup(r):   # the setting's own set-up / clean-up code, not a reader
        near = [x for x in refs if x != r and abs(x - r) < 0x100]
        if len(near) >= 2: return True
        o = va2off(r)                                   # thunk: lea rcx,[rip+x] ; jmp
        return b[o - 3:o - 1] == b'\x48\x8d' and b[o + 4] == 0xe9
    s['refs'] = [r for r in refs if not setup(r)]; s['client_readers'] = len(s['refs'])

# 3) ESM values (latest GMST export)
tsvdir = os.path.join(ROOT, 'tsv'); esm = {}
# newest by the date in the file name (tsv_source): in CI every file shares the checkout time
import tsv_source
g = tsv_source.newest('GMST_Export_*.tsv', required=False)
if g:
    for row in csv.DictReader(open(g, encoding='utf-8', errors='replace'), delimiter='\t'):
        esm[row['EDID']] = row['Value']
out = []
for s in sorted(settings.values(), key=lambda s: s['name'].lower()):
    out.append({'name': s['name'], 'exe_default': s['exe_default'], 'esm_value': esm.get(s['name']),
                'in_esm': s['name'] in esm, 'client_readers': s['client_readers'],
                'code': [hex(r) for r in s['refs']]})
os.makedirs(os.path.join(ROOT, 'dist'), exist_ok=True)
json.dump({'exe': exe, 'size': len(b), 'settings': out}, open(os.path.join(ROOT, 'dist', 'exe_settings.json'), 'w'), indent=1)
with open(os.path.join(ROOT, 'dist', 'exe_settings.tsv'), 'w', encoding='utf-8') as f:
    f.write('name\texe_default\tesm_value\tin_esm\tclient_readers\n')
    for s in out: f.write(f"{s['name']}\t{s['exe_default']}\t{s['esm_value'] or ''}\t{s['in_esm']}\t{s['client_readers']}\n")
print(f"[exe_settings] {len(out)} settings, {sum(1 for s in out if not s['in_esm'])} exe-only, "
      f"{sum(1 for s in out if s['client_readers'])} read by PC code -> dist/exe_settings.json/.tsv")

# 4) show code for named settings
if a.names:
    try: import capstone; md = capstone.Cs(capstone.CS_ARCH_X86, capstone.CS_MODE_64)
    except ImportError: md = None; print('pip install capstone to print code')
    for n in a.names:
        s = next((x for x in out if x['name'].lower() == n.lower()), None)
        if not s: print(n, 'not in exe'); continue
        print(f"\n== {s['name']}  exe {s['exe_default']}  esm {s['esm_value']}  PC readers {s['client_readers']}")
        for r in s['code']:
            r = int(r, 16); o = va2off(r); st = o
            while st > o - 0x600 and b[st - 1] != 0xCC: st -= 1      # back to the start of the function
            if md: print('\n'.join(f'  {i.address:x} {i.mnemonic} {i.op_str}' for i in md.disasm(b[st:o + 0x40], r - (o - st))) + '\n  --')
