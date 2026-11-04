#!/usr/bin/env python3
"""Extract protobuf-es v2 compact type tables + Re() enums from agent-harness.cjs."""
import re, sys

SRC = "/Users/didi/Library/Application Support/Cursor/agent-harness/versions/b951911e447652508ba64b4b209ceb5762710e1423deddf7082a6be93bca7cbc/agent-harness.cjs"
src = open(SRC, encoding="utf8", errors="replace").read()

wk_types = {}
SCALARS = {1:"double",2:"float",3:"int64",4:"uint64",5:"int32",6:"fixed64",7:"fixed32",
           8:"bool",9:"string",12:"bytes",13:"uint32",15:"sfixed32",16:"sfixed64",
           17:"sint32",18:"sint64"}
WELLKNOWN = {"Struct":"google.protobuf.Struct","Value":"google.protobuf.Value",
             "ListValue":"google.protobuf.ListValue","Timestamp":"google.protobuf.Timestamp",
             "Duration":"google.protobuf.Duration","Any":"google.protobuf.Any",
             "Empty":"google.protobuf.Empty","FieldMask":"google.protobuf.FieldMask"}

def split_elems(arr):
    elems, cur, inq = [], "", False
    for c in arr:
        if c == '"': inq = not inq; cur += c
        elif c == ',' and not inq: elems.append(cur.strip()); cur = ""
        else: cur += c
    if cur.strip(): elems.append(cur.strip())
    return elems

def bracket(start):
    depth, k = 0, start
    while k < len(src):
        if src[k] == '[': depth += 1
        elif src[k] == ']':
            depth -= 1
            if depth == 0: return src[start+1:k]
        k += 1
    return None

# --- enums: VAR=Re(l,nd,"Name",[[no,"VAL"],...],maybeOpen)
def bracket_at(start):
    depth, k = 0, start
    while k < len(src):
        if src[k] == '[': depth += 1
        elif src[k] == ']':
            depth -= 1
            if depth == 0: return src[start+1:k]
        k += 1
    return None
enums = {}   # var -> (name, [(no, VAL)])
for m in re.finditer(r'([A-Za-z_$][\w$]*)=Re\([\w$]+,[\w$]+,"([A-Za-z0-9_.]+)",', src):
    var, name = m.groups()
    epos = m.start()
    b = m.end()
    if b < len(src) and src[b] == '[':
        body = bracket_at(b)
        vals = [(int(a), c) for a, c in re.findall(r'\[(\d+),"([^"]*)"\]', body)]
        if vals: enums[var] = (name.split('.')[-1], vals, epos)

# --- classes with $() tables
classes = {}  # var -> dict(name, fields, pos)
class_matches = list(re.finditer(r'([A-Za-z_$][\w$]*)=class [\w$]+ extends ([A-Za-z_$][\w$]*)\{', src))
for ci, m in enumerate(class_matches):
    var, base = m.groups()
    pos = m.start()
    span_end = class_matches[ci+1].start() if ci+1 < len(class_matches) else len(src)
    dollar = src.find('static $(){return', m.end(), span_end)
    if dollar == -1:
        # classic-style declaration: use typeName if present in this span
        tn = re.search(r'\.typeName="([^"]+)"', src[m.end():span_end][:6000])
        if tn and tn.group(1).startswith("google.protobuf."):
            wk_types[var] = (tn.group(1), pos)
        continue
    lb = src.index('[', dollar)
    raw = bracket(lb)
    if raw is None: continue
    elems = split_elems(raw)
    if not elems or not elems[0].startswith('"'): continue
    self_str = elems[0][1:-1]
    if '|' not in self_str: continue
    refs = [e for e in elems[1:] if re.fullmatch(r'[A-Za-z_$][\w$]*', e)]
    parts = self_str.split('|')
    fields = []
    ok = True
    for f in parts[1:]:
        toks = f.split(' ')
        if toks[-1].startswith('='): toks = toks[:-1]
        if len(toks) < 3: ok = False; break
        no, name, typ = int(toks[0]), toks[1], toks[2]
        card = toks[3] if len(toks) > 3 else None
        # protobuf-es v2 compact tables put the cardinality marker on the TYPE
        # token (e.g. `1 messages #0*`, `2 name 9?`): `*` means repeated,
        # `?` means optional. The previous parser treated every suffix as
        # optional, silently collapsing repeated fields to a single value.
        if typ.endswith('*'):
            typ = typ[:-1]
            if card is None or card == '?': card = '*'
        elif typ.endswith('?'):
            typ = typ[:-1]
            if card is None: card = '?'
        fields.append((no, name, typ, card, refs))
    if ok: classes[var] = {"name": parts[0], "fields": fields, "pos": pos}

sys.stderr.write(f"enums: {len(enums)}, classes: {len(classes)}\n")

defs = []  # (pos, var, kind)
for var, info in classes.items(): defs.append((info["pos"], var, "msg"))
for var, (fullname, p) in wk_types.items(): defs.append((p, var, "wk"))
for var, (nm, vals, p) in enums.items(): defs.append((p, var, "enum"))
defs.sort()

def nearest_def(var, before):
    best = None
    for p, v, k in defs:
        if v == var and p < before:
            best = (v, k)
    # if no def before, allow any (fallback)
    if best is None:
        for p, v, k in defs:
            if v == var: best = (v, k)
    return best

def type_of(var, at=None):
    d = nearest_def(var, at) if at is not None else (var, "msg" if var in classes else ("enum" if var in enums else None))
    if d is None: return (None, None)
    v, k = d
    if k is None: return (None, None)
    if v in wk_types: return ("wk", wk_types[v][0])
    if k == "wk":
        return ("wk", wk_types[v][0])
    if k == "msg":
        n = classes[v]["name"]
        if n in WELLKNOWN: return ("wk", WELLKNOWN[n])
        return ("msg", n)
    return ("enum", enums[v][0])

def resolve(var, seen, order, emitted_names, rename):
    """walk deps, assign unique emitted names"""
    if var in seen: return
    seen.add(var)
    kind, name = type_of(var)
    if kind is None: return
    order.append(var)
    if kind == "enum": return
    for (no, fname, typ, card, refs) in classes[var]["fields"]:
        deps = []
        if typ.startswith('#'):
            i = int(typ[1:]); deps = [refs[i]] if i < len(refs) else []
        elif ',' in typ:
            v = typ.split(',')[1]
            if v.startswith('#'):
                i = int(v[1:]); deps = [refs[i]] if i < len(refs) else []
        for d in deps: resolve(d, seen, order, emitted_names, rename)

def emit(var, out, emitted, used_names):
    if var in emitted: return
    kind, name = type_of(var)
    if kind is None: return
    if kind == "enum":
        ename = name
        if ename in used_names and used_names[ename] != ("enum", var): ename = ename + "_2"
        used_names.setdefault(ename, ("enum", var))
        out.append(f"enum {ename} {{")
        pre = re.sub(r'(?<!^)(?=[A-Z])', '_', ename).upper()
        for no, v in sorted(enums[var][1]):
            out.append(f"  {pre}_{v} = {no};")
        out.append("}")
        emitted.add(var); emitted_ename[var] = ename
        return
    # deps first
    for (no, fname, typ, card, refs) in classes[var]["fields"]:
        deps = []
        if typ.startswith('#'):
            i = int(typ[1:]); deps = [refs[i]] if i < len(refs) else []
        elif ',' in typ:
            v = typ.split(',')[1]
            if v.startswith('#'):
                i = int(v[1:]); deps = [refs[i]] if i < len(refs) else []
        for d in deps: emit(d, out, emitted, used_names)
    if var in emitted: return
    if kind == "enum":
        ename = name
        if ename in used_names and used_names[ename] != ("enum", var): ename = ename + "_2"
        used_names.setdefault(ename, ("enum", var))
        out.append(f"enum {ename} {{")
        pre = re.sub(r'(?<!^)(?=[A-Z])', '_', ename).upper()
        for no, v in sorted(enums[var][1]):
            out.append(f"  {pre}_{v} = {no};")
        out.append("}")
        emitted.add(var); emitted_ename[var] = ename
        return
    # message
    mname = name
    if mname in used_names and used_names[mname] != ("msg", var): mname = mname + "_2"
    used_names.setdefault(mname, ("msg", var))
    emitted_ename[var] = mname
    lines, oneofs = [], {}
    for (no, fname, typ, card, refs) in classes[var]["fields"]:
        if typ.startswith('#'):
            i = int(typ[1:])
            rvar = refs[i] if i < len(refs) else None
            rk, rn = type_of(rvar, at=classes[var]["pos"]) if rvar else (None, None)
            rt = rn or "bytes"
        elif ',' in typ:
            k, v = typ.split(',')
            kt = SCALARS.get(int(k), "string")
            if v.startswith('#'):
                i = int(v[1:]); rvar = refs[i] if i < len(refs) else None
                rk, rn = type_of(rvar, at=classes[var]["pos"]) if rvar else (None, None)
                vt = rn or "bytes"
            else: vt = SCALARS.get(int(v), "bytes")
            lines.append(f"  map<{kt}, {vt}> {fname} = {no};")
            continue
        else:
            rt = SCALARS.get(int(typ), "string")
        oneof = card if (card not in (None, '?', '*')) else None
        pre = "optional " if card == '?' else ("repeated " if card == '*' else "")
        if oneof:
            oneofs.setdefault(oneof, []).append(f"    {rt} {fname} = {no};")
        else:
            lines.append(f"  {pre}{rt} {fname} = {no};")
    for oname, fl in oneofs.items():
        lines.append(f"  oneof {oname} {{")
        lines.extend(fl)
        lines.append("  }")
    out.extend([f"message {mname} {{"] + lines + ["}"])
    emitted.add(var)

emitted_ename = {}
ROOTS = ["RunInferenceClientMessage", "RunInferenceServerMessage", "BidiAppendRequest", "BidiRequestId"]
out = ['syntax = "proto3";', "", 'package agent.v1;', "", 'import "google/protobuf/struct.proto";', ""]
emitted, used_names = set(), {}
for r in ROOTS:
    for var, info in classes.items():
        if info["name"] == r:
            emit(var, out, emitted, used_names)
            break
body = "\n".join(out)
# shorten fully-qualified google types (map value uses full path already ok)
open("/tmp/rif/runinference_v1.proto", "w").write(body + "\n")
sys.stderr.write(f"lines: {len(out)}\n")
bad = [l for l in out if "bytes" in l and re.search(r'(args|parameters|role|value) =', l)]
sys.stderr.write("suspicious bytes fields: " + str(len(bad)) + "\n")
