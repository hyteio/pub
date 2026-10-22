"""Extract OpenWire v12 loose-encoding field layouts from ActiveMQ's generated marshallers."""
import re, sys, os, json

SRC = sys.argv[1]  # .../org/apache/activemq
V12 = os.path.join(SRC, "openwire/v12")

types = {}
for line in open(os.path.join(SRC, "command/CommandTypes.java")):
    m = re.match(r"\s*byte\s+(\w+)\s*=\s*(-?\d+);", line)
    if m:
        types[m.group(1)] = int(m.group(2))

def parse(name):
    path = os.path.join(V12, name + "Marshaller.java")
    text = open(path).read()
    parent = re.search(r"class \w+Marshaller extends (\w+)Marshaller", text)
    parent = parent.group(1) if parent else None
    body = re.search(r"public void looseMarshal\(.*?\{(.*?)\n    \}", text, re.S)
    fields = []
    if body:
        for stmt in body.group(1).split(";"):
            s = " ".join(stmt.split())
            if not s or "beforeMarshall" in s or s.startswith(name[0].lower()) and "=" in s and "(" not in s.split("=")[0]:
                pass
            if "super.looseMarshal" in s:
                fields.append(("<super>", parent))
                continue
            m = re.search(r"(looseMarshal\w+|dataOut\.write\w+)\((.*)\)$", s)
            if not m:
                continue
            kind, args = m.group(1), m.group(2)
            getter = re.search(r"info\.(?:get|is)(\w+)\(\)", args)
            g = getter.group(1) if getter else args
            if kind == "dataOut.writeBoolean" and not getter:
                continue
            fields.append((kind.replace("dataOut.", "").replace("looseMarshal", "loose:"), g))
    abstract = "abstract class" in text
    dtype = re.search(r"return (\w+)\.DATA_STRUCTURE_TYPE", text)
    return {"parent": parent, "fields": fields, "abstract": abstract, "type_const": dtype.group(1) if dtype else None}

def flatten(name, seen=None):
    info = parse(name)
    out = []
    for kind, g in info["fields"]:
        if kind == "<super>":
            if g and g not in ("BaseDataStream", "BaseDataStructure") and os.path.exists(os.path.join(V12, g + "Marshaller.java")):
                out.extend(flatten(g))
        else:
            out.append((kind, g))
    return out

wanted = sys.argv[2].split(",")
result = {}
for w in wanted:
    info = parse(w)
    result[w] = {"chain": [], "fields": flatten(w)}
    p = w
    while p:
        result[w]["chain"].append(p)
        nxt = parse(p)["parent"] if os.path.exists(os.path.join(V12, p + "Marshaller.java")) else None
        if not nxt or not os.path.exists(os.path.join(V12, nxt + "Marshaller.java")):
            break
        p = nxt

for w, r in result.items():
    print(f"## {w}  ({' <- '.join(r['chain'])})")
    for i, (k, g) in enumerate(r["fields"], 1):
        print(f"  {i:2}. {k:28} {g}")
    print()
print("## CommandTypes (subset)")
for k in sorted(types, key=lambda k: types[k]):
    print(f"  {types[k]:4}  {k}")
