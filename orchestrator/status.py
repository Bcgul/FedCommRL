"""Progress of a campaign: runs done per cell and protocol, failed rounds, median round time.
  python3 orchestrator/status.py [campaign/phase1.json]   -> prints and writes results/PROGRESS.md"""
import json, statistics, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
camp = json.loads(Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "campaign/phase1.json").read_text())["runs"]
tot, done, bad, T = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(list)
cells, protos = [], ["mqtt", "amqp", "grpc", "http3"]
for c in camp:
    k = (c["cell"], c["proto"]); tot[k] += 1
    if c["cell"] not in cells:
        cells.append(c["cell"])
    f = ROOT / "results/runs" / f"{c['id']}.json"
    if f.exists():
        d = json.loads(f.read_text()); done[k] += 1
        for r in d["rows"][1:]:
            if r["ok"]:
                T[k].append(r["T_round"])
            else:
                bad[k] += 1
lines = [f"# Progress: {sum(done.values())} of {len(camp)} runs done", "",
         "Cell | " + " | ".join(protos), "--- | " + " | ".join("---" for _ in protos)]
for cell in cells:
    cs = []
    for p in protos:
        k = (cell, p)
        med = f"{statistics.median(T[k]):.1f}s" if T[k] else "-"
        cs.append(f"{done[k]}/{tot[k]} · {med}" + (f" · {bad[k]} failed" if bad[k] else ""))
    lines.append(f"{cell} | " + " | ".join(cs))
lines += ["", "Each entry: runs done / planned · median round time (rounds 2+) · failed rounds."]
txt = "\n".join(lines)
(ROOT / "results/PROGRESS.md").write_text(txt + "\n")
print(txt)
