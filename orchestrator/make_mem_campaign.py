"""Memory measurement campaign: the static cells G (loss-free) and L (lossy) for every protocol
implementation, 8 rounds, 1 repetition, run with analysis/mem_sampler.py sampling the resident memory of every
process.  python3 orchestrator/make_mem_campaign.py -> campaign/mem.json"""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
c = json.loads((ROOT / "campaign" / "phase2_static.json").read_text())["runs"]
want = [f"p2a_{s}_{p}_r0" for s in ("G", "L") for p in ("mqtt", "amqp", "grpc", "http3")] + \
       [f"p2q_{s}_{p}_r0" for s in ("G", "L") for p in ("quic", "quinn")]
runs = []
for r in c:
    if r["id"] in want:
        r = dict(r); r["rounds"] = 8; r["id"] = "mem_" + r["id"][4:-3]; runs.append(r)
(ROOT / "campaign" / "mem.json").write_text(json.dumps({"runs": runs}, indent=0))
print(len(runs), [r["id"] for r in runs])
