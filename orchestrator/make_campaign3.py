"""Phase 3 campaign: the FedCommRL selector (per-client, per-direction tabular Q-learning) and BOLR on dynamic traces.

  python3 orchestrator/make_campaign3.py -> campaign/phase3_dynamic.json (p3d_)

Same link states and CUBIC everywhere as Phase 2; 15-minute horizon so that epsilon (0.95^t) decays.
Traces: flip and mobility as in Phase 2 (all clients share the trace; repetition r uses Phase 2's trace of rep r),
and heterogeneous versions in which every client follows its own trace (independent vehicles): mobility_hetero
(own Markov chain + handover at each change) and intermittent_hetero (own outages).
Arms: fixed HTTP/3 (best fixed protocol of Phase 2), fixed gRPC (best TCP protocol overall), BOLR, FedCommRL.
"""
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_campaign2 import STATES, APP_PROTOS, SIZE, trace

ROOT = Path(__file__).resolve().parents[1]
HORIZON = 900
REPS = 3
ARMS = ["fixed:http3", "fixed:grpc", "bolr", "rl"]


def traces_for(kind, rep, n=5):
    base = ["flip", "bursty", "intermittent", "mobility"]
    if kind in base:
        return {"trace": trace(kind, seed=1000 * rep + base.index(kind), horizon=HORIZON + 600)}
    k = kind.replace("_hetero", "")
    return {"traces": {str(i): trace(k, seed=50_000 + 1000 * rep + 10 * base.index(k) + i, horizon=HORIZON + 600) for i in range(1, n + 1)},
            "trace": trace(k, seed=50_000 + 1000 * rep + base.index(k), horizon=HORIZON + 600)}


def runs():
    rng = random.Random(3)
    out = []
    for rep in range(REPS):
        for kind in ["mobility_hetero", "intermittent_hetero", "mobility", "flip"]:
            tr = traces_for(kind, rep)
            arms = ARMS[:]; rng.shuffle(arms)
            for a in arms:
                out.append(dict(id=f"p3d_{kind}_{a.replace('fixed:', '')}_r{rep}", cell=kind, arm=a.replace("fixed:", ""), rep=rep, phase=3,
                                pc=True, n=5, size=SIZE, rounds=0, horizon_s=HORIZON, max_rounds=5000, protos=APP_PROTOS, policy=a,
                                rl={"alpha": 0.1, "gamma": 0.9, "eps0": 1.0, "decay": 0.95, "eps_min": 0.01},
                                cc="cubic", states=STATES, links=[STATES["B"]], timeout=240, attempt=30, gap_s=0.2,
                                est_s=HORIZON + 300, **tr))
    return out


if __name__ == "__main__":
    r = runs()
    (ROOT / "campaign" / "phase3_dynamic.json").write_text(json.dumps({"runs": r}, indent=0))
    print(len(r), "runs")
