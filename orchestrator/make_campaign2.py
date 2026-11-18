"""Phase 2 campaigns (matched congestion control; dynamic conditions with a per-round router).

  python3 orchestrator/make_campaign2.py static    -> campaign/phase2_static.json   (p2a_, p2e_, p2q_, p2b_)
  python3 orchestrator/make_campaign2.py dynamic   -> campaign/phase2_dynamic.json  (p2d_; needs p2a_ results)

All Phase 2 runs set the TCP congestion control explicitly ("cc"); primary = CUBIC for every protocol
(quic-go is CUBIC; quinn is given -cc). Model size 2.5 MB so that dynamic runs fit 20 rounds.
Link states are shared by the static anchors and the dynamic traces, so the oracle router's table and the
anchor results describe the same conditions.
Ordering: repetition-major, protocol order shuffled per cell and repetition (as Phase 1).
"""
import json, random, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_PROTOS = ["mqtt", "amqp", "grpc", "http3"]
SIZE = 2_500_000
HORIZON = 360       # s of trace per dynamic run (all arms see the identical stretch)
REPS_DYN = 5


def L(bw=20, delay=30, loss=0.5, jitter=0, burst=1, qms=100):
    return {"bw": bw, "delay": delay, "jitter": jitter, "loss": loss, "burst": burst, "qms": qms}


def sym(**kw):
    return {"up": L(**kw), "down": L(**kw)}


# link states (one-way delay in ms; RTT = 2 x delay)
STATES = {
    "G": sym(loss=0),                      # good: 20 Mbit/s, 60 ms RTT, no loss
    "B": sym(),                            # baseline: 0.5 % random loss
    "L": sym(loss=2),                      # lossy: 2 % random loss
    "F": sym(loss=2, burst=20),            # fading: 2 % loss in bursts of mean 20 packets (Gilbert-Elliott)
    "R": sym(delay=100, loss=0.2),         # long path: 200 ms RTT, 0.2 % loss
    "W": sym(bw=5, delay=60, loss=0.5),    # weak / cell edge: 5 Mbit/s, 120 ms RTT
    "OUT": sym(loss=100),                  # outage (trace only)
}
EVENTS = {
    "outage5s": {"type": "outage", "at_s": 1.0, "dur_s": 5.0, "clients": [1]},
    "handover20s": {"type": "handover", "every_s": 20.0, "gap_s": 0.2, "clients": [1]},
}


def static_run(cell, proto, rep, prefix, cc="cubic", links=None, rounds=4, **kw):
    r = dict(n=5, size=SIZE, rounds=rounds, links=[links or STATES[cell]], proto=proto, cc=cc, rep=rep, phase=2)
    arm = proto if proto != "quinn" or cc == "cubic" else f"quinn-{cc}"
    r.update(id=f"{prefix}_{cell}_{arm}{'' if proto == 'quinn' or cc == 'cubic' else '-' + cc}_r{rep}", cell=cell, arm=arm, **kw)
    return r


def static():
    rng = random.Random(20261106)
    runs = []
    for rep in range(10):
        # p2a: anchors for every trace state, the four application protocols, 3 reps
        if rep < 3:
            for cell in ["G", "B", "L", "F", "R", "W"]:
                ps = APP_PROTOS[:]; rng.shuffle(ps)
                runs += [static_run(cell, p, rep, "p2a") for p in ps]
        # p2e: more repetitions where Phase 1 varied most: bursts, outage, handover -> 10 reps
        for cell in ["F", "outage5s", "handover20s"]:
            if cell == "F" and rep < 3:
                continue
            ps = APP_PROTOS[:]; rng.shuffle(ps)
            ev = EVENTS.get(cell)
            runs += [static_run(cell, p, rep, "p2e", links=STATES["B"] if ev else None, events=ev, rounds=6 if ev else 4) for p in ps]
        # p2q: second QUIC stack. raw quic-go vs quinn (both CUBIC, identical stream framing) next to HTTP/3 (quic-go)
        if rep < 3:
            for cell in ["G", "B", "L", "F", "R", "outage5s", "handover20s"]:
                ev = EVENTS.get(cell)
                ps = ["quic", "quinn"]; rng.shuffle(ps)
                runs += [static_run(cell, p, rep, "p2q", links=STATES["B"] if ev else None, events=ev, rounds=6 if ev else 4) for p in ps]
        # p2b: BBR everywhere (TCP-BBR vs quinn-BBR; quic-go has no BBR)
        if rep < 3:
            for cell in ["B", "L", "R"]:
                ps = ["mqtt", "amqp", "grpc", "quinn"]; rng.shuffle(ps)
                runs += [static_run(cell, p, rep, "p2b", cc="bbr") for p in ps]
    # priority order: anchors (needed by the dynamic campaign), then events, QUIC stacks, BBR
    order = {"p2a": 0, "p2e": 1, "p2q": 2, "p2b": 3}
    runs.sort(key=lambda r: order[r["id"][:3]])
    return runs


# ------------------------------------------------------------------ dynamic
def trace(kind, seed, horizon=2400):
    rng = random.Random(seed)
    tr, t = [], 0
    if kind == "flip":          # loss level flips: good <-> lossy every 45 s
        while t < horizon:
            tr += [[45, "G", 0], [45, "L", 0]]; t += 90
    elif kind == "bursty":      # baseline with recurring fading episodes
        while t < horizon:
            a, b = rng.uniform(40, 80), rng.uniform(20, 40)
            tr += [[a, "B", 0], [b, "F", 0]]; t += a + b
    elif kind == "intermittent":  # baseline with short outages (tunnels, shadowing)
        while t < horizon:
            a, b = rng.uniform(20, 45), rng.uniform(3, 8)
            tr += [[a, "B", 0], [b, "OUT", 0]]; t += a + b
    elif kind == "mobility":    # vehicular: Markov chain over link states, a handover at every change of state
        cur = "G"
        P = {"G": ["B", "R", "W"], "B": ["G", "L", "W", "R"], "L": ["B", "W"], "R": ["G", "B"], "W": ["B", "L", "G"]}
        while t < horizon:
            d = rng.uniform(20, 60)
            tr.append([d, cur, 1 if tr else 0]); t += d
            cur = rng.choice(P[cur])
    return [[round(d, 1), s, h] for d, s, h in tr]


def dynamic():
    sys.path.insert(0, str(ROOT / "analysis"))
    import pandas as pd
    from analyze_phase2 import load_static
    df = load_static(prefix="p2a")
    # cost = median over runs of the effective time per successful round (failed rounds count with their time)
    med = df.groupby(["cell", "arm"])["eff_s"].median().unstack()
    table = {s: med.loc[s].idxmin() for s in med.index}
    table["OUT"] = table["B"]
    # static ranking for the reactive and bandit routers: geometric mean of the anchors' medians
    import numpy as np
    rank = list(np.exp(np.log(med[APP_PROTOS]).mean()).sort_values().index)
    rng = random.Random(7)
    runs = []
    for rep in range(REPS_DYN):
        for kind in ["flip", "bursty", "intermittent", "mobility"]:
            tr = trace(kind, seed=1000 * rep + ["flip", "bursty", "intermittent", "mobility"].index(kind))
            arms = [f"fixed:{p}" for p in APP_PROTOS] + ["oracle", "reactive", "ducb"]
            rng.shuffle(arms)
            for a in arms:
                runs.append(dict(id=f"p2d_{kind}_{a.replace('fixed:', '')}_r{rep}", cell=kind, arm=a.replace("fixed:", ""), rep=rep,
                                 phase=2, n=5, size=SIZE, rounds=0, horizon_s=HORIZON, max_rounds=1000, protos=APP_PROTOS, policy=a, rank=rank,
                                 oracle_table=table, cc="cubic", states=STATES, trace=tr, links=[STATES["B"]],
                                 timeout=240, attempt=30, gap_s=0.2, est_s=HORIZON + 300))
    runs.sort(key=lambda r: r["rep"])
    return runs


if __name__ == "__main__":
    what = sys.argv[1]
    runs = static() if what == "static" else dynamic()
    out = ROOT / "campaign" / f"phase2_{what}.json"
    out.write_text(json.dumps({"runs": runs}, indent=0))
    from collections import Counter
    print(len(runs), "runs ->", out, dict(Counter(r["id"][:3] for r in runs)))
