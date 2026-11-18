"""Trace-driven replay of per-client / per-direction protocol selection on measured samples (Phase 3).

  python3 analysis/replay_pc.py [--rounds 300 --seeds 20]  -> results/analysis/phase3_replay.{md,csv}, phase3_replay_curves.csv

Samples: every measured round (rounds >= 2) of the Phase 2 static cells (CUBIC, 5 clients, 2.5 MB) gives, per client,
the downlink time, the uplink time (confirmed receipt - downlink end) and CPU time, for one protocol in one state.
States: G, B, L, F, R, W (p2a/p2e anchors), OUT (5 s outage on client 1, p2e outage5s), HO (handover every 20 s
on client 1, p2e handover20s). A failed client-round keeps the time the round actually took.
Replay round: the state of the round comes from a round-indexed schedule; for every protocol one measured round
is drawn (common random numbers: the same draws for all policies); client i downloading with protocol d and
uploading with u gets client i's downlink time from the draw of d and its uplink time from the draw of u; the
round time is max_i(dl_i + ul_i). Not modelled: reconnection cost of idle sessions broken by outages/handovers,
and correlation between a client's downlink and uplink when they use different protocols (see the live runs).
The policies are the same code as in the live runs (orchestrator/policies_pc.py).
"""
import argparse, json, math, random, sys
from collections import defaultdict
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "orchestrator"))
from policies_pc import PROTOS, QLearn, BOLR, Fixed, K_TX, P_CPU  # noqa: E402

SIZE = 2_500_000

STATE_CELLS = {"G": "G", "B": "B", "L": "L", "F": "F", "R": "R", "W": "W", "OUT": "outage5s", "HO": "handover20s"}


def load_pools():
    pools = defaultdict(list)    # (state, proto) -> list of rounds; round = {client: (dl, ul, cpu_dl, cpu_ul, ok)}
    for f in sorted((ROOT / "results" / "runs").glob("p2[ae]_*.json")):
        d = json.loads(f.read_text())
        st = next((s for s, c in STATE_CELLS.items() if c == d["cell"]), None)
        if st is None or d["proto"] not in PROTOS:
            continue
        R = d["rows"]
        for j, r in enumerate(R):
            if r["r"] < 2:
                continue
            dur = (R[j + 1]["t0"] - r["t0"]) if j + 1 < len(R) else (r["T_round"] or 120.0)
            rnd = {}
            for cid, c in r["clients"].items():
                ok = bool(c.get("ok")) and c.get("recv") is not None
                dl = c.get("dl") if c.get("dl") is not None else dur
                ul = (c["recv"] - dl) if ok else max(dur - dl, 0.0)
                cpu = c.get("cpu_s") or 0.0
                tot = dl + ul
                rnd[int(cid)] = (dl, ul, cpu * dl / tot if tot else cpu, cpu * ul / tot if tot else 0.0, ok, c.get("dl") is not None)
            pools[(st, d["proto"])].append(rnd)
    return pools


def schedule(kind, rounds, rng):
    if kind.startswith("stat_"):
        return [kind[5:]] * rounds
    if kind == "block":                    # baseline -> lossy -> clean -> weak, equal blocks
        seq = ["B", "L", "G", "W"]
        return [seq[min(t * len(seq) // rounds, len(seq) - 1)] for t in range(rounds)]
    if kind == "markov":                   # vehicular-like chain over link states, mean dwell 10 rounds, rare events
        P = {"G": ["B", "R", "W"], "B": ["G", "L", "W", "R", "F"], "L": ["B", "W"], "R": ["G", "B"], "W": ["B", "L", "G"], "F": ["B"]}
        cur, out = "G", []
        while len(out) < rounds:
            d = max(1, int(rng.expovariate(1 / 10)))
            out += [cur] * d
            if rng.random() < 0.15:
                out.append(rng.choice(["OUT", "HO"]))
            cur = rng.choice(P[cur])
        return out[:rounds]
    raise ValueError(kind)


def policies(clients, seed):
    ps = {f"fixed:{p}": Fixed(p, clients) for p in PROTOS}
    ps["bolr"] = BOLR(clients)
    ps["rl equal w"] = QLearn(clients, seed=seed)
    ps["rl shared table"] = QLearn(clients, seed=seed, shared=True)
    ps["rl gamma=0 (one-step)"] = QLearn(clients, seed=seed, gamma=0.0)
    ps["rl w=time only"] = QLearn(clients, seed=seed, weights=(1, 0, 0))
    ps["rl w=energy 0.6"] = QLearn(clients, seed=seed, weights=(0.2, 0.6, 0.2))
    ps["rl w=cpu 0.6"] = QLearn(clients, seed=seed, weights=(0.2, 0.2, 0.6))
    ps["rl slow decay 0.99"] = QLearn(clients, seed=seed, decay=0.99)
    return ps


def run(rounds, seeds, kinds, clients=(1, 2, 3, 4, 5)):
    pools = load_pools()
    have = {s for s, _ in pools}
    rows, curves = [], []
    for kind in kinds:
        for seed in range(seeds):
            rng = random.Random(1000 + seed)
            if kind.endswith("_hetero"):     # every client follows its own schedule (independent vehicles)
                per = {i: schedule(kind[:-7], rounds, random.Random(1000 * seed + i)) for i in clients}
                sch = [{i: (per[i][t] if per[i][t] in have else "B") for i in clients} for t in range(rounds)]
            else:
                one = schedule(kind, rounds, rng)
                sch = [{i: (one[t] if one[t] in have else "B") for i in clients} for t in range(rounds)]
            # one measured round per (state, protocol) and replay round, shared by all policies and by all clients in
            # that state (keeps the within-round correlation between clients)
            draws = [{(st, p): rng.choice(pools[(st, p)]) for st in sorted(set(sv.values())) for p in PROTOS} for sv in sch]
            best = {}
            for st in {x for sv in sch for x in sv.values()}:
                for i in clients:
                    for di in (0, 1):
                        best[(st, i, di)] = min(PROTOS, key=lambda p: np.mean([r[i][di] for r in pools[(st, p)] if i in r]))  # mean: failures and tails count
            ps = policies(list(clients), seed)
            for name, pol in list(ps.items()) + [("oracle (state-wise, per client/direction)", None)]:
                tot, en, cp = [], [], []
                for k, (sv, dr) in enumerate(zip(sch, draws), start=1):
                    state = sv if kind.endswith("_hetero") else sv[clients[0]]
                    ch = ({i: (best[(sv[i], i, 0)], best[(sv[i], i, 1)]) for i in clients} if pol is None else pol.choose(k, state))
                    obs, rt, e_r, c_r = {}, 0.0, 0.0, 0.0
                    for i, (d, u) in ch.items():
                        dl, _, cdl, _, _, dl_ok = dr[(sv[i], d)][i]
                        _, ul, _, cul, oku, _ = dr[(sv[i], u)][i]
                        c_r += cdl + cul
                        e_r += 2 * K_TX * 8 * SIZE + P_CPU * (cdl + cul)
                        obs[i] = {"dl_s": dl, "ul_s": ul if dl_ok else None, "ok_dl": dl_ok, "ok_ul": dl_ok and oku,
                                  "cpu_dl": cdl, "cpu_ul": cul}
                        rt = max(rt, dl + (ul if dl_ok else 0.0))
                    if pol is not None:
                        pol.update(k, state, ch, obs)
                    tot.append(rt); en.append(e_r); cp.append(c_r)
                    if isinstance(pol, QLearn) and seed < 5 and kind in ("markov", "markov_hetero", "stat_B", "stat_L"):
                        g = pol.greedy(state)
                        hit = np.mean([g[i][0] == best[(sv[i], i, 0)] for i in clients] + [g[i][1] == best[(sv[i], i, 1)] for i in clients])
                        curves.append(dict(kind=kind, seed=seed, policy=name, k=k, round_s=rt, **pol.log(), greedy_is_best=hit))
                rows.append(dict(kind=kind, seed=seed, policy=name, mean_all=np.mean(tot),
                                 mean_last100=np.mean(tot[-100:]), mean_first50=np.mean(tot[:50]),
                                 energy_last100=np.mean(en[-100:]), cpu_last100=np.mean(cp[-100:])))
    df = pd.DataFrame(rows)
    return df, pd.DataFrame(curves)


def summarise(df):
    out = []
    for kind, g in df.groupby("kind"):
        piv = g.pivot_table(index="seed", columns="policy", values="mean_last100")
        fixed = [c for c in piv if c.startswith("fixed:")]
        bf = piv[fixed].mean().idxmin()
        for pol in piv:
            ex = 100 * (piv[pol] - piv[bf]) / piv[bf]
            en = g[g.policy == pol].energy_last100.mean(); cpu = g[g.policy == pol].cpu_last100.mean()
            out.append(dict(schedule=kind, policy=pol, best_fixed=bf, s_per_round_last100=piv[pol].mean(),
                            energy_J_per_round=en, cpu_s_per_round=cpu,
                            excess_vs_best_fixed_pct=ex.mean(), excess_sd=ex.std(), beats_best_fixed=f"{int((ex < 0).sum())}/{len(ex)}"))
    sm = pd.DataFrame(out)
    # the agent's objective: equal-weight cost of time, energy and CPU utilisation, each min-max normalised over
    # the four fixed protocols of the schedule (0 = best fixed protocol on that metric, 1 = worst)
    sm["cpu_util"] = sm.cpu_s_per_round / sm.s_per_round_last100
    parts = []
    for kind, g in sm.groupby("schedule"):
        fx = g[g.policy.str.startswith("fixed:")]
        nz = lambda c: (g[c] - fx[c].min()) / (fx[c].max() - fx[c].min()) if fx[c].max() > fx[c].min() else g[c] * 0
        g = g.assign(objective_cost=(nz("s_per_round_last100") + nz("energy_J_per_round") + nz("cpu_util")) / 3)
        g["best_fixed_on_objective"] = g[g.policy.str.startswith("fixed:")].sort_values("objective_cost").policy.iloc[0]
        parts.append(g)
    return pd.concat(parts)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=300); ap.add_argument("--seeds", type=int, default=20)
    a = ap.parse_args()
    kinds = ["stat_G", "stat_B", "stat_L", "stat_F", "stat_R", "stat_W", "block", "markov", "markov_hetero"]
    df, cv = run(a.rounds, a.seeds, kinds)
    od = ROOT / "results" / "analysis"
    df.to_csv(od / "phase3_replay.csv", index=False); cv.to_csv(od / "phase3_replay_curves.csv", index=False)
    sm = summarise(df)
    md = ["# Phase 3 trace-driven replay (per client, per direction)", "",
          f"{a.rounds} rounds x {a.seeds} seeds per schedule; metric = mean round time over the last 100 rounds; "
          "excess = relative to the best fixed protocol of the same schedule and seed (common random numbers). "
          "objective_cost = the agent's equal-weight reward cost (time, energy, CPU utilisation), each term min-max "
          "normalised over the four fixed protocols of the schedule (lower is better).", ""]
    for kind, g in sm.groupby("schedule"):
        md += [f"## {kind} (best fixed on time: {g.best_fixed.iloc[0]}; on the equal-weight objective: {g.best_fixed_on_objective.iloc[0]})", "",
               g.drop(columns=["schedule", "best_fixed", "best_fixed_on_objective", "cpu_util"]).round(3).to_markdown(index=False), ""]
    (od / "phase3_replay.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
