"""Phase 1 analysis (preliminary campaign).

  python3 analysis/analyze_phase1.py   -> results/analysis/{phase1_runs.csv, phase1_cells.csv, phase1_pairs.csv, phase1_summary.md, *.png}

Unit of analysis: per run, the mean round time over successful rounds 2..R ("T"), plus the share of
rounds that failed. Per cell: Kruskal-Wallis across protocols (Benjamini-Hochberg across cells),
Holm-corrected Mann-Whitney pairs with Cliff's delta, bootstrap 95 % CI of each protocol's time ratio
to the cell's best, and TOST equivalence (+-5 %, Welch t on log times) for the TCP-based protocols.
Value of adaptation over the static cells: (best fixed protocol total - per-cell best total) / best fixed total.
"""
import json, itertools
from pathlib import Path
import numpy as np, pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "results" / "analysis"; OUT.mkdir(parents=True, exist_ok=True)
PROTOS = ["mqtt", "amqp", "grpc", "http3"]
TCP = ["mqtt", "amqp", "grpc"]
EVENT_CELLS = {"outage5s", "handover20s", "hetero_poor"}
MARGIN = np.log(1.05)


def load():
    rows = []
    for f in sorted((ROOT / "results/runs").glob("p1_*.json")):
        d = json.loads(f.read_text())
        rr = d["rows"][1:]  # round 1 excluded (warm-up)
        ok = [r["T_round"] for r in rr if r["ok"] and r["T_round"] is not None]
        rows.append(dict(id=d["id"], cell=d["cell"], hyp=d["hyp"], proto=d["proto"], rep=d["rep"],
                         T=np.mean(ok) if ok else np.nan, n_ok=len(ok), n_fail=len(rr) - len(ok),
                         fail_rate=(len(rr) - len(ok)) / len(rr) if rr else np.nan,
                         srv_cpu=np.nanmean([r["server_cpu_s"] or np.nan for r in rr]) if rr else np.nan,
                         broker_cpu=np.nanmean([r["broker_cpu_s"] for r in rr]) if rr else np.nan,
                         host_cpu=np.nanmean([r["host_cpu_pct"] for r in rr]) if rr else np.nan,
                         cli_cpu=np.nanmean([np.nanmean([c["cpu_s"] or np.nan for c in r["clients"].values()]) for r in rr]) if rr else np.nan,
                         attempts=np.nanmean([np.nanmean([c.get("attempts") or np.nan for c in r["clients"].values()]) for r in rr]) if rr else np.nan))
    return pd.DataFrame(rows)


def cliffs(a, b):
    a, b = np.asarray(a), np.asarray(b)
    return (np.sum(a[:, None] < b[None, :]) - np.sum(a[:, None] > b[None, :])) / (len(a) * len(b))


def holm(p):
    p = np.asarray(p); o = np.argsort(p); adj = np.empty_like(p); m = len(p); run = 0
    for k, i in enumerate(o):
        run = max(run, (m - k) * p[i]); adj[i] = min(1, run)
    return adj


def bh(p):
    p = np.asarray(p); m = len(p); o = np.argsort(p); adj = np.empty_like(p); prev = 1
    for k in range(m - 1, -1, -1):
        i = o[k]; prev = min(prev, p[i] * m / (k + 1)); adj[i] = prev
    return adj


def tost(a, b):
    """Two one-sided Welch t tests on log times; returns the larger p (equivalent if < 0.05)."""
    la, lb = np.log(a), np.log(b)
    d = la.mean() - lb.mean()
    va, vb = la.var(ddof=1) / len(la), lb.var(ddof=1) / len(lb)
    se = np.sqrt(va + vb)
    if se == 0:
        return 0.0 if abs(d) < MARGIN else 1.0
    df = (va + vb) ** 2 / (va ** 2 / (len(la) - 1) + vb ** 2 / (len(lb) - 1))
    p1 = 1 - stats.t.cdf((d + MARGIN) / se, df)
    p2 = stats.t.cdf((d - MARGIN) / se, df)
    return float(max(p1, p2))


def boot_ratio(a, b, rng, n=4000):
    ra = rng.choice(a, (n, len(a))).mean(1); rb = rng.choice(b, (n, len(b))).mean(1)
    r = ra / rb
    return float(np.percentile(r, 2.5)), float(np.percentile(r, 97.5))


def main():
    df = load(); df.to_csv(OUT / "phase1_runs.csv", index=False)
    rng = np.random.default_rng(0)
    cells, pairs = [], []
    for cell, g in df.groupby("cell", sort=False):
        grp = {p: g[(g.proto == p)]["T"].dropna().values for p in PROTOS}
        mean = {p: grp[p].mean() if len(grp[p]) else np.nan for p in PROTOS}
        best = min((p for p in PROTOS if len(grp[p])), key=lambda p: mean[p])
        valid = [grp[p] for p in PROTOS if len(grp[p]) >= 2]
        H, pkw = stats.kruskal(*valid) if len(valid) >= 2 else (np.nan, np.nan)
        rec = dict(cell=cell, hyp=g.hyp.iloc[0], best=best, H=H, p_kw=pkw, n=len(g))
        for p in PROTOS:
            s = g[g.proto == p]
            rec[f"T_{p}"] = mean[p]; rec[f"sd_{p}"] = grp[p].std(ddof=1) if len(grp[p]) > 1 else np.nan
            rec[f"fail_{p}"] = s.fail_rate.mean(); rec[f"n_{p}"] = len(grp[p])
            if len(grp[p]) >= 2 and p != best:
                lo, hi = boot_ratio(grp[p], grp[best], rng)
                rec[f"ratio_{p}"] = mean[p] / mean[best]; rec[f"ci_{p}"] = f"{lo:.2f}-{hi:.2f}"
            elif p == best:
                rec[f"ratio_{p}"] = 1.0
        # equivalence of the TCP-based protocols: all pairs
        tp = [tost(grp[a], grp[b]) for a, b in itertools.combinations(TCP, 2) if len(grp[a]) >= 2 and len(grp[b]) >= 2]
        rec["tost_tcp_maxp"] = max(tp) if tp else np.nan
        cells.append(rec)
        pp = []
        for a, b in itertools.combinations(PROTOS, 2):
            if len(grp[a]) >= 2 and len(grp[b]) >= 2:
                u = stats.mannwhitneyu(grp[a], grp[b], alternative="two-sided")
                pp.append(dict(cell=cell, a=a, b=b, ratio=mean[a] / mean[b], p=u.pvalue, delta=cliffs(grp[a], grp[b])))
        for q, ph in zip(pp, holm([q["p"] for q in pp]) if pp else []):
            q["p_holm"] = ph
        pairs += pp
    C = pd.DataFrame(cells); C["p_kw_bh"] = bh(C["p_kw"].fillna(1).values)
    C.to_csv(OUT / "phase1_cells.csv", index=False); P = pd.DataFrame(pairs); P.to_csv(OUT / "phase1_pairs.csv", index=False)

    # value of adaptation over static cells (no events), all protocols and TCP-only
    S = C[~C.cell.isin(EVENT_CELLS)]
    def voa(protos):
        tot = {p: S[f"T_{p}"].sum() for p in protos}
        bf = min(tot, key=tot.get)
        per = S[[f"T_{p}" for p in protos]].min(1).sum()
        return bf, 100 * (tot[bf] - per) / tot[bf], {p: round(100 * (S[f"T_{p}"] / S[[f"T_{q}" for q in protos]].min(1) - 1).max(), 1) for p in protos}
    bf_all, v_all, worst_all = voa(PROTOS); bf_tcp, v_tcp, worst_tcp = voa(TCP)

    L = ["# Phase 1 results (generated by analysis/analyze_phase1.py)", "",
         f"Runs analysed: {len(df)}; cells: {len(C)}. Round time = mean over successful rounds 2..R; seconds.", "",
         "| Cell | Hyp | MQTT | AMQP | gRPC | HTTP/3 | Best | KW p (BH) | TCP equivalent ±5 % (TOST p) |", "|---|---|---|---|---|---|---|---|---|"]
    for _, r in C.iterrows():
        cs = []
        for p in PROTOS:
            t = r[f"T_{p}"]; f = r[f"fail_{p}"]
            cs.append(("–" if np.isnan(t) else f"{t:.1f} ± {r[f'sd_{p}']:.1f}") + (f" ({100 * f:.0f} % fail)" if f and f > 0 else ""))
        eq = r["tost_tcp_maxp"]
        L.append(f"| {r.cell} | {r.hyp} | " + " | ".join(cs) + f" | {r.best} | {r.p_kw_bh:.3g} | " +
                 ("–" if np.isnan(eq) else ("yes" if eq < 0.05 else "no") + f" ({eq:.2g})") + " |")
    L += ["", "## Value of adaptation over the static cells", "",
          f"- All four protocols: best fixed = {bf_all}; per-cell best saves {v_all:.1f} % of total round time. Worst-case penalty of each fixed protocol vs the cell best (%): {worst_all}",
          f"- TCP-based only: best fixed = {bf_tcp}; per-cell best saves {v_tcp:.1f} %. Worst-case penalty (%): {worst_tcp}", "",
          "## Event cells (outage, handover, heterogeneous clients)", ""]
    for _, r in C[C.cell.isin(EVENT_CELLS)].iterrows():
        L.append(f"- {r.cell}: best {r.best}; " + ", ".join(f"{p} {r[f'T_{p}']:.1f} s" + (f" ({100 * r[f'fail_{p}']:.0f} % rounds failed)" if r[f'fail_{p}'] > 0 else "") for p in PROTOS))
    (OUT / "phase1_summary.md").write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
