"""Phase 3 analysis: live per-client / per-direction selection on dynamic traces (p3d_ runs) and figures.

  python3 analysis/analyze_phase3.py  -> results/analysis/phase3_summary.md, phase3_live.csv, phase3_live_rounds.csv,
                                         results/figures/fig_p3_*.pdf|png

Metric (as Phase 2): seconds per successful round over the fixed 900 s horizon (rounds still running at the horizon
are not counted; a run with no successful round counts as infinitely slow). Arms are paired by repetition (identical
traces); the pooled test pairs every (trace, repetition) with fixed HTTP/3.
"""
import json, math
from collections import Counter
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import wilcoxon

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / "results" / "runs"
OUT = ROOT / "results" / "analysis"
FIG = ROOT / "results" / "figures"
ARMS = ["http3", "grpc", "bolr", "rl"]
DISPLAY = {"bolr": "BOLR", "rl equal w": "FedCommRL (equal weights)",
           "rl shared table": "FedCommRL, shared table", "rl gamma=0 (one-step)": "FedCommRL, $\\gamma=0$",
           "rl w=time only": "FedCommRL, time only", "rl w=energy 0.6": "FedCommRL, energy 0.6",
           "rl w=cpu 0.6": "FedCommRL, CPU 0.6", "rl slow decay 0.99": "FedCommRL, decay 0.99",
           "oracle (state-wise, per client/direction)": "oracle (state-wise)"}
LABEL = {"http3": "fixed HTTP/3", "grpc": "fixed gRPC", "bolr": "BOLR", "rl": "FedCommRL"}
from style import ARM_COLOR as COLOR, PROTO_COLOR, PROTO_NAME, setup as style_setup  # noqa: E402
KINDS = ["flip", "mobility", "mobility_hetero", "intermittent_hetero"]


def load():
    runs, rounds = [], []
    for f in sorted(RUNS.glob("p3d_*.json")):
        d = json.loads(f.read_text())
        if d.get("status") != "ok":
            continue
        rows = [r for r in d["rows"] if not r.get("summary")]
        s = next(r for r in d["rows"] if r.get("summary"))
        share = {"dl": Counter(), "ul": Counter()}
        sw, cpu, last = 0, [], {}
        for r in rows:
            for cid, c in r["clients"].items():
                share["dl"][c["d"]] += 1; share["ul"][c["u"]] += 1
                if cid in last and last[cid] != (c["d"], c["u"]):
                    sw += 1
                last[cid] = (c["d"], c["u"])
            cpu.append(sum((c.get("cpu_dl") or 0) + (c.get("cpu_ul") or 0) for c in r["clients"].values()))
            st = r["state"]
            rounds.append(dict(id=d["id"], kind=d["cell"], arm=d["arm"], rep=d["rep"], k=r["r"], t_end=r["t_end"],
                               in_h=r["in_horizon"], ok=r["ok"], elapsed=r["elapsed"],
                               eps=(r.get("policy") or {}).get("eps"), dq=(r.get("policy") or {}).get("dq"),
                               state=json.dumps(st), cpu_s=cpu[-1],
                               dl=" ".join(c["d"] for c in r["clients"].values()),
                               ul=" ".join(c["u"] for c in r["clients"].values()),
                               n_http3=sum((c["d"] == "http3") + (c["u"] == "http3") for c in r["clients"].values()),
                               changes=len(r.get("changes") or [])))
        n_dec = max(sum(share["dl"].values()), 1)
        ok_h = s["ok_rounds"]
        runs.append(dict(id=d["id"], kind=d["cell"], arm=d["arm"], rep=d["rep"], ok_rounds=ok_h, attempted=s["attempted"],
                         failed=sum(1 for r in rows if not r["ok"]),
                         s_per_round=(s["horizon_s"] / ok_h) if ok_h else math.inf,
                         cpu_s_per_round=float(np.mean(cpu)) if cpu else np.nan,
                         eps_end=(rows[-1].get("policy") or {}).get("eps") if rows else None,
                         switches_per_client_round=sw / max(len(rows) * 5, 1),
                         **{f"dl_{p}": share["dl"][p] / n_dec for p in ["mqtt", "amqp", "grpc", "http3"]},
                         **{f"ul_{p}": share["ul"][p] / n_dec for p in ["mqtt", "amqp", "grpc", "http3"]}))
    return pd.DataFrame(runs), pd.DataFrame(rounds)


def summary(df):
    md = ["# Phase 3 live results: per-client, per-direction selection on dynamic traces", "",
          "5 clients, 2.5 MB model, CUBIC everywhere, 900 s identical trace per arm and repetition; metric = seconds per "
          "successful round over the horizon (lower is better). RL = the FedCommRL agent (dual Q-tables, "
          "alpha 0.1, gamma 0.9, epsilon 0.95^t, equal reward weights). `_hetero` = every client has its own trace.", ""]
    done = df.groupby(["kind", "arm"]).size().unstack(fill_value=0)
    md += ["Runs completed per cell:", "", done.to_markdown(), ""]
    med = df.groupby(["kind", "arm"]).s_per_round.median().unstack()
    med = med[[a for a in ARMS if a in med]]
    md += ["## Seconds per successful round (median over repetitions)", "", med.round(1).to_markdown(), ""]
    # paired ratios vs fixed HTTP/3
    piv = df.pivot_table(index=["kind", "rep"], columns="arm", values="s_per_round")
    lines = []
    for a in [x for x in ARMS if x != "http3" and x in piv]:
        p = piv[["http3", a]].dropna()
        p = p[np.isfinite(p).all(axis=1)]
        if len(p) == 0:
            continue
        r = p[a] / p["http3"]
        try:
            pv = wilcoxon(np.log(p[a] / p["http3"])).pvalue  # signed-rank on log ratios (pooled traces differ in scale) if len(p) >= 5 else float("nan")
        except ValueError:
            pv = float("nan")
        lines.append(dict(arm=LABEL[a], pairs=len(p), geo_mean_ratio_vs_http3=float(np.exp(np.log(r).mean())),
                          min_ratio=r.min(), max_ratio=r.max(), slower_in=f"{int((r > 1).sum())}/{len(r)}", wilcoxon_p=pv))
    if lines:
        md += ["## Paired with fixed HTTP/3 (same trace and repetition), pooled over traces", "",
               pd.DataFrame(lines).round(4).to_markdown(index=False), ""]
    cols = ["kind", "arm", "rep", "ok_rounds", "failed", "s_per_round", "cpu_s_per_round", "eps_end", "switches_per_client_round",
            "dl_http3", "ul_http3", "dl_grpc", "ul_grpc", "dl_mqtt", "ul_mqtt", "dl_amqp", "ul_amqp"]
    md += ["## Per run", "", df.sort_values(["kind", "arm", "rep"])[cols].round(3).to_markdown(index=False), ""]
    return md


def figures(df, rd):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    style_setup(plt)
    FIG.mkdir(parents=True, exist_ok=True)

    # (a) live: s per successful round per trace and arm (dots = repetitions, bar = median)
    kinds = [k for k in KINDS if k in set(df.kind)]
    if kinds:
        fig, ax = plt.subplots(figsize=(7.0, 2.6))
        w = 0.2
        for j, a in enumerate(ARMS):
            for i, k in enumerate(kinds):
                v = df[(df.kind == k) & (df.arm == a)].s_per_round.replace(np.inf, np.nan).dropna()
                x = i + (j - 1.5) * w
                if len(v):
                    ax.bar(x, v.median(), w * 0.9, color=COLOR[a], alpha=0.35, label=LABEL[a] if i == 0 else None, zorder=2)
                    ax.scatter([x] * len(v), v, s=10, color=COLOR[a], zorder=3, edgecolor="white", linewidth=0.5)
        ax.set_xticks(range(len(kinds))); ax.set_xticklabels([k.replace("_hetero", "\n(per-client traces)") for k in kinds])
        ax.set_ylabel("s per successful round"); ax.legend(ncol=4, loc="upper left", fontsize=7)
        fig.tight_layout(); [fig.savefig(FIG / f"fig_p3_live.{e}", dpi=200) for e in ("pdf", "png")]; plt.close(fig)

    # (b) live FedCommRL run: round time of the agent and two fixed references on the identical trace, the
    # downlink and uplink protocol of every client in every round, and epsilon. Printed at full page width.
    rl = rd[(rd.arm == "rl")]
    if len(rl):
        rid = rl.groupby("id").k.max().idxmax()      # the agent's run with most rounds
        r = rl[rl.id == rid].sort_values("k")
        style_setup(plt, size=9)
        fig, axs = plt.subplots(3, 1, figsize=(7.16, 5.4), sharex=True, gridspec_kw={"height_ratios": [1.3, 2.0, 0.7]})
        x = (r.t_end - r.elapsed).to_numpy()          # round start, seconds since trace start
        axs[0].step(x, r.elapsed, where="post", color=COLOR["rl"], lw=1.6, label="FedCommRL")
        for a, ls in [("http3", "-"), ("bolr", "--")]:
            o = rd[(rd.kind == r.kind.iloc[0]) & (rd.rep == r.rep.iloc[0]) & (rd.arm == a)].sort_values("k")
            if len(o):
                axs[0].step(o.t_end - o.elapsed, o.elapsed, where="post", color=COLOR[a], lw=1.4, ls=ls, label=LABEL[a])
        axs[0].set_ylabel("round time (s)")
        axs[0].legend(loc="upper left", ncol=3, frameon=False, bbox_to_anchor=(0, 1.18))
        protos = ["mqtt", "amqp", "grpc", "http3"]
        width = np.append(np.diff(x), r.elapsed.iloc[-1])
        rows = []
        for c in range(5):
            rows += [(c, 0, f"client {c + 1} DL"), (c, 1, f"client {c + 1} UL")]
        for yi, (c, d, lab) in enumerate(rows):
            for p in protos:
                sel = [(xx, ww) for xx, ww, (_, row) in zip(x, width, r.iterrows())
                       if (row.dl if d == 0 else row.ul).split()[c] == p]
                if sel:
                    axs[1].broken_barh(sel, (yi - 0.42, 0.84), facecolors=PROTO_COLOR[p], edgecolor="white", linewidth=0.3)
        axs[1].set_yticks(range(len(rows))); axs[1].set_yticklabels([lab for *_, lab in rows])
        axs[1].set_ylim(len(rows) - 0.5, -0.5); axs[1].grid(False)
        for c in range(1, 5):
            axs[1].axhline(2 * c - 0.5, color="#ffffff", lw=2.0)
        handles = [plt.Rectangle((0, 0), 1, 1, color=PROTO_COLOR[p]) for p in protos]
        axs[1].legend(handles, [PROTO_NAME[p] for p in protos], ncol=4, frameon=False, loc="lower center",
                      bbox_to_anchor=(0.5, 1.0))
        axs[2].step(x, r.eps, where="post", color="#000000", lw=1.4); axs[2].set_ylabel(r"$\varepsilon$")
        axs[2].set_xlabel("time since trace start (s)"); axs[2].set_ylim(0, 1)
        fig.tight_layout(); fig.subplots_adjust(hspace=0.28)
        [fig.savefig(FIG / f"fig_p3_rl_live.{e}", dpi=300) for e in ("pdf", "png")]; plt.close(fig)
        style_setup(plt)

    # (c) replay learning curves (300 rounds, 5 seeds, markov_hetero)
    cvf = OUT / "phase3_replay_curves.csv"
    if cvf.exists():
        cv = pd.read_csv(cvf)
        sel = cv[(cv.kind == "markov_hetero")]
        if len(sel):
            pols = [p for p in ["rl equal w", "rl w=time only", "rl gamma=0 (one-step)"] if p in set(sel.policy)]
            cols = ["#000000", PROTO_COLOR["http3"], PROTO_COLOR["mqtt"]]
            fig, axs = plt.subplots(1, 3, figsize=(7.16, 2.4))
            for p, c in zip(pols, cols):
                g = sel[sel.policy == p].groupby("k")
                rt = g.round_s.mean().rolling(10, min_periods=1).mean()
                axs[0].plot(rt.index, rt, color=c, lw=1.5, label=DISPLAY.get(p, p))
                axs[1].plot(g.dq.median().index, g.dq.median().rolling(10, min_periods=1).mean(), color=c, lw=1.5)
                axs[2].plot(g.greedy_is_best.mean().index, g.greedy_is_best.mean().rolling(10, min_periods=1).mean(), color=c, lw=1.5)
            e = sel[sel.policy == pols[0]].groupby("k").eps.mean()
            axs[1].plot(e.index, e, color="#555555", lw=1.1, ls="--", label=r"$\varepsilon$")
            axs[0].set_ylabel("round time (s, 10-round mean)"); axs[1].set_ylabel("|dQ| (median) / epsilon")
            axs[1].set_yscale("log"); axs[2].set_ylabel("greedy = fastest in state"); axs[2].set_ylim(0, 1)
            for a in axs: a.set_xlabel("FL round")
            axs[0].legend(frameon=False); axs[1].legend(frameon=False)
            fig.tight_layout(); [fig.savefig(FIG / f"fig_p3_replay_curves.{e}", dpi=200) for e in ("pdf", "png")]; plt.close(fig)

    # (d) replay: excess over best fixed per schedule (time) and the equal-weight objective
    rpf = OUT / "phase3_replay.csv"
    if rpf.exists():
        import sys
        sys.path.insert(0, str(ROOT / "analysis"))
        from replay_pc import summarise
        sm = summarise(pd.read_csv(rpf))
        show = ["bolr", "rl equal w", "rl shared table", "rl gamma=0 (one-step)", "rl w=time only",
                "rl w=energy 0.6", "rl w=cpu 0.6", "rl slow decay 0.99", "oracle (state-wise, per client/direction)"]
        sched = ["stat_B", "stat_L", "stat_R", "stat_W", "block", "markov", "markov_hetero"]
        M = sm.pivot_table(index="policy", columns="schedule", values="excess_vs_best_fixed_pct").reindex(index=show, columns=sched)
        fig, ax = plt.subplots(figsize=(7.0, 2.8))
        im = ax.imshow(M.values, cmap="RdBu_r", vmin=-60, vmax=60, aspect="auto")
        for i in range(M.shape[0]):
            for j in range(M.shape[1]):
                v = M.values[i, j]
                if np.isfinite(v):
                    ax.text(j, i, f"{v:+.0f}", ha="center", va="center", fontsize=7, color="#0b0b0b" if abs(v) < 40 else "white")
        ax.set_xticks(range(len(sched))); ax.set_xticklabels([{"block": "block", "markov": "Markov", "markov_hetero": "Markov\n(per-client)"}.get(s, s.replace("stat_", "static ")) for s in sched])
        ax.set_yticks(range(len(show))); ax.set_yticklabels([DISPLAY.get(s, s) for s in show])
        ax.grid(False); fig.colorbar(im, ax=ax, label="% vs best fixed protocol (time)")
        fig.tight_layout(); [fig.savefig(FIG / f"fig_p3_replay_excess.{e}", dpi=200) for e in ("pdf", "png")]; plt.close(fig)


def switching_cost():
    """Warm vs cold sessions in the live selector runs: a transfer is 'cold' if the client has not used that protocol
    (in either direction) since a handover of its link, i.e. the session may be broken while idle. Reports mean transfer
    time and the share of stalls (> 25 s, close to the 30 s attempt timeout) per class."""
    out = []
    for f in sorted(RUNS.glob("p3d_*.json")):
        d = json.loads(f.read_text())
        if d.get("status") != "ok" or d["arm"] not in ("rl", "bolr"):
            continue
        s = next(r for r in d["rows"] if r.get("summary"))
        ho = {}                                     # client -> list of handover times (s since run start)
        for t, name, h in s["timeline"]:
            if h:
                if ":" in str(name):
                    ho.setdefault(name.split(":")[0], []).append(t)
                else:
                    for i in map(str, range(1, d["n"] + 1)):
                        ho.setdefault(i, []).append(t)
        last_use = {}                               # (client, proto) -> time of last use
        for r in [x for x in d["rows"] if not x.get("summary")]:
            t0 = r["t_since_start"]
            for cid, c in r["clients"].items():
                for dirn, p, dur in (("dl", c["d"], c["dl_s"]), ("ul", c["u"], c["ul_s"])):
                    if dur is None:
                        continue
                    lu = last_use.get((cid, p))
                    cold = lu is None or any(lu < h <= t0 for h in ho.get(cid, []))
                    out.append(dict(arm=d["arm"], kind=d["cell"], proto=p, dirn=dirn, cold=cold, first=lu is None, t=dur, stall=dur > 25))
                last_use[(cid, c["d"])] = last_use[(cid, c["u"])] = r["t_end"]
    df = pd.DataFrame(out)
    if not len(df):
        return []
    df["transport"] = np.where(df.proto == "http3", "QUIC", "TCP")
    g = df[~df["first"]].groupby(["transport", "cold"]).agg(n=("t", "size"), mean_s=("t", "mean"), median_s=("t", "median"), stall_share=("stall", "mean"))
    return ["## Switching cost: idle sessions across handovers (live selector runs, first use of each protocol excluded)", "",
            "cold = the client has had a handover since it last used this protocol; stall = transfer > 25 s.", "",
            g.round(3).to_markdown(), ""]


if __name__ == "__main__":
    df, rd = load()
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / "phase3_live.csv", index=False); rd.to_csv(OUT / "phase3_live_rounds.csv", index=False)
    md = (summary(df) if len(df) else ["no runs"]) + switching_cost()
    (OUT / "phase3_summary.md").write_text("\n".join(md) + "\n")
    print("\n".join(md[:40]))
    figures(df, rd)

