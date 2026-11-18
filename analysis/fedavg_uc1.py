"""FedAvg on the UC1 in-vehicle thermal-comfort data: IID vs non-IID partitions, and the
effect of a client whose update is lost in a fraction f of rounds (the failure shares
measured for each protocol in the burst-loss cell).

Task: regress the driver's PMV vote (Zone1) from cabin/ambient conditions and the driver
zone's skin temperature and humidity.  Model: MLP 6-32-1 (tanh), numpy, FedAvg with
E local epochs of mini-batch SGD with momentum.  Five clients:
  IID      random equal shards of the training data;
  non-IID  one client per driver session: driver 111's drive split into three contiguous
           time blocks, drivers 222 and 555 one client each (label and covariate shift).
Test set: random 20 % of every driver's samples (pooled), the same for both partitions.
Data: data/uc1/uc1_thermal_comfort.csv (rows in recording order; see data/uc1/README.md).
  python3 analysis/fedavg_uc1.py [path/to/uc1_thermal_comfort.csv]
  python3 analysis/fedavg_uc1.py --summary        (re-summarise an existing results/analysis/fedavg_uc1.csv)
Writes results/analysis/fedavg_uc1.csv, fedavg_uc1_summary.md, figures/fig_fedavg_uc1.*
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "results" / "analysis"; FIG = ROOT / "results" / "figures"
FEATS = ["Ambient Temperature", "Cabin Temperature", "Relative Humidity", "Solar Load",
         "Zone1 skin Temperature", "Zone1 RH"]
TARGET = "Zone1 PMV (driver)"
# share of rounds in which the update of the client on the bad link is lost
# (burst-loss cell F, CUBIC; results/analysis/phase2_summary.md)
FAIL = {"HTTP/3": 0.0, "gRPC": 0.10, "MQTT": 0.13, "AMQP": 0.80}
T_ROUND = {"HTTP/3": 39.1, "gRPC": 77.0, "MQTT": 110.1, "AMQP": float("inf")}
ROUNDS, E, B, LR, MOM, H = 200, 5, 32, 0.01, 0.9, 32
SEEDS = range(10)


def load(path):
    # rows are stored in recording order; round_trip keeps the floats bit-identical to the source
    return pd.read_csv(path, float_precision="round_trip")


def split(d, seed):
    rng = np.random.default_rng(1000 + seed)
    test = np.zeros(len(d), bool)
    for u, g in d.groupby("driver_id"):
        idx = g.index.to_numpy()
        test[rng.choice(idx, int(round(0.2 * len(idx))), replace=False)] = True
    return ~test, test


def partitions(d, train, seed):
    tr = np.flatnonzero(train)
    rng = np.random.default_rng(2000 + seed)
    iid = np.array_split(rng.permutation(tr), 5)
    non = []
    u111 = tr[d.driver_id.to_numpy()[tr] == 111]          # already time-sorted
    non += np.array_split(u111, 3)
    non += [tr[d.driver_id.to_numpy()[tr] == 222], tr[d.driver_id.to_numpy()[tr] == 555]]
    return {"IID": iid, "non-IID": non}


def init(rng, k):
    return [rng.normal(0, 1 / np.sqrt(k), (k, H)), np.zeros(H), rng.normal(0, 1 / np.sqrt(H), (H, 1)), np.zeros(1)]


def fwd(w, X):
    h = np.tanh(X @ w[0] + w[1]); return h, (h @ w[2] + w[3]).ravel()


def local(w, X, y, rng):
    w = [p.copy() for p in w]; v = [np.zeros_like(p) for p in w]
    for _ in range(E):
        for b in np.array_split(rng.permutation(len(y)), max(1, len(y) // B)):
            h, yp = fwd(w, X[b]); g = 2 * (yp - y[b])[:, None] / len(b)
            gh = (g @ w[2].T) * (1 - h ** 2)
            grads = [X[b].T @ gh, gh.sum(0), h.T @ g, g.sum(0)]
            for i in range(4):
                v[i] = MOM * v[i] - LR * grads[i]; w[i] += v[i]
    return w


def r2(y, yp):
    return 1 - ((y - yp) ** 2).sum() / ((y - y.mean()) ** 2).sum()


def run(X, y, parts, test, bad, f, seed):
    rng = np.random.default_rng(seed)
    w = init(np.random.default_rng(seed), X.shape[1])
    n = np.array([len(p) for p in parts], float)
    curve = []
    for t in range(ROUNDS):
        ups, ws = [], []
        for c, p in enumerate(parts):
            if c == bad and rng.random() < f:
                continue                                  # update lost (deadline missed)
            ups.append(local(w, X[p], y[p], rng)); ws.append(n[c])
        ws = np.array(ws) / sum(ws)
        w = [sum(wi * u[i] for wi, u in zip(ws, ups)) for i in range(4)]
        curve.append(r2(y[test], fwd(w, X[test])[1]))
    return np.array(curve), w


def main(path):
    d = load(path)
    X0 = d[FEATS].to_numpy(float); y = d[TARGET].to_numpy(float)
    rows = []
    for seed in SEEDS:
        train, test = split(d, seed)
        mu, sd = X0[train].mean(0), X0[train].std(0) + 1e-9
        X = (X0 - mu) / sd
        test_idx = np.flatnonzero(test)
        u = d.driver_id.to_numpy()
        for pname, parts in partitions(d, train, seed).items():
            for proto, f in FAIL.items():
                for bad in range(5):                       # every client in turn on the bad link
                    if f == 0 and bad > 0:
                        continue
                    curve, w = run(X, y, parts, test_idx, bad, f, seed * 10 + bad)
                    yp = fwd(w, X[test_idx])[1]
                    # R2 on the test samples of the bad client's driver
                    bad_user = u[parts[bad][0]]
                    m = u[test_idx] == bad_user
                    rows.append(dict(seed=seed, partition=pname, protocol=proto, f=f, bad=bad,
                                     bad_user=int(bad_user), r2_final=curve[-1],
                                     r2_bad_driver=r2(y[test_idx][m], yp[m]),
                                     curve=json.dumps(np.round(curve, 4).tolist())))
        print("seed", seed, "done", flush=True)
    df = pd.DataFrame(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / "fedavg_uc1.csv", index=False)
    summarise(df)


def rounds_to(curve, target):
    c = np.asarray(curve); k = np.flatnonzero(c >= target)
    return int(k[0]) + 1 if len(k) else np.nan


def summarise(df):
    df = df.copy(); df["curve"] = df.curve.apply(json.loads)
    ref = df[(df.f == 0)]
    # target: 95 % of the median final R2 of IID FedAvg without losses
    tgt = 0.95 * ref[ref.partition == "IID"].r2_final.median()
    df["rounds_to_target"] = df.curve.apply(lambda c: rounds_to(c, tgt))
    lines = [f"# FedAvg on UC1: IID vs non-IID, lost updates\n", f"Target R2 = {tgt:.3f} (95 % of IID median final R2)\n"]
    g = df.groupby(["partition", "protocol"]).agg(
        r2_final=("r2_final", "median"), r2_bad_driver=("r2_bad_driver", "median"),
        rounds_to_target=("rounds_to_target", "median"),
        reached=("rounds_to_target", lambda s: f"{s.notna().mean():.0%}"), n=("seed", "size"))
    g["round_time_s"] = [{"HTTP/3": 39.1, "gRPC": 77.0, "MQTT": 110.1, "AMQP": float("inf")}[p] for _, p in g.index]
    g["time_to_target_min"] = g.rounds_to_target * g.round_time_s / 60
    lines.append(g.round(3).to_markdown())
    # worst case: the distinct driver 555 on the bad link
    w = df[(df.partition == "non-IID") & (df.bad_user == 555)].groupby("protocol")[["r2_final", "r2_bad_driver"]].median()
    lines.append("\n\nnon-IID, driver 555 on the bad link (median over seeds):\n")
    lines.append(w.round(3).to_markdown())
    w2 = df[(df.partition == "IID")].groupby("protocol")[["r2_final", "r2_bad_driver"]].median()
    lines.append("\n\nIID (median over seeds and bad clients):\n")
    lines.append(w2.round(3).to_markdown())
    (OUT / "fedavg_uc1_summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    figure(df, tgt)


def figure(df, tgt):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from style import NAME_COLOR as col, setup
    setup(plt, size=9)
    styles = {"HTTP/3": "-", "gRPC": "--", "MQTT": "-.", "AMQP": ":"}
    fig, ax = plt.subplots(1, 2, figsize=(7.16, 3.1), sharey=True)       # printed at full page width
    for a, pname in zip(ax, ["IID", "non-IID"]):
        for proto in ["HTTP/3", "gRPC", "MQTT", "AMQP"]:
            s = df[(df.partition == pname) & (df.protocol == proto)]
            if pname == "non-IID" and proto != "HTTP/3":
                s = s[s.bad_user == 555]
            c = np.median(np.stack(s.curve.to_numpy()), axis=0)
            lab = f"{proto} (f={int(100*s.f.iloc[0])} %)"
            a.plot(np.arange(1, len(c) + 1), c, color=col[proto], lw=2.2 if proto == "AMQP" else 1.6, ls=styles[proto], label=lab)
        a.axhline(tgt, color="#555555", lw=0.8, ls=(0, (1, 2)))
        a.set_title(pname if pname == "IID" else "non-IID (driver 555 on the bad link)")
        a.set_xlabel("FL round")
        a.spines[["top", "right"]].set_visible(False); a.grid(axis="y", lw=0.3, alpha=0.5)
    ax[0].set_ylabel("test $R^2$ (median of 10 seeds)")
    ax[0].set_ylim(bottom=max(-0.2, ax[0].get_ylim()[0]))
    ax[1].legend(frameon=False, loc="lower right")
    fig.tight_layout()
    FIG.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(FIG / f"fig_fedavg_uc1.{ext}", dpi=200)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--summary":
        summarise(pd.read_csv(OUT / "fedavg_uc1.csv"))
    else:
        main(sys.argv[1] if len(sys.argv) > 1 else str(ROOT / "data" / "uc1" / "uc1_thermal_comfort.csv"))
