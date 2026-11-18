"""Phase 1 campaign (preliminary, host default congestion control): one-factor sweeps around a baseline.

  python3 orchestrator/make_campaign.py   ->  campaign/phase1.json

Runs are ordered repetition-major (all cells of repetition 0 first), with the protocol order shuffled per
cell and repetition, so a partially finished campaign already covers every cell and time drift does not
line up with protocol. The 'gate' cells (most likely to show protocol differences) come first.
Changing this file after runs exist only adds or removes runs; finished run files are never touched.
"""
import json, random
from pathlib import Path

PROTOS = ["mqtt", "amqp", "grpc", "http3"]
REPS = 5
MB = 1_000_000


def L(bw=20, delay=30, loss=0.5, jitter=0, burst=1, qms=100):
    return {"bw": bw, "delay": delay, "jitter": jitter, "loss": loss, "burst": burst, "qms": qms}


def sym(**kw):
    return {"up": L(**kw), "down": L(**kw)}


BASE = dict(n=5, size=12_500_000, rounds=6, links=[sym()])
POOR = sym(bw=5, delay=80, loss=2.0)

# name: (question tag, overrides); H1 protocol differences on a clean link, H2 loss and delay, H4 RTT, H5 handover,
# H6 outage, H7 heterogeneous clients, H8 model size, H9 CPU limit, H10 number of clients
CELLS = {
    # ---- gate cells
    "base":        ("H1", {}),
    "loss1":       ("H2", {"links": [sym(loss=1)]}),
    "loss2":       ("H2", {"links": [sym(loss=2)], "rounds": 4}),
    "loss5":       ("H2", {"links": [sym(loss=5)], "rounds": 3}),
    "burst2_b5":   ("H2", {"links": [sym(loss=2, burst=5)], "rounds": 4}),
    "burst2_b20":  ("H2", {"links": [sym(loss=2, burst=20)], "rounds": 4}),
    "outage5s":    ("H6", {"events": {"type": "outage", "at_s": 1.0, "dur_s": 5.0, "clients": [1]}}),
    "handover20s": ("H5", {"events": {"type": "handover", "every_s": 20.0, "gap_s": 0.2, "clients": [1]}}),
    "cpu50":       ("H9", {"cpu": 0.5}),
    "cpu25":       ("H9", {"cpu": 0.25}),
    "hetero_poor": ("H7", {"links": [POOR, sym()], "rounds": 4}),
    # ---- landscape cells
    "loss0":       ("H1", {"links": [sym(loss=0)]}),
    "rtt10":       ("H2,H4", {"links": [sym(delay=5)]}),
    "rtt160":      ("H2,H4", {"links": [sym(delay=80)]}),
    "rtt300":      ("H2,H4", {"links": [sym(delay=150)], "rounds": 4}),
    "bw5":         ("H1", {"links": [sym(bw=5)], "rounds": 4}),
    "bw100":       ("H1", {"links": [sym(bw=100)]}),
    "asym100_20":  ("H1", {"links": [{"up": L(bw=20), "down": L(bw=100)}]}),
    "size1.4MB":   ("H1,H8", {"size": 1_400_000}),
    "size50MB":    ("H8", {"size": 50 * MB, "rounds": 4}),
    "size100MB":   ("H8", {"size": 100 * MB, "rounds": 3}),
    "loss10":      ("H2", {"links": [sym(loss=10)], "rounds": 3}),
    "n10":         ("H10", {"n": 10, "rounds": 4}),
    "buf20ms":     ("H2", {"links": [sym(loss=1, qms=20)]}),
}


def main():
    rng = random.Random(2026)
    runs = []
    for rep in range(REPS):
        for name, (hyp, ov) in CELLS.items():
            protos = PROTOS[:]; rng.shuffle(protos)
            for p in protos:
                c = dict(BASE); c.update(ov)
                c.update(id=f"p1_{name}_{p}_r{rep}", cell=name, hyp=hyp, proto=p, rep=rep, phase=1)
                runs.append(c)
    out = Path(__file__).resolve().parents[1] / "campaign" / "phase1.json"
    out.write_text(json.dumps({"runs": runs}, indent=0))
    print(len(runs), "runs ->", out)


if __name__ == "__main__":
    main()
