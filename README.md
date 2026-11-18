# FedCommRL testbed: communication protocols for federated learning over dynamic networks

This repository contains the emulation testbed, the campaign definitions, all raw measurements, the analysis
scripts and the in-vehicle thermal-comfort data used in

> B. C. Gül, R. R. Bekal, N. Jazdi and M. Weyrich, "Communication Protocols for Federated Learning over Dynamic
> Networks," *IEEE Access*.

The testbed measures the communication part of synchronous federated learning (FL) rounds: a server distributes a
model of a given size to N clients, and every client uploads a model of the same size and waits for an
application-level acknowledgement. MQTT, AMQP, gRPC and HTTP/3 are implemented on equal terms (same chunk format,
acknowledgement and retry policy, persistent TLS 1.3 sessions), together with raw QUIC streams on two QUIC
implementations (quic-go and quinn). Each client runs in its own network namespace, and every link direction has its
own rate limiter and delay/loss emulator, which reproduces random and burst loss, outages, handovers with address
change and time-varying link-state traces. The congestion control is set explicitly in every run.

On top of the testbed, the repository contains FedCommRL, a per-client, per-direction tabular Q-learning selector
that chooses the protocol for each client and direction in every round, a reactive best-of-last-round (BOLR)
heuristic, global per-round routers (oracle, reactive, discounted UCB), and a trace-driven replay that evaluates the
selectors for 300 rounds over measured samples.

## Repository layout

| Path | Content |
|---|---|
| `bench/` | `fedbench` (Go): server and client of one FL round protocol over MQTT, AMQP, gRPC, HTTP/3 and raw QUIC (quic-go) |
| `qbench/` | `qbench` (Rust): the same round protocol over raw QUIC streams with quinn |
| `netem/` | `nfqdelay.c`, a userspace delay/loss emulator on NFQUEUE (Gilbert-Elliott burst loss, outages), and `topo.py`, which sets up the namespaces, links, rate limits and handovers |
| `config/` | broker configurations and the script that creates the test certificates |
| `orchestrator/` | campaign generators, the resumable campaign runner, the dynamic-trace harness and the selector policies (`policies_pc.py`) |
| `campaign/` | the campaign files that define every run (one JSON entry per run) |
| `results/runs/` | one JSON file per run with the configuration and the per-round, per-client measurements |
| `results/runs_superseded/` | runs that were repeated after a client fix, kept for transparency (see the README there) |
| `results/mem/` | memory measurement per process |
| `results/analysis/` | tables and summaries produced by the analysis scripts |
| `results/figures/` | figures produced by the analysis scripts |
| `analysis/` | analysis scripts, the trace-driven replay and the FedAvg experiment |
| `data/uc1/` | in-vehicle thermal-comfort data used in the FedAvg experiment (see `data/uc1/README.md`) |

## Campaigns

The campaigns are numbered in the order in which they were run. Run identifiers start with the campaign prefix.

| Prefix | Campaign file | Content | Runs |
|---|---|---|---|
| `p1` | `phase1.json` | preliminary campaign with the host's default congestion control (BBR for TCP) | 480 |
| `p2a`, `p2e`, `p2q`, `p2b` | `phase2_static.json` | static link states, outage, handover and burst-loss cells, second QUIC stack, BBR everywhere (CUBIC unless stated) | 258 |
| `p2diag` | `phase2_diag_grpc.json` | gRPC flow-control window under loss | 27 |
| `p2d` | `phase2_dynamic.json` | fixed protocols and global routers on four changing-link traces | 140 |
| `p3mech` | `phase3_mech.json` | single transfers for the QUIC vs TCP-CUBIC loss-recovery comparison | 4 |
| `p3d` | `phase3_dynamic.json` | FedCommRL, BOLR, fixed HTTP/3 and fixed gRPC with per-client, per-direction selection, 900 s per run | 48 |
| `mem_` | `mem.json` | memory per process (8 rounds per protocol, states G and L) | 12 |

The link states are defined in `orchestrator/make_campaign2.py` (G: 20 Mbit/s, 60 ms RTT, no loss; B: 0.5 % random
loss; L: 2 % random loss; F: 2 % loss in bursts of mean length 20 packets; R: 200 ms RTT, 0.2 % loss; W: 5 Mbit/s,
120 ms RTT, 0.5 % loss).

## Reproducing the analysis

The analysis needs only Python and the shipped results; it does not need root rights or the testbed.

```bash
pip install -r requirements.txt
python3 analysis/analyze_phase2.py      # static, event and router results  -> results/analysis/phase2_summary.md
python3 analysis/replay_pc.py           # 300-round replay of the selectors  -> results/analysis/phase3_replay.md
python3 analysis/analyze_phase3.py      # live selector runs and figures     -> results/analysis/phase3_summary.md
python3 analysis/fedavg_uc1.py          # FedAvg on the UC1 data             -> results/analysis/fedavg_uc1_summary.md
python3 analysis/analyze_phase1.py      # preliminary campaign               -> results/analysis/phase1_summary.md
```

The replay and the FedAvg experiment are deterministic for a given set of run files. The FedAvg experiment takes
several minutes on one CPU core.

## Running the testbed

The testbed runs on a single Linux host and needs root rights (network namespaces, `tc`, `iptables` NFQUEUE).
It was used on Ubuntu 24.04 with Go 1.24, Rust 1.8x or later, Python 3.12/3.13, Mosquitto 2.0 and RabbitMQ 3.12.

```bash
sudo bash setup.sh                       # packages, emulator, certificates, Go and Rust binaries
sudo python3 orchestrator/run_cells.py campaign/phase2_static.json --only p2a_G_
sudo bash orchestrator/run_all.sh        # all campaigns in dependency order (several days)
```

The runner is resumable: a finished run is written to `results/runs/<id>.json`, and existing run files are
skipped. To repeat a run, delete its file. `--only PREFIX` and `--only-re REGEX` restrict a campaign to a subset of
runs, and `--budget-min M` stops starting new runs after M minutes.

## Run file format

Every run file contains the configuration of the run (links, protocol or policy, congestion control, model size,
number of clients, repetition) and a list `rows` with one entry per FL round. The main fields are:

| Field | Meaning |
|---|---|
| `r` | round number (round 1 is a warm-up round and is excluded from the analysis) |
| `ok` | all clients completed the round within the round deadline |
| `T_round` / `elapsed` | round time in seconds (static runs / trace runs) |
| `clients[i].dl`, `clients[i].recv` | download time and time until the server confirmed the upload, seconds from round start (static runs) |
| `clients[i].dl_s`, `clients[i].ul_s` | download and upload time (per-client selection runs) |
| `clients[i].d`, `clients[i].u` | downlink and uplink protocol chosen for client i (per-client selection runs) |
| `clients[i].cpu_s`, `cpu_dl`, `cpu_ul` | CPU time of the client process |
| `server_cpu_s`, `broker_cpu_s` | CPU time of the server and broker processes |
| `state`, `changes` | link state at round start and state changes during the round (trace runs) |
| `t_end`, `in_horizon` | end of the round in seconds since trace start, and whether it ended within the horizon |
| `policy` | selector internals per round (epsilon, Q-value change) for FedCommRL and BOLR |

## License

The code is released under the MIT License (`LICENSE`). The data in `data/` and the measurements in `results/` are
released under the Creative Commons Attribution 4.0 International License (`data/LICENSE`).
