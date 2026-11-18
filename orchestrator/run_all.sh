#!/bin/bash
# Runs all campaigns in the order in which their inputs depend on each other. Every step is resumable:
# finished runs (results/runs/<id>.json) are skipped. Requires root (network namespaces, NFQUEUE).
#   bash orchestrator/run_all.sh
set -e
cd "$(dirname "$0")/.."
R="python3 orchestrator/run_cells.py"

$R campaign/phase1.json                # preliminary campaign (host default congestion control, BBR)
$R campaign/phase2_static.json         # static, event and burst-loss cells, second QUIC stack, BBR check
$R campaign/phase2_diag_grpc.json      # gRPC flow-control window diagnosis
python3 orchestrator/make_campaign2.py dynamic   # oracle table and router ranking from the static anchors
$R campaign/phase2_dynamic.json        # global routers on changing-link traces
$R campaign/phase3_mech.json           # single transfers for the QUIC vs TCP-CUBIC mechanism
$R campaign/phase3_dynamic.json        # per-client, per-direction selectors (FedCommRL, BOLR) live
python3 orchestrator/make_mem_campaign.py
python3 analysis/mem_sampler.py results/mem/peak_rss.json & SAMPLER=$!   # memory per process
$R campaign/mem.json
kill -TERM $SAMPLER; wait $SAMPLER || true
mkdir -p results/mem/runs && mv results/runs/mem_*.json results/mem/runs/ 2>/dev/null || true
