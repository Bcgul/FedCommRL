#!/bin/bash
# One-time setup of a Debian/Ubuntu machine (run as root). Idempotent.
#   bash setup.sh
set -e
cd "$(dirname "$0")"
export DEBIAN_FRONTEND=noninteractive
need=""
for p in iproute2 mosquitto rabbitmq-server iperf3 ethtool iptables libnetfilter-queue-dev libmnl-dev gcc openssl; do
  dpkg -s $p >/dev/null 2>&1 || need="$need $p"
done
if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need >/dev/null; fi
systemctl stop mosquitto rabbitmq-server 2>/dev/null || true
pip install -q -r requirements.txt 2>/dev/null || pip install -q --break-system-packages -r requirements.txt
gcc -O2 -o netem/nfqdelay netem/nfqdelay.c -lnetfilter_queue -lnfnetlink -lmnl
bash config/make_certs.sh "$PWD/certs"
# go.mod resolves golang.org/x/* and google.golang.org/* through their GitHub mirrors
( cd bench && GOPROXY=direct GOFLAGS=-mod=mod go build -o fedbench . )
( cd qbench && cargo build --release )
echo "setup ok"
