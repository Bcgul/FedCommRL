"""Samples the resident set size (RSS) of every testbed process every 0.2 s until killed; on SIGTERM writes the
peak RSS per (process, role, protocol) to the given JSON file."""
import json, signal, sys, time
import psutil
out = sys.argv[1]; peak = {}
NAMES = ("fedbench", "qbench", "mosquitto", "beam.smp")
def dump(*_):
    json.dump(peak, open(out, "w"), indent=1); sys.exit(0)
signal.signal(signal.SIGTERM, dump)
while True:
    for p in psutil.process_iter(["name", "cmdline", "memory_info"]):
        n = p.info["name"] or ""
        if n not in NAMES or not p.info["memory_info"]:
            continue
        cl = p.info["cmdline"] or []
        def arg(k):
            return cl[cl.index(k) + 1] if k in cl else ""
        role = arg("-role") or "broker"; proto = arg("-proto") or n
        key = f"{n}|{role}|{proto}"
        rss = p.info["memory_info"].rss / 2**20
        peak[key] = max(peak.get(key, 0), rss)
    time.sleep(0.2)
