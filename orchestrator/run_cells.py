"""Resumable campaign runner.

  python3 orchestrator/run_cells.py campaign/phase2_static.json [--budget-min M] [--only PREFIX] [--only-re REGEX]

Each entry of the campaign file is one run (one cell x protocol x repetition) with a unique 'id'.
A finished run is written atomically to results/runs/<id>.json; runs whose file exists are skipped, so a
campaign can be interrupted at any time and resumed later. With --budget-min the runner starts no new run
once the given time budget is used up.
"""
import argparse, re, json, os, queue, random, signal, subprocess, sys, threading, time, traceback
from pathlib import Path
import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "netem"))
from topo import Testbed, Link, sh, srv_ip, cpu_limit  # noqa: E402

BIN = ROOT / "bench" / "fedbench"
QBIN = ROOT / "qbench" / "target" / "release" / "qbench"
CERTS = ROOT / "certs"
RUNS = ROOT / "results" / "runs"
LOGS = Path("/tmp/fb_logs")
RMQ_ENV = {"RABBITMQ_NODENAME": "rabbit@localhost", "RABBITMQ_NODE_IP_ADDRESS": "0.0.0.0", "HOME": "/var/lib/rabbitmq",
           "RABBITMQ_LOG_BASE": "/tmp/fb_rmqlog", "RABBITMQ_MNESIA_BASE": "/tmp/fb_rmqdb",
           "RABBITMQ_CONFIG_FILE": "/tmp/fb_rabbitmq", "PATH": "/usr/sbin:/usr/bin:/bin:/usr/lib/rabbitmq/bin"}


def kill_all():
    me = {os.getpid(), os.getppid()}
    for p in psutil.process_iter(["pid", "name"]):
        if p.info["pid"] in me:
            continue
        if (p.info["name"] or "") in ("fedbench", "qbench", "mosquitto", "beam.smp", "epmd", "erl_child_setup", "inet_gethost", "nfqdelay"):
            try:
                p.kill()
            except psutil.Error:
                pass
    time.sleep(0.5)


class Proc:
    """A fedbench process speaking JSON lines on stdin/stdout."""
    def __init__(self, args, ns, log, binary=BIN):
        self.p = subprocess.Popen(["ip", "netns", "exec", ns, str(binary)] + args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=open(log, "a"), text=True, bufsize=1)
        self.q = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            try:
                self.q.put(json.loads(line))
            except json.JSONDecodeError:
                pass
        self.q.put({"op": "eof"})

    def send(self, d):
        self.p.stdin.write(json.dumps(d) + "\n"); self.p.stdin.flush()

    def recv(self, timeout):
        return self.q.get(timeout=timeout)

    def wait_op(self, op, timeout):
        end = time.time() + timeout
        while True:
            m = self.recv(max(0.01, end - time.time()))
            if m.get("op") in (op, "fatal", "eof"):
                return m

    def stop(self):
        try:
            self.send({"op": "quit"}); self.p.wait(3)
        except Exception:
            self.p.kill()


class Brokers:
    def __init__(self):
        self.mq = self.rmq = None

    def need(self, proto):
        if proto == "mqtt" and (self.mq is None or self.mq.poll() is not None):
            conf = Path("/tmp/fb_mosquitto.conf")
            conf.write_text((ROOT / "config" / "mosquitto.conf").read_text().replace("CERTS", str(CERTS)))
            self.mq = subprocess.Popen(["ip", "netns", "exec", "srv", "mosquitto", "-c", str(conf)],
                                       stdout=open(LOGS / "mosquitto.log", "a"), stderr=subprocess.STDOUT)
            self._wait_port(8883)
        if proto == "amqp" and (self.rmq is None or self.rmq.poll() is not None):
            Path("/tmp/fb_rabbitmq.conf").write_text((ROOT / "config" / "rabbitmq.conf").read_text().replace("CERTS", str(CERTS)))
            sh("rm -rf /tmp/fb_rmqdb", check=False)
            self.rmq = subprocess.Popen(["ip", "netns", "exec", "srv", "rabbitmq-server"], env=RMQ_ENV,
                                        stdout=open(LOGS / "rabbitmq.log", "a"), stderr=subprocess.STDOUT)
            self._wait_port(5671, 120)
            time.sleep(2)

    def restart_mqtt(self):
        """Fresh broker per MQTT run: no persistent sessions or queued messages carry over between runs."""
        if self.mq is not None:
            self.mq.terminate(); self.mq.wait(5); self.mq = None
        self.need("mqtt")

    @staticmethod
    def _wait_port(port, timeout=30):
        for _ in range(int(timeout * 4)):
            if f":{port} " in sh("ip netns exec srv ss -ltnH", check=False):
                return
            time.sleep(0.25)
        raise RuntimeError(f"broker port {port} not listening")

    def cpu(self):
        tot = 0.0
        for p in psutil.process_iter(["name"]):
            if (p.info["name"] or "") in ("mosquitto", "beam.smp"):
                try:
                    ct = p.cpu_times(); tot += ct.user + ct.system
                except psutil.Error:
                    pass
        return tot


def expected_round_s(cell):
    """Rough expected round time, used only for timeouts and scheduling."""
    worst = 0.0
    for spec in cell["links"]:
        up, dn = spec["up"], spec["down"]
        rtt = (up["delay"] + dn["delay"]) / 1000
        t = 0.0
        for L in (up, dn):
            bw = L["bw"] * 1e6 / 8
            loss = max(L["loss"] / 100, 1e-4)
            mathis = 1460 * 1.22 / (max(rtt, 0.002) * loss ** 0.5)  # TCP throughput bound (Mathis et al.)
            t += cell["size"] / min(bw, mathis) + 4 * rtt
        worst = max(worst, t)
    return worst / max(cell.get("cpu", 1) or 1, 0.1) if cell.get("cpu") else worst


def links_for(cell, n):
    out = []
    for i in range(1, n + 1):
        spec = cell["links"][min(i - 1, len(cell["links"]) - 1)]
        out.append((Link(**spec["up"]), Link(**spec["down"])))
    return out


def event_thread(tb, cell, t_start, stop):
    ev = cell.get("events") or {}
    if not ev:
        return None

    def run():
        if ev["type"] == "outage":  # outage of dur_s seconds, at_s after every round start (rounds signalled via ev_round)
            while not stop.is_set():
                r0 = cell["_round_start"].get()
                if r0 is None:
                    return
                time.sleep(max(0, r0 + ev["at_s"] - time.time()))
                for i in ev["clients"]:
                    tb.outage(i, True)
                time.sleep(ev["dur_s"])
                for i in ev["clients"]:
                    tb.outage(i, False)
        elif ev["type"] == "handover":  # periodic address change with a gap
            k = 1
            while not stop.is_set():
                t = t_start + k * ev["every_s"]
                while time.time() < t and not stop.is_set():
                    time.sleep(0.05)
                if stop.is_set():
                    return
                for i in ev["clients"]:
                    tb.handover(i, ev.get("gap_s", 0))
                k += 1
    th = threading.Thread(target=run, daemon=True); th.start()
    return th


def set_cc(tb, cc, extra=None):
    """Congestion control for every TCP socket in the testbed namespaces (QUIC stacks choose their own:
    quic-go = CUBIC, qbench/quinn = the -cc flag)."""
    sh("sysctl -qw net.ipv4.tcp_allowed_congestion_control='reno cubic bbr'", check=False)
    for ns in ["srv"] + [f"c{i}" for i in range(1, tb.n + 1)]:
        sh(f"ip netns exec {ns} sysctl -qw net.ipv4.tcp_congestion_control={cc}")
        for k, v in (extra or {}).items():   # per-cell TCP knobs, e.g. net.ipv4.tcp_slow_start_after_idle
            sh(f"ip netns exec {ns} sysctl -qw {k}={v}")


def start_procs(cell, proto, n, tag=""):
    """Start the server and the n clients of one protocol; returns (server, {i: client}) once all are ready."""
    binary = QBIN if proto == "quinn" else BIN
    common = ["-proto", proto, "-n", str(n), "-size", str(cell["size"]), "-chunk", str(cell.get("chunk", 1 << 20)),
              "-certs", str(CERTS)]
    if proto == "quinn":
        common += ["-cc", cell.get("cc") or "cubic"]
    else:
        common += ["-window", str(cell.get("window", 64))]
        if cell.get("grpcwin"):
            common += ["-grpcwin", str(cell["grpcwin"])]
    lg = LOGS / f"{cell['id']}{tag}"
    S = Proc(["-role", "server", "-server", "127.0.0.1"] + common, "srv", f"{lg}.server.log", binary)
    m = S.wait_op("ready", 60)
    if m.get("op") != "ready":
        raise RuntimeError(f"{proto} server not ready: {m}")
    C = {}
    for i in range(1, n + 1):
        C[i] = Proc(["-role", "client", "-id", str(i), "-server", srv_ip(i)] + common, f"c{i}", f"{lg}.c{i}.log", binary)
        if cell.get("cpu"):
            cpu_limit(C[i].p.pid, cell["cpu"], f"c{i}")
    for i, P in C.items():
        m = P.wait_op("ready", 60)
        if m.get("op") != "ready":
            raise RuntimeError(f"{proto} client {i} not ready: {m}")
    return S, C


def run_one(cell, tb, brokers, base_round):
    n, proto = cell["n"], cell["proto"]
    set_cc(tb, cell.get("cc") or "bbr", cell.get("sysctl"))
    # sessions are established on a loss-free link with the cell's rate and delay; impairments
    # (loss, bursts) are applied once every process is ready, i.e. they act on transfers, not on setup
    for i, (u, d) in enumerate(links_for(cell, n), start=1):
        tb.set_link(i, Link(u.bw, u.delay, u.jitter, 0, 1, u.qms), Link(d.bw, d.delay, d.jitter, 0, 1, d.qms))
    if proto == "mqtt":
        brokers.restart_mqtt()
    brokers.need(proto)
    S, C = start_procs(cell, proto, n)
    for i, (u, d) in enumerate(links_for(cell, n), start=1):
        tb.set_link(i, u, d)
    timeout = max(60.0, 6 * expected_round_s(cell) + 20)
    # per-attempt stall limit of the shared retry policy: twice the expected round time, at least 10 s
    attempt = cell.get("attempt") or max(10.0, 2 * expected_round_s(cell))
    rows, stop = [], threading.Event()
    cell["_round_start"] = queue.Queue()
    ev = event_thread(tb, cell, time.time(), stop)
    try:
        for r in range(1, cell["rounds"] + 1):
            rid = base_round + r
            b0 = brokers.cpu(); psutil.cpu_percent(None)
            S.send({"op": "round", "r": rid, "timeout": timeout})
            st = S.wait_op("started", 30)
            cell["_round_start"].put(st.get("t0", time.time()))
            res = {}

            def go(i):
                C[i].send({"op": "round", "r": rid, "timeout": timeout, "attempt": attempt})
                try:
                    res[i] = C[i].wait_op("round_done", timeout + 30)
                except queue.Empty:
                    res[i] = {"ok": False, "err": "no response"}
            th = [threading.Thread(target=go, args=(i,)) for i in C]
            [t.start() for t in th]; [t.join() for t in th]
            try:
                sd = S.wait_op("round_done", timeout + 30)
            except queue.Empty:
                sd = {"recv": {}}
            t0 = st.get("t0", float("nan"))
            recv = {int(k): v for k, v in (sd.get("recv") or {}).items()}
            ok_all = len(recv) == n and all(res[i].get("ok") for i in C)
            row = {"r": r, "t0": t0, "ok": ok_all,
                   "T_round": (max(recv.values()) - t0) if ok_all else None,
                   "server_cpu_s": sd.get("cpu_s"), "broker_cpu_s": brokers.cpu() - b0, "host_cpu_pct": psutil.cpu_percent(None),
                   "bad_chunks": sd.get("bad_chunks"), "pub_done": sd.get("pub_done"), "clients": {}}
            for i in C:
                x = res[i]
                row["clients"][i] = {"ok": x.get("ok"), "err": x.get("err"), "dl": (x["t_dl"] - t0) if x.get("t_dl") else None,
                                     "ul": (x["t_ul"] - x["t_dl"]) if x.get("t_ul") and x.get("t_dl") else None,
                                     "recv": (recv[i] - t0) if i in recv else None, "cpu_s": x.get("cpu_s"),
                                     "attempts": x.get("attempts")}
            rows.append(row)
            print(f"  r{r} T={row['T_round'] if row['T_round'] is None else round(row['T_round'], 2)} ok={ok_all}", flush=True)
            if not ok_all and cell.get("events") is None:
                # transient failure without injected events: keep going, the row is flagged
                pass
    finally:
        stop.set(); cell["_round_start"].put(None)
        for P in list(C.values()) + [S]:
            P.stop()
    return rows


import dynamic  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("campaign"); ap.add_argument("--budget-min", type=float, default=0, help="0 = no limit")
    ap.add_argument("--only", default="")
    ap.add_argument("--max-rep", type=int, default=None, help="only runs with rep <= this")
    ap.add_argument("--only-re", default=None, help="only runs whose id matches this regular expression")
    a = ap.parse_args()
    cells = json.loads(Path(a.campaign).read_text())["runs"]
    RUNS.mkdir(parents=True, exist_ok=True); LOGS.mkdir(parents=True, exist_ok=True)
    todo = [c for c in cells if c["id"].startswith(a.only) and not (RUNS / f"{c['id']}.json").exists()
            and (a.max_rep is None or c.get("rep", 0) <= a.max_rep)
            and (a.only_re is None or re.match(a.only_re, c["id"]))]
    print(f"{len(cells)} runs in campaign, {len(todo)} to do", flush=True)
    t_start = time.time()
    deadline = t_start + a.budget_min * 60 if a.budget_min > 0 else float("inf")
    kill_all()
    tb, brokers = None, Brokers()
    done = 0
    for k, cell in enumerate(todo):
        est = cell.get("est_s") or expected_round_s(cell) * cell["rounds"] + 20
        if time.time() + min(est, 1800) > deadline:
            print(f"budget reached after {done} runs; {len(todo) - done} left", flush=True)
            break
        if tb is None or tb.n != cell["n"]:
            kill_all(); brokers = Brokers()
            if tb:
                tb.down()
            tb = Testbed(cell["n"]).up()
        print(f"[{k + 1}/{len(todo)}] {cell['id']}", flush=True)
        t0 = time.time()
        try:
            br = random.randint(1, 1 << 30) // 1000 * 1000
            if cell.get("trace"):
                rows = dynamic.run_dynamic(dict(cell), tb, brokers, br, sys.modules[__name__])
            else:
                rows = run_one(dict(cell), tb, brokers, base_round=br)
            status = "ok"
        except Exception as e:
            traceback.print_exc()
            rows, status = [], f"error: {e}"
            kill_all(); brokers = Brokers(); tb.down(); tb = None
        out = {k2: v for k2, v in cell.items() if not k2.startswith("_")}
        out.update(status=status, rows=rows, started=t0, wall_s=time.time() - t0)
        if status != "ok":
            print(f"  run failed ({status}); not saved, will be retried next time", flush=True)
            continue
        tmp = RUNS / f".{cell['id']}.tmp"; tmp.write_text(json.dumps(out)); tmp.rename(RUNS / f"{cell['id']}.json")
        done += 1
    kill_all()
    if tb:
        tb.down()
    print(f"BATCH_END done={done} remaining={len(todo) - done} elapsed_min={(time.time() - t_start) / 60:.1f}", flush=True)


if __name__ == "__main__":
    main()
