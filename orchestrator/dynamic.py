"""Dynamic conditions with a per-round protocol router (Phase 2).

Every protocol in cell["protos"] keeps a live session (server + n clients) for the whole run. A trace driver
changes the link state of all clients over wall-clock time (cyclic list of [duration_s, state, handover]); a
policy chooses the protocol for each FL round. Fixed-protocol arms run through the same harness (policy
"fixed:<proto>"), so fixed protocols and routers face identical conditions and identical idle sessions.

Metric: successful rounds completed within a fixed wall-clock horizon cell["horizon_s"] measured from trace start
(every arm sees the identical stretch of trace; rounds still running at the horizon are not counted). Each row records the chosen protocol, the state at round start and every
state change during the round.
"""
import math, queue, statistics, threading, time
import psutil


def mk_link(H, d):
    return H.Link(**d)


# ------------------------------------------------------------------ policies
class Fixed:
    def __init__(self, p): self.p = p
    def choose(self, state): return self.p
    def update(self, p, elapsed, ok): pass


class Oracle:
    """Knows the true link state at round start and picks the best protocol for it (table from static anchors)."""
    def __init__(self, table, rank): self.t, self.rank = table, rank
    def choose(self, state): return self.t.get(state, self.rank[0])
    def update(self, p, elapsed, ok): pass


class Reactive:
    """Stays on the current protocol; moves to the next one in the static ranking when a round fails or takes
    more than `factor` x the median of the current protocol's last 3 successful rounds."""
    def __init__(self, rank, factor=1.5): self.rank, self.f, self.cur, self.hist = rank, factor, 0, {p: [] for p in rank}
    def choose(self, state): return self.rank[self.cur]
    def update(self, p, elapsed, ok):
        h = self.hist[p]
        slow = len(h) >= 2 and elapsed > self.f * statistics.median(h[-3:])
        if ok:
            h.append(elapsed)
        if not ok or slow:
            self.cur = (self.cur + 1) % len(self.rank)
            self.hist[self.rank[self.cur]] = []   # the new protocol's old times describe an older state


class DUCB:
    """Discounted UCB (Garivier & Moulines 2011). Reward = fastest elapsed seen / elapsed (failed round = 0)."""
    def __init__(self, rank, gamma=0.9, c=0.3): self.rank, self.g, self.c, self.obs, self.best = rank, gamma, c, [], None
    def choose(self, state):
        tried = {p for p, _ in self.obs}
        for p in self.rank:
            if p not in tried:
                return p
        T = len(self.obs); N = {p: 0.0 for p in self.rank}; S = {p: 0.0 for p in self.rank}
        for k, (p, r) in enumerate(self.obs):
            w = self.g ** (T - 1 - k); N[p] += w; S[p] += w * r
        tot = sum(N.values())
        return max(self.rank, key=lambda p: S[p] / N[p] + self.c * math.sqrt(max(math.log(tot), 0) / N[p]) if N[p] > 0 else 1e9)
    def update(self, p, elapsed, ok):
        if ok:
            self.best = elapsed if self.best is None else min(self.best, elapsed)
        self.obs.append((p, (self.best / elapsed) if ok else 0.0))


def make_policy(cell):
    pol, rank = cell["policy"], cell.get("rank") or cell["protos"]
    if pol.startswith("fixed:"):
        return Fixed(pol.split(":", 1)[1])
    if pol == "oracle":
        return Oracle(cell["oracle_table"], rank)
    if pol == "reactive":
        return Reactive(rank)
    if pol == "ducb":
        return DUCB(rank)
    raise ValueError(pol)


# ------------------------------------------------------------------ trace driver
class TraceDriver(threading.Thread):
    """Applies a cyclic trace [[duration_s, state, handover], ...] to the clients' links. cell["trace"] is shared by
    all clients; cell["traces"] = {client: trace} gives every client its own trace (heterogeneous clients)."""
    def __init__(self, H, tb, cell):
        super().__init__(daemon=True)
        self.H, self.tb, self.cell, self.stop = H, tb, cell, threading.Event()
        per = cell.get("traces")
        self.tr = {i: (per[str(i)] if per else cell["trace"]) for i in range(1, tb.n + 1)}
        self.state = {i: self.tr[i][0][1] for i in self.tr}
        self.timeline = []
        self.lock = threading.Lock()

    def apply(self, i, name):
        st = self.cell["states"][name]
        self.tb.set_link(i, mk_link(self.H, st["up"]), mk_link(self.H, st["down"]))

    def run_client(self, i):
        k, t_next, tr = 0, self.t_start, self.tr[i]
        while not self.stop.is_set():
            dur, name, ho = tr[k % len(tr)]
            if ho:
                self.tb.handover(i, self.cell.get("gap_s", 0.2))
            self.apply(i, name)
            with self.lock:
                self.state[i] = name
                self.timeline.append((time.time(), name if not self.cell.get("traces") else f"{i}:{name}", bool(ho)))
            t_next += dur
            while time.time() < t_next and not self.stop.is_set():
                time.sleep(0.05)
            k += 1

    def run(self):
        self.t_start = time.time()
        if self.cell.get("traces"):
            ths = [threading.Thread(target=self.run_client, args=(i,), daemon=True) for i in self.tr]
            [t.start() for t in ths]; [t.join() for t in ths]
        else:   # shared trace: same as Phase 2 (all clients change together; handovers in parallel)
            k, t_next, tr = 0, self.t_start, self.tr[1]
            while not self.stop.is_set():
                dur, name, ho = tr[k % len(tr)]
                if ho:
                    ths = [threading.Thread(target=self.tb.handover, args=(i, self.cell.get("gap_s", 0.2))) for i in self.tr]
                    [t.start() for t in ths]; [t.join() for t in ths]
                for i in self.tr:
                    self.apply(i, name)
                with self.lock:
                    for i in self.tr:
                        self.state[i] = name
                    self.timeline.append((time.time(), name, bool(ho)))
                t_next += dur
                while time.time() < t_next and not self.stop.is_set():
                    time.sleep(0.05)
                k += 1

    def current(self):
        """Shared trace: the state name; per-client traces: {client: state}."""
        with self.lock:
            return dict(self.state) if self.cell.get("traces") else self.state[1]

    def between(self, a, b):
        with self.lock:
            return [(round(t - a, 2), n, h) for t, n, h in self.timeline if a < t <= b]


# ------------------------------------------------------------------ one run
def run_dynamic(cell, tb, brokers, base_round, H):
    if cell.get("pc"):   # per-client / per-direction selection (Phase 3)
        from dynamic_pc import run_dynamic_pc
        return run_dynamic_pc(cell, tb, brokers, base_round, H)
    n, protos = cell["n"], cell["protos"]
    H.set_cc(tb, cell.get("cc") or "cubic")
    first = cell["states"][cell["trace"][0][1]]
    for i in range(1, n + 1):  # sessions are set up on a loss-free link with the first state's rate and delay
        u, d = mk_link(H, first["up"]), mk_link(H, first["down"])
        tb.set_link(i, H.Link(u.bw, u.delay, u.jitter, 0, 1, u.qms), H.Link(d.bw, d.delay, d.jitter, 0, 1, d.qms))
    if "mqtt" in protos:
        brokers.restart_mqtt()
    for p in protos:
        brokers.need(p)
    procs = {p: H.start_procs(cell, p, n, tag=f".{p}") for p in protos}
    pol = make_policy(cell)
    rows, ok_n, k = [], 0, 0
    t_run0 = time.time()
    drv = TraceDriver(H, tb, cell); drv.start()
    timeout, attempt = cell["timeout"], cell["attempt"]
    try:
        horizon = cell["horizon_s"]
        while time.time() - t_run0 < horizon and k < cell["max_rounds"]:
            k += 1
            state0 = drv.current()
            p = pol.choose(state0)
            S, C = procs[p]
            rid = base_round + k
            t_send = time.time(); psutil.cpu_percent(None)
            S.send({"op": "round", "r": rid, "timeout": timeout})
            st = S.wait_op("started", 30)
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
                sd = S.wait_op("round_done", 30)
            except queue.Empty:
                sd = {"recv": {}}
            t_end = time.time()
            t0 = st.get("t0", t_send)
            recv = {int(kk): v for kk, v in (sd.get("recv") or {}).items()}
            ok = len(recv) == n and all(res[i].get("ok") for i in C)
            elapsed = (max(recv.values()) - t0) if ok else (t_end - t_send)
            pol.update(p, elapsed, ok)
            in_h = (t_end - t_run0) <= horizon
            ok_n += ok and in_h
            rows.append({"r": k, "t_end": round(t_end - t_run0, 2), "in_horizon": in_h, "proto": p, "state": state0, "changes": drv.between(t_send, t_end), "ok": ok,
                         "T_round": elapsed if ok else None, "elapsed": elapsed, "t_since_start": round(t_send - t_run0, 2),
                         "host_cpu_pct": psutil.cpu_percent(None),
                         "clients": {i: {"ok": res[i].get("ok"), "err": res[i].get("err"), "attempts": res[i].get("attempts"),
                                         "dl": (res[i]["t_dl"] - t0) if res[i].get("t_dl") else None,
                                         "recv": (recv[i] - t0) if i in recv else None} for i in C}})
            print(f"  r{k} {p:5s} state={state0} T={elapsed:.1f} ok={ok}", flush=True)
    finally:
        drv.stop.set()
        for S, C in procs.values():
            for P in list(C.values()) + [S]:
                P.stop()
    rows.append({"summary": True, "horizon_s": cell["horizon_s"], "ok_rounds": ok_n, "attempted": k,
                 "s_per_round": cell["horizon_s"] / ok_n if ok_n else None,
                 "timeline": [(round(t - t_run0, 2), nme, h) for t, nme, h in drv.timeline]})
    return rows
