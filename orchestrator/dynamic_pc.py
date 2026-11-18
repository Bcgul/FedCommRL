"""Dynamic traces with per-client, per-direction protocol selection (Phase 3).

Same harness as dynamic.py (all protocols keep live sessions; the trace driver changes every client's link over
wall-clock time; fixed horizon), but every client chooses a downlink and an uplink protocol each round. A server
receives the list of clients whose downlink it delivers and whose upload it expects; a client whose two directions
use different protocols downloads through one protocol process and then uploads through the other.
Round time = last confirmed upload - round start (synchronous FedAvg waits for the slowest client).
"""
import queue, threading, time
import psutil

from dynamic import TraceDriver, mk_link
from policies_pc import make_policy_pc


def run_dynamic_pc(cell, tb, brokers, base_round, H):
    n, protos = cell["n"], cell["protos"]
    clients = list(range(1, n + 1))
    H.set_cc(tb, cell.get("cc") or "cubic")
    first = cell["states"][cell["trace"][0][1]]
    for i in clients:
        u, d = mk_link(H, first["up"]), mk_link(H, first["down"])
        tb.set_link(i, H.Link(u.bw, u.delay, u.jitter, 0, 1, u.qms), H.Link(d.bw, d.delay, d.jitter, 0, 1, d.qms))
    if "mqtt" in protos:
        brokers.restart_mqtt()
    for p in protos:
        brokers.need(p)
    procs = {p: H.start_procs(cell, p, n, tag=f".{p}") for p in protos}
    pol = make_policy_pc(cell, clients, seed=cell.get("rep", 0))
    rows, ok_n, k = [], 0, 0
    t_run0 = time.time()
    drv = TraceDriver(H, tb, cell); drv.start()
    timeout, attempt, horizon = cell["timeout"], cell["attempt"], cell["horizon_s"]
    try:
        while time.time() - t_run0 < horizon and k < cell["max_rounds"]:
            k += 1
            state0 = drv.current()
            ch = pol.choose(k, state0)
            used = sorted({p for d, u in ch.values() for p in (d, u)})
            rid = base_round + k
            t_send = time.time(); psutil.cpu_percent(None)
            t0s = []
            for p in used:
                S = procs[p][0]
                S.send({"op": "round", "r": rid, "timeout": timeout,
                        "dl": [i for i in clients if ch[i][0] == p], "ul": [i for i in clients if ch[i][1] == p]})
            for p in used:
                t0s.append(procs[p][0].wait_op("started", 30).get("t0", t_send))
            t0 = min(t0s)
            res = {}

            def go(i):
                d, u = ch[i]
                cmd = {"op": "round", "r": rid, "timeout": timeout, "attempt": attempt}
                try:
                    if d == u:
                        procs[d][1][i].send(cmd)
                        r = procs[d][1][i].wait_op("round_done", timeout + 30)
                        res[i] = (r, None)
                    else:
                        procs[d][1][i].send(dict(cmd, phase="dl"))
                        rd = procs[d][1][i].wait_op("round_done", timeout + 30)
                        if not rd.get("ok"):
                            res[i] = (rd, None); return
                        procs[u][1][i].send(dict(cmd, phase="ul"))
                        res[i] = (rd, procs[u][1][i].wait_op("round_done", timeout + 30))
                except queue.Empty:
                    res[i] = ({"ok": False, "err": "no response"}, None)
            th = [threading.Thread(target=go, args=(i,)) for i in clients]
            [t.start() for t in th]; [t.join() for t in th]
            recv = {}
            for p in used:
                try:
                    sd = procs[p][0].wait_op("round_done", 30)
                except queue.Empty:
                    sd = {"recv": {}}
                for kk, v in (sd.get("recv") or {}).items():
                    if ch[int(kk)][1] == p:
                        recv[int(kk)] = v
            t_end = time.time()
            obs, cl = {}, {}
            for i in clients:
                d, u = ch[i]
                rd, ru = res[i]
                t_dl = rd.get("t_dl")
                ok_dl = t_dl is not None
                if ru is None:   # same protocol both ways (or download failed)
                    ok_ul = bool(rd.get("ok")) and i in recv
                    cpu = rd.get("cpu_s") or 0.0
                    dl_s = (t_dl - t0) if ok_dl else (t_end - t0)
                    ul_s = (recv[i] - t_dl) if ok_ul else ((t_end - t_dl) if ok_dl else None)
                    tot = (dl_s or 0) + (ul_s or 0)
                    cpu_dl = cpu * dl_s / tot if tot > 0 else cpu
                    cpu_ul = cpu - cpu_dl if ul_s is not None else None
                else:
                    ok_ul = bool(ru.get("ok")) and i in recv
                    dl_s = t_dl - t0
                    ul_s = (recv[i] - t_dl) if ok_ul else (t_end - t_dl)
                    cpu_dl, cpu_ul = rd.get("cpu_s"), ru.get("cpu_s")
                obs[i] = {"dl_s": dl_s, "ul_s": ul_s, "ok_dl": ok_dl, "ok_ul": ok_ul, "cpu_dl": cpu_dl, "cpu_ul": cpu_ul}
                cl[i] = {"d": d, "u": u, "ok": ok_dl and ok_ul, "dl_s": round(dl_s, 3), "ul_s": None if ul_s is None else round(ul_s, 3),
                         "cpu_dl": cpu_dl, "cpu_ul": cpu_ul, "err": (ru or rd).get("err"),
                         "attempts": (rd.get("attempts") or 0) + ((ru or {}).get("attempts") or 0)}
            ok = all(c["ok"] for c in cl.values())
            elapsed = (max(recv.values()) - t0) if ok else (t_end - t_send)
            pol.update(k, state0, ch, obs)
            in_h = (t_end - t_run0) <= horizon
            ok_n += ok and in_h
            rows.append({"r": k, "t_end": round(t_end - t_run0, 2), "in_horizon": in_h, "state": state0,
                         "changes": drv.between(t_send, t_end), "ok": ok, "T_round": elapsed if ok else None,
                         "elapsed": elapsed, "t_since_start": round(t_send - t_run0, 2), "host_cpu_pct": psutil.cpu_percent(None),
                         "policy": pol.log(), "clients": cl})
            mix = " ".join(f"{c['d'][:2]}/{c['u'][:2]}" for c in cl.values())
            print(f"  r{k} state={state0} T={elapsed:.1f} ok={ok} [{mix}] {pol.log()}", flush=True)
    finally:
        drv.stop.set()
        for S, C in procs.values():
            for P in list(C.values()) + [S]:
                P.stop()
    rows.append({"summary": True, "horizon_s": horizon, "ok_rounds": ok_n, "attempted": k,
                 "s_per_round": horizon / ok_n if ok_n else None,
                 "timeline": [(round(t - t_run0, 2), nme, h) for t, nme, h in drv.timeline]})
    return rows
