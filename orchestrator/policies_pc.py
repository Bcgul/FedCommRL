"""Per-client, per-direction protocol selection policies (Phase 3). Used live (dynamic_pc.py) and in the
trace-driven replay (analysis/replay_pc.py), so both evaluate exactly the same code.

Interface:
    choose(k, state)            -> {client: (downlink_proto, uplink_proto)}
    update(k, state, choices, obs)
where obs[client] = {"dl_s", "ul_s", "ok_dl", "ok_ul", "cpu_dl", "cpu_ul"} (seconds; ul_* None if the
upload was not attempted because the download failed).

QLearn is the FedCommRL agent: one agent per client with separate downlink and uplink Q-tables
(shared=True uses one table for both directions), epsilon-greedy with epsilon_t = eps0 * decay^t, the
bootstrapped update Q <- Q + a [r + g max_a' Q(s', a') - Q], and the multi-objective reward
r = -(w_t T^ + w_e E^ + w_c C^) with running min-max normalisation of each term. Each direction makes one
decision per round and is rewarded with its own delivered time; with the non-positive reward, Q0 = 0 is
optimistic, so every protocol is tried in a state before it is dropped.
State = network-state class observed at round start (perfect sensing). Resource and battery levels are
constant in the emulation and are not part of the state.
"""
import math, random

PROTOS = ["mqtt", "amqp", "grpc", "http3"]
K_TX = 1e-8      # J/bit (deployed constant)
P_CPU = 10.0     # W


def _st(state, i):
    """Per-client state: a dict {client: state} (heterogeneous clients) or one state shared by all clients."""
    return state[i] if isinstance(state, dict) else state


class Fixed:
    name = "fixed"

    def __init__(self, p, clients):
        self.p, self.clients = p, clients

    def choose(self, k, state):
        return {i: (self.p, self.p) for i in self.clients}

    def update(self, k, state, choices, obs):
        pass

    def log(self):
        return {}


class BOLR:
    """Best-of-Last-Round (deployable form): per client and direction, probe every protocol once, then use the
    protocol with the lowest last observed transfer time (a failed transfer counts with its elapsed time)."""
    name = "bolr"

    def __init__(self, clients, protos=PROTOS):
        self.clients, self.protos = clients, protos
        self.last = {(i, d): {} for i in clients for d in ("dl", "ul")}

    def _pick(self, i, d, k):
        seen = self.last[(i, d)]
        for p in self.protos:
            if p not in seen:
                return p
        return min(self.protos, key=lambda p: seen[p])

    def choose(self, k, state):
        return {i: (self._pick(i, "dl", k), self._pick(i, "ul", k)) for i in self.clients}

    def update(self, k, state, choices, obs):
        for i, (d, u) in choices.items():
            o = obs[i]
            self.last[(i, "dl")][d] = o["dl_s"]
            if o.get("ul_s") is not None:
                self.last[(i, "ul")][u] = o["ul_s"]

    def log(self):
        return {}


class _Norm:
    def __init__(self):
        self.lo, self.hi = math.inf, -math.inf

    def __call__(self, x):
        self.lo, self.hi = min(self.lo, x), max(self.hi, x)
        return 0.0 if self.hi <= self.lo else (x - self.lo) / (self.hi - self.lo)


class QLearn:
    name = "rl"

    def __init__(self, clients, protos=PROTOS, alpha=0.1, gamma=0.9, eps0=1.0, decay=0.95, eps_min=0.01,
                 weights=(1 / 3, 1 / 3, 1 / 3), shared=False, size_bytes=2_500_000, seed=0):
        self.clients, self.protos = clients, protos
        self.a, self.g, self.eps0, self.decay, self.eps_min = alpha, gamma, eps0, decay, eps_min
        self.w, self.shared, self.bits = weights, shared, 8 * size_bytes
        self.rng = random.Random(seed)
        self.Q = {}                    # (client, table) -> {state: [q per protocol]}
        self.norm = {}                 # (client, direction, term) -> _Norm
        self.t = 0
        self.pending = []              # (client, table, state, action index, reward) awaiting s'
        self.last_dq = 0.0

    def _tab(self, i, d):
        key = (i, "shared" if self.shared else d)
        return self.Q.setdefault(key, {})

    def _q(self, i, d, s):
        return self._tab(i, d).setdefault(s, [0.0] * len(self.protos))

    def eps(self):
        return max(self.eps_min, self.eps0 * self.decay ** self.t)

    def _act(self, i, d, s):
        q = self._q(i, d, s)
        if self.rng.random() < self.eps():
            return self.rng.randrange(len(self.protos))
        m = max(q)
        return self.rng.choice([j for j, v in enumerate(q) if v == m])

    def _flush(self, s_next):
        dq = 0.0
        for i, d, s, a, r in self.pending:
            q = self._q(i, d, s)
            target = r + self.g * max(self._q(i, d, _st(s_next, i)))
            delta = self.a * (target - q[a])
            q[a] += delta
            dq += abs(delta)
        self.last_dq = dq / max(len(self.pending), 1)
        self.pending = []

    def choose(self, k, state):
        self._flush(state)             # complete last round's updates with s' = the state now observed
        return {i: (self.protos[self._act(i, "dl", _st(state, i))], self.protos[self._act(i, "ul", _st(state, i))]) for i in self.clients}

    def _reward(self, i, d, t_s, cpu_s, ok):
        if not ok:
            return -1.0
        n = lambda term: self.norm.setdefault((i, d, term), _Norm())
        e = K_TX * self.bits + P_CPU * (cpu_s or 0.0)
        c = (cpu_s or 0.0) / max(t_s, 1e-6)
        wt, we, wc = self.w
        return -(wt * n("t")(t_s) + we * n("e")(e) + wc * n("c")(c))

    def update(self, k, state, choices, obs):
        for i, (d, u) in choices.items():
            o = obs[i]
            self.pending.append((i, "dl", _st(state, i), self.protos.index(d), self._reward(i, "dl", o["dl_s"], o.get("cpu_dl"), o["ok_dl"])))
            if o.get("ul_s") is not None:
                self.pending.append((i, "ul", _st(state, i), self.protos.index(u), self._reward(i, "ul", o["ul_s"], o.get("cpu_ul"), o["ok_ul"])))
        self.t += 1

    def greedy(self, state):
        out = {}
        for i in self.clients:
            out[i] = tuple(self.protos[max(range(len(self.protos)), key=lambda j: self._q(i, d, _st(state, i))[j])] for d in ("dl", "ul"))
        return out

    def log(self):
        return {"eps": round(self.eps(), 4), "dq": round(self.last_dq, 5)}


def make_policy_pc(cell, clients, seed=0):
    pol = cell["policy"]
    if pol.startswith("fixed:"):
        return Fixed(pol.split(":", 1)[1], clients)
    if pol == "bolr":
        return BOLR(clients)
    if pol.startswith("rl"):
        kw = dict(cell.get("rl", {}))
        if "weights" in kw:
            kw["weights"] = tuple(kw["weights"])
        return QLearn(clients, size_bytes=cell["size"], seed=seed, **kw)
    raise ValueError(pol)
