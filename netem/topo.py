"""Single-host network emulation: one server namespace 'srv', N client namespaces 'c1'..'cN',
one point-to-point veth link per client (10.20.i.0/24; server 10.20.i.1, client 10.20.i.2 or .3).

Per link and direction:
  rate        HTB on the sender's egress (client end = uplink, server end = downlink)
  delay, jitter, loss, burst-loss, outages
              nfqdelay at the RECEIVER's INPUT hook (srv INPUT queue 10+i = uplink, client INPUT queue 2 = downlink);
              delaying at the sender would interact with TCP Small Queues and throttle the sender.
Handover = the client's address changes (.2 <-> .3), optionally after a gap with 100 % loss; established TCP
connections from the old address die, QUIC can continue on the new path if the stack supports it.
"""
import os, subprocess, time, signal
from pathlib import Path

HERE = Path(__file__).resolve().parent
NFQ = str(HERE / "nfqdelay")


def sh(cmd, check=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\n{r.stderr}")
    return r.stdout


def srv_ip(i):
    return f"10.20.{i}.1"


class Link:
    """One direction of one client link."""
    def __init__(self, bw=1000.0, delay=0.0, jitter=0.0, loss=0.0, burst=1.0, qms=100.0):
        # qms = bottleneck buffer in milliseconds of line rate (pfifo limit = bw * qms, at least 10 packets)
        self.bw, self.delay, self.jitter, self.loss, self.burst, self.qms = bw, delay, jitter, loss, burst, qms

    def as_dict(self):
        return dict(bw=self.bw, delay=self.delay, jitter=self.jitter, loss=self.loss, burst=self.burst, qms=self.qms)


class Testbed:
    def __init__(self, n_clients=5, workdir="/tmp/fbnet"):
        self.n = n_clients
        self.wd = Path(workdir); self.wd.mkdir(parents=True, exist_ok=True)
        self.daemons, self.addr, self.links = {}, {}, {}

    def up(self):
        self.down()
        sh("sysctl -qw net.core.rmem_max=7500000 net.core.wmem_max=7500000", check=False)  # quic-go UDP buffers
        sh("ip netns add srv"); sh("ip netns exec srv ip link set lo up")
        for i in range(1, self.n + 1):
            c = f"c{i}"
            sh(f"ip netns add {c}"); sh(f"ip netns exec {c} ip link set lo up")
            sh(f"ip link add s{i} type veth peer name e{i}")
            sh(f"ip link set s{i} netns srv"); sh(f"ip link set e{i} netns {c}")
            sh(f"ip netns exec srv ip addr add {srv_ip(i)}/24 dev s{i}"); sh(f"ip netns exec srv ip link set s{i} up")
            # promote_secondaries: deleting the old address on handover must not also remove the new one
            sh(f"ip netns exec {c} sysctl -qw net.ipv4.conf.all.promote_secondaries=1 net.ipv4.conf.e{i}.promote_secondaries=1", check=False)
            sh(f"ip netns exec {c} ip addr add 10.20.{i}.2/24 dev e{i}"); sh(f"ip netns exec {c} ip link set e{i} up")
            self.addr[i] = 2
            for dev, ns in ((f"s{i}", "srv"), (f"e{i}", c)):
                sh(f"ip netns exec {ns} ethtool -K {dev} tso off gso off gro off", check=False)
                sh(f"ip netns exec {ns} tc qdisc add dev {dev} root handle 1: htb default 10")
                sh(f"ip netns exec {ns} tc class add dev {dev} parent 1: classid 1:10 htb rate 1000mbit ceil 1000mbit")
                sh(f"ip netns exec {ns} tc qdisc add dev {dev} parent 1:10 handle 10: pfifo limit 1000")
            sh(f"ip netns exec srv iptables -A INPUT -i s{i} -j NFQUEUE --queue-num {10 + i}")
            sh(f"ip netns exec {c} iptables -A INPUT -i e{i} -j NFQUEUE --queue-num 2")
            for key, ns, q in ((f"up{i}", "srv", 10 + i), (f"dn{i}", c, 2)):
                pf = self.wd / f"{key}.params"; pf.write_text(f"{q} 0 0 0 1\n")
                log = open(self.wd / f"{key}.nfq.log", "w")
                self.daemons[key] = subprocess.Popen(["ip", "netns", "exec", ns, NFQ, str(pf), str(q)], stdout=log, stderr=log)
            self.links[i] = (Link(), Link())
        time.sleep(0.5)
        return self

    def _hup(self, key):
        p = self.daemons.get(key)
        if p and p.poll() is None:
            os.kill(p.pid, signal.SIGHUP)

    def set_link(self, i, up: Link, down: Link):
        """Apply uplink (client->server) and downlink parameters to client i's link."""
        for dev, ns, L in ((f"e{i}", f"c{i}", up), (f"s{i}", "srv", down)):
            burst_bytes = max(32000, int(L.bw * 1e6 / 8 * 0.002))
            sh(f"ip netns exec {ns} tc class change dev {dev} parent 1: classid 1:10 htb rate {L.bw}mbit ceil {L.bw}mbit "
               f"burst {burst_bytes} cburst {burst_bytes}")
            limit = max(10, int(L.bw * 1e6 / 8 * L.qms / 1000 / 1500))
            sh(f"ip netns exec {ns} tc qdisc change dev {dev} parent 1:10 handle 10: pfifo limit {limit}")
        (self.wd / f"up{i}.params").write_text(f"{10 + i} {up.delay} {up.jitter} {up.loss} {up.burst}\n")
        (self.wd / f"dn{i}.params").write_text(f"2 {down.delay} {down.jitter} {down.loss} {down.burst}\n")
        self._hup(f"up{i}"); self._hup(f"dn{i}")
        self.links[i] = (up, down)

    def outage(self, i, on: bool):
        """Drop everything on client i's link (both directions) while on."""
        up, down = self.links[i]
        lu = 100 if on else up.loss
        ld = 100 if on else down.loss
        (self.wd / f"up{i}.params").write_text(f"{10 + i} {up.delay} {up.jitter} {lu} {up.burst}\n")
        (self.wd / f"dn{i}.params").write_text(f"2 {down.delay} {down.jitter} {ld} {down.burst}\n")
        self._hup(f"up{i}"); self._hup(f"dn{i}")

    def handover(self, i, gap_s=0.0):
        """Client i moves to a new address after an outage of gap_s seconds."""
        if gap_s > 0:
            self.outage(i, True); time.sleep(gap_s)
        old, new = self.addr[i], 5 - self.addr[i]
        sh(f"ip netns exec c{i} ip addr add 10.20.{i}.{new}/24 dev e{i}")
        sh(f"ip netns exec c{i} ip addr del 10.20.{i}.{old}/24 dev e{i}")
        self.addr[i] = new
        if gap_s > 0:
            self.outage(i, False)

    def down(self):
        for p in list(self.daemons.values()):
            try:
                p.terminate(); p.wait(3)
            except Exception:
                pass
        self.daemons = {}
        for line in sh("ip netns list", check=False).splitlines():
            ns = line.split()[0]
            if ns == "srv" or (ns.startswith("c") and ns[1:].isdigit()):
                sh(f"ip netns pids {ns} | xargs -r kill -9", check=False)
                sh(f"ip netns del {ns}", check=False)


def cpu_limit(pid, frac, name):
    """Limit a process to `frac` of one CPU (cgroup v1 cfs quota); frac <= 0 removes nothing."""
    if frac <= 0:
        return
    g = Path(f"/sys/fs/cgroup/cpu/fb_{name}")
    g.mkdir(exist_ok=True)
    (g / "cpu.cfs_period_us").write_text("100000")
    (g / "cpu.cfs_quota_us").write_text(str(int(100000 * frac)))
    (g / "cgroup.procs").write_text(str(pid))
