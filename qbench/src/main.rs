//! qbench: the fedbench round protocol over raw QUIC streams with quinn.
//! Download: client opens a bidirectional stream, sends 4-byte round, server streams the framed chunks and finishes.
//! Upload: client opens a bidirectional stream, streams its framed chunks and finishes; server replies "ok" after the
//! complete, CRC-verified upload (application acknowledgement). Retry policy as in bench/main.go.
use std::{collections::HashMap, net::SocketAddr, sync::{Arc, Mutex}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use quinn::{congestion, crypto::rustls::{QuicClientConfig, QuicServerConfig}, Endpoint, TransportConfig, VarInt};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::Notify;

const HDR: usize = 24;
const PORT: u16 = 4436;

fn now() -> f64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() }
fn cpu_s() -> f64 {
    unsafe { let mut ru: libc::rusage = std::mem::zeroed(); libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) as f64 + (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) as f64 / 1e6 }
}
fn emit(v: Value) { println!("{}", v); }
fn logf(s: String) { eprintln!("{:.3} {}", now(), s); }

#[derive(Clone)]
struct Cfg { role: String, server: String, certs: String, id: u32, n: u32, size: usize, chunk: usize, cc: String }

struct Payload { size: usize, chunk: usize, blk: Vec<u8> }
impl Payload {
    fn new(size: usize, chunk: usize) -> Self {
        let mut x: u64 = 42; let blk = (0..chunk).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x as u8 }).collect();
        Payload { size, chunk, blk }
    }
    fn n(&self) -> usize { (self.size + self.chunk - 1) / self.chunk }
    fn len_of(&self, i: usize) -> usize { std::cmp::min(self.chunk, self.size - i * self.chunk) }
    fn chunk_bytes(&self, round: u32, cid: u32, i: usize) -> Vec<u8> {
        let n = self.len_of(i); let crc = crc32fast::hash(&self.blk[..n]);
        let mut b = Vec::with_capacity(HDR + n);
        for v in [round, cid, i as u32, self.n() as u32, n as u32, crc] { b.extend_from_slice(&v.to_le_bytes()); }
        b.extend_from_slice(&self.blk[..n]); b
    }
    /// returns (round, cid, idx) of a verified chunk
    fn verify(&self, h: &[u8], data: &[u8]) -> Result<(u32, u32, usize), String> {
        let f = |k: usize| u32::from_le_bytes(h[k..k + 4].try_into().unwrap());
        let (r, c, i, tot, l, crc) = (f(0), f(4), f(8) as usize, f(12) as usize, f(16) as usize, f(20));
        if tot != self.n() || i >= tot || l != self.len_of(i) || l != data.len() { return Err("geometry".into()); }
        if crc32fast::hash(data) != crc || crc != crc32fast::hash(&self.blk[..l]) { return Err("crc".into()); }
        Ok((r, c, i))
    }
}

struct Asm { got: Vec<bool>, n: usize, t: f64, done: Arc<Notify>, complete: bool }
impl Asm {
    fn new(total: usize) -> Self { Asm { got: vec![false; total], n: 0, t: 0.0, done: Arc::new(Notify::new()), complete: false } }
    fn add(&mut self, i: usize) { if i < self.got.len() && !self.got[i] { self.got[i] = true; self.n += 1;
        if self.n == self.got.len() { self.t = now(); self.complete = true; self.done.notify_waiters(); } } }
}

fn transport(cc: &str) -> TransportConfig {
    let mut t = TransportConfig::default();
    t.max_idle_timeout(Some(VarInt::from_u32(60_000).into()));
    t.keep_alive_interval(Some(Duration::from_secs(15)));
    t.stream_receive_window(VarInt::from_u32(16 << 20));
    t.receive_window(VarInt::from_u32(32 << 20));
    t.send_window(32 << 20);
    match cc {
        "bbr" => { t.congestion_controller_factory(Arc::new(congestion::BbrConfig::default())); }
        "newreno" => { t.congestion_controller_factory(Arc::new(congestion::NewRenoConfig::default())); }
        _ => { t.congestion_controller_factory(Arc::new(congestion::CubicConfig::default())); }
    }
    t
}

fn load_certs(dir: &str) -> (Vec<rustls::pki_types::CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>, rustls::RootCertStore) {
    let certs = rustls_pemfile::certs(&mut &*std::fs::read(format!("{dir}/server.pem")).unwrap()).map(|c| c.unwrap()).collect();
    let key = rustls_pemfile::private_key(&mut &*std::fs::read(format!("{dir}/server.key")).unwrap()).unwrap().unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_pemfile::certs(&mut &*std::fs::read(format!("{dir}/ca.pem")).unwrap()) { roots.add(c.unwrap()).unwrap(); }
    (certs, key, roots)
}

async fn read_chunk(rs: &mut quinn::RecvStream) -> Result<Option<(Vec<u8>, Vec<u8>)>, String> {
    let mut h = vec![0u8; HDR];
    match rs.read_exact(&mut h).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    }
    let l = u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize;
    if l > 64 << 20 { return Err("chunk too large".into()); }
    let mut d = vec![0u8; l];
    rs.read_exact(&mut d).await.map_err(|e| e.to_string())?;
    Ok(Some((h, d)))
}

// ------------------------------------------------------------------ server
struct Srv { p: Payload, n: u32, round: Mutex<(u32, HashMap<u32, Asm>)> }

async fn serve_conn(s: Arc<Srv>, conn: quinn::Connection) {
    loop {
        let (mut ws, mut rs) = match conn.accept_bi().await { Ok(x) => x, Err(_) => return };
        let s = s.clone();
        tokio::spawn(async move {
            let mut kind = [0u8; 1];
            if rs.read_exact(&mut kind).await.is_err() { return; }
            if kind[0] == b'D' {
                let mut rb = [0u8; 4];
                if rs.read_exact(&mut rb).await.is_err() { return; }
                let r = u32::from_le_bytes(rb);
                if s.round.lock().unwrap().0 != r { let _ = ws.reset(VarInt::from_u32(1)); return; }
                for i in 0..s.p.n() { if ws.write_all(&s.p.chunk_bytes(r, 0, i)).await.is_err() { return; } }
                let _ = ws.finish();
                let _ = ws.stopped().await;
            } else {
                let (mut cid, mut r) = (0, 0);
                loop {
                    match read_chunk(&mut rs).await {
                        Ok(Some((h, d))) => match s.p.verify(&h, &d) {
                            Ok((rr, c, i)) => { cid = c; r = rr; let mut g = s.round.lock().unwrap();
                                if g.0 == rr { if let Some(a) = g.1.get_mut(&c) { a.add(i); } } }
                            Err(e) => { logf(format!("bad chunk {e}")); return; }
                        },
                        Ok(None) => break,
                        Err(_) => return,
                    }
                }
                let ok = { let g = s.round.lock().unwrap(); g.0 == r && g.1.get(&cid).map(|a| a.complete).unwrap_or(false) };
                if ok { let _ = ws.write_all(b"ok").await; let _ = ws.finish(); let _ = ws.stopped().await; }
                else { let _ = ws.reset(VarInt::from_u32(2)); }
            }
        });
    }
}

async fn server(cfg: Cfg) {
    let (certs, key, _) = load_certs(&cfg.certs);
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13]).unwrap().with_no_client_auth().with_single_cert(certs, key).unwrap();
    tls.alpn_protocols = vec![b"fb".to_vec()];
    let mut sc = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap()));
    sc.transport_config(Arc::new(transport(&cfg.cc)));
    let ep = Endpoint::server(sc, SocketAddr::from(([0, 0, 0, 0], PORT))).unwrap();
    let s = Arc::new(Srv { p: Payload::new(cfg.size, cfg.chunk), n: cfg.n, round: Mutex::new((0, HashMap::new())) });
    { let s = s.clone(); tokio::spawn(async move {
        while let Some(inc) = ep.accept().await { let s = s.clone();
            tokio::spawn(async move { if let Ok(c) = inc.await { serve_conn(s, c).await } }); } }); }
    emit(json!({"op": "ready", "proto": "quinn", "chunks": s.p.n()}));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(l)) = lines.next_line().await {
        let c: Value = match serde_json::from_str(&l) { Ok(v) => v, Err(_) => continue };
        match c["op"].as_str() {
            Some("round") => {
                let r = c["r"].as_u64().unwrap_or(0) as u32; let timeout = c["timeout"].as_f64().unwrap_or(60.0);
                let waits: Vec<(u32, Arc<Notify>)> = { let mut g = s.round.lock().unwrap(); g.0 = r; g.1.clear();
                    for i in 1..=s.n { g.1.insert(i, Asm::new(s.p.n())); }
                    g.1.iter().map(|(k, a)| (*k, a.done.clone())).collect() };
                let (t0, c0) = (now(), cpu_s());
                emit(json!({"op": "started", "r": r, "t0": t0}));
                let s2 = s.clone();
                tokio::spawn(async move {
                    let dl = tokio::time::Instant::now() + Duration::from_secs_f64(timeout);
                    let mut recv = serde_json::Map::new();
                    for (cid, nt) in waits {
                        loop {
                            let fut = nt.notified();
                            { let g = s2.round.lock().unwrap(); if let Some(a) = g.1.get(&cid) { if a.complete { recv.insert(cid.to_string(), json!(a.t)); break; } } }
                            if tokio::time::timeout_at(dl, fut).await.is_err() { break; }
                        }
                    }
                    emit(json!({"op": "round_done", "r": r, "t0": t0, "recv": recv, "pub_done": 0, "cpu_s": cpu_s() - c0, "bad_chunks": 0}));
                });
            }
            Some("quit") => std::process::exit(0),
            _ => {}
        }
    }
}

// ------------------------------------------------------------------ client
struct Cli { cfg: Cfg, p: Payload, ep: Endpoint, cc: quinn::ClientConfig, conn: Option<quinn::Connection> }

impl Cli {
    async fn connect(&mut self) -> Result<quinn::Connection, String> {
        if let Some(c) = &self.conn { if c.close_reason().is_none() { return Ok(c.clone()); } }
        let addr: SocketAddr = format!("{}:{}", self.cfg.server, PORT).parse().unwrap();
        let c = self.ep.connect_with(self.cc.clone(), addr, "fedbench").map_err(|e| e.to_string())?.await.map_err(|e| e.to_string())?;
        self.conn = Some(c.clone()); Ok(c)
    }
    async fn download(&mut self, r: u32) -> Result<f64, String> {
        let c = self.connect().await?;
        let (mut ws, mut rs) = c.open_bi().await.map_err(|e| e.to_string())?;
        let mut q = vec![b'D']; q.extend_from_slice(&r.to_le_bytes());
        ws.write_all(&q).await.map_err(|e| e.to_string())?; let _ = ws.finish();
        let mut a = Asm::new(self.p.n());
        while let Some((h, d)) = read_chunk(&mut rs).await? { let (_, _, i) = self.p.verify(&h, &d)?; a.add(i); }
        if a.complete { Ok(a.t) } else { Err("downlink incomplete".into()) }
    }
    async fn upload(&mut self, r: u32) -> Result<(), String> {
        let c = self.connect().await?;
        let (mut ws, mut rs) = c.open_bi().await.map_err(|e| e.to_string())?;
        ws.write_all(b"U").await.map_err(|e| e.to_string())?;
        for i in 0..self.p.n() { ws.write_all(&self.p.chunk_bytes(r, self.cfg.id, i)).await.map_err(|e| e.to_string())?; }
        let _ = ws.finish();
        let mut ack = Vec::new();
        rs.read_to_end(16).await.map(|b| ack = b).map_err(|e| e.to_string())?;
        if ack == b"ok" { Ok(()) } else { Err("no ack".into()) }
    }
    fn reconnect(&mut self) { if let Some(c) = self.conn.take() { c.close(VarInt::from_u32(0), b"retry"); } }
}

async fn client(cfg: Cfg) {
    let (_, _, roots) = load_certs(&cfg.certs);
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13]).unwrap().with_root_certificates(roots).with_no_client_auth();
    tls.alpn_protocols = vec![b"fb".to_vec()];
    let mut cc = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
    cc.transport_config(Arc::new(transport(&cfg.cc)));
    let ep = Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0))).unwrap();
    let mut cl = Cli { p: Payload::new(cfg.size, cfg.chunk), cfg, ep, cc, conn: None };
    let start_ok = tokio::time::timeout(Duration::from_secs(30), cl.connect()).await.map(|r| r.is_ok()).unwrap_or(false);
    if !start_ok { emit(json!({"op": "fatal", "err": "connect failed"})); std::process::exit(1); }
    emit(json!({"op": "ready", "id": cl.cfg.id, "proto": "quinn"}));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(l)) = lines.next_line().await {
        let c: Value = match serde_json::from_str(&l) { Ok(v) => v, Err(_) => continue };
        match c["op"].as_str() {
            Some("round") => {
                let r = c["r"].as_u64().unwrap_or(0) as u32;
                let timeout = Duration::from_secs_f64(c["timeout"].as_f64().unwrap_or(60.0));
                let att = Duration::from_secs_f64(c["attempt"].as_f64().filter(|a| *a > 0.0).unwrap_or(timeout.as_secs_f64()));
                let (t_cmd, c0, end) = (now(), cpu_s(), Instant::now() + timeout);
                let mut tries = 0; let mut res = json!({"op": "round_done", "r": r, "t_cmd": t_cmd, "ok": false});
                let mut t_dl = None; let mut err = String::new();
                while Instant::now() < end {
                    tries += 1; let lim = std::cmp::min(att, end.saturating_duration_since(Instant::now()));
                    match tokio::time::timeout(lim, cl.download(r)).await {
                        Ok(Ok(t)) => { t_dl = Some(t); break; }
                        Ok(Err(e)) => err = e, Err(_) => err = "downlink timeout".into(),
                    }
                    logf(format!("round {r} attempt {tries} failed: {err}")); cl.reconnect();
                }
                if let Some(t) = t_dl {
                    res["t_dl"] = json!(t);
                    let mut ok = false;
                    while Instant::now() < end {
                        tries += 1; let lim = std::cmp::min(att, end.saturating_duration_since(Instant::now()));
                        match tokio::time::timeout(lim, cl.upload(r)).await {
                            Ok(Ok(())) => { ok = true; break; }
                            Ok(Err(e)) => err = e, Err(_) => err = "upload timeout".into(),
                        }
                        logf(format!("round {r} attempt {tries} failed: {err}")); cl.reconnect();
                    }
                    if ok { res["ok"] = json!(true); res["t_ul"] = json!(now()); }
                }
                if res["ok"] != json!(true) { res["err"] = json!(err); }
                res["attempts"] = json!(tries); res["cpu_s"] = json!(cpu_s() - c0);
                emit(res);
            }
            Some("quit") => std::process::exit(0),
            _ => {}
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| a.iter().position(|x| x == k).and_then(|i| a.get(i + 1)).cloned().unwrap_or(d.to_string());
    let cfg = Cfg { role: get("-role", "server"), server: get("-server", "127.0.0.1"), certs: get("-certs", "certs"),
        id: get("-id", "1").parse().unwrap(), n: get("-n", "5").parse().unwrap(), size: get("-size", "12500000").parse().unwrap(),
        chunk: get("-chunk", "1048576").parse().unwrap(), cc: get("-cc", "cubic") };
    if cfg.role == "server" { server(cfg).await } else { client(cfg).await }
}
