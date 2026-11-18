// fedbench: communication benchmark for synchronous FL rounds over MQTT, AMQP, gRPC and HTTP/3.
//
// One server process and N client processes. Each process reads JSON commands on stdin and writes JSON
// events on stdout (one per line). A round is: the server makes the global model of round r available
// (push protocols: published once through the broker; pull protocols: served on request), every client
// downloads it, verifies it, uploads its own model of the same size and waits for the server's
// application-level acknowledgement (sent only after the complete, checksum-verified upload).
//
// Wire format (identical for every protocol): the model is split into chunks; each chunk is a 24-byte
// little-endian header {round, cid, idx, total, len, crc32} followed by len payload bytes.
package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"
)

const hdrLen = 24

type Hdr struct{ Round, Cid, Idx, Total, Len, Crc uint32 }

func (h Hdr) put(b []byte) {
	le := binary.LittleEndian
	le.PutUint32(b[0:], h.Round)
	le.PutUint32(b[4:], h.Cid)
	le.PutUint32(b[8:], h.Idx)
	le.PutUint32(b[12:], h.Total)
	le.PutUint32(b[16:], h.Len)
	le.PutUint32(b[20:], h.Crc)
}

func parseHdr(b []byte) (Hdr, error) {
	if len(b) < hdrLen {
		return Hdr{}, errors.New("short header")
	}
	le := binary.LittleEndian
	return Hdr{le.Uint32(b[0:]), le.Uint32(b[4:]), le.Uint32(b[8:]), le.Uint32(b[12:]), le.Uint32(b[16:]), le.Uint32(b[20:])}, nil
}

// Payload produces the chunks of a model of `size` bytes. The content is a fixed pseudo-random block
// (TLS does not compress, so content does not affect timing); every chunk carries its own CRC.
type Payload struct {
	Size, Chunk int
	blk         []byte
	crc         map[int]uint32
	mu          sync.Mutex
}

func NewPayload(size, chunk int) *Payload {
	r := rand.New(rand.NewSource(42))
	b := make([]byte, chunk)
	r.Read(b)
	return &Payload{Size: size, Chunk: chunk, blk: b, crc: map[int]uint32{}}
}

func (p *Payload) N() int { return (p.Size + p.Chunk - 1) / p.Chunk }

func (p *Payload) lenOf(i int) int {
	if l := p.Size - i*p.Chunk; l < p.Chunk {
		return l
	}
	return p.Chunk
}

func (p *Payload) crcOf(n int) uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.crc[n]
	if !ok {
		c = crc32.ChecksumIEEE(p.blk[:n])
		p.crc[n] = c
	}
	return c
}

// ChunkBytes returns a fresh buffer (header + data) for chunk i.
func (p *Payload) ChunkBytes(round, cid, i int) []byte {
	n := p.lenOf(i)
	b := make([]byte, hdrLen+n)
	Hdr{uint32(round), uint32(cid), uint32(i), uint32(p.N()), uint32(n), p.crcOf(n)}.put(b)
	copy(b[hdrLen:], p.blk[:n])
	return b
}

// Verify checks a received chunk (header + data) and returns its header.
func (p *Payload) Verify(b []byte) (Hdr, error) {
	h, err := parseHdr(b)
	if err != nil {
		return h, err
	}
	if int(h.Len) != len(b)-hdrLen {
		return h, fmt.Errorf("length mismatch %d vs %d", h.Len, len(b)-hdrLen)
	}
	if int(h.Total) != p.N() || int(h.Len) != p.lenOf(int(h.Idx)) {
		return h, fmt.Errorf("unexpected chunk geometry idx=%d total=%d len=%d", h.Idx, h.Total, h.Len)
	}
	if crc32.ChecksumIEEE(b[hdrLen:]) != h.Crc || h.Crc != p.crcOf(int(h.Len)) {
		return h, errors.New("crc mismatch")
	}
	return h, nil
}

// Asm tracks the chunks of one model transfer.
type Asm struct {
	got  []bool
	n    int
	Done chan struct{}
	T    float64
}

func NewAsm(total int) *Asm { return &Asm{got: make([]bool, total), Done: make(chan struct{})} }

// Add marks chunk idx; returns true exactly once, when the transfer becomes complete.
func (a *Asm) Add(idx int) bool {
	if idx < 0 || idx >= len(a.got) || a.got[idx] {
		return false
	}
	a.got[idx] = true
	a.n++
	if a.n == len(a.got) {
		a.T = now()
		close(a.Done)
		return true
	}
	return false
}

func now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func cpuSeconds() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

var outMu sync.Mutex

func emit(v any) {
	b, _ := json.Marshal(v)
	outMu.Lock()
	os.Stdout.Write(append(b, '\n'))
	outMu.Unlock()
}

func logf(format string, a ...any) { fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05.000 ")+format+"\n", a...) }

type Cmd struct {
	Op      string  `json:"op"`
	R       int     `json:"r"`
	Timeout float64 `json:"timeout"`
	Attempt float64 `json:"attempt"` // per-attempt timeout (s); a stalled transfer is aborted and retried
	// per-client / per-direction selection (Phase 3). Server: Dl = clients whose downlink this server delivers,
	// Ul = clients whose upload it expects (nil = all clients, both directions). Client: Phase "dl", "ul" or "" (both).
	Dl    []int  `json:"dl"`
	Ul    []int  `json:"ul"`
	Phase string `json:"phase"`
}

func commands() <-chan Cmd {
	ch := make(chan Cmd)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var c Cmd
			if json.Unmarshal(sc.Bytes(), &c) == nil {
				ch <- c
			}
		}
		close(ch)
	}()
	return ch
}

func tlsConfigs(dir string) (srv, cli *tls.Config) {
	cert, err := tls.LoadX509KeyPair(dir+"/server.pem", dir+"/server.key")
	if err != nil {
		panic(err)
	}
	ca, err := os.ReadFile(dir + "/ca.pem")
	if err != nil {
		panic(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	srv = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	cli = &tls.Config{RootCAs: pool, ServerName: "fedbench", MinVersion: tls.VersionTLS13}
	return
}
