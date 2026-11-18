package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

type SrvTransport interface {
	Start(s *Server) error
	Push() bool
	PushDown(r int, cids []int) error // push protocols: publish the global model (cids nil = broadcast to all)
	Ack(cid, r int) error  // broker protocols: application acknowledgement of a complete upload
}

type CliTransport interface {
	Start(c *Client) error
	Push() bool
	Download(ctx context.Context, r int) error // pull protocols only
	Upload(ctx context.Context, r int) error   // returns after the server's acknowledgement
	Reconnect(ctx context.Context) error        // re-establish the session after a failed attempt
}

type Cfg struct {
	Role, Proto, Server, Certs string
	ID, N, Size, Chunk, Window  int
	GrpcWin                     int
}

// ---------------------------------------------------------------- server
type Server struct {
	Cfg
	P     *Payload
	tr    SrvTransport
	mu    sync.Mutex
	round int
	up    map[int]*Asm
	bad   int
}

func (s *Server) OnUpChunk(b []byte) (cid, r int, complete bool, err error) {
	h, err := s.P.Verify(b)
	if err != nil {
		s.mu.Lock()
		s.bad++
		s.mu.Unlock()
		return 0, 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(h.Round) != s.round {
		return int(h.Cid), int(h.Round), false, nil // stale chunk from an earlier round
	}
	a := s.up[int(h.Cid)]
	if a == nil {
		return int(h.Cid), int(h.Round), false, fmt.Errorf("unknown client %d", h.Cid)
	}
	done := a.Add(int(h.Idx))
	if !done && int(h.Idx) == s.P.N()-1 {
		select { // the last chunk of a repeated upload of an already complete update: acknowledge again
		case <-a.Done:
			done = true // (the client retried because the first acknowledgement was lost, e.g. on a reconnect)
		default:
		}
	}
	return int(h.Cid), int(h.Round), done, nil
}

func (s *Server) UpAsm(cid, r int) *Asm {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r != s.round {
		return nil
	}
	return s.up[cid]
}

// ServeDown writes the global model of round r chunk by chunk (pull protocols).
func (s *Server) ServeDown(r int, write func([]byte) error) error {
	s.mu.Lock()
	cur := s.round
	s.mu.Unlock()
	if r != cur {
		return fmt.Errorf("round %d requested, current %d", r, cur)
	}
	for i := 0; i < s.P.N(); i++ {
		if err := write(s.P.ChunkBytes(r, 0, i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) run() {
	if err := s.tr.Start(s); err != nil {
		emit(map[string]any{"op": "fatal", "err": err.Error()})
		os.Exit(1)
	}
	emit(map[string]any{"op": "ready", "proto": s.Proto, "chunks": s.P.N()})
	for c := range commands() {
		switch c.Op {
		case "round":
			s.mu.Lock()
			s.round = c.R
			s.up = map[int]*Asm{}
			if c.Ul == nil {
				for i := 1; i <= s.N; i++ {
					s.up[i] = NewAsm(s.P.N())
				}
			} else {
				for _, i := range c.Ul {
					s.up[i] = NewAsm(s.P.N())
				}
			}
			asms := s.up
			s.mu.Unlock()
			t0, cpu0 := now(), cpuSeconds()
			emit(map[string]any{"op": "started", "r": c.R, "t0": t0})
			pubDone := make(chan float64, 1)
			if s.tr.Push() && (c.Dl == nil || len(c.Dl) > 0) {
				go func(r int, cids []int) {
					if err := s.tr.PushDown(r, cids); err != nil {
						logf("push round %d: %v", r, err)
					}
					pubDone <- now()
				}(c.R, c.Dl)
			} else {
				pubDone <- 0
			}
			go func(r int, t0, cpu0 float64, timeout float64) {
				dl := time.After(time.Duration(timeout * float64(time.Second)))
				recv := map[string]float64{}
				for cid, a := range asms {
					select {
					case <-a.Done:
						recv[strconv.Itoa(cid)] = a.T
					case <-dl:
					}
				}
				pub := 0.0
				select {
				case pub = <-pubDone:
				case <-time.After(time.Second):
				}
				emit(map[string]any{"op": "round_done", "r": r, "t0": t0, "recv": recv, "pub_done": pub, "cpu_s": cpuSeconds() - cpu0, "bad_chunks": s.bad})
			}(c.R, t0, cpu0, c.Timeout)
		case "quit":
			return
		}
	}
}

// ---------------------------------------------------------------- client
type Client struct {
	Cfg
	P    *Payload
	tr   CliTransport
	mu   sync.Mutex
	down map[int]*Asm
	acks map[int]chan struct{}
}

func (c *Client) DownAsm(r int) *Asm {
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.down[r]
	if a == nil {
		a = NewAsm(c.P.N())
		c.down[r] = a
	}
	return a
}

func (c *Client) OnDownChunk(b []byte) error {
	h, err := c.P.Verify(b)
	if err != nil {
		return err
	}
	a := c.DownAsm(int(h.Round))
	c.mu.Lock()
	a.Add(int(h.Idx))
	c.mu.Unlock()
	return nil
}

func (c *Client) AckCh(r int) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.acks[r]
	if ch == nil {
		ch = make(chan struct{})
		c.acks[r] = ch
	}
	return ch
}

func (c *Client) OnAck(r int) {
	ch := c.AckCh(r)
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// round: download (wait for the push, or pull with retries), then upload with retries.
// Retry policy, identical for every protocol: an attempt that fails or makes no progress within
// cmd.Attempt seconds is abandoned, the session is re-established and the whole transfer is retried,
// until the round deadline cmd.Timeout. Chunks already delivered are not re-counted by the receiver.
func (c *Client) round(cmd Cmd) map[string]any {
	res := map[string]any{"op": "round_done", "r": cmd.R, "t_cmd": now(), "ok": false}
	cpu0 := cpuSeconds()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cmd.Timeout*float64(time.Second)))
	defer cancel()
	att := time.Duration(cmd.Attempt * float64(time.Second))
	if att <= 0 {
		att = time.Duration(cmd.Timeout * float64(time.Second))
	}
	tries := 0
	try := func(f func(context.Context) error) error {
		for {
			tries++
			actx, acancel := context.WithTimeout(ctx, att)
			err := f(actx)
			acancel()
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return err
			}
			logf("round %d attempt %d failed: %v", cmd.R, tries, err)
			rctx, rcancel := context.WithTimeout(ctx, att)
			c.tr.Reconnect(rctx)
			rcancel()
		}
	}
	a := c.DownAsm(cmd.R)
	var err error
	if cmd.Phase == "ul" { // downlink delivered by another protocol: upload only
		res["t_dl"] = now()
		err = try(func(actx context.Context) error { return c.tr.Upload(actx, cmd.R) })
		goto done
	}
	err = try(func(actx context.Context) error {
		if c.tr.Push() {
			select {
			case <-a.Done:
				return nil
			case <-actx.Done():
				return errors.New("downlink timeout")
			}
		}
		if err := c.tr.Download(actx, cmd.R); err != nil {
			return err
		}
		select {
		case <-a.Done:
			return nil
		default:
			return errors.New("downlink incomplete")
		}
	})
	if err == nil {
		res["t_dl"] = a.T
		if cmd.Phase != "dl" {
			err = try(func(actx context.Context) error { return c.tr.Upload(actx, cmd.R) })
		}
	}
done:
	if err != nil {
		res["err"] = err.Error()
	} else {
		res["ok"] = true
		res["t_ul"] = now()
	}
	res["attempts"] = tries
	res["cpu_s"] = cpuSeconds() - cpu0
	c.mu.Lock()
	delete(c.down, cmd.R-2)
	delete(c.acks, cmd.R-2)
	c.mu.Unlock()
	return res
}

func (c *Client) run() {
	if err := c.tr.Start(c); err != nil {
		emit(map[string]any{"op": "fatal", "err": err.Error()})
		os.Exit(1)
	}
	emit(map[string]any{"op": "ready", "id": c.ID, "proto": c.Proto})
	for cmd := range commands() {
		switch cmd.Op {
		case "round":
			emit(c.round(cmd))
		case "quit":
			return
		}
	}
}

func main() {
	var cfg Cfg
	flag.StringVar(&cfg.Role, "role", "server", "server|client")
	flag.StringVar(&cfg.Proto, "proto", "grpc", "mqtt|amqp|grpc|http3")
	flag.StringVar(&cfg.Server, "server", "127.0.0.1", "server address (client) / broker address (server)")
	flag.StringVar(&cfg.Certs, "certs", "certs", "directory with ca.pem, server.pem, server.key")
	flag.IntVar(&cfg.ID, "id", 1, "client id")
	flag.IntVar(&cfg.N, "n", 5, "number of clients")
	flag.IntVar(&cfg.Size, "size", 12_500_000, "model size in bytes")
	flag.IntVar(&cfg.Chunk, "chunk", 1<<20, "chunk payload bytes")
	flag.IntVar(&cfg.Window, "window", 64, "max unacknowledged chunks in flight (MQTT, AMQP)")
	flag.IntVar(&cfg.GrpcWin, "grpcwin", 0, "gRPC: fixed HTTP/2 stream and connection window in bytes (0 = grpc-go default, BDP-estimated)")
	flag.Parse()
	P := NewPayload(cfg.Size, cfg.Chunk)
	if cfg.Role == "server" {
		s := &Server{Cfg: cfg, P: P}
		s.tr = newSrvTransport(cfg)
		s.run()
	} else {
		c := &Client{Cfg: cfg, P: P, down: map[int]*Asm{}, acks: map[int]chan struct{}{}}
		c.tr = newCliTransport(cfg)
		c.run()
	}
}

func newSrvTransport(cfg Cfg) SrvTransport {
	switch cfg.Proto {
	case "mqtt":
		return &mqttSrv{}
	case "amqp":
		return &amqpSrv{}
	case "grpc":
		return &grpcSrv{}
	case "http3":
		return &h3Srv{}
	case "quic":
		return &quicSrv{}
	}
	panic("unknown proto " + cfg.Proto)
}

func newCliTransport(cfg Cfg) CliTransport {
	switch cfg.Proto {
	case "mqtt":
		return &mqttCli{}
	case "amqp":
		return &amqpCli{}
	case "grpc":
		return &grpcCli{}
	case "http3":
		return &h3Cli{}
	case "quic":
		return &quicCli{}
	}
	panic("unknown proto " + cfg.Proto)
}
