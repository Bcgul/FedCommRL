package main

// Raw QUIC streams (quic-go), no HTTP/3 layer; same stream protocol as qbench (quinn), so the two QUIC stacks are
// compared on identical framing. Download: bidi stream, client sends 'D' + uint32 round, server streams the chunks
// and closes. Upload: bidi stream, client sends 'U' + chunks and closes its side; server answers "ok" after the
// complete, verified upload. Flow-control windows equal to qbench (16 MiB stream, 32 MiB connection).

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"os"
	"sort"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/logging"
)

const quicPort = ":4435"

func rawQuicConf() *quic.Config {
	c := &quic.Config{MaxIdleTimeout: 60 * time.Second, KeepAlivePeriod: 15 * time.Second,
		InitialStreamReceiveWindow: 16 << 20, MaxStreamReceiveWindow: 16 << 20,
		InitialConnectionReceiveWindow: 32 << 20, MaxConnectionReceiveWindow: 32 << 20}
	if os.Getenv("FB_CCTRACE") != "" { // diagnosis only: count losses, congestion events and cwnd
		c.Tracer = ccTracer
	}
	return c
}

// ccTracer logs, per connection on close: lost packets, entries into recovery (= congestion events),
// packets sent and the median congestion window while data was in flight.
func ccTracer(_ context.Context, p logging.Perspective, _ logging.ConnectionID) *logging.ConnectionTracer {
	var mu sync.Mutex
	var lost, recov, sent int
	var cw []int
	inRecovery := false
	report := func() {
		c := append([]int(nil), cw...)
		sort.Ints(c)
		med := 0
		if len(c) > 0 {
			med = c[len(c)/2]
		}
		logf("CCTRACE perspective=%v sent=%d lost=%d congestion_events=%d cwnd_median_bytes=%d", p, sent, lost, recov, med)
	}
	go func() {
		for range time.Tick(5 * time.Second) {
			mu.Lock(); report(); mu.Unlock()
		}
	}()
	return &logging.ConnectionTracer{
		SentShortHeaderPacket: func(*logging.ShortHeader, logging.ByteCount, logging.ECN, *logging.AckFrame, []logging.Frame) {
			mu.Lock(); sent++; mu.Unlock()
		},
		LostPacket: func(logging.EncryptionLevel, logging.PacketNumber, logging.PacketLossReason) { mu.Lock(); lost++; mu.Unlock() },
		UpdatedCongestionState: func(st logging.CongestionState) {
			mu.Lock()
			if st == logging.CongestionStateRecovery && !inRecovery {
				recov++
			}
			inRecovery = st == logging.CongestionStateRecovery
			mu.Unlock()
		},
		UpdatedMetrics: func(_ *logging.RTTStats, cwnd, inFlight logging.ByteCount, _ int) {
			if inFlight > 0 {
				mu.Lock(); cw = append(cw, int(cwnd)); mu.Unlock()
			}
		},
		ClosedConnection: func(error) { mu.Lock(); report(); mu.Unlock() },
	}
}

type quicSrv struct{ s *Server }

func (q *quicSrv) Push() bool           { return false }
func (q *quicSrv) PushDown(r int, cids []int) error { return nil }
func (q *quicSrv) Ack(cid, r int) error { return nil }

func (q *quicSrv) Start(s *Server) error {
	q.s = s
	tlsS, _ := tlsConfigs(s.Certs)
	tlsS = tlsS.Clone()
	tlsS.NextProtos = []string{"fb"}
	ln, err := quic.ListenAddr(quicPort, tlsS, rawQuicConf())
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				for {
					st, err := conn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					go q.handle(st)
				}
			}()
		}
	}()
	return nil
}

func (q *quicSrv) handle(st *quic.Stream) {
	k := make([]byte, 1)
	if _, err := io.ReadFull(st, k); err != nil {
		return
	}
	if k[0] == 'D' {
		rb := make([]byte, 4)
		if _, err := io.ReadFull(st, rb); err != nil {
			return
		}
		err := q.s.ServeDown(int(binary.LittleEndian.Uint32(rb)), func(c []byte) error { _, err := st.Write(c); return err })
		if err != nil {
			st.CancelWrite(1)
			return
		}
		st.Close()
		return
	}
	cid, r := 0, 0
	err := readChunks(st, func(b []byte) error {
		c, rr, _, err := q.s.OnUpChunk(b)
		cid, r = c, rr
		return err
	})
	if err == nil {
		if a := q.s.UpAsm(cid, r); a != nil {
			select {
			case <-a.Done:
				st.Write([]byte("ok"))
				st.Close()
				return
			default:
			}
		}
	}
	st.CancelWrite(2)
}

type quicCli struct {
	c    *Client
	conn *quic.Conn
}

func (q *quicCli) Push() bool { return false }

func (q *quicCli) Start(c *Client) error {
	q.c = c
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return q.dial(ctx)
}

func (q *quicCli) dial(ctx context.Context) error {
	_, tlsC := tlsConfigs(q.c.Certs)
	tlsC = tlsC.Clone()
	tlsC.NextProtos = []string{"fb"}
	conn, err := quic.DialAddr(ctx, q.c.Server+quicPort, tlsC, rawQuicConf())
	if err != nil {
		return err
	}
	q.conn = conn
	return nil
}

func (q *quicCli) session(ctx context.Context) (*quic.Conn, error) {
	if q.conn == nil || q.conn.Context().Err() != nil {
		if err := q.dial(ctx); err != nil {
			return nil, err
		}
	}
	return q.conn, nil
}

func (q *quicCli) Download(ctx context.Context, r int) error {
	conn, err := q.session(ctx)
	if err != nil {
		return err
	}
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { st.CancelRead(3); st.CancelWrite(3) })
	defer stop()
	b := []byte{'D', 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(b[1:], uint32(r))
	if _, err := st.Write(b); err != nil {
		return err
	}
	st.Close()
	return readChunks(st, q.c.OnDownChunk)
}

func (q *quicCli) Upload(ctx context.Context, r int) error {
	conn, err := q.session(ctx)
	if err != nil {
		return err
	}
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { st.CancelRead(3); st.CancelWrite(3) })
	defer stop()
	if _, err := st.Write([]byte{'U'}); err != nil {
		return err
	}
	for i := 0; i < q.c.P.N(); i++ {
		if _, err := st.Write(q.c.P.ChunkBytes(r, q.c.ID, i)); err != nil {
			return err
		}
	}
	st.Close()
	ack, err := io.ReadAll(st)
	if err != nil {
		return err
	}
	if string(ack) != "ok" {
		return fmt.Errorf("no ack")
	}
	return nil
}

func (q *quicCli) Reconnect(ctx context.Context) error {
	if q.conn != nil {
		q.conn.CloseWithError(0, "retry")
		q.conn = nil
	}
	return nil
}
