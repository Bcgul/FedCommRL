package main

// HTTP/3 over QUIC (quic-go). GET /down?r=R streams the global model as the response body;
// POST /up?r=R&cid=C streams the upload as the request body; the 200 response is the application
// acknowledgement, sent after the complete upload has been verified. Chunks are framed in the
// body exactly as on the other protocols. quic-go flow-control windows are left at library defaults.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func quicConf() *quic.Config {
	return &quic.Config{MaxIdleTimeout: 60 * time.Second, KeepAlivePeriod: 15 * time.Second}
}

// readChunks reads framed chunks from a byte stream and passes each to f.
func readChunks(r io.Reader, f func([]byte) error) error {
	hb := make([]byte, hdrLen)
	for {
		if _, err := io.ReadFull(r, hb); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		h, _ := parseHdr(hb)
		if h.Len > 64<<20 {
			return fmt.Errorf("chunk too large")
		}
		b := make([]byte, hdrLen+int(h.Len))
		copy(b, hb)
		if _, err := io.ReadFull(r, b[hdrLen:]); err != nil {
			return err
		}
		if err := f(b); err != nil {
			return err
		}
	}
}

type h3Srv struct{ s *Server }

func (h *h3Srv) Push() bool           { return false }
func (h *h3Srv) PushDown(r int, cids []int) error { return nil }
func (h *h3Srv) Ack(cid, r int) error { return nil }

func (h *h3Srv) Start(s *Server) error {
	h.s = s
	tlsS, _ := tlsConfigs(s.Certs)
	mux := http.NewServeMux()
	mux.HandleFunc("/down", func(w http.ResponseWriter, q *http.Request) {
		r, _ := strconv.Atoi(q.URL.Query().Get("r"))
		fl, _ := w.(http.Flusher)
		err := s.ServeDown(r, func(c []byte) error {
			_, err := w.Write(c)
			if fl != nil {
				fl.Flush()
			}
			return err
		})
		if err != nil {
			logf("h3 down: %v", err)
		}
	})
	mux.HandleFunc("/up", func(w http.ResponseWriter, q *http.Request) {
		r, _ := strconv.Atoi(q.URL.Query().Get("r"))
		cid, _ := strconv.Atoi(q.URL.Query().Get("cid"))
		err := readChunks(q.Body, func(b []byte) error { _, _, _, err := s.OnUpChunk(b); return err })
		if err == nil {
			a := s.UpAsm(cid, r)
			if a == nil {
				err = fmt.Errorf("stale round")
			} else {
				select {
				case <-a.Done:
				default:
					err = fmt.Errorf("upload incomplete")
				}
			}
		}
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		w.Write([]byte("ok"))
	})
	srv := &http3.Server{Addr: ":4433", Handler: mux, TLSConfig: http3.ConfigureTLSConfig(tlsS), QUICConfig: quicConf()}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logf("h3 server: %v", err)
		}
	}()
	return nil
}

type h3Cli struct {
	c    *Client
	tr   *http3.Transport
	http *http.Client
	base string
}

func (h *h3Cli) Push() bool { return false }

func (h *h3Cli) Start(c *Client) error {
	h.c = c
	_, tlsC := tlsConfigs(c.Certs)
	h.tr = &http3.Transport{TLSClientConfig: tlsC, QUICConfig: quicConf()}
	h.http = &http.Client{Transport: h.tr}
	h.base = "https://" + c.Server + ":4433"
	return nil
}

func (h *h3Cli) Download(ctx context.Context, r int) error {
	q, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/down?r=%d&cid=%d", h.base, r, h.c.ID), nil)
	resp, err := h.http.Do(q)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return readChunks(resp.Body, h.c.OnDownChunk)
}

func (h *h3Cli) Upload(ctx context.Context, r int) error {
	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < h.c.P.N(); i++ {
			if _, err := pw.Write(h.c.P.ChunkBytes(r, h.c.ID, i)); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()
	q, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/up?r=%d&cid=%d", h.base, r, h.c.ID), pr)
	resp, err := h.http.Do(q)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (h *h3Cli) Reconnect(ctx context.Context) error {
	old := h.tr
	_, tlsC := tlsConfigs(h.c.Certs)
	h.tr = &http3.Transport{TLSClientConfig: tlsC, QUICConfig: quicConf()}
	h.http = &http.Client{Transport: h.tr}
	go old.Close()
	return nil
}
