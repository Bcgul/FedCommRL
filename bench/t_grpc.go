package main

// gRPC over TLS (grpc-go). Service fb.Bench with a server-streaming Download and a client-streaming
// Upload; messages are raw chunk bytes (a pass-through codec, no protobuf re-encoding), so every
// protocol carries identical bytes. The Upload response is the application acknowledgement.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return *(v.(*[]byte)), nil }
func (rawCodec) Unmarshal(data []byte, v any) error {
	p := v.(*[]byte)
	*p = append((*p)[:0], data...)
	return nil
}
func (rawCodec) Name() string { return "raw" }

type req struct {
	R   int `json:"r"`
	Cid int `json:"cid"`
}

var benchDesc = grpc.ServiceDesc{
	ServiceName: "fb.Bench",
	HandlerType: (*any)(nil),
	Streams: []grpc.StreamDesc{
		{StreamName: "Download", ServerStreams: true, Handler: func(srv any, st grpc.ServerStream) error {
			s := srv.(*grpcSrv).s
			var b []byte
			if err := st.RecvMsg(&b); err != nil {
				return err
			}
			var q req
			json.Unmarshal(b, &q)
			return s.ServeDown(q.R, func(c []byte) error { return st.SendMsg(&c) })
		}},
		{StreamName: "Upload", ClientStreams: true, Handler: func(srv any, st grpc.ServerStream) error {
			s := srv.(*grpcSrv).s
			cid, r := 0, 0
			for {
				var b []byte
				err := st.RecvMsg(&b)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				c, rr, _, err := s.OnUpChunk(b)
				if err != nil {
					return err
				}
				cid, r = c, rr
			}
			a := s.UpAsm(cid, r)
			if a == nil {
				return fmt.Errorf("upload for stale round %d", r)
			}
			select {
			case <-a.Done:
			default:
				return fmt.Errorf("upload incomplete")
			}
			ok := []byte("ok")
			return st.SendMsg(&ok)
		}},
	},
}

type grpcSrv struct{ s *Server }

func (g *grpcSrv) Push() bool           { return false }
func (g *grpcSrv) PushDown(r int, cids []int) error { return nil }
func (g *grpcSrv) Ack(cid, r int) error { return nil }

func (g *grpcSrv) Start(s *Server) error {
	g.s = s
	tlsS, _ := tlsConfigs(s.Certs)
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsS)), grpc.ForceServerCodec(rawCodec{})}
	if s.GrpcWin > 0 {
		opts = append(opts, grpc.InitialWindowSize(int32(s.GrpcWin)), grpc.InitialConnWindowSize(int32(s.GrpcWin)))
	}
	gs := grpc.NewServer(opts...)
	gs.RegisterService(&benchDesc, g)
	go gs.Serve(lis)
	return nil
}

type grpcCli struct {
	c    *Client
	conn *grpc.ClientConn
}

func (g *grpcCli) Push() bool { return false }

func (g *grpcCli) Start(c *Client) error {
	g.c = c
	return g.dialNew()
}

func (g *grpcCli) dialNew() error {
	_, tlsC := tlsConfigs(g.c.Certs)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsC)), grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}))}
	if g.c.GrpcWin > 0 {
		opts = append(opts, grpc.WithInitialWindowSize(int32(g.c.GrpcWin)), grpc.WithInitialConnWindowSize(int32(g.c.GrpcWin)))
	}
	conn, err := grpc.NewClient(g.c.Server+":50051", opts...)
	if err != nil {
		return err
	}
	conn.Connect()
	g.conn = conn
	return nil
}

func (g *grpcCli) Download(ctx context.Context, r int) error {
	st, err := g.conn.NewStream(ctx, &benchDesc.Streams[0], "/fb.Bench/Download")
	if err != nil {
		return err
	}
	b, _ := json.Marshal(req{r, g.c.ID})
	if err = st.SendMsg(&b); err != nil {
		return err
	}
	if err = st.CloseSend(); err != nil {
		return err
	}
	for {
		var m []byte
		err := st.RecvMsg(&m)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := g.c.OnDownChunk(m); err != nil {
			return err
		}
	}
}

func (g *grpcCli) Upload(ctx context.Context, r int) error {
	st, err := g.conn.NewStream(ctx, &benchDesc.Streams[1], "/fb.Bench/Upload")
	if err != nil {
		return err
	}
	for i := 0; i < g.c.P.N(); i++ {
		b := g.c.P.ChunkBytes(r, g.c.ID, i)
		if err := st.SendMsg(&b); err != nil {
			return err
		}
	}
	if err := st.CloseSend(); err != nil {
		return err
	}
	var ack []byte
	return st.RecvMsg(&ack)
}

// Reconnect re-establishes the session as the retry policy requires: a new channel and a new TCP/TLS connection
// (resetting the backoff alone kept using a dead HTTP/2 connection after an address change until TCP gave up).
func (g *grpcCli) Reconnect(ctx context.Context) error {
	old := g.conn
	if err := g.dialNew(); err != nil {
		return err
	}
	go old.Close()
	return nil
}
